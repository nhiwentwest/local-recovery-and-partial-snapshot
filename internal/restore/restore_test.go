package restore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

func TestRestoreAndReplay_MinimalFlow(t *testing.T) {
	// Work in an isolated temp dir so we can satisfy relative paths in code (./changelog/opb.jsonl)
	oldWD, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(oldWD) })
	base := t.TempDir()
	if err := os.Chdir(base); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	// Prepare manifest in base dir
	mf := manifest.NewFilesystemManifest(base)
	if err := mf.PublishLatest("sid-test", 1); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	// Prepare changelog at ./changelog/opb.jsonl
	if err := os.MkdirAll("changelog", 0o755); err != nil {
		t.Fatalf("mkdir changelog: %v", err)
	}
	clPath := filepath.Join("changelog", "opb.jsonl")
	f, err := os.Create(clPath)
	if err != nil {
		t.Fatalf("create changelog: %v", err)
	}
	_, _ = f.WriteString(`{"key":"A#p1#100","seq":1,"delta":100,"deltaQty":1,"ts":1}` + "\n")
	_, _ = f.WriteString(`{"key":"A#p2#100","seq":1,"delta":200,"deltaQty":2,"ts":2}` + "\n")
	_, _ = f.WriteString(`{"key":"A#p3#100","seq":2,"delta":300,"deltaQty":3,"ts":3}` + "\n")
	_ = f.Close()

	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res, err := r.RestoreAndReplay()
	if err != nil {
		t.Fatalf("RestoreAndReplay error: %v", err)
	}

	// With lastChangelogOffset=1, the first line is skipped; the remaining 2 are applied.
	if res.Applied != 2 || res.Skipped != 0 {
		t.Fatalf("unexpected result: %+v (GOOS=%s)", res, runtime.GOOS)
	}
}

func TestRestoreFromSnapshot_LoadsState(t *testing.T) {
	base := t.TempDir()
	// Prepare snapshot JSON manually
	sid := "sid-001"
	dir := filepath.Join(base, sid)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir snapshot: %v", err)
	}
	// Map of key -> RecordState
	dump := map[string]state.RecordState{
		"A#p1#100": {SumAmount: 1500, SumQty: 3, LastSeq: 3},
		"B#p2#200": {SumAmount: 700, SumQty: 2, LastSeq: 1},
	}
	b, _ := json.Marshal(dump)
	if err := os.WriteFile(filepath.Join(dir, "state.json"), b, 0o644); err != nil {
		t.Fatalf("write state.json: %v", err)
	}

	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	if err := r.RestoreFromSnapshot(sid); err != nil {
		t.Fatalf("RestoreFromSnapshot error: %v", err)
	}
	// Assert store loaded
	st1, ok := st.Get("A#p1#100")
	if !ok || st1.SumAmount != 1500 || st1.SumQty != 3 || st1.LastSeq != 3 {
		t.Fatalf("bad state for A#p1#100: %+v", st1)
	}
	st2, ok := st.Get("B#p2#200")
	if !ok || st2.SumAmount != 700 || st2.SumQty != 2 || st2.LastSeq != 1 {
		t.Fatalf("bad state for B#p2#200: %+v", st2)
	}
}

func TestReplayChangelog_IdempotencyAndGaps(t *testing.T) {
	base := t.TempDir()
	clDir := filepath.Join(base, "changelog")
	if err := os.MkdirAll(clDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(clDir, "opb.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	// seq=1 apply, seq=1 duplicate skip, seq=3 gap apply, seq=2 lower-than-last skip
	_, _ = f.WriteString(`{"key":"K#1","seq":1,"delta":10,"deltaQty":1,"ts":1}` + "\n")
	_, _ = f.WriteString(`{"key":"K#1","seq":1,"delta":999,"deltaQty":9,"ts":2}` + "\n")
	_, _ = f.WriteString(`{"key":"K#1","seq":3,"delta":5,"deltaQty":1,"ts":3}` + "\n")
	_, _ = f.WriteString(`{"key":"K#1","seq":2,"delta":100,"deltaQty":10,"ts":4}` + "\n")
	_ = f.Close()

	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res := r.ReplayChangelog(path, 0)
	if res.Error != nil {
		t.Fatalf("replay error: %v", res.Error)
	}
	if res.Applied != 2 || res.Skipped != 2 {
		t.Fatalf("want applied=2 skipped=2, got %+v", res)
	}
	// Validate final state
	stFin, ok := st.Get("K#1")
	if !ok {
		t.Fatalf("missing key")
	}
	if stFin.LastSeq != 3 || stFin.SumAmount != 15 || stFin.SumQty != 2 {
		t.Fatalf("unexpected final state: %+v", stFin)
	}
}

func TestReplayChangelog_EmptyAndMalformed(t *testing.T) {
	base := t.TempDir()
	clDir := filepath.Join(base, "changelog")
	if err := os.MkdirAll(clDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Empty file
	empty := filepath.Join(clDir, "empty.jsonl")
	if err := os.WriteFile(empty, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res := r.ReplayChangelog(empty, 0)
	if res.Error != nil || res.Applied != 0 || res.Skipped != 0 {
		t.Fatalf("empty file unexpected: %+v", res)
	}
	// Malformed after one good line
	bad := filepath.Join(clDir, "bad.jsonl")
	content := `{"key":"A#p","seq":1,"delta":1,"ts":1}\n{bad json}`
	if err := os.WriteFile(bad, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	res = r.ReplayChangelog(bad, 0)
	if res.Error == nil {
		t.Fatalf("expected error for malformed JSONL, got nil")
	}
}
