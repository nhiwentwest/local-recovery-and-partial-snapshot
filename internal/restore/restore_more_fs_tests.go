package restore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

func TestReplayChangelog_MixedKeys_SeqSkipCounts(t *testing.T) {
	base := t.TempDir()
	cl := filepath.Join(base, "cl_mix.jsonl")
	var b strings.Builder
	// K1 seq1 apply
	b.WriteString(`{"key":"K#1","seq":1,"delta":1}` + "\n")
	// K1 seq1 duplicate skip
	b.WriteString(`{"key":"K#1","seq":1,"delta":999}` + "\n")
	// K2 seq1 apply
	b.WriteString(`{"key":"K#2","seq":1,"delta":2}` + "\n")
	// K1 seq2 apply
	b.WriteString(`{"key":"K#1","seq":2,"delta":3}` + "\n")
	// K2 seq1 duplicate skip
	b.WriteString(`{"key":"K#2","seq":1,"delta":4}` + "\n")
	if err := os.WriteFile(cl, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res := r.ReplayChangelog(cl, 0)
	if res.Error != nil {
		t.Fatalf("err: %v", res.Error)
	}
	if res.Applied != 3 || res.Skipped != 2 {
		t.Fatalf("want applied=3 skipped=2, got %+v", res)
	}
}

func TestReplayChangelog_EmptyLineBetween_ShouldError(t *testing.T) {
	base := t.TempDir()
	cl := filepath.Join(base, "cl_emptyline.jsonl")
	content := `{"key":"K#1","seq":1,"delta":1}` + "\n" + "\n" + `{"key":"K#1","seq":2,"delta":1}` + "\n"
	if err := os.WriteFile(cl, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res := r.ReplayChangelog(cl, 0)
	if res.Error == nil {
		t.Fatalf("expected error on empty line JSON")
	}
}

func TestRestoreFromSnapshot_LargeLoadAll(t *testing.T) {
	base := t.TempDir()
	sid := "sid-large"
	dir := filepath.Join(base, sid)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	m := make(map[string]state.RecordState)
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("K#%d#100", i)
		m[k] = state.RecordState{SumAmount: int64(i), SumQty: 1, LastSeq: 1}
	}
	b, _ := json.Marshal(m)
	if err := os.WriteFile(filepath.Join(dir, "state.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	if err := r.RestoreFromSnapshot(sid); err != nil {
		t.Fatalf("err: %v", err)
	}
	// count keys by iterating Range
	cnt := 0
	_ = st.Range(func(_ string, _ state.RecordState) error { cnt++; return nil })
	if cnt != 100 {
		t.Fatalf("expected 100 keys loaded, got %d", cnt)
	}
}
