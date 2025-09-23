package restore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

// Integration: snapshot -> manifest -> changelog -> RestoreAndReplay -> final state
func TestIntegration_RestoreAndReplay_EndToEnd(t *testing.T) {
	// Isolate working directory so relative paths like ./changelog/opb.jsonl work
	oldWD, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(oldWD) })
	base := t.TempDir()
	if err := os.Chdir(base); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	// 1) Prepare initial state and write snapshot
	prep := state.NewInMemoryStore()
	// key1: A#p1#100 -> seq=2, amount=200, qty=2
	_, _, _ = prep.Apply("A#p1#100", 100, 1, 1)
	_, _, _ = prep.Apply("A#p1#100", 100, 1, 2)
	// key2: B#p2#100 -> seq=1, amount=50, qty=1
	_, _, _ = prep.Apply("B#p2#100", 50, 1, 1)

	snap := snapshot.NewFilesystemSnapshotter(base)
	sid := "sid-int"
	if err := snap.WriteSnapshot(sid, prep); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	// Sanity: snapshot file exists and contains data
	b, err := os.ReadFile(filepath.Join(base, sid, "state.json"))
	if err != nil {
		t.Fatalf("read state.json: %v", err)
	}
	var dump map[string]state.RecordState
	if err := json.Unmarshal(b, &dump); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if len(dump) != 2 {
		t.Fatalf("snapshot should have 2 keys, got %d", len(dump))
	}

	// 2) Publish manifest pointing to snapshot; fromOffset=1
	mf := manifest.NewFilesystemManifest(base)
	if err := mf.PublishLatest(sid, 1); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	// 3) Create changelog with multiple lines
	if err := os.MkdirAll("changelog", 0o755); err != nil {
		t.Fatalf("mkdir changelog: %v", err)
	}
	cl := filepath.Join("changelog", "opb.jsonl")
	f, err := os.Create(cl)
	if err != nil {
		t.Fatalf("create changelog: %v", err)
	}
	// offset=1 will skip first line; still make it a duplicate seq for key1
	_, _ = f.WriteString(`{"key":"A#p1#100","seq":2,"delta":999,"deltaQty":9,"ts":1}` + "\n")
	// apply on key1 with seq=3
	_, _ = f.WriteString(`{"key":"A#p1#100","seq":3,"delta":30,"deltaQty":3,"ts":2}` + "\n")
	// duplicate or older for key2 (seq=1) -> should skip because LastSeq=1 after restore
	_, _ = f.WriteString(`{"key":"B#p2#100","seq":1,"delta":123,"deltaQty":1,"ts":3}` + "\n")
	// apply on key2 with seq=2
	_, _ = f.WriteString(`{"key":"B#p2#100","seq":2,"delta":20,"deltaQty":2,"ts":4}` + "\n")
	// new key3 apply
	_, _ = f.WriteString(`{"key":"C#p3#200","seq":1,"delta":5,"deltaQty":1,"ts":5}` + "\n")
	_ = f.Close()

	// 4) Run RestoreAndReplay on a fresh store
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res, err := r.RestoreAndReplay()
	if err != nil {
		t.Fatalf("RestoreAndReplay: %v", err)
	}

	// 5) Assert final state values
	k1, _ := st.Get("A#p1#100")
	if k1.LastSeq != 3 || k1.SumAmount != 230 || k1.SumQty != 5 {
		t.Fatalf("key1 unexpected: %+v", k1)
	}
	k2, _ := st.Get("B#p2#100")
	if k2.LastSeq != 2 || k2.SumAmount != 70 || k2.SumQty != 3 {
		t.Fatalf("key2 unexpected: %+v", k2)
	}
	k3, ok := st.Get("C#p3#200")
	if !ok || k3.LastSeq != 1 || k3.SumAmount != 5 || k3.SumQty != 1 {
		t.Fatalf("key3 unexpected: %+v", k3)
	}

	// And the result counters: applied lines are 3 (seq=3 for k1, seq=2 for k2, seq=1 for k3), skipped = 1 (duplicate/older). Offset-skipped lines are not counted in Skipped.
	if res.Applied != 3 || res.Skipped != 1 {
		t.Fatalf("result unexpected: %+v", res)
	}
}
