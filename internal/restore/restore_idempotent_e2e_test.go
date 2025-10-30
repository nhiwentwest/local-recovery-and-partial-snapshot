package restore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

// This test automates the end-to-end scenario:
// 1) A snapshot exists with state already reflecting the deltas in changelog
// 2) A manifest points to that snapshot and fromOffset=0
// 3) Replay of changelog should be fully idempotent: applied=0, skipped=3
func TestEndToEnd_SnapshotThenRestoreReplay_Idempotent(t *testing.T) {
	base := t.TempDir()

	// Ensure relative paths inside RestoreAndReplay (./changelog/opb.jsonl) resolve to this base
	oldWD, _ := os.Getwd()
	if err := os.Chdir(base); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer func() { _ = os.Chdir(oldWD) }()

	// 1) Create snapshot with two keys that already include the following deltas
	sid := "sid-e2e-001"
	snapDir := filepath.Join(base, sid)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("mkdir snapshot: %v", err)
	}
	// State matches the deltas we will put into changelog below
	dump := map[string]state.RecordState{
		"A#p1#1694499900": {SumAmount: 30000, SumQty: 3, LastSeq: 2}, // seq 1 then 2 already applied
		"A#p2#1694499900": {SumAmount: 15000, SumQty: 3, LastSeq: 1}, // seq 1 already applied
	}
	b, _ := json.Marshal(dump)
	if err := os.WriteFile(filepath.Join(snapDir, "state.json"), b, 0o644); err != nil {
		t.Fatalf("write state.json: %v", err)
	}

	// 2) Publish manifest pointing to this snapshot, fromOffset=0
	mf := manifest.NewFilesystemManifest(base)
	if err := mf.PublishLatest(sid, 0); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	// 3) Prepare changelog with 3 lines that are duplicates of what's already in snapshot state
	clDir := filepath.Join(base, "changelog")
	if err := os.MkdirAll(clDir, 0o755); err != nil {
		t.Fatalf("mkdir changelog: %v", err)
	}
	clPath := filepath.Join(clDir, "opb.jsonl")
	f, err := os.Create(clPath)
	if err != nil {
		t.Fatalf("create changelog: %v", err)
	}
	// Same as the sample run: two lines for A#p1 (seq 1,2) and one for A#p2 (seq 1)
	_, _ = f.WriteString(`{"key":"A#p1#1694499900","seq":1,"delta":10000,"deltaQty":1,"ts":1}` + "\n")
	_, _ = f.WriteString(`{"key":"A#p1#1694499900","seq":2,"delta":20000,"deltaQty":2,"ts":2}` + "\n")
	_, _ = f.WriteString(`{"key":"A#p2#1694499900","seq":1,"delta":15000,"deltaQty":3,"ts":3}` + "\n")
	_ = f.Close()

	// Run restore+replay
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res, err := r.RestoreAndReplay()
	if err != nil {
		t.Fatalf("RestoreAndReplay error: %v", err)
	}
	if res.Applied != 0 || res.Skipped != 3 {
		t.Fatalf("want applied=0 skipped=3, got %+v", res)
	}

	// Validate that state remains the same as snapshot (no double-apply)
	rs, ok := st.Get("A#p1#1694499900")
	if !ok || rs.SumAmount != 30000 || rs.SumQty != 3 || rs.LastSeq != 2 {
		t.Fatalf("unexpected A#p1 state: %+v", rs)
	}
	rs2, ok := st.Get("A#p2#1694499900")
	if !ok || rs2.SumAmount != 15000 || rs2.SumQty != 3 || rs2.LastSeq != 1 {
		t.Fatalf("unexpected A#p2 state: %+v", rs2)
	}
}
