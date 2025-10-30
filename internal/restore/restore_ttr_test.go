package restore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

// This test measures a coarse-grained TTR (time-to-recover) for file-based restore+replay.
// It ensures that for a moderate changelog size, recovery completes quickly and correctly.
func TestRestoreReplay_TTR_Coarse(t *testing.T) {
	base := t.TempDir()
	oldWD, _ := os.Getwd()
	if err := os.Chdir(base); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer func() { _ = os.Chdir(oldWD) }()

	// Snapshot with one key
	sid := "sid-ttr-001"
	snapDir := filepath.Join(base, sid)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("mkdir snapshot: %v", err)
	}
	dump := map[string]state.RecordState{
		"TTR#k": {SumAmount: 0, SumQty: 0, LastSeq: 0},
	}
	b, _ := json.Marshal(dump)
	if err := os.WriteFile(filepath.Join(snapDir, "state.json"), b, 0o644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	// Manifest from offset 0
	mf := manifest.NewFilesystemManifest(base)
	if err := mf.PublishLatest(sid, 0); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	// Changelog with N lines
	if err := os.MkdirAll("changelog", 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join("changelog", "opb.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	const N = 1000
	for i := 1; i <= N; i++ {
		// strictly increasing seq to ensure all applied
		_, _ = f.WriteString(fmt.Sprintf(`{"key":"TTR#k","seq":%d,"delta":1,"deltaQty":1,"ts":%d}`+"\n", i, i))
	}
	_ = f.Close()

	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)

	t0 := time.Now()
	res, err := r.RestoreAndReplay()
	dur := time.Since(t0)
	if err != nil {
		t.Fatalf("restore+replay error: %v", err)
	}
	if res.Applied != N || res.Skipped != 0 {
		t.Fatalf("want applied=%d skipped=0, got %+v", N, res)
	}
	// Coarse SLA: should be fast (≤ 2s) for N=1000 lines on local FS
	if dur > 2*time.Second {
		t.Fatalf("recovery too slow: %v (N=%d)", dur, N)
	}
}
