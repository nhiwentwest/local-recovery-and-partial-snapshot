package restore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

func TestRestoreAndReplay_HonorsManifestOffset(t *testing.T) {
	base := t.TempDir()
	old, _ := os.Getwd()
	if err := os.Chdir(base); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(old) }()
	// Prepare manifest with lastChangelogOffset=2
	mf := manifest.NewFilesystemManifest(base)
	if err := mf.PublishLatest("", 2); err != nil {
		t.Fatal(err)
	}
	// Prepare changelog with 5 deltas (same key, increasing seq)
	if err := os.MkdirAll(filepath.Join(base, "changelog"), 0o755); err != nil {
		t.Fatal(err)
	}
	cl := filepath.Join(base, "changelog", "opb.jsonl")
	f, err := os.Create(cl)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	type d struct {
		Key   string `json:"key"`
		Seq   int64  `json:"seq"`
		Delta int64  `json:"delta"`
	}
	for i := 1; i <= 5; i++ {
		_ = enc.Encode(d{Key: "K#1", Seq: int64(i), Delta: 1})
	}
	_ = f.Close()

	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res, err := r.RestoreAndReplay()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Applied != 3 || res.Skipped != 0 {
		t.Fatalf("want applied=3 skipped=0 from offset=2, got %+v", res)
	}
}
