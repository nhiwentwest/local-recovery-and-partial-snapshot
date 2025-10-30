package restore

import (
	"os"
	"path/filepath"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

func TestRestoreAndReplay_NoManifestFile(t *testing.T) {
	base := t.TempDir()
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	if _, err := r.RestoreAndReplay(); err == nil {
		t.Fatalf("expected error when manifest.latest.json missing")
	}
}

func TestRestoreAndReplay_BadManifestJSON(t *testing.T) {
	base := t.TempDir()
	// write malformed manifest.latest.json
	if err := os.WriteFile(filepath.Join(base, "manifest.latest.json"), []byte("{bad json}"), 0o644); err != nil {
		t.Fatal(err)
	}
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	if _, err := r.RestoreAndReplay(); err == nil {
		t.Fatalf("expected error for malformed manifest JSON")
	}
}

func TestReplayChangelog_OffsetBeyondLength(t *testing.T) {
	base := t.TempDir()
	// empty changelog file
	cl := filepath.Join(base, "empty.jsonl")
	if err := os.WriteFile(cl, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res := r.ReplayChangelog(cl, 1000)
	if res.Error != nil || res.Applied != 0 || res.Skipped != 0 {
		t.Fatalf("unexpected result when offset beyond length: %+v", res)
	}
}
