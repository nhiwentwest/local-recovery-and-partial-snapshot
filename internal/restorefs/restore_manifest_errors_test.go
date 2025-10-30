package restorefs

import (
	"os"
	"path/filepath"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

func TestRestoreAndReplay_NoManifestFile_FS(t *testing.T) {
	base := t.TempDir()
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	if _, err := r.RestoreAndReplay(); err == nil {
		t.Fatalf("expected error when manifest.latest.json missing")
	}
}

func TestRestoreAndReplay_ManifestMissingOffset_FieldDefault_FS(t *testing.T) {
	base := t.TempDir()
	old, _ := os.Getwd()
	if err := os.Chdir(base); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(old) }()
	data := []byte(`{"snapshotId":"sid-x","createdAt":123}`)
	if err := os.WriteFile(filepath.Join(base, "manifest.latest.json"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(base, "changelog"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "changelog", "opb.jsonl"), []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res, err := r.RestoreAndReplay()
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.Applied != 0 || res.Skipped != 0 {
		t.Fatalf("want 0/0, got %+v", res)
	}
}
