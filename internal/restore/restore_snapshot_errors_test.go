package restore

import (
	"os"
	"path/filepath"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

func TestRestoreFromSnapshot_JSONMalformed(t *testing.T) {
	base := t.TempDir()
	sid := "sid-bad"
	dir := filepath.Join(base, sid)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// malformed JSON
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte("{bad json}"), 0o644); err != nil {
		t.Fatal(err)
	}
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	if err := r.RestoreFromSnapshot(sid); err == nil {
		t.Fatalf("expected unmarshal error for malformed snapshot JSON")
	}
}

func TestRestoreFromSnapshot_EmptyMap_OK(t *testing.T) {
	base := t.TempDir()
	sid := "sid-empty"
	dir := filepath.Join(base, sid)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// empty map JSON
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	if err := r.RestoreFromSnapshot(sid); err != nil {
		t.Fatalf("unexpected error restoring empty snapshot: %v", err)
	}
	if n := countKeys(st); n != 0 {
		t.Fatalf("expected 0 keys loaded, got %d", n)
	}
}

func countKeys(st state.Store) int {
	c := 0
	_ = st.Range(func(_ string, _ state.RecordState) error { c++; return nil })
	return c
}
