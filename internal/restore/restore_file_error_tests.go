package restore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

// manifest with missing fields -> snapshotID empty, offset default 0, changelog missing -> open error
func TestRestoreAndReplay_ManifestMissingFields_ChangelogMissing(t *testing.T) {
	base := t.TempDir()
	bad := map[string]any{"createdAt": 123}
	b, _ := json.Marshal(bad)
	if err := os.WriteFile(filepath.Join(base, "manifest.latest.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	if _, err := r.RestoreAndReplay(); err == nil {
		t.Fatalf("expected error due to missing changelog file")
	}
}

func TestRestoreFromSnapshot_SnapshotMissing_ThenReplayEmptyChangelog(t *testing.T) {
	base := t.TempDir()
	// manifest points to non-existent snapshot id
	mf := manifest.NewFilesystemManifest(base)
	if err := mf.PublishLatest("sid-missing", 0); err != nil {
		t.Fatal(err)
	}
	// create empty changelog to allow replay
	if err := os.WriteFile(filepath.Join(base, "changelog", "opb.jsonl"), []byte(""), 0o644); err != nil {
		if err := os.MkdirAll(filepath.Join(base, "changelog"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(base, "changelog", "opb.jsonl"), []byte(""), 0o644); err != nil {
			t.Fatal(err)
		}
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

func TestReplayChangelog_MalformedAfterGoodLine(t *testing.T) {
	base := t.TempDir()
	cl := filepath.Join(base, "changelog", "opb.jsonl")
	if err := os.MkdirAll(filepath.Dir(cl), 0o755); err != nil {
		t.Fatal(err)
	}
	content := "{\"key\":\"K#1\",\"seq\":1,\"delta\":1}\n{bad json}\n"
	if err := os.WriteFile(cl, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res := r.ReplayChangelog(cl, 0)
	if res.Error == nil {
		t.Fatalf("expected error for malformed JSONL")
	}
	if res.Applied != 0 && res.Applied != 1 {
		t.Fatalf("applied should be 0 or 1, got %d", res.Applied)
	}
}
