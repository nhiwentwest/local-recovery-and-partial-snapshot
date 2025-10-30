package restore

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

type errStoreFS struct{ state.Store }

func (e *errStoreFS) Apply(key string, da, dq, seq int64) (bool, state.RecordState, error) {
	if seq > 1 {
		return false, state.RecordState{}, fmt.Errorf("apply failed")
	}
	return e.Store.Apply(key, da, dq, seq)
}

func TestReplayChangelog_ApplyErrorStops(t *testing.T) {
	base := t.TempDir()
	cl := filepath.Join(base, "cl_apply_err.jsonl")
	content := `{"key":"K#1","seq":1,"delta":1}` + "\n" + `{"key":"K#1","seq":2,"delta":1}` + "\n"
	if err := os.WriteFile(cl, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	st := &errStoreFS{Store: state.NewInMemoryStore()}
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res := r.ReplayChangelog(cl, 0)
	if res.Error == nil {
		t.Fatalf("expected apply error, got nil")
	}
}
