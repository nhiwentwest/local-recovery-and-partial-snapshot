package restore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

func TestReplayChangelog_FromOffsetAndCounts(t *testing.T) {
	base := t.TempDir()
	cl := filepath.Join(base, "cl.jsonl")
	var b strings.Builder
	for i := 1; i <= 5; i++ {
		// key K#1 with increasing seq
		b.WriteString(fmt.Sprintf(`{"key":"K#1","seq":%d,"delta":1}`+"\n", i))
	}
	if err := os.WriteFile(cl, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res := r.ReplayChangelog(cl, 2)
	if res.Error != nil {
		t.Fatalf("err: %v", res.Error)
	}
	if res.Applied != 3 || res.Skipped != 0 {
		t.Fatalf("want applied=3 skipped=0, got %+v", res)
	}
}

func TestReplayChangelog_ScannerErrTooLong(t *testing.T) {
	base := t.TempDir()
	cl := filepath.Join(base, "cl_long.jsonl")
	// Create a very long JSON line to exceed bufio.Scanner token limit
	longVal := strings.Repeat("A", 70_000)
	content := fmt.Sprintf(`{"key":"K#1","seq":1,"delta":1}`+"\n"+"%s\n", longVal)
	if err := os.WriteFile(cl, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res := r.ReplayChangelog(cl, 0)
	if res.Error == nil {
		t.Fatalf("expected scanner error for too long token")
	}
}
