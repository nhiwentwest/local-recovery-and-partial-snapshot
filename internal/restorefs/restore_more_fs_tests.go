package restorefs

import (
	"bytes"
	"strings"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

// Empty head/tail lines and gaps should be handled strictly (scanner returns empty tokens -> error)
func TestReplayLines_EmptyHeadTailAndGaps(t *testing.T) {
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(t.TempDir()), "")
	// head empty
	content := "\n{" + "\"key\":\"K#1\",\"seq\":1,\"delta\":1}" + "\n"
	res := r.replayLines(bytes.NewBufferString(content), 0)
	if res.Error == nil {
		t.Fatalf("expected error on empty head line")
	}
	// tail empty
	content = `{"key":"K#1","seq":1,"delta":1}` + "\n\n"
	res = r.replayLines(bytes.NewBufferString(content), 0)
	if res.Error == nil {
		t.Fatalf("expected error on empty tail line")
	}
	// gaps with valid lines around (fromOffset skips first two)
	content = strings.Join([]string{
		`{"key":"K#1","seq":1,"delta":1}`,
		`{"key":"K#1","seq":2,"delta":1}`,
		`{"key":"K#2","seq":1,"delta":1}`,
		`{"key":"K#1","seq":4,"delta":1}`,
	}, "\n") + "\n"
	res = r.replayLines(bytes.NewBufferString(content), 2)
	if res.Error != nil || res.Applied != 2 {
		t.Fatalf("want applied=2 from offset 2, got %+v", res)
	}
}
