package snapshot

import (
	"encoding/json"
	"hpb/internal/state"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteSnapshot_WritesStateJSON(t *testing.T) {
	dir := t.TempDir()
	s := state.NewInMemoryStore()
	_, _, _ = s.Apply("A#p1#100", 1500, 3, 1)
	_, _, _ = s.Apply("A#p2#100", 700, 2, 1)

	snap := NewFilesystemSnapshotter(dir, FormatJSON, 1)
	meta, err := snap.WriteSnapshot("sid", s)
	if err != nil {
		t.Fatalf("WriteSnapshot error: %v", err)
	}
	if meta.Shards != 1 || meta.Keys != 2 {
		t.Fatalf("unexpected meta: %+v", meta)
	}

	path := filepath.Join(dir, "sid", "state.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("state.json missing: %v", err)
	}
	var m map[string]state.RecordState
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("bad json: %v", err)
	}
	if len(m) != 2 {
		t.Fatalf("unexpected keys: %v", m)
	}
}

func TestWriteSnapshot_WritesStateMsgpack(t *testing.T) {
	dir := t.TempDir()
	s := state.NewInMemoryStore()
	_, _, _ = s.Apply("B#p1#200", 2100, 5, 1)

	snap := NewFilesystemSnapshotter(dir, FormatMsgpack, 1)
	meta, err := snap.WriteSnapshot("sid", s)
	if err != nil {
		t.Fatalf("WriteSnapshot error: %v", err)
	}
	if meta.Shards != 1 || meta.Keys != 1 {
		t.Fatalf("unexpected meta: %+v", meta)
	}

	path := filepath.Join(dir, "sid", FormatMsgpack.FileName())
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("state.msgpack missing: %v", err)
	}
	m, err := DecodeSnapshot(b, FormatMsgpack)
	if err != nil {
		t.Fatalf("decode msgpack: %v", err)
	}
	if len(m) != 1 {
		t.Fatalf("unexpected keys: %v", m)
	}
}
