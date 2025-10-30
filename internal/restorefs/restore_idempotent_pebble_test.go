package restorefs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

type testPebbleStore struct{ db *pebble.DB }

func newTestPebbleStore() *testPebbleStore {
	db, err := pebble.Open("/pebble", &pebble.Options{FS: vfs.NewMem(), MemTableSize: 8 << 20, MaxConcurrentCompactions: func() int { return 1 }, L0CompactionThreshold: 2, L0StopWritesThreshold: 4, WALBytesPerSync: 1 << 20, DisableWAL: false, WALMinSyncInterval: func() time.Duration { return 0 }})
	if err != nil {
		panic(err)
	}
	return &testPebbleStore{db: db}
}

func (p *testPebbleStore) Close() error { return p.db.Close() }
func (p *testPebbleStore) Apply(key string, da, dq, seq int64) (bool, state.RecordState, error) {
	k := []byte(key)
	var cur state.RecordState
	v, closer, err := p.db.Get(k)
	if err == nil {
		_ = json.Unmarshal(v, &cur)
		_ = closer.Close()
	} else if err != pebble.ErrNotFound {
		return false, state.RecordState{}, err
	}
	if seq <= cur.LastSeq {
		return false, cur, nil
	}
	cur.SumAmount += da
	cur.SumQty += dq
	cur.LastSeq = seq
	b, _ := json.Marshal(cur)
	if err := p.db.Set(k, b, pebble.NoSync); err != nil {
		return false, state.RecordState{}, err
	}
	return true, cur, nil
}
func (p *testPebbleStore) Get(key string) (state.RecordState, bool) {
	v, closer, err := p.db.Get([]byte(key))
	if err != nil {
		return state.RecordState{}, false
	}
	defer closer.Close()
	var st state.RecordState
	_ = json.Unmarshal(v, &st)
	return st, true
}
func (p *testPebbleStore) Range(fn func(key string, st state.RecordState) error) error {
	it, _ := p.db.NewIter(nil)
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		k := append([]byte(nil), it.Key()...)
		v := append([]byte(nil), it.Value()...)
		var st state.RecordState
		if err := json.Unmarshal(v, &st); err != nil {
			return err
		}
		if err := fn(string(k), st); err != nil {
			return err
		}
	}
	return nil
}
func (p *testPebbleStore) LoadAll(all map[string]state.RecordState) {
	var del [][]byte
	it, _ := p.db.NewIter(nil)
	for it.First(); it.Valid(); it.Next() {
		del = append(del, append([]byte(nil), it.Key()...))
	}
	it.Close()
	if len(del) > 0 {
		wb := p.db.NewBatch()
		for _, k := range del {
			_ = wb.Delete(k, nil)
		}
		_ = wb.Commit(pebble.NoSync)
		_ = wb.Close()
	}
	if len(all) > 0 {
		wb := p.db.NewBatch()
		for k, st := range all {
			b, _ := json.Marshal(st)
			_ = wb.Set([]byte(k), b, nil)
		}
		_ = wb.Commit(pebble.NoSync)
		_ = wb.Close()
	}
}

func TestEndToEnd_PebbleMem_SnapshotThenRestoreReplay_Idempotent_FS(t *testing.T) {
	base := t.TempDir()
	oldWD, _ := os.Getwd()
	if err := os.Chdir(base); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	defer func() { _ = os.Chdir(oldWD) }()
	sid := "sid-e2e-pebble"
	snapDir := filepath.Join(base, sid)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("mkdir snapshot: %v", err)
	}
	dump := map[string]state.RecordState{"A#p1#1694499900": {SumAmount: 30000, SumQty: 3, LastSeq: 2}, "A#p2#1694499900": {SumAmount: 15000, SumQty: 3, LastSeq: 1}}
	b, _ := json.Marshal(dump)
	if err := os.WriteFile(filepath.Join(snapDir, "state.json"), b, 0o644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	mf := manifest.NewFilesystemManifest(base)
	if err := mf.PublishLatest(sid, 0); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(base, "changelog"), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(base, "changelog", "opb.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(`{"key":"A#p1#1694499900","seq":1,"delta":10000,"deltaQty":1,"ts":1}` + "\n")
	_, _ = f.WriteString(`{"key":"A#p1#1694499900","seq":2,"delta":20000,"deltaQty":2,"ts":2}` + "\n")
	_, _ = f.WriteString(`{"key":"A#p2#1694499900","seq":1,"delta":15000,"deltaQty":3,"ts":3}` + "\n")
	_ = f.Close()
	st := newTestPebbleStore()
	defer st.Close()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(base), base)
	res, err := r.RestoreAndReplay()
	if err != nil {
		t.Fatalf("RestoreAndReplay error: %v", err)
	}
	if res.Applied != 0 || res.Skipped != 3 {
		t.Fatalf("want applied=0 skipped=3, got %+v", res)
	}
}
