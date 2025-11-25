package state

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

// PebbleStore implements Store using PebbleDB.
type PebbleStore struct {
	db         *pebble.DB
	instanceID string
	// In-memory set for dirty keys since the last snapshot.
	// This is simpler than using a separate Pebble key-space for this transient data.
	dirtyMu sync.Mutex
	dirty   map[string]struct{}
}

type pebbleSnapshotView struct {
	snap *pebble.Snapshot
}

func (v *pebbleSnapshotView) Range(fn func(key string, st RecordState) error) error {
	it, _ := v.snap.NewIter(nil)
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		k := append([]byte(nil), it.Key()...)
		vbytes := append([]byte(nil), it.Value()...)
		st, err := decodePebbleState(vbytes)
		if err != nil {
			return err
		}
		if err := fn(string(k), st); err != nil {
			return err
		}
	}
	return nil
}

func (v *pebbleSnapshotView) Close() error { return v.snap.Close() }

func NewPebbleStore(dir string) (*PebbleStore, error) {
	opts := &pebble.Options{
		// Optimized for high throughput
		MemTableSize:             256 << 20,               // 256MB (4x larger)
		MaxConcurrentCompactions: func() int { return 4 }, // More parallel compactions
		L0CompactionThreshold:    4,                       // Start compaction earlier
		L0StopWritesThreshold:    8,                       // Allow more writes before stopping
		WALBytesPerSync:          1 << 20,                 // 1MB WAL sync (vs default 512KB)
		// Disable WAL sync for better performance (trade-off: durability)
		DisableWAL:         false,                             // Keep WAL for durability
		WALMinSyncInterval: func() time.Duration { return 0 }, // No minimum sync interval
	}
	d, err := pebble.Open(filepath.Clean(dir), opts)
	if err != nil {
		return nil, fmt.Errorf("pebble open: %w", err)
	}
	return &PebbleStore{db: d, dirty: make(map[string]struct{})}, nil
}

// SetInstanceID sets the instance id used for LastUpdatedBy (transient only).
func (p *PebbleStore) SetInstanceID(id string) { p.instanceID = id }

func (p *PebbleStore) Close() error { return p.db.Close() }

// NewSnapshotView creates a consistent read-only snapshot using Pebble's snapshot API.
func (p *PebbleStore) NewSnapshotView() (SnapshotView, error) {
	s := p.db.NewSnapshot()
	return &pebbleSnapshotView{snap: s}, nil
}

func encodePebbleState(st RecordState) ([]byte, error) { return json.Marshal(st) }
func decodePebbleState(val []byte) (RecordState, error) {
	var st RecordState
	if err := json.Unmarshal(val, &st); err != nil {
		return RecordState{}, err
	}
	return st, nil
}

func (p *PebbleStore) Apply(key string, deltaAmount int64, deltaQty int64, seq int64) (bool, RecordState, error) {
	k := []byte(key)
	// Read current
	var cur RecordState
	v, closer, err := p.db.Get(k)
	if err == nil {
		cur, err = decodePebbleState(v)
		_ = closer.Close()
		if err != nil {
			return false, RecordState{}, err
		}
	} else if err != pebble.ErrNotFound {
		return false, RecordState{}, err
	}
	// Idempotency / ordering
	if seq <= cur.LastSeq {
		return false, cur, nil
	}
	// Allow gap similar to InMemory/Badger
	cur.SumAmount += deltaAmount
	cur.SumQty += deltaQty
	cur.LastSeq = seq
	// Set transient field for the returned state (not persisted)
	cur.LastUpdatedBy = p.instanceID
	bytes, err := encodePebbleState(cur)
	if err != nil {
		return false, RecordState{}, err
	}
	// Use NoSync for better performance (WAL will handle durability)
	if err := p.db.Set(k, bytes, pebble.NoSync); err != nil {
		return false, RecordState{}, err
	}
	p.dirtyMu.Lock()
	p.dirty[key] = struct{}{}
	p.dirtyMu.Unlock()
	return true, cur, nil
}

// ApplyBatch applies a batch of deltas grouped by key using a Pebble write batch.
func (p *PebbleStore) ApplyBatch(batch []Delta) (int, int, error) {
	if len(batch) == 0 {
		return 0, 0, nil
	}
	// Group deltas by key to minimize reads/writes
	groups := make(map[string][]Delta)
	for _, d := range batch {
		groups[d.Key] = append(groups[d.Key], d)
	}
	wb := p.db.NewBatch()
	applied, skipped := 0, 0
	newlyDirty := make([]string, 0, len(groups))
	for key, ds := range groups {
		k := []byte(key)
		// Read current state (if any)
		var cur RecordState
		v, closer, err := p.db.Get(k)
		if err == nil {
			cur, err = decodePebbleState(v)
			_ = closer.Close()
			if err != nil {
				_ = wb.Close()
				return applied, skipped, err
			}
		} else if err != pebble.ErrNotFound {
			_ = wb.Close()
			return applied, skipped, err
		}
		// Apply in-order
		anyApplied := false
		for _, d := range ds {
			if d.Seq <= cur.LastSeq {
				skipped++
				continue
			}
			cur.SumAmount += d.DeltaAmount
			cur.SumQty += d.DeltaQty
			cur.LastSeq = d.Seq
			cur.LastUpdatedBy = p.instanceID // transient
			applied++
			anyApplied = true
		}
		// Write only if this key had at least one applied
		if anyApplied {
			bytes, err := encodePebbleState(cur)
			if err != nil {
				_ = wb.Close()
				return applied, skipped, err
			}
			if err := wb.Set(k, bytes, nil); err != nil {
				_ = wb.Close()
				return applied, skipped, err
			}
			newlyDirty = append(newlyDirty, key)
		}
	}
	if err := wb.Commit(pebble.NoSync); err != nil {
		_ = wb.Close()
		return applied, skipped, err
	}
	_ = wb.Close()
	if len(newlyDirty) > 0 {
		p.dirtyMu.Lock()
		for _, key := range newlyDirty {
			p.dirty[key] = struct{}{}
		}
		p.dirtyMu.Unlock()
	}
	return applied, skipped, nil
}

func (p *PebbleStore) Get(key string) (RecordState, bool) {
	v, closer, err := p.db.Get([]byte(key))
	if err != nil {
		return RecordState{}, false
	}
	defer closer.Close()
	st, e := decodePebbleState(v)
	if e != nil {
		return RecordState{}, false
	}
	return st, true
}

func (p *PebbleStore) Range(fn func(key string, st RecordState) error) error {
	it, _ := p.db.NewIter(nil)
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		k := append([]byte(nil), it.Key()...)
		v := append([]byte(nil), it.Value()...)
		st, err := decodePebbleState(v)
		if err != nil {
			return err
		}
		if err := fn(string(k), st); err != nil {
			return err
		}
	}
	return nil
}

// LoadAll loads a full snapshot into Pebble by replacing all keys.
func (p *PebbleStore) LoadAll(all map[string]RecordState) {
	// Collect existing keys first, then delete, then write snapshot.
	var toDelete [][]byte
	it, _ := p.db.NewIter(nil)
	for it.First(); it.Valid(); it.Next() {
		k := append([]byte(nil), it.Key()...)
		toDelete = append(toDelete, k)
	}
	it.Close()
	if len(toDelete) > 0 {
		wb := p.db.NewBatch()
		for _, k := range toDelete {
			_ = wb.Delete(k, nil)
		}
		_ = wb.Commit(pebble.NoSync)
		_ = wb.Close()
	}
	if len(all) > 0 {
		wb := p.db.NewBatch()
		for k, st := range all {
			bytes, err := encodePebbleState(st)
			if err != nil {
				continue
			}
			_ = wb.Set([]byte(k), bytes, nil)
		}
		_ = wb.Commit(pebble.NoSync)
		_ = wb.Close()
	}
	p.dirtyMu.Lock()
	p.dirty = make(map[string]struct{}) // Reset dirty map after loading
	p.dirtyMu.Unlock()
}

func (p *PebbleStore) Delete(key string) error {
	p.dirtyMu.Lock()
	p.dirty[key] = struct{}{}
	p.dirtyMu.Unlock()
	return p.db.Delete([]byte(key), pebble.NoSync)
}

// GetDirtyKeys returns a slice of keys that have been modified since the last snapshot.
func (p *PebbleStore) GetDirtyKeys() []string {
	p.dirtyMu.Lock()
	defer p.dirtyMu.Unlock()
	keys := make([]string, 0, len(p.dirty))
	for k := range p.dirty {
		keys = append(keys, k)
	}
	return keys
}

// MarkSnapshotDone clears the dirty key tracking map.
func (p *PebbleStore) MarkSnapshotDone(keys ...string) {
	p.dirtyMu.Lock()
	defer p.dirtyMu.Unlock()
	if len(keys) == 0 {
	p.dirty = make(map[string]struct{}) // Reset dirty map
		return
	}
	for _, k := range keys {
		delete(p.dirty, k)
	}
}
