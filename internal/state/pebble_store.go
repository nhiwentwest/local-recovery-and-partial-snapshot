package state

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/pebble"
)

// PebbleStore implements Store using PebbleDB.
type PebbleStore struct {
	db *pebble.DB
}

func NewPebbleStore(dir string) (*PebbleStore, error) {
	opts := &pebble.Options{
		// Reasonable defaults; can tune later if needed
		MemTableSize: 64 << 20, // 64MB
	}
	d, err := pebble.Open(filepath.Clean(dir), opts)
	if err != nil {
		return nil, fmt.Errorf("pebble open: %w", err)
	}
	return &PebbleStore{db: d}, nil
}

func (p *PebbleStore) Close() error { return p.db.Close() }

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
	bytes, err := encodePebbleState(cur)
	if err != nil {
		return false, RecordState{}, err
	}
	if err := p.db.Set(k, bytes, pebble.Sync); err != nil {
		return false, RecordState{}, err
	}
	return true, cur, nil
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
		_ = wb.Commit(pebble.Sync)
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
		_ = wb.Commit(pebble.Sync)
		_ = wb.Close()
	}
}

