package state

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	badger "github.com/dgraph-io/badger/v4"
)

// BadgerStore implements Store using BadgerDB.
type BadgerStore struct {
	db *badger.DB
}

func NewBadgerStore(dir string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(filepath.Clean(dir))
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("badger open: %w", err)
	}
	return &BadgerStore{db: db}, nil
}

func (b *BadgerStore) Close() error { return b.db.Close() }

func encodeState(st RecordState) ([]byte, error) { return json.Marshal(st) }
func decodeState(val []byte) (RecordState, error) {
	var st RecordState
	if err := json.Unmarshal(val, &st); err != nil {
		return RecordState{}, err
	}
	return st, nil
}

func (b *BadgerStore) Apply(key string, deltaAmount int64, deltaQty int64, seq int64) (bool, RecordState, error) {
	var applied bool
	var out RecordState
	err := b.db.Update(func(txn *badger.Txn) error {
		var cur RecordState
		item, err := txn.Get([]byte(key))
		if err == nil {
			v, e := item.ValueCopy(nil)
			if e != nil {
				return e
			}
			cur, e = decodeState(v)
			if e != nil {
				return e
			}
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		if seq <= cur.LastSeq {
			applied = false
			out = cur
			return nil
		}
		if seq > cur.LastSeq+1 {
			// allow gap (same behavior as InMemoryStore)
		}
		cur.SumAmount += deltaAmount
		cur.SumQty += deltaQty
		cur.LastSeq = seq
		bytes, e := encodeState(cur)
		if e != nil {
			return e
		}
		if e = txn.Set([]byte(key), bytes); e != nil {
			return e
		}
		applied = true
		out = cur
		return nil
	})
	return applied, out, err
}

func (b *BadgerStore) Get(key string) (RecordState, bool) {
	var st RecordState
	err := b.db.View(func(txn *badger.Txn) error {
		item, e := txn.Get([]byte(key))
		if e != nil {
			return e
		}
		v, e := item.ValueCopy(nil)
		if e != nil {
			return e
		}
		var dErr error
		st, dErr = decodeState(v)
		return dErr
	})
	if err != nil {
		return RecordState{}, false
	}
	return st, true
}

func (b *BadgerStore) Range(fn func(key string, st RecordState) error) error {
	return b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			st, err := decodeState(v)
			if err != nil {
				return err
			}
			if err := fn(string(k), st); err != nil {
				return err
			}
		}
		return nil
	})
}

// LoadAll loads a full snapshot into Badger by replacing all keys.
func (b *BadgerStore) LoadAll(all map[string]RecordState) {
	_ = b.db.Update(func(txn *badger.Txn) error {
		// Collect keys first to avoid mutating while iterating.
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		var keysToDelete [][]byte
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			keysToDelete = append(keysToDelete, k)
		}
		it.Close()
		for _, k := range keysToDelete {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		for k, st := range all {
			bytes, err := encodeState(st)
			if err != nil {
				return err
			}
			if err := txn.Set([]byte(k), bytes); err != nil {
				return err
			}
		}
		return nil
	})
}
