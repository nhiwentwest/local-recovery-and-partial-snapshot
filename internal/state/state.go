package state

import (
	"fmt"
	"sync"
)

// RecordState represents aggregated state per key.
type RecordState struct {
	SumAmount int64
	SumQty    int64
	LastSeq   int64
}

// Store abstracts the state backend.
// Note: For Phase 1, only InMemoryStore is implemented.
type Store interface {
	Apply(key string, deltaAmount int64, deltaQty int64, seq int64) (applied bool, newState RecordState, err error)
	Get(key string) (RecordState, bool)
	Range(fn func(key string, st RecordState) error) error
	LoadAll(all map[string]RecordState)
}

// InMemoryStore is a simple thread-safe map store.
type InMemoryStore struct {
	mu   sync.RWMutex
	data map[string]RecordState
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{data: make(map[string]RecordState)}
}

// LoadAll replaces the store contents with the provided snapshot (used by restore in Phase 1).
func (s *InMemoryStore) LoadAll(all map[string]RecordState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]RecordState, len(all))
	for k, v := range all {
		s.data[k] = v
	}
}

func (s *InMemoryStore) Apply(key string, deltaAmount int64, deltaQty int64, seq int64) (bool, RecordState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.data[key]
	if seq <= st.LastSeq {
		return false, st, nil
	}
	if seq > st.LastSeq+1 {
		// For Phase 1, allow gap but note it.
		// In later phases, we may enforce ordering.
	}
	st.SumAmount += deltaAmount
	st.SumQty += deltaQty
	st.LastSeq = seq
	s.data[key] = st
	return true, st, nil
}

func (s *InMemoryStore) Get(key string) (RecordState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.data[key]
	return st, ok
}

func (s *InMemoryStore) Range(fn func(key string, st RecordState) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range s.data {
		if err := fn(k, v); err != nil {
			return fmt.Errorf("range callback failed: %w", err)
		}
	}
	return nil
}
