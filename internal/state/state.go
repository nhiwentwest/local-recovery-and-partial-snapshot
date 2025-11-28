package state

import (
	"fmt"
	"sync"
)

// RecordState represents aggregated state per key.
type RecordState struct {
	SumAmount     int64
	SumQty        int64
	LastSeq       int64
	LastUpdatedBy string `json:"-"`
}

// Delta is a batched state change for a single key.
type Delta struct {
	Key         string
	DeltaAmount int64
	DeltaQty    int64
	Seq         int64
}

// SnapshotView is a read-only, point-in-time view over the store.
// Implementations must provide a consistent snapshot that is unaffected by
// concurrent writes after the view is created.
type SnapshotView interface {
	// Range iterates over all key/value pairs in the snapshot view.
	Range(fn func(key string, st RecordState) error) error
	// Close releases resources held by the snapshot view.
	Close() error
}

// Store abstracts the state backend.
// Note: For Phase 1, only InMemoryStore is implemented.
type Store interface {
	Apply(key string, deltaAmount int64, deltaQty int64, seq int64) (applied bool, newState RecordState, err error)
	// ApplyBatch applies a batch of deltas atomically from the perspective of external readers.
	// Implementations may choose their own internal locking/transaction semantics.
	// The method should process deltas in-order and return counts of applied vs skipped (by seq).
	ApplyBatch(batch []Delta) (applied int, skipped int, err error)
	Get(key string) (RecordState, bool)
	Range(fn func(key string, st RecordState) error) error
	LoadAll(all map[string]RecordState)
	Delete(key string) error
	// NewSnapshotView returns a consistent, read-only view for iteration without
	// blocking writers. Callers must Close() the returned view.
	NewSnapshotView() (SnapshotView, error)
	// Dirty key tracking for incremental snapshots
	GetDirtyKeys() []string
	// MarkSnapshotDone clears dirty tracking. If keys are provided, only those keys are cleared; if none provided, clears all.
	MarkSnapshotDone(keys ...string)
}

// CheckpointCapable is implemented by stores that can export/import their on-disk
// representation (e.g., Pebble SSTables) for fast snapshot shipping. This is
// optional and only used when snapshot format is set to "pebble".
type CheckpointCapable interface {
	// ExportSSTables writes a consistent set of SSTable files representing the
	// current state into dstDir. It returns the relative file names and a
	// backend-specific format version for manifest bookkeeping.
	ExportSSTables(dstDir string) (files []string, formatVersion string, err error)
	// ImportSSTables replaces the current store contents with the SSTables found
	// in srcDir. Implementations should ensure atomicity from the caller's POV.
	ImportSSTables(srcDir string) error
}

// DeltaCheckpointCapable extends CheckpointCapable with delta SSTable export/ingest.
// Phase 2: instead of exporting the entire DB, export only dirty keys as a delta SSTable.
type DeltaCheckpointCapable interface {
	CheckpointCapable
	// ExportDeltaSSTables exports only the specified dirty keys as a new SSTable.
	ExportDeltaSSTables(dstDir string, dirtyKeys []string) (files []string, formatVersion string, err error)
	// IngestDeltaSSTables ingests delta SSTables into the existing DB without full replacement.
	IngestDeltaSSTables(srcDir string, files []string) error
}

// IncrementalCheckpointCapable extends DeltaCheckpointCapable with incremental file-level export.
// Phase 3: export only new SSTable files created since the last checkpoint.
type IncrementalCheckpointCapable interface {
	DeltaCheckpointCapable
	// ExportIncrementalSSTables exports only new SSTable files since the last checkpoint.
	// Returns (newFiles, allFiles, formatVersion, error).
	ExportIncrementalSSTables(dstDir string) (newFiles []string, allFiles []string, formatVersion string, err error)
	// IngestIncrementalFiles ingests incremental SSTable files into the existing DB.
	IngestIncrementalFiles(srcDir string, files []string) error
}

// PartialLoader is implemented by stores that can apply a subset of records
// without replacing the entire state (used for delta restore).
type PartialLoader interface {
	LoadPartial(partial map[string]RecordState)
}

// InMemoryStore is a simple thread-safe map store.
type InMemoryStore struct {
	mu         sync.RWMutex
	data       map[string]RecordState
	dirty      map[string]struct{}
	instanceID string
}

type memSnapshotView struct {
	data map[string]RecordState
}

func (v *memSnapshotView) Range(fn func(key string, st RecordState) error) error {
	for k, st := range v.data {
		if err := fn(k, st); err != nil {
			return fmt.Errorf("range callback failed: %w", err)
		}
	}
	return nil
}

func (v *memSnapshotView) Close() error { return nil }

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		data:  make(map[string]RecordState),
		dirty: make(map[string]struct{}),
	}
}

// SetInstanceID sets the instance id used for LastUpdatedBy (transient only).
func (s *InMemoryStore) SetInstanceID(id string) { s.instanceID = id }

// LoadAll replaces the store contents with the provided snapshot and resets the dirty map.
func (s *InMemoryStore) LoadAll(all map[string]RecordState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]RecordState, len(all))
	for k, v := range all {
		s.data[k] = v
	}
	s.dirty = make(map[string]struct{}) // Reset dirty map after loading
}

// LoadPartial applies the provided state without clearing the entire store.
func (s *InMemoryStore) LoadPartial(partial map[string]RecordState) {
	if len(partial) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range partial {
		s.data[k] = v
		delete(s.dirty, k)
	}
}

func (s *InMemoryStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; ok {
		delete(s.data, key)
		s.dirty[key] = struct{}{}
	}
	return nil
}

func (s *InMemoryStore) Apply(key string, deltaAmount int64, deltaQty int64, seq int64) (bool, RecordState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.data[key]
	if seq <= st.LastSeq {
		return false, st, nil
	}
	if seq > st.LastSeq+1 {
		// Allow gap but note: later phases may enforce ordering.
	}
	st.SumAmount += deltaAmount
	st.SumQty += deltaQty
	st.LastSeq = seq
	st.LastUpdatedBy = s.instanceID
	s.data[key] = st
	s.dirty[key] = struct{}{}
	return true, st, nil
}

// ApplyBatch applies deltas sequentially under a single lock for efficiency.
func (s *InMemoryStore) ApplyBatch(batch []Delta) (int, int, error) {
	if len(batch) == 0 {
		return 0, 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	applied, skipped := 0, 0
	for _, d := range batch {
		st := s.data[d.Key]
		if d.Seq <= st.LastSeq {
			skipped++
			continue
		}
		st.SumAmount += d.DeltaAmount
		st.SumQty += d.DeltaQty
		st.LastSeq = d.Seq
		st.LastUpdatedBy = s.instanceID
		s.data[d.Key] = st
		s.dirty[d.Key] = struct{}{}
		applied++
	}
	return applied, skipped, nil
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

// NewSnapshotView returns a stable copy of the current map for iteration.
func (s *InMemoryStore) NewSnapshotView() (SnapshotView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copyMap := make(map[string]RecordState, len(s.data))
	for k, v := range s.data {
		copyMap[k] = v
	}
	return &memSnapshotView{data: copyMap}, nil
}

// GetDirtyKeys returns a slice of keys that have been modified since the last snapshot.
func (s *InMemoryStore) GetDirtyKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.dirty))
	for k := range s.dirty {
		keys = append(keys, k)
	}
	return keys
}

// MarkSnapshotDone clears the dirty key tracking map.
func (s *InMemoryStore) MarkSnapshotDone(keys ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(keys) == 0 {
		s.dirty = make(map[string]struct{}) // Reset dirty map
		return
	}
	for _, k := range keys {
		delete(s.dirty, k)
	}
}
