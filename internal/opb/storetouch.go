package opb

import "sync"

// StoreTouchIndex maintains a cluster-wide map of storeId -> set(instanceId)
// fed by a compacted topic of "touch" events.
type StoreTouchIndex struct {
	mu   sync.RWMutex
	data map[string]map[string]struct{}
}

func NewStoreTouchIndex() *StoreTouchIndex {
	return &StoreTouchIndex{data: make(map[string]map[string]struct{})}
}

func (s *StoreTouchIndex) Add(storeID, instanceID string) {
	s.mu.Lock()
	m := s.data[storeID]
	if m == nil {
		m = make(map[string]struct{})
		s.data[storeID] = m
	}
	m[instanceID] = struct{}{}
	s.mu.Unlock()
}

func (s *StoreTouchIndex) Instances(storeID string) []string {
	s.mu.RLock()
	m := s.data[storeID]
	if m == nil {
		s.mu.RUnlock()
		return nil
	}
	out := make([]string, 0, len(m))
	for inst := range m {
		out = append(out, inst)
	}
	s.mu.RUnlock()
	return out
}
