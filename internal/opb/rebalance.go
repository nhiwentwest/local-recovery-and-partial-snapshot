package opb

import (
	"sort"
	"sync"
)

// RebalanceTracker tracks current assigned partitions safely.
type RebalanceTracker struct {
	mu       sync.RWMutex
	assigned map[int32]struct{}
}

func NewRebalanceTracker() *RebalanceTracker {
	return &RebalanceTracker{assigned: make(map[int32]struct{})}
}

// ApplyAssign adds partitions; idempotent and thread-safe.
func (r *RebalanceTracker) ApplyAssign(parts []int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range parts {
		r.assigned[p] = struct{}{}
	}
}

// ApplyRevoke removes partitions; idempotent and thread-safe.
func (r *RebalanceTracker) ApplyRevoke(parts []int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range parts {
		delete(r.assigned, p)
	}
}

// Snapshot returns sorted list of currently assigned partitions.
func (r *RebalanceTracker) Snapshot() []int32 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]int32, 0, len(r.assigned))
	for p := range r.assigned {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
