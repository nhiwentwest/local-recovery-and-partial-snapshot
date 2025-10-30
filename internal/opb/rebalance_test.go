package opb

import (
	"sync"
	"testing"
)

func TestRebalanceTracker_AssignRevoke_Idempotent(t *testing.T) {
	rt := NewRebalanceTracker()
	rt.ApplyAssign([]int32{0, 1, 1})
	rt.ApplyAssign([]int32{2})
	got := rt.Snapshot()
	want := []int32{0, 1, 2}
	if len(got) != len(want) || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Fatalf("unexpected snapshot: %+v", got)
	}
	// revoke idempotent
	rt.ApplyRevoke([]int32{1})
	rt.ApplyRevoke([]int32{1})
	got = rt.Snapshot()
	want = []int32{0, 2}
	if len(got) != len(want) || got[0] != 0 || got[1] != 2 {
		t.Fatalf("unexpected snapshot after revoke: %+v", got)
	}
}

func TestRebalanceTracker_ConcurrentSafety(t *testing.T) {
	rt := NewRebalanceTracker()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(base int32) {
			defer wg.Done()
			for j := int32(0); j < 100; j++ {
				rt.ApplyAssign([]int32{base + j})
				_ = rt.Snapshot()
				rt.ApplyRevoke([]int32{base + j})
			}
		}(int32(i * 1000))
	}
	wg.Wait()
	if n := len(rt.Snapshot()); n != 0 {
		t.Fatalf("expected empty after concurrent assign/revoke, got %d", n)
	}
}
