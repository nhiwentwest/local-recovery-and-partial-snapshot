package state

import (
	"sync"
	"testing"
)

func TestInMemoryStore_ConcurrentAppliesDifferentKeys(t *testing.T) {
	s := NewInMemoryStore()
	var wg sync.WaitGroup
	keys := []string{"A#p1#100", "A#p2#100", "B#p1#200", "C#p3#300"}
	iters := 1000

	for _, k := range keys {
		k := k
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 1; i <= iters; i++ {
				_, _, err := s.Apply(k, 1, 1, int64(i))
				if err != nil {
					t.Errorf("apply err: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	for _, k := range keys {
		st, ok := s.Get(k)
		if !ok {
			t.Fatalf("missing key %s", k)
		}
		if st.SumAmount != int64(iters) || st.SumQty != int64(iters) || st.LastSeq != int64(iters) {
			t.Fatalf("bad state for %s: %+v", k, st)
		}
	}
}
