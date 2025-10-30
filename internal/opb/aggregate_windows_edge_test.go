package opb

import (
	"hpb/internal/state"
	"testing"
)

func TestAggregate_WindowEdgeBoundaries(t *testing.T) {
	st := state.NewInMemoryStore()
	w := 300
	// Event at t = w-1 goes to window 0, event at t = w goes to window w
	e1 := OrderEnriched{OrderID: "o1", ProductID: "p1", Price: 10, Qty: 1, StoreID: "S", NormTS: int64(w - 1)}
	e2 := OrderEnriched{OrderID: "o2", ProductID: "p1", Price: 20, Qty: 2, StoreID: "S", NormTS: int64(w)}

	a1, o1, s1, err := AggregateAndBuildOutput(st, w, e1)
	if err != nil || !a1 || s1 != 1 {
		t.Fatalf("first apply failed: err=%v a1=%v s1=%d", err, a1, s1)
	}
	a2, o2, s2, err := AggregateAndBuildOutput(st, w, e2)
	if err != nil || !a2 || s2 != 1 {
		t.Fatalf("second apply failed: err=%v a2=%v s2=%d", err, a2, s2)
	}
	if o1.Key == o2.Key {
		t.Fatalf("events across boundary must yield different keys: %s vs %s", o1.Key, o2.Key)
	}
}
