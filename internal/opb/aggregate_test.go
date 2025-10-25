package opb

import (
	"hpb/internal/state"
	"testing"
)

func TestAggregate_AccumulateAndIdempotent(t *testing.T) {
	st := state.NewInMemoryStore()
	old := NowUnix
	defer func() { NowUnix = old }()
	NowUnix = func() int64 { return 111 }

	ord1 := OrderEnriched{OrderID: "o1", ProductID: "p1", Price: 1000, Qty: 2, StoreID: "A", NormTS: 1000}
	applied, out, seq, err := AggregateAndBuildOutput(st, 300, ord1)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !applied || seq != 1 {
		t.Fatalf("first should apply and seq=1; applied=%v seq=%d", applied, seq)
	}
	if out.SumAmount != 2000 || out.SumQty != 2 || out.UpdatedAt != 111 {
		t.Fatalf("unexpected out after first: %+v", out)
	}

	ord2 := OrderEnriched{OrderID: "o2", ProductID: "p1", Price: 500, Qty: 1, StoreID: "A", NormTS: 1001}
	applied, out, seq, err = AggregateAndBuildOutput(st, 300, ord2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !applied || seq != 2 {
		t.Fatalf("second should apply and seq=2; applied=%v seq=%d", applied, seq)
	}
	if out.SumAmount != 2500 || out.SumQty != 3 {
		t.Fatalf("unexpected out after second: %+v", out)
	}
}

func TestAggregate_DifferentWindowsDifferentKeys(t *testing.T) {
	st := state.NewInMemoryStore()
	old := NowUnix
	defer func() { NowUnix = old }()
	NowUnix = func() int64 { return 222 }

	// Same store/product but different windows => different keys/state buckets
	ord1 := OrderEnriched{OrderID: "o1", ProductID: "p1", Price: 100, Qty: 1, StoreID: "A", NormTS: 0}
	ord2 := OrderEnriched{OrderID: "o2", ProductID: "p1", Price: 200, Qty: 2, StoreID: "A", NormTS: 600} // window size 300 => new bucket

	applied, out, seq, err := AggregateAndBuildOutput(st, 300, ord1)
	if err != nil || !applied || seq != 1 {
		t.Fatalf("first apply failed: err=%v applied=%v seq=%d", err, applied, seq)
	}
	key1 := out.Key

	applied, out, seq, err = AggregateAndBuildOutput(st, 300, ord2)
	if err != nil || !applied || seq != 1 {
		t.Fatalf("second bucket first seq failed: err=%v applied=%v seq=%d", err, applied, seq)
	}
	key2 := out.Key
	if key1 == key2 {
		t.Fatalf("keys should differ across windows: %s vs %s", key1, key2)
	}
}
