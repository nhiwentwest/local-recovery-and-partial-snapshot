package opb

import (
	"hpb/internal/state"
	"testing"
)

func TestAggregate_IdempotentSameSeq(t *testing.T) {
	st := state.NewInMemoryStore()
	ord := OrderEnriched{OrderID: "o1", ProductID: "p1", Price: 100, Qty: 1, StoreID: "A", NormTS: 1000}
	applied, _, seq, err := AggregateAndBuildOutput(st, 300, ord)
	if err != nil || !applied || seq != 1 {
		t.Fatalf("first apply failed: err=%v applied=%v seq=%d", err, applied, seq)
	}
	// Re-apply logically same next event but force same seq by poking store
	// AggregateAndBuildOutput increments based on stored LastSeq, so we simulate duplicate by calling Apply directly.
	applied2, st2, err := st.Apply(OutputKey(ord.StoreID, ord.ProductID, WindowStart(ord.NormTS, 300)), ord.Price*ord.Qty, ord.Qty, 1, state.SourceUnspecified)
	if err != nil {
		t.Fatalf("apply duplicate seq err: %v", err)
	}
	if applied2 {
		t.Fatalf("duplicate seq should not apply")
	}
	if st2.LastSeq != 1 || st2.SumAmount != 100 || st2.SumQty != 1 {
		t.Fatalf("state changed on duplicate: %+v", st2)
	}
}
