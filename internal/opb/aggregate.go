package opb

import (
	"hpb/internal/state"
)

// AggregateAndBuildOutput applies the order to state and returns an OutputRecord if applied.
// seq is derived from state's LastSeq+1 per key (Phase 1 behavior).
func AggregateAndBuildOutput(st state.Store, windowSizeSec int, ord OrderEnriched) (applied bool, out OutputRecord, seq int64, err error) {
	ws := WindowStart(ord.NormTS, windowSizeSec)
	key := OutputKey(ord.StoreID, ord.ProductID, ws)
	prev, _ := st.Get(key)
	nextSeq := prev.LastSeq + 1
	deltaAmount := ord.Price * ord.Qty
	deltaQty := ord.Qty
	applied, newState, err := st.Apply(key, deltaAmount, deltaQty, nextSeq)
	if err != nil {
		return false, OutputRecord{}, 0, err
	}
	if !applied {
		return false, OutputRecord{}, nextSeq, nil
	}
	return true, OutputRecord{
		Key:         key,
		SumAmount:   newState.SumAmount,
		SumQty:      newState.SumQty,
		WindowStart: ws,
		StoreID:     ord.StoreID,
		ProductID:   ord.ProductID,
		UpdatedAt:   NowUnix(),
	}, nextSeq, nil
}
