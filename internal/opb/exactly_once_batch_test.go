package opb

import (
	"hpb/internal/state"
	"testing"
)

// This test simulates an EOS batch by applying a series of monotonic seq deltas,
// then replays the same deltas: state must not increase on replay.
func TestExactlyOnceBatch_ReplayNoIncrement(t *testing.T) {
	st := state.NewInMemoryStore()
	w := 300
	key := OutputKey("S", "p1", WindowStart(1000, w))

	// First pass: seq 1..5
	var totalAmt int64
	var totalQty int64
	for i := int64(1); i <= 5; i++ {
		applied, cur, err := st.Apply(key, 10*i, 1, i)
		if err != nil || !applied {
			t.Fatalf("first pass apply failed at seq=%d err=%v applied=%v", i, err, applied)
		}
		totalAmt += 10 * i
		totalQty += 1
		if cur.SumAmount != totalAmt || cur.SumQty != totalQty || cur.LastSeq != i {
			t.Fatalf("unexpected state after seq=%d: %+v", i, cur)
		}
	}

	// Replay same batch: should be skipped (seqs <= LastSeq)
	for i := int64(1); i <= 5; i++ {
		applied, cur, err := st.Apply(key, 10*i, 1, i)
		if err != nil {
			t.Fatalf("replay apply err at seq=%d: %v", i, err)
		}
		if applied {
			t.Fatalf("replay should not apply at seq=%d", i)
		}
		if cur.SumAmount != totalAmt || cur.SumQty != totalQty || cur.LastSeq != 5 {
			t.Fatalf("state changed on replay: %+v", cur)
		}
	}
}
