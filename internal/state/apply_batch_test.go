package state

import (
	"os"
	"path/filepath"
	"testing"
)

type storeFactory func(t *testing.T) Store

func memStoreFactory(t *testing.T) Store { return NewInMemoryStore() }

func pebbleStoreFactory(t *testing.T) Store {
	dir := t.TempDir()
	ps, err := NewPebbleStore(filepath.Clean(dir))
	if err != nil {
		t.Fatalf("pebble init: %v", err)
	}
	t.Cleanup(func() { _ = ps.Close(); _ = os.RemoveAll(dir) })
	return ps
}

func forEachStore(t *testing.T, fn func(t *testing.T, name string, st Store)) {
	t.Helper()
	cases := []struct {
		name string
		mk   storeFactory
	}{
		{"memory", memStoreFactory},
		{"pebble", pebbleStoreFactory},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			st := c.mk(t)
			fn(t, c.name, st)
		})
	}
}

func TestApplyBatch_MixedKeysAndSeq(t *testing.T) {
	forEachStore(t, func(t *testing.T, _ string, st Store) {
		batch := []Delta{
			{Key: "A#1", DeltaAmount: 5, DeltaQty: 1, Seq: 1}, // apply
			{Key: "B#1", DeltaAmount: 7, DeltaQty: 1, Seq: 1}, // apply
			{Key: "A#1", DeltaAmount: 3, DeltaQty: 2, Seq: 2}, // apply
			{Key: "B#1", DeltaAmount: 2, DeltaQty: 1, Seq: 1}, // dup -> skip
			{Key: "A#1", DeltaAmount: 4, DeltaQty: 1, Seq: 2}, // dup -> skip
			{Key: "B#1", DeltaAmount: 5, DeltaQty: 3, Seq: 2}, // apply
		}
		ap, sk, err := st.ApplyBatch(batch)
		if err != nil {
			t.Fatalf("ApplyBatch error: %v", err)
		}
		if ap != 4 || sk != 2 {
			t.Fatalf("want applied=4 skipped=2, got applied=%d skipped=%d", ap, sk)
		}
		if stA, ok := st.Get("A#1"); !ok || stA.SumAmount != 8 || stA.SumQty != 3 || stA.LastSeq != 2 {
			t.Fatalf("A state mismatch: %+v ok=%v", stA, ok)
		}
		if stB, ok := st.Get("B#1"); !ok || stB.SumAmount != 12 || stB.SumQty != 4 || stB.LastSeq != 2 {
			t.Fatalf("B state mismatch: %+v ok=%v", stB, ok)
		}
	})
}

func TestApplyBatch_AllSkipped(t *testing.T) {
	forEachStore(t, func(t *testing.T, _ string, st Store) {
		// Seed state so that incoming batch seq are all <= lastSeq
		_, _, _ = st.Apply("A#1", 10, 1, 2, SourceUnspecified) // lastSeq=2
		_, _, _ = st.Apply("B#1", 20, 2, 5, SourceUnspecified) // lastSeq=5
		prevA, _ := st.Get("A#1")
		prevB, _ := st.Get("B#1")

		batch := []Delta{
			{Key: "A#1", DeltaAmount: 1, DeltaQty: 1, Seq: 1}, // <=2
			{Key: "A#1", DeltaAmount: 1, DeltaQty: 1, Seq: 2}, // <=2
			{Key: "B#1", DeltaAmount: 1, DeltaQty: 1, Seq: 5}, // <=5
		}
		ap, sk, err := st.ApplyBatch(batch)
		if err != nil {
			t.Fatalf("ApplyBatch error: %v", err)
		}
		if ap != 0 || sk != len(batch) {
			t.Fatalf("want applied=0 skipped=%d, got %d/%d", len(batch), ap, sk)
		}
		curA, _ := st.Get("A#1")
		curB, _ := st.Get("B#1")
		if curA.SumAmount != prevA.SumAmount || curA.SumQty != prevA.SumQty || curA.LastSeq != prevA.LastSeq {
			t.Fatalf("state changed unexpectedly for A: prev=%+v cur=%+v", prevA, curA)
		}
		if curB.SumAmount != prevB.SumAmount || curB.SumQty != prevB.SumQty || curB.LastSeq != prevB.LastSeq {
			t.Fatalf("state changed unexpectedly for B: prev=%+v cur=%+v", prevB, curB)
		}
	})
}
