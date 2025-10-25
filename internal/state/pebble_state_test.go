package state

import (
	"testing"
)

func TestPebbleStore_ApplySeqRulesAndGet(t *testing.T) {
	dir := t.TempDir()
	st, err := NewPebbleStore(dir)
	if err != nil {
		t.Fatalf("pebble open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	applied, s, err := st.Apply("k", 10, 1, 1)
	if err != nil {
		t.Fatalf("apply err: %v", err)
	}
	if !applied || s.LastSeq != 1 || s.SumAmount != 10 || s.SumQty != 1 {
		t.Fatalf("unexpected after first apply: %+v applied=%v", s, applied)
	}

	// same seq => idempotent skip
	applied, s, err = st.Apply("k", 20, 2, 1)
	if err != nil {
		t.Fatalf("apply err: %v", err)
	}
	if applied || s.LastSeq != 1 || s.SumAmount != 10 || s.SumQty != 1 {
		t.Fatalf("should skip same-seq; got %+v applied=%v", s, applied)
	}

	// gap allowed: seq=3
	applied, s, err = st.Apply("k", 30, 3, 3)
	if err != nil {
		t.Fatalf("apply err: %v", err)
	}
	if !applied || s.LastSeq != 3 || s.SumAmount != 40 || s.SumQty != 4 {
		t.Fatalf("unexpected after gap: %+v applied=%v", s, applied)
	}

	// Get should reflect latest
	got, ok := st.Get("k")
	if !ok {
		t.Fatalf("missing key")
	}
	if got != s {
		t.Fatalf("get mismatch: %v vs %v", got, s)
	}
}

func TestPebbleStore_LoadAllAndRange(t *testing.T) {
	dir := t.TempDir()
	st, err := NewPebbleStore(dir)
	if err != nil {
		t.Fatalf("pebble open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	dump := map[string]RecordState{
		"A#p1#100": {SumAmount: 100, SumQty: 1, LastSeq: 1},
		"B#p2#200": {SumAmount: 50, SumQty: 2, LastSeq: 2},
	}
	st.LoadAll(dump)

	// Validate via Get
	if s, ok := st.Get("A#p1#100"); !ok || s.SumAmount != 100 || s.SumQty != 1 || s.LastSeq != 1 {
		t.Fatalf("bad A: %+v ok=%v", s, ok)
	}
	if s, ok := st.Get("B#p2#200"); !ok || s.SumAmount != 50 || s.SumQty != 2 || s.LastSeq != 2 {
		t.Fatalf("bad B: %+v ok=%v", s, ok)
	}

	// Range should visit both keys without error
	count := 0
	if err := st.Range(func(key string, rs RecordState) error { count++; return nil }); err != nil {
		t.Fatalf("range err: %v", err)
	}
	if count != 2 {
		t.Fatalf("range count=%d want=2", count)
	}
}
