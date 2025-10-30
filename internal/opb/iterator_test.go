package opb

import (
	"strings"
	"testing"

	"hpb/internal/state"
)

func TestIterate_LimitAndFilter(t *testing.T) {
	st := state.NewInMemoryStore()
	// Seed store
	st.LoadAll(map[string]state.RecordState{
		"A#p1#100": {SumAmount: 10, SumQty: 1, LastSeq: 1},
		"B#p2#100": {SumAmount: 20, SumQty: 2, LastSeq: 1},
		"A#p3#100": {SumAmount: 30, SumQty: 3, LastSeq: 2},
		"C#p1#200": {SumAmount: 40, SumQty: 4, LastSeq: 3},
	})

	// Filter only keys with prefix "A#"
	filter := func(k string, _ state.RecordState) bool { return strings.HasPrefix(k, "A#") }
	res, err := Iterate(st, 1, filter)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 entry with limit=1, got %d", len(res))
	}
	if !strings.HasPrefix(res[0].Key, "A#") {
		t.Fatalf("unexpected key: %s", res[0].Key)
	}

	// No limit, same filter
	res, err = Iterate(st, 0, filter)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 entries for prefix A#, got %d", len(res))
	}

	// No filter, limit 3
	res, err = Iterate(st, 3, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("expected 3 entries with limit=3, got %d", len(res))
	}
}
