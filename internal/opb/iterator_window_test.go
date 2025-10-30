package opb

import (
	"strconv"
	"strings"
	"testing"

	"hpb/internal/state"
)

func windowStartFromKey(k string) (int64, bool) {
	parts := strings.Split(k, "#")
	if len(parts) != 3 {
		return 0, false
	}
	ws, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return 0, false
	}
	return ws, true
}

func TestIterate_FilterByWindowStart_Exact(t *testing.T) {
	st := state.NewInMemoryStore()
	st.LoadAll(map[string]state.RecordState{
		"A#p1#100": {SumAmount: 1},
		"A#p1#200": {SumAmount: 2},
		"B#p2#200": {SumAmount: 3},
		"C#p3#300": {SumAmount: 4},
	})
	wantWS := int64(200)
	filter := func(k string, _ state.RecordState) bool {
		ws, ok := windowStartFromKey(k)
		return ok && ws == wantWS
	}
	res, err := Iterate(st, 0, filter)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 entries at ws=200, got %d", len(res))
	}
	for _, e := range res {
		ws, _ := windowStartFromKey(e.Key)
		if ws != wantWS {
			t.Fatalf("unexpected ws: %d for key %s", ws, e.Key)
		}
	}
}

func TestIterate_FilterByWindowStart_RangeAndLimit(t *testing.T) {
	st := state.NewInMemoryStore()
	st.LoadAll(map[string]state.RecordState{
		"A#p1#100": {SumAmount: 1},
		"A#p2#150": {SumAmount: 2},
		"B#p1#200": {SumAmount: 3},
		"B#p2#250": {SumAmount: 4},
		"C#p3#300": {SumAmount: 5},
	})
	lo, hi := int64(150), int64(260)
	filter := func(k string, _ state.RecordState) bool {
		ws, ok := windowStartFromKey(k)
		return ok && ws >= lo && ws <= hi
	}
	// limit=2 should return exactly 2 entries in the range
	res, err := Iterate(st, 2, filter)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 entries with limit=2, got %d", len(res))
	}
	for _, e := range res {
		ws, _ := windowStartFromKey(e.Key)
		if ws < lo || ws > hi {
			t.Fatalf("ws %d out of [%d,%d] for key %s", ws, lo, hi, e.Key)
		}
	}
}
