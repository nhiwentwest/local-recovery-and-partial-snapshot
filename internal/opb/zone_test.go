package opb

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"hpb/internal/state"
)

func TestZoneIndex_OnAppliedAndSnapshot(t *testing.T) {
	idx := NewZoneIndex()
	idx.OnApplied("A", 100, 2, "B1")
	idx.OnApplied("A", 50, 1, "B2")
	sa, sq, rel := idx.Snapshot("A", 0)
	if sa != 150 || sq != 3 {
		t.Fatalf("agg mismatch: sumAmount=%d sumQty=%d", sa, sq)
	}
	if len(rel) != 2 { t.Fatalf("instances mismatch: %v", rel) }
}

func TestZoneDetailsHandler_Validation(t *testing.T) {
	st := state.NewInMemoryStore()
	h := NewZoneDetailsHandler(st, NewZoneIndex(), 60, "X1", RealClock{})
	r := httptest.NewRequest("GET", "/api/zone-details", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 { t.Fatalf("want 400, got %d", w.Code) }
}

func TestZoneDetailsHandler_ExactAndStoreModes(t *testing.T) {
	st := state.NewInMemoryStore()
	st.LoadAll(map[string]state.RecordState{
		"A#p1#100": {SumAmount: 300, SumQty: 3, LastSeq: 2},
	})
	idx := NewZoneIndex()
	idx.OnApplied("A", 300, 3, "B1")
	h := NewZoneDetailsHandler(st, idx, 60, "X1", RealClock{})

	// Exact
	r1 := httptest.NewRequest("GET", "/api/zone-details?id=A&productId=p1&ws=100", nil)
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, r1)
	if w1.Code != 200 { t.Fatalf("exact: %d", w1.Code) }
	var exact map[string]any
	_ = json.Unmarshal(w1.Body.Bytes(), &exact)
	if exact["sumQty"].(float64) != 3 { t.Fatalf("exact sumQty: %v", exact["sumQty"]) }

	// Store mode
	r2 := httptest.NewRequest("GET", "/api/zone-details?id=A", nil)
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, r2)
	if w2.Code != 200 { t.Fatalf("store: %d", w2.Code) }
	var store map[string]any
	_ = json.Unmarshal(w2.Body.Bytes(), &store)
	if store["sumQty"].(float64) != 3 { t.Fatalf("store sumQty: %v", store["sumQty"]) }
}

