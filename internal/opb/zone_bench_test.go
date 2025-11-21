package opb

import (
	"net/http/httptest"
	"testing"

	"hpb/internal/state"
)

func BenchmarkZoneDetailsHandler_Store(b *testing.B) {
	st := state.NewInMemoryStore()
	idx := NewZoneIndex()
	for i := 0; i < 10000; i++ {
		idx.OnApplied("A", 100, 1, "B1")
	}
	h := NewZoneDetailsHandler(st, idx, 60, "B1", RealClock{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := httptest.NewRequest("GET", "/api/zone-details?id=A", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 { b.Fatalf("status %d", w.Code) }
	}
}

func BenchmarkZoneDetailsHandler_Exact(b *testing.B) {
	st := state.NewInMemoryStore()
	st.LoadAll(map[string]state.RecordState{
		"A#p1#100": {SumAmount: 100, SumQty: 1, LastSeq: 1},
	})
	idx := NewZoneIndex()
	h := NewZoneDetailsHandler(st, idx, 60, "B1", RealClock{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := httptest.NewRequest("GET", "/api/zone-details?id=A&productId=p1&ws=100", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 { b.Fatalf("status %d", w.Code) }
	}
}

