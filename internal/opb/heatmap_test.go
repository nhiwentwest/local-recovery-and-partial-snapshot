package opb

import (
	"net/http/httptest"
	"strings"
	"testing"

	"hpb/internal/state"
)

func TestBuildHeatmap_GroupAndLimit(t *testing.T) {
	st := state.NewInMemoryStore()
	st.LoadAll(map[string]state.RecordState{
		"A#p1#100": {SumQty: 2, SumAmount: 200, LastSeq: 1},
		"A#p2#100": {SumQty: 3, SumAmount: 300, LastSeq: 2},
		"B#p1#100": {SumQty: 5, SumAmount: 500, LastSeq: 1},
		"A#p1#200": {SumQty: 7, SumAmount: 700, LastSeq: 1},
	})
	cells := BuildHeatmap(st, 100, "", "sumQty", 10)
	if len(cells) != 2 {
		t.Fatalf("want 2 cells, got %d", len(cells))
	}
	if cells[0].StoreID != "B" || cells[0].Value != 5 {
		t.Fatalf("top should be B=5, got %+v", cells[0])
	}
	cells = BuildHeatmap(st, 100, "A", "sumAmount", 1)
	if len(cells) != 1 || cells[0].StoreID != "A" {
		t.Fatalf("limit/prefix failed: %+v", cells)
	}
}

func TestNewHeatmapHandler_JSON(t *testing.T) {
	st := state.NewInMemoryStore()
	st.LoadAll(map[string]state.RecordState{
		"A#p1#100": {SumQty: 2, SumAmount: 200, LastSeq: 1},
		"B#p1#100": {SumQty: 5, SumAmount: 500, LastSeq: 1},
	})
	h := NewHeatmapHandler(st, 60, "X1")
	req := httptest.NewRequest("GET", "/viz/heatmap?ws=100&metric=sumQty&limit=1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("status %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "\"cells\"") || !strings.Contains(body, "\"ws\":100") {
		t.Fatalf("bad body: %s", body)
	}
}
