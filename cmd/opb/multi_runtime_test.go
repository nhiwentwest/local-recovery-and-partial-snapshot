package main

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"hpb/internal/state"
)

type row struct {
	Key   string            `json:"key"`
	State state.RecordState `json:"state"`
}

func TestImportStateFromPeer(t *testing.T) {
	// Prepare NDJSON payload with two records
	rows := []row{
		{Key: "s1#p1#100", State: state.RecordState{SumAmount: 1000, SumQty: 1, LastSeq: 1}},
		{Key: "s2#p2#200", State: state.RecordState{SumAmount: 2000, SumQty: 2, LastSeq: 2}},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		bw := bufio.NewWriter(w)
		for _, r := range rows {
			b, _ := json.Marshal(r)
			bw.Write(b)
			bw.WriteByte('\n')
		}
		bw.Flush()
	}))
	defer ts.Close()

	st := state.NewInMemoryStore()
	count, err := importStateFromPeer(ts.URL, st)
	if err != nil {
		t.Fatalf("import error: %v", err)
	}
	if count != len(rows) {
		t.Fatalf("unexpected count: got %d want %d", count, len(rows))
	}
	for _, r := range rows {
		got, ok := st.Get(r.Key)
		if !ok {
			t.Fatalf("key not found: %s", r.Key)
		}
		if got.SumAmount != r.State.SumAmount || got.SumQty != r.State.SumQty || got.LastSeq != r.State.LastSeq {
			t.Fatalf("state mismatch for %s: got=%+v want=%+v", r.Key, got, r.State)
		}
	}
}

