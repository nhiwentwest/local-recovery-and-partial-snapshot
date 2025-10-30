package model

import (
	"encoding/json"
	"testing"
)

func TestNormalize_RoundTripSchema(t *testing.T) {
	in := Order{OrderID: "o1", ProductID: "p1", Price: 9000, Qty: 2, StoreID: "S", TS: 1001}
	out := Normalize(in)
	if out.OrderID != in.OrderID || out.ProductID != in.ProductID || out.Price != in.Price || out.Qty != in.Qty || out.StoreID != in.StoreID || out.TS != in.TS {
		t.Fatalf("fields mismatch after normalize: %+v", out)
	}
	if !out.Validated || out.NormTS != in.TS {
		t.Fatalf("normalized flags invalid: validated=%v normTs=%d", out.Validated, out.NormTS)
	}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal enriched: %v", err)
	}
	var dec OrderEnriched
	if err := json.Unmarshal(b, &dec); err != nil {
		t.Fatalf("unmarshal enriched: %v", err)
	}
	if dec.OrderID != in.OrderID || dec.NormTS != in.TS {
		t.Fatalf("round-trip mismatch: %+v", dec)
	}
}
