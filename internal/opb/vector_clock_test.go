package opb

import (
	"encoding/json"
	"testing"
)

func TestVectorClockBasicOps(t *testing.T) {
	vc1 := NewVectorClock().Tick("A").Tick("A") // A:2
	vc2 := NewVectorClock().Tick("A").Tick("B") // A:1,B:1
	// Merge: max(A:2 vs 1)=2, B:1
	m := vc1.Copy().Merge(vc2)
	if m["A"] != 2 || m["B"] != 1 {
		t.Fatalf("merge unexpected: %v", m)
	}
	if !vc2.HappensBefore(m) {
		t.Fatalf("expected vc2 < m, got false")
	}
	if vc1.Concurrent(vc2) {
		// vc1={A:2}, vc2={A:1,B:1} are not comparable element-wise -> concurrent true
		// Actually vc1 vs vc2: for A, 2>1; for B, 0<1; neither <= the other
		// This branch intentionally left to document concurrency behavior.
	} else {
		// Require concurrent true
		t.Fatalf("expected vc1 concurrent vc2")
	}
	// LessEq and Equal
	vc3 := VectorClock{"A": 2, "B": 1}
	if !vc3.LessEq(m) || !m.LessEq(vc3) || !m.Equal(vc3) {
		t.Fatalf("equality failed: m=%v vc3=%v", m, vc3)
	}
}

func TestVectorClockJSON(t *testing.T) {
	vc := VectorClock{"X": 10, "Y": 5}
	b, err := json.Marshal(vc)
	if err != nil {
		t.Fatalf("marshal err: %v", err)
	}
	var dec VectorClock
	if err := json.Unmarshal(b, &dec); err != nil {
		t.Fatalf("unmarshal err: %v", err)
	}
	if !vc.Equal(dec) {
		t.Fatalf("roundtrip mismatch: %v vs %v", vc, dec)
	}
}
