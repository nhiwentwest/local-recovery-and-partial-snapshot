package opb

import (
	"reflect"
	"testing"
)

func TestNInputOperator_ThreeInputs_OrderVaried(t *testing.T) {
	op := NewNInputOperator(3)
	var propCount int
	var completed string
	var inflight [][]Event
	op.Propagate = func(m Marker) { propCount++ }
	op.Complete = func(id string, in [][]Event) { completed = id; inflight = in }

	m := Marker{SnapshotID: "S", VC: VectorClock{"s": 1}}

	// First marker on ch=1 (index 1)
	op.OnIn(1, Event{Marker: &m})
	if op.cutID != "S" || !op.blocked[1] {
		t.Fatalf("expected cut S and block ch1: %+v", op)
	}
	// Data arrives on ch0 and ch2, should be recorded as inflight
	A := Event{Key: "A"}
	B := Event{Key: "B"}
	C := Event{Key: "C"}
	op.OnIn(0, A)
	op.OnIn(2, B)
	op.OnIn(2, C)
	// Marker arrives on ch0, stop recording for ch0
	op.OnIn(0, Event{Marker: &m})
	// More data on ch2 still recorded until ch2 marker
	D := Event{Key: "D"}
	op.OnIn(2, D)
	// Final marker on ch2 -> complete
	op.OnIn(2, Event{Marker: &m})
	if completed != "S" {
		t.Fatalf("expected completed S, got %s", completed)
	}
	// inflight[0] should have [A]
	if !reflect.DeepEqual(inflight[0], []Event{A}) {
		t.Fatalf("inflight[0] mismatch: %v", inflight[0])
	}
	// inflight[1] should be empty (first marker on ch1, no recording for ch1)
	if len(inflight[1]) != 0 {
		t.Fatalf("inflight[1] expected empty, got %v", inflight[1])
	}
	// inflight[2] should have [B,C,D]
	if !reflect.DeepEqual(inflight[2], []Event{B, C, D}) {
		t.Fatalf("inflight[2] mismatch: %v", inflight[2])
	}
	if propCount != 1 {
		t.Fatalf("Propagate should be called once, got %d", propCount)
	}
	// State reset after completion
	if op.cutID != "" {
		t.Fatalf("state not reset: %v", op.cutID)
	}
}

