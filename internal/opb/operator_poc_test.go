package opb

import (
	"reflect"
	"testing"
)

func TestTwoInputOperator_MarkerOrder_In1First(t *testing.T) {
	var propagated []Marker
	var completedID string
	var inflight1, inflight2 []Event
	op := &TwoInputOperator{
		Propagate: func(m Marker) { propagated = append(propagated, m) },
		Complete: func(id string, in1 []Event, in2 []Event) {
			completedID = id
			inflight1 = append([]Event{}, in1...)
			inflight2 = append([]Event{}, in2...)
		},
	}
	m := Marker{SnapshotID: "S1", VC: VectorClock{"src": 1}}
	// First marker on ch1 -> block ch1, start recording inflight from ch2
	op.OnIn1(Event{Marker: &m})
	if op.cutID != "S1" || !op.blocked1 || !op.recordInflight2 {
		t.Fatalf("unexpected state after first marker ch1: %+v", op)
	}
	// Data on ch2 should be captured as inflight
	data2a := Event{Key: "k2a", VC: VectorClock{"src": 2}}
	data2b := Event{Key: "k2b", VC: VectorClock{"src": 3}}
	op.OnIn2(data2a)
	op.OnIn2(data2b)
	if got := len(op.inflightFrom2); got != 2 {
		t.Fatalf("want inflight2=2 got=%d", got)
	}
	// When marker arrives on ch2, snapshot completes
	op.OnIn2(Event{Marker: &m})
	if completedID != "S1" {
		t.Fatalf("expected complete id S1, got %s", completedID)
	}
	if len(inflight1) != 0 {
		t.Fatalf("expected inflight1 empty, got %v", inflight1)
	}
	if !reflect.DeepEqual(inflight2, []Event{data2a, data2b}) {
		t.Fatalf("unexpected inflight2: %v", inflight2)
	}
	// Ensure state reset
	if op.cutID != "" || op.blocked1 || op.blocked2 || op.recordInflight1 || op.recordInflight2 {
		t.Fatalf("state not reset: %+v", op)
	}
	// Propagate called once
	if len(propagated) != 1 || propagated[0].SnapshotID != "S1" {
		t.Fatalf("propagate not called once correctly: %v", propagated)
	}
}

func TestTwoInputOperator_MarkerOrder_In2First(t *testing.T) {
	var completedID string
	var inflight1, inflight2 []Event
	op := &TwoInputOperator{
		Complete: func(id string, in1 []Event, in2 []Event) {
			completedID = id
			inflight1 = append([]Event{}, in1...)
			inflight2 = append([]Event{}, in2...)
		},
	}
	m := Marker{SnapshotID: "S2", VC: VectorClock{"src": 1}}
	// First marker on ch2 -> block ch2, start recording inflight from ch1
	op.OnIn2(Event{Marker: &m})
	if op.cutID != "S2" || !op.blocked2 || !op.recordInflight1 {
		t.Fatalf("unexpected state after first marker ch2: %+v", op)
	}
	// Data on ch1 should be captured as inflight
	data1a := Event{Key: "k1a"}
	data1b := Event{Key: "k1b"}
	op.OnIn1(data1a)
	op.OnIn1(data1b)
	// Finish with marker on ch1
	op.OnIn1(Event{Marker: &m})
	if completedID != "S2" {
		t.Fatalf("expected complete id S2, got %s", completedID)
	}
	if len(inflight2) != 0 {
		t.Fatalf("expected inflight2 empty, got %v", inflight2)
	}
	if !reflect.DeepEqual(inflight1, []Event{data1a, data1b}) {
		t.Fatalf("unexpected inflight1: %v", inflight1)
	}
}
