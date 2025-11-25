package opb

// Phase 3.2 PoC: two-input operator with Chandy-Lamport-style marker logic.
// This is an in-memory/state-machine only PoC intended to be tested with unit tests.

// Marker represents a snapshot initiation marker.
type Marker struct {
	SnapshotID string
	VC         VectorClock
}

// Event is a generic envelope for data or marker. If Marker is non-nil, this is a marker event.
type Event struct {
	Key    string
	VC     VectorClock
	Marker *Marker
}

// TwoInputOperator models an operator with two inputs following marker protocol.
// It records in-flight messages on the non-marked channel after the first marker
// until the same marker arrives on that channel, then completes the snapshot.
type TwoInputOperator struct {
	// callbacks for testing/embedding
	Propagate func(m Marker) // called once when first marker for a snapshot is seen
	Complete  func(id string, inflightFrom1 []Event, inflightFrom2 []Event)

	// internal state
	cutID                 string
	seen1, seen2          bool
	blocked1, blocked2    bool
	recordInflight1       bool
	recordInflight2       bool
	inflightFrom1         []Event
	inflightFrom2         []Event
}

// Reset clears operator state (used after completion).
func (op *TwoInputOperator) Reset() {
	op.cutID = ""
	op.seen1, op.seen2 = false, false
	op.blocked1, op.blocked2 = false, false
	op.recordInflight1, op.recordInflight2 = false, false
	op.inflightFrom1 = nil
	op.inflightFrom2 = nil
}

// OnIn1 handles an event arriving on input 1.
func (op *TwoInputOperator) OnIn1(ev Event) {
	if ev.Marker != nil {
		op.onMarker(1, *ev.Marker)
		return
	}
	// Data path
	if op.blocked1 {
		// channel 1 is blocked until snapshot completes; for PoC we ignore processing while blocked
		return
	}
	if op.recordInflight1 {
		op.inflightFrom1 = append(op.inflightFrom1, ev)
	}
	// In a real operator, processing would occur here.
}

// OnIn2 handles an event arriving on input 2.
func (op *TwoInputOperator) OnIn2(ev Event) {
	if ev.Marker != nil {
		op.onMarker(2, *ev.Marker)
		return
	}
	// Data path
	if op.blocked2 {
		// channel 2 is blocked until snapshot completes; for PoC we ignore processing while blocked
		return
	}
	if op.recordInflight2 {
		op.inflightFrom2 = append(op.inflightFrom2, ev)
	}
	// In a real operator, processing would occur here.
}

func (op *TwoInputOperator) onMarker(ch int, m Marker) {
	if op.cutID == "" {
		// First marker for this snapshot
		op.cutID = m.SnapshotID
		if ch == 1 {
			op.seen1, op.blocked1 = true, true
			// record in-flight from the other channel
			op.recordInflight2 = true
		} else {
			op.seen2, op.blocked2 = true, true
			// record in-flight from the other channel
			op.recordInflight1 = true
		}
		if op.Propagate != nil {
			op.Propagate(m)
		}
		return
	}
	// Subsequent marker; must match current cut id
	if m.SnapshotID != op.cutID {
		// For PoC we ignore stray markers (in production, this would be an error)
		return
	}
	if ch == 1 {
		op.seen1, op.blocked1 = true, true
		// stop recording from ch1 (only recorded between first marker and this one)
		op.recordInflight1 = false
	} else {
		op.seen2, op.blocked2 = true, true
		op.recordInflight2 = false
	}
	op.maybeComplete()
}

func (op *TwoInputOperator) maybeComplete() {
	if op.cutID == "" || !op.seen1 || !op.seen2 {
		return
	}
	// Both markers received: complete snapshot
	if op.Complete != nil {
		id := op.cutID
		in1 := make([]Event, len(op.inflightFrom1))
		copy(in1, op.inflightFrom1)
		in2 := make([]Event, len(op.inflightFrom2))
		copy(in2, op.inflightFrom2)
		op.Complete(id, in1, in2)
	}
	// Unblock both channels and reset for next snapshot
	op.Reset()
}

