package opb

// Phase 3.3: Generalized N-input operator with marker protocol.

// NInputOperator maintains marker state across N input channels and records
// in-flight messages from non-marked channels between the first marker arrival
// and the marker arrival on each specific channel.
type NInputOperator struct {
	Inputs int
	// Callbacks
	Propagate func(m Marker)                      // first time seeing a marker for snapshot
	Complete  func(id string, inflight [][]Event) // when all inputs have seen the marker
	OnData    func(ch int, ev Event)              // optional: process data when channel not blocked
	OnBlock   func(ch int)                        // optional: invoked when a channel gets blocked
	OnUnblock func()                              // optional: invoked when snapshot completes

	// State
	cutID          string
	seen           []bool
	blocked        []bool
	recordInflight []bool
	inflight       [][]Event
}

// NewNInputOperator constructs an N-input operator.
func NewNInputOperator(n int) *NInputOperator {
	if n <= 0 {
		n = 1
	}
	op := &NInputOperator{Inputs: n}
	op.Reset()
	return op
}

// Reset clears snapshot state retaining configured Inputs.
func (op *NInputOperator) Reset() {
	if op.Inputs <= 0 {
		op.Inputs = 1
	}
	op.cutID = ""
	op.seen = make([]bool, op.Inputs)
	op.blocked = make([]bool, op.Inputs)
	op.recordInflight = make([]bool, op.Inputs)
	op.inflight = make([][]Event, op.Inputs)
}

// OnIn handles an event from channel ch (0-based).
func (op *NInputOperator) OnIn(ch int, ev Event) {
	if ev.Marker != nil {
		op.onMarker(ch, *ev.Marker)
		return
	}
	// Data path
	if ch < 0 || ch >= op.Inputs {
		return
	}
	if op.blocked[ch] {
		return
	}
	if op.recordInflight[ch] {
		op.inflight[ch] = append(op.inflight[ch], ev)
	}
	if op.OnData != nil {
		op.OnData(ch, ev)
	}
}

func (op *NInputOperator) onMarker(ch int, m Marker) {
	if ch < 0 || ch >= op.Inputs {
		return
	}
	if op.cutID == "" {
		// first marker
		op.cutID = m.SnapshotID
		op.seen[ch] = true
		op.blocked[ch] = true
		if op.OnBlock != nil {
			op.OnBlock(ch)
		}
		// start recording from all other channels
		for i := 0; i < op.Inputs; i++ {
			if i == ch {
				continue
			}
			op.recordInflight[i] = true
		}
		if op.Propagate != nil {
			op.Propagate(m)
		}
		return
	}
	// Subsequent markers must match snapshot id
	if m.SnapshotID != op.cutID {
		return
	}
	if !op.seen[ch] {
		op.seen[ch] = true
	}
	if !op.blocked[ch] {
		op.blocked[ch] = true
		if op.OnBlock != nil {
			op.OnBlock(ch)
		}
	}
	// stop recording for this channel
	op.recordInflight[ch] = false
	// maybe complete
	op.maybeComplete()
}

func (op *NInputOperator) maybeComplete() {
	if op.cutID == "" {
		return
	}
	for i := 0; i < op.Inputs; i++ {
		if !op.seen[i] {
			return
		}
	}
	if op.Complete != nil {
		// deep copy inflight arrays
		cpy := make([][]Event, op.Inputs)
		for i := 0; i < op.Inputs; i++ {
			buf := make([]Event, len(op.inflight[i]))
			copy(buf, op.inflight[i])
			cpy[i] = buf
		}
		op.Complete(op.cutID, cpy)
	}
	if op.OnUnblock != nil {
		op.OnUnblock()
	}
	op.Reset()
}
