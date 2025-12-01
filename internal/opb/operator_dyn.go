package opb

// DynamicNInputOperator supports dynamic set of input channels keyed by string (e.g., topic#partition).
// On first marker seen, it captures the expected set of channels currently known and waits for the same
// marker to arrive on all of them before completing the snapshot.
// Channels added after the first marker are ignored for that snapshot and will participate in the next one.
type DynamicNInputOperator struct {
	// Callbacks
	Propagate func(m Marker)                               // called on first marker
	Complete  func(id string, inflight map[string][]Event) // on completion with in-flight per channel
	OnBlock   func(ch string)                              // when a channel gets blocked
	OnUnblock func()                                       // when snapshot completes
	OnData    func(ch string, ev Event)                    // optional data processing when not blocked
	Expected  func() []string                              // optional provider of expected channels at first marker

	// State
	channels map[string]*dynCh
	expected map[string]bool // snapshot of channels at first marker
	cutID    string
}

// CurCutID returns the current snapshot id if a cut is in progress; empty string otherwise.
func (op *DynamicNInputOperator) CurCutID() string { return op.cutID }

type dynCh struct {
	seen           bool
	blocked        bool
	recordInflight bool
	inflight       []Event
}

func NewDynamicNInputOperator() *DynamicNInputOperator {
	return &DynamicNInputOperator{channels: make(map[string]*dynCh)}
}

// EnsureChannel creates a channel entry if not present.
func (op *DynamicNInputOperator) EnsureChannel(key string) {
	if _, ok := op.channels[key]; !ok {
		op.channels[key] = &dynCh{}
	}
}

func (op *DynamicNInputOperator) Reset() {
	op.cutID = ""
	op.expected = nil
	for k := range op.channels {
		op.channels[k] = &dynCh{}
	}
}

// OnIn handles an event for channel key.
func (op *DynamicNInputOperator) OnIn(key string, ev Event) {
	op.EnsureChannel(key)
	if ev.Marker != nil {
		op.onMarker(key, *ev.Marker)
		return
	}
	ch := op.channels[key]
	if ch.blocked {
		return
	}
	if ch.recordInflight {
		ch.inflight = append(ch.inflight, ev)
	}
	if op.OnData != nil {
		op.OnData(key, ev)
	}
}

func (op *DynamicNInputOperator) onMarker(key string, m Marker) {
	ch := op.channels[key]
	if op.cutID == "" {
		op.cutID = m.SnapshotID
		// capture expected channels at this moment
		if op.Expected != nil {
			op.expected = make(map[string]bool)
			for _, k := range op.Expected() {
				op.expected[k] = true
			}
		} else {
			op.expected = make(map[string]bool, len(op.channels))
			for k := range op.channels {
				op.expected[k] = true
			}
		}
		ch.seen, ch.blocked = true, true
		if op.OnBlock != nil {
			op.OnBlock(key)
		}
		for k, c := range op.channels {
			if k == key {
				continue
			}
			c.recordInflight = true
		}
		if op.Propagate != nil {
			op.Propagate(m)
		}
		return
	}
	// subsequent marker must match
	if m.SnapshotID != op.cutID {
		return
	}
	if c := op.channels[key]; c != nil {
		c.seen, c.blocked = true, true
		c.recordInflight = false
		if op.OnBlock != nil {
			op.OnBlock(key)
		}
	}
	op.maybeComplete()
}

func (op *DynamicNInputOperator) maybeComplete() {
	if op.cutID == "" || op.expected == nil {
		return
	}
	for k := range op.expected {
		c := op.channels[k]
		if c == nil || !c.seen {
			return
		}
	}
	// collect inflight for expected channels only
	res := make(map[string][]Event, len(op.expected))
	for k := range op.expected {
		c := op.channels[k]
		buf := make([]Event, len(c.inflight))
		copy(buf, c.inflight)
		res[k] = buf
	}
	if op.Complete != nil {
		op.Complete(op.cutID, res)
	}
	if op.OnUnblock != nil {
		op.OnUnblock()
	}
	op.Reset()
}
