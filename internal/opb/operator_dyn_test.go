package opb

import (
	"reflect"
	"testing"
	"time"
)

func TestDynamicNIO_ExpectedAndCompleteInflight(t *testing.T) {
	op := NewDynamicNInputOperator()
	// Pre-create channels so recordInflight will be enabled correctly on first marker
	op.EnsureChannel("t1#0")
	op.EnsureChannel("t1#1")
	op.EnsureChannel("t2#0")
	// Provider returns a stable expected set at first marker
	op.Expected = func() []string { return []string{"t1#0", "t1#1", "t2#0"} }

	propagated := 0
	op.Propagate = func(m Marker) { propagated++ }

	blocked := make(map[string]int)
	op.OnBlock = func(ch string) { blocked[ch]++ }

	completedCh := make(chan struct{}, 1)
	var gotID string
	var got map[string][]Event
	op.Complete = func(id string, inflight map[string][]Event) {
		gotID = id
		got = inflight
		completedCh <- struct{}{}
	}

	// First marker on t1#0 starts the cut and blocks that channel; others start recording inflight
	m := Marker{SnapshotID: "s1"}
	op.OnIn("t1#0", Event{Marker: &m})
	if propagated != 1 {
		t.Fatalf("expected propagate once, got %d", propagated)
	}
	if blocked["t1#0"] != 1 {
		t.Fatalf("expected t1#0 blocked once")
	}

	// In-flight events on other expected channels between first marker and their markers
	op.OnIn("t1#1", Event{Key: "k-11-a"})
	op.OnIn("t2#0", Event{Key: "k-20-a"})
	// Events on the first-marker channel should NOT be recorded while it is blocked
	op.OnIn("t1#0", Event{Key: "k-10-should-not-record"})

	// Now send marker on t1#1; inflight recording for t1#1 stops, channel blocked
	op.OnIn("t1#1", Event{Marker: &m})
	if blocked["t1#1"] != 1 {
		t.Fatalf("expected t1#1 blocked once")
	}
	// After its marker, further data on t1#1 should not be recorded for s1
	op.OnIn("t1#1", Event{Key: "k-11-b"})

	// Completion should not trigger yet (missing t2#0 marker)
	select {
	case <-completedCh:
		t.Fatalf("unexpected completion before all expected markers")
	case <-time.After(50 * time.Millisecond):
	}

	// Final expected marker
	op.OnIn("t2#0", Event{Marker: &m})
	if blocked["t2#0"] != 1 {
		t.Fatalf("expected t2#0 blocked once")
	}

	select {
	case <-completedCh:
		// ok
	case <-time.After(250 * time.Millisecond):
		t.Fatalf("expected completion not received")
	}
	if gotID != "s1" {
		t.Fatalf("unexpected snapshot id: %s", gotID)
	}
	// Validate inflight captured only for expected channels and only the correct items
	wantKeys := map[string][]string{
		"t1#0": {},         // first-marker channel has no inflight
		"t1#1": {"k-11-a"}, // only before its marker
		"t2#0": {"k-20-a"}, // only before its marker
	}
	if len(got) != len(wantKeys) {
		t.Fatalf("inflight size mismatch: got %d want %d", len(got), len(wantKeys))
	}
	for ch, want := range wantKeys {
		var keys []string
		for _, ev := range got[ch] {
			keys = append(keys, ev.Key)
		}
		// Treat nil and empty slices as equal for comparison
		if keys == nil {
			keys = []string{}
		}
		if want == nil {
			want = []string{}
		}
		if !reflect.DeepEqual(keys, want) {
			t.Fatalf("inflight[%s] mismatch: got %v want %v", ch, keys, want)
		}
	}
}

func TestDynamicNIO_ChannelsAddedAfterFirstMarkerIgnored(t *testing.T) {
	op := NewDynamicNInputOperator()
	// Pre-existing channels
	op.EnsureChannel("a#0")
	op.EnsureChannel("a#1")
	op.Expected = func() []string { return []string{"a#0", "a#1"} }

	completedCh := make(chan struct{}, 1)
	op.Complete = func(id string, inflight map[string][]Event) { completedCh <- struct{}{} }

	m := Marker{SnapshotID: "x"}
	op.OnIn("a#0", Event{Marker: &m}) // start

	// Add a new channel after start; it should not be part of expected nor inflight-recording
	op.EnsureChannel("b#2")
	// Send data on b#2 – should not be recorded for current snapshot
	op.OnIn("b#2", Event{Key: "late"})
	// Marker on b#2 should not be required for completion
	op.OnIn("b#2", Event{Marker: &m})

	// Complete by marking only expected remaining channel
	op.OnIn("a#1", Event{Marker: &m})
	select {
	case <-completedCh:
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected completion but timed out")
	}
}
