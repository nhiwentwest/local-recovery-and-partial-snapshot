package main

import (
	"fmt"
	"testing"
	"time"
)

// This test validates the keyOut calculation logic used by bench_latency to
// target the correct partition/window on the measurement topic, and sanity
// checks that partition pinning is stable.
func TestHitMiss_KeyAndPartitionLogic(t *testing.T) {
	store := "A"
	pid := "pX"
	window := 10

	// Simulate "now" at a deterministic value
	now := time.Unix(1_700_000_015, 0).UTC().Unix() // 15s into current minute
	ws := (now / int64(window)) * int64(window)
	keyOut := fmt.Sprintf("%s#%s#%d", store, pid, ws)

	parts := 6
	part := partitionForKey([]byte(keyOut), parts)
	if part < 0 || int(part) >= parts {
		t.Fatalf("partition out of range: %d of %d", part, parts)
	}
	// Stability
	if got := partitionForKey([]byte(keyOut), parts); got != part {
		t.Fatalf("partition not stable: %d vs %d", part, got)
	}

	// If we advance to the next window boundary, keyOut must change
	ws2 := ((now + int64(window)) / int64(window)) * int64(window)
	if ws2 == ws {
		t.Fatalf("expected next window to differ: ws=%d ws2=%d", ws, ws2)
	}
	keyOut2 := fmt.Sprintf("%s#%s#%d", store, pid, ws2)
	if keyOut2 == keyOut {
		t.Fatalf("keyOut must change across windows: %s vs %s", keyOut, keyOut2)
	}
}
