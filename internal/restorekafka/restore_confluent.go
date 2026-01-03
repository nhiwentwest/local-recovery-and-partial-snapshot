package restorekafka

import (
	"fmt"
	"time"

	rf "hpb/internal/restorefs"
	"hpb/internal/state"
)

// ReplayChangelogConfluent replays changelog from Kafka using confluent-kafka-go with read_committed semantics.
// startOffsets are exclusive (read from that offset onward per partition).
// This function orchestrates helpers extracted to reduce cyclomatic complexity.
func ReplayChangelogConfluent(st state.Store, bootstrap string, topic string, startOffsets []int64, maxDuration time.Duration) rf.RestoreResult {
	res := rf.RestoreResult{}
	if bootstrap == "" {
		res.Error = fmt.Errorf("no bootstrap provided")
		return res
	}

	// Init consumer (extracted helper)
	c, err := initRestoreConsumer(bootstrap)
	if err != nil {
		res.Error = err
		return res
	}
	defer c.Close()

	// Discover partitions (extracted helper)
	parts, err := discoverPartitions(c, topic)
	if err != nil {
		res.Error = err
		return res
	}
	if len(parts) == 0 {
		// No partitions, nothing to do
		return res
	}

	// Build assignment with desired start offsets (extracted helper)
	assign := buildAssignment(topic, parts, startOffsets)
	if err := c.Assign(assign); err != nil {
		res.Error = fmt.Errorf("assign: %w", err)
		return res
	}
	// Optionally seek precisely to offsets (Assign with Offset should suffice, but seek adds certainty)
	for _, tp := range assign {
		if tp.Offset >= 0 {
			_ = c.Seek(tp, int(2*time.Second/time.Millisecond))
		}
	}

	// Track pending partitions (for EOF detection)
	pending := make(map[int32]struct{}, len(parts))
	for _, p := range parts {
		pending[p] = struct{}{}
	}

	// Consume loop (extracted helper)
	applied, skipped, lastOffset, bytes, err := consumeLoop(c, st, topic, pending, maxDuration)
	res.Applied = int(applied)
	res.Skipped = int(skipped)
	res.LastAppliedOffset = lastOffset
	res.Bytes = bytes
	if err != nil {
		res.Error = err
	}
	return res
}
