package restorekafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"hpb/internal/changelog"
	rf "hpb/internal/restorefs"
	"hpb/internal/state"
)

// ReplayChangelogConfluent replays changelog from Kafka using confluent-kafka-go with read_committed semantics.
// startOffsets are exclusive (read from that offset onward per partition).
func ReplayChangelogConfluent(st state.Store, bootstrap string, topic string, startOffsets []int64, maxDuration time.Duration) rf.RestoreResult {
	res := rf.RestoreResult{}
	if bootstrap == "" {
		res.Error = fmt.Errorf("no bootstrap provided")
		return res
	}
	// Init consumer (read_committed)
	c, err := ck.NewConsumer(&ck.ConfigMap{
		"bootstrap.servers":    bootstrap,
		"group.id":             fmt.Sprintf("opb-restore-%d", time.Now().UnixNano()),
		"enable.auto.commit":   false,
		"isolation.level":      "read_committed",
		"auto.offset.reset":    "earliest",
		"enable.partition.eof": true,
	})
	if err != nil {
		res.Error = fmt.Errorf("consumer init: %w", err)
		return res
	}
	defer c.Close()
	// Discover partitions
	md, err := c.GetMetadata(&topic, false, int(5*time.Second/time.Millisecond))
	if err != nil {
		res.Error = fmt.Errorf("metadata: %w", err)
		return res
	}
	var parts []int32
	if tp, ok := md.Topics[topic]; ok {
		for p := range tp.Partitions {
			parts = append(parts, int32(p))
		}
	}
	if len(parts) == 0 {
		// No partitions, nothing to do
		return res
	}
	// Build assignment with desired start offsets
	assign := make([]ck.TopicPartition, 0, len(parts))
	for _, p := range parts {
		idx := int(p)
		var off ck.Offset = ck.OffsetBeginning
		if idx < len(startOffsets) && startOffsets[idx] >= 0 {
			off = ck.Offset(startOffsets[idx])
		}
		assign = append(assign, ck.TopicPartition{Topic: &topic, Partition: p, Offset: off})
		log.Printf("restore changelog: partition=%d startOffset=%d", p, func() int64 {
			if idx < len(startOffsets) {
				return startOffsets[idx]
			}
			return int64(off)
		}())
	}
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
	pending := make(map[int32]struct{}, len(parts))
	for _, p := range parts {
		pending[p] = struct{}{}
	}

	// Read loop with overall timeout
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()
	allPartitionsDone := func() bool { return len(pending) == 0 }

loop:
	for {
		select {
		case <-ctx.Done():
			res.Error = fmt.Errorf("restore timeout after %s", maxDuration)
			break loop
		default:
		}
		if allPartitionsDone() {
			break loop
		}
		ev := c.Poll(100)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *ck.Message:
			msg := e
			if msg == nil || len(msg.Value) == 0 {
				res.Skipped++
				continue
			}
			if !json.Valid(msg.Value) {
				res.Skipped++
				continue
			}
			var d changelog.Delta
			if err := json.Unmarshal(msg.Value, &d); err != nil {
				log.Printf("restore changelog: bad json at %s[%d] off=%d: %v", topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, err)
				res.Skipped++
				continue
			}
			ok, _, aerr := st.Apply(d.Key, d.Delta, d.DeltaQty, d.Seq)
			if aerr != nil {
				log.Printf("restore changelog: apply error at %s[%d] off=%d: %v", topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, aerr)
				res.Skipped++
				continue
			}
			if ok {
				res.Applied++
			} else {
				res.Skipped++
			}
			if int64(msg.TopicPartition.Offset) > res.LastAppliedOffset {
				res.LastAppliedOffset = int64(msg.TopicPartition.Offset)
			}
			res.Bytes += int64(len(msg.Value))
		case ck.PartitionEOF:
			tp := ck.TopicPartition(e)
			if _, ok := pending[tp.Partition]; ok {
				delete(pending, tp.Partition)
				if err := c.Pause([]ck.TopicPartition{tp}); err != nil {
					log.Printf("restore changelog: pause error partition=%d: %v", tp.Partition, err)
				}
			}
			if allPartitionsDone() {
				break loop
			}
		case ck.Error:
			if e.Code() == ck.ErrTimedOut {
				continue
			}
			res.Error = fmt.Errorf("read: %w", e)
			break loop
		default:
		}
	}
	return res
}
