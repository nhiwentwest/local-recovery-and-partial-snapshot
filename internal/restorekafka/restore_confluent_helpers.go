package restorekafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"hpb/internal/changelog"
	"hpb/internal/state"
)

// initRestoreConsumer creates a Kafka consumer configured for changelog replay.
// Logic copied verbatim from ReplayChangelogConfluent (no changes).
func initRestoreConsumer(bootstrap string) (*ck.Consumer, error) {
	c, err := ck.NewConsumer(&ck.ConfigMap{
		"bootstrap.servers":    bootstrap,
		"group.id":             fmt.Sprintf("opb-restore-%d", time.Now().UnixNano()),
		"enable.auto.commit":   false,
		"isolation.level":      "read_committed",
		"auto.offset.reset":    "earliest",
		"enable.partition.eof": true,
	})
	if err != nil {
		return nil, fmt.Errorf("consumer init: %w", err)
	}
	return c, nil
}

// discoverPartitions queries Kafka metadata to find all partitions for the topic.
// Returns empty slice if topic not found or has no partitions.
func discoverPartitions(c *ck.Consumer, topic string) ([]int32, error) {
	md, err := c.GetMetadata(&topic, false, int(5*time.Second/time.Millisecond))
	if err != nil {
		return nil, fmt.Errorf("metadata: %w", err)
	}
	var parts []int32
	if tp, ok := md.Topics[topic]; ok {
		for p := range tp.Partitions {
			parts = append(parts, int32(p))
		}
	}
	return parts, nil
}

// buildAssignment creates TopicPartition assignments with start offsets.
// If startOffsets[idx] >= 0, uses that offset; otherwise uses OffsetBeginning.
func buildAssignment(topic string, parts []int32, startOffsets []int64) []ck.TopicPartition {
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
	return assign
}

// consumeLoop reads messages from the consumer and applies them to the state store.
// It tracks applied/skipped counts, last offset, and bytes read.
// Returns when all partitions reach EOF or timeout occurs.
func consumeLoop(
	c *ck.Consumer,
	st state.Store,
	topic string,
	pending map[int32]struct{},
	maxDuration time.Duration,
) (applied int64, skipped int64, lastOffset int64, bytes int64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), maxDuration)
	defer cancel()
	allPartitionsDone := func() bool { return len(pending) == 0 }

loop:
	for {
		select {
		case <-ctx.Done():
			err = fmt.Errorf("restore timeout after %s", maxDuration)
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
				skipped++
				continue
			}
			if !json.Valid(msg.Value) {
				skipped++
				continue
			}
			var d changelog.Delta
			if err := json.Unmarshal(msg.Value, &d); err != nil {
				log.Printf("restore changelog: bad json at %s[%d] off=%d: %v", topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, err)
				skipped++
				continue
			}
			ok, _, aerr := st.Apply(d.Key, d.Delta, d.DeltaQty, d.Seq, state.SourceUnspecified)
			if aerr != nil {
				log.Printf("restore changelog: apply error at %s[%d] off=%d: %v", topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, aerr)
				skipped++
				continue
			}
			if ok {
				applied++
			} else {
				skipped++
			}
			if int64(msg.TopicPartition.Offset) > lastOffset {
				lastOffset = int64(msg.TopicPartition.Offset)
			}
			bytes += int64(len(msg.Value))
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
			err = fmt.Errorf("read: %w", e)
			break loop
		default:
		}
	}
	return applied, skipped, lastOffset, bytes, err
}

