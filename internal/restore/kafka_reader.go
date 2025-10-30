//go:build integration
// +build integration

package restore

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"

	"hpb/internal/changelog"
)

// KafkaMessageReader abstracts kafka.Reader for testability.
type KafkaMessageReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

// NewKafkaReaderAdapter builds a Kafka reader implementing KafkaMessageReader.
func NewKafkaReaderAdapter(brokers []string, topic string) KafkaMessageReader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
}

// ReplayChangelogKafkaWith replays using an injected KafkaMessageReader.
// fromOffset is interpreted as message index (dev simplification) like ReplayChangelogKafka.
func (r *Restorer) ReplayChangelogKafkaWith(reader KafkaMessageReader, fromOffset int64) RestoreResult {
	defer reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	applied, skipped := 0, 0
	var bytes int64
	var lastOffset int64 = -1
	idx := int64(0)
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || err == context.DeadlineExceeded {
				break
			}
			return RestoreResult{Applied: applied, Skipped: skipped, Error: err}
		}
		idx++
		if idx <= fromOffset {
			continue
		}
		var d changelog.Delta
		if err := json.Unmarshal(m.Value, &d); err != nil {
			return RestoreResult{Applied: applied, Skipped: skipped, Error: err}
		}
		ok, _, err := r.stateStore.Apply(d.Key, d.Delta, d.DeltaQty, d.Seq)
		if err != nil {
			return RestoreResult{Applied: applied, Skipped: skipped, Error: err}
		}
		if ok {
			applied++
		} else {
			skipped++
		}
		bytes += int64(len(m.Value))
		lastOffset = m.Offset
	}
	return RestoreResult{Applied: applied, Skipped: skipped, Bytes: bytes, LastAppliedOffset: lastOffset}
}
