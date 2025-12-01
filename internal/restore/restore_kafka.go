//go:build integration
// +build integration

package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"hpb/internal/changelog"
	"hpb/internal/manifest"
)

// KafkaReader reads latest manifest record from a compacted Kafka topic.
type KafkaReader struct {
	brokers []string
	topic   string
	key     []byte
}

func NewKafkaReader(brokers []string, topic string, key string) *KafkaReader {
	return &KafkaReader{brokers: brokers, topic: topic, key: []byte(key)}
}

func (k *KafkaReader) ReadLatest() (manifest.Manifest, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   k.brokers,
		Topic:     k.topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var last manifest.Manifest
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return manifest.Manifest{}, fmt.Errorf("read kafka: %w", err)
		}
		if string(m.Key) != string(k.key) {
			continue
		}
		var man manifest.Manifest
		if err := json.Unmarshal(m.Value, &man); err != nil {
			return manifest.Manifest{}, fmt.Errorf("unmarshal kafka manifest: %w", err)
		}
		last = man
	}
	if last.SnapshotID == "" {
		return manifest.Manifest{}, fmt.Errorf("no manifest found for key")
	}
	return last, nil
}

// ReplayChangelogKafka consumes deltas from Kafka topic (partition 0) and applies them.
func (r *Restorer) ReplayChangelogKafka(brokers []string, topic string, fromOffset int64) RestoreResult {
	rd := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer rd.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	applied, skipped := 0, 0
	var bytes int64
	var lastOffset int64 = -1
	idx := int64(0)
	for {
		m, err := rd.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return RestoreResult{Applied: applied, Skipped: skipped, Error: fmt.Errorf("read kafka: %w", err)}
		}
		idx++
		if idx <= fromOffset {
			continue
		}
		var d changelog.Delta
		if err := json.Unmarshal(m.Value, &d); err != nil {
			return RestoreResult{Applied: applied, Skipped: skipped, Error: fmt.Errorf("unmarshal delta: %w", err)}
		}
		ok, _, err := r.stateStore.Apply(d.Key, d.Delta, d.DeltaQty, d.Seq, state.SourceUnspecified)
		if err != nil {
			return RestoreResult{Applied: applied, Skipped: skipped, Error: fmt.Errorf("apply: %w", err)}
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
