package restorekafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"hpb/internal/changelog"
	"hpb/internal/manifest"
	"hpb/internal/restorefs"
	"hpb/internal/state"
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
	if len(k.brokers) == 0 {
		return manifest.Manifest{}, fmt.Errorf("no brokers configured")
	}
	conn, err := kafka.DialLeader(context.Background(), "tcp", k.brokers[0], k.topic, 0)
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("dial leader: %w", err)
	}
	defer conn.Close()
	first, last, err := conn.ReadOffsets()
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("read offsets: %w", err)
	}
	if last <= first {
		return manifest.Manifest{}, fmt.Errorf("no manifest found (topic empty)")
	}

	windowSizes := []int64{500, 5_000, 50_000, last - first}
	seen := make(map[int64]struct{})
	for attempt, window := range windowSizes {
		if window <= 0 {
			continue
		}
		if _, ok := seen[window]; ok {
			continue
		}
		seen[window] = struct{}{}
		start := last - window
		if start < first {
			start = first
		}
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     k.brokers,
			Topic:       k.topic,
			Partition:   0,
			StartOffset: start,
			MinBytes:    1,
			MaxBytes:    10e6,
			MaxWait:     500 * time.Millisecond,
		})
		var latest manifest.Manifest
		timeout := 5*time.Second + time.Duration(attempt)*5*time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		for {
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				cancel()
				reader.Close()
				return manifest.Manifest{}, fmt.Errorf("read kafka: %w", err)
			}
			if string(m.Key) == string(k.key) {
				var man manifest.Manifest
				if err := json.Unmarshal(m.Value, &man); err != nil {
					cancel()
					reader.Close()
					return manifest.Manifest{}, fmt.Errorf("unmarshal kafka manifest: %w", err)
				}
				latest = man
			}
			if m.Offset >= last-1 {
				break
			}
		}
		cancel()
		reader.Close()
		if latest.SnapshotID != "" {
			return latest, nil
		}
	}
	return manifest.Manifest{}, fmt.Errorf("no manifest found for key after scanning windows up to offset %d", last)
}

func ReplayChangelogKafka(st state.Store, brokers []string, topic string, fromOffset int64) restorefs.RestoreResult {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	applied, skipped := 0, 0
	var bytes int64
	var lastOffset int64 = -1
	// Discover partitions
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return restorefs.RestoreResult{Error: fmt.Errorf("dial broker: %w", err)}
	}
	defer conn.Close()
	parts, err := conn.ReadPartitions(topic)
	if err != nil {
		return restorefs.RestoreResult{Error: fmt.Errorf("read partitions: %w", err)}
	}
	// Read each partition sequentially for simplicity within timeout
	for _, p := range parts {
		if p.Topic != topic {
			continue
		}
		partConn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, p.ID)
		if err != nil {
			rd := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, Partition: p.ID, MinBytes: 1, MaxBytes: 10e6})
			drainPartition(ctx, rd, p.ID, fromOffset, st, &applied, &skipped, &bytes, &lastOffset)
			continue
		}
		first, high, err := partConn.ReadOffsets()
		partConn.Close()
		if err != nil {
			rd := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, Partition: p.ID, MinBytes: 1, MaxBytes: 10e6})
			drainPartition(ctx, rd, p.ID, fromOffset, st, &applied, &skipped, &bytes, &lastOffset)
			continue
		}
		rd := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, Partition: p.ID, MinBytes: 1, MaxBytes: 10e6})
		if err := rd.SetOffset(first); err != nil {
			drainPartition(ctx, rd, p.ID, fromOffset, st, &applied, &skipped, &bytes, &lastOffset)
			continue
		}
		drainPartitionWithHigh(ctx, rd, p.ID, fromOffset, high, st, &applied, &skipped, &bytes, &lastOffset)
	}
	return restorefs.RestoreResult{Applied: applied, Skipped: skipped, Bytes: bytes, LastAppliedOffset: lastOffset}
}

func drainPartitionWithHigh(parent context.Context, rd *kafka.Reader, partition int, fromOffset int64, high int64, st state.Store, applied, skipped *int, bytes *int64, lastOffset *int64) {
	defer rd.Close()
	var idx int64
	for {
		if high > 0 && idx >= high {
			return
		}
		ctx, cancel := context.WithTimeout(parent, 2*time.Second)
		m, err := rd.ReadMessage(ctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			return
		}
		idx++
		if high > 0 && m.Offset >= high-1 {
			processRestoreMessage(m, partition, idx, fromOffset, st, applied, skipped, bytes, lastOffset)
			return
		}
		processRestoreMessage(m, partition, idx, fromOffset, st, applied, skipped, bytes, lastOffset)
	}
}

func drainPartition(parent context.Context, rd *kafka.Reader, partition int, fromOffset int64, st state.Store, applied, skipped *int, bytes *int64, lastOffset *int64) {
	defer rd.Close()
	var idx int64
	for {
		ctx, cancel := context.WithTimeout(parent, 2*time.Second)
		m, err := rd.ReadMessage(ctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			return
		}
		idx++
		processRestoreMessage(m, partition, idx, fromOffset, st, applied, skipped, bytes, lastOffset)
	}
}

func processRestoreMessage(m kafka.Message, partition int, idx, fromOffset int64, st state.Store, applied, skipped *int, bytes *int64, lastOffset *int64) {
	if idx <= fromOffset {
		return
	}
	if len(m.Value) == 0 {
		log.Printf("restore changelog: skip tombstone at offset=%d partition=%d", m.Offset, partition)
		*skipped++
		return
	}
	if !json.Valid(m.Value) {
		log.Printf("restore changelog: skip invalid json at offset=%d partition=%d", m.Offset, partition)
		*skipped++
		return
	}
	var d changelog.Delta
	if err := json.Unmarshal(m.Value, &d); err != nil {
		log.Printf("restore changelog: unmarshal error at offset=%d partition=%d: %v", m.Offset, partition, err)
		*skipped++
		return
	}
	ok, _, err := st.Apply(d.Key, d.Delta, d.DeltaQty, d.Seq)
	if err != nil {
		log.Printf("restore changelog: apply error at offset=%d partition=%d: %v", m.Offset, partition, err)
		*skipped++
		return
	}
	if ok {
		(*applied)++
	} else {
		(*skipped)++
	}
	*bytes += int64(len(m.Value))
	if m.Offset > *lastOffset {
		*lastOffset = m.Offset
	}
}
