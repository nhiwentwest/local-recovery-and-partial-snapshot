package restorekafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"hpb/internal/changelog"
	"hpb/internal/manifest"
	"hpb/internal/restorefs"
	"hpb/internal/state"
)

const (
	defaultReplayBatchSize   = 1000
	defaultReplayFlushPeriod = 100 * time.Millisecond
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
	return ReplayChangelogKafkaParallel(st, brokers, topic, nil, fromOffset, 0)
}

// ReplayChangelogKafkaParallel replays changelog with optional per-partition start offsets using up to `workers` goroutines (0=auto).
func ReplayChangelogKafkaParallel(st state.Store, brokers []string, topic string, startOffsets []int64, fromOffset int64, workers int) restorefs.RestoreResult {
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
	// Determine worker concurrency
	maxWorkers := len(parts)
	if workers > 0 && workers < maxWorkers {
		maxWorkers = workers
	}
	if maxWorkers > 8 {
		maxWorkers = 8
	}
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	sem := make(chan struct{}, maxWorkers)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, p := range parts {
		if p.Topic != topic {
			continue
		}
		pid := p.ID
		// Pre-check high watermark to skip partitions without backlog when startOffsets are available
		var first, high int64
		hasWatermark := false
		if conn2, e2 := kafka.DialLeader(ctx, "tcp", brokers[0], topic, pid); e2 == nil {
			if f, h, e3 := conn2.ReadOffsets(); e3 == nil {
				first, high = f, h
				hasWatermark = true
			}
			_ = conn2.Close()
		}
		useStartOffset := int(pid) < len(startOffsets) && startOffsets != nil && startOffsets[int(pid)] >= 0
		if hasWatermark && useStartOffset {
			if high <= startOffsets[int(pid)] {
				// No backlog on this partition, skip launching reader
				continue
			}
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(pid int, first int64, high int64, hasWatermark bool, useStartOffset bool) {
			defer func() { <-sem; wg.Done() }()
			localApplied, localSkipped := 0, 0
			var localBytes int64
			var localLast int64 = -1
			// Create reader and position
			rd := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, Partition: pid, MinBytes: 1, MaxBytes: 10e6, MaxWait: 50 * time.Millisecond})
			if useStartOffset {
				_ = rd.SetOffset(startOffsets[pid])
				if hasWatermark {
					drainPartitionWithHigh(ctx, rd, pid, 0, high, st, &localApplied, &localSkipped, &localBytes, &localLast)
				} else {
					drainPartition(ctx, rd, pid, 0, st, &localApplied, &localSkipped, &localBytes, &localLast)
				}
			} else {
				if err := rd.SetOffset(first); err != nil || !hasWatermark {
					drainPartition(ctx, rd, pid, fromOffset, st, &localApplied, &localSkipped, &localBytes, &localLast)
				} else {
					drainPartitionWithHigh(ctx, rd, pid, fromOffset, high, st, &localApplied, &localSkipped, &localBytes, &localLast)
				}
			}
			mu.Lock()
			applied += localApplied
			skipped += localSkipped
			bytes += localBytes
			if localLast > lastOffset {
				lastOffset = localLast
			}
			mu.Unlock()
		}(int(pid), first, high, hasWatermark, useStartOffset)
	}
	wg.Wait()
	return restorefs.RestoreResult{Applied: applied, Skipped: skipped, Bytes: bytes, LastAppliedOffset: lastOffset}
}

func drainPartitionWithHigh(parent context.Context, rd *kafka.Reader, partition int, fromOffset int64, high int64, st state.Store, applied, skipped *int, bytes *int64, lastOffset *int64) {
	defer rd.Close()
	var idx int64
	batch := make([]state.Delta, 0, defaultReplayBatchSize)
	lastFlush := time.Now()
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ap, sk, err := st.ApplyBatch(batch)
		if err != nil {
			// In batch mode, treat error as skip for safety but log it
			log.Printf("restore changelog: batch apply error on partition %d: %v", partition, err)
			*skipped += len(batch)
		} else {
			*applied += ap
			*skipped += sk
		}
		batch = batch[:0]
		lastFlush = time.Now()
	}
	for {
		if high > 0 && rd.Offset() >= high {
			flush()
			return
		}
		// reduce per-read timeout to 200ms when we know high watermark
		to := 200 * time.Millisecond
		ctx, cancel := context.WithTimeout(parent, to)
		m, err := rd.ReadMessage(ctx)
		cancel()
		if err != nil {
			if parent.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
				flush()
				return // Parent context cancelled or read timed out
			}
			flush()
			return // Other read error
		}
		idx++
		if idx <= fromOffset {
			continue
		}
		if len(m.Value) == 0 {
			log.Printf("restore changelog: skip tombstone at offset=%d partition=%d", m.Offset, partition)
			*skipped++
			continue
		}
		if !json.Valid(m.Value) {
			log.Printf("restore changelog: skip invalid json at offset=%d partition=%d", m.Offset, partition)
			*skipped++
			continue
		}
		var d changelog.Delta
		if err := json.Unmarshal(m.Value, &d); err != nil {
			log.Printf("restore changelog: unmarshal error at offset=%d partition=%d: %v", m.Offset, partition, err)
			*skipped++
			continue
		}
		batch = append(batch, state.Delta{Key: d.Key, DeltaAmount: d.Delta, DeltaQty: d.DeltaQty, Seq: d.Seq})
		*bytes += int64(len(m.Value))
		if m.Offset > *lastOffset {
			*lastOffset = m.Offset
		}
		if len(batch) >= defaultReplayBatchSize || time.Since(lastFlush) >= defaultReplayFlushPeriod {
			prevApplied := *applied
			flush()
			if *applied > prevApplied && *applied%1000 == 0 {
				log.Printf("replay: partition %d applied %d messages (offset %d)", partition, *applied, m.Offset)
			}
		}
		if high > 0 && m.Offset >= high-1 {
			flush()
			return
		}
	}
}

func drainPartition(parent context.Context, rd *kafka.Reader, partition int, fromOffset int64, st state.Store, applied, skipped *int, bytes *int64, lastOffset *int64) {
	defer rd.Close()
	var idx int64
	batch := make([]state.Delta, 0, defaultReplayBatchSize)
	lastFlush := time.Now()
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ap, sk, err := st.ApplyBatch(batch)
		if err != nil {
			log.Printf("restore changelog: batch apply error on partition %d: %v", partition, err)
			*skipped += len(batch)
		} else {
			*applied += ap
			*skipped += sk
		}
		batch = batch[:0]
		lastFlush = time.Now()
	}
	for {
		// reduce per-read timeout to 200ms
		ctx, cancel := context.WithTimeout(parent, 200*time.Millisecond)
		m, err := rd.ReadMessage(ctx)
		cancel()
		if err != nil {
			if parent.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
				flush()
				return // Parent context cancelled or read timed out
			}
			flush()
			return // Other read error
		}
		idx++
		if idx <= fromOffset {
			continue
		}
		if len(m.Value) == 0 {
			log.Printf("restore changelog: skip tombstone at offset=%d partition=%d", m.Offset, partition)
			*skipped++
			continue
		}
		if !json.Valid(m.Value) {
			log.Printf("restore changelog: skip invalid json at offset=%d partition=%d", m.Offset, partition)
			*skipped++
			continue
		}
		var d changelog.Delta
		if err := json.Unmarshal(m.Value, &d); err != nil {
			log.Printf("restore changelog: unmarshal error at offset=%d partition=%d: %v", m.Offset, partition, err)
			*skipped++
			continue
		}
		batch = append(batch, state.Delta{Key: d.Key, DeltaAmount: d.Delta, DeltaQty: d.DeltaQty, Seq: d.Seq})
		*bytes += int64(len(m.Value))
		if m.Offset > *lastOffset {
			*lastOffset = m.Offset
		}
		if len(batch) >= defaultReplayBatchSize || time.Since(lastFlush) >= defaultReplayFlushPeriod {
			prevApplied := *applied
			flush()
			if *applied > prevApplied && *applied%1000 == 0 {
				log.Printf("replay: partition %d applied %d messages (offset %d)", partition, *applied, m.Offset)
			}
		}
	}
}
