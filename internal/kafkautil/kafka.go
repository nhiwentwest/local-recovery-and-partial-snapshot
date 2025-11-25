package kafkautil

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	kgo "github.com/segmentio/kafka-go"
)

// CollectChangelogOffsets returns high watermark (exclusive) per partition for a topic.
func CollectChangelogOffsets(bootstrap string, topic string) ([]int64, int, error) {
	c, err := ck.NewConsumer(&ck.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"group.id":           fmt.Sprintf("opb-offsets-%d", time.Now().UnixNano()),
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		return nil, 0, err
	}
	defer c.Close()
	md, err := c.GetMetadata(&topic, false, int((3 * time.Second).Milliseconds()))
	if err != nil {
		return nil, 0, err
	}
	tp, ok := md.Topics[topic]
	if !ok {
		return nil, 0, fmt.Errorf("topic not found: %s", topic)
	}
	parts := len(tp.Partitions)
	offs := make([]int64, parts)
	for i := 0; i < parts; i++ {
		_, high, err := c.QueryWatermarkOffsets(topic, int32(i), int((2 * time.Second).Milliseconds()))
		if err != nil {
			return nil, 0, fmt.Errorf("query watermark partition %d: %w", i, err)
		}
		offs[i] = high
	}
	return offs, parts, nil
}

// ChangelogHasBacklog compares current Kafka high watermarks with target offsets captured in the manifest.
// It returns true when at least one partition has advanced beyond the recorded offset.
func ChangelogHasBacklog(bootstrap string, topic string, target []int64) (bool, error) {
	offs, _, err := CollectChangelogOffsets(bootstrap, topic)
	if err != nil {
		return false, err
	}
	for i, high := range offs {
		var baseline int64
		if i < len(target) && target[i] > 0 {
			baseline = target[i]
		}
		if high > baseline {
			return true, nil
		}
	}
	return false, nil
}

// ScanDirtyKeysKafka scans Kafka from per-partition start offsets (exclusive) to end offsets (exclusive)
// on the given topic and returns the set of changed state keys (deduplicated).
func ScanDirtyKeysKafka(brokers []string, topic string, from []int64, to []int64, workers int, perReadTimeout time.Duration) ([]string, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers configured")
	}
	// Use a short initial context for setup, but the main logic will use its own timeout.
	ctxSetup, cancelSetup := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelSetup()
	conn, err := kgo.DialContext(ctxSetup, "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()
	parts, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("read partitions: %w", err)
	}
	// Dynamic timeout based on partition count, with a ceiling.
	estimatedTimeout := time.Duration(len(parts)) * 2 * time.Second
	if estimatedTimeout > 60*time.Second {
		estimatedTimeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), estimatedTimeout)
	defer cancel()

	if workers <= 0 {
		workers = len(parts)
		if workers > 8 {
			workers = 8
		}
	}
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	mu := &sync.Mutex{}
	set := make(map[string]struct{})
	for _, p := range parts {
		if p.Topic != topic {
			continue
		}
		pid := p.ID
		var start, end int64
		if int(pid) < len(from) {
			start = from[int(pid)]
		}
		if int(pid) < len(to) {
			end = to[int(pid)]
		}
		if end <= start {
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(pid int, start, end int64) {
			defer func() { <-sem; wg.Done() }()
			rd := kgo.NewReader(kgo.ReaderConfig{Brokers: brokers, Topic: topic, Partition: pid, MinBytes: 1, MaxBytes: 10e6})
			defer rd.Close()
			if err := rd.SetOffset(start); err != nil {
				log.Printf("scan: partition %d set offset error: %v", pid, err)
				return
			}
			localKeys := make(map[string]struct{})
			lastLog := time.Now()
			var currentOffset int64
			for {
				ctxR, cancelR := context.WithTimeout(ctx, perReadTimeout)
				m, err := rd.ReadMessage(ctxR)
				cancelR()
				if err != nil {
					if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
						log.Printf("scan: partition %d timed out", pid)
					}
					break
				}
				currentOffset = m.Offset
				// Progress logging
				if time.Since(lastLog) > 10*time.Second {
					log.Printf("scan: partition %d progress: offset %d/%d", pid, currentOffset, end)
					lastLog = time.Now()
				}
				key := string(m.Key)
				// Optimization: assume producer always sets Kafka message key for changelog; avoid JSON unmarshal fallback
				// to reduce CPU during dirty-keys scan for delta snapshots.
				// If key is empty, we skip the message.
				if key != "" {
					localKeys[key] = struct{}{}
				}
				if m.Offset >= end-1 {
					break
				}
			}
			// Batch apply to shared map
			if len(localKeys) > 0 {
				mu.Lock()
				for k := range localKeys {
					set[k] = struct{}{}
				}
				mu.Unlock()
			}
		}(int(pid), start, end)
	}
	wg.Wait()
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	return keys, nil
}

