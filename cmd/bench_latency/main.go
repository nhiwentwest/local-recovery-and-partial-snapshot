package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// murmur2 matches Kafka Java Murmur2 used by default partitioner
func murmur2(data []byte) uint32 {
	const seed uint32 = 0x9747b28c
	const m uint32 = 0x5bd1e995
	const r uint32 = 24
	length := uint32(len(data))
	h := seed ^ length
	i := 0
	for length >= 4 {
		k := uint32(data[i+0]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
		i += 4
		length -= 4
	}
	switch length {
	case 3:
		h ^= uint32(data[i+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[i+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[i])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

func partitionForKey(key []byte, partitions int) int32 {
	if partitions <= 0 {
		return 0
	}
	h := murmur2(key)
	pos := (h & 0x7fffffff) % uint32(partitions)
	return int32(pos)
}

func metadataPartitionCount(c *ck.Consumer, topic string, timeout time.Duration) int {
	md, err := c.GetMetadata(&topic, false, int(timeout/time.Millisecond))
	if err != nil {
		return 1
	}
	if ti, ok := md.Topics[topic]; ok {
		return len(ti.Partitions)
	}
	return 1
}

// produceBurstEvents produces a burst of enriched events for the same key.
func produceBurstEvents(prod *ck.Producer, topicIn, store, pid string, ws int64, bursts, sleepMs int, sampleNum int) time.Time {
	t0send := time.Now()
	topic := topicIn
	for b := 0; b < bursts; b++ {
		value := fmt.Sprintf(`{"orderId":"lt-%d-%d-%d","productId":"%s","price":9000,"qty":1,"storeId":"%s","ts":%d,"validated":true,"normTs":%d}`,
			sampleNum, b, time.Now().UnixNano(), pid, store, ws+1, ws+1)
		delivery := make(chan ck.Event, 1)
		err := prod.Produce(&ck.Message{
			TopicPartition: ck.TopicPartition{Topic: &topic, Partition: ck.PartitionAny},
			Key:            []byte(fmt.Sprintf("%s#%s", store, pid)),
			Value:          []byte(value),
			Headers:        []ck.Header{{Key: "t0", Value: []byte(fmt.Sprintf("%d", time.Now().UnixNano()))}},
		}, delivery)
		if err != nil {
			log.Fatalf("produce: %v", err)
		}
		select {
		case ev := <-delivery:
			m := ev.(*ck.Message)
			if m.TopicPartition.Error != nil {
				log.Fatalf("delivery error: %v", m.TopicPartition.Error)
			}
		case <-time.After(5 * time.Second):
			log.Fatalf("delivery timeout")
		}
		if sleepMs > 0 {
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		}
	}
	return t0send
}

// measureLatency polls for messages and measures latency until deadline or hit.
func measureLatency(cons *ck.Consumer, measureTopic, keyOut string, part int32, t0send time.Time, deadline time.Time) (time.Duration, bool) {
	for time.Now().Before(deadline) {
		ev := cons.Poll(200)
		if ev == nil {
			continue
		}
		switch m := ev.(type) {
		case *ck.Message:
			if m.TopicPartition.Topic != nil {
				if strings.EqualFold(*m.TopicPartition.Topic, measureTopic) && string(m.Key) == keyOut {
					// prefer t1-t0 from headers if available
					var lat time.Duration
					var have bool
					var t0h, t1h []byte
					for _, h := range m.Headers {
						if h.Key == "t0" {
							t0h = h.Value
						}
						if h.Key == "t1" {
							t1h = h.Value
						}
					}
					if len(t0h) > 0 && len(t1h) > 0 {
						var t0v, t1v int64
						_, _ = fmt.Sscan(string(t0h), &t0v)
						_, _ = fmt.Sscan(string(t1h), &t1v)
						if t1v > 0 && t0v > 0 {
							lat = time.Duration(t1v - t0v)
							have = true
						}
					}
					if !have {
						lat = time.Since(t0send)
					}
					return lat, true
				}
			}
		case ck.Error:
			// ignore transient
		default:
			// ignore
		}
	}
	return 0, false
}

// calculatePercentiles calculates p50, p95, p99 from latency measurements.
func calculatePercentiles(lats []time.Duration) (p50, p95, p99 time.Duration) {
	// sort latencies (simple bubble sort)
	sorted := make([]time.Duration, len(lats))
	copy(sorted, lats)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	p := func(q float64) time.Duration {
		idx := int(float64(len(sorted)-1) * q)
		if idx < 0 {
			idx = 0
		}
		if idx > len(sorted)-1 {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}
	return p(0.50), p(0.95), p(0.99)
}

func main() {
	var (
		bootstrap    string
		topicIn      string
		topicOut     string
		measureTopic string
		store        string
		windowSize   int
		samples      int
		productPr    string
		bursts       int
		sleepMs      int
	)
	flag.StringVar(&bootstrap, "bootstrap", "127.0.0.1:9092", "kafka bootstrap")
	flag.StringVar(&topicIn, "topic-in", "p2.orders.enriched", "input topic (enriched)")
	flag.StringVar(&topicOut, "topic-out", "p2.orders.output", "output topic")
	flag.StringVar(&measureTopic, "measure-topic", "", "topic to measure latency on (default: topic-out; set to p2.opb-changelog for stable hits)")
	flag.StringVar(&store, "store", "A", "store id")
	flag.IntVar(&windowSize, "window", 10, "window size seconds")
	flag.IntVar(&samples, "n", 5, "number of samples")
	flag.StringVar(&productPr, "pid-prefix", "pL", "product id prefix")
	flag.IntVar(&bursts, "bursts", 3, "events per sample for same key")
	flag.IntVar(&sleepMs, "sleep-ms", 200, "sleep between burst events (ms)")
	flag.Parse()

	if measureTopic == "" {
		measureTopic = topicOut
	}

	prod, err := ck.NewProducer(&ck.ConfigMap{
		"bootstrap.servers": bootstrap,
		"linger.ms":         0,
		"compression.type":  "lz4",
	})
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer prod.Close()

	cons, err := ck.NewConsumer(&ck.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"group.id":           fmt.Sprintf("bench-lat-%d", time.Now().UnixNano()),
		"enable.auto.commit": false,
		"isolation.level":    "read_committed",
	})
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer cons.Close()

	// prepare partition count for measurement topic
	parts := metadataPartitionCount(cons, measureTopic, 5*time.Second)
	log.Printf("measure topic=%s partitions=%d", measureTopic, parts)

	var lats []time.Duration
	for i := 1; i <= samples; i++ {
		pid := fmt.Sprintf("%s%d", productPr, i)
		// compute windowStart based on current time (current window)
		now := time.Now().UTC().Unix()
		ws := (now / int64(windowSize)) * int64(windowSize)
		keyOut := fmt.Sprintf("%s#%s#%d", store, pid, ws)

		// pin consumer to partition for this key, start at end (on measureTopic)
		part := partitionForKey([]byte(keyOut), parts)
		err = cons.Assign([]ck.TopicPartition{{Topic: &measureTopic, Partition: part, Offset: ck.OffsetEnd}})
		if err != nil {
			log.Fatalf("assign: %v", err)
		}

		// produce burst of enriched events for same key (ws+1)
		t0send := produceBurstEvents(prod, topicIn, store, pid, ws, bursts, sleepMs, i)

		deadline := time.Now().Add(30 * time.Second)
		lat, hit := measureLatency(cons, measureTopic, keyOut, part, t0send, deadline)
		if hit {
			lats = append(lats, lat)
			log.Printf("sample %d: HIT key=%s part=%d in %v", i, keyOut, part, lat)
		} else {
			log.Printf("sample %d: MISS key=%s part=%d", i, keyOut, part)
		}
		_ = cons.Unassign()
	}

	// summarize
	if len(lats) == 0 {
		fmt.Println("Latency: all MISS")
		os.Exit(1)
	}
	p50, p95, p99 := calculatePercentiles(lats)
	fmt.Printf("Latency: p50=%v p95=%v p99=%v (n=%d)\n", p50, p95, p99, len(lats))
}
