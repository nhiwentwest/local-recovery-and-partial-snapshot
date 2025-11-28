package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"hpb/internal/opb"
)

func main() {
	// Backward-compatible defaults
	var (
		mode       string
		count      int
		outputFile string
		bootstrap  string
		topic      string
		stores     int
		products   int
		nPerKey    int
		windowSize int
		lingerMs   int
	)

	flag.StringVar(&mode, "mode", "kafka", "output mode: kafka|file")
	flag.IntVar(&count, "count", 100, "number of orders to generate (file mode)")
	flag.StringVar(&outputFile, "output", "p1.orders.enriched.jsonl", "output file (file mode)")
	flag.StringVar(&bootstrap, "bootstrap", "127.0.0.1:9092", "Kafka bootstrap (kafka mode)")
	flag.StringVar(&topic, "topic", "p1.orders.enriched", "Kafka topic (kafka mode)")
	flag.IntVar(&stores, "stores", 200, "number of stores (kafka mode)")
	flag.IntVar(&products, "products", 1000, "number of products per store (kafka mode)")
	flag.IntVar(&nPerKey, "n-per-key", 1, "events per (store,product,ws) key (kafka mode)")
	flag.IntVar(&windowSize, "window-size", 3600, "window size seconds for NormTS (kafka mode)")
	flag.IntVar(&lingerMs, "linger-ms", 10, "producer linger.ms (kafka mode)")
	flag.Parse()

	switch mode {
	case "file":
		if err := generateFile(count, outputFile); err != nil {
			log.Fatalf("generation failed: %v", err)
		}
		log.Printf("generated %d orders to %s", count, outputFile)
		return
	case "kafka":
		if err := generateKafka(bootstrap, topic, stores, products, nPerKey, windowSize, lingerMs); err != nil {
			log.Fatalf("kafka publish failed: %v", err)
		}
		return
	default:
		log.Fatalf("unknown mode: %s (use kafka|file)", mode)
	}
}

// generateFile keeps legacy behavior: write NDJSON of OrderEnriched values only.
func generateFile(count int, outputFile string) error {
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer file.Close()

	stores := []string{"A", "B", "C"}
	products := []string{"p1", "p2", "p3", "p4", "p5"}

	baseTime := time.Now().UTC().Unix()
	rand.Seed(time.Now().UnixNano())

	enc := json.NewEncoder(file)
	for i := 0; i < count; i++ {
		order := opb.OrderEnriched{
			OrderID:   fmt.Sprintf("o%d", i+1),
			ProductID: products[rand.Intn(len(products))],
			Price:     int64(1000 + rand.Intn(9000)),
			Qty:       int64(1 + rand.Intn(5)),
			StoreID:   stores[rand.Intn(len(stores))],
			TS:        baseTime + int64(i*10),
			Validated: true,
			NormTS:    baseTime + int64(i*10),
		}
		if err := enc.Encode(&order); err != nil {
			return fmt.Errorf("encode order %d: %w", i+1, err)
		}
	}
	return nil
}

// generateKafka publishes high-cardinality OrderEnriched events directly to Kafka.
func generateKafka(bootstrap, topic string, stores, products, nPerKey, windowSize, lingerMs int) error {
	if stores <= 0 || products <= 0 || nPerKey <= 0 {
		return fmt.Errorf("invalid params: stores, products, n-per-key must be > 0")
	}
	if windowSize <= 0 {
		windowSize = 3600
	}
	cfg := &ck.ConfigMap{
		"bootstrap.servers":            bootstrap,
		"linger.ms":                    lingerMs,
		"compression.type":             "lz4",
		"acks":                         "1",
		"queue.buffering.max.messages": 500000,
		"queue.buffering.max.kbytes":   524288, // 512MB
		"queue.buffering.max.ms":       1000,
	}
	p, err := ck.NewProducer(cfg)
	if err != nil {
		return fmt.Errorf("producer init: %w", err)
	}
	defer p.Close()

	// Delivery reporter (non-blocking)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *ck.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("deliver error: %v", ev.TopicPartition.Error)
				}
			}
		}
	}()

	now := time.Now().Unix()
	ws := (now / int64(windowSize)) * int64(windowSize)

	total := 0
	start := time.Now()
	for i := 1; i <= stores; i++ {
		storeID := fmt.Sprintf("RECOVERY-%04d", i)
		for j := 1; j <= products; j++ {
			prodID := fmt.Sprintf("p%04d", j)
			key := fmt.Sprintf("%s#%s#%d", storeID, prodID, ws)
			for n := 0; n < nPerKey; n++ {
				ord := opb.OrderEnriched{
					OrderID:   fmt.Sprintf("%s-%s-%d-%d", storeID, prodID, ws, n),
					ProductID: prodID,
					Price:     10000,
					Qty:       1,
					StoreID:   storeID,
					TS:        ws,
					Validated: true,
					NormTS:    ws,
				}
				val, _ := json.Marshal(ord)
				msg := &ck.Message{TopicPartition: ck.TopicPartition{Topic: &topic, Partition: ck.PartitionAny}, Key: []byte(key), Value: val}
				for {
					if err := p.Produce(msg, nil); err != nil {
						if ke, ok := err.(ck.Error); ok && ke.Code() == ck.ErrQueueFull {
							log.Printf("producer queue full after %d events, flushing...", total)
							p.Flush(10_000)
							continue
						}
						return fmt.Errorf("produce: %w", err)
					}
					break
				}
				total++
			}
		}
	}
	if remaining := p.Flush(60_000); remaining > 0 {
		return fmt.Errorf("producer flush timeout: %d message(s) still pending", remaining)
	}
	log.Printf("published %d events to %s (stores=%d products=%d nPerKey=%d) in %s", total, topic, stores, products, nPerKey, time.Since(start))
	return nil
}
