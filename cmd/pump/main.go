package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Order struct {
	OrderID   string `json:"orderId"`
	ProductID string `json:"productId"`
	Price     int64  `json:"price"`
	Qty       int64  `json:"qty"`
	StoreID   string `json:"storeId"`
	TS        int64  `json:"ts"`
}

type OrderEnriched struct {
	OrderID   string `json:"orderId"`
	ProductID string `json:"productId"`
	Price     int64  `json:"price"`
	Qty       int64  `json:"qty"`
	StoreID   string `json:"storeId"`
	TS        int64  `json:"ts"`
	Validated bool   `json:"validated"`
	NormTS    int64  `json:"normTs"`
}

func windowStart(ts int64, windowSize int64) int64 { return (ts / windowSize) * windowSize }

func main() {
	var (
		bootstrap   string
		topic       string
		mode        string // raw|enriched
		n           int
		parallel    int
		storesCSV   string
		lingerMs    int
		compression string
		startIdx    int
		// EOS deterministic flags
		eosStore       string
		eosWindowStart int64
		eosCount       int
		windowSize     int
	)
	flag.StringVar(&bootstrap, "bootstrap", "127.0.0.1:9092", "Kafka bootstrap servers")
	flag.StringVar(&topic, "topic", "p1.orders", "Kafka topic")
	flag.StringVar(&mode, "mode", "raw", "payload mode: raw|enriched")
	flag.IntVar(&n, "n", 1000, "number of messages")
	flag.IntVar(&parallel, "parallel", 1, "number of parallel workers")
	flag.StringVar(&storesCSV, "stores", "A-,B-,C-,D-,E-,F-,G-,H-,I-,J-", "comma-separated store prefixes")
	flag.IntVar(&lingerMs, "linger.ms", 5, "producer linger.ms")
	flag.StringVar(&compression, "compression", "lz4", "producer compression type")
	flag.IntVar(&startIdx, "start-idx", 1, "starting index offset for generated order IDs (>=1)")
	flag.StringVar(&eosStore, "eos-store", "", "if set, generate deterministic EOS events for this store prefix")
	flag.Int64Var(&eosWindowStart, "eos-window-start", 0, "unix seconds window start for EOS test")
	flag.IntVar(&eosCount, "eos-count", 0, "number of EOS deterministic events to produce")
	flag.IntVar(&windowSize, "window-size", 60, "window size seconds (for enriched keys and EOS ws)")
	flag.Parse()

	p, err := ck.NewProducer(&ck.ConfigMap{
		"bootstrap.servers": bootstrap,
		"linger.ms":         lingerMs,
		"compression.type":  compression,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "producer error: %v\n", err)
		os.Exit(1)
	}
	defer p.Close()

	stores := strings.Split(storesCSV, ",")
	rand.Seed(time.Now().UnixNano())

	deliveries := make(chan ck.Event, n)
	var wg sync.WaitGroup

	send := func(idxStart, idxEnd int) {
		defer wg.Done()
		for i := idxStart; i <= idxEnd; i++ {
			var key []byte
			var val []byte
			if eosStore != "" && eosCount > 0 && eosWindowStart > 0 {
				// deterministic EOS events 1..eosCount for a fixed store and window
				if i > eosCount {
					return
				}
				prodID := fmt.Sprintf("p%d", i)
				ordID := fmt.Sprintf("eos-%d", i)
				ts := eosWindowStart + int64(i)
				if mode == "raw" {
					o := Order{OrderID: ordID, ProductID: prodID, Price: 10000, Qty: 1, StoreID: eosStore, TS: ts}
					b, _ := json.Marshal(o)
					val = b
					key = nil
				} else {
					ws := int64(eosWindowStart)
					key = []byte(fmt.Sprintf("%s#%s#%d", eosStore, prodID, ws))
					o := OrderEnriched{OrderID: ordID, ProductID: prodID, Price: 10000, Qty: 1, StoreID: eosStore, TS: ts, Validated: true, NormTS: ts}
					b, _ := json.Marshal(o)
					val = b
				}
			} else {
				// random load
				store := stores[rand.Intn(len(stores))]
				prodID := fmt.Sprintf("p%d", (i-1)%100+1)
				ts := time.Now().Unix()
				price := int64(5000 + rand.Intn(15000))
				qty := int64(1 + rand.Intn(5))
				ordID := fmt.Sprintf("rand-%d", i)
				if mode == "raw" {
					o := Order{OrderID: ordID, ProductID: prodID, Price: price, Qty: qty, StoreID: store, TS: ts}
					b, _ := json.Marshal(o)
					val = b
					key = nil
				} else {
					ws := windowStart(ts, int64(windowSize))
					key = []byte(fmt.Sprintf("%s#%s#%d", store, prodID, ws))
					o := OrderEnriched{OrderID: ordID, ProductID: prodID, Price: price, Qty: qty, StoreID: store, TS: ts, Validated: true, NormTS: ts}
					b, _ := json.Marshal(o)
					val = b
				}
			}
			m := &ck.Message{TopicPartition: ck.TopicPartition{Topic: &topic, Partition: ck.PartitionAny}, Key: key, Value: val}
			if err := p.Produce(m, deliveries); err != nil {
				fmt.Fprintf(os.Stderr, "produce error: %v\n", err)
			}
		}
	}

	if parallel < 1 {
		parallel = 1
	}
	if startIdx < 1 {
		startIdx = 1
	}
	per := (n + parallel - 1) / parallel
	maxEnd := startIdx + n - 1
	current := startIdx
	for w := 0; w < parallel; w++ {
		if current > maxEnd {
			break
		}
		s := current
		e := s + per - 1
		if e > maxEnd {
			e = maxEnd
		}
		wg.Add(1)
		go send(s, e)
		current += per
	}

	// Drain delivery reports
	go func() {
		for ev := range deliveries {
			sw := ev.(ck.Event)
			_ = sw // can be expanded to check errors if needed
		}
	}()

	wg.Wait()
	p.Flush(15_000)
	close(deliveries)
	fmt.Printf("done: produced %d messages to %s in %s mode\n", n, topic, mode)
}
