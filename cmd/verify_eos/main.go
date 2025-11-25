package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"net/http"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func consumeCount(bootstrap, groupID, topic, isolation string, timeout time.Duration) (int, error) {
	c, err := ck.NewConsumer(&ck.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"group.id":           groupID,
		"enable.auto.commit": false,
		"isolation.level":    isolation,
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		return 0, err
	}
	defer c.Close()
	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		return 0, err
	}
	end := time.Now().Add(timeout)
	n := 0
	for time.Now().Before(end) {
		msg, err := c.ReadMessage(200 * time.Millisecond)
		if err != nil {
			if err.(ck.Error).Code() == ck.ErrTimedOut {
				continue
			}
			// ignore other transient
			continue
		}
		_ = msg
		n++
	}
	return n, nil
}

func consumeCountByKey(bootstrap, groupID, topic, isolation string, timeout time.Duration) (map[string]int, error) {
	c, err := ck.NewConsumer(&ck.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"group.id":           groupID,
		"enable.auto.commit": false,
		"isolation.level":    isolation,
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		return nil, err
	}
	defer c.Close()
	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		return nil, err
	}
	end := time.Now().Add(timeout)
	m := make(map[string]int)
	for time.Now().Before(end) {
		msg, err := c.ReadMessage(200 * time.Millisecond)
		if err != nil {
			if err.(ck.Error).Code() == ck.ErrTimedOut {
				continue
			}
			continue
		}
		if msg.Key != nil {
			m[string(msg.Key)]++
		}
	}
	return m, nil
}

func main() {
	var (
		bootstrap string
		topic     string
		groupBase string
		durSec    int
		httpAddr  string
		perKey    bool
	)
	flag.StringVar(&bootstrap, "bootstrap", "localhost:9092", "kafka bootstrap")
	flag.StringVar(&topic, "topic", "p1.orders.enriched", "topic to verify")
	flag.StringVar(&groupBase, "group", "verify-eos", "group id base")
	flag.IntVar(&durSec, "duration", 5, "seconds to read")
	flag.StringVar(&httpAddr, "http", ":9109", "http listen for /metrics")
	flag.BoolVar(&perKey, "per-key", false, "compute eos_gap by key (sum of positive gaps)")
	flag.Parse()

	// Prom registry for eos_gap
	eosGap := prometheus.NewGauge(prometheus.GaugeOpts{Name: "verify_eos_gap"})
	reg := prometheus.NewRegistry()
	_ = reg.Register(eosGap)
	go func() {
		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		_ = http.ListenAndServe(httpAddr, nil)
	}()

	window := time.Duration(durSec) * time.Second
	log.Printf("verifying topic=%s window=%s", topic, window)
	committed, err := consumeCount(bootstrap, groupBase+"-rc", topic, "read_committed", window)
	if err != nil {
		log.Fatalf("read_committed err: %v", err)
	}
	uncommitted, err := consumeCount(bootstrap, groupBase+"-ru", topic, "read_uncommitted", window)
	if err != nil {
		log.Fatalf("read_uncommitted err: %v", err)
	}
	fmt.Printf("read_committed=%d read_uncommitted=%d\n", committed, uncommitted)
	if !perKey {
		gap := uncommitted - committed
		if gap < 0 {
			gap = 0
		}
		eosGap.Set(float64(gap))
	} else {
		rcMap, err := consumeCountByKey(bootstrap, groupBase+"-rc-map", topic, "read_committed", window)
		if err != nil {
			log.Fatalf("map read_committed err: %v", err)
		}
		ruMap, err := consumeCountByKey(bootstrap, groupBase+"-ru-map", topic, "read_uncommitted", window)
		if err != nil {
			log.Fatalf("map read_uncommitted err: %v", err)
		}
		var total int
		for k, u := range ruMap {
			c := rcMap[k]
			if u > c {
				total += (u - c)
			}
		}
		eosGap.Set(float64(total))
	}
	if committed > uncommitted {
		log.Printf("WARN: committed>%d > uncommitted>%d (unexpected)", committed, uncommitted)
	}
}
