package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	var (
		bootstrap string
		topic     string
		group     string
		seconds   int
		target    int
	)
	flag.StringVar(&bootstrap, "bootstrap", "127.0.0.1:9092", "kafka bootstrap")
	flag.StringVar(&topic, "topic", "p2.opb-changelog", "topic to count")
	flag.StringVar(&group, "group", "count-changelog", "consumer group id")
	flag.IntVar(&seconds, "seconds", 70, "max count window seconds (ignored if -target reached)")
	flag.IntVar(&target, "target", 0, "stop after consuming N messages (0=disabled)")
	flag.Parse()

	cons, err := ck.NewConsumer(&ck.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"group.id":           fmt.Sprintf("%s-%d", group, time.Now().UnixNano()),
		"enable.auto.commit": false,
		"isolation.level":    "read_committed",
		"auto.offset.reset":  "latest",
	})
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer cons.Close()

	if err := cons.SubscribeTopics([]string{topic}, nil); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	var count int64
	var t0 time.Time
	for time.Now().Before(deadline) {
		ev := cons.Poll(200)
		if ev == nil {
			continue
		}
		switch m := ev.(type) {
		case *ck.Message:
			_ = m
			if t0.IsZero() {
				t0 = time.Now()
			}
			count++
			if target > 0 && int(count) >= target {
				deadline = time.Now() // break outer loop
			}
		case ck.Error:
			// ignore
		default:
		}
	}
	var durSec float64
	if !t0.IsZero() {
		durSec = time.Since(t0).Seconds()
	} else {
		durSec = float64(seconds)
	}
	if durSec <= 0 {
		durSec = 1
	}
	rate := float64(count) / durSec
	fmt.Printf("count=%d rate=%.2f msgs/s duration=%.2fs\n", count, rate, durSec)
}
