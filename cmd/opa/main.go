package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"time"

	"hpb/internal/model"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	var (
		bootstrap string
		groupID   string
		topicIn   string
		topicOut  string
		txID      string
		mode      string // normal|wrapper
		crashMode string // before|mid|after|none
	)
	flag.StringVar(&bootstrap, "bootstrap", "localhost:19092", "kafka bootstrap servers")
	flag.StringVar(&groupID, "group-id", "opa", "consumer group id")
	flag.StringVar(&topicIn, "topic-in", "p1.orders", "input topic")
	flag.StringVar(&topicOut, "topic-out", "p1.orders.enriched", "output topic")
	flag.StringVar(&txID, "tx-id", "opa-local-1", "transactional id")
	flag.StringVar(&mode, "mode", "normal", "normal|wrapper")
	flag.StringVar(&crashMode, "crash-mode", "none", "before|mid|after|none")
	flag.Parse()

	if mode == "wrapper" {
		runWrapper(bootstrap, groupID, txID)
		return
	}
	runOpA(bootstrap, groupID, topicIn, topicOut, txID, crashMode)
}

func runOpA(bootstrap, groupID, topicIn, topicOut, txID, crashMode string) {
	p, err := ck.NewProducer(&ck.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"enable.idempotence": true,
		"acks":               "all",
		"transactional.id":   txID,
	})
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer p.Close()

	c, err := ck.NewConsumer(&ck.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"group.id":           groupID,
		"enable.auto.commit": false,
		"isolation.level":    "read_committed",
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer c.Close()

	if err := c.SubscribeTopics([]string{topicIn}, nil); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	if err := p.InitTransactions(context.TODO()); err != nil {
		log.Fatalf("init tx: %v", err)
	}
	log.Printf("OpA started bootstrap=%s in=%s out=%s", bootstrap, topicIn, topicOut)

	for {
		if err := p.BeginTransaction(); err != nil {
			log.Fatalf("begin tx: %v", err)
		}

		msg, err := c.ReadMessage(10 * time.Second)
		if err != nil {
			_ = p.AbortTransaction(context.TODO())
			continue
		}

		var o model.Order
		if err := json.Unmarshal(msg.Value, &o); err != nil {
			_ = p.AbortTransaction(context.TODO())
			continue
		}
		eo := model.Normalize(o)
		val, _ := json.Marshal(eo)

		if err := p.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &topicOut, Partition: ck.PartitionAny}, Key: []byte(o.OrderID), Value: val}, nil); err != nil {
			_ = p.AbortTransaction(context.TODO())
			continue
		}

		// crash matrix simulation
		if crashMode == "before" {
			log.Fatal("crash before commit")
		}

		// SendOffsetsToTransaction binds consumer offsets atomically
		offsets, _ := c.Commit() // get current offsets synchronously (not committed to broker)
		meta, _ := c.GetConsumerGroupMetadata()
		if err := p.SendOffsetsToTransaction(context.Background(), offsets, meta); err != nil {
			_ = p.AbortTransaction(context.TODO())
			continue
		}

		if crashMode == "mid" {
			time.Sleep(2 * time.Second)
			log.Fatal("crash mid commit")
		}

		// Optionally flush pending deliveries before commit
		_ = p.Flush(5000)
		if err := p.CommitTransaction(context.TODO()); err != nil {
			_ = p.AbortTransaction(context.TODO())
			continue
		}

		if crashMode == "after" {
			log.Fatal("crash after commit")
		}
	}
}

func runWrapper(bootstrap, groupID, txID string) {
	in := "p1.orders.enriched"
	out := "p1.orders.output"

	p, err := ck.NewProducer(&ck.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"enable.idempotence": true,
		"acks":               "all",
		"transactional.id":   txID,
	})
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer p.Close()

	c, err := ck.NewConsumer(&ck.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"group.id":           groupID,
		"enable.auto.commit": false,
		// Allow switching between committed/uncommitted by separate tools; wrapper uses committed path
		"isolation.level":   "read_committed",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	defer c.Close()

	if err := c.SubscribeTopics([]string{in}, nil); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	if err := p.InitTransactions(context.TODO()); err != nil {
		log.Fatalf("init tx: %v", err)
	}
	log.Printf("OpA wrapper started bootstrap=%s in=%s out=%s", bootstrap, in, out)

	for {
		if err := p.BeginTransaction(); err != nil { // confluent lib uses internal context
			log.Fatalf("begin tx: %v", err)
		}
		msg, err := c.ReadMessage(5 * time.Second)
		if err != nil {
			_ = p.AbortTransaction(context.TODO())
			continue
		}

		// passthrough enriched -> output (key preserved)
		if err := p.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &out, Partition: ck.PartitionAny}, Key: msg.Key, Value: msg.Value}, nil); err != nil {
			_ = p.AbortTransaction(context.TODO())
			continue
		}

		// bind offsets atomically
		offsets, _ := c.Commit()
		meta, _ := c.GetConsumerGroupMetadata()
		if err := p.SendOffsetsToTransaction(context.Background(), offsets, meta); err != nil {
			_ = p.AbortTransaction(context.TODO())
			continue
		}
		if err := p.CommitTransaction(context.TODO()); err != nil {
			_ = p.AbortTransaction(context.TODO())
			continue
		}
	}
}
