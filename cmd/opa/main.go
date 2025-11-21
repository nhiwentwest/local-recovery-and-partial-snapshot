package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"hpb/internal/model"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// maybeAttachT0 ensures a t0 header is present for latency tracking.
func maybeAttachT0(headers []ck.Header) []ck.Header {
	for _, h := range headers {
		if h.Key == "t0" {
			return headers // t0 already exists
		}
	}
	t0 := []byte(fmt.Sprintf("%d", time.Now().UnixNano()))
	return append(headers, ck.Header{Key: "t0", Value: t0})
}

func main() {
	var (
		bootstrap string
		groupID   string
		topicIn   string
		topicOut  string
		txID      string
		crashMode string // before|mid|after|none
		httpAddr  string
	)
	flag.StringVar(&bootstrap, "bootstrap", "localhost:9092", "kafka bootstrap servers")
	flag.StringVar(&groupID, "group-id", "opa-pipeline", "consumer group id")
	flag.StringVar(&topicIn, "topic-in", "p1.orders", "input topic")
	flag.StringVar(&topicOut, "topic-out", "p1.orders.enriched", "output topic")
	flag.StringVar(&txID, "tx-id", "opa-local-1", "transactional id")
	flag.StringVar(&crashMode, "crash-mode", "none", "before|mid|after|none")
	flag.StringVar(&httpAddr, "http", ":8088", "http listen address for metrics/health")
	flag.Parse()

	// metrics registry for OpA
	opaTxProduced := prometheus.NewCounter(prometheus.CounterOpts{Name: "opa_tx_produced_total"})
	opaTxAborted := prometheus.NewCounter(prometheus.CounterOpts{Name: "opa_tx_aborted_total"})
	opaTxLatency := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "opa_tx_latency_seconds", Buckets: prometheus.DefBuckets})
	opaReg := prometheus.NewRegistry()
	opaReg.MustRegister(opaTxProduced, opaTxAborted, opaTxLatency)

	// start metrics HTTP
	go func() {
		http.Handle("/metrics", promhttp.HandlerFor(opaReg, promhttp.HandlerOpts{}))
		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprint(w, "ok") })
		_ = http.ListenAndServe(httpAddr, nil)
	}()

	runOpA(bootstrap, groupID, topicIn, topicOut, txID, crashMode, opaTxProduced, opaTxAborted, opaTxLatency)
}

func runOpA(bootstrap, groupID, topicIn, topicOut, txID, crashMode string, txProduced prometheus.Counter, txAborted prometheus.Counter, txLatency prometheus.Histogram) {
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
		"isolation.level":    "read_uncommitted",
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

	const batchSize = 100
	const batchTimeout = 5 * time.Second

	for {
		// Bắt đầu một transaction mới cho mỗi batch
		if err := p.BeginTransaction(); err != nil {
			log.Printf("begin tx error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		batch := make([]*ck.Message, 0, batchSize)
		batchStartTime := time.Now()
		// Theo dõi offset cao nhất +1 cho mỗi partition của topic input
		batchOffsets := make(map[int32]ck.TopicPartition)

		// Đọc message cho đến khi đủ batch hoặc timeout
		for len(batch) < batchSize && time.Since(batchStartTime) < batchTimeout {
			readTimeout := batchTimeout - time.Since(batchStartTime)
			if readTimeout < 0 {
				readTimeout = 0
			}
			msg, err := c.ReadMessage(readTimeout)
			if err != nil {
				if e, ok := err.(ck.Error); ok && e.Code() == ck.ErrTimedOut {
					break // Hết thời gian chờ, xử lý batch hiện tại
				}
				log.Printf("ReadMessage error: %v", err)
				break
			}
			batch = append(batch, msg)
		}

		// Nếu không có message nào, bỏ qua và bắt đầu transaction mới
		if len(batch) == 0 {
			_ = p.AbortTransaction(context.TODO()) // Hủy transaction rỗng
			continue
		}

		log.Printf("Processing batch of %d messages...", len(batch))
		var produceErr error
		for _, msg := range batch {
			var o model.Order
			if err := json.Unmarshal(msg.Value, &o); err != nil {
				log.Printf("json.Unmarshal error: %v for message: %s", err, string(msg.Value))
				continue // Bỏ qua message lỗi, không tính vào offsets
			}
			eo := model.Normalize(o)
			val, _ := json.Marshal(eo)


			headers := maybeAttachT0(msg.Headers)
			if err := p.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &topicOut, Partition: ck.PartitionAny}, Key: []byte(o.OrderID), Value: val, Headers: headers}, nil); err != nil {
				log.Printf("produce error: %v", err)
				produceErr = err
				break
			}
			// Track highest offset+1 per partition cho offsets giao dịch
			tp := ck.TopicPartition{Topic: msg.TopicPartition.Topic, Partition: msg.TopicPartition.Partition, Offset: msg.TopicPartition.Offset + 1}
			if existing, ok := batchOffsets[tp.Partition]; !ok || tp.Offset > existing.Offset {
				batchOffsets[tp.Partition] = tp
			}
		}

		if produceErr != nil {
			_ = p.AbortTransaction(context.TODO())
			continue
		}

		// Chuẩn bị offsets để gửi trong transaction
		offsets := make([]ck.TopicPartition, 0, len(batchOffsets))
		for _, tp := range batchOffsets {
			offsets = append(offsets, tp)
		}
		meta, err := c.GetConsumerGroupMetadata()
		if err != nil {
			log.Printf("get metadata error: %v", err)
			_ = p.AbortTransaction(context.TODO())
			continue
		}

		t0 := time.Now()
		if err := p.SendOffsetsToTransaction(context.Background(), offsets, meta); err != nil {
			log.Printf("send offsets error: %v", err)
			_ = p.AbortTransaction(context.TODO())
			continue
		}

		// Commit transaction
		if err := p.CommitTransaction(context.TODO()); err != nil {
			log.Printf("CommitTransaction error: %v", err)
			txAborted.Inc()
			_ = p.AbortTransaction(context.TODO())
			continue
		}
		txProduced.Inc()
		txLatency.Observe(time.Since(t0).Seconds())
		log.Printf("Committed batch of %d messages successfully.", len(batch))
	}
}
