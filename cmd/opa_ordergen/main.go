package main

import (
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Order struct {
	OrderID   string `json:"orderId,omitempty"`
	ProductID string `json:"productId,omitempty"`
	Price     int64  `json:"price,omitempty"`
	Qty       int64  `json:"qty,omitempty"`
	StoreID   string `json:"storeId,omitempty"`
	TS        int64  `json:"ts,omitempty"`
}

func main() {
	var bootstrap string
	var topic string
	var count int
	var failRatio float64
	flag.StringVar(&bootstrap, "bootstrap", "localhost:9092", "Kafka bootstrap servers")
	flag.StringVar(&topic, "topic", "p1.orders", "Target Kafka topic")
	flag.IntVar(&count, "count", 20, "Number of messages to produce")
	flag.Float64Var(&failRatio, "fail-ratio", 0.3, "Fraction of bad messages [0.0-1.0]")
	flag.Parse()

	p, err := ck.NewProducer(&ck.ConfigMap{
		"bootstrap.servers": bootstrap,
	})
	if err != nil {
		log.Fatalf("producer: %v", err)
	}
	defer p.Close()

	rand.Seed(time.Now().UnixNano())
	stores := []string{"A", "B", "C"}
	products := []string{"p1", "p2", "p3"}

	for i := 0; i < count; i++ {
		isBad := rand.Float64() < failRatio
		var val []byte

		if isBad {
			// 30% chance: malformed or missing info
			switch rand.Intn(3) {
			case 0:
				// Missing orderId
				o := Order{
					ProductID: products[rand.Intn(len(products))],
					Price:     1000 + int64(rand.Intn(9000)),
					Qty:       1 + int64(rand.Intn(5)),
					StoreID:   stores[rand.Intn(len(stores))],
					TS:        time.Now().Unix(),
				}
				val, _ = json.Marshal(o)
			case 1:
				// Invalid JSON
				val = []byte("{broken_json:true,missing_quotes}")
			case 2:
				// Negative price or qty
				o := Order{
					OrderID:   "o" + randomID(),
					ProductID: products[rand.Intn(len(products))],
					Price:     -999,
					Qty:       -1,
					StoreID:   stores[rand.Intn(len(stores))],
					TS:        time.Now().Unix(),
				}
				val, _ = json.Marshal(o)
			}
			log.Printf("sending BAD order %d", i+1)
		} else {
			// Normal valid order
			o := Order{
				OrderID:   "o" + randomID(),
				ProductID: products[rand.Intn(len(products))],
				Price:     1000 + int64(rand.Intn(9000)),
				Qty:       1 + int64(rand.Intn(5)),
				StoreID:   stores[rand.Intn(len(stores))],
				TS:        time.Now().Unix(),
			}
			val, _ = json.Marshal(o)
			log.Printf("sending GOOD order %d", i+1)
		}

		var keyBytes []byte
// try to set key = orderId if JSON is valid and present
var tmp Order
if json.Unmarshal(val, &tmp) == nil && tmp.OrderID != "" {
    keyBytes = []byte(tmp.OrderID)
} else {
    keyBytes = []byte("key-" + randomID())
}
p.Produce(&ck.Message{
    TopicPartition: ck.TopicPartition{Topic: &topic, Partition: int32(ck.PartitionAny)},
    Key:            keyBytes,
    Value:          val,
}, nil)


		time.Sleep(500 * time.Millisecond)
	}

	p.Flush(5000)
	log.Printf("Done generating %d messages (%.0f%% bad)", count, failRatio*100)
}

func randomID() string {
	return string('A'+rune(rand.Intn(26))) + time.Now().Format("150405.000")
}
