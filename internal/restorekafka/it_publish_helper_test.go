//go:build integration
// +build integration

package restorekafka

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"

	"hpb/internal/changelog"
)

func publishDeltas(brokers []string, topic string, records []changelog.Delta) error {
	w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.LeastBytes{}}
	defer w.Close()
	msgs := make([]kafka.Message, 0, len(records))
	for _, d := range records {
		b, _ := json.Marshal(d)
		msgs = append(msgs, kafka.Message{Key: []byte(d.Key), Value: b, Time: time.Now()})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return w.WriteMessages(ctx, msgs...)
}
