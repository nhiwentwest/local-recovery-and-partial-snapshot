package opa

import (
	"context"
	"fmt"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

//go:generate go run github.com/golang/mock/mockgen -source=tx.go -destination=mocks/mock_tx.go -package=mocks

// TxProducer defines the interface for a transactional Kafka producer.
type TxProducer interface {
	BeginTransaction() error
	Produce(*ck.Message, chan ck.Event) error
	SendOffsetsToTransaction(ctx context.Context, partitions []ck.TopicPartition, metadata *ck.ConsumerGroupMetadata) error
	CommitTransaction(ctx context.Context) error
	AbortTransaction(ctx context.Context) error
}

// ConsumerOffsets describes the minimal consumer API we need.
type ConsumerOffsets interface {
	Commit() ([]ck.TopicPartition, error)
	GetConsumerGroupMetadata() (*ck.ConsumerGroupMetadata, error)
}

// TxMetrics is a minimal metrics surface.
type TxMetrics interface {
	TxAborted()
	TxProduced()
}

// ProduceAndCommit performs the OpA transactional sequence for a single consumed message.
// It assumes BeginTransaction has already been called by the caller.
func ProduceAndCommit(c ConsumerOffsets, p TxProducer, outTopic string, key []byte, value []byte, headers []ck.Header, m TxMetrics) error {
	if err := p.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &outTopic, Partition: ck.PartitionAny}, Key: key, Value: value, Headers: headers}, nil); err != nil {
		_ = p.AbortTransaction(context.Background())
		if m != nil {
			m.TxAborted()
		}
		return fmt.Errorf("produce: %w", err)
	}
	// Bind offsets atomically
	offsets, _ := c.Commit()
	meta, _ := c.GetConsumerGroupMetadata()
	if err := p.SendOffsetsToTransaction(context.Background(), offsets, meta); err != nil {
		_ = p.AbortTransaction(context.Background())
		if m != nil {
			m.TxAborted()
		}
		return fmt.Errorf("send offsets: %w", err)
	}
	if err := p.CommitTransaction(context.Background()); err != nil {
		_ = p.AbortTransaction(context.Background())
		if m != nil {
			m.TxAborted()
		}
		return fmt.Errorf("commit: %w", err)
	}
	if m != nil {
		m.TxProduced()
	}
	return nil
}
