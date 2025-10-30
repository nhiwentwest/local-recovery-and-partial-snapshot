//go:generate go run github.com/golang/mock/mockgen -source=tx.go -destination=mocks/mock_tx.go -package=mocks

package opb

import (
	"context"
	"fmt"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// TxProducer defines the interface for a transactional Kafka producer, abstracting
// away the concrete implementation for testing with mocks.
type TxProducer interface {
	BeginTransaction() error
	Produce(*ck.Message, chan ck.Event) error
	SendOffsetsToTransaction(ctx context.Context, partitions []ck.TopicPartition, metadata *ck.ConsumerGroupMetadata) error
	CommitTransaction(ctx context.Context) error
	AbortTransaction(ctx context.Context) error
	InitTransactions(ctx context.Context) error
	Close()
}

// ConsumerOffsets defines the interface for a consumer that can provide offset information.
type ConsumerOffsets interface {
	GetConsumerGroupMetadata() (*ck.ConsumerGroupMetadata, error)
}

// Clock defines an interface for getting the current time, for testability.
type Clock interface {
	Now() time.Time
}

// RealClock implements Clock using the real time.
type RealClock struct{}

// Now returns the current time.
func (c RealClock) Now() time.Time { return time.Now() }

// BuildHeaders creates the standard t0/t1 headers for latency measurement.
// It takes the current clock and an optional t0 header value from the input message.
func BuildHeaders(clock Clock, hdrT0 []byte) []ck.Header {
	t1 := []byte(fmt.Sprintf("%d", clock.Now().UnixNano()))
	headers := []ck.Header{{Key: "t1", Value: t1}}
	if len(hdrT0) > 0 {
		headers = append(headers, ck.Header{Key: "t0", Value: hdrT0})
	}
	return headers
}

// BuildHeadersWithEpoch builds standard headers and attaches an epoch fencing token.
func BuildHeadersWithEpoch(clock Clock, hdrT0 []byte, epoch []byte) []ck.Header {
	hs := BuildHeaders(clock, hdrT0)
	if len(epoch) > 0 {
		hs = append(hs, ck.Header{Key: "epoch", Value: epoch})
	}
	return hs
}

// TxMetrics provides an interface for metrics related to transactions, allowing for
// mock implementations in tests.
type TxMetrics interface {
	TxAborted()
	TxProduced()
	TxLatencySec(float64)
	OffsetsBoundLag(float64)
}

// CommitBatch commits a transactional batch. It sends the provided offsets and
// commits the transaction. If any step fails, it aborts the transaction.
func CommitBatch(c ConsumerOffsets, p TxProducer, batchOffsets map[int32]ck.TopicPartition, mreg TxMetrics) error {
	// If no offsets, still try to commit the tx to flush produced records
	if len(batchOffsets) == 0 {
		if err := p.CommitTransaction(context.Background()); err != nil {
			_ = p.AbortTransaction(context.Background())
			mreg.TxAborted()
			return fmt.Errorf("commit empty tx: %w", err)
		}
		mreg.TxProduced()
		return nil
	}
	meta, err := c.GetConsumerGroupMetadata()
	if err != nil {
		_ = p.AbortTransaction(context.Background())
		mreg.TxAborted()
		return fmt.Errorf("get consumer group metadata: %w", err)
	}
	parts := make([]ck.TopicPartition, 0, len(batchOffsets))
	for _, tp := range batchOffsets {
		parts = append(parts, tp)
	}
	t0 := time.Now()
	if err := p.SendOffsetsToTransaction(context.Background(), parts, meta); err != nil {
		_ = p.AbortTransaction(context.Background())
		mreg.TxAborted()
		return fmt.Errorf("send offsets: %w", err)
	}
	// Observe a proxy for offsets bound: number of partitions in this batch
	mreg.OffsetsBoundLag(float64(len(parts)))
	if err := p.CommitTransaction(context.Background()); err != nil {
		_ = p.AbortTransaction(context.Background())
		mreg.TxAborted()
		return fmt.Errorf("commit tx: %w", err)
	}
	mreg.TxProduced()
	mreg.TxLatencySec(time.Since(t0).Seconds())
	return nil
}
