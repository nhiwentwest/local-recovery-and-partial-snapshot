package opa

import (
	"context"
	"fmt"
	"testing"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/mock/gomock"

	"hpb/internal/opa/mocks"
)

type noOpMetrics struct{}

func (noOpMetrics) TxAborted()  {}
func (noOpMetrics) TxProduced() {}

func strPtr(s string) *string { return &s }

func TestProduceAndCommit_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mp := mocks.NewMockTxProducer(ctrl)
	mc := mocks.NewMockConsumerOffsets(ctrl)

	out := "topic-out"
	key := []byte("k")
	val := []byte("v")
	hdrs := []ck.Header{{Key: "t0", Value: []byte("1")}}
	offsets := []ck.TopicPartition{{Topic: strPtr("in"), Partition: 0, Offset: 10}}
	meta := &ck.ConsumerGroupMetadata{}

	mp.EXPECT().Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &out, Partition: ck.PartitionAny}, Key: key, Value: val, Headers: hdrs}, gomock.Nil()).Return(nil)
	mc.EXPECT().Commit().Return(offsets, nil)
	mc.EXPECT().GetConsumerGroupMetadata().Return(meta, nil)
	mp.EXPECT().SendOffsetsToTransaction(context.Background(), offsets, meta).Return(nil)
	mp.EXPECT().CommitTransaction(context.Background()).Return(nil)

	if err := ProduceAndCommit(mc, mp, out, key, val, hdrs, noOpMetrics{}); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestProduceAndCommit_SendOffsetsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mp := mocks.NewMockTxProducer(ctrl)
	mc := mocks.NewMockConsumerOffsets(ctrl)

	out := "topic-out"
	key := []byte("k")
	val := []byte("v")
	hdrs := []ck.Header{{Key: "t0", Value: []byte("1")}}
	offsets := []ck.TopicPartition{{Topic: strPtr("in"), Partition: 0, Offset: 10}}
	meta := &ck.ConsumerGroupMetadata{}
	errSend := fmt.Errorf("send offsets failed")

	mp.EXPECT().Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &out, Partition: ck.PartitionAny}, Key: key, Value: val, Headers: hdrs}, gomock.Nil()).Return(nil)
	mc.EXPECT().Commit().Return(offsets, nil)
	mc.EXPECT().GetConsumerGroupMetadata().Return(meta, nil)
	mp.EXPECT().SendOffsetsToTransaction(context.Background(), offsets, meta).Return(errSend)
	mp.EXPECT().AbortTransaction(context.Background()).Return(nil)

	if err := ProduceAndCommit(mc, mp, out, key, val, hdrs, noOpMetrics{}); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestProduceAndCommit_CommitError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mp := mocks.NewMockTxProducer(ctrl)
	mc := mocks.NewMockConsumerOffsets(ctrl)

	out := "topic-out"
	key := []byte("k")
	val := []byte("v")
	hdrs := []ck.Header{{Key: "t0", Value: []byte("1")}}
	offsets := []ck.TopicPartition{{Topic: strPtr("in"), Partition: 0, Offset: 10}}
	meta := &ck.ConsumerGroupMetadata{}
	errCommit := fmt.Errorf("commit failed")

	mp.EXPECT().Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &out, Partition: ck.PartitionAny}, Key: key, Value: val, Headers: hdrs}, gomock.Nil()).Return(nil)
	mc.EXPECT().Commit().Return(offsets, nil)
	mc.EXPECT().GetConsumerGroupMetadata().Return(meta, nil)
	mp.EXPECT().SendOffsetsToTransaction(context.Background(), offsets, meta).Return(nil)
	mp.EXPECT().CommitTransaction(context.Background()).Return(errCommit)
	mp.EXPECT().AbortTransaction(context.Background()).Return(nil)

	if err := ProduceAndCommit(mc, mp, out, key, val, hdrs, noOpMetrics{}); err == nil {
		t.Fatalf("expected error, got nil")
	}
}
