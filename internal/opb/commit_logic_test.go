package opb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"hpb/internal/opb/mocks"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

type mockClock struct {
	t int64
}

func (mc *mockClock) Now() time.Time {
	mc.t++
	return time.Unix(0, mc.t)
}

func TestBuildHeaders_NonNegative(t *testing.T) {
	mc := &mockClock{t: 1234567890}
	h := BuildHeaders(mc, []byte("999"))
	expt1 := []byte(fmt.Sprintf("%d", mc.t))
	assert.Equal(t, []ck.Header{{Key: "t1", Value: expt1}, {Key: "t0", Value: []byte("999")}}, h)
}

func TestCommitBatch_SuccessSequence(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProducer := mocks.NewMockTxProducer(ctrl)
	mockConsumer := mocks.NewMockConsumerOffsets(ctrl)
	mockMetrics := mocks.NewMockTxMetrics(ctrl)

	ctx := context.Background()
	batchOffsets := map[int32]ck.TopicPartition{0: {Offset: 1}}
	meta := &ck.ConsumerGroupMetadata{}

	mockConsumer.EXPECT().GetConsumerGroupMetadata().Return(meta, nil)
	mockProducer.EXPECT().SendOffsetsToTransaction(ctx, gomock.Any(), meta).Return(nil)
	mockProducer.EXPECT().CommitTransaction(ctx).Return(nil)
	mockMetrics.EXPECT().TxProduced()
	mockMetrics.EXPECT().TxLatencySec(gomock.Any())

	err := CommitBatch(mockConsumer, mockProducer, batchOffsets, mockMetrics)
	assert.NoError(t, err)
}

func TestCommitBatch_SendOffsetsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProducer := mocks.NewMockTxProducer(ctrl)
	mockConsumer := mocks.NewMockConsumerOffsets(ctrl)
	mockMetrics := mocks.NewMockTxMetrics(ctrl)

	ctx := context.Background()
	batchOffsets := map[int32]ck.TopicPartition{0: {Offset: 1}}
	meta := &ck.ConsumerGroupMetadata{}
	sendErr := errors.New("send failed")

	mockConsumer.EXPECT().GetConsumerGroupMetadata().Return(meta, nil)
	mockProducer.EXPECT().SendOffsetsToTransaction(ctx, gomock.Any(), meta).Return(sendErr)
	mockProducer.EXPECT().AbortTransaction(ctx).Return(nil)
	mockMetrics.EXPECT().TxAborted()

	err := CommitBatch(mockConsumer, mockProducer, batchOffsets, mockMetrics)
	assert.ErrorIs(t, err, sendErr)
}

func TestCommitBatch_CommitError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProducer := mocks.NewMockTxProducer(ctrl)
	mockConsumer := mocks.NewMockConsumerOffsets(ctrl)
	mockMetrics := mocks.NewMockTxMetrics(ctrl)

	ctx := context.Background()
	batchOffsets := map[int32]ck.TopicPartition{0: {Offset: 1}}
	meta := &ck.ConsumerGroupMetadata{}
	commitErr := errors.New("commit failed")

	mockConsumer.EXPECT().GetConsumerGroupMetadata().Return(meta, nil)
	mockProducer.EXPECT().SendOffsetsToTransaction(ctx, gomock.Any(), meta).Return(nil)
	mockProducer.EXPECT().CommitTransaction(ctx).Return(commitErr)
	mockProducer.EXPECT().AbortTransaction(ctx).Return(nil)
	mockMetrics.EXPECT().TxAborted()

	err := CommitBatch(mockConsumer, mockProducer, batchOffsets, mockMetrics)
	assert.ErrorIs(t, err, commitErr)
}

func TestCommitBatch_EmptyOffsets(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProducer := mocks.NewMockTxProducer(ctrl)
	mockConsumer := mocks.NewMockConsumerOffsets(ctrl)
	mockMetrics := mocks.NewMockTxMetrics(ctrl)

	ctx := context.Background()

	mockProducer.EXPECT().CommitTransaction(ctx).Return(nil)
	mockMetrics.EXPECT().TxProduced()

	err := CommitBatch(mockConsumer, mockProducer, make(map[int32]ck.TopicPartition), mockMetrics)
	assert.NoError(t, err)
}
