package main

import (
	"testing"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
)

func TestMaybeAttachT0_NilHeaders(t *testing.T) {
	var headers []ck.Header
	result := maybeAttachT0(headers)

	assert.Len(t, result, 1)
	assert.Equal(t, "t0", result[0].Key)
	assert.NotEmpty(t, result[0].Value)
}

func TestMaybeAttachT0_T0Exists(t *testing.T) {
	headers := []ck.Header{{Key: "t0", Value: []byte("123")}}
	result := maybeAttachT0(headers)

	assert.Equal(t, headers, result)
}

func TestMaybeAttachT0_OtherHeadersExist(t *testing.T) {
	headers := []ck.Header{{Key: "other", Value: []byte("abc")}}
	result := maybeAttachT0(headers)

	assert.Len(t, result, 2)
	assert.Equal(t, "other", result[0].Key)
	assert.Equal(t, "t0", result[1].Key)
}
