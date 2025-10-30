package opb

import (
	"testing"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestIsEpochAccept(t *testing.T) {
	if !IsEpochAccept(nil, []byte("10")) {
		t.Fatal("empty current should accept")
	}
	if IsEpochAccept([]byte("10"), nil) {
		t.Fatal("missing incoming should reject when current set")
	}
	if !IsEpochAccept([]byte("10"), []byte("10")) {
		t.Fatal("equal epoch accepts")
	}
	if !IsEpochAccept([]byte("10"), []byte("11")) {
		t.Fatal("higher epoch accepts")
	}
	if IsEpochAccept([]byte("10"), []byte("9")) {
		t.Fatal("lower epoch rejects")
	}
	if IsEpochAccept([]byte("x"), []byte("11")) {
		t.Fatal("bad current rejects")
	}
	if IsEpochAccept([]byte("10"), []byte("x")) {
		t.Fatal("bad incoming rejects")
	}
}

func TestAcceptMessageByEpoch(t *testing.T) {
	var prev int64 = 10
	// lower → reject
	if AcceptMessageByEpoch(&prev, []ck.Header{{Key: "epoch", Value: []byte("9")}}) {
		t.Fatal("expected reject for lower epoch")
	}
	// equal → accept keep prev
	if !AcceptMessageByEpoch(&prev, []ck.Header{{Key: "epoch", Value: []byte("10")}}) {
		t.Fatal("expected accept for equal epoch")
	}
	if prev != 10 {
		t.Fatalf("prev should remain 10, got %d", prev)
	}
	// higher → accept update prev
	if !AcceptMessageByEpoch(&prev, []ck.Header{{Key: "epoch", Value: []byte("11")}}) {
		t.Fatal("expected accept for higher epoch")
	}
	if prev != 11 {
		t.Fatalf("prev should update to 11, got %d", prev)
	}
	// missing epoch with prev set → reject
	if AcceptMessageByEpoch(&prev, nil) {
		t.Fatal("expected reject when epoch missing and prev set")
	}
}
