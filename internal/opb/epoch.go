package opb

import (
	"strconv"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// IsEpochAccept returns true if incoming epoch is >= current epoch.
// Empty current means accept; empty incoming means reject when current is set.
func IsEpochAccept(current []byte, incoming []byte) bool {
	if len(current) == 0 {
		return true
	}
	if len(incoming) == 0 {
		return false
	}
	cur, err1 := strconv.ParseInt(string(current), 10, 64)
	inc, err2 := strconv.ParseInt(string(incoming), 10, 64)
	if err1 != nil || err2 != nil {
		// if parse fails, be conservative and reject
		return false
	}
	return inc >= cur
}

// AcceptMessageByEpoch applies fencing against a rolling highest epoch.
// Returns true if headers contain an acceptable epoch and updates prevEpoch if needed.
func AcceptMessageByEpoch(prevEpoch *int64, headers []ck.Header) bool {
	var incBytes []byte
	for _, h := range headers {
		if h.Key == "epoch" {
			incBytes = h.Value
			break
		}
	}
	if incBytes == nil {
		if prevEpoch == nil || *prevEpoch == 0 {
			return true
		}
		return false
	}
	inc, err := strconv.ParseInt(string(incBytes), 10, 64)
	if err != nil {
		return false
	}
	if prevEpoch != nil {
		if inc < *prevEpoch {
			return false
		}
		if inc > *prevEpoch {
			*prevEpoch = inc
		}
		return true
	}
	return true
}
