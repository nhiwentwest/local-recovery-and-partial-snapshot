package opb

import (
	"testing"
)

func TestBuildHeadersWithEpoch(t *testing.T) {
	clk := RealClock{}
	h := BuildHeadersWithEpoch(clk, []byte("t0v"), []byte("123"))
	var hasT0, hasT1, hasEpoch bool
	for _, x := range h {
		switch x.Key {
		case "t0":
			hasT0 = string(x.Value) == "t0v"
		case "t1":
			hasT1 = len(x.Value) > 0
		case "epoch":
			hasEpoch = string(x.Value) == "123"
		}
	}
	if !hasT0 || !hasT1 || !hasEpoch {
		t.Fatalf("headers missing fields: t0=%v t1=%v epoch=%v", hasT0, hasT1, hasEpoch)
	}
}
