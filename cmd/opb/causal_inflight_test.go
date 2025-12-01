package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"hpb/internal/opb"
)

// TestInflightRecordMarshalVectorClock đảm bảo rằng khi VC != nil
// thì field JSON "vectorClock" không bị rơi mất (tránh case null/ngầm quên).
func TestInflightRecordMarshalVectorClock(t *testing.T) {
	vc := opb.NewVectorClock().Tick("genorders")
	rec := inflightRecord{
		Key:     "k",
		Payload: json.RawMessage(`{"x":1}`),
		VC:      vc,
	}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal inflightRecord: %v", err)
	}
	if !bytes.Contains(b, []byte(`"vectorClock"`)) {
		t.Fatalf("expected vectorClock field in JSON, got %s", string(b))
	}
	if !bytes.Contains(b, []byte(`"genorders"`)) {
		t.Fatalf("expected vectorClock dimension 'genorders', got %s", string(b))
	}
}
