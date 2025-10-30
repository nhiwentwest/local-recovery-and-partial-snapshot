//go:build integration
// +build integration

package restorekafka

import (
	"os"
	"testing"
	"time"

	"hpb/internal/changelog"
	"hpb/internal/state"
)

func TestIntegration_ReplayChangelogKafka(t *testing.T) {
	bootstrap := os.Getenv("KAFKA_BOOTSTRAP")
	topic := os.Getenv("KAFKA_TOPIC_CHANGELOG")
	if bootstrap == "" || topic == "" {
		t.Skip("set KAFKA_BOOTSTRAP and KAFKA_TOPIC_CHANGELOG to run integration")
	}
	from := int64(0)
	// publish some deltas to ensure there is data to apply
	_ = publishDeltas([]string{bootstrap}, topic, []changelog.Delta{
		{Key: "IT#p1#1694499900", Seq: 1, Delta: 100, DeltaQty: 1, TS: time.Now().Unix()},
		{Key: "IT#p1#1694499900", Seq: 2, Delta: 200, DeltaQty: 2, TS: time.Now().Unix()},
	})
	st := state.NewInMemoryStore()
	res := ReplayChangelogKafka(st, []string{bootstrap}, topic, from)
	if res.Error != nil {
		t.Fatalf("replay error: %v", res.Error)
	}
	if res.Applied <= 0 {
		t.Fatalf("expected Applied>0, got %+v", res)
	}
}
