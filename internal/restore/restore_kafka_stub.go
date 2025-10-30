//go:build !integration
// +build !integration

package restore

import (
	"fmt"

	"hpb/internal/manifest"
)

// Stubbed (non-integration) Kafka reader/replay to satisfy callers at build time.
type KafkaReader struct{}

func NewKafkaReader(brokers []string, topic string, key string) *KafkaReader { return &KafkaReader{} }

func (k *KafkaReader) ReadLatest() (manifest.Manifest, error) {
	return manifest.Manifest{}, fmt.Errorf("kafka manifest reader not available without -tags=integration")
}

func (r *Restorer) ReplayChangelogKafka(brokers []string, topic string, fromOffset int64) RestoreResult {
	return RestoreResult{Error: fmt.Errorf("kafka replay not available without -tags=integration")}
}
