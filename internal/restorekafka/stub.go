//go:build !integration
// +build !integration

package restorekafka

import (
	"fmt"

	"hpb/internal/manifest"
	"hpb/internal/restorefs"
	"hpb/internal/state"
)

type KafkaReader struct{}

func NewKafkaReader(brokers []string, topic string, key string) *KafkaReader { return &KafkaReader{} }

func (k *KafkaReader) ReadLatest() (manifest.Manifest, error) {
	return manifest.Manifest{}, fmt.Errorf("kafka manifest reader not available without -tags=integration")
}

func ReplayChangelogKafka(st state.Store, brokers []string, topic string, fromOffset int64) restorefs.RestoreResult {
	return restorefs.RestoreResult{Error: fmt.Errorf("kafka replay not available without -tags=integration")}
}
