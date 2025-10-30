//go:build integration
// +build integration

package restore

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/segmentio/kafka-go"

	"hpb/internal/changelog"
	"hpb/internal/manifest"
	"hpb/internal/state"
)

type fakeKafkaReader struct {
	msgs   []kafka.Message
	cursor int
}

func (f *fakeKafkaReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if f.cursor < len(f.msgs) {
		m := f.msgs[f.cursor]
		f.cursor++
		return m, nil
	}
	// simulate timeout end
	return kafka.Message{}, context.DeadlineExceeded
}

func (f *fakeKafkaReader) Close() error { return nil }

func TestReplayChangelogKafkaWith_BytesAndOffsets(t *testing.T) {
	// Prepare fake messages
	deltas := []changelog.Delta{{Key: "K#1", Seq: 1, Delta: 10}, {Key: "K#1", Seq: 2, Delta: 5}}
	var msgs []kafka.Message
	for i, d := range deltas {
		b, _ := json.Marshal(d)
		msgs = append(msgs, kafka.Message{Offset: int64(i), Value: b})
	}
	reader := &fakeKafkaReader{msgs: msgs}

	// Restorer with in-memory store and dummy manifest
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(t.TempDir()), "")

	res := r.ReplayChangelogKafkaWith(reader, 0)
	if res.Error != nil {
		t.Fatalf("unexpected error: %v", res.Error)
	}
	if res.Applied != 2 || res.Skipped != 0 {
		t.Fatalf("want applied=2 skipped=0, got %+v", res)
	}
	if res.Bytes <= 0 {
		t.Fatalf("expected bytes > 0, got %d", res.Bytes)
	}
	if res.LastAppliedOffset != 1 {
		t.Fatalf("unexpected last offset: %d", res.LastAppliedOffset)
	}
}
