package manifest

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestPublishAndReadLatest(t *testing.T) {
	dir := t.TempDir()
	m := NewFilesystemManifest(dir)
	if err := m.PublishLatest("sid-123", 42); err != nil {
		t.Fatalf("PublishLatest error: %v", err)
	}
	got, err := m.ReadLatest()
	if err != nil {
		t.Fatalf("ReadLatest error: %v", err)
	}
	if got.SnapshotID != "sid-123" || got.LastChangelogOffset != 42 || got.CreatedAtEpochSecond == 0 {
		t.Fatalf("unexpected manifest: %+v", got)
	}
}

// fakeKafkaWriter implements kafkaMessageWriter for tests
type fakeKafkaWriter struct {
	msgs []kafka.Message
	fail bool
}

func (f *fakeKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if f.fail {
		return errors.New("fail")
	}
	f.msgs = append(f.msgs, msgs...)
	return nil
}

func TestKafkaManifest_PublishLatest_Success(t *testing.T) {
	fk := &fakeKafkaWriter{}
	km := NewKafkaManifestWith(fk, "opb-manifest-latest")
	if err := km.PublishLatest("sid-abc", 99); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(fk.msgs) != 1 {
		t.Fatalf("want 1 msg, got %d", len(fk.msgs))
	}
	if string(fk.msgs[0].Key) != "opb-manifest-latest" {
		t.Fatalf("bad key: %s", string(fk.msgs[0].Key))
	}
}

func TestKafkaManifest_PublishLatest_Fail(t *testing.T) {
	fk := &fakeKafkaWriter{fail: true}
	km := NewKafkaManifestWith(fk, "opb-manifest-latest")
	if err := km.PublishLatest("sid-abc", 99); err == nil {
		t.Fatalf("expected error")
	}
}
