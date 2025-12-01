package manifest

import (
	"encoding/json"
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

// TestReplayRequiredRoundTrip ensures the ReplayRequired hint survives JSON
// marshal/unmarshal and defaults behave as expected.
func TestReplayRequiredRoundTrip(t *testing.T) {
	// Explicit false should survive round-trip.
	replay := false
	m1 := Manifest{
		SnapshotID:          "snap-1",
		LastChangelogOffset: 123,
		ReplayRequired:      &replay,
	}
	b, err := json.Marshal(m1)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m2 Manifest
	if err := json.Unmarshal(b, &m2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m2.ReplayRequired == nil || *m2.ReplayRequired != false {
		t.Fatalf("expected ReplayRequired=false after round-trip, got %+v", m2.ReplayRequired)
	}

	// When ReplayRequired is nil, consumer code should treat it as "unknown"
	// and decide at restore-time; here we only check that omitting it does
	// not break JSON encoding.
	m3 := Manifest{
		SnapshotID:          "snap-2",
		LastChangelogOffset: 456,
	}
	if _, err := json.Marshal(m3); err != nil {
		t.Fatalf("marshal without ReplayRequired: %v", err)
	}
}

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
