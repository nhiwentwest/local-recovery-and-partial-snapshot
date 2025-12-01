//go:build integration
// +build integration

package restore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"

	"hpb/internal/changelog"
	"hpb/internal/manifest"
	"hpb/internal/state"
)

type fakeReaderSeq struct {
	msgs []kafka.Message
	idx  int
	errs []error
}

func (f *fakeReaderSeq) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if f.idx < len(f.msgs) {
		m := f.msgs[f.idx]
		e := error(nil)
		if f.idx < len(f.errs) {
			e = f.errs[f.idx]
		}
		f.idx++
		if e != nil {
			return kafka.Message{}, e
		}
		return m, nil
	}
	return kafka.Message{}, context.DeadlineExceeded
}
func (f *fakeReaderSeq) Close() error { return nil }

type errStore struct{ state.Store }

func (e *errStore) Apply(key string, da, dq, seq int64, _ state.SourceKind) (bool, state.RecordState, error) {
	if seq > 1 {
		return false, state.RecordState{}, errors.New("apply failed")
	}
	return e.Store.Apply(key, da, dq, seq, state.SourceUnspecified)
}

func TestReplayChangelogKafkaWith_JSONError(t *testing.T) {
	good := changelog.Delta{Key: "K#1", Seq: 1, Delta: 1}
	b, _ := json.Marshal(good)
	msgs := []kafka.Message{{Offset: 0, Value: b}, {Offset: 1, Value: []byte("{bad}")}}
	r := &fakeReaderSeq{msgs: msgs}
	st := state.NewInMemoryStore()
	rr := NewRestorer(st, nil, manifest.NewFilesystemManifest(t.TempDir()), "")
	res := rr.ReplayChangelogKafkaWith(r, 0)
	if res.Error == nil {
		t.Fatalf("expected json error")
	}
	if res.Applied != 1 {
		t.Fatalf("applied should be 1 before error, got %d", res.Applied)
	}
}

func TestReplayChangelogKafkaWith_ApplyError(t *testing.T) {
	// seq=1 ok, seq=2 triggers store error
	d1 := changelog.Delta{Key: "K#1", Seq: 1, Delta: 1}
	d2 := changelog.Delta{Key: "K#1", Seq: 2, Delta: 1}
	b1, _ := json.Marshal(d1)
	b2, _ := json.Marshal(d2)
	msgs := []kafka.Message{{Offset: 0, Value: b1}, {Offset: 1, Value: b2}}
	r := &fakeReaderSeq{msgs: msgs}
	st := &errStore{Store: state.NewInMemoryStore()}
	rr := NewRestorer(st, nil, manifest.NewFilesystemManifest(t.TempDir()), "")
	res := rr.ReplayChangelogKafkaWith(r, 0)
	if res.Error == nil {
		t.Fatalf("expected apply error")
	}
	if res.Applied != 1 {
		t.Fatalf("applied should be 1 before error, got %d", res.Applied)
	}
}

func TestReplayChangelogKafkaWith_ContextCancel(t *testing.T) {
	// Empty sequence; reader returns DeadlineExceeded immediately
	r := &fakeReaderSeq{msgs: nil}
	st := state.NewInMemoryStore()
	rr := NewRestorer(st, nil, manifest.NewFilesystemManifest(t.TempDir()), "")
	res := rr.ReplayChangelogKafkaWith(r, 0)
	if res.Error != nil {
		t.Fatalf("unexpected err: %v", res.Error)
	}
	if res.Applied != 0 || res.Skipped != 0 {
		t.Fatalf("want 0/0, got %+v", res)
	}
}
