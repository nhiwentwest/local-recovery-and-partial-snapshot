package snapcut

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"hpb/internal/manifest"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

type fakeCollector struct {
	out *manifest.OffsetsInfo
	err error
}

func (f fakeCollector) Collect(ctx context.Context) (*manifest.OffsetsInfo, error) {
	return f.out, f.err
}

type fakeScanner struct {
	keys []string
	err  error
}

func (f fakeScanner) Scan(ctx context.Context, prev *manifest.Manifest, cur *manifest.OffsetsInfo) ([]string, error) {
	return f.keys, f.err
}

type fakeWriter struct {
	fullCalled  bool
	deltaCalled bool
	lastID      string
	lastKeys    []string
	fullRes     snapshot.Result
	deltaRes    snapshot.Result
	fullErr     error
	deltaErr    error
}

func (w *fakeWriter) WriteFull(id string, view state.SnapshotView) (snapshot.Result, error) {
	w.fullCalled = true
	w.lastID = id
	return w.fullRes, w.fullErr
}
func (w *fakeWriter) WriteDelta(id string, view state.SnapshotView, keys []string) (snapshot.Result, error) {
	w.deltaCalled = true
	w.lastID = id
	w.lastKeys = append([]string(nil), keys...)
	return w.deltaRes, w.deltaErr
}

type capturePublisher struct {
	last manifest.Manifest
	err  error
}

func (p *capturePublisher) PublishLatest(snapshotID string, lastChangelogOffset int64) error {
	p.last = manifest.Manifest{SnapshotID: snapshotID, LastChangelogOffset: lastChangelogOffset}
	return p.err
}
func (p *capturePublisher) Publish(m manifest.Manifest) error { p.last = m; return p.err }

func TestPerformBarrierCut_Full(t *testing.T) {
	st := state.NewInMemoryStore()
	// seed some state and dirty keys
	_, _, _ = st.Apply("k1", 10, 1, 1)
	_, _, _ = st.Apply("k2", 20, 2, 1)
	// fakes
	coll := fakeCollector{out: &manifest.OffsetsInfo{Topic: "chg", Partitions: 1, Offsets: []int64{10}}}
	w := &fakeWriter{fullRes: snapshot.Result{Format: snapshot.FormatJSON, Shards: 1, Keys: 2}}
	pub := &capturePublisher{}
	// fixed clock
	now := func() time.Time { return time.Unix(1700000000, 0).UTC() }
	m, res, err := PerformBarrierCut(context.Background(), manifest.SnapshotTypeFull, nil, st, coll, fakeScanner{}, w, pub, 123, nil, now)
	if err != nil {
		t.Fatalf("PerformBarrierCut full: %v", err)
	}
	if !w.fullCalled || w.deltaCalled {
		t.Fatalf("writer calls: full=%v delta=%v", w.fullCalled, w.deltaCalled)
	}
	if got := strings.ToLower(m.SnapshotType); got != manifest.SnapshotTypeFull {
		t.Fatalf("manifest type=%s want=full", got)
	}
	if res.Keys != 2 {
		t.Fatalf("result keys=%d want 2", res.Keys)
	}
	// dirty keys should be cleared fully
	if keys := st.GetDirtyKeys(); len(keys) != 0 {
		t.Fatalf("dirty not cleared: %v", keys)
	}
	if pub.last.SnapshotID == "" {
		t.Fatalf("publisher not called")
	}
}

func TestPerformBarrierCut_Delta(t *testing.T) {
	st := state.NewInMemoryStore()
	// mark two dirty keys
	st.MarkSnapshotDone()
	_, _, _ = st.Apply("ka", 10, 1, 1)
	_, _, _ = st.Apply("kb", 20, 2, 1)
	prev := &manifest.Manifest{SnapshotID: "full-1", SnapshotType: manifest.SnapshotTypeFull, Changelog: &manifest.OffsetsInfo{Topic: "chg", Partitions: 1, Offsets: []int64{100}}}
	coll := fakeCollector{out: &manifest.OffsetsInfo{Topic: "chg", Partitions: 1, Offsets: []int64{200}}}
	scan := fakeScanner{keys: []string{"ka"}}
	w := &fakeWriter{deltaRes: snapshot.Result{Format: snapshot.FormatJSON, Shards: 1, Keys: 1}}
	pub := &capturePublisher{}
	now := func() time.Time { return time.Unix(1700000100, 0).UTC() }
	m, res, err := PerformBarrierCut(context.Background(), manifest.SnapshotTypeDelta, prev, st, coll, scan, w, pub, 555, nil, now)
	if err != nil {
		t.Fatalf("PerformBarrierCut delta: %v", err)
	}
	if !w.deltaCalled || w.fullCalled {
		t.Fatalf("writer calls: full=%v delta=%v", w.fullCalled, w.deltaCalled)
	}
	if strings.ToLower(m.SnapshotType) != manifest.SnapshotTypeDelta {
		t.Fatalf("type=%s want=delta", m.SnapshotType)
	}
	if m.BaseSnapshotID != prev.SnapshotID || m.ParentSnapshotID != prev.SnapshotID || m.DeltaSequence != 1 {
		t.Fatalf("chain wrong: %+v", m)
	}
	if res.Keys != 1 {
		t.Fatalf("result keys=%d want 1", res.Keys)
	}
	// dirty should only clear 'ka', leaving 'kb'
	keys := st.GetDirtyKeys()
	if len(keys) != 1 || keys[0] != "kb" {
		t.Fatalf("dirty after delta want [kb], got %v", keys)
	}
	if !reflect.DeepEqual(w.lastKeys, []string{"ka"}) {
		t.Fatalf("writer lastKeys=%v", w.lastKeys)
	}
}

func TestPerformBarrierCut_DeltaMissingPrev(t *testing.T) {
	st := state.NewInMemoryStore()
	coll := fakeCollector{out: &manifest.OffsetsInfo{Topic: "chg", Partitions: 1, Offsets: []int64{10}}}
	w := &fakeWriter{}
	pub := &capturePublisher{}
	_, _, err := PerformBarrierCut(context.Background(), manifest.SnapshotTypeDelta, nil, st, coll, fakeScanner{}, w, pub, 0, nil, time.Now)
	if err == nil {
		t.Fatalf("expected error when prev missing for delta")
	}
}
