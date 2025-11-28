package main

import (
	"context"
	"fmt"
	"time"

	"hpb/internal/kafkautil"
	"hpb/internal/manifest"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

// fixedViewWriter forces snapshots to be taken from a pre-created SnapshotView
// (captured at cut-begin) regardless of the view passed by callers. This ensures
// the snapshot is a pre-cut state while inflight captures post-cut messages.
// It delegates actual writing to the underlying snapshot implementation.
type fixedViewWriter struct {
	snap snapshotViewWriter
	view state.SnapshotView
}

func (w fixedViewWriter) WriteSnapshotFromView(id string, _ state.SnapshotView) (snapshot.Result, error) {
	if w.view == nil {
		return snapshot.Result{}, fmt.Errorf("fixedViewWriter: nil view")
	}
	return w.snap.WriteSnapshotFromView(id, w.view)
}

func (w fixedViewWriter) WriteDeltaSnapshotFromView(id string, _ state.SnapshotView, keys []string) (snapshot.Result, error) {
	if w.view == nil {
		return snapshot.Result{}, fmt.Errorf("fixedViewWriter: nil view")
	}
	return w.snap.WriteDeltaSnapshotFromView(id, w.view, keys)
}

// pebbleDeltaWriter wraps PebbleSnapshotter to call WriteDeltaSnapshot with dirty keys.
type pebbleDeltaWriter struct {
	snap *snapshot.PebbleSnapshotter
	st   state.Store
}

func (w pebbleDeltaWriter) WriteSnapshotFromView(id string, _ state.SnapshotView) (snapshot.Result, error) {
	return w.snap.WriteSnapshot(id, w.st)
}

func (w pebbleDeltaWriter) WriteDeltaSnapshotFromView(id string, _ state.SnapshotView, keys []string) (snapshot.Result, error) {
	return w.snap.WriteDeltaSnapshot(id, w.st, keys)
}

type kafkaOffsetsCollector struct {
	bootstrap string
	topic     string
}

func (k kafkaOffsetsCollector) Collect(ctx context.Context) (*manifest.OffsetsInfo, error) {
	if k.bootstrap == "" || k.topic == "" {
		return nil, fmt.Errorf("collector missing bootstrap/topic")
	}
	offsets, partitions, err := kafkautil.CollectChangelogOffsets(k.bootstrap, k.topic)
	if err != nil {
		return nil, err
	}
	return &manifest.OffsetsInfo{
		Topic:      k.topic,
		Partitions: partitions,
		Offsets:    offsets,
	}, nil
}

type kafkaDirtyScanner struct {
	bootstrap string
	timeout   time.Duration
}

func (k kafkaDirtyScanner) Scan(ctx context.Context, prev *manifest.Manifest, cur *manifest.OffsetsInfo) ([]string, error) {
	if prev == nil || prev.Changelog == nil {
		return nil, fmt.Errorf("delta scan missing prev manifest changelog info")
	}
	if cur == nil {
		return nil, fmt.Errorf("delta scan missing current offsets")
	}
	if k.bootstrap == "" {
		return nil, fmt.Errorf("delta scan missing bootstrap servers")
	}
	timeout := k.timeout
	if timeout <= 0 {
		timeout = 1500 * time.Millisecond
	}
	return kafkautil.ScanDirtyKeysKafka(
		[]string{k.bootstrap},
		prev.Changelog.Topic,
		prev.Changelog.Offsets,
		cur.Offsets,
		0,
		timeout,
	)
}

type snapshotViewWriter interface {
	WriteSnapshotFromView(id string, view state.SnapshotView) (snapshot.Result, error)
	WriteDeltaSnapshotFromView(id string, view state.SnapshotView, keys []string) (snapshot.Result, error)
}

type snapshotWriter struct {
	store       state.Store
	full        snapshot.Snapshotter
	fullView    snapshotViewWriter
	delta       snapshotViewWriter
	incremental *snapshot.PebbleSnapshotter
}

func (w snapshotWriter) WriteFull(id string, view state.SnapshotView) (snapshot.Result, error) {
	if w.fullView != nil {
		return w.fullView.WriteSnapshotFromView(id, view)
	}
	if w.full != nil {
		return w.full.WriteSnapshot(id, w.store)
	}
	return snapshot.Result{}, fmt.Errorf("snapshot writer not initialized for full snapshots")
}

func (w snapshotWriter) WriteDelta(id string, view state.SnapshotView, keys []string) (snapshot.Result, error) {
	if w.incremental != nil {
		return w.incremental.WriteIncrementalSnapshot(id, w.store)
	}
	if w.delta == nil {
		return snapshot.Result{}, fmt.Errorf("snapshot writer not initialized for delta snapshots")
	}
	return w.delta.WriteDeltaSnapshotFromView(id, view, keys)
}
