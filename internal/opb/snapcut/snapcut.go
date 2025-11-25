package snapcut

import (
	"context"
	"fmt"
	"time"

	"hpb/internal/manifest"
	"hpb/internal/opb"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

// OffsetsCollector abstracts how to collect current changelog offsets.
// In prod, this is backed by Kafka; in tests, a stub can be provided.
type OffsetsCollector interface {
	Collect(ctx context.Context) (*manifest.OffsetsInfo, error)
}

// DirtyKeyScanner abstracts scanning dirty keys between two offset windows.
// In prod, this scans Kafka changelog; in tests, a stub can be provided.
type DirtyKeyScanner interface {
	Scan(ctx context.Context, prev *manifest.Manifest, cur *manifest.OffsetsInfo) ([]string, error)
}

// SnapshotWriter abstracts writing full or delta snapshots from a state snapshot view.
// In prod, this wraps snapshot.FilesystemSnapshotter; in tests, a stub can be provided.
type SnapshotWriter interface {
	WriteFull(id string, view state.SnapshotView) (snapshot.Result, error)
	WriteDelta(id string, view state.SnapshotView, keys []string) (snapshot.Result, error)
}

// CausalInfo allows callers to attach causal metadata (channels, inflight, vector clock).
type CausalInfo struct {
	Channels       []string
	InflightFile   string
	InflightEvents int
	VectorClock    opb.VectorClock
}

// NowFunc allows injection of clock for testability.
type NowFunc func() time.Time

// PerformBarrierCut performs a single barrier-based snapshot cut (full or delta),
// builds and publishes the manifest, and resets dirty keys as appropriate.
func PerformBarrierCut(
	ctx context.Context,
	cutType string,
	prev *manifest.Manifest,
	st state.Store,
	collector OffsetsCollector,
	scanner DirtyKeyScanner,
	writer SnapshotWriter,
	mani manifest.Publisher,
	changelogAppendedCount int64,
	causalFn func(string) (*CausalInfo, error),
	now NowFunc,
) (manifest.Manifest, snapshot.Result, error) {
	var zero manifest.Manifest
	var zeroRes snapshot.Result
	if now == nil {
		now = time.Now
	}
	// Collect changelog offsets
	offInfo, err := collector.Collect(ctx)
	if err != nil {
		return zero, zeroRes, fmt.Errorf("collect offsets: %w", err)
	}
	id := now().UTC().Format(time.RFC3339)
	var causal *CausalInfo
	if causalFn != nil {
		var cerr error
		causal, cerr = causalFn(id)
		if cerr != nil {
			return zero, zeroRes, fmt.Errorf("causal metadata: %w", cerr)
		}
	}
	view, verr := st.NewSnapshotView()
	if verr != nil {
		return zero, zeroRes, fmt.Errorf("snapshot view: %w", verr)
	}
	defer view.Close()

	mtype := manifest.SnapshotTypeFull
	var baseID, parentID string
	var dseq int

	var meta snapshot.Result
	if cutType == manifest.SnapshotTypeDelta {
		// Validate prerequisites
		if prev == nil || prev.Changelog == nil || offInfo == nil || prev.Changelog.Topic == "" || len(prev.Changelog.Offsets) == 0 {
			return zero, zeroRes, fmt.Errorf("delta requested but missing prev/offsets")
		}
		// Determine base/parent/dseq from prev
		if prev.SnapshotType == manifest.SnapshotTypeDelta && prev.BaseSnapshotID != "" {
			baseID = prev.BaseSnapshotID
			dseq = prev.DeltaSequence + 1
		} else {
			baseID = prev.SnapshotID
			dseq = 1
		}
		parentID = prev.SnapshotID
		// Scan dirty keys
		keys, kerr := scanner.Scan(ctx, prev, offInfo)
		if kerr != nil {
			return zero, zeroRes, fmt.Errorf("dirty scan: %w", kerr)
		}
		mtype = manifest.SnapshotTypeDelta
		meta, err = writer.WriteDelta(id, view, keys)
		if err != nil {
			return zero, zeroRes, fmt.Errorf("write delta: %w", err)
		}
		// Reset only the keys we captured
		if len(keys) > 0 {
			st.MarkSnapshotDone(keys...)
		}
	} else {
		meta, err = writer.WriteFull(id, view)
		if err != nil {
			return zero, zeroRes, fmt.Errorf("write full: %w", err)
		}
		st.MarkSnapshotDone()
	}
	m := manifest.Manifest{
		SnapshotID:           id,
		SnapshotFormat:       meta.Format.String(),
		SnapshotShards:       meta.Shards,
		SnapshotKeys:         meta.Keys,
		SnapshotType:         mtype,
		BaseSnapshotID:       baseID,
		ParentSnapshotID:     parentID,
		DeltaSequence:        dseq,
		LastChangelogOffset:  changelogAppendedCount,
		CreatedAtEpochSecond: now().UTC().Unix(),
		Changelog:            offInfo,
	}
	if causal != nil {
		if len(causal.Channels) > 0 {
			m.Channels = append([]string(nil), causal.Channels...)
		}
		m.InflightFile = causal.InflightFile
		m.InflightEvents = causal.InflightEvents
		if causal.VectorClock != nil {
			m.SnapshotVectorClock = causal.VectorClock.Copy()
		}
	}
	if fp, ok := mani.(manifest.FullPublisher); ok {
		if err := fp.Publish(m); err != nil {
			return zero, zeroRes, fmt.Errorf("publish manifest: %w", err)
		}
	} else {
		if err := mani.PublishLatest(id, changelogAppendedCount); err != nil {
			return zero, zeroRes, fmt.Errorf("publish latest: %w", err)
		}
	}
	return m, meta, nil
}
