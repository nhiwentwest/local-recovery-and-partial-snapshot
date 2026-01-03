package main

import (
    "fmt"

    "hpb/internal/manifest"
    "hpb/internal/snapshot"
	"hpb/internal/state"

	"sync"

	"hpb/internal/opb"
)

// multiRuntimeCtx will progressively accumulate shared objects used across the
// multi-input runtime pipeline.  Only the pieces needed for the current step
// (store, snapshotter, manifest publisher) are included; additional fields will
// be added in subsequent refactor steps.
// barrierCutContext holds metadata for a snapshot cut in progress.
type barrierCutContext struct {
	id      string
	cutType string
	prev    *manifest.Manifest
}

// activeCutsMap tracks in-progress snapshot cuts by cut ID.
type activeCutsMap struct {
	mu sync.Mutex
	m  map[string]*barrierCutContext
}

type multiRuntimeCtx struct {
	cfg  Config
	st   state.Store
	snap pebbleSnapshotViewAdapter
	mani manifest.Publisher

	// Consumer-related fields
	assign    *assignCache
	pauseAll  func()
	resumeAll func()

	// Operator and cut-coordination fields
	cutReqCh   chan snapshotCutRequest
	activeCuts struct {
		mu sync.Mutex
		m  map[string]*barrierCutContext
	}
	op *opb.DynamicNInputOperator
}

// snapshotCutRequest is used for admin-triggered snapshot cuts.
type snapshotCutRequest struct {
	cutType string
	prev    *manifest.Manifest
}

// initMiStoreSnapshot moves the state-store / snapshotter / manifest
// initialisation logic previously embedded in runMultiInputRuntime.  The code
// path is kept identical – only rearranged for clarity.
func initMiStoreSnapshot(cfg Config) (*multiRuntimeCtx, error) {
    if cfg.StateBackend != "pebble" {
        return nil, fmt.Errorf("multi-input runtime requires --state-backend=pebble")
    }
    if cfg.SnapshotShards < 1 {
        cfg.SnapshotShards = 1
    }

    // Initialise Pebble store
    ps, err := state.NewPebbleStore(cfg.StateDir)
    if err != nil {
        return nil, fmt.Errorf("init pebble: %w", err)
    }
    // Set transient instance id for LastUpdatedBy visibility
    ps.SetInstanceID(cfg.InstanceID)
    var st state.Store = ps

    // Snapshot adapter wrapping Pebble snapshotter and the store
    snapAdapter := pebbleSnapshotViewAdapter{
        snap:  snapshot.NewPebbleSnapshotter(cfg.SnapshotDir),
        store: st,
    }

    // Manifest publisher (filesystem + optional Kafka)
    maniFS := manifest.NewFilesystemManifest(cfg.SnapshotDir)
    var mani manifest.Publisher = maniFS
    if (cfg.ManifestSink == "kafka" || cfg.ManifestSink == "both") && cfg.KafkaBootstrap != "" {
        maniK := manifest.NewKafkaManifest(cfg.KafkaBootstrap, cfg.TopicSnapshots, "opb-manifest-latest")
        if cfg.ManifestSink == "kafka" {
            mani = maniK
        } else {
            mani = manifest.MultiPublisher(maniFS, maniK)
        }
    }

    ctx := &multiRuntimeCtx{
        cfg:  cfg,
        st:   st,
        snap: snapAdapter,
        mani: mani,
    }
    return ctx, nil
}

