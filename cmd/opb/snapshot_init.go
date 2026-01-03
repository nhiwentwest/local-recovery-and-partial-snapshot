package main

import (
    "fmt"
    "log"

    "hpb/internal/snapshot"
    "hpb/internal/state"
)

// InitSnapshotters prepares snapshotting helpers used by OpB when the state
// backend is Pebble. Logic is copied verbatim from the original run() before
// refactor – no functional changes.
//
// Returns:
//   fullSnap         – mandatory snapshot writer (full)
//   fullSnapView     – optional view-based writer for full snapshots (nil when not used)
//   deltaSnapView    – optional view-based writer for delta snapshots (nil when not used / Phase2)
//   deltaIncremental – Phase3 incremental snapshotter (nil when Phase3 disabled or not supported)
func InitSnapshotters(cfg Config, st state.Store) (snapshot.Snapshotter, snapshotViewWriter, snapshotViewWriter, *snapshot.PebbleSnapshotter, error) {
    // Currently only Pebble backend is supported. Earlier checks in run() should
    // ensure this, but we keep the guard to prevent misuse.
    if cfg.StateBackend != "pebble" {
        return nil, nil, nil, nil, fmt.Errorf("InitSnapshotters: unsupported state-backend %s (expected pebble)", cfg.StateBackend)
    }

    // Base pebble snapshotter (Phase1 full snapshot shipping)
    pebbleSnapper := snapshot.NewPebbleSnapshotter(cfg.SnapshotDir)

    var (
        fullSnap         snapshot.Snapshotter    = pebbleSnapper
        fullSnapView     snapshotViewWriter      // nil – never used for Pebble full (we snapshot directly from store)
        deltaSnapView    snapshotViewWriter      // may be set for Phase2 delta snapshots
        deltaIncremental *snapshot.PebbleSnapshotter // Phase3 incremental shipping
    )

    if cfg.EnablePebblePhase3 {
        if _, ok := st.(state.IncrementalCheckpointCapable); ok {
            deltaIncremental = pebbleSnapper
            log.Printf("delta snapshots will use Pebble incremental shipping (Phase 3)")
        } else {
            log.Printf("warning: enable-pebble-phase3 set but store is not IncrementalCheckpointCapable; falling back to Phase 2 delta")
        }
    }

    // If Phase3 not enabled/supported, fall back to Phase2 delta snapshots via pebbleDeltaWriter
    if deltaIncremental == nil {
        if _, ok := st.(state.DeltaCheckpointCapable); ok {
            deltaSnapView = pebbleDeltaWriter{snap: pebbleSnapper, st: st}
        } else {
            return nil, nil, nil, nil, fmt.Errorf("state store does not support Pebble delta snapshots")
        }
    }

    return fullSnap, fullSnapView, deltaSnapView, deltaIncremental, nil
}

