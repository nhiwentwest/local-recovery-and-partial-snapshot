package restorefs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

// TestRestoreChain_PebbleBaseWithIncrementalDeltas tests restoring from a Pebble base
// followed by incremental Pebble deltas (Phase 3).
func TestRestoreChain_PebbleBaseWithIncrementalDeltas(t *testing.T) {
	baseDir := t.TempDir()
	snapshotsDir := filepath.Join(baseDir, "snapshots")
	stateDir := filepath.Join(baseDir, "state")
	ps, err := state.NewPebbleStore(stateDir)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer ps.Close()

	// Create base snapshot (full Pebble checkpoint).
	_, _, _ = ps.Apply("k1", 100, 1, 1)
	_, _, _ = ps.Apply("k2", 200, 2, 2)
	baseSnapDir := filepath.Join(snapshotsDir, "B")
	baseFiles, baseVer, err := ps.ExportSSTables(baseSnapDir)
	if err != nil {
		t.Fatalf("ExportSSTables base: %v", err)
	}
	baseManifest := manifest.Manifest{
		SnapshotID:          "B",
		SnapshotType:        manifest.SnapshotTypeFull,
		SnapshotFormat:      "pebble",
		PebbleSSTFiles:      baseFiles,
		PebbleFormatVersion: baseVer,
	}
	writeManifest(t, snapshotsDir, "B", baseManifest)

	// Create incremental delta 1.
	_, _, _ = ps.Apply("k3", 300, 3, 3)
	d1SnapDir := filepath.Join(snapshotsDir, "D1")
	newFiles1, allFiles1, d1Ver, err := ps.ExportIncrementalSSTables(d1SnapDir)
	if err != nil {
		t.Fatalf("ExportIncrementalSSTables D1: %v", err)
	}
	d1Manifest := manifest.Manifest{
		SnapshotID:             "D1",
		SnapshotType:           manifest.SnapshotTypeDelta,
		SnapshotFormat:         "pebble",
		ParentSnapshotID:       "B",
		BaseSnapshotID:         "B",
		DeltaSequence:          1,
		PebbleSSTFiles:         allFiles1,
		PebbleFormatVersion:    d1Ver,
		PebbleIncrementalFiles: newFiles1,
	}
	writeManifest(t, snapshotsDir, "D1", d1Manifest)

	// Create incremental delta 2.
	_, _, _ = ps.Apply("k4", 400, 4, 4)
	d2SnapDir := filepath.Join(snapshotsDir, "D2")
	newFiles2, allFiles2, d2Ver, err := ps.ExportIncrementalSSTables(d2SnapDir)
	if err != nil {
		t.Fatalf("ExportIncrementalSSTables D2: %v", err)
	}
	d2Manifest := manifest.Manifest{
		SnapshotID:             "D2",
		SnapshotType:           manifest.SnapshotTypeDelta,
		SnapshotFormat:         "pebble",
		ParentSnapshotID:       "D1",
		BaseSnapshotID:         "B",
		DeltaSequence:          2,
		PebbleSSTFiles:         allFiles2,
		PebbleFormatVersion:    d2Ver,
		PebbleIncrementalFiles: newFiles2,
	}
	writeManifest(t, snapshotsDir, "D2", d2Manifest)
	ps.Close()

	// Restore to a new DB.
	restoreStateDir := filepath.Join(baseDir, "restore_state")
	ps2, err := state.NewPebbleStore(restoreStateDir)
	if err != nil {
		t.Fatalf("NewPebbleStore restore: %v", err)
	}

	r := NewRestorerWithOptions(ps2, nil, manifest.NewFilesystemManifest(snapshotsDir), snapshotsDir, snapshot.FormatPebble, 1)
	if err := r.RestoreChainFromLatestWithOptions(d2Manifest, RestoreOptions{Parallelism: 0, ValidateChain: true}); err != nil {
		ps2.Close()
		t.Fatalf("RestoreChainFromLatest: %v", err)
	}

	// Verify all keys are present.
	for i, k := range []string{"k1", "k2", "k3", "k4"} {
		st, ok := ps2.Get(k)
		if !ok {
			ps2.Close()
			t.Fatalf("key %s not found after restore", k)
		}
		expectedAmount := int64((i + 1) * 100)
		if st.SumAmount != expectedAmount {
			ps2.Close()
			t.Fatalf("key %s: amount=%d want=%d", k, st.SumAmount, expectedAmount)
		}
	}
	ps2.Close()
}

// TestRestoreChain_MixedIncrementalAndFullDelta tests a chain with mixed delta types.
func TestRestoreChain_MixedIncrementalAndFullDelta(t *testing.T) {
	baseDir := t.TempDir()
	snapshotsDir := filepath.Join(baseDir, "snapshots")
	stateDir := filepath.Join(baseDir, "state")
	ps, err := state.NewPebbleStore(stateDir)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer ps.Close()

	// Base snapshot.
	_, _, _ = ps.Apply("k1", 100, 1, 1)
	baseSnapDir := filepath.Join(snapshotsDir, "B")
	baseFiles, baseVer, err := ps.ExportSSTables(baseSnapDir)
	if err != nil {
		t.Fatalf("ExportSSTables base: %v", err)
	}
	baseManifest := manifest.Manifest{
		SnapshotID:          "B",
		SnapshotType:        manifest.SnapshotTypeFull,
		SnapshotFormat:      "pebble",
		PebbleSSTFiles:      baseFiles,
		PebbleFormatVersion: baseVer,
	}
	writeManifest(t, snapshotsDir, "B", baseManifest)

	// Incremental delta 1.
	_, _, _ = ps.Apply("k2", 200, 2, 2)
	d1SnapDir := filepath.Join(snapshotsDir, "D1")
	newFiles1, allFiles1, d1Ver, err := ps.ExportIncrementalSSTables(d1SnapDir)
	if err != nil {
		t.Fatalf("ExportIncrementalSSTables D1: %v", err)
	}
	d1Manifest := manifest.Manifest{
		SnapshotID:             "D1",
		SnapshotType:           manifest.SnapshotTypeDelta,
		SnapshotFormat:         "pebble",
		ParentSnapshotID:       "B",
		BaseSnapshotID:         "B",
		DeltaSequence:          1,
		PebbleSSTFiles:         allFiles1,
		PebbleFormatVersion:    d1Ver,
		PebbleIncrementalFiles: newFiles1,
	}
	writeManifest(t, snapshotsDir, "D1", d1Manifest)

	// JSON delta 2 (fallback to JSON for testing mixed chain).
	_, _, _ = ps.Apply("k3", 300, 3, 3)
	d2SnapDir := filepath.Join(snapshotsDir, "D2")
	if err := os.MkdirAll(d2SnapDir, 0o755); err != nil {
		t.Fatalf("mkdir D2: %v", err)
	}
	st3, _ := ps.Get("k3")
	d2Data := map[string]state.RecordState{"k3": st3}
	b, _ := json.MarshalIndent(d2Data, "", "  ")
	if err := os.WriteFile(filepath.Join(d2SnapDir, "state.delta.json"), b, 0o644); err != nil {
		t.Fatalf("write D2 delta: %v", err)
	}
	d2Manifest := manifest.Manifest{
		SnapshotID:       "D2",
		SnapshotType:     manifest.SnapshotTypeDelta,
		SnapshotFormat:   "json",
		ParentSnapshotID: "D1",
		BaseSnapshotID:   "B",
		DeltaSequence:    2,
	}
	writeManifest(t, snapshotsDir, "D2", d2Manifest)
	ps.Close()

	// Restore to a new DB.
	restoreStateDir := filepath.Join(baseDir, "restore_state")
	ps2, err := state.NewPebbleStore(restoreStateDir)
	if err != nil {
		t.Fatalf("NewPebbleStore restore: %v", err)
	}

	r := NewRestorerWithOptions(ps2, nil, manifest.NewFilesystemManifest(snapshotsDir), snapshotsDir, snapshot.FormatPebble, 1)
	if err := r.RestoreChainFromLatestWithOptions(d2Manifest, RestoreOptions{Parallelism: 0, ValidateChain: true}); err != nil {
		ps2.Close()
		t.Fatalf("RestoreChainFromLatest: %v", err)
	}

	// Verify all keys.
	for i, k := range []string{"k1", "k2", "k3"} {
		st, ok := ps2.Get(k)
		if !ok {
			ps2.Close()
			t.Fatalf("key %s not found after restore", k)
		}
		expectedAmount := int64((i + 1) * 100)
		if st.SumAmount != expectedAmount {
			ps2.Close()
			t.Fatalf("key %s: amount=%d want=%d", k, st.SumAmount, expectedAmount)
		}
	}
	ps2.Close()
}
