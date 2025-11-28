package restore

import (
	"path/filepath"
	"strings"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

// TestRestoreAndReplay_PebbleVsJSON verifies that using a Pebble snapshot backend
// produces the same logical state as the JSON backend for a simple dataset.
func TestRestoreAndReplay_PebbleVsJSON(t *testing.T) {
	dir := t.TempDir()
	snapDir := filepath.Join(dir, "snapshots")
	changelogDir := filepath.Join(dir, "changelog")

	// Create initial store and apply a few keys.
	stJSON, err := state.NewPebbleStore(filepath.Join(dir, "db-json"))
	if err != nil {
		t.Fatalf("NewPebbleStore json error: %v", err)
	}
	defer stJSON.Close()
	_, _, _ = stJSON.Apply("S1#p1#100", 1000, 2, 1)
	_, _, _ = stJSON.Apply("S1#p1#200", 500, 1, 2)

	// Snapshot using JSON backend.
	fsSnap := snapshot.NewFilesystemSnapshotter(snapDir, snapshot.FormatJSON, 1)
	metaJSON, err := fsSnap.WriteSnapshot("sid-json", stJSON)
	if err != nil {
		t.Fatalf("WriteSnapshot JSON error: %v", err)
	}
	mJSON := manifest.Manifest{
		SnapshotID:     "sid-json",
		SnapshotFormat: string(metaJSON.Format),
		SnapshotShards: metaJSON.Shards,
		SnapshotKeys:   metaJSON.Keys,
	}

	// Restore into a fresh Pebble store using JSON backend.
	stJSONRestore, err := state.NewPebbleStore(filepath.Join(dir, "db-json-restore"))
	if err != nil {
		t.Fatalf("NewPebbleStore json-restore error: %v", err)
	}
	defer stJSONRestore.Close()
	rJSON := NewRestorerWithOptions(stJSONRestore, fsSnap, manifest.NewFilesystemManifest(snapDir), snapDir, snapshot.FormatJSON, 1)
	// Inject manifest directly.
	if err := rJSON.RestoreFromSnapshotWithFormat(mJSON.SnapshotID, snapshot.FormatJSON, mJSON.SnapshotShards, mJSON.SnapshotKeys); err != nil {
		t.Fatalf("RestoreFromSnapshotWithFormat JSON error: %v", err)
	}
	afterJSON, _ := stJSONRestore.Get("S1#p1#100")

	// Snapshot using Pebble backend.
	stPebble, err := state.NewPebbleStore(filepath.Join(dir, "db-pebble"))
	if err != nil {
		t.Fatalf("NewPebbleStore pebble error: %v", err)
	}
	defer stPebble.Close()
	_, _, _ = stPebble.Apply("S1#p1#100", 1000, 2, 1)
	_, _, _ = stPebble.Apply("S1#p1#200", 500, 1, 2)

	pebSnap := snapshot.NewPebbleSnapshotter(snapDir)
	metaPeb, err := pebSnap.WriteSnapshot("sid-pebble", stPebble)
	if err != nil {
		t.Fatalf("WriteSnapshot Pebble error: %v", err)
	}
	mPeb := manifest.Manifest{
		SnapshotID:     "sid-pebble",
		SnapshotFormat: string(metaPeb.Format),
		SnapshotShards: metaPeb.Shards,
		SnapshotKeys:   metaPeb.Keys,
	}

	// Restore into another Pebble store using Pebble backend.
	stPebRestore, err := state.NewPebbleStore(filepath.Join(dir, "db-pebble-restore"))
	if err != nil {
		t.Fatalf("NewPebbleStore pebble-restore error: %v", err)
	}
	defer stPebRestore.Close()
	rPeb := NewRestorerWithOptions(stPebRestore, pebSnap, manifest.NewFilesystemManifest(snapDir), snapDir, snapshot.FormatPebble, 1)
	if err := rPeb.RestoreFromSnapshotWithFormat(mPeb.SnapshotID, snapshot.FormatPebble, mPeb.SnapshotShards, mPeb.SnapshotKeys); err != nil {
		t.Fatalf("RestoreFromSnapshotWithFormat Pebble error: %v", err)
	}
	afterPeb, _ := stPebRestore.Get("S1#p1#100")

	if afterJSON.SumQty != afterPeb.SumQty || afterJSON.SumAmount != afterPeb.SumAmount {
		t.Fatalf("mismatch after restore: json=%+v pebble=%+v", afterJSON, afterPeb)
	}
	_ = changelogDir
}


// TestRestorePebbleFromManifest_HappyPath verifies that restorePebbleFromManifest
// can successfully import a Pebble checkpoint when SST checksums match.
func TestRestorePebbleFromManifest_HappyPath(t *testing.T) {
	dir := t.TempDir()
	snapDir := filepath.Join(dir, "snapshots")

	// Source store with some data.
	src, err := state.NewPebbleStore(filepath.Join(dir, "db-src"))
	if err != nil {
		t.Fatalf("NewPebbleStore(src) error: %v", err)
	}
	defer src.Close()
	if _, _, err := src.Apply("S1#p1#100", 1000, 2, 1); err != nil {
		t.Fatalf("Apply(src) error: %v", err)
	}
	if _, _, err := src.Apply("S1#p2#200", 500, 1, 2); err != nil {
		t.Fatalf("Apply(src) error: %v", err)
	}

	// Write a Pebble snapshot with checksums.
	pebSnap := snapshot.NewPebbleSnapshotter(snapDir)
	meta, err := pebSnap.WriteSnapshot("sid-pebble-manifest", src)
	if err != nil {
		t.Fatalf("WriteSnapshot Pebble error: %v", err)
	}
	if len(meta.PebbleSSTFiles) == 0 {
		t.Fatalf("expected at least one SSTable file, got 0")
	}
	if len(meta.PebbleSSTChecksums) == 0 {
		t.Fatalf("expected non-empty PebbleSSTChecksums")
	}

	m := manifest.Manifest{
		SnapshotID:          "sid-pebble-manifest",
		SnapshotFormat:      meta.Format.String(),
		PebbleSSTFiles:      append([]string(nil), meta.PebbleSSTFiles...),
		PebbleSSTChecksums:  meta.PebbleSSTChecksums,
		PebbleFormatVersion: meta.PebbleFormatVersion,
	}

	// Restore into a fresh Pebble store using checksum-aware path.
	dst, err := state.NewPebbleStore(filepath.Join(dir, "db-dst"))
	if err != nil {
		t.Fatalf("NewPebbleStore(dst) error: %v", err)
	}
	defer dst.Close()

	r := NewRestorerWithOptions(dst, pebSnap, manifest.NewFilesystemManifest(snapDir), snapDir, snapshot.FormatPebble, 1)
	if err := r.restorePebbleFromManifest(m); err != nil {
		t.Fatalf("restorePebbleFromManifest happy-path error: %v", err)
	}

	if rs, ok := dst.Get("S1#p1#100"); !ok || rs.SumQty != 2 || rs.SumAmount != 1000 {
		t.Fatalf("unexpected state for S1#p1#100 after restore: %+v ok=%v", rs, ok)
	}
	if rs, ok := dst.Get("S1#p2#200"); !ok || rs.SumQty != 1 || rs.SumAmount != 500 {
		t.Fatalf("unexpected state for S1#p2#200 after restore: %+v ok=%v", rs, ok)
	}
}

// TestRestorePebbleFromManifest_ChecksumMismatch verifies that
// restorePebbleFromManifest fails when an SST checksum does not match.
func TestRestorePebbleFromManifest_ChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	snapDir := filepath.Join(dir, "snapshots")

	// Source store with some data.
	src, err := state.NewPebbleStore(filepath.Join(dir, "db-src"))
	if err != nil {
		t.Fatalf("NewPebbleStore(src) error: %v", err)
	}
	defer src.Close()
	if _, _, err := src.Apply("S1#p1#100", 1000, 2, 1); err != nil {
		t.Fatalf("Apply(src) error: %v", err)
	}

	pebSnap := snapshot.NewPebbleSnapshotter(snapDir)
	meta, err := pebSnap.WriteSnapshot("sid-pebble-bad", src)
	if err != nil {
		t.Fatalf("WriteSnapshot Pebble error: %v", err)
	}
	if len(meta.PebbleSSTFiles) == 0 {
		t.Fatalf("expected at least one SSTable file, got 0")
	}

	// Build a manifest with a deliberately wrong checksum for the first file.
	badChecksums := make(map[string]string, len(meta.PebbleSSTChecksums))
	for k, v := range meta.PebbleSSTChecksums {
		badChecksums[k] = v
	}
	firstFile := meta.PebbleSSTFiles[0]
	badChecksums[firstFile] = "deadbeef"

	mBad := manifest.Manifest{
		SnapshotID:          "sid-pebble-bad",
		SnapshotFormat:      meta.Format.String(),
		PebbleSSTFiles:      append([]string(nil), meta.PebbleSSTFiles...),
		PebbleSSTChecksums:  badChecksums,
		PebbleFormatVersion: meta.PebbleFormatVersion,
	}

	dst, err := state.NewPebbleStore(filepath.Join(dir, "db-dst"))
	if err != nil {
		t.Fatalf("NewPebbleStore(dst) error: %v", err)
	}
	defer dst.Close()

	r := NewRestorerWithOptions(dst, pebSnap, manifest.NewFilesystemManifest(snapDir), snapDir, snapshot.FormatPebble, 1)
	err = r.restorePebbleFromManifest(mBad)
	if err == nil {
		t.Fatalf("expected checksum mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error, got: %v", err)
	}
}


