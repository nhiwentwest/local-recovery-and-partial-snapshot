package snapshot

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"hpb/internal/manifest"
)

// createTestSnapshot creates a directory and a manifest.json for a test snapshot.
func createTestSnapshot(t *testing.T, baseDir string, m manifest.Manifest) {
	t.Helper()
	snapDir := filepath.Join(baseDir, m.SnapshotID)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("Failed to create snapshot dir %s: %v", snapDir, err)
	}

	// Create a dummy state file to make the snapshot appear valid to file system checks.
	dummyFileName := "state.json"
	if m.SnapshotType == manifest.SnapshotTypeDelta {
		dummyFileName = "state.delta.json"
	}
	dummyFile := filepath.Join(snapDir, dummyFileName)
	if err := os.WriteFile(dummyFile, []byte("{}"), 0o644); err != nil {
		t.Fatalf("Failed to write dummy state file for %s: %v", m.SnapshotID, err)
	}

	// Write the manifest for this specific snapshot, which GC uses to read metadata.
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal manifest for %s: %v", m.SnapshotID, err)
	}
	if err := os.WriteFile(filepath.Join(snapDir, "manifest.json"), b, 0o644); err != nil {
		t.Fatalf("Failed to write manifest for %s: %v", m.SnapshotID, err)
	}
}

// updateLatestManifest points the manifest.latest.json to the specified manifest.
func updateLatestManifest(t *testing.T, baseDir string, m manifest.Manifest) {
	t.Helper()
	latestFile := filepath.Join(baseDir, "manifest.latest.json")
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal latest manifest: %v", err)
	}
	if err := os.WriteFile(latestFile, b, 0o644); err != nil {
		t.Fatalf("Failed to write latest manifest: %v", err)
	}
}

func TestGarbageCollector_ProtectedChain(t *testing.T) {
	baseDir := t.TempDir()
	maniReader := manifest.NewFilesystemManifest(baseDir)

	// Create an old, unrelated full snapshot that should be deleted.
	oldTime := time.Now().UTC().Add(-48 * time.Hour)
	oldFull := manifest.Manifest{SnapshotID: "old-full", SnapshotType: manifest.SnapshotTypeFull, CreatedAtEpochSecond: oldTime.Unix()}
	createTestSnapshot(t, baseDir, oldFull)

	// Create a chain: B -> D1 -> D2 (latest)
	now := time.Now().UTC()
	base := manifest.Manifest{SnapshotID: "base", SnapshotType: manifest.SnapshotTypeFull, CreatedAtEpochSecond: now.Add(-3 * time.Hour).Unix()}
	d1 := manifest.Manifest{SnapshotID: "d1", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "base", CreatedAtEpochSecond: now.Add(-2 * time.Hour).Unix()}
	d2 := manifest.Manifest{SnapshotID: "d2", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "d1", CreatedAtEpochSecond: now.Add(-1 * time.Hour).Unix()}

	createTestSnapshot(t, baseDir, base)
	createTestSnapshot(t, baseDir, d1)
	createTestSnapshot(t, baseDir, d2)

	// Point latest to the head of the chain.
	updateLatestManifest(t, baseDir, d2)

	// GC with a tight retention policy that would delete the chain if it weren't protected.
	gc := NewGarbageCollector(baseDir, 1, 1, maniReader)
	deleted, err := gc.Collect()
	if err != nil {
		t.Fatalf("GC Collect failed: %v", err)
	}

	if len(deleted) != 1 || deleted[0] != "old-full" {
		t.Fatalf("Expected to delete [old-full], but got %v", deleted)
	}

	// Verify that the entire protected chain remains.
	for _, id := range []string{"base", "d1", "d2"} {
		if _, err := os.Stat(filepath.Join(baseDir, id)); os.IsNotExist(err) {
			t.Errorf("Protected snapshot %s was deleted but should have been kept", id)
		}
	}
}

func TestGarbageCollector_RetentionByCount(t *testing.T) {
	baseDir := t.TempDir()
	maniReader := manifest.NewFilesystemManifest(baseDir)

	// Create 5 snapshots, all full and unrelated for simplicity.
	snapshots := []manifest.Manifest{
		{SnapshotID: "snap-5", CreatedAtEpochSecond: time.Now().UTC().Add(-1 * time.Hour).Unix()},
		{SnapshotID: "snap-4", CreatedAtEpochSecond: time.Now().UTC().Add(-2 * time.Hour).Unix()},
		{SnapshotID: "snap-3", CreatedAtEpochSecond: time.Now().UTC().Add(-3 * time.Hour).Unix()},
		{SnapshotID: "snap-2", CreatedAtEpochSecond: time.Now().UTC().Add(-4 * time.Hour).Unix()},
		{SnapshotID: "snap-1", CreatedAtEpochSecond: time.Now().UTC().Add(-5 * time.Hour).Unix()},
	}

	for _, s := range snapshots {
		createTestSnapshot(t, baseDir, s)
	}
	// Latest points to the newest one.
	updateLatestManifest(t, baseDir, snapshots[0])

	// Keep the latest 3 (snap-5, snap-4, snap-3).
	gc := NewGarbageCollector(baseDir, 3, 0, maniReader)
	deleted, err := gc.Collect()
	if err != nil {
		t.Fatalf("GC Collect failed: %v", err)
	}

	sort.Strings(deleted)
	expectedDeleted := []string{"snap-1", "snap-2"}
	if !equalSlices(deleted, expectedDeleted) {
		t.Fatalf("Expected to delete %v, but got %v", expectedDeleted, deleted)
	}

	// Verify kept and deleted files.
	for _, id := range []string{"snap-3", "snap-4", "snap-5"} {
		if _, err := os.Stat(filepath.Join(baseDir, id)); os.IsNotExist(err) {
			t.Errorf("Snapshot %s was deleted but should have been kept", id)
		}
	}
	for _, id := range expectedDeleted {
		if _, err := os.Stat(filepath.Join(baseDir, id)); !os.IsNotExist(err) {
			t.Errorf("Snapshot %s was kept but should have been deleted", id)
		}
	}
}

func TestGarbageCollector_RetentionByDays(t *testing.T) {
	baseDir := t.TempDir()
	maniReader := manifest.NewFilesystemManifest(baseDir)

	// Create snapshots with various ages.
	snapshots := []manifest.Manifest{
		{SnapshotID: "snap-1d-old", CreatedAtEpochSecond: time.Now().UTC().Add(-25 * time.Hour).Unix()},
		{SnapshotID: "snap-3d-old", CreatedAtEpochSecond: time.Now().UTC().Add(-73 * time.Hour).Unix()},
		{SnapshotID: "snap-8d-old", CreatedAtEpochSecond: time.Now().UTC().Add(-8 * 24 * time.Hour).Unix()},
	}

	for _, s := range snapshots {
		createTestSnapshot(t, baseDir, s)
	}
	updateLatestManifest(t, baseDir, snapshots[0])

	// Keep snapshots newer than 7 days.
	gc := NewGarbageCollector(baseDir, 0, 7, maniReader)
	deleted, err := gc.Collect()
	if err != nil {
		t.Fatalf("GC Collect failed: %v", err)
	}

	if len(deleted) != 1 || deleted[0] != "snap-8d-old" {
		t.Fatalf("Expected to delete [snap-8d-old], but got %v", deleted)
	}
}

// equalSlices is a helper to compare sorted string slices.
func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

