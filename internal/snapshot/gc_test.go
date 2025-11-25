package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"hpb/internal/manifest"
)

type mockManifestReader struct {
	latest manifest.Manifest
	err    error
}

func (m *mockManifestReader) ReadLatest() (manifest.Manifest, error) {
	return m.latest, m.err
}

func writeTestManifest(t *testing.T, dir, id string, m manifest.Manifest) {
	t.Helper()
	snapDir := filepath.Join(dir, id)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapDir, "manifest.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestGC_RetentionCount(t *testing.T) {
	baseDir := t.TempDir()
	manifests := make([]manifest.Manifest, 5)
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("snap-%d", i)
		m := manifest.Manifest{SnapshotID: id, CreatedAtEpochSecond: time.Now().Unix() - int64(10*(5-i))}
		manifests[i] = m
		writeTestManifest(t, baseDir, id, m)
	}

	mr := &mockManifestReader{latest: manifests[4]} // latest is snap-4
	gc := NewGarbageCollector(baseDir, 3, 0, mr)

	deleted, err := gc.Collect()
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	if len(deleted) != 2 {
		t.Fatalf("want 2 deleted snapshots, got %d", len(deleted))
	}

	deletedMap := make(map[string]bool)
	for _, id := range deleted {
		deletedMap[id] = true
	}

	if !deletedMap["snap-0"] || !deletedMap["snap-1"] {
		t.Errorf("expected snap-0 and snap-1 to be deleted, got %v", deleted)
	}
}

func TestGC_RetentionDays(t *testing.T) {
	baseDir := t.TempDir()
	now := time.Now()
	m1 := manifest.Manifest{SnapshotID: "snap-old", CreatedAtEpochSecond: now.Add(-48 * time.Hour).Unix()}
	m2 := manifest.Manifest{SnapshotID: "snap-new", CreatedAtEpochSecond: now.Add(-12 * time.Hour).Unix()}
	writeTestManifest(t, baseDir, m1.SnapshotID, m1)
	writeTestManifest(t, baseDir, m2.SnapshotID, m2)

	mr := &mockManifestReader{latest: m2}
	gc := NewGarbageCollector(baseDir, 0, 1, mr) // Keep for 1 day

	deleted, err := gc.Collect()
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	if len(deleted) != 1 || deleted[0] != "snap-old" {
		t.Fatalf("expected ['snap-old'] to be deleted, got %v", deleted)
	}
}

func TestGC_ProtectedChain(t *testing.T) {
	baseDir := t.TempDir()
	now := time.Now()

	// A chain that is older than any retention policy
	base := manifest.Manifest{SnapshotID: "base-old", SnapshotType: manifest.SnapshotTypeFull, CreatedAtEpochSecond: now.Add(-100 * 24 * time.Hour).Unix()}
	delta1 := manifest.Manifest{SnapshotID: "delta1-old", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "base-old", CreatedAtEpochSecond: now.Add(-99 * 24 * time.Hour).Unix()}
	latest := manifest.Manifest{SnapshotID: "latest-old", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "delta1-old", CreatedAtEpochSecond: now.Add(-98 * 24 * time.Hour).Unix()}

	// A standalone snapshot that should be deleted
	standalone := manifest.Manifest{SnapshotID: "standalone-old", SnapshotType: manifest.SnapshotTypeFull, CreatedAtEpochSecond: now.Add(-101 * 24 * time.Hour).Unix()}

	writeTestManifest(t, baseDir, base.SnapshotID, base)
	writeTestManifest(t, baseDir, delta1.SnapshotID, delta1)
	writeTestManifest(t, baseDir, latest.SnapshotID, latest)
	writeTestManifest(t, baseDir, standalone.SnapshotID, standalone)

	mr := &mockManifestReader{latest: latest}
	gc := NewGarbageCollector(baseDir, 1, 1, mr)

	deleted, err := gc.Collect()
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	if len(deleted) != 1 || deleted[0] != standalone.SnapshotID {
		t.Fatalf("expected only ['standalone-old'] to be deleted, got %v", deleted)
	}
}

func TestGC_NoPolicy(t *testing.T) {
	baseDir := t.TempDir()
	m1 := manifest.Manifest{SnapshotID: "snap-1", CreatedAtEpochSecond: time.Now().Unix()}
	writeTestManifest(t, baseDir, m1.SnapshotID, m1)

	mr := &mockManifestReader{latest: m1}
	gc := NewGarbageCollector(baseDir, 0, 0, mr) // No retention

	deleted, err := gc.Collect()
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	if len(deleted) != 0 {
		t.Fatalf("expected 0 deleted snapshots, got %d", len(deleted))
	}
}

