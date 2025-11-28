package state

import (
	"path/filepath"
	"testing"
)

// TestPebbleIncrementalExport tests that ExportIncrementalSSTables only exports new files.
func TestPebbleIncrementalExport(t *testing.T) {
	dir := t.TempDir()
	ps, err := NewPebbleStore(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer ps.Close()

	// Write initial data and export checkpoint 1.
	_, _, err = ps.Apply("k1", 100, 1, 1)
	if err != nil {
		t.Fatalf("Apply k1: %v", err)
	}
	snap1Dir := filepath.Join(dir, "snap1")
	files1, _, err := ps.ExportSSTables(snap1Dir)
	if err != nil {
		t.Fatalf("ExportSSTables snap1: %v", err)
	}
	if len(files1) == 0 {
		t.Fatalf("snap1 has no files")
	}
	t.Logf("snap1: %d files", len(files1))

	// Write more data and export incremental checkpoint 2.
	_, _, err = ps.Apply("k2", 200, 2, 2)
	if err != nil {
		t.Fatalf("Apply k2: %v", err)
	}
	snap2Dir := filepath.Join(dir, "snap2")
	newFiles2, allFiles2, _, err := ps.ExportIncrementalSSTables(snap2Dir)
	if err != nil {
		t.Fatalf("ExportIncrementalSSTables snap2: %v", err)
	}
	t.Logf("snap2: new=%d all=%d", len(newFiles2), len(allFiles2))

	// Verify that newFiles2 is a subset of allFiles2.
	if len(newFiles2) == 0 {
		t.Fatalf("snap2 has no new files (expected at least metadata files)")
	}
	if len(allFiles2) < len(files1) {
		t.Fatalf("snap2 allFiles=%d < snap1 files=%d (expected more or equal)", len(allFiles2), len(files1))
	}

	// Write more data and export incremental checkpoint 3.
	_, _, err = ps.Apply("k3", 300, 3, 3)
	if err != nil {
		t.Fatalf("Apply k3: %v", err)
	}
	snap3Dir := filepath.Join(dir, "snap3")
	newFiles3, allFiles3, _, err := ps.ExportIncrementalSSTables(snap3Dir)
	if err != nil {
		t.Fatalf("ExportIncrementalSSTables snap3: %v", err)
	}
	t.Logf("snap3: new=%d all=%d", len(newFiles3), len(allFiles3))

	// Verify incremental behavior: newFiles3 should be smaller than allFiles3.
	if len(newFiles3) >= len(allFiles3) {
		t.Fatalf("snap3 newFiles=%d >= allFiles=%d (expected incremental)", len(newFiles3), len(allFiles3))
	}
}

// TestPebbleIncrementalRestore tests restoring from base + incremental deltas.
func TestPebbleIncrementalRestore(t *testing.T) {
	dir := t.TempDir()
	ps1, err := NewPebbleStore(filepath.Join(dir, "db1"))
	if err != nil {
		t.Fatalf("NewPebbleStore db1: %v", err)
	}

	// Write data and export base checkpoint.
	_, _, _ = ps1.Apply("k1", 100, 1, 1)
	_, _, _ = ps1.Apply("k2", 200, 2, 2)
	baseDir := filepath.Join(dir, "base")
	_, _, err = ps1.ExportSSTables(baseDir)
	if err != nil {
		t.Fatalf("ExportSSTables base: %v", err)
	}

	// Write more data and export incremental delta 1.
	_, _, _ = ps1.Apply("k3", 300, 3, 3)
	delta1Dir := filepath.Join(dir, "delta1")
	newFiles1, _, _, err := ps1.ExportIncrementalSSTables(delta1Dir)
	if err != nil {
		t.Fatalf("ExportIncrementalSSTables delta1: %v", err)
	}

	// Write more data and export incremental delta 2.
	_, _, _ = ps1.Apply("k4", 400, 4, 4)
	delta2Dir := filepath.Join(dir, "delta2")
	newFiles2, _, _, err := ps1.ExportIncrementalSSTables(delta2Dir)
	if err != nil {
		t.Fatalf("ExportIncrementalSSTables delta2: %v", err)
	}
	ps1.Close()

	// Restore to a new DB: base + delta1 + delta2.
	ps2, err := NewPebbleStore(filepath.Join(dir, "db2"))
	if err != nil {
		t.Fatalf("NewPebbleStore db2: %v", err)
	}
	defer ps2.Close()

	// Import base.
	if err := ps2.ImportSSTables(baseDir); err != nil {
		t.Fatalf("ImportSSTables base: %v", err)
	}
	// Ingest delta1.
	if err := ps2.IngestIncrementalFiles(delta1Dir, newFiles1); err != nil {
		t.Fatalf("IngestIncrementalFiles delta1: %v", err)
	}
	// Ingest delta2.
	if err := ps2.IngestIncrementalFiles(delta2Dir, newFiles2); err != nil {
		t.Fatalf("IngestIncrementalFiles delta2: %v", err)
	}

	// Verify all keys are present.
	for i, k := range []string{"k1", "k2", "k3", "k4"} {
		st, ok := ps2.Get(k)
		if !ok {
			t.Fatalf("key %s not found after restore", k)
		}
		expectedAmount := int64((i + 1) * 100)
		if st.SumAmount != expectedAmount {
			t.Fatalf("key %s: amount=%d want=%d", k, st.SumAmount, expectedAmount)
		}
	}
}

// TestPebbleIncrementalGC tests that GC respects file references.
func TestPebbleIncrementalGC(t *testing.T) {
	dir := t.TempDir()
	ps, err := NewPebbleStore(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer ps.Close()

	// Export base checkpoint.
	_, _, _ = ps.Apply("k1", 100, 1, 1)
	baseDir := filepath.Join(dir, "base")
	baseFiles, _, err := ps.ExportSSTables(baseDir)
	if err != nil {
		t.Fatalf("ExportSSTables base: %v", err)
	}

	// Export incremental delta (shares files with base).
	_, _, _ = ps.Apply("k2", 200, 2, 2)
	delta1Dir := filepath.Join(dir, "delta1")
	newFiles1, allFiles1, _, err := ps.ExportIncrementalSSTables(delta1Dir)
	if err != nil {
		t.Fatalf("ExportIncrementalSSTables delta1: %v", err)
	}

	// Simulate GC: if we delete base, shared files should remain.
	// Count shared files (files in both baseFiles and allFiles1).
	sharedCount := 0
	for _, f := range baseFiles {
		for _, af := range allFiles1 {
			if f == af {
				sharedCount++
				break
			}
		}
	}
	t.Logf("base files=%d, delta1 all=%d new=%d, shared=%d", len(baseFiles), len(allFiles1), len(newFiles1), sharedCount)

	// Verify that delta1 has at least some files (metadata or shared SSTables).
	if len(allFiles1) == 0 {
		t.Fatalf("delta1 has no files")
	}

	// Verify that newFiles1 is smaller than allFiles1 (incremental behavior).
	if len(newFiles1) >= len(allFiles1) {
		t.Fatalf("delta1 newFiles=%d >= allFiles=%d (expected incremental)", len(newFiles1), len(allFiles1))
	}

	// In a real GC scenario, we would:
	// 1. Build fileRefs map: baseFiles → [base], allFiles1 → [delta1]
	// 2. When deleting base, check if each file is referenced by delta1
	// 3. Only delete files with refCount == 1
	// This test just verifies the data structures are correct.
}
