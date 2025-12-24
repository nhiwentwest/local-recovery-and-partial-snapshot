package state

import (
	"path/filepath"
	"testing"
)

// TestPebbleDeltaExportIngest verifies that ExportDeltaSSTables creates ingestable
// SST files for a set of dirty keys and that IngestDeltaSSTables applies them correctly.
func TestPebbleDeltaExportIngest(t *testing.T) {
	baseDir := t.TempDir()
	stateDir := filepath.Join(baseDir, "state-src")
	ps, err := NewPebbleStore(stateDir)
	if err != nil {
		t.Fatalf("NewPebbleStore src: %v", err)
	}
	defer ps.Close()

	// Seed initial data and checkpoint as base.
	_, _, _ = ps.Apply("k1", 100, 1, 1, SourceUnspecified)
	_, _, _ = ps.Apply("k2", 200, 2, 2, SourceUnspecified)
	baseSnapDir := filepath.Join(baseDir, "snap-B")
	baseFiles, _, err := ps.ExportSSTables(baseSnapDir)
	if err != nil {
		t.Fatalf("ExportSSTables base: %v", err)
	}
	if len(baseFiles) == 0 {
		t.Fatalf("expected base checkpoint to produce files")
	}
	// Mark snapshot done to reset dirty tracking for future deltas.
	ps.MarkSnapshotDone()

	// Create a destination DB and import base.
	dstDir := filepath.Join(baseDir, "state-dst")
	psDst, err := NewPebbleStore(dstDir)
	if err != nil {
		t.Fatalf("NewPebbleStore dst: %v", err)
	}
	defer psDst.Close()
	if err := psDst.ImportSSTables(baseSnapDir); err != nil {
		t.Fatalf("ImportSSTables base: %v", err)
	}

	// Apply updates on source and export delta for dirty keys.
	_, _, _ = ps.Apply("k2", 50, 0, 3, SourceUnspecified)  // update existing
	_, _, _ = ps.Apply("k3", 300, 3, 4, SourceUnspecified) // new key
	dirty := ps.GetDirtyKeys()
	if len(dirty) == 0 {
		t.Fatalf("expected dirty keys after Apply")
	}
	deltaDir := filepath.Join(baseDir, "snap-D1")
	files, _, err := ps.ExportDeltaSSTables(deltaDir, dirty)
	if err != nil {
		t.Fatalf("ExportDeltaSSTables: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("expected delta export to produce files")
	}

	// Ingest delta into destination DB.
	if err := psDst.IngestDeltaSSTables(deltaDir, files); err != nil {
		t.Fatalf("IngestDeltaSSTables: %v", err)
	}

	// Validate final state in destination.
	st1, ok := psDst.Get("k1")
	if !ok || st1.SumAmount != 100 {
		t.Fatalf("k1 mismatch: ok=%v amt=%d", ok, st1.SumAmount)
	}
	st2, ok := psDst.Get("k2")
	if !ok || st2.SumAmount != 250 { // 200 + 50 delta
		t.Fatalf("k2 mismatch: ok=%v amt=%d", ok, st2.SumAmount)
	}
	st3, ok := psDst.Get("k3")
	if !ok || st3.SumAmount != 300 {
		t.Fatalf("k3 mismatch: ok=%v amt=%d", ok, st3.SumAmount)
	}
}

