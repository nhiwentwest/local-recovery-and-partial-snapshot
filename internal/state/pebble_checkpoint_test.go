package state

import (
	"os"
	"path/filepath"
	"testing"
)

// TestPebbleCheckpointRoundTrip verifies that ExportSSTables and ImportSSTables
// can be used to round-trip a small PebbleStore database.
func TestPebbleCheckpointRoundTrip(t *testing.T) {
	dir := t.TempDir()
	// Initial store
	st, err := NewPebbleStore(filepath.Join(dir, "db1"))
	if err != nil {
		t.Fatalf("NewPebbleStore error: %v", err)
	}
	defer st.Close()

	_, _, err = st.Apply("A#p1#100", 1000, 2, 1, SourceUnspecified)
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	_, _, err = st.Apply("B#p1#100", 500, 1, 1, SourceUnspecified)
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}

	cp, ok := any(st).(CheckpointCapable)
	if !ok {
		t.Skip("PebbleStore does not implement CheckpointCapable")
	}

	// Export SSTables into a snapshot-like directory.
	snapDir := filepath.Join(dir, "snap-1")
	files, _, err := cp.ExportSSTables(snapDir)
	if err != nil {
		t.Fatalf("ExportSSTables error: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("expected at least one SSTable file, got 0")
	}
	// Import into a new PebbleStore instance pointing at the exported dir.
	st2, err := NewPebbleStore(filepath.Join(dir, "db2"))
	if err != nil {
		t.Fatalf("NewPebbleStore(db2) error: %v", err)
	}
	defer st2.Close()

	cp2, ok := any(st2).(CheckpointCapable)
	if !ok {
		t.Skip("second PebbleStore does not implement CheckpointCapable")
	}
	if err := cp2.ImportSSTables(snapDir); err != nil {
		t.Fatalf("ImportSSTables error: %v", err)
	}

	// Verify keys are present after import.
	if rs, ok := st2.Get("A#p1#100"); !ok || rs.SumQty != 2 || rs.SumAmount != 1000 {
		t.Fatalf("unexpected state for A#p1#100 after import: %+v ok=%v", rs, ok)
	}
	if rs, ok := st2.Get("B#p1#100"); !ok || rs.SumQty != 1 || rs.SumAmount != 500 {
		t.Fatalf("unexpected state for B#p1#100 after import: %+v ok=%v", rs, ok)
	}

	// Sanity check that snapshot directory exists.
	if _, err := os.Stat(snapDir); err != nil {
		t.Fatalf("snapshot dir missing after export: %v", err)
	}
}
