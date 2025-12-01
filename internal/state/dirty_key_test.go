package state

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func testDirtyKeyTracking(t *testing.T, s Store) {
	// 1. Apply single key, check if dirty
	_, _, _ = s.Apply("k1", 10, 1, 1, SourceUnspecified)
	dirty1 := s.GetDirtyKeys()
	if len(dirty1) != 1 || dirty1[0] != "k1" {
		t.Fatalf("after Apply, expected [k1], got %v", dirty1)
	}

	// 2. Mark snapshot done, check if dirty map is cleared
	s.MarkSnapshotDone()
	dirty2 := s.GetDirtyKeys()
	if len(dirty2) != 0 {
		t.Fatalf("after MarkSnapshotDone, expected [], got %v", dirty2)
	}

	// 3. Apply a batch, check if all keys are dirty
	batch := []Delta{
		{Key: "k2", DeltaAmount: 20, DeltaQty: 2, Seq: 1},
		{Key: "k3", DeltaAmount: 30, DeltaQty: 3, Seq: 1},
	}
	_, _, _ = s.ApplyBatch(batch)
	dirty3 := s.GetDirtyKeys()
	sort.Strings(dirty3)
	want3 := []string{"k2", "k3"}
	if !reflect.DeepEqual(dirty3, want3) {
		t.Fatalf("after ApplyBatch, expected %v, got %v", want3, dirty3)
	}

	// 4. Delete a key, check if it's marked dirty
	s.MarkSnapshotDone() // clear first
	_ = s.Delete("k2")
	dirty4 := s.GetDirtyKeys()
	if len(dirty4) != 1 || dirty4[0] != "k2" {
		t.Fatalf("after Delete, expected [k2], got %v", dirty4)
	}

	// 5. LoadAll should reset dirty keys
	_, _, _ = s.Apply("k-before-load", 1, 1, 1, SourceUnspecified)
	s.LoadAll(map[string]RecordState{"new-k1": {SumAmount: 1}})
	dirty5 := s.GetDirtyKeys()
	if len(dirty5) != 0 {
		t.Fatalf("after LoadAll, expected dirty map to be empty, got %v", dirty5)
	}
}

func TestInMemoryStore_DirtyKeyTracking(t *testing.T) {
	s := NewInMemoryStore()
	testDirtyKeyTracking(t, s)
}

func TestPebbleStore_DirtyKeyTracking(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble_dirty_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	testDirtyKeyTracking(t, s)
}

// --- New tests: partial dirty reset behavior ---

func testPartialDirtyReset(t *testing.T, s Store) {
	// Seed two dirty keys
	s.MarkSnapshotDone()
	_, _, _ = s.Apply("k2", 1, 1, 1, SourceUnspecified)
	_, _, _ = s.Apply("k3", 1, 1, 1, SourceUnspecified)
	// Verify both present
	dk := s.GetDirtyKeys()
	sort.Strings(dk)
	if !reflect.DeepEqual(dk, []string{"k2", "k3"}) {
		t.Fatalf("seed dirty = [k2 k3], got %v", dk)
	}
	// Partial reset only k2
	s.MarkSnapshotDone("k2")
	// Expect only k3 remains
	dk2 := s.GetDirtyKeys()
	if len(dk2) != 1 || dk2[0] != "k3" {
		t.Fatalf("after partial reset k2, expected [k3], got %v", dk2)
	}
	// Full reset clears all
	s.MarkSnapshotDone()
	if len(s.GetDirtyKeys()) != 0 {
		t.Fatalf("after full reset, expected []")
	}
}

func TestInMemoryStore_PartialDirtyReset(t *testing.T) {
	s := NewInMemoryStore()
	testPartialDirtyReset(t, s)
}

func TestPebbleStore_PartialDirtyReset(t *testing.T) {
	dir, err := os.MkdirTemp("", "pebble_partial_dirty_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewPebbleStore(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	testPartialDirtyReset(t, s)
}
