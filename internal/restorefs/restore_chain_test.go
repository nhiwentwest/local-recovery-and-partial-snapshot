package restorefs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"hpb/internal/manifest"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

// helpers
func writeManifest(t *testing.T, baseDir, id string, m manifest.Manifest) {
	t.Helper()
	dir := filepath.Join(baseDir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	b, _ := json.MarshalIndent(m, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
}

func writeFullSnapshotJSON(t *testing.T, baseDir, id string, dump map[string]state.RecordState) {
	t.Helper()
	dir := filepath.Join(baseDir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	b, _ := json.MarshalIndent(dump, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "state.json"), b, 0o644); err != nil {
		t.Fatalf("write full snapshot: %v", err)
	}
}

func writeDeltaSnapshotJSON(t *testing.T, baseDir, id string, dump map[string]state.RecordState) {
	t.Helper()
	dir := filepath.Join(baseDir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	b, _ := json.MarshalIndent(dump, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "state.delta.json"), b, 0o644); err != nil {
		t.Fatalf("write delta snapshot: %v", err)
	}
}

func TestRestoreChain_HappyPath(t *testing.T) {
	base := t.TempDir()
	st := state.NewInMemoryStore()
	r := NewRestorerWithOptions(st, nil, manifest.NewFilesystemManifest(base), base, snapshot.FormatJSON, 1)

	// Build: B (full) -> D1 -> D2(latest)
	B := manifest.Manifest{SnapshotID: "B", SnapshotType: manifest.SnapshotTypeFull, SnapshotFormat: "json", SnapshotShards: 1}
	D1 := manifest.Manifest{SnapshotID: "D1", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "B", BaseSnapshotID: "B", DeltaSequence: 1, SnapshotFormat: "json", SnapshotShards: 1}
	D2 := manifest.Manifest{SnapshotID: "D2", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "D1", BaseSnapshotID: "B", DeltaSequence: 2, SnapshotFormat: "json", SnapshotShards: 1}

	writeManifest(t, base, "B", B)
	writeManifest(t, base, "D1", D1)
	writeManifest(t, base, "D2", D2)

	writeFullSnapshotJSON(t, base, "B", map[string]state.RecordState{
		"k1": {SumAmount: 10, SumQty: 1, LastSeq: 1},
	})
	writeDeltaSnapshotJSON(t, base, "D1", map[string]state.RecordState{
		"k1": {SumAmount: 20, SumQty: 2, LastSeq: 2},
	})
	writeDeltaSnapshotJSON(t, base, "D2", map[string]state.RecordState{
		"k2": {SumAmount: 5, SumQty: 1, LastSeq: 1},
	})

	if err := r.RestoreChainFromLatestWithOptions(D2, RestoreOptions{Parallelism: 0, ValidateChain: true}); err != nil {
		t.Fatalf("restore chain: %v", err)
	}
	if v, ok := st.Get("k1"); !ok || v.SumAmount != 20 || v.SumQty != 2 || v.LastSeq != 2 {
		t.Fatalf("k1 mismatch: %+v ok=%v", v, ok)
	}
	if v, ok := st.Get("k2"); !ok || v.SumAmount != 5 || v.SumQty != 1 || v.LastSeq != 1 {
		t.Fatalf("k2 mismatch: %+v ok=%v", v, ok)
	}
}

func TestRestoreChain_MissingDelta_SkipFalse(t *testing.T) {
	base := t.TempDir()
	st := state.NewInMemoryStore()
	r := NewRestorerWithOptions(st, nil, manifest.NewFilesystemManifest(base), base, snapshot.FormatJSON, 1)

	B := manifest.Manifest{SnapshotID: "B", SnapshotType: manifest.SnapshotTypeFull, SnapshotFormat: "json"}
	D1 := manifest.Manifest{SnapshotID: "D1", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "B", BaseSnapshotID: "B", DeltaSequence: 1, SnapshotFormat: "json"}
	D2 := manifest.Manifest{SnapshotID: "D2", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "D1", BaseSnapshotID: "B", DeltaSequence: 2, SnapshotFormat: "json"}

	writeManifest(t, base, "B", B)
	writeManifest(t, base, "D1", D1)
	writeManifest(t, base, "D2", D2)
	writeFullSnapshotJSON(t, base, "B", map[string]state.RecordState{"k": {SumAmount: 1}})
	// NOTE: Intentionally NOT writing D1 delta file
	writeDeltaSnapshotJSON(t, base, "D2", map[string]state.RecordState{"k": {SumAmount: 2}})

	err := r.RestoreChainFromLatestWithOptions(D2, RestoreOptions{ValidateChain: true, SkipMissingDelta: false})
	if err == nil {
		t.Fatalf("expected error for missing delta, got nil")
	}
}

func TestRestoreChain_MissingDelta_SkipTrue(t *testing.T) {
	base := t.TempDir()
	st := state.NewInMemoryStore()
	r := NewRestorerWithOptions(st, nil, manifest.NewFilesystemManifest(base), base, snapshot.FormatJSON, 1)

	B := manifest.Manifest{SnapshotID: "B", SnapshotType: manifest.SnapshotTypeFull, SnapshotFormat: "json"}
	D1 := manifest.Manifest{SnapshotID: "D1", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "B", BaseSnapshotID: "B", DeltaSequence: 1, SnapshotFormat: "json"}
	D2 := manifest.Manifest{SnapshotID: "D2", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "D1", BaseSnapshotID: "B", DeltaSequence: 2, SnapshotFormat: "json"}

	writeManifest(t, base, "B", B)
	writeManifest(t, base, "D1", D1)
	writeManifest(t, base, "D2", D2)
	writeFullSnapshotJSON(t, base, "B", map[string]state.RecordState{"k": {SumAmount: 1}})
	// Missing D1 file; present D2
	writeDeltaSnapshotJSON(t, base, "D2", map[string]state.RecordState{"k": {SumAmount: 3}})

	if err := r.RestoreChainFromLatestWithOptions(D2, RestoreOptions{ValidateChain: true, SkipMissingDelta: true}); err != nil {
		t.Fatalf("unexpected error with SkipMissingDelta=true: %v", err)
	}
	if v, ok := st.Get("k"); !ok || v.SumAmount != 3 {
		t.Fatalf("want k sum=3 after skipping D1, got %+v ok=%v", v, ok)
	}
}

func TestValidateChain_BrokenParentLink(t *testing.T) {
	base := t.TempDir()
	st := state.NewInMemoryStore()
	r := NewRestorerWithOptions(st, nil, manifest.NewFilesystemManifest(base), base, snapshot.FormatJSON, 1)

	B := manifest.Manifest{SnapshotID: "B", SnapshotType: manifest.SnapshotTypeFull}
	D1 := manifest.Manifest{SnapshotID: "D1", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "B-MISSING", BaseSnapshotID: "B", DeltaSequence: 1}
	writeManifest(t, base, "B", B)
	writeManifest(t, base, "D1", D1)
	// files for base to pass file existence if it ever gets there
	writeFullSnapshotJSON(t, base, "B", map[string]state.RecordState{"k": {SumAmount: 1}})

	if _, err := r.validateChainIntegrity(D1); err == nil {
		t.Fatalf("expected chain validation error for broken parent link")
	}
}

func TestValidateChain_CycleDetection(t *testing.T) {
	base := t.TempDir()
	st := state.NewInMemoryStore()
	r := NewRestorerWithOptions(st, nil, manifest.NewFilesystemManifest(base), base, snapshot.FormatJSON, 1)

	D1 := manifest.Manifest{SnapshotID: "D1", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "D2", BaseSnapshotID: "B", DeltaSequence: 1}
	D2 := manifest.Manifest{SnapshotID: "D2", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "D1", BaseSnapshotID: "B", DeltaSequence: 2}
	writeManifest(t, base, "D1", D1)
	writeManifest(t, base, "D2", D2)

	if _, err := r.validateChainIntegrity(D2); err == nil {
		t.Fatalf("expected cycle detection error, got nil")
	}
}

func TestValidateChain_InvalidDeltaSequence(t *testing.T) {
	base := t.TempDir()
	st := state.NewInMemoryStore()
	r := NewRestorerWithOptions(st, nil, manifest.NewFilesystemManifest(base), base, snapshot.FormatJSON, 1)

	B := manifest.Manifest{SnapshotID: "B", SnapshotType: manifest.SnapshotTypeFull}
	D1 := manifest.Manifest{SnapshotID: "D1", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "B", BaseSnapshotID: "B", DeltaSequence: 2}
	writeManifest(t, base, "B", B)
	writeManifest(t, base, "D1", D1)
	if _, err := r.validateChainIntegrity(D1); err == nil {
		t.Fatalf("expected invalid delta sequence error")
	}
}

func TestValidateChain_BaseNotFull(t *testing.T) {
	base := t.TempDir()
	st := state.NewInMemoryStore()
	r := NewRestorerWithOptions(st, nil, manifest.NewFilesystemManifest(base), base, snapshot.FormatJSON, 1)

	// Latest is a delta pointing to another delta; no full base exists
	D1 := manifest.Manifest{SnapshotID: "D1", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "D0", BaseSnapshotID: "D0", DeltaSequence: 1}
	D0 := manifest.Manifest{SnapshotID: "D0", SnapshotType: manifest.SnapshotTypeDelta}
	writeManifest(t, base, "D0", D0)
	writeManifest(t, base, "D1", D1)
	if _, err := r.validateChainIntegrity(D1); err == nil {
		t.Fatalf("expected base-not-full error")
	}
}

func TestValidateFiles_MissingBaseFile(t *testing.T) {
	base := t.TempDir()
	st := state.NewInMemoryStore()
	r := NewRestorerWithOptions(st, nil, manifest.NewFilesystemManifest(base), base, snapshot.FormatJSON, 1)

	B := manifest.Manifest{SnapshotID: "B", SnapshotType: manifest.SnapshotTypeFull, SnapshotFormat: "json"}
	writeManifest(t, base, "B", B)
	// INTENTIONALLY do not create state.json
	if err := r.validateSnapshotFiles(B); err == nil {
		t.Fatalf("expected missing base file error")
	}
}

// TestRestoreChain_ManyDeltas tests restore with a longer chain (base + 5 deltas)
func TestRestoreChain_ManyDeltas(t *testing.T) {
	base := t.TempDir()
	st := state.NewInMemoryStore()
	r := NewRestorerWithOptions(st, nil, manifest.NewFilesystemManifest(base), base, snapshot.FormatJSON, 1)

	// Build: B (full) -> D1 -> D2 -> D3 -> D4 -> D5 (latest)
	B := manifest.Manifest{SnapshotID: "B", SnapshotType: manifest.SnapshotTypeFull, SnapshotFormat: "json", SnapshotShards: 1}
	D1 := manifest.Manifest{SnapshotID: "D1", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "B", BaseSnapshotID: "B", DeltaSequence: 1, SnapshotFormat: "json", SnapshotShards: 1}
	D2 := manifest.Manifest{SnapshotID: "D2", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "D1", BaseSnapshotID: "B", DeltaSequence: 2, SnapshotFormat: "json", SnapshotShards: 1}
	D3 := manifest.Manifest{SnapshotID: "D3", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "D2", BaseSnapshotID: "B", DeltaSequence: 3, SnapshotFormat: "json", SnapshotShards: 1}
	D4 := manifest.Manifest{SnapshotID: "D4", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "D3", BaseSnapshotID: "B", DeltaSequence: 4, SnapshotFormat: "json", SnapshotShards: 1}
	D5 := manifest.Manifest{SnapshotID: "D5", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "D4", BaseSnapshotID: "B", DeltaSequence: 5, SnapshotFormat: "json", SnapshotShards: 1}

	writeManifest(t, base, "B", B)
	writeManifest(t, base, "D1", D1)
	writeManifest(t, base, "D2", D2)
	writeManifest(t, base, "D3", D3)
	writeManifest(t, base, "D4", D4)
	writeManifest(t, base, "D5", D5)

	// Base: k1=10, k2=5
	writeFullSnapshotJSON(t, base, "B", map[string]state.RecordState{
		"k1": {SumAmount: 10, SumQty: 1, LastSeq: 1},
		"k2": {SumAmount: 5, SumQty: 1, LastSeq: 1},
	})
	// D1: update k1=20
	writeDeltaSnapshotJSON(t, base, "D1", map[string]state.RecordState{
		"k1": {SumAmount: 20, SumQty: 2, LastSeq: 2},
	})
	// D2: add k3=15
	writeDeltaSnapshotJSON(t, base, "D2", map[string]state.RecordState{
		"k3": {SumAmount: 15, SumQty: 1, LastSeq: 1},
	})
	// D3: update k2=25
	writeDeltaSnapshotJSON(t, base, "D3", map[string]state.RecordState{
		"k2": {SumAmount: 25, SumQty: 3, LastSeq: 2},
	})
	// D4: update k1=30, k3=20
	writeDeltaSnapshotJSON(t, base, "D4", map[string]state.RecordState{
		"k1": {SumAmount: 30, SumQty: 3, LastSeq: 3},
		"k3": {SumAmount: 20, SumQty: 2, LastSeq: 2},
	})
	// D5: add k4=100
	writeDeltaSnapshotJSON(t, base, "D5", map[string]state.RecordState{
		"k4": {SumAmount: 100, SumQty: 1, LastSeq: 1},
	})

	if err := r.RestoreChainFromLatestWithOptions(D5, RestoreOptions{Parallelism: 0, ValidateChain: true, SkipMissingDelta: false}); err != nil {
		t.Fatalf("restore chain: %v", err)
	}

	// Verify final state: k1=30, k2=25, k3=20, k4=100
	if v, ok := st.Get("k1"); !ok || v.SumAmount != 30 || v.SumQty != 3 || v.LastSeq != 3 {
		t.Fatalf("k1 mismatch: %+v ok=%v", v, ok)
	}
	if v, ok := st.Get("k2"); !ok || v.SumAmount != 25 || v.SumQty != 3 || v.LastSeq != 2 {
		t.Fatalf("k2 mismatch: %+v ok=%v", v, ok)
	}
	if v, ok := st.Get("k3"); !ok || v.SumAmount != 20 || v.SumQty != 2 || v.LastSeq != 2 {
		t.Fatalf("k3 mismatch: %+v ok=%v", v, ok)
	}
	if v, ok := st.Get("k4"); !ok || v.SumAmount != 100 || v.SumQty != 1 || v.LastSeq != 1 {
		t.Fatalf("k4 mismatch: %+v ok=%v", v, ok)
	}
}

// TestRestoreChain_SkipMissingDelta_Multiple tests skipping multiple missing deltas
func TestRestoreChain_SkipMissingDelta_Multiple(t *testing.T) {
	base := t.TempDir()
	st := state.NewInMemoryStore()
	r := NewRestorerWithOptions(st, nil, manifest.NewFilesystemManifest(base), base, snapshot.FormatJSON, 1)

	B := manifest.Manifest{SnapshotID: "B", SnapshotType: manifest.SnapshotTypeFull, SnapshotFormat: "json", SnapshotShards: 1}
	D1 := manifest.Manifest{SnapshotID: "D1", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "B", BaseSnapshotID: "B", DeltaSequence: 1, SnapshotFormat: "json", SnapshotShards: 1}
	D2 := manifest.Manifest{SnapshotID: "D2", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "D1", BaseSnapshotID: "B", DeltaSequence: 2, SnapshotFormat: "json", SnapshotShards: 1}
	D3 := manifest.Manifest{SnapshotID: "D3", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "D2", BaseSnapshotID: "B", DeltaSequence: 3, SnapshotFormat: "json", SnapshotShards: 1}

	writeManifest(t, base, "B", B)
	writeManifest(t, base, "D1", D1)
	writeManifest(t, base, "D2", D2)
	writeManifest(t, base, "D3", D3)

	writeFullSnapshotJSON(t, base, "B", map[string]state.RecordState{
		"k": {SumAmount: 1, SumQty: 1, LastSeq: 1},
	})
	// Missing D1 and D2 files; only D3 present
	writeDeltaSnapshotJSON(t, base, "D3", map[string]state.RecordState{
		"k": {SumAmount: 10, SumQty: 2, LastSeq: 3},
	})

	if err := r.RestoreChainFromLatestWithOptions(D3, RestoreOptions{ValidateChain: true, SkipMissingDelta: true}); err != nil {
		t.Fatalf("unexpected error with SkipMissingDelta=true: %v", err)
	}
	// Should have base k=1, then D3 applies k=10 (D1 and D2 skipped)
	if v, ok := st.Get("k"); !ok || v.SumAmount != 10 || v.SumQty != 2 || v.LastSeq != 3 {
		t.Fatalf("want k sum=10 qty=2 seq=3 after skipping D1+D2, got %+v ok=%v", v, ok)
	}
}

// TestRestoreChain_ValidateChainFalse tests restore with validation disabled
func TestRestoreChain_ValidateChainFalse(t *testing.T) {
	base := t.TempDir()
	st := state.NewInMemoryStore()
	r := NewRestorerWithOptions(st, nil, manifest.NewFilesystemManifest(base), base, snapshot.FormatJSON, 1)

	B := manifest.Manifest{SnapshotID: "B", SnapshotType: manifest.SnapshotTypeFull, SnapshotFormat: "json", SnapshotShards: 1}
	D1 := manifest.Manifest{SnapshotID: "D1", SnapshotType: manifest.SnapshotTypeDelta, ParentSnapshotID: "B", BaseSnapshotID: "B", DeltaSequence: 1, SnapshotFormat: "json", SnapshotShards: 1}

	writeManifest(t, base, "B", B)
	writeManifest(t, base, "D1", D1)

	writeFullSnapshotJSON(t, base, "B", map[string]state.RecordState{
		"k1": {SumAmount: 5, SumQty: 1, LastSeq: 1},
	})
	writeDeltaSnapshotJSON(t, base, "D1", map[string]state.RecordState{
		"k1": {SumAmount: 15, SumQty: 2, LastSeq: 2},
		"k2": {SumAmount: 20, SumQty: 1, LastSeq: 1},
	})

	if err := r.RestoreChainFromLatestWithOptions(D1, RestoreOptions{ValidateChain: false, SkipMissingDelta: false}); err != nil {
		t.Fatalf("restore chain with ValidateChain=false: %v", err)
	}
	if v, ok := st.Get("k1"); !ok || v.SumAmount != 15 || v.SumQty != 2 {
		t.Fatalf("k1 mismatch: %+v ok=%v", v, ok)
	}
	if v, ok := st.Get("k2"); !ok || v.SumAmount != 20 || v.SumQty != 1 {
		t.Fatalf("k2 mismatch: %+v ok=%v", v, ok)
	}
}

func TestRestoreChain_PebbleBaseWithDelta(t *testing.T) {
	snapDir := t.TempDir()
	baseStateDir := t.TempDir()
	baseStore, err := state.NewPebbleStore(baseStateDir)
	if err != nil {
		t.Fatalf("pebble store: %v", err)
	}
	defer baseStore.Close()
	if _, _, err := baseStore.Apply("k1", 100, 1, 1); err != nil {
		t.Fatalf("apply k1: %v", err)
	}
	if _, _, err := baseStore.Apply("k2", 200, 2, 2); err != nil {
		t.Fatalf("apply k2: %v", err)
	}
	pebSnap := snapshot.NewPebbleSnapshotter(snapDir)
	res, err := pebSnap.WriteSnapshot("B", baseStore)
	if err != nil {
		t.Fatalf("write pebble snapshot: %v", err)
	}
	writeManifest(t, snapDir, "B", manifest.Manifest{
		SnapshotID:           "B",
		SnapshotType:         manifest.SnapshotTypeFull,
		SnapshotFormat:       res.Format.String(),
		PebbleSSTFiles:       append([]string(nil), res.PebbleSSTFiles...),
		PebbleSSTChecksums:   res.PebbleSSTChecksums,
		PebbleFormatVersion:  res.PebbleFormatVersion,
		CreatedAtEpochSecond: time.Now().Unix(),
	})
	writeDeltaSnapshotJSON(t, snapDir, "D1", map[string]state.RecordState{
		"k2": {SumAmount: 500, SumQty: 5, LastSeq: 5},
		"k3": {SumAmount: 50, SumQty: 1, LastSeq: 1},
	})
	writeManifest(t, snapDir, "D1", manifest.Manifest{
		SnapshotID:       "D1",
		SnapshotType:     manifest.SnapshotTypeDelta,
		ParentSnapshotID: "B",
		BaseSnapshotID:   "B",
		DeltaSequence:    1,
		SnapshotFormat:   "json",
	})

	restoreStateDir := t.TempDir()
	restoreStore, err := state.NewPebbleStore(restoreStateDir)
	if err != nil {
		t.Fatalf("restore pebble store: %v", err)
	}
	defer restoreStore.Close()
	r := NewRestorerWithOptions(restoreStore, nil, manifest.NewFilesystemManifest(snapDir), snapDir, snapshot.FormatJSON, 1)
	latest := manifest.Manifest{
		SnapshotID:       "D1",
		SnapshotType:     manifest.SnapshotTypeDelta,
		ParentSnapshotID: "B",
		BaseSnapshotID:   "B",
	}
	if err := r.RestoreChainFromLatestWithOptions(latest, RestoreOptions{Parallelism: 0, ValidateChain: true}); err != nil {
		t.Fatalf("restore chain pebble+delta: %v", err)
	}
	if v, ok := restoreStore.Get("k1"); !ok || v.SumAmount != 100 || v.SumQty != 1 {
		t.Fatalf("k1 mismatch after restore: %+v ok=%v", v, ok)
	}
	if v, ok := restoreStore.Get("k2"); !ok || v.SumAmount != 500 || v.SumQty != 5 {
		t.Fatalf("k2 mismatch after restore: %+v ok=%v", v, ok)
	}
	if v, ok := restoreStore.Get("k3"); !ok || v.SumAmount != 50 || v.SumQty != 1 {
		t.Fatalf("k3 mismatch after restore: %+v ok=%v", v, ok)
	}
}
