package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

func main() {
	// Create test data for Person 3 recovery testing
	baseDir := "."

	// 1. Create snapshot with compatible key format
	snapshotID := fmt.Sprintf("p3-snapshot-%d", time.Now().Unix())
	snapshotDir := filepath.Join(baseDir, snapshotID)
	os.MkdirAll(snapshotDir, 0755)

	// Use key format compatible with original: "A#p3#timestamp"
	snapshotData := map[string]state.RecordState{
		"A#p3#1694500000": {SumAmount: 5000, SumQty: 5, LastSeq: 3},
		"A#p3#1694500001": {SumAmount: 3000, SumQty: 3, LastSeq: 2},
	}

	snapshotFile := filepath.Join(snapshotDir, "state.json")
	data, _ := json.Marshal(snapshotData)
	os.WriteFile(snapshotFile, data, 0644)

	// 2. Create P3-specific manifest to avoid conflict with original
	m := manifest.Manifest{
		SnapshotID:           "2025-09-22T14_51_49Z", // Luôn dùng snapshot gốc
		LastChangelogOffset:  2,                      // Offset cho P3
		CreatedAtEpochSecond: time.Now().Unix(),
	}
	manifestData, _ := json.Marshal(m)
	os.WriteFile(filepath.Join(baseDir, "manifest.latest.json"), manifestData, 0644)

	// 3. Create changelog with compatible key format
	os.MkdirAll("changelog", 0755)
	changelogData := []string{
		`{"key":"A#p3#1694500000","seq":2,"delta":1000,"deltaQty":1,"ts":1694500001}`,
		`{"key":"A#p3#1694500000","seq":3,"delta":500,"deltaQty":1,"ts":1694500002}`,
		`{"key":"A#p3#1694500000","seq":4,"delta":300,"deltaQty":1,"ts":1694500003}`,
		`{"key":"A#p3#1694500001","seq":1,"delta":1500,"deltaQty":1,"ts":1694500004}`,
		`{"key":"A#p3#1694500001","seq":2,"delta":1000,"deltaQty":1,"ts":1694500005}`,
		`{"key":"A#p3#1694500002","seq":1,"delta":200,"deltaQty":1,"ts":1694500006}`,
	}

	changelogFile := filepath.Join("changelog", "opb.jsonl") // SỬA TÊN
	f, _ := os.Create(changelogFile)
	for _, line := range changelogData {
		fmt.Fprintln(f, line)
	}
	f.Close()

	// 4. Optional: Create integrated snapshot if original snapshots exist
	createIntegratedSnapshotIfPossible(baseDir, snapshotData)

	log.Printf("Test data generated for Person 3:")
	log.Printf("  - Snapshot: %s", snapshotID)
	log.Printf("  - Manifest: manifest-p3.json (P3-specific)")
	log.Printf("  - Changelog: changelog/opb.jsonl (6 records, offset 2)") // SỬA LOG
}

// createIntegratedSnapshotIfPossible creates a merged snapshot if original snapshots are available
func createIntegratedSnapshotIfPossible(baseDir string, p3Data map[string]state.RecordState) {
	originalSnapshotsDir := filepath.Join(baseDir, "snapshots")
	if _, err := os.Stat(originalSnapshotsDir); os.IsNotExist(err) {
		return // Original snapshots don't exist, skip integration
	}

	// Find latest original snapshot
	latestOriginal := findLatestSnapshot(originalSnapshotsDir)
	if latestOriginal == "" {
		return // No original snapshots found
	}

	// Read original snapshot
	originalPath := filepath.Join(originalSnapshotsDir, latestOriginal, "state.json")
	originalData, err := os.ReadFile(originalPath)
	if err != nil {
		log.Printf("Warning: Could not read original snapshot for integration: %v", err)
		return
	}

	var originalState map[string]state.RecordState
	if err := json.Unmarshal(originalData, &originalState); err != nil {
		log.Printf("Warning: Could not parse original snapshot: %v", err)
		return
	}

	// Merge original and P3 data
	integratedState := make(map[string]state.RecordState)
	for k, v := range originalState {
		integratedState[k] = v
	}
	for k, v := range p3Data {
		integratedState[k] = v
	}

	// Create integrated snapshot directory
	integratedDir := filepath.Join(baseDir, "snapshots-integrated")
	os.MkdirAll(integratedDir, 0755)

	// Write integrated snapshot
	integratedData, _ := json.Marshal(integratedState)
	integratedFile := filepath.Join(integratedDir, "state.json")
	if err := os.WriteFile(integratedFile, integratedData, 0644); err != nil {
		log.Printf("Warning: Could not create integrated snapshot: %v", err)
		return
	}

	log.Printf("  - Integrated: snapshots-integrated/state.json (%d original + %d P3 = %d total keys)",
		len(originalState), len(p3Data), len(integratedState))
}

// findLatestSnapshot finds the latest snapshot directory based on timestamp
func findLatestSnapshot(snapshotsDir string) string {
	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		return ""
	}

	var latest string
	for _, entry := range entries {
		if entry.IsDir() {
			current := entry.Name()
			if latest == "" || current > latest {
				latest = current
			}
		}
	}
	return latest
}
