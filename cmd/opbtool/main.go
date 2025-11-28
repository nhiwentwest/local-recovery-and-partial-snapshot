package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble"

	"hpb/internal/manifest"
)

func main() {
	mode := flag.String("mode", "inspect", "tool mode: inspect")
	snapshotDir := flag.String("snapshot-dir", "./snapshots", "snapshot directory")
	snapshotID := flag.String("snapshot-id", "", "snapshot ID to inspect")
	showKeys := flag.Int("keys", 5, "number of sample keys to print when inspecting Pebble snapshots")
	flag.Parse()

	switch strings.ToLower(*mode) {
	case "inspect":
		if err := runInspect(*snapshotDir, *snapshotID, *showKeys); err != nil {
			log.Fatalf("inspect: %v", err)
		}
	default:
		log.Fatalf("unknown mode %s", *mode)
	}
}

func runInspect(baseDir, snapshotID string, keyLimit int) error {
	if snapshotID == "" {
		return fmt.Errorf("snapshot-id is required")
	}
	snapPath := filepath.Join(baseDir, snapshotID)
	manifestPath := filepath.Join(snapPath, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	var m manifest.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}
	fmt.Printf("Snapshot ID: %s\n", m.SnapshotID)
	fmt.Printf("Type: %s (format=%s)\n", m.SnapshotType, m.SnapshotFormat)
	fmt.Printf("Base: %s  Parent: %s  DeltaSeq: %d\n", m.BaseSnapshotID, m.ParentSnapshotID, m.DeltaSequence)
	if len(m.PebbleSSTFiles) > 0 {
		fmt.Printf("Pebble files: %d  incremental new files: %d\n", len(m.PebbleSSTFiles), len(m.PebbleIncrementalFiles))
	}

	if strings.ToLower(m.SnapshotFormat) != "pebble" {
		fmt.Println("Non-Pebble snapshot; raw JSON/Msgpack inspection not implemented")
		return nil
	}
	opts := &pebble.Options{ReadOnly: true, ErrorIfNotExists: true}
	db, err := pebble.Open(snapPath, opts)
	if err != nil {
		return fmt.Errorf("open pebble snapshot: %w", err)
	}
	defer db.Close()

	it, err := db.NewIter(nil)
	if err != nil {
		return fmt.Errorf("new iter: %w", err)
	}
	defer it.Close()

	fmt.Printf("Sample keys (limit %d):\n", keyLimit)
	count := 0
	for it.First(); it.Valid() && count < keyLimit; it.Next() {
		fmt.Printf("  %s => %s\n", string(it.Key()), string(it.Value()))
		count++
	}
	if count == 0 {
		fmt.Println("  (no keys in snapshot)")
	}

	// Print file sizes
	var totalBytes int64
	for _, f := range m.PebbleSSTFiles {
		fp := filepath.Join(snapPath, f)
		if fi, err := os.Stat(fp); err == nil {
			totalBytes += fi.Size()
		}
	}
	fmt.Printf("Approx total bytes: %.2f MB\n", float64(totalBytes)/1024.0/1024.0)
	return nil
}
