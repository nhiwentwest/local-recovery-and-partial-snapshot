package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"hpb/internal/manifest"
	"hpb/internal/snapshot"
)

// resolveSnapshotFormat resolves snapshot format from manifest or uses default.
func resolveSnapshotFormat(manifestFormat string, defaultFormat snapshot.Format) snapshot.Format {
	format := defaultFormat
	if manifestFormat != "" {
		if parsed, perr := snapshot.ParseFormat(manifestFormat); perr == nil {
			format = parsed
		} else {
			log.Printf("restore: unknown snapshot format %s, defaulting to %s", manifestFormat, format)
		}
	}
	return format
}

// resolveSnapshotShards resolves snapshot shards from manifest or uses config default.
func resolveSnapshotShards(manifestShards, configShards int) int {
	if manifestShards > 0 {
		return manifestShards
	}
	if configShards > 0 {
		return configShards
	}
	return 1
}

// readSnapshotManifest reads a snapshot manifest from filesystem.
func readSnapshotManifest(snapshotDir, snapID string) (manifest.Manifest, error) {
	if snapID == "" {
		return manifest.Manifest{}, fmt.Errorf("empty snapshot id")
	}
	p := filepath.Join(snapshotDir, snapID, "manifest.json")
	b, err := os.ReadFile(p)
	if err != nil {
		return manifest.Manifest{}, err
	}
	var m manifest.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return manifest.Manifest{}, err
	}
	return m, nil
}

// snapshotSizeBytes calculates total size of snapshot files.
func snapshotSizeBytes(snapshotDir, snapshotID string, format snapshot.Format, shards int) float64 {
	dir := filepath.Join(snapshotDir, snapshotID)
	if shards <= 1 {
		fp := filepath.Join(dir, format.FileName())
		if fi, err := os.Stat(fp); err == nil {
			return float64(fi.Size())
		}
		return 0
	}
	var total float64
	for i := 0; i < shards; i++ {
		fp := filepath.Join(dir, format.FileNameForShard(i, shards))
		if fi, err := os.Stat(fp); err == nil {
			total += float64(fi.Size())
		}
	}
	return total
}

// deltaSnapshotSizeBytes calculates total size of delta snapshot files.
func deltaSnapshotSizeBytes(snapshotDir, snapshotID string, format snapshot.Format, shards int) float64 {
	dir := filepath.Join(snapshotDir, snapshotID)
	if shards <= 1 {
		fp := filepath.Join(dir, format.FileNameDelta())
		if fi, err := os.Stat(fp); err == nil {
			return float64(fi.Size())
		}
		return 0
	}
	var total float64
	for i := 0; i < shards; i++ {
		fp := filepath.Join(dir, format.FileNameDeltaForShard(i, shards))
		if fi, err := os.Stat(fp); err == nil {
			total += float64(fi.Size())
		}
	}
	return total
}

// snapshotIncrementalBytes calculates total size of incremental snapshot files.
func snapshotIncrementalBytes(snapshotDir, snapshotID string, files []string) float64 {
	if len(files) == 0 {
		return 0
	}
	dir := filepath.Join(snapshotDir, snapshotID)
	var total float64
	for _, f := range files {
		fp := filepath.Join(dir, f)
		if fi, err := os.Stat(fp); err == nil {
			total += float64(fi.Size())
		}
	}
	return total
}

