package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"hpb/internal/state"
)

// PebbleSnapshotter is an experimental snapshotter that uses the underlying
// Pebble store's checkpoint capability to export/import SSTables instead of
// emitting a logical JSON/msgpack dump. It is only valid when the state.Store
// implements state.CheckpointCapable and the configured format is FormatPebble.
type PebbleSnapshotter struct {
	baseDir string
}

func NewPebbleSnapshotter(baseDir string) *PebbleSnapshotter {
	return &PebbleSnapshotter{baseDir: baseDir}
}

// WriteSnapshot exports SSTables from the underlying Pebble store into the
// snapshot directory. The snapshotID must match the manifest's snapshotId.
func (p *PebbleSnapshotter) WriteSnapshot(snapshotID string, st state.Store) (Result, error) {
	if snapshotID == "" {
		return Result{}, fmt.Errorf("pebble snapshot: empty snapshotID")
	}
	capable, ok := st.(state.CheckpointCapable)
	if !ok {
		return Result{}, fmt.Errorf("pebble snapshot: store does not support checkpoint")
	}
	snapDir := filepath.Join(p.baseDir, snapshotID)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return Result{}, fmt.Errorf("pebble snapshot: mkdir: %w", err)
	}
	files, formatVersion, err := capable.ExportSSTables(snapDir)
	if err != nil {
		return Result{}, fmt.Errorf("pebble snapshot: export sstables: %w", err)
	}
	// Compute SHA256 checksums for each exported SSTable to allow validation on restore.
	checksums := make(map[string]string, len(files))
	for _, f := range files {
		full := filepath.Join(snapDir, f)
		fd, err := os.Open(full)
		if err != nil {
			return Result{}, fmt.Errorf("pebble snapshot: open sstable %s: %w", f, err)
		}
		h := sha256.New()
		if _, err := io.Copy(h, fd); err != nil {
			fd.Close()
			return Result{}, fmt.Errorf("pebble snapshot: checksum sstable %s: %w", f, err)
		}
		if err := fd.Close(); err != nil {
			return Result{}, fmt.Errorf("pebble snapshot: close sstable %s: %w", f, err)
		}
		checksums[f] = hex.EncodeToString(h.Sum(nil))
	}
	// We don't know exact key count here without scanning; leave Keys=0 and
	// let manifest stats or separate processes fill it if needed.
	return Result{
		Format:              FormatPebble,
		Shards:              1,
		Keys:                0,
		PebbleSSTFiles:      files,
		PebbleFormatVersion: formatVersion,
		PebbleSSTChecksums:  checksums,
	}, nil
}

// WriteDeltaSnapshot exports only dirty keys as a delta SSTable (Phase 2).
func (p *PebbleSnapshotter) WriteDeltaSnapshot(snapshotID string, st state.Store, dirtyKeys []string) (Result, error) {
	if snapshotID == "" {
		return Result{}, fmt.Errorf("pebble delta snapshot: empty snapshotID")
	}
	if len(dirtyKeys) == 0 {
		return Result{}, fmt.Errorf("pebble delta snapshot: no dirty keys")
	}
	capable, ok := st.(state.DeltaCheckpointCapable)
	if !ok {
		return Result{}, fmt.Errorf("pebble delta snapshot: store does not support delta checkpoint")
	}
	snapDir := filepath.Join(p.baseDir, snapshotID)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return Result{}, fmt.Errorf("pebble delta snapshot: mkdir: %w", err)
	}
	files, formatVersion, err := capable.ExportDeltaSSTables(snapDir, dirtyKeys)
	if err != nil {
		return Result{}, fmt.Errorf("pebble delta snapshot: export delta sstables: %w", err)
	}
	// Compute checksums for delta SSTables.
	checksums := make(map[string]string, len(files))
	for _, f := range files {
		full := filepath.Join(snapDir, f)
		fd, err := os.Open(full)
		if err != nil {
			return Result{}, fmt.Errorf("pebble delta snapshot: open sstable %s: %w", f, err)
		}
		h := sha256.New()
		if _, err := io.Copy(h, fd); err != nil {
			fd.Close()
			return Result{}, fmt.Errorf("pebble delta snapshot: checksum sstable %s: %w", f, err)
		}
		if err := fd.Close(); err != nil {
			return Result{}, fmt.Errorf("pebble delta snapshot: close sstable %s: %w", f, err)
		}
		checksums[f] = hex.EncodeToString(h.Sum(nil))
	}
	return Result{
		Format:              FormatPebble,
		Shards:              1,
		Keys:                len(dirtyKeys),
		PebbleSSTFiles:      files,
		PebbleFormatVersion: formatVersion,
		PebbleSSTChecksums:  checksums,
	}, nil
}

// WriteIncrementalSnapshot exports only new SSTable files since the last checkpoint (Phase 3).
func (p *PebbleSnapshotter) WriteIncrementalSnapshot(snapshotID string, st state.Store) (Result, error) {
	if snapshotID == "" {
		return Result{}, fmt.Errorf("pebble incremental snapshot: empty snapshotID")
	}
	capable, ok := st.(state.IncrementalCheckpointCapable)
	if !ok {
		return Result{}, fmt.Errorf("pebble incremental snapshot: store does not support incremental checkpoint")
	}
	snapDir := filepath.Join(p.baseDir, snapshotID)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return Result{}, fmt.Errorf("pebble incremental snapshot: mkdir: %w", err)
	}
	newFiles, allFiles, formatVersion, err := capable.ExportIncrementalSSTables(snapDir)
	if err != nil {
		return Result{}, fmt.Errorf("pebble incremental snapshot: export incremental sstables: %w", err)
	}
	// Compute checksums for all files (including new ones).
	checksums := make(map[string]string, len(allFiles))
	for _, f := range allFiles {
		full := filepath.Join(snapDir, f)
		fd, err := os.Open(full)
		if err != nil {
			return Result{}, fmt.Errorf("pebble incremental snapshot: open sstable %s: %w", f, err)
		}
		h := sha256.New()
		if _, err := io.Copy(h, fd); err != nil {
			fd.Close()
			return Result{}, fmt.Errorf("pebble incremental snapshot: checksum sstable %s: %w", f, err)
		}
		if err := fd.Close(); err != nil {
			return Result{}, fmt.Errorf("pebble incremental snapshot: close sstable %s: %w", f, err)
		}
		checksums[f] = hex.EncodeToString(h.Sum(nil))
	}
	return Result{
		Format:                 FormatPebble,
		Shards:                 1,
		Keys:                   0, // unknown without scanning
		PebbleSSTFiles:         allFiles,
		PebbleFormatVersion:    formatVersion,
		PebbleSSTChecksums:     checksums,
		PebbleIncrementalFiles: newFiles,
	}, nil
}
