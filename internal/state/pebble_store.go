package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
)

// PebbleStore implements Store using PebbleDB.
type PebbleStore struct {
	db         *pebble.DB
	instanceID string
	dir        string
	closed     bool
	// In-memory set for dirty keys since the last snapshot.
	// This is simpler than using a separate Pebble key-space for this transient data.
	dirtyMu sync.Mutex
	dirty   map[string]struct{}
	// Phase 3: Track checkpoint state for incremental export.
	checkpointMu           sync.Mutex
	lastCheckpointFiles    []string // SSTable files from last checkpoint
	lastCheckpointManifest string   // MANIFEST file name from last checkpoint
}

type pebbleSnapshotView struct {
	snap *pebble.Snapshot
}

func (v *pebbleSnapshotView) Range(fn func(key string, st RecordState) error) error {
	it, _ := v.snap.NewIter(nil)
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		k := append([]byte(nil), it.Key()...)
		vbytes := append([]byte(nil), it.Value()...)
		st, err := decodePebbleState(vbytes)
		if err != nil {
			return err
		}
		if err := fn(string(k), st); err != nil {
			return err
		}
	}
	return nil
}

func (v *pebbleSnapshotView) Close() error { return v.snap.Close() }

func NewPebbleStore(dir string) (*PebbleStore, error) {
	opts := &pebble.Options{
		// Optimized for high throughput
		MemTableSize:             256 << 20,               // 256MB (4x larger)
		MaxConcurrentCompactions: func() int { return 4 }, // More parallel compactions
		L0CompactionThreshold:    4,                       // Start compaction earlier
		L0StopWritesThreshold:    8,                       // Allow more writes before stopping
		WALBytesPerSync:          1 << 20,                 // 1MB WAL sync (vs default 512KB)
		// Disable WAL sync for better performance (trade-off: durability)
		DisableWAL:         false,                             // Keep WAL for durability
		WALMinSyncInterval: func() time.Duration { return 0 }, // No minimum sync interval
	}
	cleanDir := filepath.Clean(dir)
	d, err := pebble.Open(cleanDir, opts)
	if err != nil {
		return nil, fmt.Errorf("pebble open: %w", err)
	}
	return &PebbleStore{db: d, dir: cleanDir, dirty: make(map[string]struct{})}, nil
}

// SetInstanceID sets the instance id used for LastUpdatedBy (transient only).
func (p *PebbleStore) SetInstanceID(id string) { p.instanceID = id }

func (p *PebbleStore) Close() error {
	if p.closed || p.db == nil {
		return nil
	}
	p.closed = true
	return p.db.Close()
}

// NewSnapshotView creates a consistent read-only snapshot using Pebble's snapshot API.
func (p *PebbleStore) NewSnapshotView() (SnapshotView, error) {
	s := p.db.NewSnapshot()
	return &pebbleSnapshotView{snap: s}, nil
}

func encodePebbleState(st RecordState) ([]byte, error) { return json.Marshal(st) }
func decodePebbleState(val []byte) (RecordState, error) {
	var st RecordState
	if err := json.Unmarshal(val, &st); err != nil {
		return RecordState{}, err
	}
	return st, nil
}

func (p *PebbleStore) Apply(key string, deltaAmount int64, deltaQty int64, seq int64, src SourceKind) (bool, RecordState, error) {
	k := []byte(key)
	// Read current
	var cur RecordState
	v, closer, err := p.db.Get(k)
	if err == nil {
		cur, err = decodePebbleState(v)
		_ = closer.Close()
		if err != nil {
			return false, RecordState{}, err
		}
	} else if err != pebble.ErrNotFound {
		return false, RecordState{}, err
	}
	// Idempotency / ordering
	if seq <= cur.LastSeq {
		return false, cur, nil
	}
	// Allow gap similar to InMemory/Badger
	cur.SumAmount += deltaAmount
	cur.SumQty += deltaQty
	cur.LastSeq = seq
	// Set transient field for the returned state (not persisted)
	cur.LastUpdatedBy = p.instanceID
	if src != SourceUnspecified {
		if cur.Sources == nil {
			cur.Sources = make(map[SourceKind]SourceStats)
		}
		ss := cur.Sources[src]
		ss.SumAmount += deltaAmount
		ss.SumQty += deltaQty
		cur.Sources[src] = ss
	}
	bytes, err := encodePebbleState(cur)
	if err != nil {
		return false, RecordState{}, err
	}
	// Use NoSync for better performance (WAL will handle durability)
	if err := p.db.Set(k, bytes, pebble.NoSync); err != nil {
		return false, RecordState{}, err
	}
	p.dirtyMu.Lock()
	p.dirty[key] = struct{}{}
	p.dirtyMu.Unlock()
	return true, cur, nil
}

// ApplyBatch applies a batch of deltas grouped by key using a Pebble write batch.
func (p *PebbleStore) ApplyBatch(batch []Delta) (int, int, error) {
	if len(batch) == 0 {
		return 0, 0, nil
	}
	// Group deltas by key to minimize reads/writes
	groups := make(map[string][]Delta)
	for _, d := range batch {
		groups[d.Key] = append(groups[d.Key], d)
	}
	wb := p.db.NewBatch()
	applied, skipped := 0, 0
	newlyDirty := make([]string, 0, len(groups))
	for key, ds := range groups {
		k := []byte(key)
		// Read current state (if any)
		var cur RecordState
		v, closer, err := p.db.Get(k)
		if err == nil {
			cur, err = decodePebbleState(v)
			_ = closer.Close()
			if err != nil {
				_ = wb.Close()
				return applied, skipped, err
			}
		} else if err != pebble.ErrNotFound {
			_ = wb.Close()
			return applied, skipped, err
		}
		// Apply in-order
		anyApplied := false
		for _, d := range ds {
			if d.Seq <= cur.LastSeq {
				skipped++
				continue
			}
			cur.SumAmount += d.DeltaAmount
			cur.SumQty += d.DeltaQty
			cur.LastSeq = d.Seq
			cur.LastUpdatedBy = p.instanceID // transient
			if d.Source != SourceUnspecified {
				if cur.Sources == nil {
					cur.Sources = make(map[SourceKind]SourceStats)
				}
				ss := cur.Sources[d.Source]
				ss.SumAmount += d.DeltaAmount
				ss.SumQty += d.DeltaQty
				cur.Sources[d.Source] = ss
			}
			applied++
			anyApplied = true
		}
		// Write only if this key had at least one applied
		if anyApplied {
			bytes, err := encodePebbleState(cur)
			if err != nil {
				_ = wb.Close()
				return applied, skipped, err
			}
			if err := wb.Set(k, bytes, nil); err != nil {
				_ = wb.Close()
				return applied, skipped, err
			}
			newlyDirty = append(newlyDirty, key)
		}
	}
	if err := wb.Commit(pebble.NoSync); err != nil {
		_ = wb.Close()
		return applied, skipped, err
	}
	_ = wb.Close()
	if len(newlyDirty) > 0 {
		p.dirtyMu.Lock()
		for _, key := range newlyDirty {
			p.dirty[key] = struct{}{}
		}
		p.dirtyMu.Unlock()
	}
	return applied, skipped, nil
}

func (p *PebbleStore) Get(key string) (RecordState, bool) {
	v, closer, err := p.db.Get([]byte(key))
	if err != nil {
		return RecordState{}, false
	}
	defer closer.Close()
	st, e := decodePebbleState(v)
	if e != nil {
		return RecordState{}, false
	}
	return st, true
}

func (p *PebbleStore) Range(fn func(key string, st RecordState) error) error {
	it, _ := p.db.NewIter(nil)
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		k := append([]byte(nil), it.Key()...)
		v := append([]byte(nil), it.Value()...)
		st, err := decodePebbleState(v)
		if err != nil {
			return err
		}
		if err := fn(string(k), st); err != nil {
			return err
		}
	}
	return nil
}

// LoadAll loads a full snapshot into Pebble by replacing all keys.
func (p *PebbleStore) LoadAll(all map[string]RecordState) {
	// Collect existing keys first, then delete, then write snapshot.
	var toDelete [][]byte
	it, _ := p.db.NewIter(nil)
	for it.First(); it.Valid(); it.Next() {
		k := append([]byte(nil), it.Key()...)
		toDelete = append(toDelete, k)
	}
	it.Close()
	if len(toDelete) > 0 {
		wb := p.db.NewBatch()
		for _, k := range toDelete {
			_ = wb.Delete(k, nil)
		}
		_ = wb.Commit(pebble.NoSync)
		_ = wb.Close()
	}
	if len(all) > 0 {
		wb := p.db.NewBatch()
		for k, st := range all {
			bytes, err := encodePebbleState(st)
			if err != nil {
				continue
			}
			_ = wb.Set([]byte(k), bytes, nil)
		}
		_ = wb.Commit(pebble.NoSync)
		_ = wb.Close()
	}
	p.dirtyMu.Lock()
	p.dirty = make(map[string]struct{}) // Reset dirty map after loading
	p.dirtyMu.Unlock()
}

// LoadPartial applies the provided records without clearing the entire DB.
func (p *PebbleStore) LoadPartial(partial map[string]RecordState) {
	if len(partial) == 0 {
		return
	}
	for k, st := range partial {
		bytes, err := encodePebbleState(st)
		if err != nil {
			continue
		}
		_ = p.db.Set([]byte(k), bytes, pebble.NoSync)
	}
	p.dirtyMu.Lock()
	if p.dirty == nil {
		p.dirty = make(map[string]struct{})
	}
	for k := range partial {
		delete(p.dirty, k)
	}
	p.dirtyMu.Unlock()
}

// ExportSSTables implements CheckpointCapable by creating a Pebble checkpoint and
// copying its SSTable files into dstDir. For now we rely on Pebble's built-in
// Checkpoint, which creates a consistent view of the DB at a point in time.
func (p *PebbleStore) ExportSSTables(dstDir string) ([]string, string, error) {
	// Pebble's Checkpoint API expects dstDir to not exist. Ensure we remove any
	// previous contents before creating a new checkpoint.
	if err := os.RemoveAll(dstDir); err != nil {
		return nil, "", fmt.Errorf("pebble export: cleanup dstDir: %w", err)
	}
	// Ensure memtable/WAL contents are flushed so the checkpoint has all keys.
	if err := p.db.Flush(); err != nil {
		return nil, "", fmt.Errorf("pebble export: flush: %w", err)
	}
	if err := p.db.Checkpoint(dstDir); err != nil {
		return nil, "", fmt.Errorf("pebble export: checkpoint: %w", err)
	}
	// Collect SSTable file names relative to dstDir.
	entries, err := os.ReadDir(dstDir)
	if err != nil {
		return nil, "", fmt.Errorf("pebble export: readdir: %w", err)
	}
	var files []string
	var manifestFile string
	for _, e := range entries {
		// We keep all files; Pebble will validate on open. For manifest bookkeeping
		// we record relative paths.
		files = append(files, e.Name())
		if strings.HasPrefix(e.Name(), "MANIFEST") {
			manifestFile = e.Name()
		}
		full := filepath.Join(dstDir, e.Name())
		f, err := os.Open(full)
		if err != nil {
			return nil, "", fmt.Errorf("pebble export: open %s for fsync: %w", e.Name(), err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, "", fmt.Errorf("pebble export: fsync %s: %w", e.Name(), err)
		}
		if err := f.Close(); err != nil {
			return nil, "", fmt.Errorf("pebble export: close %s: %w", e.Name(), err)
		}
	}
	// Phase 3: Track checkpoint state for incremental export.
	p.checkpointMu.Lock()
	p.lastCheckpointFiles = files
	p.lastCheckpointManifest = manifestFile
	p.checkpointMu.Unlock()
	// Pebble does not currently expose a formal format version via API; we can
	// leave this empty for now or hard-code a placeholder.
	return files, "pebble-unknown", nil
}

// fileWritable implements objstorage.Writable by wrapping an os.File.
type fileWritable struct {
	f *os.File
}

var _ objstorage.Writable = (*fileWritable)(nil) // Ensure fileWritable implements objstorage.Writable

func (fw *fileWritable) Write(p []byte) error {
	_, err := fw.f.Write(p)
	return err
}

func (fw *fileWritable) Finish() error {
	if err := fw.f.Sync(); err != nil {
		fw.f.Close()
		return err
	}
	return fw.f.Close()
}

func (fw *fileWritable) Abort() {
	fw.f.Close()
	// Try to remove the file if it was created
	os.Remove(fw.f.Name())
}

// ExportDeltaSSTables exports only the dirty keys as a new SSTable file.
// This implementation uses sstable.Writer to create an external SSTable
// with sequence numbers set to 0, which is required for ingestion.
func (p *PebbleStore) ExportDeltaSSTables(dstDir string, dirtyKeys []string) ([]string, string, error) {
	if len(dirtyKeys) == 0 {
		return nil, "", fmt.Errorf("pebble export delta: no dirty keys")
	}
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return nil, "", fmt.Errorf("pebble export delta: mkdir: %w", err)
	}

	// Sort keys as required by sstable.Writer.
	sort.Strings(dirtyKeys)

	sstName := "delta.sst"
	sstPath := filepath.Join(dstDir, sstName)

	// Create a file and wrap it in a fileWritable to implement objstorage.Writable
	f, err := os.Create(sstPath)
	if err != nil {
		return nil, "", fmt.Errorf("pebble export delta: create sst file: %w", err)
	}
	writable := &fileWritable{f: f}

	w := sstable.NewWriter(writable, sstable.WriterOptions{})

	for _, k := range dirtyKeys {
		val, closer, err := p.db.Get([]byte(k))
		if err == pebble.ErrNotFound {
			// Key was deleted, represent as a tombstone.
			if err := w.Delete([]byte(k)); err != nil {
				w.Close()
				writable.Abort()
				return nil, "", fmt.Errorf("pebble export delta: write tombstone for %s: %w", k, err)
			}
			continue
		}
		if err != nil {
			w.Close()
			writable.Abort()
			return nil, "", fmt.Errorf("pebble export delta: get key %s: %w", k, err)
		}

		// IMPORTANT: We must copy the value, as the buffer is only valid until closer.Close().
		valCopy := append([]byte(nil), val...)
		_ = closer.Close()

		if err := w.Set([]byte(k), valCopy); err != nil {
			w.Close()
			writable.Abort()
			return nil, "", fmt.Errorf("pebble export delta: write key %s: %w", k, err)
		}
	}

	// Close the writer. The sstable.Writer will handle finishing the writable.
	if err := w.Close(); err != nil {
		writable.Abort()
		return nil, "", fmt.Errorf("pebble export delta: close writer: %w", err)
	}
	// Note: sstable.Writer.Close() should handle finishing the writable, but if it doesn't,
	// we need to ensure the file is synced. Let's sync the file path directly.
	sstFile, err := os.OpenFile(sstPath, os.O_RDWR, 0o644)
	if err == nil {
		sstFile.Sync()
		sstFile.Close()
	}

	return []string{sstName}, "pebble-delta", nil
}

// ImportSSTables implements CheckpointCapable by opening a Pebble DB from the
// SSTables found in srcDir. For simplicity we close the existing DB and reopen
// it pointing at the imported directory. Callers should ensure no concurrent
// use during import.
func (p *PebbleStore) ImportSSTables(srcDir string) error {
	// Close existing DB first.
	if err := p.db.Close(); err != nil {
		return fmt.Errorf("pebble import: close existing db: %w", err)
	}
	// Replace the contents of the state dir with the imported checkpoint.
	if err := os.RemoveAll(p.dir); err != nil {
		return fmt.Errorf("pebble import: cleanup state dir: %w", err)
	}
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return fmt.Errorf("pebble import: recreate state dir: %w", err)
	}
	entries, err := os.ReadDir(filepath.Clean(srcDir))
	if err != nil {
		return fmt.Errorf("pebble import: readdir src: %w", err)
	}
	for _, e := range entries {
		srcPath := filepath.Join(srcDir, e.Name())
		dstPath := filepath.Join(p.dir, e.Name())
		data, err := os.ReadFile(srcPath)
		if err != nil {
			return fmt.Errorf("pebble import: read %s: %w", srcPath, err)
		}
		if err := os.WriteFile(dstPath, data, 0o644); err != nil {
			return fmt.Errorf("pebble import: write %s: %w", dstPath, err)
		}
	}
	// Re-open DB at the original state dir.
	opts := &pebble.Options{
		MemTableSize:             256 << 20,
		MaxConcurrentCompactions: func() int { return 4 },
		L0CompactionThreshold:    4,
		L0StopWritesThreshold:    8,
		WALBytesPerSync:          1 << 20,
		DisableWAL:               false,
		WALMinSyncInterval:       func() time.Duration { return 0 },
	}
	db, err := pebble.Open(p.dir, opts)
	if err != nil {
		return fmt.Errorf("pebble import: open: %w", err)
	}
	p.db = db
	p.closed = false
	// Reset dirty tracking after import.
	p.dirtyMu.Lock()
	p.dirty = make(map[string]struct{})
	p.dirtyMu.Unlock()
	return nil
}

// IngestDeltaSSTables ingests a delta SSTable into the existing DB without
// closing/reopening. This is used for Phase 2 delta restore.
func (p *PebbleStore) IngestDeltaSSTables(srcDir string, files []string) error {
	if len(files) == 0 {
		return nil
	}
	// Pebble's Ingest API requires files to be in a specific location or moved.
	// For simplicity, we'll copy the delta SSTable into a temp location and ingest.
	var paths []string
	for _, f := range files {
		srcPath := filepath.Join(srcDir, f)
		// Ingest expects files to be in a location it can move/link from.
		// We'll use the source path directly if possible, or copy to a temp dir.
		paths = append(paths, srcPath)
	}
	if err := p.db.Ingest(paths); err != nil {
		return fmt.Errorf("pebble ingest delta: %w", err)
	}
	// After ingestion, reset dirty tracking for ingested keys (we don't track which
	// keys were in the delta SSTable here; caller should handle if needed).
	return nil
}

// ExportIncrementalSSTables exports only new SSTable files since the last checkpoint.
// Phase 3: file-level incremental export.
func (p *PebbleStore) ExportIncrementalSSTables(dstDir string) ([]string, []string, string, error) {
	if err := os.RemoveAll(dstDir); err != nil {
		return nil, nil, "", fmt.Errorf("pebble incremental export: cleanup dstDir: %w", err)
	}
	if err := p.db.Flush(); err != nil {
		return nil, nil, "", fmt.Errorf("pebble incremental export: flush: %w", err)
	}
	if err := p.db.Checkpoint(dstDir); err != nil {
		return nil, nil, "", fmt.Errorf("pebble incremental export: checkpoint: %w", err)
	}
	entries, err := os.ReadDir(dstDir)
	if err != nil {
		return nil, nil, "", fmt.Errorf("pebble incremental export: readdir: %w", err)
	}
	var allFiles []string
	var manifestFile string
	for _, e := range entries {
		allFiles = append(allFiles, e.Name())
		if strings.HasPrefix(e.Name(), "MANIFEST") {
			manifestFile = e.Name()
		}
		full := filepath.Join(dstDir, e.Name())
		f, err := os.Open(full)
		if err != nil {
			return nil, nil, "", fmt.Errorf("pebble incremental export: open %s for fsync: %w", e.Name(), err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, nil, "", fmt.Errorf("pebble incremental export: fsync %s: %w", e.Name(), err)
		}
		if err := f.Close(); err != nil {
			return nil, nil, "", fmt.Errorf("pebble incremental export: close %s: %w", e.Name(), err)
		}
	}
	// Determine new files by comparing with lastCheckpointFiles.
	p.checkpointMu.Lock()
	lastFiles := make(map[string]bool)
	for _, f := range p.lastCheckpointFiles {
		lastFiles[f] = true
	}
	var newFiles []string
	for _, f := range allFiles {
		// Always include MANIFEST, CURRENT, LOCK, OPTIONS as they're metadata.
		// For .sst files, only include if not in last checkpoint.
		if strings.HasSuffix(f, ".sst") {
			if !lastFiles[f] {
				newFiles = append(newFiles, f)
			}
		} else {
			// Include all metadata files in newFiles for simplicity.
			newFiles = append(newFiles, f)
		}
	}
	p.lastCheckpointFiles = allFiles
	p.lastCheckpointManifest = manifestFile
	p.checkpointMu.Unlock()
	return newFiles, allFiles, "pebble-incremental", nil
}

// IngestIncrementalFiles ingests incremental SSTable files into the existing DB.
// Phase 3: similar to IngestDeltaSSTables but for file-level incremental.
// Note: Pebble's Ingest requires SSTables to have zero sequence numbers for external ingestion.
// For incremental snapshots, we need to use a different approach or ensure SSTables are properly formatted.
func (p *PebbleStore) IngestIncrementalFiles(srcDir string, files []string) error {
	if len(files) == 0 {
		return nil
	}
	// Filter to only .sst files for ingestion.
	var sstPaths []string
	for _, f := range files {
		if strings.HasSuffix(f, ".sst") {
			sstPaths = append(sstPaths, filepath.Join(srcDir, f))
		}
	}
	if len(sstPaths) == 0 {
		return nil
	}
	// Pebble's Ingest API has limitations with external SSTables that have sequence numbers.
	// For Phase 3, we use a workaround: read keys from the incremental SSTables and write them.
	// This is less efficient but ensures correctness.
	tmpOpts := &pebble.Options{
		DisableWAL:       true,
		ReadOnly:         true,
		ErrorIfNotExists: false,
	}
	tmpDB, err := pebble.Open(srcDir, tmpOpts)
	if err != nil {
		return fmt.Errorf("pebble ingest incremental: open src: %w", err)
	}

	// Iterate over all keys in the source DB and copy to destination.
	iter, _ := tmpDB.NewIter(nil)
	batch := p.db.NewBatch()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		k := append([]byte(nil), iter.Key()...)
		v := append([]byte(nil), iter.Value()...)
		if err := batch.Set(k, v, nil); err != nil {
			iter.Close()
			tmpDB.Close()
			return fmt.Errorf("pebble ingest incremental: batch set: %w", err)
		}
		count++
		if count%1000 == 0 {
			if err := batch.Commit(pebble.NoSync); err != nil {
				iter.Close()
				tmpDB.Close()
				return fmt.Errorf("pebble ingest incremental: batch commit: %w", err)
			}
			batch = p.db.NewBatch()
		}
	}
	iter.Close()
	tmpDB.Close()

	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("pebble ingest incremental: final batch commit: %w", err)
	}
	return nil
}

func (p *PebbleStore) Delete(key string) error {
	p.dirtyMu.Lock()
	p.dirty[key] = struct{}{}
	p.dirtyMu.Unlock()
	return p.db.Delete([]byte(key), pebble.NoSync)
}

// GetDirtyKeys returns a slice of keys that have been modified since the last snapshot.
func (p *PebbleStore) GetDirtyKeys() []string {
	p.dirtyMu.Lock()
	defer p.dirtyMu.Unlock()
	keys := make([]string, 0, len(p.dirty))
	for k := range p.dirty {
		keys = append(keys, k)
	}
	return keys
}

// MarkSnapshotDone clears the dirty key tracking map.
func (p *PebbleStore) MarkSnapshotDone(keys ...string) {
	p.dirtyMu.Lock()
	defer p.dirtyMu.Unlock()
	if len(keys) == 0 {
		p.dirty = make(map[string]struct{}) // Reset dirty map
		return
	}
	for _, k := range keys {
		delete(p.dirty, k)
	}
}
