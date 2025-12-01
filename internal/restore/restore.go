package restore

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"hpb/internal/changelog"
	"hpb/internal/manifest"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

type Restorer struct {
	stateStore      state.Store
	snapshotter     snapshot.Snapshotter
	manifestReader  manifest.Reader
	snapshotBaseDir string
	defaultFormat   snapshot.Format
	defaultShards   int
	statsMu         sync.Mutex
	lastStats       SnapshotStats
}

type Reader interface {
	ReadLatest() (manifest.Manifest, error)
}

type FilesystemReader struct {
	baseDir string
}

func NewFilesystemReader(baseDir string) *FilesystemReader {
	return &FilesystemReader{baseDir: baseDir}
}

func (r *FilesystemReader) ReadLatest() (manifest.Manifest, error) {
	file := filepath.Join(r.baseDir, "manifest.latest.json")
	data, err := os.ReadFile(file)
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("read manifest: %w", err)
	}
	var m manifest.Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return manifest.Manifest{}, fmt.Errorf("unmarshal manifest: %w", err)
	}
	return m, nil
}

// Kafka manifest reader moved to integration build (see restore_kafka.go).

func NewRestorer(st state.Store, snap snapshot.Snapshotter, mr manifest.Reader, snapshotBaseDir string) *Restorer {
	return NewRestorerWithFormat(st, snap, mr, snapshotBaseDir, snapshot.FormatJSON)
}

func NewRestorerWithFormat(st state.Store, snap snapshot.Snapshotter, mr manifest.Reader, snapshotBaseDir string, format snapshot.Format) *Restorer {
	return NewRestorerWithOptions(st, snap, mr, snapshotBaseDir, format, 1)
}

func NewRestorerWithOptions(st state.Store, snap snapshot.Snapshotter, mr manifest.Reader, snapshotBaseDir string, format snapshot.Format, shards int) *Restorer {
	if format == "" {
		format = snapshot.FormatJSON
	}
	if shards < 1 {
		shards = 1
	}
	return &Restorer{
		stateStore:      st,
		snapshotter:     snap,
		manifestReader:  mr,
		snapshotBaseDir: snapshotBaseDir,
		defaultFormat:   format,
		defaultShards:   shards,
	}
}

type RestoreResult struct {
	Applied int
	Skipped int
	// Bytes is the total bytes of replayed deltas (Kafka/file)
	Bytes int64
	// LastAppliedOffset is the Kafka offset of the last applied delta (Kafka only)
	LastAppliedOffset int64
	Error             error
}

type SnapshotStats struct {
	Shards     int
	Keys       int
	ReadNs     int64
	DecodeNs   int64
	LoadNs     int64
	Format     snapshot.Format
	SnapshotID string
}

func (r *Restorer) setSnapshotStats(stats SnapshotStats) {
	r.statsMu.Lock()
	r.lastStats = stats
	r.statsMu.Unlock()
}

func (r *Restorer) LastSnapshotStats() SnapshotStats {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	return r.lastStats
}

func (r *Restorer) RestoreFromSnapshot(snapshotID string) error {
	return r.RestoreFromSnapshotWithFormat(snapshotID, r.defaultFormat, r.defaultShards, 0)
}

func (r *Restorer) RestoreFromSnapshotWithFormat(snapshotID string, format snapshot.Format, shards int, keysHint int) error {
	if snapshotID == "" {
		return nil
	}
	if format == "" {
		format = r.defaultFormat
	}
	if shards <= 0 {
		shards = r.defaultShards
	}
	if shards <= 0 {
		shards = 1
	}
	baseDir := filepath.Join(r.snapshotBaseDir, snapshotID)

	// Pebble-specific restore path is handled via restorePebbleFromManifest when using
	// RestoreAndReplay, since it has access to manifest metadata (SST files + checksums).
	// For direct calls, we keep the legacy behavior of delegating to CheckpointCapable
	// without checksum validation for backward compatibility.
	if format == snapshot.FormatPebble {
		if cap, ok := r.stateStore.(state.CheckpointCapable); ok {
			if err := cap.ImportSSTables(baseDir); err != nil {
				return fmt.Errorf("restore pebble snapshot: %w", err)
			}
			// We don't have precise key stats without scanning; record minimal stats.
			r.setSnapshotStats(SnapshotStats{
				Shards:     1,
				Keys:       0,
				ReadNs:     0,
				DecodeNs:   0,
				LoadNs:     0,
				Format:     format,
				SnapshotID: snapshotID,
			})
			log.Printf("restore: restored Pebble snapshot %s from %s", snapshotID, baseDir)
			return nil
		}
		// Fallback: if store is not checkpoint-capable, treat as JSON backend.
		format = snapshot.FormatJSON
	}
	if shards <= 1 {
		path := filepath.Join(baseDir, format.FileName())
		readStart := time.Now()
		data, err := os.ReadFile(path)
		readDur := time.Since(readStart)
		if err != nil {
			if os.IsNotExist(err) && format == snapshot.FormatMsgpack {
				format = snapshot.FormatJSON
				path = filepath.Join(baseDir, format.FileName())
				readStart = time.Now()
				data, err = os.ReadFile(path)
				readDur = time.Since(readStart)
			}
			if os.IsNotExist(err) {
				log.Printf("restore: snapshot not found at %s, skipping", path)
				return nil
			}
			return fmt.Errorf("read snapshot: %w", err)
		}
		decodeStart := time.Now()
		dump, err := snapshot.DecodeSnapshot(data, format)
		decodeDur := time.Since(decodeStart)
		if err != nil {
			return err
		}
		loadStart := time.Now()
		r.stateStore.LoadAll(dump)
		loadDur := time.Since(loadStart)
		r.setSnapshotStats(SnapshotStats{
			Shards:     1,
			Keys:       len(dump),
			ReadNs:     readDur.Nanoseconds(),
			DecodeNs:   decodeDur.Nanoseconds(),
			LoadNs:     loadDur.Nanoseconds(),
			Format:     format,
			SnapshotID: snapshotID,
		})
		log.Printf("restore: loaded %d keys from snapshot %s", len(dump), snapshotID)
		return nil
	}
	firstShard := filepath.Join(baseDir, format.FileNameForShard(0, shards))
	if _, err := os.Stat(firstShard); os.IsNotExist(err) {
		return r.RestoreFromSnapshotWithFormat(snapshotID, format, 1, keysHint)
	}
	var merged map[string]state.RecordState
	if keysHint > 0 {
		merged = make(map[string]state.RecordState, keysHint)
	} else {
		merged = make(map[string]state.RecordState)
	}
	var readNs, decodeNs int64
	for i := 0; i < shards; i++ {
		fp := filepath.Join(baseDir, format.FileNameForShard(i, shards))
		readStart := time.Now()
		data, err := os.ReadFile(fp)
		readNs += time.Since(readStart).Nanoseconds()
		if err != nil {
			return fmt.Errorf("read shard %d: %w", i, err)
		}
		decodeStart := time.Now()
		dump, err := snapshot.DecodeSnapshot(data, format)
		decodeNs += time.Since(decodeStart).Nanoseconds()
		if err != nil {
			return fmt.Errorf("decode shard %d: %w", i, err)
		}
		for k, v := range dump {
			merged[k] = v
		}
	}
	loadStart := time.Now()
	r.stateStore.LoadAll(merged)
	loadDur := time.Since(loadStart)
	r.setSnapshotStats(SnapshotStats{
		Shards:     shards,
		Keys:       len(merged),
		ReadNs:     readNs,
		DecodeNs:   decodeNs,
		LoadNs:     loadDur.Nanoseconds(),
		Format:     format,
		SnapshotID: snapshotID,
	})
	log.Printf("restore: loaded %d keys from snapshot %s (shards=%d)", len(merged), snapshotID, shards)
	return nil
}

func (r *Restorer) ReplayChangelog(changelogPath string, fromOffset int64) RestoreResult {
	file, err := os.Open(changelogPath)
	if err != nil {
		return RestoreResult{Error: fmt.Errorf("open changelog: %w", err)}
	}
	defer file.Close()
	return r.replayLines(file, fromOffset)
}

// ReplayChangelogKafka consumes deltas from Kafka topic (partition 0) and applies them.
// fromOffset here is interpreted as message index (dev simplification).
// Kafka replay moved to integration build (see restore_kafka.go).

func (r *Restorer) restorePebbleFromManifest(m manifest.Manifest) error {
	baseDir := filepath.Join(r.snapshotBaseDir, m.SnapshotID)
	// Validate presence and checksums for all declared SSTables when checksums are available.
	for _, f := range m.PebbleSSTFiles {
		full := filepath.Join(baseDir, f)
		if _, err := os.Stat(full); err != nil {
			return fmt.Errorf("restore pebble: missing sstable %s: %w", f, err)
		}
		if m.PebbleSSTChecksums == nil {
			continue
		}
		want, ok := m.PebbleSSTChecksums[f]
		if !ok || want == "" {
			continue
		}
		fd, err := os.Open(full)
		if err != nil {
			return fmt.Errorf("restore pebble: open sstable %s: %w", f, err)
		}
		h := sha256.New()
		if _, err := io.Copy(h, fd); err != nil {
			fd.Close()
			return fmt.Errorf("restore pebble: checksum sstable %s: %w", f, err)
		}
		if err := fd.Close(); err != nil {
			return fmt.Errorf("restore pebble: close sstable %s: %w", f, err)
		}
		got := hex.EncodeToString(h.Sum(nil))
		if got != want {
			return fmt.Errorf("restore pebble: checksum mismatch for %s: have=%s want=%s", f, got, want)
		}
	}
	cap, ok := r.stateStore.(state.CheckpointCapable)
	if !ok {
		return fmt.Errorf("restore pebble: state store is not checkpoint-capable")
	}
	if err := cap.ImportSSTables(baseDir); err != nil {
		return fmt.Errorf("restore pebble snapshot: %w", err)
	}
	// We don't have precise key stats without scanning; record minimal stats.
	r.setSnapshotStats(SnapshotStats{
		Shards:     1,
		Keys:       0,
		ReadNs:     0,
		DecodeNs:   0,
		LoadNs:     0,
		Format:     snapshot.FormatPebble,
		SnapshotID: m.SnapshotID,
	})
	log.Printf("restore: restored Pebble snapshot %s from %s (files=%d)", m.SnapshotID, baseDir, len(m.PebbleSSTFiles))
	return nil
}

func (r *Restorer) RestoreAndReplay() (RestoreResult, error) {
	// Read latest manifest
	m, err := r.manifestReader.ReadLatest()
	if err != nil {
		return RestoreResult{}, fmt.Errorf("read manifest: %w", err)
	}

	// Restore from snapshot
	format := r.defaultFormat
	if m.SnapshotFormat != "" {
		if parsed, perr := snapshot.ParseFormat(m.SnapshotFormat); perr == nil {
			format = parsed
		} else {
			log.Printf("restore: unknown snapshot format %s, defaulting to %s", m.SnapshotFormat, format)
		}
	}
	if format == snapshot.FormatPebble {
		if err := r.restorePebbleFromManifest(m); err != nil {
			return RestoreResult{}, err
		}
	} else {
		shards := m.SnapshotShards
		if shards == 0 {
			shards = r.defaultShards
		}
		if err := r.RestoreFromSnapshotWithFormat(m.SnapshotID, format, shards, m.SnapshotKeys); err != nil {
			return RestoreResult{}, fmt.Errorf("restore snapshot: %w", err)
		}
	}

	// By default use file-based replay (callers can invoke Kafka variant directly if needed)
	result := r.ReplayChangelog("./changelog/opb.jsonl", m.LastChangelogOffset)
	return result, result.Error
}

// parseAndApplyLine parses a JSON delta line and applies it to the store.
// Returns (applied, skipped, err).
func (r *Restorer) parseAndApplyLine(line []byte) (bool, bool, error) {
	var d changelog.Delta
	if err := json.Unmarshal(line, &d); err != nil {
		return false, false, fmt.Errorf("unmarshal delta: %w", err)
	}
	ok, _, err := r.stateStore.Apply(d.Key, d.Delta, d.DeltaQty, d.Seq, state.SourceUnspecified)
	if err != nil {
		return false, false, fmt.Errorf("apply: %w", err)
	}
	if ok {
		return true, false, nil
	}
	return false, true, nil
}

// replayLines replays deltas from an io.Reader with optional fromOffset (line index).
func (r *Restorer) replayLines(reader io.Reader, fromOffset int64) RestoreResult {
	scanner := bufio.NewScanner(reader)
	applied, skipped := 0, 0
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		if int64(lineNum) <= fromOffset {
			continue
		}
		appliedNow, skippedNow, err := r.parseAndApplyLine(scanner.Bytes())
		if err != nil {
			return RestoreResult{Error: fmt.Errorf("line %d: %w", lineNum, err)}
		}
		if appliedNow {
			applied++
		} else if skippedNow {
			skipped++
		}
	}
	if err := scanner.Err(); err != nil {
		return RestoreResult{Error: fmt.Errorf("scan changelog: %w", err)}
	}
	return RestoreResult{Applied: applied, Skipped: skipped}
}
