package restorefs

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
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

type ChainValidationError struct {
	SnapshotID string
	Reason     string
	Err        error
}

func (e *ChainValidationError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Err != nil {
		return fmt.Sprintf("chain validation failed at snapshot %s: %s: %v", e.SnapshotID, e.Reason, e.Err)
	}
	return fmt.Sprintf("chain validation failed at snapshot %s: %s", e.SnapshotID, e.Reason)
}

type Reader interface {
	ReadLatest() (manifest.Manifest, error)
}

type FilesystemReader struct{ baseDir string }

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
	Applied           int
	Skipped           int
	Bytes             int64
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
	return r.RestoreFromSnapshotWithFormatParallel(snapshotID, format, shards, keysHint, 0)
}

// RestoreFromSnapshotWithFormatParallel restores from a sharded snapshot using up to `parallelism` workers (0=auto).
func (r *Restorer) RestoreFromSnapshotWithFormatParallel(snapshotID string, format snapshot.Format, shards int, keysHint int, parallelism int) error {
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
	if shards <= 1 {
		path := filepath.Join(baseDir, format.FileName())
		readStart := time.Now()
	data, err := os.ReadFile(path)
		readDur := time.Since(readStart)
	if err != nil {
			if os.IsNotExist(err) && format == snapshot.FormatMsgpack {
				// Fallback to JSON for backward compatibility with old snapshots.
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
	// Determine parallelism
	if parallelism <= 0 {
		parallelism = shards
		if parallelism > 8 {
			parallelism = 8
		}
	}
	if parallelism < 1 {
		parallelism = 1
	}
	// Worker pool to read+decode shards concurrently
	type shardOut struct {
		idx    int
		data   map[string]state.RecordState
		read   int64
		decode int64
		err    error
	}
	jobs := make(chan int, shards)
	outs := make(chan shardOut, shards)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for i := range jobs {
			fp := filepath.Join(baseDir, format.FileNameForShard(i, shards))
			readStart := time.Now()
			data, err := os.ReadFile(fp)
			readDur := time.Since(readStart).Nanoseconds()
			if err != nil {
				outs <- shardOut{idx: i, err: fmt.Errorf("read shard %d: %w", i, err)}
				continue
			}
			decodeStart := time.Now()
			dump, derr := snapshot.DecodeSnapshot(data, format)
			decodeDur := time.Since(decodeStart).Nanoseconds()
			if derr != nil {
				outs <- shardOut{idx: i, err: fmt.Errorf("decode shard %d: %w", i, derr)}
				continue
			}
			outs <- shardOut{idx: i, data: dump, read: readDur, decode: decodeDur}
		}
	}
	for w := 0; w < parallelism; w++ {
		wg.Add(1)
		go worker()
	}
	for i := 0; i < shards; i++ {
		jobs <- i
	}
	close(jobs)
	go func() {
		wg.Wait()
		close(outs)
	}()
	var readNs, decodeNs int64
	for out := range outs {
		if out.err != nil {
			return out.err
		}
		readNs += out.read
		decodeNs += out.decode
		for k, v := range out.data {
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
	log.Printf("restore: loaded %d keys from snapshot %s (shards=%d, parallelism=%d)", len(merged), snapshotID, shards, parallelism)
	return nil
}

func (r *Restorer) ReplayChangelog(changelogPath string, fromOffset int64) RestoreResult {
	f, err := os.Open(changelogPath)
	if err != nil {
		return RestoreResult{Error: fmt.Errorf("open changelog: %w", err)}
	}
	defer f.Close()
	return r.replayLines(f, fromOffset)
}

func (r *Restorer) RestoreAndReplay() (RestoreResult, error) {
	m, err := r.manifestReader.ReadLatest()
	if err != nil {
		return RestoreResult{}, fmt.Errorf("read manifest: %w", err)
	}
	format := r.defaultFormat
	if m.SnapshotFormat != "" {
		if parsed, perr := snapshot.ParseFormat(m.SnapshotFormat); perr == nil {
			format = parsed
		} else {
			log.Printf("restore: unknown snapshot format %s, defaulting to %s", m.SnapshotFormat, format)
		}
	}
	shards := m.SnapshotShards
	if shards == 0 {
		shards = r.defaultShards
	}
	if err := r.RestoreFromSnapshotWithFormat(m.SnapshotID, format, shards, m.SnapshotKeys); err != nil {
		return RestoreResult{}, fmt.Errorf("restore snapshot: %w", err)
	}
	res := r.ReplayChangelog("./changelog/opb.jsonl", m.LastChangelogOffset)
	return res, res.Error
}

func (r *Restorer) parseAndApplyLine(line []byte) (bool, bool, error) {
	var d changelog.Delta
	if err := json.Unmarshal(line, &d); err != nil {
		return false, false, fmt.Errorf("unmarshal delta: %w", err)
	}
	ok, _, err := r.stateStore.Apply(d.Key, d.Delta, d.DeltaQty, d.Seq)
	if err != nil {
		return false, false, fmt.Errorf("apply: %w", err)
	}
	if ok {
		return true, false, nil
	}
	return false, true, nil
}

func (r *Restorer) replayLines(reader io.Reader, fromOffset int64) RestoreResult {
	sc := bufio.NewScanner(reader)
	applied, skipped := 0, 0
	line := 0
	var bytes int64
	var last int64 = -1
	for sc.Scan() {
		line++
		if int64(line) <= fromOffset {
			continue
		}
		b := sc.Bytes()
		a, s, err := r.parseAndApplyLine(b)
		if err != nil {
			return RestoreResult{Error: fmt.Errorf("line %d: %w", line, err)}
		}
		if a {
			applied++
		} else if s {
			skipped++
		}
		bytes += int64(len(b))
		last = int64(line)
	}
	if err := sc.Err(); err != nil {
		return RestoreResult{Error: fmt.Errorf("scan changelog: %w", err)}
	}
	return RestoreResult{Applied: applied, Skipped: skipped, Bytes: bytes, LastAppliedOffset: last}
}

// ---- Restore Chain (full + sequential deltas) ----

// readSnapshotManifest reads snapshots/<id>/manifest.json
func (r *Restorer) readSnapshotManifest(id string) (manifest.Manifest, error) {
	if id == "" {
		return manifest.Manifest{}, fmt.Errorf("empty snapshot id")
	}
	path := filepath.Join(r.snapshotBaseDir, id, "manifest.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return manifest.Manifest{}, fmt.Errorf("read snapshot manifest %s: %w", id, err)
	}
	var m manifest.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return manifest.Manifest{}, fmt.Errorf("unmarshal snapshot manifest %s: %w", id, err)
	}
	return m, nil
}

// validateChainIntegrity checks the parent links and structure of the snapshot chain.
// Returns ordered chain [base, delta1, ..., latest] or ChainValidationError.
func (r *Restorer) validateChainIntegrity(latest manifest.Manifest) ([]manifest.Manifest, error) {
	if latest.SnapshotID == "" {
		return nil, &ChainValidationError{SnapshotID: "", Reason: "empty latest snapshot id"}
	}
	visited := make(map[string]bool)
	chain := make([]manifest.Manifest, 0, 8)
	cur := latest
	for {
		if cur.SnapshotID == "" {
			return nil, &ChainValidationError{SnapshotID: cur.SnapshotID, Reason: "empty snapshot id in chain"}
		}
		if visited[cur.SnapshotID] {
			return nil, &ChainValidationError{SnapshotID: cur.SnapshotID, Reason: "cycle detected"}
		}
		visited[cur.SnapshotID] = true
		// Always re-read the on-disk manifest for this snapshot id to avoid trusting a possibly stale latest pointer
		diskMan, err := r.readSnapshotManifest(cur.SnapshotID)
		if err != nil {
			return nil, &ChainValidationError{SnapshotID: cur.SnapshotID, Reason: "read snapshot manifest", Err: err}
		}
		// Use the on-disk manifest for all further checks
		cur = diskMan
		chain = append(chain, cur)
		if strings.ToLower(cur.SnapshotType) != manifest.SnapshotTypeDelta {
			break
		}
		if cur.ParentSnapshotID == "" {
			return nil, &ChainValidationError{SnapshotID: cur.SnapshotID, Reason: "delta missing parent"}
		}
		pm, err := r.readSnapshotManifest(cur.ParentSnapshotID)
		if err != nil {
			return nil, &ChainValidationError{SnapshotID: cur.SnapshotID, Reason: "read parent manifest", Err: err}
		}
		cur = pm
	}
	// reverse into forward order [base, ..., latest]
	ordered := make([]manifest.Manifest, len(chain))
	for i := range chain {
		ordered[i] = chain[len(chain)-1-i]
	}
	if len(ordered) == 0 {
		return nil, &ChainValidationError{SnapshotID: latest.SnapshotID, Reason: "empty chain"}
	}
	// base must be full
	if strings.ToLower(ordered[0].SnapshotType) == manifest.SnapshotTypeDelta {
		return nil, &ChainValidationError{SnapshotID: ordered[0].SnapshotID, Reason: "base snapshot not full"}
	}
	// validate forward links and base consistency
	baseID := ordered[0].SnapshotID
	for i := 1; i < len(ordered); i++ {
		if ordered[i].ParentSnapshotID != ordered[i-1].SnapshotID {
			return nil, &ChainValidationError{SnapshotID: ordered[i].SnapshotID, Reason: fmt.Sprintf("broken parent link: parent=%s expected=%s", ordered[i].ParentSnapshotID, ordered[i-1].SnapshotID)}
		}
		if ordered[i].BaseSnapshotID != "" && ordered[i].BaseSnapshotID != baseID {
			return nil, &ChainValidationError{SnapshotID: ordered[i].SnapshotID, Reason: fmt.Sprintf("inconsistent base id: got %s expected %s", ordered[i].BaseSnapshotID, baseID)}
		}
		// delta sequence should be consecutive starting at 1 when provided
		if ordered[i].DeltaSequence > 0 && ordered[i].DeltaSequence != i {
			return nil, &ChainValidationError{SnapshotID: ordered[i].SnapshotID, Reason: fmt.Sprintf("invalid delta sequence: got %d expected %d", ordered[i].DeltaSequence, i)}
		}
	}
	return ordered, nil
}

// validateSnapshotFiles checks that the declared snapshot files exist (with basic format fallback)
func (r *Restorer) validateSnapshotFiles(m manifest.Manifest) error {
	if m.SnapshotID == "" {
		return &ChainValidationError{SnapshotID: "", Reason: "empty snapshot id"}
	}
	format := r.defaultFormat
	if m.SnapshotFormat != "" {
		if pf, err := snapshot.ParseFormat(m.SnapshotFormat); err == nil {
			format = pf
		}
	}
	shards := m.SnapshotShards
	if shards <= 0 {
		shards = r.defaultShards
		if shards <= 0 {
			shards = 1
		}
	}
	baseDir := filepath.Join(r.snapshotBaseDir, m.SnapshotID)
	// manifest.json must exist
	if _, err := os.Stat(filepath.Join(baseDir, "manifest.json")); err != nil {
		return &ChainValidationError{SnapshotID: m.SnapshotID, Reason: "manifest.json missing", Err: err}
	}
	isDelta := strings.ToLower(m.SnapshotType) == manifest.SnapshotTypeDelta
	if !isDelta {
		// full snapshot files
		if shards <= 1 {
			fp := filepath.Join(baseDir, format.FileName())
			if _, err := os.Stat(fp); err != nil {
				if format == snapshot.FormatMsgpack {
					alt := filepath.Join(baseDir, snapshot.FormatJSON.FileName())
					if _, err2 := os.Stat(alt); err2 == nil {
						return nil
					}
				}
				return &ChainValidationError{SnapshotID: m.SnapshotID, Reason: "full snapshot file missing", Err: err}
			}
			return nil
		}
		missing := []string{}
		for i := 0; i < shards; i++ {
			fp := filepath.Join(baseDir, format.FileNameForShard(i, shards))
			if _, err := os.Stat(fp); err != nil {
				if format == snapshot.FormatMsgpack {
					alt := filepath.Join(baseDir, snapshot.FormatJSON.FileNameForShard(i, shards))
					if _, err2 := os.Stat(alt); err2 == nil {
						continue
					}
				}
				missing = append(missing, fp)
			}
		}
		if len(missing) > 0 {
			return &ChainValidationError{SnapshotID: m.SnapshotID, Reason: "full shard files missing: " + strings.Join(missing, ", ")}
		}
		return nil
	}
	// delta snapshot files
	if shards <= 1 {
		fp := filepath.Join(baseDir, format.FileNameDelta())
		if _, err := os.Stat(fp); err != nil {
			if format == snapshot.FormatMsgpack {
				alt := filepath.Join(baseDir, snapshot.FormatJSON.FileNameDelta())
				if _, err2 := os.Stat(alt); err2 == nil {
					return nil
				}
			}
			return &ChainValidationError{SnapshotID: m.SnapshotID, Reason: "delta file missing", Err: err}
		}
		return nil
	}
	missing := []string{}
	for i := 0; i < shards; i++ {
		fp := filepath.Join(baseDir, format.FileNameDeltaForShard(i, shards))
		if _, err := os.Stat(fp); err != nil {
			if format == snapshot.FormatMsgpack {
				alt := filepath.Join(baseDir, snapshot.FormatJSON.FileNameDeltaForShard(i, shards))
				if _, err2 := os.Stat(alt); err2 == nil {
					continue
				}
			}
			missing = append(missing, fp)
		}
	}
	if len(missing) > 0 {
		return &ChainValidationError{SnapshotID: m.SnapshotID, Reason: "delta shard files missing: " + strings.Join(missing, ", ")}
	}
	return nil
}

// readSnapshotToMap reads full snapshot into map
func (r *Restorer) readSnapshotToMap(id string, format snapshot.Format, shards int, parallelism int) (map[string]state.RecordState, SnapshotStats, error) {
	if id == "" {
		return map[string]state.RecordState{}, SnapshotStats{}, nil
	}
	baseDir := filepath.Join(r.snapshotBaseDir, id)
	// Determine if sharded exists
	if shards <= 0 {
		shards = r.defaultShards
	}
	readStart := time.Now()
	// If shards>1 but first shard missing, try single file
	if shards <= 1 {
		fp := filepath.Join(baseDir, format.FileName())
		d, err := os.ReadFile(fp)
		readDur := time.Since(readStart)
		if err != nil {
			return nil, SnapshotStats{}, err
		}
		decStart := time.Now()
		mm, err := snapshot.DecodeSnapshot(d, format)
		decDur := time.Since(decStart)
		return mm, SnapshotStats{Shards: 1, Keys: len(mm), ReadNs: readDur.Nanoseconds(), DecodeNs: decDur.Nanoseconds(), Format: format, SnapshotID: id}, nil
	}
	firstShard := filepath.Join(baseDir, format.FileNameForShard(0, shards))
	if _, err := os.Stat(firstShard); os.IsNotExist(err) {
		// fall back to single file
		return r.readSnapshotToMap(id, format, 1, 0)
	}
	if parallelism <= 0 { parallelism = shards; if parallelism > 8 { parallelism = 8 } }
	jobs := make(chan int, shards)
	outs := make(chan struct{ idx int; data map[string]state.RecordState; readNs, decNs int64; err error }, shards)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for i := range jobs {
			fp := filepath.Join(baseDir, format.FileNameForShard(i, shards))
			st := time.Now(); by, err := os.ReadFile(fp); rns := time.Since(st).Nanoseconds()
			if err != nil { outs <- struct{ idx int; data map[string]state.RecordState; readNs, decNs int64; err error }{idx:i, err:fmt.Errorf("read shard %d: %w", i, err)}; continue }
			ds := time.Now(); mm, derr := snapshot.DecodeSnapshot(by, format); dns := time.Since(ds).Nanoseconds()
			if derr != nil { outs <- struct{ idx int; data map[string]state.RecordState; readNs, decNs int64; err error }{idx:i, err:fmt.Errorf("decode shard %d: %w", i, derr)}; continue }
			outs <- struct{ idx int; data map[string]state.RecordState; readNs, decNs int64; err error }{idx:i, data:mm, readNs:rns, decNs:dns}
		}
	}
	for w:=0; w<parallelism; w++ { wg.Add(1); go worker() }
	for i:=0; i<shards; i++ { jobs <- i }
	close(jobs)
	go func(){ wg.Wait(); close(outs) }()
	merged := make(map[string]state.RecordState)
	var readNs, decNs int64
	for out := range outs {
		if out.err != nil { return nil, SnapshotStats{}, out.err }
		readNs += out.readNs; decNs += out.decNs
		for k,v := range out.data { merged[k] = v }
	}
	return merged, SnapshotStats{Shards: shards, Keys: len(merged), ReadNs: readNs, DecodeNs: decNs, Format: format, SnapshotID: id}, nil
}

// readDeltaToMap reads delta snapshot files into map
func (r *Restorer) readDeltaToMap(id string, format snapshot.Format, shards int, parallelism int) (map[string]state.RecordState, SnapshotStats, error) {
	baseDir := filepath.Join(r.snapshotBaseDir, id)
	if shards <= 1 {
		fp := filepath.Join(baseDir, format.FileNameDelta())
		st := time.Now(); by, err := os.ReadFile(fp); rns := time.Since(st)
		if err != nil { return nil, SnapshotStats{}, err }
		ds := time.Now(); mm, derr := snapshot.DecodeSnapshot(by, format); dns := time.Since(ds)
		if derr != nil { return nil, SnapshotStats{}, derr }
		return mm, SnapshotStats{Shards:1, Keys:len(mm), ReadNs:rns.Nanoseconds(), DecodeNs:dns.Nanoseconds(), Format:format, SnapshotID:id}, nil
	}
	firstShard := filepath.Join(baseDir, format.FileNameDeltaForShard(0, shards))
	if _, err := os.Stat(firstShard); os.IsNotExist(err) {
		// fallback: single delta file
		return r.readDeltaToMap(id, format, 1, 0)
	}
	if parallelism <= 0 { parallelism = shards; if parallelism > 8 { parallelism = 8 } }
	jobs := make(chan int, shards)
	outs := make(chan struct{ idx int; data map[string]state.RecordState; readNs, decNs int64; err error }, shards)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for i := range jobs {
			fp := filepath.Join(baseDir, format.FileNameDeltaForShard(i, shards))
			st := time.Now(); by, err := os.ReadFile(fp); rns := time.Since(st).Nanoseconds()
			if err != nil { outs <- struct{ idx int; data map[string]state.RecordState; readNs, decNs int64; err error }{idx:i, err:fmt.Errorf("read delta shard %d: %w", i, err)}; continue }
			ds := time.Now(); mm, derr := snapshot.DecodeSnapshot(by, format); dns := time.Since(ds).Nanoseconds()
			if derr != nil { outs <- struct{ idx int; data map[string]state.RecordState; readNs, decNs int64; err error }{idx:i, err:fmt.Errorf("decode delta shard %d: %w", i, derr)}; continue }
			outs <- struct{ idx int; data map[string]state.RecordState; readNs, decNs int64; err error }{idx:i, data:mm, readNs:rns, decNs:dns}
		}
	}
	for w:=0; w<parallelism; w++ { wg.Add(1); go worker() }
	for i:=0; i<shards; i++ { jobs <- i }
	close(jobs)
	go func(){ wg.Wait(); close(outs) }()
	merged := make(map[string]state.RecordState)
	var readNs, decNs int64
	for out := range outs {
		if out.err != nil { return nil, SnapshotStats{}, out.err }
		readNs += out.readNs; decNs += out.decNs
		for k,v := range out.data { merged[k] = v }
	}
	return merged, SnapshotStats{Shards: shards, Keys: len(merged), ReadNs: readNs, DecodeNs: decNs, Format: format, SnapshotID: id}, nil
}

type RestoreOptions struct {
	Parallelism     int
	SkipMissingDelta bool
	ValidateChain   bool // default true
}

// RestoreChainFromLatest loads base full and applies all deltas up to `latest` into memory, then LoadAll.
func (r *Restorer) RestoreChainFromLatest(latest manifest.Manifest, parallelism int) error {
	return r.RestoreChainFromLatestWithOptions(latest, RestoreOptions{Parallelism: parallelism, ValidateChain: true})
}

// RestoreChainFromLatestWithOptions is like RestoreChainFromLatest, with extra controls.
func (r *Restorer) RestoreChainFromLatestWithOptions(latest manifest.Manifest, opts RestoreOptions) error {
	if latest.SnapshotID == "" {
		return fmt.Errorf("empty latest snapshot id")
	}
	if !opts.ValidateChain {
		// Even when validation is disabled, still build the chain by following parents until base.
		// We will rely on file existence checks to catch basic issues.
	}
	// Validate chain and get ordered manifests [base, d1, ..., latest]
	ordered, err := r.validateChainIntegrity(latest)
	if err != nil {
		return err
	}
	if len(ordered) == 0 {
		return fmt.Errorf("validateChainIntegrity returned empty chain")
	}
	base := ordered[0]
	// Validate files exist; optionally allow skipping missing delta files
	filtered := make([]manifest.Manifest, 0, len(ordered))
	for i, m := range ordered {
		ferr := r.validateSnapshotFiles(m)
		if ferr == nil {
			filtered = append(filtered, m)
			continue
		}
		isDelta := strings.ToLower(m.SnapshotType) == manifest.SnapshotTypeDelta
		if i > 0 && isDelta && opts.SkipMissingDelta {
			// Allow skipping only when the error is about delta file(s) missing.
			if ce, ok := ferr.(*ChainValidationError); ok {
				reason := strings.ToLower(ce.Reason)
				if strings.Contains(reason, "delta file missing") || strings.Contains(reason, "delta shard files missing") {
					log.Printf("restore: warning: skipping missing delta %s (%s)", m.SnapshotID, ce.Reason)
					continue
				}
			}
		}
		return ferr
	}
	if len(filtered) == 0 {
		return fmt.Errorf("no snapshots to restore after filtering")
	}
	ordered = filtered
	// Resolve base format/shards (warn if others differ)
	fmtBase := r.defaultFormat
	if base.SnapshotFormat != "" {
		if pf, perr := snapshot.ParseFormat(base.SnapshotFormat); perr == nil {
			fmtBase = pf
		}
	}
	shardsBase := base.SnapshotShards
	if shardsBase == 0 {
		shardsBase = r.defaultShards
	}
	// Log chain summary
	var ids []string
	for _, m := range ordered { ids = append(ids, m.SnapshotID) }
	log.Printf("restore: chain length=%d base=%s deltas=%d ids=%s", len(ordered), base.SnapshotID, len(ordered)-1, strings.Join(ids, ","))
	// Read base
	parallelism := opts.Parallelism
	merged, _, err := r.readSnapshotToMap(base.SnapshotID, fmtBase, shardsBase, parallelism)
	if err != nil {
		return fmt.Errorf("read base snapshot %s: %w", base.SnapshotID, err)
	}
	// Apply deltas in order
	for i := 1; i < len(ordered); i++ {
		d := ordered[i]
		fmtD := fmtBase
		if d.SnapshotFormat != "" {
			if pf, e2 := snapshot.ParseFormat(d.SnapshotFormat); e2 == nil {
				if pf != fmtBase {
					log.Printf("restore: warning: delta %s format %s differs from base %s", d.SnapshotID, pf, fmtBase)
				}
				fmtD = pf
			}
		}
		shardsD := d.SnapshotShards
		if shardsD == 0 {
			shardsD = r.defaultShards
		}
		if shardsD != shardsBase {
			log.Printf("restore: warning: delta %s shards=%d differs from base shards=%d", d.SnapshotID, shardsD, shardsBase)
		}
		mm, _, err := r.readDeltaToMap(d.SnapshotID, fmtD, shardsD, parallelism)
		if err != nil {
			if opts.SkipMissingDelta {
				log.Printf("restore: warning: skipping delta %s due to read error: %v", d.SnapshotID, err)
				continue
			}
			return fmt.Errorf("read delta %s: %w", d.SnapshotID, err)
		}
		for k, v := range mm {
			merged[k] = v
		}
	}
	// Load merged into store
	start := time.Now()
	r.stateStore.LoadAll(merged)
	loadDur := time.Since(start)
	r.setSnapshotStats(SnapshotStats{Shards: 0, Keys: len(merged), ReadNs: 0, DecodeNs: 0, LoadNs: loadDur.Nanoseconds(), Format: fmtBase, SnapshotID: latest.SnapshotID})
	log.Printf("restore: chain applied base=%s deltas=%d keys=%d", base.SnapshotID, len(ordered)-1, len(merged))
	return nil
}
