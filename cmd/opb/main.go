package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"hpb/internal/changelog"
	"hpb/internal/kafkautil"
	"hpb/internal/manifest"
	"hpb/internal/metrics"
	"hpb/internal/opb"
	snapcut "hpb/internal/opb/snapcut"
	rf "hpb/internal/restorefs"
	rk "hpb/internal/restorekafka"
	"hpb/internal/snapshot"
	"hpb/internal/state"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Config holds CLI flags for OpB.
type Config struct {
	// Multi-input PoC
	MultiInputTopics string // comma-separated input topics for multi-input runtime (Phase 3.3)
	GroupID          string
	WindowSizeSec    int
	SnapshotInterval int
	SnapshotDir      string
	SnapshotShards   int
	StateDir         string
	StateBackend     string // memory|pebble
	InstanceID       string // for logging/visibility when running multiple replicas
	// Kafka sinks
	KafkaBootstrap  string
	ChangelogSink   string // file|kafka|both
	ManifestSink    string // file|kafka|both
	ChangelogSource string // file|kafka
	ChangelogDir    string // for file-based changelog
	TopicChangelog  string
	TopicSnapshots  string
	ManifestSource  string // file|kafka
	// Kafka input for orders.enriched
	InputSource   string // sample|kafka
	TopicEnriched string
	// Output EOS (orders.output)
	OutputTopic string
	// HTTP
	HTTPAddr string
	PromURL  string
	// EOS batching
	TxBatchSize      int
	TxLingerMs       int
	InjectorLingerMs int
	// Consumer group tuning
	SessionTimeoutMs    int
	HeartbeatIntervalMs int
	// Peers for cluster viz (comma-separated HTTP base URLs)
	PeersCSV string
	// Restore control
	RestoreOnStart          bool // perform restore at process start (use true on restart)
	RestoreOnly             bool // perform restore then exit (no consume); useful for staged restart
	SkipInflightReplay      bool // when true, skip replaying inflight snapshot (useful for stage-2 restart in demo)
	RestoreParallelism      int  // parallelism for snapshot restore (0=auto)
	RestoreValidateChain    bool // validate chain integrity before restore (default true)
	RestoreSkipMissingDelta bool // skip missing delta files instead of failing (default false)
	RestoreTrustManifest    bool // trust manifest freeze hints to skip changelog replay
	ReplayWorkers           int  // workers for Kafka changelog replay (0=auto)
	// Bundle 2 / EOS idempotent replay demo: extra replay passes on the same changelog.
	// When >1 and used together with --restore-on-start --restore-only, the restore
	// phase will:
	//   - run the normal snapshot+changelog replay once (pass=1),
	//   - then invoke the same replay function multiple additional times on the
	//     already-restored state store (pass=2..N), logging per-pass applied/skipped.
	// This is intentionally a *diagnostic/demo* feature: metrics/TTR are reported
	// for the first pass only; extra passes are visible via log lines
	//   "bundle2: replay pass=X applied=... skipped=...".
	ReplayExtraPasses    int
	RebalanceImportState bool // on partition assignment, attempt to import state from a peer (best-effort)
	// Snapshot compaction policy
	SnapMaxDeltas  int // after this many deltas -> cut full (<=0 disables delta)
	SnapMaxDeltaMB int // after delta chain bytes exceed this (MB) -> cut full (0=ignore)
	// Snapshot retention/GC
	SnapRetentionCount int  // keep last N snapshots (0=disable)
	SnapRetentionDays  int  // keep snapshots newer than N days (0=disable)
	SnapGCIntervalSec  int  // run GC every N seconds (0=disable, default 3600)
	EnablePebblePhase3 bool // gate incremental Pebble shipping (phase 3)
}

const (
	defaultVizPeerInterval = 500 * time.Millisecond
	defaultVizPeerTimeout  = 250 * time.Millisecond
	defaultVizPeerTTL      = 2 * time.Second
	defaultVizPeerBackoff  = 2 * time.Second
)

type restoreMetrics struct {
	SnapshotID          string              `json:"snapshotId"`
	LastChangelogOffset int64               `json:"lastChangelogOffset"`
	Applied             int64               `json:"applied"`
	Skipped             int64               `json:"skipped"`
	TTRMs               int64               `json:"ttrMs"`
	CausalReplayEvents  int64               `json:"causalReplayEvents,omitempty"`
	InflightEvents      int                 `json:"inflightEvents,omitempty"`
	InflightChannels    int                 `json:"inflightChannels,omitempty"`
	UpdatedAt           time.Time           `json:"updatedAt"`
	Phases              restorePhaseTimings `json:"phases,omitempty"`
}

type restorePhaseTimings struct {
	ManifestMs       int64 `json:"manifestMs,omitempty"`
	SnapshotTotalMs  int64 `json:"snapshotTotalMs,omitempty"`
	SnapshotReadMs   int64 `json:"snapshotReadMs,omitempty"`
	SnapshotDecodeMs int64 `json:"snapshotDecodeMs,omitempty"`
	SnapshotLoadMs   int64 `json:"snapshotLoadMs,omitempty"`
	ChangelogMs      int64 `json:"changelogMs,omitempty"`
	MetricsMs        int64 `json:"metricsMs,omitempty"`
	TotalMs          int64 `json:"totalMs,omitempty"`
}

func manifestAllowsReplaySkip(m manifest.Manifest) bool {
	return m.ReplayRequired != nil && !*m.ReplayRequired && m.InflightFile != ""
}

type ingestCommand struct {
	pause bool
	done  chan error
}

// sendIngestCommand sends an ingest command (pause/resume) and waits for acknowledgment.
func sendIngestCommand(ingestCtrl chan ingestCommand, pause bool) error {
	cmd := ingestCommand{pause: pause, done: make(chan error, 1)}
	select {
	case ingestCtrl <- cmd:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout enqueue ingest command")
	}
	select {
	case err := <-cmd.done:
		return err
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout waiting ingest ack")
	}
}

// makeSelfURL constructs the self URL from HTTP address configuration.
func makeSelfURL(httpAddr string) string {
	addr := strings.TrimSpace(httpAddr)
	if strings.HasPrefix(addr, ":") {
		return "http://127.0.0.1" + addr
	}
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr
	}
	return "http://" + addr
}

// metricsAdapter implements the opb.TxMetrics interface using a metrics.Registry.
type metricsAdapter struct{ *metrics.Registry }

func (a metricsAdapter) TxAborted()                { a.Registry.TxAborted.Inc() }
func (a metricsAdapter) TxProduced()               { a.Registry.TxProduced.Inc() }
func (a metricsAdapter) TxLatencySec(v float64)    { a.Registry.TxLatencySec.Observe(v) }
func (a metricsAdapter) OffsetsBoundLag(v float64) { a.Registry.OffsetsBoundLag.Set(v) }

// Debug logger controlled by OPB_DEBUG env ("1" or "true")
var opbDebug = func() bool { v := os.Getenv("OPB_DEBUG"); return v == "1" || strings.ToLower(v) == "true" }()

func dlogf(format string, args ...any) {
	if opbDebug {
		log.Printf(format, args...)
	}
}

func main() {
	cfg := readFlags()
	if err := run(cfg); err != nil {
		log.Fatalf("opb failed: %v", err)
	}
}

func readFlags() Config {
	var cfg Config
	flag.StringVar(&cfg.GroupID, "group-id", "opb", "consumer group id")
	flag.IntVar(&cfg.WindowSizeSec, "window-size", 300, "aggregation window seconds")
	flag.IntVar(&cfg.SnapshotInterval, "snapshot-interval", 60, "snapshot interval seconds")
	flag.StringVar(&cfg.SnapshotDir, "snapshot-dir", "./snapshots", "snapshot directory")
	flag.IntVar(&cfg.SnapshotShards, "snapshot-shards", 1, "snapshot shards per cut (>=1)")
	flag.StringVar(&cfg.StateDir, "state-dir", "./data/opb", "state data directory")
	flag.StringVar(&cfg.StateBackend, "state-backend", "pebble", "state backend: memory|pebble")
	flag.StringVar(&cfg.InstanceID, "instance-id", "", "instance id for logging (replicas)")
	flag.StringVar(&cfg.KafkaBootstrap, "kafka-bootstrap", "", "kafka bootstrap servers, e.g. localhost:9092")
	flag.StringVar(&cfg.ChangelogSink, "changelog-sink", "file", "changelog sink: file|kafka|both")
	flag.StringVar(&cfg.ManifestSink, "manifest-sink", "file", "manifest sink: file|kafka|both")
	flag.StringVar(&cfg.ChangelogSource, "changelog-source", "file", "changelog source for restore: file|kafka")
	flag.StringVar(&cfg.ChangelogDir, "changelog-dir", "./changelog", "directory for file-based changelog")
	flag.StringVar(&cfg.TopicChangelog, "topic-changelog", "p1.opb-changelog", "kafka topic for changelog (compacted)")
	flag.StringVar(&cfg.TopicSnapshots, "topic-snapshots", "p1.opb-snapshots", "kafka topic for manifest (compacted)")
	flag.StringVar(&cfg.ManifestSource, "manifest-source", "file", "manifest source for restore: file|kafka")
	flag.StringVar(&cfg.InputSource, "input-source", "sample", "orders.enriched source: sample|kafka")
	flag.StringVar(&cfg.TopicEnriched, "topic-enriched", "p1.orders.enriched", "kafka topic for orders.enriched input")
	flag.StringVar(&cfg.OutputTopic, "output-topic", "p1.orders.output", "kafka topic for orders.output")
	flag.StringVar(&cfg.HTTPAddr, "http", ":8080", "http listen address for metrics/health")
	flag.StringVar(&cfg.PromURL, "prom-url", os.Getenv("OPB_PROM_URL"), "Prometheus base URL for viz panels (e.g. http://127.0.0.1:9090)")
	flag.IntVar(&cfg.TxBatchSize, "tx-batch-size", 1000, "transactional batch size (messages per commit)")
	flag.IntVar(&cfg.TxLingerMs, "tx-linger-ms", 100, "transactional linger in ms before forcing a commit")
	flag.IntVar(&cfg.InjectorLingerMs, "injector-linger-ms", 5, "injector producer linger ms")
	// Peers for cluster viz from flag or env OPB_PEERS
	flag.StringVar(&cfg.PeersCSV, "peers", os.Getenv("OPB_PEERS"), "peer HTTP base URLs, comma-separated (e.g. http://127.0.0.1:8089,http://127.0.0.1:8090)")
	flag.IntVar(&cfg.SessionTimeoutMs, "session-timeout-ms", 10000, "consumer session timeout")
	flag.IntVar(&cfg.HeartbeatIntervalMs, "heartbeat-interval-ms", 3000, "consumer heartbeat interval")
	// Restore control: perform restore at process start only when explicitly enabled (use true on restart)
	flag.BoolVar(&cfg.RestoreOnStart, "restore-on-start", false, "perform restore at process start (use true on restart)")
	flag.BoolVar(&cfg.RestoreOnly, "restore-only", false, "perform restore then exit (no consume); useful for staged restart")
	flag.BoolVar(&cfg.SkipInflightReplay, "skip-inflight-replay", false, "skip inflight snapshot replay (useful for stage-2 restart in demo)")
	flag.IntVar(&cfg.RestoreParallelism, "restore-parallelism", 0, "parallelism for snapshot restore (0=auto)")
	flag.BoolVar(&cfg.RestoreValidateChain, "restore-validate-chain", true, "validate chain integrity before restore")
	flag.BoolVar(&cfg.RestoreSkipMissingDelta, "restore-skip-missing-delta", false, "skip missing delta files instead of failing")
	flag.BoolVar(&cfg.RestoreTrustManifest, "restore-trust-manifest", false, "trust manifest hint (replayRequired=false) to skip Kafka changelog replay")
	flag.IntVar(&cfg.ReplayWorkers, "replay-workers", 0, "workers for Kafka changelog replay (0=auto)")
	flag.IntVar(&cfg.ReplayExtraPasses, "replay-extra-passes", 1, "for EOS/idempotent replay demos: total replay passes to run during restore (>=1; 1=normal behaviour)")
	flag.BoolVar(&cfg.RebalanceImportState, "rebalance-import-state", false, "on partition assignment, attempt to import state from a peer (best-effort)")
	flag.IntVar(&cfg.SnapMaxDeltas, "snap-max-deltas", 3, "after this many deltas, force full (<=0 disables)")
	flag.IntVar(&cfg.SnapMaxDeltaMB, "snap-max-delta-mb", 128, "after delta chain bytes exceed this (MB), force full (0=ignore)")
	flag.IntVar(&cfg.SnapRetentionCount, "snap-retention-count", 0, "keep last N snapshots (0=disable GC)")
	flag.IntVar(&cfg.SnapRetentionDays, "snap-retention-days", 0, "keep snapshots newer than N days (0=disable)")
	flag.IntVar(&cfg.SnapGCIntervalSec, "snap-gc-interval-sec", 3600, "run GC every N seconds (0=disable)")
	flag.StringVar(&cfg.MultiInputTopics, "multi-input-topics", "", "comma-separated input topics for multi-input runtime (Phase 3.3)")
	flag.BoolVar(&cfg.EnablePebblePhase3, "enable-pebble-phase3", false, "enable incremental Pebble snapshot shipping (Phase 3)")
	flag.Parse()
	return cfg
}

func run(cfg Config) error {
	log.Printf("starting OpB with window=%ds snapshot-interval=%ds", cfg.WindowSizeSec, cfg.SnapshotInterval)

	// Phase 3.3: multi-input runtime (partition-level channels)
	if cfg.MultiInputTopics != "" && cfg.KafkaBootstrap != "" {
		log.Printf("phase 3.3: running multi-input runtime (topics=%s)", cfg.MultiInputTopics)
		return runMultiInputRuntime(cfg)
	}

	if cfg.StateBackend != "pebble" {
		return fmt.Errorf("state-backend must be 'pebble' (pebble-only mode)")
	}
	snapFormat := snapshot.FormatPebble
	if cfg.SnapshotShards < 1 {
		cfg.SnapshotShards = 1
	}
	// Helper functions extracted to restore_helpers.go
	// Create closures that capture cfg.SnapshotDir and snapFormat for convenience
	resolveSnapshotFormat := func(manifestFormat string) snapshot.Format {
		return resolveSnapshotFormat(manifestFormat, snapFormat)
	}
	resolveSnapshotShards := func(manifestShards int) int {
		return resolveSnapshotShards(manifestShards, cfg.SnapshotShards)
	}
	readSnapshotManifest := func(snapID string) (manifest.Manifest, error) {
		return readSnapshotManifest(cfg.SnapshotDir, snapID)
	}
	snapshotSizeBytes := func(snapshotID string, format snapshot.Format, shards int) float64 {
		return snapshotSizeBytes(cfg.SnapshotDir, snapshotID, format, shards)
	}
	deltaSnapshotSizeBytes := func(snapshotID string, format snapshot.Format, shards int) float64 {
		return deltaSnapshotSizeBytes(cfg.SnapshotDir, snapshotID, format, shards)
	}
	snapshotIncrementalBytes := func(snapshotID string, files []string) float64 {
		return snapshotIncrementalBytes(cfg.SnapshotDir, snapshotID, files)
	}

	// Init state store (extracted helper)
	st, cleanup, err := InitStateStore(cfg)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}

	// Init snapshotters (extracted helper)
	fullSnap, fullSnapView, deltaSnapView, deltaIncremental, err := InitSnapshotters(cfg, st)
	if err != nil {
		return err
	}

	maniFS := manifest.NewFilesystemManifest(cfg.SnapshotDir)
	var mani manifest.Publisher = maniFS
	var maniReader rf.Reader = rf.NewFilesystemReader(cfg.SnapshotDir)
	// Configure manifest publisher based on sink
	if (cfg.ManifestSink == "kafka" || cfg.ManifestSink == "both") && cfg.KafkaBootstrap != "" {
		maniK := manifest.NewKafkaManifest(cfg.KafkaBootstrap, cfg.TopicSnapshots, "opb-manifest-latest")
		if cfg.ManifestSink == "kafka" {
			mani = maniK
		} else {
			// both: publish to both Kafka and filesystem
			mani = manifest.MultiPublisher(maniFS, maniK)
		}
	}
	// Configure manifest reader based solely on source (independent of sink)
	if cfg.ManifestSource == "kafka" && cfg.KafkaBootstrap != "" {
		maniReader = rk.NewKafkaReader([]string{cfg.KafkaBootstrap}, cfg.TopicSnapshots, "opb-manifest-latest")
	}

	// Init changelog writer (helper)
	clog, changelogKafkaEnabled, err := InitChangelog(cfg)
	if err != nil {
		return err
	}
	// Track how many changelog records have been appended so far (for manifest offset)
	var changelogAppendedCount int64

	// Prometheus metrics registry
	mreg := metrics.NewRegistry()
	// Helper: seed per-store Prometheus gauges from current state (used after restore)
	seedStoreGauges := func() {
		if mreg.StoreSumQty == nil || mreg.StoreSumAmount == nil {
			return
		}
		totals := make(map[string]struct {
			sumA int64
			sumQ int64
		})
		_ = st.Range(func(key string, rs state.RecordState) error {
			parts := strings.Split(key, "#")
			if len(parts) != 3 {
				return nil
			}
			store := parts[0]
			agg := totals[store]
			agg.sumA += rs.SumAmount
			agg.sumQ += rs.SumQty
			totals[store] = agg
			return nil
		})
		for store, agg := range totals {
			mreg.StoreSumQty.WithLabelValues(store).Set(float64(agg.sumQ))
			mreg.StoreSumAmount.WithLabelValues(store).Set(float64(agg.sumA))
		}
	}
	// Seed once at startup in case state store already has data
	seedStoreGauges()
	// HTTP for health/metrics on dedicated mux to avoid handler conflicts
	appStatus := opb.NewStatusManager(cfg.InstanceID, cfg.GroupID, cfg.WindowSizeSec)
	metricsPath := filepath.Join(cfg.StateDir, "restore-metrics.json")
	// Admin channels
	snapshotCutReq := make(chan snapshotCutRequest, 4)
	ingestCtrl := make(chan ingestCommand)
	var ingestPaused atomic.Bool
	var importOnce sync.Once
	var stateImported atomic.Bool
	// Barrier cut tracking (non-blocking snapshot)
	type barrierCut struct {
		id          string
		expected    []int32
		seen        map[int32]bool
		started     bool // guard to avoid double-trigger
		cutType     string
		prev        *manifest.Manifest
		channels    []string
		inflight    map[string][]inflightRecord
		vectorClock opb.VectorClock
		preView     state.SnapshotView // snapshot view captured at cut-begin (pre-cut)
	}
	var (
		cutMu      sync.Mutex
		currentCut *barrierCut
	)
	ingestControlEnabled := cfg.InputSource == "kafka" && cfg.KafkaBootstrap != ""
	if rm, err := readRestoreMetrics(metricsPath); err == nil {
		appStatus.ApplyRestoreHistory(rm.TTRMs, rm.SnapshotID, rm.LastChangelogOffset, rm.Applied, rm.Skipped)
		appStatus.SetCausalReplay(rm.CausalReplayEvents)
		log.Printf("restore history: loaded snapshotId=%s applied=%d skipped=%d causalReplay=%d", rm.SnapshotID, rm.Applied, rm.Skipped, rm.CausalReplayEvents)
	} else if !errors.Is(err, os.ErrNotExist) {
		log.Printf("restore history: read error: %v", err)
	}
	zoneIdx := opb.NewZoneIndex()
	// Shared injection producer and simple rate limiter
	var injP *ck.Producer
	var injErr error
	// Admin snapshot-cut helpers
	var pauseMu sync.Mutex
	if cfg.KafkaBootstrap != "" {
		injP, injErr = ck.NewProducer(&ck.ConfigMap{
			"bootstrap.servers": cfg.KafkaBootstrap,
			"linger.ms":         cfg.InjectorLingerMs,
			"compression.type":  "lz4",
		})
		if injErr != nil {
			log.Printf("inject: producer init error: %v", injErr)
		}
	}
	injLast := make(map[string]time.Time)

	// Setup HTTP handlers (extracted helper)
	deps := httpHandlersDeps{
		cfg:                    cfg,
		st:                     st,
		appStatus:              appStatus,
		mreg:                   mreg,
		zoneIdx:                zoneIdx,
		maniReader:             maniReader,
		snapshotCutReq:         snapshotCutReq, // chan snapshotCutRequest
		ingestCtrl:             ingestCtrl,
		ingestPaused:           &ingestPaused,
		ingestControlEnabled:   ingestControlEnabled,
		injP:                   injP,
		injErr:                 injErr,
		injLast:                injLast,
		pauseMu:                &pauseMu,
		resolveSnapshotFormat:  resolveSnapshotFormat,
		resolveSnapshotShards:  resolveSnapshotShards,
		readSnapshotManifest:   readSnapshotManifest,
		snapshotSizeBytes:      snapshotSizeBytes,
		deltaSnapshotSizeBytes: deltaSnapshotSizeBytes,
	}
	go func(addr string) {
		setupHTTPHandlers(addr, deps)
	}(cfg.HTTPAddr)

	// Start GC worker if enabled
	if cfg.SnapGCIntervalSec > 0 && (cfg.SnapRetentionCount > 0 || cfg.SnapRetentionDays > 0) {
		gc := snapshot.NewGarbageCollector(cfg.SnapshotDir, cfg.SnapRetentionCount, cfg.SnapRetentionDays, maniReader)
		go func() {
			ticker := time.NewTicker(time.Duration(cfg.SnapGCIntervalSec) * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				deleted, err := gc.Collect()
				if err != nil {
					log.Printf("gc: error: %v", err)
				} else if len(deleted) > 0 {
					log.Printf("gc: deleted %d snapshots: %v", len(deleted), deleted)
				}
			}
		}()
	}

	// Perform recovery (restore snapshot + replay changelog) before starting Kafka consume loop
	// Only when explicitly enabled via --restore-on-start to avoid delaying first start
	if cfg.RestoreOnStart && !stateImported.Load() {
		deps := restoreDependencies{
			cfg:                      cfg,
			st:                       st,
			fullSnap:                 fullSnap,
			maniReader:               maniReader,
			snapFormat:               snapFormat,
			snapshotShards:           cfg.SnapshotShards,
			appStatus:                appStatus,
			mreg:                     mreg,
			seedStoreGauges:          seedStoreGauges,
			resolveSnapshotFormat:    resolveSnapshotFormat,
			resolveSnapshotShards:    resolveSnapshotShards,
			readSnapshotManifest:     readSnapshotManifest,
			snapshotSizeBytes:        snapshotSizeBytes,
			deltaSnapshotSizeBytes:   deltaSnapshotSizeBytes,
			snapshotIncrementalBytes: snapshotIncrementalBytes,
			metricsPath:              metricsPath,
			stateImported:            &stateImported,
		}
		if err := performRestore(deps); err != nil {
			return err
		}
	}

	if cfg.InputSource == "kafka" && cfg.KafkaBootstrap != "" {
		// Start periodic snapshot + manifest publisher in background (Kafka mode)
		if cfg.SnapshotInterval > 0 {
			go func() {
				ticker := time.NewTicker(time.Duration(cfg.SnapshotInterval) * time.Second)
				defer ticker.Stop()
				for range ticker.C {
					id := time.Now().UTC().Format(time.RFC3339)
					// Collect current changelog offsets first (needed for delta dirty-keys window)
					var offInfo *manifest.OffsetsInfo
					if cfg.KafkaBootstrap != "" && cfg.TopicChangelog != "" {
						if offs, parts, err := kafkautil.CollectChangelogOffsets(cfg.KafkaBootstrap, cfg.TopicChangelog); err == nil {
							offInfo = &manifest.OffsetsInfo{Topic: cfg.TopicChangelog, Partitions: parts, Offsets: offs}
						} else {
							log.Printf("manifest: collect offsets error: %v", err)
						}
					}
					// Decide full vs delta (auto policy using thresholds and manifest chain)
					cutType := manifest.SnapshotTypeFull
					var prev *manifest.Manifest
					if cfg.SnapMaxDeltas > 0 {
						if m, err := maniReader.ReadLatest(); err == nil && m.SnapshotID != "" {
							// compute delta chain length and bytes
							deltaCount := 0
							var deltaBytes float64
							if strings.ToLower(m.SnapshotType) == manifest.SnapshotTypeDelta {
								cur := m
								for {
									deltaCount++
									cm, e2 := readSnapshotManifest(cur.SnapshotID)
									if e2 == nil {
										format := resolveSnapshotFormat(cm.SnapshotFormat)
										shards := resolveSnapshotShards(cm.SnapshotShards)
										deltaBytes += deltaSnapshotSizeBytes(cur.SnapshotID, format, shards)
									}
									if cur.ParentSnapshotID == "" || strings.ToLower(cur.SnapshotType) != manifest.SnapshotTypeDelta {
										break
									}
									pm, e3 := readSnapshotManifest(cur.ParentSnapshotID)
									if e3 != nil {
										break
									}
									cur = pm
								}
							}
							if deltaCount < cfg.SnapMaxDeltas && (cfg.SnapMaxDeltaMB <= 0 || (deltaBytes/1024.0/1024.0) < float64(cfg.SnapMaxDeltaMB)) && offInfo != nil && m.Changelog != nil && len(m.Changelog.Offsets) > 0 && m.Changelog.Topic != "" {
								cutType = manifest.SnapshotTypeDelta
								prev = &m
							}
						}
					}
					t0 := time.Now()
					var meta snapshot.Result
					var serr error
					mtype := manifest.SnapshotTypeFull
					var baseID, parentID string
					var dseq int
					var deltaKeys []string
					deltaClearedAll := false
					doDelta := cutType == manifest.SnapshotTypeDelta && prev != nil && offInfo != nil
					if doDelta && deltaSnapView == nil && deltaIncremental == nil {
						log.Printf("periodic-cut: delta format not supported, falling back to full snapshot")
						doDelta = false
					}
					if doDelta {
						// Determine base/parent and delta sequence
						if strings.ToLower(prev.SnapshotType) == manifest.SnapshotTypeDelta && prev.BaseSnapshotID != "" {
							baseID = prev.BaseSnapshotID
							dseq = prev.DeltaSequence + 1
						} else {
							baseID = prev.SnapshotID
							dseq = 1
						}
						parentID = prev.SnapshotID
						if deltaIncremental != nil {
							meta, serr = deltaIncremental.WriteIncrementalSnapshot(id, st)
							if serr != nil {
								log.Printf("snapshot incremental error: %v", serr)
								doDelta = false
							} else {
								deltaClearedAll = true
							}
						} else {
							view, verr := st.NewSnapshotView()
							if verr != nil {
								log.Printf("snapshot view error: %v", verr)
								doDelta = false
							} else {
								keys, kerr := kafkautil.ScanDirtyKeysKafka([]string{cfg.KafkaBootstrap}, prev.Changelog.Topic, prev.Changelog.Offsets, offInfo.Offsets, 0, 1500*time.Millisecond)
								if kerr != nil {
									_ = view.Close()
									log.Printf("delta dirty-keys scan error: %v", kerr)
									doDelta = false
								} else {
									deltaKeys = keys
									meta, serr = deltaSnapView.WriteDeltaSnapshotFromView(id, view, keys)
									_ = view.Close()
									if serr != nil {
										log.Printf("snapshot error: %v", serr)
										doDelta = false
									}
								}
							}
						}
					}
					if doDelta {
						mtype = manifest.SnapshotTypeDelta
					} else {
						writeFull := func() (snapshot.Result, error) {
							if fullSnapView != nil {
								view, verr := st.NewSnapshotView()
								if verr != nil {
									return snapshot.Result{}, verr
								}
								defer view.Close()
								return fullSnapView.WriteSnapshotFromView(id, view)
							}
							if fullSnap != nil {
								return fullSnap.WriteSnapshot(id, st)
							}
							return snapshot.Result{}, fmt.Errorf("no full snapshotter configured")
						}
						meta, serr = writeFull()
						mtype = manifest.SnapshotTypeFull
						baseID = ""
						parentID = ""
						dseq = 0
					}
					if serr != nil {
						continue
					}
					durMs := float64(time.Since(t0).Milliseconds())
					mreg.SnapshotTimeMs.Observe(durMs)
					// Metrics: set SnapshotBytes based on type
					var bytes float64
					if mtype == manifest.SnapshotTypeDelta {
						bytes = deltaSnapshotSizeBytes(id, meta.Format, meta.Shards)
						log.Printf("periodic-cut: delta-metrics id=%s keys=%d bytes=%.0f durMs=%.0f", id, meta.Keys, bytes, durMs)
					} else {
						bytes = snapshotSizeBytes(id, meta.Format, meta.Shards)
					}
					mreg.SnapshotBytes.Set(bytes)
					// Build and publish manifest
					m := manifest.Manifest{
						SnapshotID:           id,
						SnapshotFormat:       meta.Format.String(),
						SnapshotShards:       meta.Shards,
						SnapshotKeys:         meta.Keys,
						SnapshotType:         mtype,
						BaseSnapshotID:       baseID,
						ParentSnapshotID:     parentID,
						DeltaSequence:        dseq,
						LastChangelogOffset:  changelogAppendedCount,
						CreatedAtEpochSecond: time.Now().UTC().Unix(),
						Changelog:            offInfo,
					}
					// Set Pebble-specific fields if format is pebble
					if meta.Format == snapshot.FormatPebble {
						m.PebbleSSTFiles = meta.PebbleSSTFiles
						m.PebbleFormatVersion = meta.PebbleFormatVersion
						m.PebbleSSTChecksums = meta.PebbleSSTChecksums
						m.PebbleIncrementalFiles = meta.PebbleIncrementalFiles
						m.PebbleAllFiles = meta.PebbleSSTFiles
					}
					if fp, ok := mani.(manifest.FullPublisher); ok {
						if err := fp.Publish(m); err != nil {
							log.Printf("manifest publish error: %v", err)
							continue
						}
					} else {
						if err := mani.PublishLatest(id, changelogAppendedCount); err != nil {
							log.Printf("manifest publish error: %v", err)
							continue
						}
					}
					// Reset dirty keys after successful snapshot publish
					if mtype == manifest.SnapshotTypeDelta {
						if deltaClearedAll {
							st.MarkSnapshotDone()
						} else if len(deltaKeys) > 0 {
							st.MarkSnapshotDone(deltaKeys...)
						}
					}
					log.Printf("snapshot and manifest published: %s type=%s (offset=%d)", id, mtype, changelogAppendedCount)
				}
			}()
		}
		// Consume orders.enriched from Kafka
		cfgMap := &ck.ConfigMap{
			"bootstrap.servers":             cfg.KafkaBootstrap,
			"group.id":                      cfg.GroupID,
			"enable.auto.commit":            false,
			"isolation.level":               "read_committed",
			"auto.offset.reset":             "earliest",
			"partition.assignment.strategy": "cooperative-sticky",
			"client.id":                     cfg.InstanceID,
			// NOTE: disable static membership to avoid assigned-0 stalls on quick restarts
			// "group.instance.id":             cfg.InstanceID,
			"session.timeout.ms":    cfg.SessionTimeoutMs,
			"heartbeat.interval.ms": cfg.HeartbeatIntervalMs,
			"max.poll.interval.ms":  300000,
			// High throughput tuning
			"fetch.min.bytes":           1,
			"fetch.wait.max.ms":         10,
			"max.partition.fetch.bytes": 8388608,
			"queued.min.messages":       500000,
			"fetch.max.bytes":           52428800,
		}
		if opbDebug {
			_ = cfgMap.SetKey("debug", "cgrp,consumer,protocol")
		}
		c, err := ck.NewConsumer(cfgMap)
		// Admin snapshot-cut worker using barrier injection (non-blocking)
		go func() {
			for req := range snapshotCutReq {
				if injP == nil {
					log.Printf("snapshot-cut: injector unavailable; cannot inject barrier")
					continue
				}
				ass, _ := c.Assignment()
				if len(ass) == 0 {
					log.Printf("snapshot-cut: no assignment; cannot inject barrier")
					continue
				}
				parts := make([]int32, 0, len(ass))
				for _, tp := range ass {
					parts = append(parts, tp.Partition)
				}
				id := fmt.Sprintf("cut-%d", time.Now().UnixNano())
				channels := make([]string, 0, len(parts))
				for _, pnum := range parts {
					channels = append(channels, fmt.Sprintf("%s#%d", cfg.TopicEnriched, pnum))
				}
				// Capture pre-cut snapshot view before enabling inflight recording
				view, vErr := st.NewSnapshotView()
				if vErr != nil {
					log.Printf("snapshot-cut: snapshot view error: %v", vErr)
				}
				cutMu.Lock()
				currentCut = &barrierCut{
					id:       id,
					expected: parts,
					seen:     map[int32]bool{},
					cutType:  req.cutType,
					prev:     req.prev,
					channels: channels,
				}
				cutMu.Unlock()
				appStatus.BeginCausalCut(id, len(parts))
				// attach pre-cut snapshot view
				cutMu.Lock()
				if currentCut != nil {
					currentCut.preView = view
				}
				cutMu.Unlock()
				for _, pnum := range parts {
					hdrs := opb.BarrierHeaders(id)
					tp := ck.TopicPartition{Topic: &cfg.TopicEnriched, Partition: pnum}
					_ = injP.Produce(&ck.Message{TopicPartition: tp, Key: []byte("barrier"), Value: nil, Headers: hdrs}, nil)
				}
				injP.Flush(5000)
				// Phase will be set to "marker-propagation" when first barrier is received
				log.Printf("snapshot-cut: barrier injected id=%s parts=%v type=%s", id, parts, req.cutType)
			}
		}()
		if err != nil {
			return fmt.Errorf("consumer: %w", err)
		}
		defer c.Close()
		refreshAssignment := func() {
			ass, err := c.Assignment()
			if err != nil {
				return
			}
			if len(ass) == 0 {
				appStatus.SetAssignment(cfg.TopicEnriched, nil)
				appStatus.SetLagTotal(0)
				return
			}
			parts := make([]int, 0, len(ass))
			for _, tp := range ass {
				parts = append(parts, int(tp.Partition))
			}
			appStatus.SetAssignment(cfg.TopicEnriched, parts)
			appStatus.SetLagTotal(0)
		}
		rebalanceCb := func(c *ck.Consumer, event ck.Event) error {
			switch ev := event.(type) {
			case ck.AssignedPartitions:
				log.Printf("%% Rebalance: %d partitions assigned", len(ev.Partitions))
				appStatus.SetRebalanceStatus(fmt.Sprintf("assigned %d", len(ev.Partitions)))
				if err := c.IncrementalAssign(ev.Partitions); err != nil {
					log.Printf("rebalance: incremental assign error: %v", err)
				}
				refreshAssignment()
				// Best-effort state import from a peer when enabled
				if cfg.RebalanceImportState && cfg.PeersCSV != "" {
					importOnce.Do(func() {
						go func() {
							// helper to send ingest command and wait ack
							sendIngestCmd := func(pause bool) error {
								return sendIngestCommand(ingestCtrl, pause)
							}
							// derive self url
							mkSelf := func() string {
								return makeSelfURL(cfg.HTTPAddr)
							}
							self := mkSelf()
							var peer string
							for _, p := range strings.Split(cfg.PeersCSV, ",") {
								p = strings.TrimSpace(p)
								if p == "" || p == self {
									continue
								}
								peer = p
								break
							}
							if peer == "" {
								return
							}
							// pause ingestion
							if err := sendIngestCmd(true); err != nil {
								log.Printf("import: pause error: %v", err)
								return
							}
							defer func() {
								if err := sendIngestCmd(false); err != nil {
									log.Printf("import: resume error: %v", err)
								}
							}()
							// fetch NDJSON
							cli := &http.Client{Timeout: 15 * time.Second}
							resp, err := cli.Get(strings.TrimRight(peer, "/") + "/admin/state/export")
							if err != nil {
								log.Printf("import: fetch from %s error: %v", peer, err)
								return
							}
							defer resp.Body.Close()
							scanner := bufio.NewScanner(resp.Body)
							buf := make(map[string]state.RecordState)
							for scanner.Scan() {
								line := scanner.Bytes()
								var row struct {
									Key   string            `json:"key"`
									State state.RecordState `json:"state"`
								}
								if err := json.Unmarshal(line, &row); err == nil && row.Key != "" {
									buf[row.Key] = row.State
								}
							}
							if err := scanner.Err(); err != nil {
								log.Printf("import: scanner error: %v", err)
								return
							}
							if len(buf) == 0 {
								log.Printf("import: no data from %s", peer)
								return
							}
							// load snapshot into store
							log.Printf("import: preparing to load %d keys into state store...", len(buf))
							st.LoadAll(buf)
							stateImported.Store(true)
							seedStoreGauges()
							log.Printf("import: finished loading %d keys from %s", len(buf), peer)
						}()
					})
				}
			case ck.RevokedPartitions:
				log.Printf("%% Rebalance: %d partitions revoked", len(ev.Partitions))
				appStatus.SetRebalanceStatus(fmt.Sprintf("revoked %d", len(ev.Partitions)))
				if _, err := c.Commit(); err != nil {
					log.Printf("rebalance: commit before revoke error: %v", err)
				}
				if err := c.IncrementalUnassign(ev.Partitions); err != nil {
					log.Printf("rebalance: incremental unassign error: %v", err)
				}
				refreshAssignment()
			}
			return nil
		}
		if err := c.SubscribeTopics([]string{cfg.TopicEnriched}, rebalanceCb); err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}

		// Periodically export per-partition lag metrics and snapshot assignment/lag into AppStatus
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				ass, err := c.Assignment()
				if err != nil || len(ass) == 0 {
					continue
				}
				pos, err := c.Position(ass)
				if err != nil {
					continue
				}
				var totalLag float64
				for _, tp := range pos {
					if tp.Topic == nil {
						continue
					}
					// Query high watermark (exclusive) for this partition
					low, high, err := c.QueryWatermarkOffsets(*tp.Topic, tp.Partition, int((2 * time.Second).Milliseconds()))
					_ = low
					if err != nil {
						continue
					}
					// Guard invalid/empty cases: if no data (high==0) or offset invalid (<0), treat lag as 0
					var lag int64
					if high == 0 || int64(tp.Offset) < 0 {
						lag = 0
					} else {
						lag = high - int64(tp.Offset)
						if lag < 0 {
							lag = 0
						}
					}
					lf := float64(lag)
					totalLag += lf
					mreg.PartitionLag.WithLabelValues(*tp.Topic, fmt.Sprintf("%d", tp.Partition), cfg.GroupID, cfg.InstanceID).Set(lf)
				}
				// Snapshot current assignment and total lag to /status
				parts := make([]int, 0, len(ass))
				for _, tp := range ass {
					parts = append(parts, int(tp.Partition))
				}
				appStatus.SetAssignment(cfg.TopicEnriched, parts)
				appStatus.SetLagTotal(totalLag)
			}
		}()
		// Always create transactional producer (EOS by default) when consuming from Kafka
		var p opb.TxProducer
		// fencing epoch token per-process
		epoch := []byte(fmt.Sprintf("%d", time.Now().UnixNano()))
		// vector clock operator id (use instance id when provided)
		operatorID := cfg.InstanceID
		if operatorID == "" {
			operatorID = "opb"
		}
		// derive transactional.id: stable across restarts if instance id is provided; else fallback to timestamp
		txID := cfg.InstanceID
		if txID == "" {
			txID = fmt.Sprintf("opb-%s-%d", cfg.GroupID, time.Now().UnixNano())
		} else {
			txID = fmt.Sprintf("opb-%s-%s", cfg.GroupID, cfg.InstanceID)
		}
		pCfg := &ck.ConfigMap{
			"bootstrap.servers":  cfg.KafkaBootstrap,
			"enable.idempotence": true,
			"acks":               "all",
			"transactional.id":   txID,
			// High throughput batching tuning
			"linger.ms":          cfg.TxLingerMs, // Use config value instead of hardcoded 5ms
			"batch.num.messages": 500000,         // Tăng từ 100K
			"batch.size":         16777216,       // 16 MB
			"compression.type":   "lz4",
			// Extra EOS safety (can raise to 2 if measured safe)
			"max.in.flight.requests.per.connection": 1,
			// Additional throughput tuning
			"delivery.timeout.ms":    300000,     // 5 min timeout
			"request.timeout.ms":     30000,      // 30s request timeout
			"message.timeout.ms":     300000,     // 5 min message timeout
			"transaction.timeout.ms": 600000,     // 10 min transaction timeout
			"retries":                2147483647, // Max retries
			"retry.backoff.ms":       100,        // Fast retry
		}
		if opbDebug {
			_ = pCfg.SetKey("debug", "eos,broker,protocol")
		}
		prod, err := ck.NewProducer(pCfg)
		if err != nil {
			return fmt.Errorf("producer: %w", err)
		}
		if err := prod.InitTransactions(context.TODO()); err != nil {
			return fmt.Errorf("init tx: %w", err)
		}
		p = prod // as interface
		defer p.Close()
		// Mark ready once consumer has partition assignment (and producer, if any, is initialized)
		go func() {
			for {
				ass, err := c.Assignment()
				if err == nil && len(ass) > 0 {
					// set assignment snapshot on first ready
					parts := make([]int, 0, len(ass))
					for _, tp := range ass {
						parts = append(parts, int(tp.Partition))
					}
					appStatus.SetAssignment(cfg.TopicEnriched, parts)
					appStatus.SetHealthy()
					log.Printf("opb: ready (assigned %d partitions)", len(ass))
					return
				}
				time.Sleep(200 * time.Millisecond)
			}
		}()
		// Continuous processing loop with transactional batching.
		var (
			batchStarted   bool
			batchStartTime time.Time
			batchCount     int
			batchOffsets   = make(map[int32]ck.TopicPartition) // partition -> highest offset+1
		)
		// Simple in-process idempotency for verify: eventId = orderId#windowStart
		dedupSeen := make(map[string]struct{})
		var latestEpochSeen int64
		handleIngestCommand := func(cmd ingestCommand) {
			var cmdErr error
			if cmd.pause {
				if ingestPaused.Load() {
					// already paused
				} else {
					if p != nil && batchStarted {
						if e := opb.CommitBatch(c, p, batchOffsets, metricsAdapter{mreg}); e != nil {
							cmdErr = fmt.Errorf("commit before pause: %w", e)
						}
						batchStarted = false
						batchCount = 0
						batchOffsets = make(map[int32]ck.TopicPartition)
					}
					pauseMu.Lock()
					ass, aerr := c.Assignment()
					if cmdErr == nil && aerr != nil {
						cmdErr = fmt.Errorf("assignment before pause: %w", aerr)
					} else if cmdErr == nil && len(ass) > 0 {
						if e := c.Pause(ass); e != nil {
							cmdErr = fmt.Errorf("pause assignment: %w", e)
						}
					}
					if cmdErr == nil {
						ingestPaused.Store(true)
					}
					pauseMu.Unlock()
				}
			} else {
				if !ingestPaused.Load() {
					// already running
				} else {
					pauseMu.Lock()
					ass, aerr := c.Assignment()
					if aerr != nil {
						cmdErr = fmt.Errorf("assignment before resume: %w", aerr)
					} else if len(ass) > 0 {
						if e := c.Resume(ass); e != nil {
							cmdErr = fmt.Errorf("resume assignment: %w", e)
						}
					}
					if cmdErr == nil {
						ingestPaused.Store(false)
					}
					pauseMu.Unlock()
				}
			}
			if cmd.done != nil {
				cmd.done <- cmdErr
			}
		}
		channelName := func(partition int32) string {
			return fmt.Sprintf("%s#%d", cfg.TopicEnriched, partition)
		}
		recordInflightEvent := func(partition int32, key string, payload []byte, vc opb.VectorClock) {
			cutMu.Lock()
			defer cutMu.Unlock()
			if currentCut == nil {
				return
			}
			if currentCut.seen[partition] {
				return // Barrier for this partition has been seen, stop recording.
			}
			ch := channelName(partition)
			if currentCut.inflight == nil {
				currentCut.inflight = make(map[string][]inflightRecord)
			}
			rec := inflightRecord{Key: key}
			if opbDebug {
				log.Printf("inflight capture: part=%d key=%s", partition, key)
			}
			if len(payload) > 0 {
				rec.Payload = append([]byte(nil), payload...)
			}
			if vc != nil {
				rec.VC = vc.Copy()
				// Gộp vector clock của từng event vào snapshot-level vector clock
				// để manifest.vectorClock phản ánh bound nhân quả của toàn cut.
				if currentCut.vectorClock == nil {
					currentCut.vectorClock = opb.NewVectorClock()
				}
				currentCut.vectorClock = currentCut.vectorClock.Merge(vc)
			}
			currentCut.inflight[ch] = append(currentCut.inflight[ch], rec)
		}
		for {
			if ingestControlEnabled {
				select {
				case cmd := <-ingestCtrl:
					handleIngestCommand(cmd)
					continue
				default:
				}
			}
			if ingestPaused.Load() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			// Read first to avoid spinning when no input
			msg, err := c.ReadMessage(200 * time.Millisecond)
			if err != nil {
				// On timeout/no message: if we have an open batch and linger expired, commit it
				if p != nil && batchStarted && time.Since(batchStartTime) >= time.Duration(cfg.TxLingerMs)*time.Millisecond {
					if err := opb.CommitBatch(c, p, batchOffsets, metricsAdapter{mreg}); err != nil {
						log.Printf("tx: batch commit error: %v", err)
					}
					batchStarted = false
					batchCount = 0
					batchOffsets = make(map[int32]ck.TopicPartition)
				}
				continue
			}
			// Track highest offset+1 per partition ASAP to avoid reprocessing on retries/rebalances
			tpTrack := ck.TopicPartition{Topic: msg.TopicPartition.Topic, Partition: msg.TopicPartition.Partition, Offset: msg.TopicPartition.Offset + 1}
			if existing, ok := batchOffsets[tpTrack.Partition]; !ok || tpTrack.Offset > existing.Offset {
				batchOffsets[tpTrack.Partition] = tpTrack
			}
			// Barrier detection before payload decode
			if ok, bid := opb.IsBarrier(msg.Headers); ok {
				cutMu.Lock()
				bc := currentCut
				if bc != nil && bc.id == bid {
					wasFirstBarrier := len(bc.seen) == 0
					bc.seen[msg.TopicPartition.Partition] = true
					seenCount := 0
					for _, ok := range bc.seen {
						if ok {
							seenCount++
						}
					}
					appStatus.SetCausalMarkers(seenCount)
					// Set phase to "marker-propagation" when first barrier is received
					if wasFirstBarrier {
						appStatus.SetCausalPhase("marker-propagation")
						if opbDebug {
							log.Printf("barrier-cut: first barrier received id=%s part=%d", bid, msg.TopicPartition.Partition)
						}
					}
					if opbDebug {
						log.Printf("barrier-cut: ack id=%s part=%d seen=%d/%d", bid, msg.TopicPartition.Partition, seenCount, len(bc.expected))
					}
					// check all seen
					all := true
					for _, p := range bc.expected {
						if !bc.seen[p] {
							all = false
							break
						}
					}
					if all && !bc.started {
						appStatus.SetCausalPhase("channel-state")
						if opbDebug {
							log.Printf("barrier-cut: all partitions ready id=%s type=%s", bid, bc.cutType)
						}
						bc.started = true
						cutMu.Unlock()
						pauseMu.Lock()
						func(bc *barrierCut) {
							defer pauseMu.Unlock()
							ass, _ := c.Assignment()
							if opbDebug {
								log.Printf("barrier-cut: pause request id=%s partitions=%v", bc.id, ass)
							}
							if len(ass) > 0 {
								_ = c.Pause(ass)
							}
							if p != nil && batchStarted {
								if opbDebug {
									log.Printf("barrier-cut: committing batch before snapshot id=%s count=%d", bc.id, batchCount)
								}
								if err := opb.CommitBatch(c, p, batchOffsets, metricsAdapter{mreg}); err != nil {
									log.Printf("barrier-cut: commit before snapshot error: %v", err)
								}
								// Force commit of consumer offsets to ensure no reprocessing after restart
								if _, err := c.Commit(); err != nil {
									log.Printf("barrier-cut: explicit consumer commit error: %v", err)
								}
								batchStarted = false
								batchCount = 0
								batchOffsets = make(map[int32]ck.TopicPartition)
							}
							if opbDebug {
								log.Printf("barrier-cut: commit sync complete id=%s", bc.id)
							}
							time.Sleep(150 * time.Millisecond)

							ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
							defer cancel()
							collector := kafkaOffsetsCollector{bootstrap: cfg.KafkaBootstrap, topic: cfg.TopicChangelog}
							scanner := kafkaDirtyScanner{bootstrap: cfg.KafkaBootstrap, timeout: 1500 * time.Millisecond}
							// Use pre-cut snapshot view when available to ensure snapshot is pre-cut
							fullWriter := fullSnapView
							deltaWriter := deltaSnapView
							if bc.preView != nil {
								if fullWriter != nil {
									fullWriter = fixedViewWriter{snap: fullWriter, view: bc.preView}
								}
								if deltaWriter != nil {
									deltaWriter = fixedViewWriter{snap: deltaWriter, view: bc.preView}
								}
							}
							writer := snapshotWriter{
								store:       st,
								full:        fullSnap,
								fullView:    fullWriter,
								delta:       deltaWriter,
								incremental: deltaIncremental,
							}
							if writer.full == nil && writer.fullView == nil {
								log.Printf("barrier-cut: no full snapshot writer configured; skipping cut id=%s type=%s", bc.id, bc.cutType)
								return
							}
							if bc.cutType == manifest.SnapshotTypeDelta && writer.delta == nil && writer.incremental == nil {
								log.Printf("barrier-cut: delta snapshot format not supported; skipping cut id=%s type=%s", bc.id, bc.cutType)
								return
							}
							t0 := time.Now()
							causalFn := func(snapID string) (*snapcut.CausalInfo, error) {
								cutMu.Lock()
								defer cutMu.Unlock()
								channels := append([]string(nil), bc.channels...)
								vc := bc.vectorClock
								inflight := bc.inflight
								relPath := ""
								total := 0
								var err error
								if len(inflight) > 0 {
									relPath, total, err = writeInflightSnapshot(cfg.SnapshotDir, snapID, channels, inflight)
									if err != nil {
										return nil, err
									}
									if opbDebug {
										log.Printf("barrier-cut: inflight recorded events=%d channels=%d", total, len(channels))
									}
									mreg.CausalInflight.Set(float64(total))
									appStatus.SetCausalInflight(total)
								} else {
									if opbDebug {
										log.Printf("barrier-cut: inflight empty")
									}
									mreg.CausalInflight.Set(0)
									appStatus.SetCausalInflight(0)
								}
								info := &snapcut.CausalInfo{
									Channels:       channels,
									InflightFile:   relPath,
									InflightEvents: total,
								}
								if vc != nil {
									info.VectorClock = vc.Copy()
								}
								return info, nil
							}
							if opbDebug {
								log.Printf("barrier-cut: invoking perform id=%s type=%s", bc.id, bc.cutType)
							}
							m, res, err := snapcut.PerformBarrierCut(ctx, bc.cutType, bc.prev, st, collector, scanner, writer, mani, changelogAppendedCount, causalFn, time.Now)
							if err != nil {
								log.Printf("barrier-cut: perform error: %v", err)
							} else {
								// Default: Kafka replay required unless another component explicitly flips this.
								if m.ReplayRequired == nil {
									replay := true
									m.ReplayRequired = &replay
								}
								durMs := float64(time.Since(t0).Milliseconds())
								mreg.SnapshotTimeMs.Observe(durMs)
								var bytes float64
								if strings.ToLower(m.SnapshotType) == manifest.SnapshotTypeDelta {
									bytes = deltaSnapshotSizeBytes(m.SnapshotID, res.Format, res.Shards)
									log.Printf("barrier-cut: delta-metrics id=%s keys=%d bytes=%.0f durMs=%.0f", m.SnapshotID, res.Keys, bytes, durMs)
								} else {
									bytes = snapshotSizeBytes(m.SnapshotID, res.Format, res.Shards)
								}
								mreg.SnapshotBytes.Set(bytes)
								incBytes := 0.0
								incFiles := 0
								if len(res.PebbleIncrementalFiles) > 0 {
									incBytes = snapshotIncrementalBytes(m.SnapshotID, res.PebbleIncrementalFiles)
									incFiles = len(res.PebbleIncrementalFiles)
								}
								mreg.SnapshotIncrementalBytes.Set(incBytes)
								mreg.SnapshotIncrementalFiles.Set(float64(incFiles))
								if res.Format == snapshot.FormatPebble {
									phase := "phase1"
									if len(res.PebbleIncrementalFiles) > 0 {
										phase = "phase3"
									} else if strings.ToLower(m.SnapshotType) == manifest.SnapshotTypeDelta {
										phase = "phase2"
									}
									log.Printf("snapshot: backend=pebble phase=%s type=%s id=%s files=%d newFiles=%d bytes=%.0f newBytes=%.0f", phase, m.SnapshotType, m.SnapshotID, len(res.PebbleSSTFiles), len(res.PebbleIncrementalFiles), bytes, incBytes)
								}
								log.Printf("barrier-cut: manifest published id=%s type=%s", m.SnapshotID, m.SnapshotType)
							}

							if len(ass) > 0 && !ingestPaused.Load() {
								if opbDebug {
									log.Printf("barrier-cut: resuming partitions id=%s", bc.id)
								}
								if err := c.Resume(ass); err != nil {
									log.Printf("barrier-cut: resume error: %v", err)
								}
							}
							cutMu.Lock()
							currentCut = nil
							cutMu.Unlock()
							// Clear causal cut status immediately after snapshot completes
							appStatus.ClearCausalCut()
						}(bc)
						continue
					}
				}
				cutMu.Unlock()
				continue // barrier message not processed as data
			}
			var ev opb.OrderEnriched
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				log.Printf("unmarshal error: %v", err)
				continue
			}
			// extract t0 header and apply epoch fencing
			var hdrT0 []byte
			if len(msg.Headers) > 0 {
				for _, h := range msg.Headers {
					if h.Key == "t0" {
						hdrT0 = h.Value
						break
					}
				}
			}
			if !opb.AcceptMessageByEpoch(&latestEpochSeen, msg.Headers) {
				continue
			}
			// Phase 3.1: extract vector clock from input, tick for this operator
			inVC := opb.ExtractVectorClock(msg.Headers)
			outVC := inVC.Copy().Tick(operatorID)
			// Pre-compute key for diagnostics
			ws := opb.WindowStart(ev.NormTS, cfg.WindowSizeSec)
			k := opb.OutputKey(ev.StoreID, ev.ProductID, ws)
			if strings.HasPrefix(ev.StoreID, "EOS-TEST-") {
				part := msg.TopicPartition.Partition
				off := msg.TopicPartition.Offset
				dlogf("diag: incoming store=%s product=%s qty=%d ws=%d key=%s part=%d off=%d", ev.StoreID, ev.ProductID, ev.Qty, ws, k, part, off)
			}
			// In-process idempotency by eventId (orderId#ws) to avoid double-apply in low-load tests
			eventID := fmt.Sprintf("%s#%d", ev.OrderID, ws)
			if _, ok := dedupSeen[eventID]; ok {
				dlogf("diag: dedup skip eventID=%s key=%s", eventID, k)
				mreg.EventsSkippedDedup.Inc()
				appStatus.IncEventsSkippedDedup(1)
				continue
			}
			dedupSeen[eventID] = struct{}{}
			recordInflightEvent(msg.TopicPartition.Partition, k, msg.Value, outVC)

			prevSt, _ := st.Get(k)
			applied, out, seq, err := opb.AggregateAndBuildOutput(st, cfg.WindowSizeSec, ev)
			if err != nil {
				return fmt.Errorf("aggregate: %w", err)
			}
			if applied {
				mreg.EventsApplied.Inc()
				appStatus.IncEventsApplied(1)
				// Update per-store aggregates index for zone details
				zoneIdx.OnApplied(ev.StoreID, ev.Price*ev.Qty, ev.Qty, cfg.InstanceID)
				// Update Prometheus per-store gauges so UI can query opb_store_sum_* by storeId
				if mreg.StoreSumQty != nil && mreg.StoreSumAmount != nil {
					sumA, sumQ, _ := zoneIdx.Snapshot(ev.StoreID)
					mreg.StoreSumQty.WithLabelValues(ev.StoreID).Set(float64(sumQ))
					mreg.StoreSumAmount.WithLabelValues(ev.StoreID).Set(float64(sumA))
				}
				dlogf("aggregate: applied key=%s seq=%d prevLast=%d", out.Key, seq, prevSt.LastSeq)
				b, _ := json.Marshal(out)
				if p != nil {
					if !batchStarted {
						dlogf("tx: begin transaction")
						if err := p.BeginTransaction(); err != nil {
							return fmt.Errorf("begin tx: %w", err)
						}
						batchStarted = true
						batchStartTime = time.Now()
					}
					// set t1 header, propagate t0 nếu có, kèm fencing epoch
					headers := opb.BuildHeadersWithEpochAndVC(opb.RealClock{}, hdrT0, epoch, outVC)
					if err := p.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &cfg.OutputTopic, Partition: ck.PartitionAny}, Key: []byte(out.Key), Value: b, Headers: headers}, nil); err != nil {
						_ = p.AbortTransaction(context.TODO())
						mreg.TxAborted.Inc()
						batchStarted = false
						batchCount = 0
						batchOffsets = make(map[int32]ck.TopicPartition)
						log.Printf("tx: produce error, aborted: %v", err)
						continue
					}
					batchCount++
					// Track highest offset+1 per partition
					tp := ck.TopicPartition{Topic: msg.TopicPartition.Topic, Partition: msg.TopicPartition.Partition, Offset: msg.TopicPartition.Offset + 1}
					if existing, ok := batchOffsets[tp.Partition]; !ok || tp.Offset > existing.Offset {
						batchOffsets[tp.Partition] = tp
					}
				}
				if changelogKafkaEnabled || clog != nil {
					d := changelog.Delta{Key: out.Key, Seq: seq, Delta: ev.Price * ev.Qty, DeltaQty: ev.Qty, TS: out.UpdatedAt}
					// If we have a transactional producer, write changelog to Kafka in the same transaction for immediate visibility
					if changelogKafkaEnabled && p != nil {
						bchg, _ := json.Marshal(d)
						headers := opb.BuildHeadersWithEpochAndVC(opb.RealClock{}, hdrT0, epoch, outVC)
						if err := p.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &cfg.TopicChangelog, Partition: ck.PartitionAny}, Key: []byte(d.Key), Value: bchg, Headers: headers}, nil); err != nil {
							_ = p.AbortTransaction(context.TODO())
							mreg.TxAborted.Inc()
							batchStarted = false
							batchCount = 0
							batchOffsets = make(map[int32]ck.TopicPartition)
							log.Printf("tx: produce changelog error, aborted: %v", err)
							return nil
						}
					} else if clog != nil {
						if err := clog.Append(d); err != nil {
							if p != nil && batchStarted {
								_ = p.AbortTransaction(context.TODO())
								mreg.TxAborted.Inc()
								batchStarted = false
								batchCount = 0
								batchOffsets = make(map[int32]ck.TopicPartition)
							}
							return fmt.Errorf("append changelog: %w", err)
						}
					}
					dlogf("changelog: appended key=%s seq=%d", out.Key, seq)
					mreg.ChangelogAppended.Inc()
					changelogAppendedCount++
				}
			} else {
				mreg.EventsSkippedSeq.Inc()
				appStatus.IncEventsSkippedSeq(1)
				log.Printf("aggregate: skipped key=%s prevLast=%d nextSeq=%d", k, prevSt.LastSeq, prevSt.LastSeq+1)
			}
			// Commit batch if thresholds met
			if p != nil && batchStarted && (batchCount >= cfg.TxBatchSize || time.Since(batchStartTime) >= time.Duration(cfg.TxLingerMs)*time.Millisecond) {
				if err := opb.CommitBatch(c, p, batchOffsets, metricsAdapter{mreg}); err != nil {
					log.Printf("tx: batch commit error: %v", err)
				}
				batchStarted = false
				batchCount = 0
				batchOffsets = make(map[int32]ck.TopicPartition)
			}
		}
	} else {
		// Phase 1: simulate processing some events
		sample := []opb.OrderEnriched{
			{OrderID: "o1", ProductID: "p1", Price: 10000, Qty: 1, StoreID: "A", TS: 1694500000, Validated: true, NormTS: 1694500000},
			{OrderID: "o2", ProductID: "p1", Price: 10000, Qty: 2, StoreID: "A", TS: 1694500010, Validated: true, NormTS: 1694500010},
			{OrderID: "o3", ProductID: "p2", Price: 5000, Qty: 3, StoreID: "A", TS: 1694500020, Validated: true, NormTS: 1694500020},
		}
		for _, ev := range sample {
			applied, out, seq, err := opb.AggregateAndBuildOutput(st, cfg.WindowSizeSec, ev)
			if err != nil {
				return fmt.Errorf("aggregate: %w", err)
			}
			if applied {
				b, _ := json.Marshal(out)
				log.Printf("orders.output key=%s seq=%d value=%s", out.Key, seq, string(b))
				if clog != nil {
					d := changelog.Delta{Key: out.Key, Seq: seq, Delta: ev.Price * ev.Qty, DeltaQty: ev.Qty, TS: out.UpdatedAt}
					if err := clog.Append(d); err != nil {
						return fmt.Errorf("append changelog: %w", err)
					}
				}
			}
		}
	}

	// Note: snapshot publishing in Kafka mode already runs in background; no blocking timers here.

	// Test restore and replay with status transitions only in non-Kafka input mode
	if cfg.InputSource != "kafka" {
		log.Printf("testing restore and replay...")
		restorer := rf.NewRestorerWithOptions(st, fullSnap, maniReader, cfg.SnapshotDir, snapFormat, cfg.SnapshotShards)
		// Read manifest first to expose details to status manager
		m, mErr := maniReader.ReadLatest()
		if mErr != nil {
			log.Printf("restore: read manifest failed: %v", mErr)
		} else {
			appStatus.SetRecovering(m.SnapshotID, m.LastChangelogOffset)
		}
		t0 := time.Now()
		var result rf.RestoreResult
		var err error
		restoreFmt := snapFormat
		restoreShards := cfg.SnapshotShards
		keysHint := 0
		if mErr == nil {
			restoreFmt = resolveSnapshotFormat(m.SnapshotFormat)
			restoreShards = resolveSnapshotShards(m.SnapshotShards)
			keysHint = m.SnapshotKeys
		}
		if cfg.ChangelogSource == "kafka" && cfg.KafkaBootstrap != "" {
			if mErr != nil {
				err = mErr
			} else {
				// Always restore snapshot before replaying changelog
				if e := restorer.RestoreFromSnapshotWithFormatParallel(m.SnapshotID, restoreFmt, restoreShards, keysHint, cfg.RestoreParallelism); e != nil {
					err = e
				} else {
					result = rk.ReplayChangelogKafkaParallel(st, []string{cfg.KafkaBootstrap}, cfg.TopicChangelog, nil, m.LastChangelogOffset, cfg.ReplayWorkers)
					if result.Error != nil {
						err = result.Error
					}
				}
			}
		} else {
			if mErr != nil {
				err = mErr
			} else {
				// File-based: manual restore + replay to track offsets and TTR
				if e := restorer.RestoreFromSnapshotWithFormat(m.SnapshotID, restoreFmt, restoreShards, keysHint); e != nil {
					err = e
				} else {
					result = restorer.ReplayChangelog(fmt.Sprintf("%s/opb.jsonl", cfg.ChangelogDir), m.LastChangelogOffset)
					if result.Error != nil {
						err = result.Error
					}
				}
			}
		}
		if err != nil {
			log.Printf("restore failed: %v", err)
		} else {
			log.Printf("restore completed: applied=%d skipped=%d", result.Applied, result.Skipped)
		}
		// Update recovered/healthy status with TTR
		appStatus.SetRecovered(time.Since(t0), int64(result.Applied), int64(result.Skipped))

		// In recovery scenario (no Kafka input loop) keep process alive gracefully until SIGINT/SIGTERM
		if cfg.KafkaBootstrap != "" && cfg.InputSource != "kafka" {
			log.Printf("recovery mode: standing by; health=%s (waiting for SIGINT/SIGTERM)", appStatus.Load().Status)
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			<-sigCh
			log.Printf("shutdown signal received, exiting recovery mode")
		}
	}

	log.Printf("OpB scaffold completed. Exiting.")
	return nil
}

func writeRestoreMetrics(path string, rm restoreMetrics) error {
	if path == "" {
		return fmt.Errorf("restore metrics path empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(rm, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func readRestoreMetrics(path string) (restoreMetrics, error) {
	if path == "" {
		return restoreMetrics{}, fmt.Errorf("restore metrics path empty")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return restoreMetrics{}, err
	}
	var rm restoreMetrics
	if err := json.Unmarshal(data, &rm); err != nil {
		return restoreMetrics{}, err
	}
	return rm, nil
}
