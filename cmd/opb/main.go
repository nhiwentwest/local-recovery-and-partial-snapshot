package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
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
	resolveSnapshotFormat := func(manifestFormat string) snapshot.Format {
		format := snapFormat
		if manifestFormat != "" {
			if parsed, perr := snapshot.ParseFormat(manifestFormat); perr == nil {
				format = parsed
			} else {
				log.Printf("restore: unknown snapshot format %s, defaulting to %s", manifestFormat, format)
			}
		}
		return format
	}
	resolveSnapshotShards := func(manifestShards int) int {
		if manifestShards > 0 {
			return manifestShards
		}
		if cfg.SnapshotShards > 0 {
			return cfg.SnapshotShards
		}
		return 1
	}
	readSnapshotManifest := func(snapID string) (manifest.Manifest, error) {
		if snapID == "" {
			return manifest.Manifest{}, fmt.Errorf("empty snapshot id")
		}
		p := filepath.Join(cfg.SnapshotDir, snapID, "manifest.json")
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
	snapshotSizeBytes := func(snapshotID string, format snapshot.Format, shards int) float64 {
		dir := filepath.Join(cfg.SnapshotDir, snapshotID)
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
	deltaSnapshotSizeBytes := func(snapshotID string, format snapshot.Format, shards int) float64 {
		dir := filepath.Join(cfg.SnapshotDir, snapshotID)
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
	snapshotIncrementalBytes := func(snapshotID string, files []string) float64 {
		if len(files) == 0 {
			return 0
		}
		dir := filepath.Join(cfg.SnapshotDir, snapshotID)
		var total float64
		for _, f := range files {
			fp := filepath.Join(dir, f)
			if fi, err := os.Stat(fp); err == nil {
				total += float64(fi.Size())
			}
		}
		return total
	}

	// Init state store
	var st state.Store
	switch cfg.StateBackend {
	case "pebble":
		ps, err := state.NewPebbleStore(cfg.StateDir)
		if err != nil {
			return fmt.Errorf("init pebble: %w", err)
		}
		defer ps.Close()
		st = ps
	case "memory":
		st = state.NewInMemoryStore()
	default:
		return fmt.Errorf("unknown state-backend: %s (use pebble|memory)", cfg.StateBackend)
	}

	// Set transient instance-id to state store for LastUpdatedBy visibility
	switch v := st.(type) {
	case *state.InMemoryStore:
		v.SetInstanceID(cfg.InstanceID)
	case *state.PebbleStore:
		v.SetInstanceID(cfg.InstanceID)
	}

	// Init Pebble snapshotters (pebble-only mode).
	var fullSnap snapshot.Snapshotter
	var fullSnapView snapshotViewWriter
	var deltaSnapView snapshotViewWriter
	var deltaIncremental *snapshot.PebbleSnapshotter

	pebbleSnapper := snapshot.NewPebbleSnapshotter(cfg.SnapshotDir)
	fullSnap = pebbleSnapper

	if cfg.EnablePebblePhase3 {
		if _, ok := st.(state.IncrementalCheckpointCapable); ok {
			deltaIncremental = pebbleSnapper
			log.Printf("delta snapshots will use Pebble incremental shipping (Phase 3)")
		} else {
			log.Printf("warning: enable-pebble-phase3 set but store is not IncrementalCheckpointCapable; falling back to Phase 2 delta")
		}
	}
	if deltaIncremental == nil {
		if _, ok := st.(state.DeltaCheckpointCapable); ok {
			deltaSnapView = pebbleDeltaWriter{snap: pebbleSnapper, st: st}
		} else {
			return fmt.Errorf("state store does not support Pebble delta snapshots")
		}
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

	// Init changelog writer (file by default; kafka optional)
	var clog changelog.Writer
	// Track how many changelog records have been appended so far (for manifest offset)
	var changelogAppendedCount int64
	if cfg.ChangelogSink == "file" || cfg.ChangelogSink == "both" || cfg.ChangelogSink == "" {
		fw, err := changelog.NewFileWriter(cfg.ChangelogDir, "opb.jsonl")
		if err != nil {
			return fmt.Errorf("init changelog file: %w", err)
		}
		clog = fw
	}
	if (cfg.ChangelogSink == "kafka" || cfg.ChangelogSink == "both") && cfg.KafkaBootstrap != "" {
		kw := changelog.NewKafkaWriter(cfg.KafkaBootstrap, cfg.TopicChangelog)
		if clog == nil {
			clog = kw
		} else {
			clog = changelog.NewMultiWriter(clog, kw)
		}
	}
	changelogKafkaEnabled := cfg.ChangelogSink == "kafka" || cfg.ChangelogSink == "both"

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
	type snapshotCutRequest struct {
		cutType string
		prev    *manifest.Manifest
	}
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

	go func(addr string) {
		mux := http.NewServeMux()
		// Cluster cache for viz (background polling of peers; handler reads cache only)
		type peerEntry struct {
			St       opb.AppStatus
			LastSeen time.Time
			Fails    int
			NextTry  time.Time
		}
		type clusterCache struct {
			mu sync.RWMutex
			m  map[string]*peerEntry
		}
		cc := &clusterCache{m: make(map[string]*peerEntry)}
		sendIngestCmd := func(pause bool) error {
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

		mkSelf := func() string {
			addr := strings.TrimSpace(cfg.HTTPAddr)
			if strings.HasPrefix(addr, ":") {
				return "http://127.0.0.1" + addr
			}
			if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
				return addr
			}
			return "http://" + addr
		}
		peersList := func() []string {
			self := mkSelf()
			seen := map[string]bool{self: true}
			urls := []string{self}
			if cfg.PeersCSV != "" {
				for _, p := range strings.Split(cfg.PeersCSV, ",") {
					p = strings.TrimSpace(p)
					if p == "" {
						continue
					}
					if !seen[p] {
						seen[p] = true
						urls = append(urls, p)
					}
				}
			}
			return urls
		}
		instanceForURL := func(u string) string {
			cc.mu.RLock()
			defer cc.mu.RUnlock()
			if pe := cc.m[u]; pe != nil && pe.St.Instance != "" {
				return pe.St.Instance
			}
			return u
		}
		promHTTPClient := &http.Client{Timeout: 4 * time.Second}
		vizInt := defaultVizPeerInterval
		vizTmo := defaultVizPeerTimeout
		vizTTL := defaultVizPeerTTL
		vizBackoff := defaultVizPeerBackoff
		// Self updater
		go func() {
			t := time.NewTicker(vizInt)
			defer t.Stop()
			for range t.C {
				st := appStatus.Load()
				now := time.Now()
				self := mkSelf()
				cc.mu.Lock()
				pe := cc.m[self]
				if pe == nil {
					pe = &peerEntry{}
					cc.m[self] = pe
				}
				pe.St = st
				pe.LastSeen = now
				pe.Fails = 0
				pe.NextTry = time.Time{}
				cc.mu.Unlock()
			}
		}()
		// Peers poller
		go func() {
			t := time.NewTicker(vizInt)
			defer t.Stop()
			cli := &http.Client{Timeout: vizTmo}
			for range t.C {
				for _, u := range peersList() {
					if u == mkSelf() {
						continue
					}
					now := time.Now()
					cc.mu.RLock()
					pe := cc.m[u]
					cc.mu.RUnlock()
					if pe != nil && pe.Fails > 0 && pe.NextTry.After(now) {
						continue
					}
					// fetch
					var st opb.AppStatus
					ok := false
					if resp, err := cli.Get(strings.TrimRight(u, "/") + "/status"); err == nil {
						func() {
							defer resp.Body.Close()
							if err := json.NewDecoder(resp.Body).Decode(&st); err == nil {
								ok = true
							}
						}()
					}
					cc.mu.Lock()
					if ok {
						if pe == nil {
							pe = &peerEntry{}
							cc.m[u] = pe
						}
						pe.St = st
						pe.LastSeen = now
						pe.Fails = 0
						pe.NextTry = time.Time{}
					} else {
						if pe == nil {
							pe = &peerEntry{}
							cc.m[u] = pe
						}
						pe.Fails++
						pe.NextTry = now.Add(vizBackoff)
					}
					cc.mu.Unlock()
				}
			}
		}()
		mux.Handle("/metrics", mreg.Handler())
		// Admin: trigger snapshot cut (best-effort). POST only.
		mux.HandleFunc("/admin/snapshot-cut", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			q := r.URL.Query()
			stype := strings.ToLower(strings.TrimSpace(q.Get("type")))
			if stype == "" {
				stype = manifest.SnapshotTypeFull
			}
			if stype != manifest.SnapshotTypeFull && stype != manifest.SnapshotTypeDelta && stype != "auto" {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid type (use full|delta|auto)"})
				return
			}
			var prev *manifest.Manifest
			resolved := stype
			// Auto policy: decide between full|delta based on chain length/bytes
			if stype == "auto" {
				m, err := maniReader.ReadLatest()
				if err != nil || m.SnapshotID == "" {
					resolved = manifest.SnapshotTypeFull
				} else {
					// If delta disabled by config
					if cfg.SnapMaxDeltas <= 0 {
						resolved = manifest.SnapshotTypeFull
					} else {
						// compute delta chain count and bytes from latest backwards
						deltaCount := 0
						var deltaBytes float64
						// The logic should check the chain *ending at* the current latest manifest.
						// If the latest is already a 'full', the delta count is 0.
						// If it's a 'delta', we walk backwards to count the chain length.
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
						// apply thresholds
						if deltaCount >= cfg.SnapMaxDeltas {
							resolved = manifest.SnapshotTypeFull
						} else if cfg.SnapMaxDeltaMB > 0 && (deltaBytes/1024.0/1024.0) >= float64(cfg.SnapMaxDeltaMB) {
							resolved = manifest.SnapshotTypeFull
						} else if m.Changelog != nil && len(m.Changelog.Offsets) > 0 && m.Changelog.Topic != "" {
							resolved = manifest.SnapshotTypeDelta
							prev = &m
						} else {
							resolved = manifest.SnapshotTypeFull
						}
					}
				}
			}
			if resolved == manifest.SnapshotTypeDelta && prev == nil {
				// explicit delta or resolved delta: need prev manifest with offsets
				m, err := maniReader.ReadLatest()
				if err != nil || m.SnapshotID == "" || m.Changelog == nil || len(m.Changelog.Offsets) == 0 || m.Changelog.Topic == "" {
					w.WriteHeader(http.StatusBadRequest)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": "delta cut requires existing manifest with per-partition offsets"})
					return
				}
				prev = &m
			}
			req := snapshotCutRequest{cutType: resolved, prev: prev}
			select {
			case snapshotCutReq <- req:
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted", "type": resolved})
			default:
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "busy"})
			}
		})

		// Admin: trigger snapshot GC (best-effort). POST only.
		gc := snapshot.NewGarbageCollector(cfg.SnapshotDir, cfg.SnapRetentionCount, cfg.SnapRetentionDays, maniReader)
		mux.HandleFunc("/admin/snapshot-gc", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			deleted, err := gc.Collect()
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"deleted": deleted, "count": len(deleted)})
		})

		// Admin: export full state as NDJSON of {key,state}
		mux.HandleFunc("/admin/state/export", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/x-ndjson")
			view, err := st.NewSnapshotView()
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("{\"error\":\"snapshot view error\"}\n"))
				return
			}
			defer view.Close()
			bw := bufio.NewWriter(w)
			type row struct {
				Key   string            `json:"key"`
				State state.RecordState `json:"state"`
			}
			_ = view.Range(func(k string, rs state.RecordState) error {
				b, _ := json.Marshal(row{Key: k, State: rs})
				bw.Write(b)
				bw.WriteByte('\n')
				return nil
			})
			bw.Flush()
		})

		mux.HandleFunc("/admin/ingest/pause", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			if !ingestControlEnabled {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "ingest control not available"})
				return
			}
			if ingestPaused.Load() {
				_ = json.NewEncoder(w).Encode(map[string]any{"paused": true, "status": "already-paused"})
				return
			}
			if err := sendIngestCmd(true); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"paused": true})
		})
		mux.HandleFunc("/admin/ingest/resume", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			if !ingestControlEnabled {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "ingest control not available"})
				return
			}
			if !ingestPaused.Load() {
				_ = json.NewEncoder(w).Encode(map[string]any{"paused": false, "status": "already-running"})
				return
			}
			if err := sendIngestCmd(false); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"paused": false})
		})
		// Admin: prune state keys older than a window start threshold.
		mux.HandleFunc("/admin/prune-state", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			var req struct {
				StoreID           string `json:"storeId"`
				ProductID         string `json:"productId"`
				WindowStartBefore int64  `json:"windowStartBefore"`
				Limit             int    `json:"limit"`
				DryRun            bool   `json:"dryRun"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid json"})
				return
			}
			if req.WindowStartBefore <= 0 {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "windowStartBefore must be >0"})
				return
			}
			limit := req.Limit
			if limit <= 0 || limit > 50000 {
				limit = 1000
			}
			start := time.Now()
			var (
				scanned   int
				selected  []string
				sample    []map[string]any
				errLimit  = errors.New("prune limit reached")
				matchFunc = func(key string, rs state.RecordState) error {
					scanned++
					parts := strings.Split(key, "#")
					if len(parts) != 3 {
						return nil
					}
					if req.StoreID != "" && parts[0] != req.StoreID {
						return nil
					}
					if req.ProductID != "" && parts[1] != req.ProductID {
						return nil
					}
					ws, err := strconv.ParseInt(parts[2], 10, 64)
					if err != nil {
						return nil
					}
					if ws >= req.WindowStartBefore {
						return nil
					}
					selected = append(selected, key)
					if len(sample) < 10 {
						sample = append(sample, map[string]any{
							"key":       key,
							"storeId":   parts[0],
							"productId": parts[1],
							"ws":        ws,
							"sumQty":    rs.SumQty,
							"sumAmount": rs.SumAmount,
						})
					}
					if len(selected) >= limit {
						return errLimit
					}
					return nil
				}
			)
			if err := st.Range(matchFunc); err != nil && !errors.Is(err, errLimit) {
				log.Printf("prune-state: range error: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			deleted := 0
			if !req.DryRun {
				for _, key := range selected {
					if err := st.Delete(key); err != nil {
						log.Printf("prune-state: delete key=%s err=%v", key, err)
						continue
					}
					deleted++
				}
			}
			resp := map[string]any{
				"storeId":           req.StoreID,
				"productId":         req.ProductID,
				"windowStartBefore": req.WindowStartBefore,
				"limit":             limit,
				"matched":           len(selected),
				"deleted":           deleted,
				"dryRun":            req.DryRun,
				"scanned":           scanned,
				"durationMs":        time.Since(start).Milliseconds(),
				"sample":            sample,
			}
			log.Printf("prune-state: store=%s product=%s before=%d dryRun=%v matched=%d deleted=%d scanned=%d", req.StoreID, req.ProductID, req.WindowStartBefore, req.DryRun, len(selected), deleted, scanned)
			_ = json.NewEncoder(w).Encode(resp)
		})
		// Heatmap JSON and static UI
		mux.Handle("/viz/heatmap", opb.NewHeatmapHandler(st, cfg.WindowSizeSec, cfg.InstanceID))
		mux.Handle("/api/zone-details", opb.NewZoneDetailsHandler(st, zoneIdx, cfg.WindowSizeSec, cfg.InstanceID, opb.RealClock{}))
		mux.HandleFunc("/api/inject-test-data", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			if injP == nil || injErr != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "injector unavailable"})
				return
			}
			// Rate limit: 1 req per 2s per client
			ip := r.RemoteAddr
			now := time.Now()
			if last, ok := injLast[ip]; ok && now.Sub(last) < 2*time.Second {
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "rate limited"})
				return
			}
			injLast[ip] = now
			type injectJob struct {
				StoreID   string `json:"storeId"`
				ProductID string `json:"productId"`
				WS        int64  `json:"ws"`
				Mode      string `json:"mode"`
				N         int    `json:"n"`
				Start     int    `json:"start"`
				Sync      bool   `json:"sync"`
				// Ride-like optional fields for realistic pricing
				DistanceKm    float64 `json:"distanceKm,omitempty"`
				DistanceMinKm float64 `json:"distanceMinKm,omitempty"`
				DistanceMaxKm float64 `json:"distanceMaxKm,omitempty"`
				FareBase      int64   `json:"fareBase,omitempty"`
				FarePerKm     int64   `json:"farePerKm,omitempty"`
				SurgeMin      float64 `json:"surgeMin,omitempty"`
				SurgeMax      float64 `json:"surgeMax,omitempty"`
				Currency      string  `json:"currency,omitempty"`
			}
			var req []injectJob
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid json, expected array of jobs"})
				return
			}

			if len(req) == 0 {
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "no-jobs", "jobs": 0, "totalEvents": 0})
				return
			}

			jobs := make([]injectJob, len(req))
			totalInjected := 0
			isSync := false
			for i, job := range req {
				if job.N > 50000 {
					job.N = 50000
				}
				jobs[i] = job
				totalInjected += job.N
				if job.Sync {
					isSync = true
				}
			}

			producerFunc := func() {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				// Epoch token shared across this injection batch for fencing / diagnostics.
				epoch := []byte(fmt.Sprintf("%d", time.Now().UnixNano()))
				var wg sync.WaitGroup
				for _, job := range jobs {
					job := job
					wg.Add(1)
					go func(rr injectJob) {
						defer wg.Done()
						defer func() { recover() }()
						// Simple per-job vector clock: single dimension tied to instance-id to
						// make causal headers visible in inflight.json and manifests.
						vc := opb.NewVectorClock()
						vcID := cfg.InstanceID
						if vcID == "" {
							vcID = "injector"
						}
						for i := 0; i < rr.N; i++ {
							store := rr.StoreID
							prod := rr.ProductID
							if prod == "" {
								prod = fmt.Sprintf("p%d", (i%100)+1)
							}
							idx := i + rr.Start
							ordID := fmt.Sprintf("%s-ord-%d-%d", rr.StoreID, rr.WS, idx)
							ts := time.Now().Unix()
							ws := opb.WindowStart(ts, cfg.WindowSizeSec)
							if rr.WS > 0 {
								ws = rr.WS
								ts = rr.WS
							}
							// Ride-like pricing computation (optional); fallback to default 10000
							dKm := rr.DistanceKm
							if dKm <= 0 {
								if rr.DistanceMaxKm > 0 {
									min := rr.DistanceMinKm
									max := rr.DistanceMaxKm
									if max < min {
										max = min
									}
									dKm = min + rng.Float64()*(max-min+1e-9)
								} else {
									dKm = 1.0 + rng.Float64()*4.0
								}
							}
							base := rr.FareBase
							if base <= 0 {
								base = 5000
							}
							perKm := rr.FarePerKm
							if perKm <= 0 {
								perKm = 3500
							}
							surgeMin := rr.SurgeMin
							surgeMax := rr.SurgeMax
							if surgeMin <= 0 {
								surgeMin = 1.0
							}
							if surgeMax <= 0 {
								surgeMax = surgeMin
							}
							if surgeMax < surgeMin {
								surgeMax = surgeMin
							}
							surge := surgeMin
							if surgeMax > surgeMin {
								surge = surgeMin + rng.Float64()*(surgeMax-surgeMin)
							}
							price := int64(math.Round((float64(base) + float64(perKm)*dKm) * surge))
							if price <= 0 {
								price = 10000
							}
							currency := rr.Currency
							if currency == "" {
								currency = "VND"
							}

							payload := opb.OrderEnriched{
								OrderID:   ordID,
								ProductID: prod,
								Price:     price,
								Qty:       1,
								StoreID:   store,
								TS:        ts,
								Validated: true,
								NormTS:    ws,
							}
							val, _ := json.Marshal(payload)
							key := []byte(fmt.Sprintf("%s#%s#%d", store, prod, ws))
							// Tick vector clock and attach standard t0/t1 + epoch + VC headers.
							vc = vc.Tick(vcID)
							headers := opb.BuildHeadersWithEpochAndVC(opb.RealClock{}, nil, epoch, vc)
							_ = injP.Produce(&ck.Message{
								TopicPartition: ck.TopicPartition{Topic: &cfg.TopicEnriched, Partition: ck.PartitionAny},
								Key:            key,
								Value:          val,
								Headers:        headers,
							}, nil)
						}
					}(job)
				}
				wg.Wait()
				injP.Flush(15000)
			}

			if isSync {
				producerFunc()
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "completed", "jobs": len(req), "totalEvents": totalInjected})
				return
			}

			go producerFunc()
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "queued", "jobs": len(req), "totalEvents": totalInjected})
		})
		// Lightweight exact state JSON endpoint for diagnostics
		mux.HandleFunc("/api/exact", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			q := r.URL.Query()
			storeID := q.Get("storeId")
			prodID := q.Get("productId")
			wsStr := q.Get("ws")
			if storeID == "" || prodID == "" || wsStr == "" {
				_ = json.NewEncoder(w).Encode(map[string]any{"found": false, "error": "missing params"})
				return
			}
			ws, err := strconv.ParseInt(wsStr, 10, 64)
			if err != nil {
				_ = json.NewEncoder(w).Encode(map[string]any{"found": false, "error": "bad ws"})
				return
			}
			key := opb.OutputKey(storeID, prodID, ws)
			if rec, ok := st.Get(key); ok {
				_ = json.NewEncoder(w).Encode(map[string]any{"found": true, "sumQty": rec.SumQty, "sumAmount": rec.SumAmount, "lastSeq": rec.LastSeq, "lastUpdatedBy": rec.LastUpdatedBy, "key": key})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"found": false, "key": key})
		})
		// Debug endpoint: list all keys for a storeId to understand heatmap aggregation
		mux.HandleFunc("/api/debug-store-keys", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			q := r.URL.Query()
			storeID := q.Get("storeId")
			if storeID == "" {
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "missing storeId param"})
				return
			}
			var keys []map[string]any
			var totalSumQty int64
			_ = st.Range(func(key string, rs state.RecordState) error {
				parts := strings.Split(key, "#")
				if len(parts) == 3 && parts[0] == storeID {
					totalSumQty += rs.SumQty
					keys = append(keys, map[string]any{
						"key":       key,
						"productId": parts[1],
						"ws":        parts[2],
						"sumQty":    rs.SumQty,
						"sumAmount": rs.SumAmount,
						"lastSeq":   rs.LastSeq,
					})
				}
				return nil
			})
			_ = json.NewEncoder(w).Encode(map[string]any{
				"storeId":     storeID,
				"keys":        keys,
				"totalSumQty": totalSumQty,
				"count":       len(keys),
			})
		})
		fs := http.FileServer(http.Dir("./web/viz"))
		noCache := func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
				w.Header().Set("Pragma", "no-cache")
				w.Header().Set("Expires", "0")
				h.ServeHTTP(w, r)
			})
		}

		mux.HandleFunc("/viz/snapshot-insights", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			type timelineEntry struct {
				SnapshotID       string `json:"snapshotId"`
				Type             string `json:"type"`
				BaseSnapshotID   string `json:"baseSnapshotId,omitempty"`
				ParentSnapshotID string `json:"parentSnapshotId,omitempty"`
				DeltaSequence    int    `json:"deltaSequence,omitempty"`
				CreatedAtEpoch   int64  `json:"createdAt"`
				CreatedAtISO     string `json:"createdAtIso,omitempty"`
				IncrementalFiles int    `json:"incrementalFiles"`
				TotalFiles       int    `json:"totalFiles"`
			}
			isRestorePhasesEmpty := func(ph restorePhaseTimings) bool {
				return ph.ManifestMs == 0 &&
					ph.SnapshotTotalMs == 0 &&
					ph.SnapshotReadMs == 0 &&
					ph.SnapshotDecodeMs == 0 &&
					ph.SnapshotLoadMs == 0 &&
					ph.ChangelogMs == 0 &&
					ph.MetricsMs == 0 &&
					ph.TotalMs == 0
			}
			isRestoreMetricsEmpty := func(rm restoreMetrics) bool {
				return rm.SnapshotID == "" &&
					rm.TTRMs == 0 &&
					rm.LastChangelogOffset == 0 &&
					rm.Applied == 0 &&
					rm.Skipped == 0 &&
					isRestorePhasesEmpty(rm.Phases)
			}
			resp := map[string]any{}
			if latest, err := maniReader.ReadLatest(); err == nil && latest.SnapshotID != "" {
				var timeline []timelineEntry
				cur := latest
				for i := 0; i < 6 && cur.SnapshotID != ""; i++ {
					totalFiles := len(cur.PebbleAllFiles)
					if totalFiles == 0 {
						totalFiles = len(cur.PebbleSSTFiles)
					}
					var iso string
					if cur.CreatedAtEpochSecond > 0 {
						iso = time.Unix(cur.CreatedAtEpochSecond, 0).UTC().Format(time.RFC3339)
					}
					entry := timelineEntry{
						SnapshotID:       cur.SnapshotID,
						Type:             strings.ToLower(cur.SnapshotType),
						BaseSnapshotID:   cur.BaseSnapshotID,
						ParentSnapshotID: cur.ParentSnapshotID,
						DeltaSequence:    cur.DeltaSequence,
						CreatedAtEpoch:   cur.CreatedAtEpochSecond,
						CreatedAtISO:     iso,
						IncrementalFiles: len(cur.PebbleIncrementalFiles),
						TotalFiles:       totalFiles,
					}
					timeline = append(timeline, entry)
					if cur.ParentSnapshotID == "" {
						break
					}
					if next, err := readSnapshotManifest(cur.ParentSnapshotID); err == nil {
						cur = next
					} else {
						break
					}
				}
				resp["timeline"] = timeline
				if len(timeline) > 0 {
					resp["latest"] = timeline[0]
				}
			}
			var localRestore *restoreMetrics
			if rm, err := readRestoreMetrics(metricsPath); err == nil {
				res := rm
				resp["restoreMetrics"] = res
				resp["restoreInstance"] = cfg.InstanceID
				resp["restoreSource"] = cfg.InstanceID
				if !isRestorePhasesEmpty(res.Phases) {
					resp["restorePhases"] = res.Phases
				}
				localRestore = &res
			}
			if r.URL.Query().Get("raw") == "1" {
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
			restoreFleet := map[string]restoreMetrics{}
			if localRestore != nil && !isRestoreMetricsEmpty(*localRestore) {
				restoreFleet[cfg.InstanceID] = *localRestore
			}
			client := &http.Client{Timeout: 2 * time.Second}
			for _, peer := range peersList() {
				if peer == mkSelf() {
					continue
				}
				url := strings.TrimRight(peer, "/") + "/viz/snapshot-insights?raw=1"
				respPeer, err := client.Get(url)
				if err != nil {
					continue
				}
				var payload struct {
					RestoreMetrics restoreMetrics `json:"restoreMetrics"`
					RestoreSource  string         `json:"restoreSource"`
				}
				if err := json.NewDecoder(respPeer.Body).Decode(&payload); err != nil {
					_ = respPeer.Body.Close()
					continue
				}
				_ = respPeer.Body.Close()
				if isRestoreMetricsEmpty(payload.RestoreMetrics) {
					continue
				}
				name := payload.RestoreSource
				if name == "" {
					name = instanceForURL(peer)
				}
				restoreFleet[name] = payload.RestoreMetrics
			}
			if len(restoreFleet) > 0 {
				resp["restoreFleet"] = restoreFleet
				if _, ok := resp["restorePhases"]; !ok {
					for inst, rm := range restoreFleet {
						if !isRestorePhasesEmpty(rm.Phases) {
							resp["restorePhases"] = rm.Phases
							resp["restoreSource"] = inst
							break
						}
					}
				}
				if _, ok := resp["restoreSource"]; !ok {
					for inst := range restoreFleet {
						resp["restoreSource"] = inst
						break
					}
				}
			}
			_ = json.NewEncoder(w).Encode(resp)
		})
		mux.Handle("/viz/", noCache(http.StripPrefix("/viz/", fs)))

		mux.HandleFunc("/viz/prom-range", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			query := strings.TrimSpace(r.URL.Query().Get("query"))
			if query == "" {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "query required"})
				return
			}
			seconds := 300
			if v := strings.TrimSpace(r.URL.Query().Get("seconds")); v != "" {
				if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
					if parsed > 7200 {
						parsed = 7200
					}
					seconds = parsed
				}
			}
			base := strings.TrimSpace(r.URL.Query().Get("base"))
			if base == "" {
				base = cfg.PromURL
			}
			if base == "" {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "prometheus base URL not configured"})
				return
			}
			if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
				base = "http://" + base
			}
			base = strings.TrimRight(base, "/")
			end := time.Now().Unix()
			start := end - int64(seconds)
			if start < 0 {
				start = 0
			}
			step := seconds / 200
			if step < 1 {
				step = 1
			}
			req, err := http.NewRequest(http.MethodGet, base+"/api/v1/query_range", nil)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			q := req.URL.Query()
			q.Set("query", query)
			q.Set("start", strconv.FormatInt(start, 10))
			q.Set("end", strconv.FormatInt(end, 10))
			q.Set("step", strconv.Itoa(step))
			req.URL.RawQuery = q.Encode()
			respProm, err := promHTTPClient.Do(req)
			if err != nil {
				w.WriteHeader(http.StatusBadGateway)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			defer respProm.Body.Close()
			if respProm.StatusCode >= 400 {
				w.WriteHeader(http.StatusBadGateway)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("prometheus status %d", respProm.StatusCode)})
				return
			}
			var promResp struct {
				Status string `json:"status"`
				Data   struct {
					Result []struct {
						Values [][]interface{} `json:"values"`
					} `json:"result"`
				} `json:"data"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(respProm.Body).Decode(&promResp); err != nil {
				w.WriteHeader(http.StatusBadGateway)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			if promResp.Status != "success" || len(promResp.Data.Result) == 0 {
				_ = json.NewEncoder(w).Encode(map[string]any{"values": [][]interface{}{}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"values": promResp.Data.Result[0].Values})
		})

		// Helper: build cluster response from cache
		type inst struct {
			HTTP string `json:"http"`
			opb.AppStatus
		}
		type resp struct {
			Instances  []inst            `json:"instances"`
			Assignment map[string]string `json:"assignment"`
		}
		buildCluster := func() resp {
			urls := peersList()
			out := resp{Instances: make([]inst, 0, len(urls)), Assignment: map[string]string{}}
			now := time.Now()
			cc.mu.RLock()
			for _, u := range urls {
				pe := cc.m[u]
				if pe == nil || now.Sub(pe.LastSeen) > vizTTL {
					out.Instances = append(out.Instances, inst{HTTP: u, AppStatus: opb.AppStatus{Status: "down"}})
					continue
				}
				out.Instances = append(out.Instances, inst{HTTP: u, AppStatus: pe.St})
				for _, p := range pe.St.Partitions {
					out.Assignment[fmt.Sprintf("%d", p)] = pe.St.Instance
				}
			}
			cc.mu.RUnlock()
			return out
		}
		// Cluster API: return cached cluster snapshot (no peer fetches in handler)
		mux.HandleFunc("/api/cluster", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(buildCluster())
		})

		// Cluster viz page
		mux.HandleFunc("/viz/cluster", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
			fmt.Fprintf(w, "<html><head><meta charset='utf-8'><meta http-equiv='refresh' content='5'>")
			fmt.Fprintf(w, "<style>body{font-family:system-ui,Segoe UI,Roboto,Arial;margin:16px;background:#0b1021;color:#e6e9ef}table{border-collapse:collapse;margin:8px 0;width:100%%;max-width:1100px}th,td{border:1px solid #2b3152;padding:8px 10px}th{background:#1b2244;color:#cdd6f4}tr:nth-child(even){background:#0f1530}tr:nth-child(odd){background:#0c1229}.tag{display:inline-block;padding:2px 8px;border-radius:12px;font-size:12px;border:1px solid #2b3152}.ok{background:#16331f;color:#a6e3a1;border-color:#204a2c}.down{background:#381a1a;color:#f38ba8;border-color:#5a2a2a}.muted{color:#a6accd}.small{font-size:12px;color:#a6accd}</style></head><body>")
			fmt.Fprintf(w, "<h3 style='margin:0 0 8px'>Cluster View <span class='small'>(auto-refresh 5s)</span></h3>")
			// Recovery summary panel (status-specific fields only)
			// removed recovery summary container
			fmt.Fprintf(w, "<div style='display:none'>")
			fmt.Fprintf(w, "<div style='display:none'>")
			fmt.Fprintf(w, "<!-- recovery summary removed -->")
			fmt.Fprintf(w, "</div>")
			fmt.Fprintf(w, "</div>")
			// Fetch API
			cli := &http.Client{Timeout: 2 * time.Second}
			addr := strings.TrimSpace(cfg.HTTPAddr)
			var api string
			if strings.HasPrefix(addr, ":") {
				api = "http://127.0.0.1" + addr + "/api/cluster"
			} else if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
				api = strings.TrimRight(addr, "/") + "/api/cluster"
			} else {
				api = "http://" + addr
				api = strings.TrimRight(api, "/") + "/api/cluster"
			}
			res, err := cli.Get(api)
			if err != nil {
				fmt.Fprintf(w, "<div class='down tag'>fetch error: %v</div>", err)
				fmt.Fprintf(w, "</body></html>")
				return
			}
			defer res.Body.Close()
			var data struct {
				Instances []struct {
					HTTP string `json:"http"`
					opb.AppStatus
				} `json:"instances"`
				Assignment map[string]string `json:"assignment"`
			}
			if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
				fmt.Fprintf(w, "<div class='down tag'>bad json</div>")
				fmt.Fprintf(w, "</body></html>")
				return
			}
			// Instances table
			fmt.Fprintf(w, "<h4 style='margin:16px 0 8px'>Instances</h4><table><tr><th>Instance</th><th>Status</th><th>HTTP</th><th>Topic</th><th>Partitions</th><th>LagTotal</th></tr>")
			for _, it := range data.Instances {
				statusClass := "ok"
				if strings.ToLower(it.Status) != "healthy" {
					statusClass = "down"
				}
				fmt.Fprintf(w, "<tr><td>%s</td><td><span class='tag %s'>%s</span></td><td><a href='%s' style='color:#89b4fa'>%s</a></td><td>%s</td><td>%v</td><td>%.0f</td></tr>", it.Instance, statusClass, it.Status, it.HTTP, it.HTTP, it.Topic, it.Partitions, it.LagTotal)
			}
			fmt.Fprintf(w, "</table>")
			// Assignment
			fmt.Fprintf(w, "<h4 style='margin:16px 0 8px'>Assignment</h4><table><tr><th>Partition</th><th>Instance</th></tr>")
			for part, inst := range data.Assignment {
				fmt.Fprintf(w, "<tr><td>%s</td><td>%s</td></tr>", part, inst)
			}
			fmt.Fprintf(w, "</table>")
			fmt.Fprintf(w, "<hr/><div><a href='/viz/' style='color:#89b4fa'>Back to heatmap</a></div>")
			// Minimal JS for Recovery Summary and Zone Probe
			fmt.Fprintf(w, "<script>\n(async function(){\n  async function loadStatus(){\n    try{\n      const res = await fetch('/status', {cache:'no-store'});\n      const j = await res.json();\n      const el = null;\n      if(!el) return;\n      const ttr = (j.ttrMs!==undefined? j.ttrMs+' ms':'N/A');\n      const snap = (j.restoringSnapshotId||'N/A');\n      const off = (j.lastChangelogOffset!==undefined? j.lastChangelogOffset:'N/A');\n      const ap = (j.lastRestoreApplied!==undefined? j.lastRestoreApplied:'N/A');\n      const sk = (j.lastRestoreSkipped!==undefined? j.lastRestoreSkipped:'N/A');\n      el.innerHTML = `<div>ttrMs: <b>${ttr}</b></div>`+\n                     `<div>snapshotId: <span class='muted'>${snap}</span></div>`+\n                     `<div>lastChangelogOffset: <span class='muted'>${off}</span></div>`+\n                     `<div>restore applied/skipped: <span class='muted'>${ap}</span>/<span class='muted'>${sk}</span></div>`;\n      // Default ws suggestion\n      const wsInput = document.getElementById('pf-ws');\n      if(wsInput && !wsInput.value && j.windowSizeSec){\n        const now = Math.floor(Date.now()/1000);\n        const ws = Math.floor(now / j.windowSizeSec) * j.windowSizeSec;\n        wsInput.value = String(ws);\n      }\n    }catch(e){\n      const el = null;\n      if(el) el.textContent = 'N/A';\n    }\n  }\n  function setupProbe(){\n    const btn = document.getElementById('pf-run');\n    if(!btn) return;\n    btn.addEventListener('click', async function(){\n      const s = document.getElementById('pf-store').value.trim();\n      const p = document.getElementById('pf-prod').value.trim();\n      const w = document.getElementById('pf-ws').value.trim();\n      if(!s || !p || !w){ return; }\n      const url = `/viz/zone-data?id=${encodeURIComponent(s)}&productId=${encodeURIComponent(p)}&ws=${encodeURIComponent(w)}`;\n      const pu = document.getElementById('probe-url');\n      if(pu) pu.textContent = url;\n      const fr = document.getElementById('probe-frame');\n      if(fr) fr.src = url;\n    });\n  }\n  await loadStatus();\n  setupProbe();\n})();\n</script>")
			fmt.Fprintf(w, "</body></html>")
		})

		mux.HandleFunc("/viz/zone-data", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("Expires", "0")
			q := r.URL.Query()
			id := q.Get("id")
			pid := q.Get("productId")
			wsStr := q.Get("ws")
			type windowStat struct {
				Window    int64
				SumQty    int64
				SumAmount int64
				Keys      int
			}
			type productStat struct {
				Product   string
				SumQty    int64
				SumAmount int64
				LastSeq   int64
			}
			windowStats := make(map[int64]*windowStat)
			windowProducts := make(map[int64]map[string]*productStat)
			var latestWindow int64
			var totalRecords int

			fmt.Fprintf(w, "<html><body style='font-family:system-ui; margin:16px'>")
			fmt.Fprintf(w, "<h3>Zone Data</h3>")
			fmt.Fprintf(w, "<div>id=%s productId=%s ws=%s</div>", id, pid, wsStr)
			if id == "" {
				fmt.Fprintf(w, "<div style='color:#b00020'>missing id</div></body></html>")
				return
			}
			// Store mode snapshot via index (sums local)
			sumA, sumQ, _ := zoneIdx.Snapshot(id)
			fmt.Fprintf(w, "<h4>Store mode (aggregates)</h4>")
			fmt.Fprintf(w, "<pre>{\n  \"storeId\": \"%s\",\n  \"sumAmount\": %d,\n  \"sumQty\": %d\n}</pre>", id, sumA, sumQ)
			fmt.Fprintf(w, "<div class='small muted'>Heatmap total = sumQty=%d (hiện tại). sumQty được tạo bởi số product active × số events trên mỗi product.</div>", sumQ)
			var totalSumQty, totalSumAmount int64
			var maxLastSeq int64
			var lastUpdatedBy string
			_ = st.Range(func(key string, rs state.RecordState) error {
				parts := strings.Split(key, "#")
				if len(parts) == 3 && parts[0] == id {
					totalSumQty += rs.SumQty
					totalSumAmount += rs.SumAmount
					if rs.LastSeq > maxLastSeq {
						maxLastSeq = rs.LastSeq
						lastUpdatedBy = rs.LastUpdatedBy
					}
					wsVal, err := strconv.ParseInt(parts[2], 10, 64)
					if err == nil {
						stat := windowStats[wsVal]
						if stat == nil {
							stat = &windowStat{Window: wsVal}
							windowStats[wsVal] = stat
						}
						stat.SumQty += rs.SumQty
						stat.SumAmount += rs.SumAmount
						stat.Keys++
						if wsVal > latestWindow {
							latestWindow = wsVal
						}
						pm := windowProducts[wsVal]
						if pm == nil {
							pm = make(map[string]*productStat)
							windowProducts[wsVal] = pm
						}
						ps := pm[parts[1]]
						if ps == nil {
							ps = &productStat{Product: parts[1]}
							pm[parts[1]] = ps
						}
						ps.SumQty += rs.SumQty
						ps.SumAmount += rs.SumAmount
						if rs.LastSeq > ps.LastSeq {
							ps.LastSeq = rs.LastSeq
						}
					}
					totalRecords++
				}
				return nil
			})
			fmt.Fprintf(w, "<div class='muted'>Total records=%d · windows=%d · sumQty=%d · sumAmount=%d · lastSeq=%d (by %s)</div>", totalRecords, len(windowStats), totalSumQty, totalSumAmount, maxLastSeq, lastUpdatedBy)

			windowList := make([]*windowStat, 0, len(windowStats))
			maxWindowQty := int64(0)
			for _, ws := range windowStats {
				windowList = append(windowList, ws)
				if ws.SumQty > maxWindowQty {
					maxWindowQty = ws.SumQty
				}
			}
			sort.Slice(windowList, func(i, j int) bool { return windowList[i].Window > windowList[j].Window })
			fmt.Fprintf(w, "<h4>Recent windows · windowSize=%ds</h4>", cfg.WindowSizeSec)
			if len(windowList) == 0 {
				fmt.Fprintf(w, "<div class='muted'>No per-window data for this store (yet).</div>")
			} else {
				fmt.Fprintf(w, "<div style='display:flex; flex-direction:column; gap:6px; max-width:520px'>")
				for i, ws := range windowList {
					if i >= 8 {
						break
					}
					width := 0.0
					if maxWindowQty > 0 {
						width = (float64(ws.SumQty) / float64(maxWindowQty)) * 100
					}
					fmt.Fprintf(w, "<div>")
					fmt.Fprintf(w, "<div class='small muted'>%d · sumQty=%d · products=%d</div>", ws.Window, ws.SumQty, ws.Keys)
					fmt.Fprintf(w, "<div style='height:10px;border-radius:6px;background:#1e1e2f;overflow:hidden'>")
					fmt.Fprintf(w, "<div style='height:10px;width:%.2f%%;background:#8f5cff'></div>", width)
					fmt.Fprintf(w, "</div>")
					fmt.Fprintf(w, "</div>")
				}
				fmt.Fprintf(w, "</div>")
			}

			targetWindow := latestWindow
			if wsParsed, err := strconv.ParseInt(wsStr, 10, 64); err == nil && wsParsed > 0 {
				targetWindow = wsParsed
			}
			if targetWindow > 0 {
				headline := fmt.Sprintf("Top products in window %d", targetWindow)
				if targetWindow == latestWindow {
					headline += " (latest)"
				}
				fmt.Fprintf(w, "<h4>%s</h4>", headline)
				prodMap := windowProducts[targetWindow]
				if len(prodMap) == 0 {
					fmt.Fprintf(w, "<div class='muted'>No products recorded for this window.</div>")
				} else {
					if stat, ok := windowStats[targetWindow]; ok {
						totalProducts := len(prodMap)
						avgPerProduct := float64(stat.SumQty)
						if totalProducts > 0 {
							avgPerProduct = avgPerProduct / float64(totalProducts)
						}
						fmt.Fprintf(w, "<div style='margin-bottom:8px;padding:6px 10px;border:1px solid #2b3152;border-radius:6px;background:#0c1229;color:#e6e9ef'>")
						fmt.Fprintf(w, "<div class='small'>Window %d summary</div>", targetWindow)
						fmt.Fprintf(w, "<div class='small muted'>unique products=%d · total keys=%d · sumQty=%d · avg qty/product=%.2f</div>", totalProducts, stat.Keys, stat.SumQty, avgPerProduct)
						explain := "sumQty được tính = uniqueProducts × avgQty/product."
						if avgPerProduct <= 1.05 {
							explain += " Avg qty ~1 ⇒ chênh lệch giữa các zone đến từ số lượng product active."
						} else {
							explain += " Avg qty >1 ⇒ một số product nhận nhiều event hơn, xem bảng bucket phía dưới."
						}
						fmt.Fprintf(w, "<div class='small muted'>%s</div>", explain)
						fmt.Fprintf(w, "</div>")
						// Distribution buckets
						buckets := [][2]int64{
							{1, 5},
							{6, 10},
							{11, 20},
							{21, 50},
							{51, math.MaxInt64},
						}
						bucketCounts := make([]int, len(buckets))
						for _, ps := range prodMap {
							for idx, b := range buckets {
								if ps.SumQty >= b[0] && ps.SumQty <= b[1] {
									bucketCounts[idx]++
									break
								}
							}
						}
						fmt.Fprintf(w, "<table style='border-collapse:collapse;margin:8px 0'><tr><th style='text-align:left;padding:4px'>qty range</th><th style='text-align:right;padding:4px'>product count</th></tr>")
						for idx, b := range buckets {
							label := fmt.Sprintf("%d-%s", b[0], func() string {
								if b[1] == math.MaxInt64 {
									return "+"
								}
								return fmt.Sprintf("%d", b[1])
							}())
							fmt.Fprintf(w, "<tr><td style='padding:4px'>%s</td><td style='text-align:right;padding:4px'>%d</td></tr>", label, bucketCounts[idx])
						}
						fmt.Fprintf(w, "</table>")
					}

					productList := make([]*productStat, 0, len(prodMap))
					for _, ps := range prodMap {
						productList = append(productList, ps)
					}
					sort.Slice(productList, func(i, j int) bool {
						if productList[i].SumQty == productList[j].SumQty {
							return productList[i].Product < productList[j].Product
						}
						return productList[i].SumQty > productList[j].SumQty
					})
					fmt.Fprintf(w, "<table style='border-collapse:collapse'><tr><th style='text-align:left;padding:4px'>productId</th><th style='text-align:right;padding:4px'>sumQty</th><th style='text-align:right;padding:4px'>sumAmount</th><th style='text-align:right;padding:4px'>lastSeq</th></tr>")
					for i, ps := range productList {
						if i >= 8 {
							break
						}
						fmt.Fprintf(w, "<tr><td style='padding:4px'>%s</td><td style='text-align:right;padding:4px'>%d</td><td style='text-align:right;padding:4px'>%d</td><td style='text-align:right;padding:4px'>%d</td></tr>", ps.Product, ps.SumQty, ps.SumAmount, ps.LastSeq)
					}
					fmt.Fprintf(w, "</table>")
					if stat, ok := windowStats[targetWindow]; ok {
						totalProducts := len(prodMap)
						avgPerProduct := float64(stat.SumQty)
						if totalProducts > 0 {
							avgPerProduct = avgPerProduct / float64(totalProducts)
						}
						fmt.Fprintf(w, "<div class='small muted'>Window %d summary: sumQty=%d, sumAmount=%d, keys=%d, uniqueProducts=%d, avgQtyPerProduct=%.2f</div>", targetWindow, stat.SumQty, stat.SumAmount, stat.Keys, totalProducts, avgPerProduct)
					}
				}
			}
			// Exact if pid+ws provided
			if pid != "" && wsStr != "" {
				if ws, err := strconv.ParseInt(wsStr, 10, 64); err == nil {
					key := opb.OutputKey(id, pid, ws)
					if rec, ok := st.Get(key); ok {
						fmt.Fprintf(w, "<h4>Exact mode</h4>")
						fmt.Fprintf(w, "<pre>{\n  \"storeId\": \"%s\",\n  \"productId\": \"%s\",\n  \"ws\": %d,\n  \"sumAmount\": %d,\n  \"sumQty\": %d,\n  \"lastSeq\": %d,\n  \"lastUpdatedBy\": \"%s\"\n}</pre>", id, pid, ws, rec.SumAmount, rec.SumQty, rec.LastSeq, rec.LastUpdatedBy)
					} else {
						fmt.Fprintf(w, "<div style='color:#b00020'>exact key not found (key=%s)</div>", key)
					}
				}
			}
			// Recovery + Causal + Cluster panels (data from /status and /api/cluster)
			fmt.Fprintf(w, "<div style='display:flex; flex-wrap:wrap; gap:12px; margin:12px 0'>")
			// Live Causal Cut Status (hidden by default)
			fmt.Fprintf(w, "<div id='causal-cut' style='flex:1 1 360px; padding:10px; border:1px solid #2b3152; border-radius:6px; background:#0c1229; color:#e6e9ef'>")
			fmt.Fprintf(w, "<div style='font-weight:600; margin-bottom:6px'>Live Causal Cut</div>")
			fmt.Fprintf(w, "<div class='small' id='causal-body'>no active cut</div>")
			fmt.Fprintf(w, "</div>")
			fmt.Fprintf(w, "</div>")
			// Script to populate panels
			fmt.Fprintf(w, "<script>(async function(){try{const r=await fetch('/status',{cache:'no-store'});const j=await r.json();var el=null;if(el){var ttr=(j.ttrMs!==undefined? j.ttrMs+' ms':'N/A');var snap=(j.restoringSnapshotId||'N/A');var ap=(j.lastRestoreApplied!==undefined? j.lastRestoreApplied:'N/A');var sk=(j.lastRestoreSkipped!==undefined? j.lastRestoreSkipped:'N/A');var cr=(j.causalReplayTotal!==undefined? j.causalReplayTotal:'N/A');el.innerHTML='<div>ttrMs: <b>'+ttr+'</b></div><div>snapshotId: <span class=\"muted\">'+snap+'</span></div><div>restore applied/skipped: <span class=\"muted\">'+ap+'</span>/<span class=\"muted\">'+sk+'</span></div><div>causal replay events: <span class=\"muted\">'+cr+'</span></div>';}"+
				"var cut=document.getElementById('causal-cut');var body=document.getElementById('causal-body');if(cut&&body){if(j.causalCutId){cut.style.display='block';var id=j.causalCutId;var phase=j.causalPhase||'';var seen=j.causalMarkersSeen||0;var total=j.causalMarkersTotal||0;var infl=j.causalInflight||0;try{localStorage.setItem('opbLastCausal', JSON.stringify({id, phase, seen, total, infl, ts: Date.now()}));}catch(_){/* ignore */}body.innerHTML='<div>id: <b>'+id+'</b></div>'+(phase?'<div>phase: <span class=\"muted\">'+phase+'</span></div>':'')+'<div>markers: <span class=\"muted\">'+seen+'/'+total+'</span></div><div>inflight events: <span class=\"muted\">'+infl+'</span></div>'; } else { try{var last=JSON.parse(localStorage.getItem('opbLastCausal')||'{}');}catch(_){last={};} if(last.id){ var age=Math.floor((Date.now()-(last.ts||Date.now()))/1000); if(age>300){cut.style.display='none';}else{cut.style.display='block';body.innerHTML='<div>last id: <b>'+last.id+'</b></div>'+(last.phase?'<div>phase: <span class=\"muted\">'+last.phase+'</span></div>':'')+'<div>markers: <span class=\"muted\">'+(last.seen||0)+'/'+(last.total||0)+'</span></div><div>inflight events: <span class=\"muted\">'+(last.infl||0)+'</span></div><div class=\"muted\">'+age+'s ago</div>';} } else { cut.style.display='none'; body.textContent='no active cut'; } } }}catch(e){}"+
				"})();</script>")
			fmt.Fprintf(w, "<script>(function(){const cut=document.getElementById('causal-cut');const body=document.getElementById('causal-body');async function refresh(){try{const r=await fetch('/status',{cache:'no-store'});const j=await r.json();if(!cut||!body)return;if(j.causalCutId){cut.style.display='block';const id=j.causalCutId,phase=j.causalPhase||'',seen=j.causalMarkersSeen||0,total=j.causalMarkersTotal||0,infl=j.causalInflight||0;try{localStorage.setItem('opbLastCausal',JSON.stringify({id,phase,seen,total,infl,ts:Date.now()}));}catch(_){ } body.innerHTML='<div>id: <b>'+id+'</b></div>'+(phase?'<div>phase: <span class=\"muted\">'+phase+'</span></div>':'')+'<div>markers: <span class=\"muted\">'+seen+'/'+total+'</span></div><div>inflight events: <span class=\"muted\">'+infl+'</span></div>'; } else { let last={}; try{last=JSON.parse(localStorage.getItem('opbLastCausal')||'{}');}catch(_){ } if(last.id){const age=Math.floor((Date.now()-(last.ts||Date.now()))/1000);if(age>300){cut.style.display='none';}else{cut.style.display='block';body.innerHTML='<div>last id: <b>'+last.id+'</b></div>'+(last.phase?'<div>phase: <span class=\"muted\">'+last.phase+'</span></div>':'')+'<div>markers: <span class=\"muted\">'+(last.seen||0)+'/'+(last.total||0)+'</span></div><div>inflight events: <span class=\"muted\">'+(last.infl||0)+'</span></div><div class=\"muted\">'+age+'s ago</div>';} } else { cut.style.display='none'; body.textContent='no active cut'; }} }catch(e){} } setInterval(refresh,1000);})();</script>")
			fmt.Fprintf(w, "<hr/><div><a href='/viz/'>Back to heatmap</a></div>")
			fmt.Fprintf(w, "</body></html>")
		})
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			if appStatus.Load().Status != "healthy" {
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{"status": appStatus.Load().Status})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		})
		mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			st := appStatus.Load()
			// Snapshot fast-path EOS counters from metrics registry
			// Prometheus client counters do not expose read, so we only include fields already in AppStatus for now.
			// Optionally, these counters could be mirrored into StatusManager if REST snapshots are required.
			_ = json.NewEncoder(w).Encode(st)
		})
		_ = http.ListenAndServe(addr, mux)
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
	var phaseTimings restorePhaseTimings
	if cfg.RestoreOnStart && !stateImported.Load() {
		restoreTsStart := time.Now()
		log.Printf("restore: starting (source=%s, changelogSource=%s, topicSnapshots=%s) at %s", cfg.ManifestSource, cfg.ChangelogSource, cfg.TopicSnapshots, restoreTsStart.Format(time.RFC3339Nano))
		// Read latest manifest with internal reader timeout (no long outer loop)
		manifestStart := time.Now()
		var m manifest.Manifest
		m, mErr := maniReader.ReadLatest()
		phaseTimings.ManifestMs = time.Since(manifestStart).Milliseconds()
		if mErr != nil || m.SnapshotID == "" {
			// Fallback: try filesystem manifest reader if kafka source fails and FS snapshot exists
			if cfg.SnapshotDir != "" {
				manifestStart = time.Now()
				if m2, e2 := rf.NewFilesystemReader(cfg.SnapshotDir).ReadLatest(); e2 == nil && m2.SnapshotID != "" {
					log.Printf("restore: fallback FS manifest loaded snapshotId=%s lastChangelogOffset=%d", m2.SnapshotID, m2.LastChangelogOffset)
					m, mErr = m2, nil
				}
				phaseTimings.ManifestMs += time.Since(manifestStart).Milliseconds()
			}
		}
		if mErr != nil || m.SnapshotID == "" {
			log.Printf("restore: no manifest found after wait; skipping restore (err=%v, snapshotId=%s)", mErr, m.SnapshotID)
		} else {
			log.Printf("restore: manifest loaded snapshotId=%s lastChangelogOffset=%d", m.SnapshotID, m.LastChangelogOffset)
			appStatus.SetRecovering(m.SnapshotID, m.LastChangelogOffset)
			t0 := time.Now()
			restorer := rf.NewRestorerWithOptions(st, fullSnap, maniReader, cfg.SnapshotDir, snapFormat, cfg.SnapshotShards)
			restoreFormat := resolveSnapshotFormat(m.SnapshotFormat)
			restoreShards := resolveSnapshotShards(m.SnapshotShards)
			// Always restore snapshot before replaying changelog (supports chain)
			snapshotStart := time.Now()
			var restoreErr error
			if strings.ToLower(m.SnapshotType) == manifest.SnapshotTypeDelta {
				restoreErr = restorer.RestoreChainFromLatestWithOptions(m, rf.RestoreOptions{Parallelism: cfg.RestoreParallelism, SkipMissingDelta: cfg.RestoreSkipMissingDelta, ValidateChain: cfg.RestoreValidateChain})
			} else {
				restoreErr = restorer.RestoreFromSnapshotWithFormatParallel(m.SnapshotID, restoreFormat, restoreShards, m.SnapshotKeys, cfg.RestoreParallelism)
			}
			if restoreErr != nil {
				log.Printf("restore snapshot error: %v", restoreErr)
				if cfg.RestoreOnly {
					return fmt.Errorf("restore failed: %w", restoreErr)
				}
			} else {
				phaseTimings.SnapshotTotalMs = time.Since(snapshotStart).Milliseconds()
				if stats := restorer.LastSnapshotStats(); stats.Shards >= 0 {
					phaseTimings.SnapshotReadMs = stats.ReadNs / int64(time.Millisecond)
					phaseTimings.SnapshotDecodeMs = stats.DecodeNs / int64(time.Millisecond)
					phaseTimings.SnapshotLoadMs = stats.LoadNs / int64(time.Millisecond)
				}
				log.Printf("restore: snapshot restored snapshotId=%s type=%s base=%s dseq=%d", m.SnapshotID, m.SnapshotType, m.BaseSnapshotID, m.DeltaSequence)
				var causalReplayEvents int64
				var inflightEventCount int
				var inflightChannelCount int
				if m.InflightFile != "" {
					if snap, ierr := readInflightSnapshot(cfg.SnapshotDir, m.SnapshotID, m.InflightFile); ierr != nil {
						log.Printf("restore: inflight read error: %v", ierr)
					} else {
						var replayTotal int
						for _, evs := range snap.Events {
							replayTotal += len(evs)
						}
						if cfg.SkipInflightReplay {
							log.Printf("restore: skip inflight replay: channels=%d events=%d (flag set)", len(snap.Events), replayTotal)
							// Still record the inflight count for status/metrics even when replay is skipped (stage-2 restart).
							causalReplayEvents = int64(replayTotal)
							inflightEventCount = replayTotal
							inflightChannelCount = len(snap.Events)
							if inflightChannelCount == 0 && snap.Channels != nil {
								inflightChannelCount = len(snap.Channels)
							}
						} else if err := replayInflightEvents(cfg, st, snap); err != nil {
							log.Printf("restore: inflight replay error: %v", err)
						} else if len(snap.Events) > 0 {
							mreg.CausalReplay.Add(float64(replayTotal))
							appStatus.AddCausalReplay(int64(replayTotal))
							causalReplayEvents = int64(replayTotal)
							inflightEventCount = replayTotal
							inflightChannelCount = len(snap.Events)
							if inflightChannelCount == 0 && snap.Channels != nil {
								inflightChannelCount = len(snap.Channels)
							}
							log.Printf("restore: inflight replay applied channels=%d events=%d", len(snap.Events), replayTotal)
						}
					}
				}
				var result rf.RestoreResult
				// replayFn captures the exact replay function (Kafka or file) used for the
				// initial changelog replay so that we can optionally invoke it multiple
				// additional times on the already-restored state store (Bundle 2 EOS demo).
				var replayFn func() rf.RestoreResult
				replayedOnce := false
				var changelogStart time.Time
				if cfg.ChangelogSource == "kafka" && cfg.KafkaBootstrap != "" {
					var skipKafkaReplay bool
					if cfg.RestoreTrustManifest && manifestAllowsReplaySkip(m) {
						skipKafkaReplay = true
						log.Printf("restore: freeze hint => skipping changelog replay (manifest replayRequired=false)")
					} else if m.Changelog != nil && m.Changelog.Topic != "" && len(m.Changelog.Offsets) > 0 {
						if hasBacklog, err := kafkautil.ChangelogHasBacklog(cfg.KafkaBootstrap, m.Changelog.Topic, m.Changelog.Offsets); err != nil {
							log.Printf("restore: changelog backlog check error: %v", err)
						} else if !hasBacklog {
							skipKafkaReplay = true
							log.Printf("restore: skipping changelog replay (no backlog beyond manifest offsets)")
						}
					}
					if !skipKafkaReplay {
						if m.Changelog != nil && m.Changelog.Topic != "" && len(m.Changelog.Offsets) > 0 {
							replayFn = func() rf.RestoreResult {
								return rk.ReplayChangelogKafkaParallel(st, []string{cfg.KafkaBootstrap}, m.Changelog.Topic, m.Changelog.Offsets, 0, cfg.ReplayWorkers)
							}
						} else {
							replayFn = func() rf.RestoreResult {
								return rk.ReplayChangelogKafkaParallel(st, []string{cfg.KafkaBootstrap}, cfg.TopicChangelog, nil, m.LastChangelogOffset, cfg.ReplayWorkers)
							}
						}
						changelogStart = time.Now()
						result = replayFn()
						replayedOnce = true
					}
				} else {
					// file mode
					changelogStart = time.Now()
					replayFn = func() rf.RestoreResult {
						return restorer.ReplayChangelog(fmt.Sprintf("%s/opb.jsonl", cfg.ChangelogDir), m.LastChangelogOffset)
					}
					result = replayFn()
					replayedOnce = true
				}
				if !changelogStart.IsZero() {
					phaseTimings.ChangelogMs = time.Since(changelogStart).Milliseconds()
				}
				if result.Error != nil {
					log.Printf("restore replay error: %v", result.Error)
					if cfg.RestoreOnly {
						return fmt.Errorf("replay failed: %w", result.Error)
					}
				} else {
					// Log the primary replay pass (pass=1). This is the canonical restore.
					if replayedOnce {
						log.Printf("bundle2: replay pass=%d applied=%d skipped=%d", 1, result.Applied, result.Skipped)
					}
					// For EOS/idempotent replay demos (Bundle 2), optionally run the same
					// replay function multiple additional times against the already
					// restored state store. With an idempotent backend (Pebble+LastSeq),
					// subsequent passes should have applied=0 and skipped>0.
					if cfg.RestoreOnly && cfg.ReplayExtraPasses > 1 && replayFn != nil && replayedOnce {
						for pass := 2; pass <= cfg.ReplayExtraPasses; pass++ {
							extra := replayFn()
							if extra.Error != nil {
								log.Printf("bundle2: replay pass=%d error=%v", pass, extra.Error)
								break
							}
							log.Printf("bundle2: replay pass=%d applied=%d skipped=%d", pass, extra.Applied, extra.Skipped)
						}
					}

					elapsed := time.Since(t0)
					appStatus.SetRecovered(elapsed, int64(result.Applied), int64(result.Skipped))
					// After restore, seed per-store gauges for Prometheus
					seedStoreGauges()
					restoreTsDone := time.Now()
					log.Printf("restore completed: applied=%d skipped=%d elapsedMs=%.0f finishedAt=%s", result.Applied, result.Skipped, elapsed.Seconds()*1000, restoreTsDone.Format(time.RFC3339Nano))
					log.Printf("restore ts: start=%s done=%s", restoreTsStart.Format(time.RFC3339Nano), restoreTsDone.Format(time.RFC3339Nano))
					metricsStart := time.Now()
					// Finalize phase timings before persisting them to restore-metrics.json.
					// NOTE: phaseTimings is a value type; if we copy it into restoreMetrics before filling TotalMs/MetricsMs,
					// the JSON file will show an empty "phases" object (all fields omitted).
					phaseTimings.TotalMs = time.Since(restoreTsStart).Milliseconds()
					phaseTimings.MetricsMs = time.Since(metricsStart).Milliseconds()
					newMetrics := restoreMetrics{
						SnapshotID:          m.SnapshotID,
						LastChangelogOffset: m.LastChangelogOffset,
						Applied:             int64(result.Applied),
						Skipped:             int64(result.Skipped),
						TTRMs:               time.Since(t0).Milliseconds(),
						CausalReplayEvents:  causalReplayEvents,
						InflightEvents:      inflightEventCount,
						InflightChannels:    inflightChannelCount,
						UpdatedAt:           time.Now().UTC(),
						Phases:              phaseTimings,
					}

					shouldWrite := true
					if prevMetrics, err := readRestoreMetrics(metricsPath); err == nil {
						if prevMetrics.SnapshotID == newMetrics.SnapshotID &&
							prevMetrics.Applied == newMetrics.Applied &&
							prevMetrics.Skipped == newMetrics.Skipped &&
							prevMetrics.CausalReplayEvents == newMetrics.CausalReplayEvents {
							shouldWrite = false
						}
					}
					if shouldWrite {
						if err := writeRestoreMetrics(metricsPath, newMetrics); err != nil {
							log.Printf("restore history: write error: %v", err)
						}
					}
					// Expose "Last Restore Summary" metrics to Prometheus for viz panels.
					// Labels: instance, snapshot_id, snapshot_type, format.
					formatLabel := string(restoreFormat)
					if formatLabel == "" {
						formatLabel = m.SnapshotFormat
					}
					lbls := []string{cfg.InstanceID, m.SnapshotID, m.SnapshotType, formatLabel}
					if mreg.LastRestoreTTRSeconds != nil {
						mreg.LastRestoreTTRSeconds.WithLabelValues(lbls...).Set(float64(newMetrics.TTRMs) / 1000.0)
					}
					// Lưu các metric phụ thuộc vào phaseTimings sau khi đã tính MetricsMs/TotalMs ở bên dưới.
					if mreg.LastRestoreReplaySeconds != nil {
						mreg.LastRestoreReplaySeconds.WithLabelValues(lbls...).Set(float64(phaseTimings.ChangelogMs) / 1000.0)
					}
					if mreg.LastRestoreReplayEvents != nil {
						mreg.LastRestoreReplayEvents.WithLabelValues(lbls...).Set(float64(result.Applied + result.Skipped))
					}
					if mreg.LastRestoreSnapshotBytes != nil {
						var snapBytes float64
						// Compute size using on-disk snapshot files; fall back to listed SSTs/incremental files.
						if strings.ToLower(m.SnapshotType) == manifest.SnapshotTypeDelta {
							snapBytes = deltaSnapshotSizeBytes(m.SnapshotID, restoreFormat, restoreShards)
							if snapBytes == 0 && len(m.PebbleIncrementalFiles) > 0 {
								snapBytes = snapshotIncrementalBytes(m.SnapshotID, m.PebbleIncrementalFiles)
							}
						} else {
							snapBytes = snapshotSizeBytes(m.SnapshotID, restoreFormat, restoreShards)
							if snapBytes == 0 && len(m.PebbleSSTFiles) > 0 {
								snapBytes = snapshotIncrementalBytes(m.SnapshotID, m.PebbleSSTFiles)
							}
						}
						mreg.LastRestoreSnapshotBytes.WithLabelValues(lbls...).Set(snapBytes)
					}
					if mreg.LastRestoreSSTFilesTotal != nil {
						mreg.LastRestoreSSTFilesTotal.WithLabelValues(lbls...).Set(float64(len(m.PebbleAllFiles)))
					}
					if mreg.LastRestoreIncrementalFiles != nil {
						mreg.LastRestoreIncrementalFiles.WithLabelValues(lbls...).Set(float64(len(m.PebbleIncrementalFiles)))
					}
					if mreg.LastRestoreInflightReplayed != nil {
						mreg.LastRestoreInflightReplayed.WithLabelValues(lbls...).Set(float64(newMetrics.InflightEvents))
					}
					if mreg.LastRestoreEOSOK != nil {
						// Treat a successful restore (no replay error) as EOS OK=1.
						mreg.LastRestoreEOSOK.WithLabelValues(lbls...).Set(1)
					}
					// Use the finalized timings already embedded in newMetrics.
					if mreg.LastRestoreRestoreOnlyMs != nil {
						mreg.LastRestoreRestoreOnlyMs.WithLabelValues(lbls...).Set(float64(newMetrics.Phases.TotalMs))
					}
					if newMetrics.Phases.TotalMs > 0 {
						line := map[string]any{
							"phase":   "restore-phases",
							"timings": newMetrics.Phases,
						}
						if b, err := json.Marshal(line); err == nil {
							log.Printf("restore phases: %s", b)
						}
					}
					if cfg.RestoreOnly {
						log.Printf("restore-only: exiting after successful restore")
						return nil
					}
				}
			}
		}
	} else {
		log.Printf("restore: skipped at start (restore-on-start=false)")
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
								cmd := ingestCommand{pause: pause, done: make(chan error, 1)}
								select {
								case ingestCtrl <- cmd:
									// ok
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
							// derive self url
							mkSelf := func() string {
								addr := strings.TrimSpace(cfg.HTTPAddr)
								if strings.HasPrefix(addr, ":") {
									return "http://127.0.0.1" + addr
								}
								if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
									return addr
								}
								return "http://" + addr
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
