package main

import (
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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"hpb/internal/changelog"
	"hpb/internal/manifest"
	"hpb/internal/metrics"
	"hpb/internal/opb"
	rf "hpb/internal/restorefs"
	rk "hpb/internal/restorekafka"
	"hpb/internal/snapshot"
	"hpb/internal/state"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Config holds CLI flags for OpB.
type Config struct {
	TopicPrefix      string
	GroupID          string
	WindowSizeSec    int
	SnapshotInterval int
	ChangelogOn      bool
	SnapshotDir      string
	StateDir         string
	StateBackend     string // memory|pebble
	CrashMode        string // ""|before|mid|after
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
	// Store-touch topic for cluster-wide store instance visibility (compacted)
	TopicStoreTouch string
	// Kafka input for orders.enriched
	InputSource   string // sample|kafka
	TopicEnriched string
	// Output EOS (orders.output)
	OutputTopic string
	TopicAudit  string
	// HTTP
	HTTPAddr string
	Once     bool // process exactly one message then exit (for EOS tests)
	EOSTest  bool // test mode: simulate crash cases without process exit
	// EOS batching
	TxBatchSize int
	TxLingerMs  int
	// Consumer group tuning
	SessionTimeoutMs    int
	HeartbeatIntervalMs int
	// Peers for cluster viz (comma-separated HTTP base URLs)
	PeersCSV string
	// Viz/cluster cache tuning
	VizPeerIntervalMs    int
	VizPeerTimeoutMs     int
	VizPeerTTLMs         int
	VizPeerDownBackoffMs int
	// Restore control
	RestoreOnStart bool // perform restore at process start (use true on restart)
	RestoreOnly    bool // perform restore then exit (no consume); useful for staged restart
}

type restoreMetrics struct {
	SnapshotID          string    `json:"snapshotId"`
	LastChangelogOffset int64     `json:"lastChangelogOffset"`
	Applied             int64     `json:"applied"`
	Skipped             int64     `json:"skipped"`
	TTRMs               int64     `json:"ttrMs"`
	UpdatedAt           time.Time `json:"updatedAt"`
}

// metricsAdapter implements the opb.TxMetrics interface using a metrics.Registry.
type metricsAdapter struct{ *metrics.Registry }

func (a metricsAdapter) TxAborted()                { a.Registry.TxAborted.Inc() }
func (a metricsAdapter) TxProduced()               { a.Registry.TxProduced.Inc() }
func (a metricsAdapter) TxLatencySec(v float64)    { a.Registry.TxLatencySec.Observe(v) }
func (a metricsAdapter) OffsetsBoundLag(v float64) { a.Registry.OffsetsBoundLag.Set(v) }

func main() {
	cfg := readFlags()
	if err := run(cfg); err != nil {
		log.Fatalf("opb failed: %v", err)
	}
}

func readFlags() Config {
	var cfg Config
	flag.StringVar(&cfg.TopicPrefix, "topic-prefix", "p1", "topic prefix")
	flag.StringVar(&cfg.GroupID, "group-id", "opb", "consumer group id")
	flag.IntVar(&cfg.WindowSizeSec, "window-size", 300, "aggregation window seconds")
	flag.IntVar(&cfg.SnapshotInterval, "snapshot-interval", 60, "snapshot interval seconds")
	flag.BoolVar(&cfg.ChangelogOn, "changelog", true, "enable changelog emission")
	flag.StringVar(&cfg.SnapshotDir, "snapshot-dir", "./snapshots", "snapshot directory")
	flag.StringVar(&cfg.StateDir, "state-dir", "./data/opb", "state data directory")
	flag.StringVar(&cfg.StateBackend, "state-backend", "pebble", "state backend: memory|pebble")
	flag.StringVar(&cfg.CrashMode, "crash", "", "simulate crash: before|mid|after")
	flag.StringVar(&cfg.InstanceID, "instance-id", "", "instance id for logging (replicas)")
	flag.StringVar(&cfg.KafkaBootstrap, "kafka-bootstrap", "", "kafka bootstrap servers, e.g. localhost:9092")
	flag.StringVar(&cfg.ChangelogSink, "changelog-sink", "file", "changelog sink: file|kafka|both")
	flag.StringVar(&cfg.ManifestSink, "manifest-sink", "file", "manifest sink: file|kafka|both")
	flag.StringVar(&cfg.ChangelogSource, "changelog-source", "file", "changelog source for restore: file|kafka")
	flag.StringVar(&cfg.ChangelogDir, "changelog-dir", "./changelog", "directory for file-based changelog")
	flag.StringVar(&cfg.TopicChangelog, "topic-changelog", "p1.opb-changelog", "kafka topic for changelog (compacted)")
	flag.StringVar(&cfg.TopicSnapshots, "topic-snapshots", "p1.opb-snapshots", "kafka topic for manifest (compacted)")
	flag.StringVar(&cfg.ManifestSource, "manifest-source", "file", "manifest source for restore: file|kafka")
	flag.StringVar(&cfg.TopicStoreTouch, "topic-store-touch", "p1.opb-store-touch", "compacted topic for store touch (storeId#instanceId)")
	flag.StringVar(&cfg.InputSource, "input-source", "sample", "orders.enriched source: sample|kafka")
	flag.StringVar(&cfg.TopicEnriched, "topic-enriched", "p1.orders.enriched", "kafka topic for orders.enriched input")
	flag.StringVar(&cfg.OutputTopic, "output-topic", "p1.orders.output", "kafka topic for orders.output")
	flag.StringVar(&cfg.TopicAudit, "topic-audit", "p1.opb-audit", "audit topic for tx BEGIN/COMMIT/ABORT")
	flag.StringVar(&cfg.HTTPAddr, "http", ":8080", "http listen address for metrics/health")
	flag.BoolVar(&cfg.Once, "once", false, "process exactly one message then exit (testing)")
	flag.BoolVar(&cfg.EOSTest, "eos-test-mode", false, "simulate crash cases without process exit (testing)")
	flag.IntVar(&cfg.TxBatchSize, "tx-batch-size", 1000, "transactional batch size (messages per commit)")
	flag.IntVar(&cfg.TxLingerMs, "tx-linger-ms", 100, "transactional linger in ms before forcing a commit")
	// Peers for cluster viz from flag or env OPB_PEERS
	flag.StringVar(&cfg.PeersCSV, "peers", os.Getenv("OPB_PEERS"), "peer HTTP base URLs, comma-separated (e.g. http://127.0.0.1:8089,http://127.0.0.1:8090)")
	flag.IntVar(&cfg.SessionTimeoutMs, "session-timeout-ms", 10000, "consumer session timeout")
	flag.IntVar(&cfg.HeartbeatIntervalMs, "heartbeat-interval-ms", 3000, "consumer heartbeat interval")
	// Restore control: perform restore at process start only when explicitly enabled (use true on restart)
	flag.BoolVar(&cfg.RestoreOnStart, "restore-on-start", false, "perform restore at process start (use true on restart)")
	flag.BoolVar(&cfg.RestoreOnly, "restore-only", false, "perform restore then exit (no consume); useful for staged restart")
	flag.IntVar(&cfg.VizPeerIntervalMs, "viz-peer-interval-ms", 500, "interval ms for peer polling")
	flag.IntVar(&cfg.VizPeerTimeoutMs, "viz-peer-timeout-ms", 250, "timeout ms for peer status fetch")
	flag.IntVar(&cfg.VizPeerTTLMs, "viz-peer-ttl-ms", 2000, "ttl ms before marking a peer down in cache")
	flag.IntVar(&cfg.VizPeerDownBackoffMs, "viz-peer-down-backoff-ms", 2000, "backoff ms when peer is down")
	flag.Parse()
	return cfg
}

func run(cfg Config) error {
	log.Printf("starting OpB with prefix=%s window=%ds snapshot-interval=%ds changelog=%v", cfg.TopicPrefix, cfg.WindowSizeSec, cfg.SnapshotInterval, cfg.ChangelogOn)

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

	// Init snapshotter and manifest (filesystem by default)
	snap := snapshot.NewFilesystemSnapshotter(cfg.SnapshotDir)
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

	// Prometheus metrics registry
	mreg := metrics.NewRegistry()
	// HTTP for health/metrics on dedicated mux to avoid handler conflicts
	appStatus := opb.NewStatusManager(cfg.InstanceID, cfg.GroupID, cfg.WindowSizeSec)
	metricsPath := filepath.Join(cfg.StateDir, "restore-metrics.json")
	if !cfg.RestoreOnStart {
		if rm, err := readRestoreMetrics(metricsPath); err == nil {
			appStatus.ApplyRestoreHistory(rm.TTRMs, rm.SnapshotID, rm.LastChangelogOffset, rm.Applied, rm.Skipped)
			log.Printf("restore history: loaded snapshotId=%s applied=%d skipped=%d", rm.SnapshotID, rm.Applied, rm.Skipped)
		} else if !errors.Is(err, os.ErrNotExist) {
			log.Printf("restore history: read error: %v", err)
		}
	}
	zoneIdx := opb.NewZoneIndex()
	storeTouchIdx := opb.NewStoreTouchIndex()
	// Shared injection producer and simple rate limiter
	var injP *ck.Producer
	var injErr error
	var storeTouchP *ck.Producer
	if cfg.KafkaBootstrap != "" {
		injP, injErr = ck.NewProducer(&ck.ConfigMap{
			"bootstrap.servers": cfg.KafkaBootstrap,
			"linger.ms":         5,
			"compression.type":  "lz4",
		})
		if injErr != nil {
			log.Printf("inject: producer init error: %v", injErr)
		}
		// producer for store-touch (non-transactional)
		p2, err := ck.NewProducer(&ck.ConfigMap{
			"bootstrap.servers": cfg.KafkaBootstrap,
			"linger.ms":         50,
			"compression.type":  "lz4",
		})
		if err == nil {
			storeTouchP = p2
		} else {
			log.Printf("store-touch: producer init error: %v", err)
		}
	}
	injLast := make(map[string]time.Time)
	lastTouch := make(map[string]time.Time) // key: storeId#instanceId

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
		vizInt := time.Duration(cfg.VizPeerIntervalMs) * time.Millisecond
		vizTmo := time.Duration(cfg.VizPeerTimeoutMs) * time.Millisecond
		vizTTL := time.Duration(cfg.VizPeerTTLMs) * time.Millisecond
		vizBackoff := time.Duration(cfg.VizPeerDownBackoffMs) * time.Millisecond
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
			var req struct {
				StoreID   string `json:"storeId"`
				ProductID string `json:"productId"`
				WS        int64  `json:"ws"`
				Mode      string `json:"mode"`
				N         int    `json:"n"`
				Start     int    `json:"start"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid json"})
				return
			}
			if req.StoreID == "" {
				req.StoreID = "A-"
			}
			if req.N <= 0 {
				req.N = 1000
			}
			if req.N > 5000 {
				req.N = 5000
			}
			if req.Mode == "" {
				req.Mode = "new"
			}
			// Produce asynchronously
			rr := req
			go func() {
				defer func() { recover() }()
				for i := 0; i < rr.N; i++ {
					store := rr.StoreID
					prod := rr.ProductID
					// Match NEW generation deterministically so DUP truly replays the same orders
					if prod == "" {
						prod = fmt.Sprintf("p%d", (i%100)+1)
					}
					// For duplicate mode, reuse the exact same ordId pattern used in NEW
					idx := i + rr.Start
					ordID := fmt.Sprintf("ord-%d-%d", rr.WS, idx)
					ts := time.Now().Unix()
					ws := opb.WindowStart(ts, cfg.WindowSizeSec)
					if rr.WS > 0 {
						ws = rr.WS
						ts = rr.WS
					}
					price := int64(10000)
					qty := int64(1)
					payload := map[string]any{
						"orderId":   ordID,
						"productId": prod,
						"price":     price,
						"qty":       qty,
						"storeId":   store,
						"ts":        ts,
						"validated": true,
						"normTs":    ws,
					}
					val, _ := json.Marshal(payload)
					key := []byte(fmt.Sprintf("%s#%s#%d", store, prod, ws))
					// Add epoch header so consumer AcceptMessageByEpoch will accept injected messages
					headers := []ck.Header{{Key: "epoch", Value: []byte(fmt.Sprintf("%d", time.Now().UnixNano()))}}
					_ = injP.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &cfg.TopicEnriched, Partition: ck.PartitionAny}, Key: key, Value: val, Headers: headers}, nil)
				}
				injP.Flush(15000)
			}()
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "queued", "n": req.N, "mode": req.Mode, "storeId": req.StoreID})
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
				"storeId":    storeID,
				"keys":       keys,
				"totalSumQty": totalSumQty,
				"count":      len(keys),
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
		mux.Handle("/viz/", noCache(http.StripPrefix("/viz/", fs)))

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
			fmt.Fprintf(w, "<div style='display:flex; gap:16px; flex-wrap:wrap'>")
			fmt.Fprintf(w, "<div style='flex:1 1 420px; border:1px solid #2b3152; padding:12px; border-radius:8px; background:#0c1229'>")
			fmt.Fprintf(w, "<div style='font-weight:600; margin-bottom:8px'>Recovery Summary</div>")
			fmt.Fprintf(w, "<div id='recovery-summary' class='small muted'>loading...</div>")
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
			fmt.Fprintf(w, "<script>\n(async function(){\n  async function loadStatus(){\n    try{\n      const res = await fetch('/status', {cache:'no-store'});\n      const j = await res.json();\n      const el = document.getElementById('recovery-summary');\n      if(!el) return;\n      const ttr = (j.ttrMs!==undefined? j.ttrMs+' ms':'N/A');\n      const snap = (j.restoringSnapshotId||'N/A');\n      const off = (j.lastChangelogOffset!==undefined? j.lastChangelogOffset:'N/A');\n      const ap = (j.lastRestoreApplied!==undefined? j.lastRestoreApplied:'N/A');\n      const sk = (j.lastRestoreSkipped!==undefined? j.lastRestoreSkipped:'N/A');\n      el.innerHTML = `<div>ttrMs: <b>${ttr}</b></div>`+\n                     `<div>snapshotId: <span class='muted'>${snap}</span></div>`+\n                     `<div>lastChangelogOffset: <span class='muted'>${off}</span></div>`+\n                     `<div>restore applied/skipped: <span class='muted'>${ap}</span>/<span class='muted'>${sk}</span></div>`;\n      // Default ws suggestion\n      const wsInput = document.getElementById('pf-ws');\n      if(wsInput && !wsInput.value && j.windowSizeSec){\n        const now = Math.floor(Date.now()/1000);\n        const ws = Math.floor(now / j.windowSizeSec) * j.windowSizeSec;\n        wsInput.value = String(ws);\n      }\n    }catch(e){\n      const el = document.getElementById('recovery-summary');\n      if(el) el.textContent = 'N/A';\n    }\n  }\n  function setupProbe(){\n    const btn = document.getElementById('pf-run');\n    if(!btn) return;\n    btn.addEventListener('click', async function(){\n      const s = document.getElementById('pf-store').value.trim();\n      const p = document.getElementById('pf-prod').value.trim();\n      const w = document.getElementById('pf-ws').value.trim();\n      if(!s || !p || !w){ return; }\n      const url = `/viz/zone-data?id=${encodeURIComponent(s)}&productId=${encodeURIComponent(p)}&ws=${encodeURIComponent(w)}`;\n      const pu = document.getElementById('probe-url');\n      if(pu) pu.textContent = url;\n      const fr = document.getElementById('probe-frame');\n      if(fr) fr.src = url;\n    });\n  }\n  await loadStatus();\n  setupProbe();\n})();\n</script>")
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
			fmt.Fprintf(w, "<html><body style='font-family:system-ui; margin:16px'>")
			fmt.Fprintf(w, "<h3>Zone Data</h3>")
			fmt.Fprintf(w, "<div>id=%s productId=%s ws=%s</div>", id, pid, wsStr)
			if id == "" {
				fmt.Fprintf(w, "<div style='color:#b00020'>missing id</div></body></html>")
				return
			}
			// Store mode snapshot via index (sums local) + cluster-wide instances from StoreTouchIndex
			sumA, sumQ, _ := zoneIdx.Snapshot(id)
			insts := storeTouchIdx.Instances(id)
			fmt.Fprintf(w, "<h4>Store mode (aggregates)</h4>")
			fmt.Fprintf(w, "<pre>{\n  \"storeId\": \"%s\",\n  \"sumAmount\": %d,\n  \"sumQty\": %d,\n  \"instances\": %q\n}</pre>", id, sumA, sumQ, insts)
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
			// Recovery Info (fetched from /status)
			fmt.Fprintf(w, "<div id='recovery-info' style='margin:12px 0; padding:10px; border:1px solid #2b3152; border-radius:6px; background:#0c1229; color:#e6e9ef'>")
			fmt.Fprintf(w, "<div style='font-weight:600; margin-bottom:6px'>Recovery Info</div>")
			fmt.Fprintf(w, "<div class='small' id='rec-body'>loading...</div>")
			fmt.Fprintf(w, "</div>")
			fmt.Fprintf(w, "<script>(async function(){try{const r=await fetch('/status',{cache:'no-store'});const j=await r.json();const el=document.getElementById('rec-body');if(!el)return;const ttr=(j.ttrMs!==undefined? j.ttrMs+' ms':'N/A');const snap=(j.restoringSnapshotId||'N/A');const off=(j.lastChangelogOffset!==undefined? j.lastChangelogOffset:'N/A');const ap=(j.lastRestoreApplied!==undefined? j.lastRestoreApplied:'N/A');const sk=(j.lastRestoreSkipped!==undefined? j.lastRestoreSkipped:'N/A');el.innerHTML=`<div>ttrMs: <b>${ttr}</b></div><div>snapshotId: <span class='muted'>${snap}</span></div><div>lastChangelogOffset: <span class='muted'>${off}</span></div><div>restore applied/skipped: <span class='muted'>${ap}</span>/<span class='muted'>${sk}</span></div>`;}catch(e){const el=document.getElementById('rec-body');if(el)el.textContent='N/A';}})();</script>")
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

	// Perform recovery (restore snapshot + replay changelog) before starting Kafka consume loop
	// Only when explicitly enabled via --restore-on-start to avoid delaying first start
	if cfg.RestoreOnStart {
		restoreTsStart := time.Now()
		log.Printf("restore: starting (source=%s, changelogSource=%s, topicSnapshots=%s) at %s", cfg.ManifestSource, cfg.ChangelogSource, cfg.TopicSnapshots, restoreTsStart.Format(time.RFC3339Nano))
		// Read latest manifest with internal reader timeout (no long outer loop)
		var m manifest.Manifest
		m, mErr := maniReader.ReadLatest()
		if mErr != nil || m.SnapshotID == "" {
			// Fallback: try filesystem manifest reader if kafka source fails and FS snapshot exists
			if cfg.SnapshotDir != "" {
				if m2, e2 := rf.NewFilesystemReader(cfg.SnapshotDir).ReadLatest(); e2 == nil && m2.SnapshotID != "" {
					log.Printf("restore: fallback FS manifest loaded snapshotId=%s lastChangelogOffset=%d", m2.SnapshotID, m2.LastChangelogOffset)
					m, mErr = m2, nil
				}
			}
		}
		if mErr != nil || m.SnapshotID == "" {
			log.Printf("restore: no manifest found after wait; skipping restore (err=%v, snapshotId=%s)", mErr, m.SnapshotID)
		} else {
			log.Printf("restore: manifest loaded snapshotId=%s lastChangelogOffset=%d", m.SnapshotID, m.LastChangelogOffset)
			appStatus.SetRecovering(m.SnapshotID, m.LastChangelogOffset)
			t0 := time.Now()
			restorer := rf.NewRestorer(st, snap, maniReader, cfg.SnapshotDir)
			// Always restore snapshot before replaying changelog
			if e := restorer.RestoreFromSnapshot(m.SnapshotID); e != nil {
				log.Printf("restore snapshot error: %v", e)
			} else {
				log.Printf("restore: snapshot restored snapshotId=%s", m.SnapshotID)
				var result rf.RestoreResult
				if cfg.ChangelogSource == "kafka" && cfg.KafkaBootstrap != "" {
					result = rk.ReplayChangelogKafka(st, []string{cfg.KafkaBootstrap}, cfg.TopicChangelog, m.LastChangelogOffset)
				} else {
					// file mode
					result = restorer.ReplayChangelog(fmt.Sprintf("%s/opb.jsonl", cfg.ChangelogDir), m.LastChangelogOffset)
				}
				if result.Error != nil {
					log.Printf("restore replay error: %v", result.Error)
				} else {
					elapsed := time.Since(t0)
					appStatus.SetRecovered(elapsed, int64(result.Applied), int64(result.Skipped))
					restoreTsDone := time.Now()
					log.Printf("restore completed: applied=%d skipped=%d elapsedMs=%.0f finishedAt=%s", result.Applied, result.Skipped, elapsed.Seconds()*1000, restoreTsDone.Format(time.RFC3339Nano))
					log.Printf("restore ts: start=%s done=%s", restoreTsStart.Format(time.RFC3339Nano), restoreTsDone.Format(time.RFC3339Nano))
					if err := writeRestoreMetrics(metricsPath, restoreMetrics{
						SnapshotID:          m.SnapshotID,
						LastChangelogOffset: m.LastChangelogOffset,
						Applied:             int64(result.Applied),
						Skipped:             int64(result.Skipped),
						TTRMs:               time.Since(t0).Milliseconds(),
						UpdatedAt:           time.Now().UTC(),
					}); err != nil {
						log.Printf("restore history: write error: %v", err)
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

	// Background consumer for store-touch compacted topic to build cluster-wide instances index
	if cfg.KafkaBootstrap != "" && cfg.TopicStoreTouch != "" {
		go func() {
			cst, err := ck.NewConsumer(&ck.ConfigMap{
				"bootstrap.servers":  cfg.KafkaBootstrap,
				"group.id":           fmt.Sprintf("opb-storeviz-%s", cfg.InstanceID),
				"enable.auto.commit": true,
				"auto.offset.reset":  "earliest",
				"isolation.level":    "read_committed",
				"session.timeout.ms": 10000,
			})
			if err != nil {
				log.Printf("store-touch: consumer error: %v", err)
				return
			}
			defer cst.Close()
			if err := cst.SubscribeTopics([]string{cfg.TopicStoreTouch}, nil); err != nil {
				log.Printf("store-touch: subscribe error: %v", err)
				return
			}
			for {
				msg, err := cst.ReadMessage(2 * time.Second)
				if err != nil { // timeout
					continue
				}
				if msg == nil || len(msg.Key) == 0 {
					continue
				}
				// key format: storeId#instanceId
				parts := strings.SplitN(string(msg.Key), "#", 2)
				if len(parts) != 2 {
					continue
				}
				storeID, instID := parts[0], parts[1]
				storeTouchIdx.Add(storeID, instID)
			}
		}()
	}

	if cfg.InputSource == "kafka" && cfg.KafkaBootstrap != "" {
		// Start periodic snapshot + manifest publisher in background (Kafka mode)
		if cfg.SnapshotInterval > 0 {
			go func() {
				ticker := time.NewTicker(time.Duration(cfg.SnapshotInterval) * time.Second)
				defer ticker.Stop()
				for range ticker.C {
					id := time.Now().UTC().Format(time.RFC3339)
					t0 := time.Now()
					if err := snap.WriteSnapshot(id, st); err != nil {
						log.Printf("snapshot error: %v", err)
						continue
					}
					durMs := float64(time.Since(t0).Milliseconds())
					mreg.SnapshotTimeMs.Observe(durMs)
					// read snapshot bytes
					fp := fmt.Sprintf("%s/%s/state.json", cfg.SnapshotDir, id)
					if fi, err := os.Stat(fp); err == nil {
						mreg.SnapshotBytes.Set(float64(fi.Size()))
					}
					if err := mani.PublishLatest(id, changelogAppendedCount); err != nil {
						log.Printf("manifest publish error: %v", err)
						continue
					}
					log.Printf("snapshot and manifest published: %s (offset=%d)", id, changelogAppendedCount)
				}
			}()
		}
		// Consume orders.enriched from Kafka
		c, err := ck.NewConsumer(&ck.ConfigMap{
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
			"debug":                 "cgrp,consumer,protocol",
			// High throughput tuning
			"fetch.min.bytes":           1,
			"fetch.wait.max.ms":         10,
			"max.partition.fetch.bytes": 8388608,
			"queued.min.messages":       500000,
			"fetch.max.bytes":           52428800,
		})
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
		var auditP *ck.Producer
		// fencing epoch token per-process
		epoch := []byte(fmt.Sprintf("%d", time.Now().UnixNano()))
		// derive transactional.id: stable across restarts if instance id is provided; else fallback to timestamp
		txID := cfg.InstanceID
		if txID == "" {
			txID = fmt.Sprintf("opb-%s-%d", cfg.GroupID, time.Now().UnixNano())
		} else {
			txID = fmt.Sprintf("opb-%s-%s", cfg.GroupID, cfg.InstanceID)
		}
		prod, err := ck.NewProducer(&ck.ConfigMap{
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
			"debug":                  "eos,broker,protocol",
		})
		if err != nil {
			return fmt.Errorf("producer: %w", err)
		}
		if err := prod.InitTransactions(context.TODO()); err != nil {
			return fmt.Errorf("init tx: %w", err)
		}
		p = prod // as interface
		defer p.Close()
		// audit producer (non-transactional)
		ap, err := ck.NewProducer(&ck.ConfigMap{"bootstrap.servers": cfg.KafkaBootstrap})
		if err == nil {
			auditP = ap
			defer auditP.Close()
		}
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
			batchStarted      bool
			batchStartTime    time.Time
			batchCount        int
			batchOffsets      = make(map[int32]ck.TopicPartition) // partition -> highest offset+1
			opbCrashTriggered bool
		)
		// Simple in-process idempotency for verify: eventId = orderId#windowStart
		dedupSeen := make(map[string]struct{})
		var latestEpochSeen int64
		for {
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
			// Pre-compute key for diagnostics
			ws := opb.WindowStart(ev.NormTS, cfg.WindowSizeSec)
			k := opb.OutputKey(ev.StoreID, ev.ProductID, ws)
			if strings.HasPrefix(ev.StoreID, "EOS-TEST-") {
				part := msg.TopicPartition.Partition
				off := msg.TopicPartition.Offset
				log.Printf("diag: incoming store=%s product=%s qty=%d ws=%d key=%s part=%d off=%d", ev.StoreID, ev.ProductID, ev.Qty, ws, k, part, off)
			}
			// In-process idempotency by eventId (orderId#ws) to avoid double-apply in low-load tests
			eventID := fmt.Sprintf("%s#%d", ev.OrderID, ws)
			if _, ok := dedupSeen[eventID]; ok {
				log.Printf("diag: dedup skip eventID=%s key=%s", eventID, k)
				mreg.EventsSkippedDedup.Inc()
				appStatus.IncEventsSkippedDedup(1)
				continue
			}
			dedupSeen[eventID] = struct{}{}

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
				// Emit store-touch (throttled) for cluster-wide instance visibility
				if storeTouchP != nil && cfg.TopicStoreTouch != "" {
					k := ev.StoreID + "#" + cfg.InstanceID
					now := time.Now()
					if last, ok := lastTouch[k]; !ok || now.Sub(last) > 10*time.Second {
						lastTouch[k] = now
						val := []byte(fmt.Sprintf("{\"ts\":%d}", now.Unix()))
						topic := cfg.TopicStoreTouch
						_ = storeTouchP.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &topic, Partition: ck.PartitionAny}, Key: []byte(k), Value: val}, nil)
					}
				}
				log.Printf("aggregate: applied key=%s seq=%d prevLast=%d", out.Key, seq, prevSt.LastSeq)
				b, _ := json.Marshal(out)
				if p != nil {
					if !batchStarted {
						log.Printf("tx: begin transaction")
						if err := p.BeginTransaction(); err != nil {
							return fmt.Errorf("begin tx: %w", err)
						}
						batchStarted = true
						batchStartTime = time.Now()
						if auditP != nil {
							ev := map[string]any{"evt": "BEGIN", "txId": txID, "ts": time.Now().UnixNano()}
							b, _ := json.Marshal(ev)
							_ = auditP.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &cfg.TopicAudit, Partition: ck.PartitionAny}, Value: b}, nil)
						}
					}
					// set t1 header, propagate t0 nếu có, kèm fencing epoch
					headers := opb.BuildHeadersWithEpoch(opb.RealClock{}, hdrT0, epoch)
					if err := p.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &cfg.OutputTopic, Partition: ck.PartitionAny}, Key: []byte(out.Key), Value: b, Headers: headers}, nil); err != nil {
						_ = p.AbortTransaction(context.TODO())
						mreg.TxAborted.Inc()
						batchStarted = false
						batchCount = 0
						batchOffsets = make(map[int32]ck.TopicPartition)
						log.Printf("tx: produce error, aborted: %v", err)
						if auditP != nil {
							ev := map[string]any{"evt": "ABORT", "txId": txID, "ts": time.Now().UnixNano(), "reason": "produce_error"}
							ab, _ := json.Marshal(ev)
							_ = auditP.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &cfg.TopicAudit, Partition: ck.PartitionAny}, Value: ab}, nil)
						}
						continue
					}
					batchCount++
					// Track highest offset+1 per partition
					tp := ck.TopicPartition{Topic: msg.TopicPartition.Topic, Partition: msg.TopicPartition.Partition, Offset: msg.TopicPartition.Offset + 1}
					if existing, ok := batchOffsets[tp.Partition]; !ok || tp.Offset > existing.Offset {
						batchOffsets[tp.Partition] = tp
					}
				}
				if cfg.ChangelogOn {
					d := changelog.Delta{Key: out.Key, Seq: seq, Delta: ev.Price * ev.Qty, DeltaQty: ev.Qty, TS: out.UpdatedAt}
					// If we have a transactional producer, write changelog to Kafka in the same transaction for immediate visibility
					if p != nil && (cfg.ChangelogSink == "kafka" || cfg.ChangelogSink == "both") {
						bchg, _ := json.Marshal(d)
						headers := opb.BuildHeadersWithEpoch(opb.RealClock{}, hdrT0, epoch)
						if err := p.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &cfg.TopicChangelog, Partition: ck.PartitionAny}, Key: []byte(d.Key), Value: bchg, Headers: headers}, nil); err != nil {
							_ = p.AbortTransaction(context.TODO())
							mreg.TxAborted.Inc()
							batchStarted = false
							batchCount = 0
							batchOffsets = make(map[int32]ck.TopicPartition)
							log.Printf("tx: produce changelog error, aborted: %v", err)
							if auditP != nil {
								ev := map[string]any{"evt": "ABORT", "txId": txID, "ts": time.Now().UnixNano(), "reason": "changelog_error"}
								ab, _ := json.Marshal(ev)
								_ = auditP.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &cfg.TopicAudit, Partition: ck.PartitionAny}, Value: ab}, nil)
							}
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
					log.Printf("changelog: appended key=%s seq=%d", out.Key, seq)
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
				// Crash injection points for OpB EOS (before/mid/after one-shot per process)
				if cfg.CrashMode == "before" && !opbCrashTriggered {
					log.Fatalf("opb crash: before commit (simulated)")
				}
				if err := opb.CommitBatch(c, p, batchOffsets, metricsAdapter{mreg}); err != nil {
					log.Printf("tx: batch commit error: %v", err)
				} else {
					if auditP != nil {
						// build offsets summary (count only to avoid heavy msg)
						ev := map[string]any{"evt": "COMMIT", "txId": txID, "ts": time.Now().UnixNano(), "parts": len(batchOffsets)}
						b, _ := json.Marshal(ev)
						_ = auditP.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &cfg.TopicAudit, Partition: ck.PartitionAny}, Value: b}, nil)
					}
				}
				if cfg.CrashMode == "mid" && !opbCrashTriggered {
					log.Fatalf("opb crash: mid commit (simulated)")
				}
				batchStarted = false
				batchCount = 0
				batchOffsets = make(map[int32]ck.TopicPartition)
				if cfg.CrashMode == "after" && !opbCrashTriggered {
					log.Fatalf("opb crash: after commit (simulated)")
				}
				if cfg.CrashMode != "" {
					opbCrashTriggered = true
				}
			}
			if cfg.Once {
				log.Printf("once: exiting after one message")
				return nil
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
				if cfg.ChangelogOn {
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
		restorer := rf.NewRestorer(st, snap, maniReader, cfg.SnapshotDir)
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
		if cfg.ChangelogSource == "kafka" && cfg.KafkaBootstrap != "" {
			if mErr != nil {
				err = mErr
			} else {
				// Always restore snapshot before replaying changelog
				if e := restorer.RestoreFromSnapshot(m.SnapshotID); e != nil {
					err = e
				} else {
					result = rk.ReplayChangelogKafka(st, []string{cfg.KafkaBootstrap}, cfg.TopicChangelog, m.LastChangelogOffset)
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
				if e := restorer.RestoreFromSnapshot(m.SnapshotID); e != nil {
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
