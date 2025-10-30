package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"hpb/internal/changelog"
	"hpb/internal/manifest"
	"hpb/internal/metrics"
	"hpb/internal/opb"
	"hpb/internal/restore"
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
	TopicChangelog  string
	TopicSnapshots  string
	ManifestSource  string // file|kafka
	// Kafka input for orders.enriched
	InputSource   string // sample|kafka
	TopicEnriched string
	// Output EOS (orders.output)
	OutputTopic string
	OutputTxID  string
	// HTTP
	HTTPAddr string
	Once     bool // process exactly one message then exit (for EOS tests)
	EOSTest  bool // test mode: simulate crash cases without process exit
	// EOS batching
	TxBatchSize int
	TxLingerMs  int
}

// metricsAdapter implements the opb.TxMetrics interface using a metrics.Registry.
type metricsAdapter struct{ *metrics.Registry }

func (a metricsAdapter) TxAborted()             { a.Registry.TxAborted.Inc() }
func (a metricsAdapter) TxProduced()            { a.Registry.TxProduced.Inc() }
func (a metricsAdapter) TxLatencySec(v float64) { a.Registry.TxLatencySec.Observe(v) }

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
	flag.StringVar(&cfg.TopicChangelog, "topic-changelog", "p1.opb-changelog", "kafka topic for changelog (compacted)")
	flag.StringVar(&cfg.TopicSnapshots, "topic-snapshots", "p1.opb-snapshots", "kafka topic for manifest (compacted)")
	flag.StringVar(&cfg.ManifestSource, "manifest-source", "file", "manifest source for restore: file|kafka")
	flag.StringVar(&cfg.InputSource, "input-source", "sample", "orders.enriched source: sample|kafka")
	flag.StringVar(&cfg.TopicEnriched, "topic-enriched", "p1.orders.enriched", "kafka topic for orders.enriched input")
	flag.StringVar(&cfg.OutputTopic, "output-topic", "p1.orders.output", "kafka topic for orders.output")
	flag.StringVar(&cfg.OutputTxID, "output-tx-id", "", "transactional id for orders.output (enable EOS when set)")
	flag.StringVar(&cfg.HTTPAddr, "http", ":8080", "http listen address for metrics/health")
	flag.BoolVar(&cfg.Once, "once", false, "process exactly one message then exit (testing)")
	flag.BoolVar(&cfg.EOSTest, "eos-test-mode", false, "simulate crash cases without process exit (testing)")
	flag.IntVar(&cfg.TxBatchSize, "tx-batch-size", 1000, "transactional batch size (messages per commit)")
	flag.IntVar(&cfg.TxLingerMs, "tx-linger-ms", 100, "transactional linger in ms before forcing a commit")
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

	// Init snapshotter and manifest (filesystem by default)
	snap := snapshot.NewFilesystemSnapshotter(cfg.SnapshotDir)
	maniFS := manifest.NewFilesystemManifest(cfg.SnapshotDir)
	var mani manifest.Publisher = maniFS
	var maniReader restore.Reader = restore.NewFilesystemReader(cfg.SnapshotDir)
	if cfg.ManifestSink == "kafka" || cfg.ManifestSink == "both" {
		if cfg.KafkaBootstrap != "" {
			maniK := manifest.NewKafkaManifest(cfg.KafkaBootstrap, cfg.TopicSnapshots, "opb-manifest-latest")
			if cfg.ManifestSink == "kafka" {
				mani = maniK
			} else {
				// both: wrap to publish to both Kafka and filesystem
				mani = manifest.MultiPublisher(maniFS, maniK)
			}
			if cfg.ManifestSource == "kafka" && cfg.KafkaBootstrap != "" {
				maniReader = restore.NewKafkaReader([]string{cfg.KafkaBootstrap}, cfg.TopicSnapshots, "opb-manifest-latest")
			}
		}
	}

	// Init changelog writer (file by default; kafka optional)
	var clog changelog.Writer
	// Track how many changelog records have been appended so far (for manifest offset)
	var changelogAppendedCount int64
	if cfg.ChangelogSink == "file" || cfg.ChangelogSink == "both" || cfg.ChangelogSink == "" {
		fw, err := changelog.NewFileWriter("./changelog", "opb.jsonl")
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
	var isReady atomic.Bool
	go func(addr string) {
		mux := http.NewServeMux()
		mux.Handle("/metrics", mreg.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			if !isReady.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "starting"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		})
		_ = http.ListenAndServe(addr, mux)
	}(cfg.HTTPAddr)

	if cfg.InputSource == "kafka" && cfg.KafkaBootstrap != "" {
		// Start periodic snapshot + manifest publisher in background (Kafka mode)
		if cfg.SnapshotInterval > 0 {
			go func() {
				ticker := time.NewTicker(time.Duration(cfg.SnapshotInterval) * time.Second)
				defer ticker.Stop()
				for range ticker.C {
					id := time.Now().UTC().Format(time.RFC3339)
					if err := snap.WriteSnapshot(id, st); err != nil {
						log.Printf("snapshot error: %v", err)
						continue
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
			"auto.offset.reset":             "latest",
			"partition.assignment.strategy": "cooperative-sticky",
			"client.id":                     cfg.InstanceID,
			"group.instance.id":             cfg.InstanceID,
			"session.timeout.ms":            10000,
			"max.poll.interval.ms":          300000,
			"debug":                         "cgrp,consumer,protocol",
			// High throughput tuning
			"fetch.min.bytes":           2097152,  // 2 MB (tăng từ 1MB)
			"fetch.wait.max.ms":         10,       // Giảm từ 25ms
			"max.partition.fetch.bytes": 8388608,  // 8 MB (tăng từ 2MB)
			"queued.min.messages":       500000,   // Tăng từ 100K
			"fetch.max.bytes":           52428800, // 50 MB total fetch
			"heartbeat.interval.ms":     3000,     // Tăng heartbeat frequency
		})
		if err != nil {
			return fmt.Errorf("consumer: %w", err)
		}
		defer c.Close()
		if err := c.SubscribeTopics([]string{cfg.TopicEnriched}, nil); err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}

		// Periodically export per-partition lag metrics
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
					lag := high - int64(tp.Offset)
					if lag < 0 {
						lag = 0
					}
					mreg.PartitionLag.WithLabelValues(*tp.Topic, fmt.Sprintf("%d", tp.Partition), cfg.GroupID, cfg.InstanceID).Set(float64(lag))
				}
			}
		}()
		// If EOS output is enabled, create transactional producer
		var p opb.TxProducer
		if cfg.OutputTxID != "" {
			prod, err := ck.NewProducer(&ck.ConfigMap{
				"bootstrap.servers":  cfg.KafkaBootstrap,
				"enable.idempotence": true,
				"acks":               "all",
				"transactional.id":   cfg.OutputTxID,
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
		}
		// Mark ready once consumer has partition assignment (and producer, if any, is initialized)
		go func() {
			for {
				ass, err := c.Assignment()
				if err == nil && len(ass) > 0 {
					isReady.Store(true)
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
			log.Printf("%s consume partition=%d offset=%d topic=%s", cfg.InstanceID, msg.TopicPartition.Partition, msg.TopicPartition.Offset, *msg.TopicPartition.Topic)
			var ev opb.OrderEnriched
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				log.Printf("unmarshal error: %v", err)
				continue
			}
			// extract t0 header if present
			var hdrT0 []byte
			if len(msg.Headers) > 0 {
				for _, h := range msg.Headers {
					if h.Key == "t0" {
						hdrT0 = h.Value
						break
					}
				}
			}
			// Pre-compute key for diagnostics
			ws := opb.WindowStart(ev.NormTS, cfg.WindowSizeSec)
			k := opb.OutputKey(ev.StoreID, ev.ProductID, ws)
			prevSt, _ := st.Get(k)
			applied, out, seq, err := opb.AggregateAndBuildOutput(st, cfg.WindowSizeSec, ev)
			if err != nil {
				return fmt.Errorf("aggregate: %w", err)
			}
			if applied {
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
					}
					// set t1 header và propagate t0 nếu có, dùng BuildHeaders interface
					headers := opb.BuildHeaders(opb.RealClock{}, hdrT0)
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
				if cfg.ChangelogOn {
					d := changelog.Delta{Key: out.Key, Seq: seq, Delta: ev.Price * ev.Qty, DeltaQty: ev.Qty, TS: out.UpdatedAt}
					// If we have a transactional producer, write changelog to Kafka in the same transaction for immediate visibility
					if p != nil && (cfg.ChangelogSink == "kafka" || cfg.ChangelogSink == "both") {
						bchg, _ := json.Marshal(d)
						headers := opb.BuildHeaders(opb.RealClock{}, hdrT0)
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
					log.Printf("changelog: appended key=%s seq=%d", out.Key, seq)
					mreg.ChangelogAppended.Inc()
					changelogAppendedCount++
				}
			} else {
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

	// Ticker to simulate periodic snapshot publishing
	if cfg.SnapshotInterval > 0 {
		ticker := time.NewTicker(time.Duration(cfg.SnapshotInterval) * time.Second)
		defer ticker.Stop()
		for i := 0; i < 1; i++ { // single tick in Phase 1 for scaffolding
			<-ticker.C
			id := time.Now().UTC().Format(time.RFC3339)
			if err := snap.WriteSnapshot(id, st); err != nil {
				return fmt.Errorf("write snapshot: %w", err)
			}
			// Use number of appended changelog records as the file-based replay offset
			if err := mani.PublishLatest(id, changelogAppendedCount); err != nil {
				return fmt.Errorf("publish manifest: %w", err)
			}
			log.Printf("snapshot and manifest published: %s", id)
		}
	}

	// Test restore and replay
	log.Printf("testing restore and replay...")
	restorer := restore.NewRestorer(st, snap, maniReader, cfg.SnapshotDir)
	var result restore.RestoreResult
	var err error
	if cfg.ChangelogSource == "kafka" && cfg.KafkaBootstrap != "" {
		// Read manifest (already via maniReader), then replay from Kafka topic
		m, e := maniReader.ReadLatest()
		if e != nil {
			err = e
		} else {
			result = restorer.ReplayChangelogKafka([]string{cfg.KafkaBootstrap}, cfg.TopicChangelog, m.LastChangelogOffset)
			if result.Error != nil {
				err = result.Error
			}
		}
	} else {
		result, err = restorer.RestoreAndReplay()
	}
	if err != nil {
		log.Printf("restore failed: %v", err)
	} else {
		log.Printf("restore completed: applied=%d skipped=%d", result.Applied, result.Skipped)
	}

	log.Printf("OpB scaffold completed. Exiting.")
	return nil
}
