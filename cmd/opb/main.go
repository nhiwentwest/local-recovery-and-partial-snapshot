package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
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
}

func main() {
	cfg := readFlags()
	if err := run(cfg); err != nil {
		log.Fatalf("opb failed: %v", err)
	}
}

func readFlags() Config {
	var cfg Config
	flag.StringVar(&cfg.TopicPrefix, "topic-prefix", "p2", "topic prefix")
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
	flag.StringVar(&cfg.TopicChangelog, "topic-changelog", "p2.opb-changelog", "kafka topic for changelog (compacted)")
	flag.StringVar(&cfg.TopicSnapshots, "topic-snapshots", "p2.opb-snapshots", "kafka topic for manifest (compacted)")
	flag.StringVar(&cfg.ManifestSource, "manifest-source", "file", "manifest source for restore: file|kafka")
	flag.StringVar(&cfg.InputSource, "input-source", "sample", "orders.enriched source: sample|kafka")
	flag.StringVar(&cfg.TopicEnriched, "topic-enriched", "p1.orders.enriched", "kafka topic for orders.enriched input")
	flag.StringVar(&cfg.OutputTopic, "output-topic", "p2.orders.output", "kafka topic for orders.output")
	flag.StringVar(&cfg.OutputTxID, "output-tx-id", "", "transactional id for orders.output (enable EOS when set)")
	flag.StringVar(&cfg.HTTPAddr, "http", ":8080", "http listen address for metrics/health")
	flag.BoolVar(&cfg.Once, "once", false, "process exactly one message then exit (testing)")
	flag.BoolVar(&cfg.EOSTest, "eos-test-mode", false, "simulate crash cases without process exit (testing)")
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
	case "badger": // deprecated: kept only for backward-compat if someone explicitly opts in
		bs, err := state.NewBadgerStore(cfg.StateDir)
		if err != nil {
			return fmt.Errorf("init badger: %w", err)
		}
		defer bs.Close()
		st = bs
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
	// HTTP for health/metrics
	go func() {
		http.Handle("/metrics", mreg.Handler())
		http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		})
		_ = http.ListenAndServe(cfg.HTTPAddr, nil)
	}()

	if cfg.InputSource == "kafka" && cfg.KafkaBootstrap != "" {
		// Consume orders.enriched from Kafka
        c, err := ck.NewConsumer(&ck.ConfigMap{
            "bootstrap.servers":           cfg.KafkaBootstrap,
            "group.id":                    cfg.GroupID,
            "enable.auto.commit":          false,
            "isolation.level":             "read_committed",
            "auto.offset.reset":           "earliest",
            // Throughput tuning
            "fetch.min.bytes":             1048576,      // 1 MB
            "fetch.wait.max.ms":           25,
            "max.partition.fetch.bytes":   2097152,      // 2 MB
            "queued.min.messages":         100000,
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
		var p *ck.Producer
		if cfg.OutputTxID != "" {
            prod, err := ck.NewProducer(&ck.ConfigMap{
                "bootstrap.servers":                    cfg.KafkaBootstrap,
                "enable.idempotence":                   true,
                "acks":                                  "all",
                "transactional.id":                      cfg.OutputTxID,
                // Batching/latency tuning
                "linger.ms":                             10,
                "batch.num.messages":                    100000,    // cap by batch.bytes too
                "batch.size":                            1048576,   // 1 MB
                "compression.type":                      "lz4",
                // Extra EOS safety (can raise to 2 if measured safe)
                "max.in.flight.requests.per.connection": 1,
            })
			if err != nil {
				return fmt.Errorf("producer: %w", err)
			}
			if err := prod.InitTransactions(context.TODO()); err != nil {
				return fmt.Errorf("init tx: %w", err)
			}
			p = prod
			defer p.Close()
		}
		// Continuous processing loop for scale-out demos.
		for {
			// Read first to avoid opening a transaction when there is no input.
			msg, err := c.ReadMessage(5 * time.Second)
			if err != nil {
				// no message available within timeout; continue to next iteration without txn
				continue
			}
			log.Printf("%s consume partition=%d offset=%d topic=%s", cfg.InstanceID, msg.TopicPartition.Partition, msg.TopicPartition.Offset, *msg.TopicPartition.Topic)
			var ev opb.OrderEnriched
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				// bad input; skip without txn
				continue
			}
			applied, out, seq, err := opb.AggregateAndBuildOutput(st, cfg.WindowSizeSec, ev)
			if err != nil {
				return fmt.Errorf("aggregate: %w", err)
			}
			if applied {
				b, _ := json.Marshal(out)
				log.Printf("orders.output key=%s seq=%d value=%s", out.Key, seq, string(b))
				if p != nil {
					// Begin a transaction only when we actually have something to produce
					if err := p.BeginTransaction(); err != nil {
						return fmt.Errorf("begin tx: %w", err)
					}
					log.Printf("tx: begin (instance=%s)", cfg.InstanceID)
					if cfg.CrashMode == "before" {
						if cfg.EOSTest {
							// Simulate crash-before by aborting and returning without producing
							_ = p.AbortTransaction(context.TODO())
							log.Printf("tx: test-mode before (aborted before produce)")
							if cfg.Once {
								log.Printf("once: exiting after test-before")
								return nil
							}
							continue
						}
						log.Fatalf("tx: crash before produce")
					}
					if err := p.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &cfg.OutputTopic, Partition: ck.PartitionAny}, Key: []byte(out.Key), Value: b}, nil); err != nil {
						_ = p.AbortTransaction(context.TODO())
						mreg.TxAborted.Inc()
						log.Printf("tx: produce error, aborted: %v", err)
						continue
					}
					log.Printf("tx: produced key=%s", out.Key)
				}
				if cfg.ChangelogOn {
					d := changelog.Delta{Key: out.Key, Seq: seq, Delta: ev.Price * ev.Qty, DeltaQty: ev.Qty, TS: out.UpdatedAt}
					if err := clog.Append(d); err != nil {
						if p != nil {
							_ = p.AbortTransaction(context.TODO())
							mreg.TxAborted.Inc()
						}
						return fmt.Errorf("append changelog: %w", err)
					}
					mreg.ChangelogAppended.Inc()
				}
			}
			if p != nil && applied {
				t0 := time.Now()
				// Build offsets for exactly the message we processed
				tp := ck.TopicPartition{Topic: msg.TopicPartition.Topic, Partition: msg.TopicPartition.Partition, Offset: msg.TopicPartition.Offset + 1}
				meta, _ := c.GetConsumerGroupMetadata()
				if err := p.SendOffsetsToTransaction(context.Background(), []ck.TopicPartition{tp}, meta); err != nil {
					_ = p.AbortTransaction(context.TODO())
					mreg.TxAborted.Inc()
					log.Printf("tx: send offsets error, aborted: %v", err)
					continue
				}
				log.Printf("tx: offsets sent topic=%s partition=%d offset=%d", *tp.Topic, tp.Partition, tp.Offset)
				if cfg.CrashMode == "mid" {
					if cfg.EOSTest {
						// Abort and redo in the same run
						_ = p.AbortTransaction(context.TODO())
						mreg.TxAborted.Inc()
						log.Printf("tx: test-mode mid (aborted after offsets), redoing now")
						// Redo transactional flow with freshly marshaled payload
						if err := p.BeginTransaction(); err != nil {
							return fmt.Errorf("begin tx (redo): %w", err)
						}
						rb, _ := json.Marshal(out)
						if err := p.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &cfg.OutputTopic, Partition: ck.PartitionAny}, Key: []byte(out.Key), Value: rb}, nil); err != nil {
							_ = p.AbortTransaction(context.TODO())
							mreg.TxAborted.Inc()
							log.Printf("tx: produce error (redo), aborted: %v", err)
							if cfg.Once {
								return nil
							}
							continue
						}
						if err := p.SendOffsetsToTransaction(context.Background(), []ck.TopicPartition{tp}, meta); err != nil {
							_ = p.AbortTransaction(context.TODO())
							mreg.TxAborted.Inc()
							log.Printf("tx: send offsets error (redo), aborted: %v", err)
							if cfg.Once {
								return nil
							}
							continue
						}
						if err := p.CommitTransaction(context.TODO()); err != nil {
							_ = p.AbortTransaction(context.TODO())
							mreg.TxAborted.Inc()
							log.Printf("tx: commit error (redo), aborted: %v", err)
							if cfg.Once {
								return nil
							}
							continue
						}
						log.Printf("tx: committed (redo)")
						mreg.TxProduced.Inc()
						mreg.TxLatencySec.Observe(time.Since(t0).Seconds())
						if cfg.Once {
							log.Printf("once: exiting after test-mid redo")
							return nil
						}
						continue
					}
					log.Fatalf("tx: crash mid (after offsets, before commit)")
				}
				if err := p.CommitTransaction(context.TODO()); err != nil {
					_ = p.AbortTransaction(context.TODO())
					mreg.TxAborted.Inc()
					log.Printf("tx: commit error, aborted: %v", err)
					continue
				}
				log.Printf("tx: committed")
				if cfg.CrashMode == "after" {
					if cfg.EOSTest {
						log.Printf("tx: test-mode after (committed)")
						if cfg.Once {
							log.Printf("once: exiting after test-after")
							return nil
						}
					} else {
						log.Fatalf("tx: crash after commit")
					}
				}
				mreg.TxProduced.Inc()
				mreg.TxLatencySec.Observe(time.Since(t0).Seconds())
			}
			if cfg.Once {
				// Exit after attempting to process one message (regardless of applied)
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
			if err := mani.PublishLatest(id, 0); err != nil { // lastChangelogOffset=0 placeholder
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
