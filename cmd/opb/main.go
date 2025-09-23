package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"hpb/internal/changelog"
	"hpb/internal/manifest"
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
	BadgerDir        string
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
	flag.StringVar(&cfg.BadgerDir, "badger-dir", "./data/opb", "badger data directory")
	flag.StringVar(&cfg.KafkaBootstrap, "kafka-bootstrap", "", "kafka bootstrap servers, e.g. localhost:9092")
	flag.StringVar(&cfg.ChangelogSink, "changelog-sink", "file", "changelog sink: file|kafka|both")
	flag.StringVar(&cfg.ManifestSink, "manifest-sink", "file", "manifest sink: file|kafka|both")
	flag.StringVar(&cfg.ChangelogSource, "changelog-source", "file", "changelog source for restore: file|kafka")
	flag.StringVar(&cfg.TopicChangelog, "topic-changelog", "p2.opb-changelog", "kafka topic for changelog (compacted)")
	flag.StringVar(&cfg.TopicSnapshots, "topic-snapshots", "p2.opb-snapshots", "kafka topic for manifest (compacted)")
	flag.StringVar(&cfg.ManifestSource, "manifest-source", "file", "manifest source for restore: file|kafka")
	flag.StringVar(&cfg.InputSource, "input-source", "sample", "orders.enriched source: sample|kafka")
	flag.StringVar(&cfg.TopicEnriched, "topic-enriched", "p1.orders.enriched", "kafka topic for orders.enriched input")
	flag.Parse()
	return cfg
}

func run(cfg Config) error {
	log.Printf("starting OpB with prefix=%s window=%ds snapshot-interval=%ds changelog=%v", cfg.TopicPrefix, cfg.WindowSizeSec, cfg.SnapshotInterval, cfg.ChangelogOn)

	// Init state store (Phase 1: in-memory)
	st := state.NewInMemoryStore()

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

	// Dummy HTTP for health/metrics placeholder
	go func() {
		_ = http.ListenAndServe(":8080", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		}))
	}()

	if cfg.InputSource == "kafka" && cfg.KafkaBootstrap != "" {
		// Consume orders.enriched from Kafka
		c, err := ck.NewConsumer(&ck.ConfigMap{
			"bootstrap.servers":  cfg.KafkaBootstrap,
			"group.id":           cfg.GroupID,
			"enable.auto.commit": false,
			"isolation.level":    "read_committed",
			"auto.offset.reset":  "earliest",
		})
		if err != nil {
			return fmt.Errorf("consumer: %w", err)
		}
		defer c.Close()
		if err := c.SubscribeTopics([]string{cfg.TopicEnriched}, nil); err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}
		// Read a small batch for demo (Phase 1). Production: loop.
		for i := 0; i < 5; i++ {
			msg, err := c.ReadMessage(5 * time.Second)
			if err != nil {
				break
			}
			var ev opb.OrderEnriched
			if err := json.Unmarshal(msg.Value, &ev); err != nil {
				continue
			}
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
