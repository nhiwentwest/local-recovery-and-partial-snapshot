package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"hpb/internal/manifest"
	"hpb/internal/metrics"
	rf "hpb/internal/restorefs"
	rk "hpb/internal/restorekafka"
	"hpb/internal/snapshot"
	"hpb/internal/state"

	"github.com/segmentio/kafka-go"
)

func main() {
	var (
		bootstrap       string
		groupID         string
		instanceID      string
		manifestSource  string
		changelogSource string
		topicSnapshots  string
		topicChangelog  string
		snapshotDir     string
		snapshotFormat  string
		snapshotShards  int
		httpAddr        string
		pollIntervalSec int
		advanceManifest bool
	)
	flag.StringVar(&bootstrap, "bootstrap", "127.0.0.1:9092", "kafka bootstrap")
	flag.StringVar(&groupID, "group-id", "recover", "metrics label: group id")
	flag.StringVar(&instanceID, "instance-id", "R", "metrics label: instance id")
	flag.StringVar(&manifestSource, "manifest-source", "kafka", "file|kafka")
	flag.StringVar(&changelogSource, "changelog-source", "kafka", "file|kafka")
	flag.StringVar(&topicSnapshots, "topic-snapshots", "p1.opb-snapshots", "manifest topic")
	flag.StringVar(&topicChangelog, "topic-changelog", "p1.opb-changelog", "changelog topic")
	flag.StringVar(&snapshotDir, "snapshot-dir", "./snapshots", "snapshot dir for file mode")
	flag.StringVar(&snapshotFormat, "snapshot-format", "json", "snapshot format to expect when manifest is missing field (json|msgpack)")
	flag.IntVar(&snapshotShards, "snapshot-shards", 1, "snapshot shards to assume when manifest omits the field")
	flag.StringVar(&httpAddr, "http", ":9090", "http listen for /metrics")
	flag.IntVar(&pollIntervalSec, "poll", 10, "poll interval seconds for manifest")
	flag.BoolVar(&advanceManifest, "advance-manifest", true, "after replay, publish updated manifest with new last offset")
	flag.Parse()

	mreg := metrics.NewRegistry()
	go func() {
		http.Handle("/metrics", mreg.Handler())
		_ = http.ListenAndServe(httpAddr, nil)
	}()

	// Build readers
	var mReader rf.Reader
	if manifestSource == "file" {
		mReader = rf.NewFilesystemReader(snapshotDir)
	} else {
		mReader = rk.NewKafkaReader([]string{bootstrap}, topicSnapshots, "opb-manifest-latest")
	}

	// Build publisher for advancing manifest
	var mPublisher manifest.Publisher
	if advanceManifest {
		if manifestSource == "file" {
			mPublisher = manifest.NewFilesystemManifest(snapshotDir)
		} else {
			mPublisher = manifest.NewKafkaManifest(bootstrap, topicSnapshots, "opb-manifest-latest")
		}
	}

	defaultFormat, err := snapshot.ParseFormat(snapshotFormat)
	if err != nil {
		log.Fatalf("parse snapshot-format: %v", err)
	}
	if snapshotShards < 1 {
		snapshotShards = 1
	}

	resolveFormat := func(manifestFormat string) snapshot.Format {
		format := defaultFormat
		if manifestFormat != "" {
			if parsed, perr := snapshot.ParseFormat(manifestFormat); perr == nil {
				format = parsed
			} else {
				log.Printf("recover: unknown snapshot format %s, defaulting to %s", manifestFormat, format)
			}
		}
		return format
	}
	resolveShards := func(manifestShards int) int {
		if manifestShards > 0 {
			return manifestShards
		}
		return snapshotShards
	}

	ticker := time.NewTicker(time.Duration(pollIntervalSec) * time.Second)
	defer ticker.Stop()
	for {
		t1 := time.Now()
		// Use Restorer with a fresh in-memory state each cycle (demo simplicity)
		st := state.NewInMemoryStore()
		r := rf.NewRestorerWithOptions(st, nil, mReader, snapshotDir, defaultFormat, snapshotShards)
		m, err := mReader.ReadLatest()
		if err != nil {
			log.Printf("read manifest: %v", err)
			<-ticker.C
			continue
		}
		restoreFmt := resolveFormat(m.SnapshotFormat)
		if err := r.RestoreFromSnapshotWithFormat(m.SnapshotID, restoreFmt, resolveShards(m.SnapshotShards), m.SnapshotKeys); err != nil {
			log.Printf("restore snapshot: %v", err)
			<-ticker.C
			continue
		}

		var res rf.RestoreResult
		if changelogSource == "file" {
			res = r.ReplayChangelog("./changelog/opb.jsonl", m.LastChangelogOffset)
		} else {
			res = rk.ReplayChangelogKafka(st, []string{bootstrap}, topicChangelog, m.LastChangelogOffset)
		}
		if res.Error != nil {
			log.Printf("replay: %v", res.Error)
			<-ticker.C
			continue
		}

		// Update metrics
		mreg.Applied.Add(float64(res.Applied))
		mreg.Skipped.Add(float64(res.Skipped))
		if res.Bytes > 0 {
			mreg.ReplayBytes.Add(float64(res.Bytes))
		}
		if (res.Applied + res.Skipped) > 0 {
			mreg.ReplayRecords.Add(float64(res.Applied + res.Skipped))
		}
		mreg.TTRSec.Set(time.Since(t1).Seconds())
		// Compute lag: headOffset - lastAppliedOffset
		if changelogSource == "kafka" {
			// Per-partition head and last applied (we only have a single lastAppliedOffset across partitions in demo).
			// For visualization, export head lag at partition 0, and set partition-labeled gauge where possible.
			// Head for partition 0 (compat):
			head := headOffset(topicChangelog, bootstrap)
			if head >= 0 && res.LastAppliedOffset >= 0 {
				mreg.Lag.Set(float64(head - res.LastAppliedOffset))
			}
			// Export partition 0 lag as labeled metric as a minimal viable demo
			if head >= 0 && res.LastAppliedOffset >= 0 {
				mreg.PartitionLag.WithLabelValues(topicChangelog, "0", groupID, instanceID).Set(float64(head - res.LastAppliedOffset))
			}
		}
		// Manifest age
		mreg.LastManifestAgeSec.Set(time.Since(time.Unix(m.CreatedAtEpochSecond, 0)).Seconds())
		// Optionally advance manifest to new last offset
		if advanceManifest && mPublisher != nil {
			if res.LastAppliedOffset > m.LastChangelogOffset {
				if err := mPublisher.PublishLatest(m.SnapshotID, res.LastAppliedOffset); err != nil {
					log.Printf("advance manifest: %v", err)
				} else {
					log.Printf("advanced manifest: snapshot=%s lastOffset=%d (prev=%d)", m.SnapshotID, res.LastAppliedOffset, m.LastChangelogOffset)
				}
			}
		}
		log.Printf("recovery cycle: applied=%d skipped=%d ttr=%.3fs", res.Applied, res.Skipped, time.Since(t1).Seconds())

		<-ticker.C
	}
}

// headOffset returns the last (high-watermark - 1) offset of partition 0 for a topic
func headOffset(topic string, bootstrap string) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := kafka.DialLeader(ctx, "tcp", bootstrap, topic, 0)
	if err != nil {
		return -1
	}
	defer conn.Close()
	off, err := conn.ReadLastOffset()
	if err != nil {
		return -1
	}
	return off - 1
}

// (no adapter needed; state.NewInMemoryStore satisfies state.Store)
