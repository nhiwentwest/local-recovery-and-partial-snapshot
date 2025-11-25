package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"hpb/internal/manifest"
	rf "hpb/internal/restorefs"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

func main() {
	var snapshotDir string
	var snapshotID string
	var formatOverride string
	var shardsOverride int
	var pretty bool
	flag.StringVar(&snapshotDir, "snapshot-dir", "./snapshots", "snapshot directory")
	flag.StringVar(&snapshotID, "snapshot-id", "", "snapshot ID to decode (defaults to latest)")
	flag.StringVar(&formatOverride, "format", "", "snapshot format override (json|msgpack)")
	flag.IntVar(&shardsOverride, "shards", 0, "snapshot shards override (defaults to manifest value)")
	flag.BoolVar(&pretty, "pretty", true, "pretty-print JSON output")
	flag.Parse()

	if snapshotDir == "" {
		log.Fatal("snapshot-dir is required")
	}

	var manifestInfo *manifest.Manifest
	if m, err := rf.NewFilesystemReader(snapshotDir).ReadLatest(); err == nil {
		manifestInfo = &m
	}
	if snapshotID == "" {
		if manifestInfo == nil || manifestInfo.SnapshotID == "" {
			log.Fatal("manifest.latest.json does not contain snapshotId and --snapshot-id not provided")
		}
		snapshotID = manifestInfo.SnapshotID
	}
	if formatOverride == "" && manifestInfo != nil {
		formatOverride = manifestInfo.SnapshotFormat
	}
	if shardsOverride == 0 && manifestInfo != nil {
		shardsOverride = manifestInfo.SnapshotShards
	}

	format, err := snapshot.ParseFormat(formatOverride)
	if err != nil {
		log.Fatalf("parse format: %v", err)
	}

	if shardsOverride < 1 {
		shardsOverride = 1
	}

	var loadSingle func(format snapshot.Format) (map[string]state.RecordState, error)
	loadSingle = func(format snapshot.Format) (map[string]state.RecordState, error) {
		path := filepath.Join(snapshotDir, snapshotID, format.FileName())
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) && format == snapshot.FormatMsgpack {
				return loadSingle(snapshot.FormatJSON)
			}
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		dump, err := snapshot.DecodeSnapshot(data, format)
		return dump, err
	}

	var (
		dump    map[string]state.RecordState
		loadErr error
	)
	if shardsOverride <= 1 {
		dump, loadErr = loadSingle(format)
		if loadErr != nil {
			log.Fatalf("load snapshot: %v", loadErr)
		}
	} else {
		combined := make(map[string]state.RecordState)
		for i := 0; i < shardsOverride; i++ {
			fp := filepath.Join(snapshotDir, snapshotID, format.FileNameForShard(i, shardsOverride))
			data, readErr := os.ReadFile(fp)
			if readErr != nil {
				if os.IsNotExist(readErr) && i == 0 {
					// fallback to single file
					dump, loadErr = loadSingle(format)
					if loadErr != nil {
						log.Fatalf("load snapshot fallback: %v", loadErr)
					}
					shardsOverride = 1
					break
				}
				log.Fatalf("read shard %d: %v", i, readErr)
			}
			shardDump, decErr := snapshot.DecodeSnapshot(data, format)
			if decErr != nil {
				log.Fatalf("decode shard %d: %v", i, decErr)
			}
			for k, v := range shardDump {
				combined[k] = v
			}
		}
		if shardsOverride > 1 {
			dump = combined
		}
	}
	if dump == nil {
		log.Fatal("no snapshot data decoded")
	}

	enc := json.NewEncoder(os.Stdout)
	if pretty {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(dump); err != nil {
		log.Fatalf("encode output: %v", err)
	}
}
