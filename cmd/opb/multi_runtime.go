package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"hpb/internal/kafkautil"
	"hpb/internal/manifest"
	"hpb/internal/opb"
	rf "hpb/internal/restorefs"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

// runMultiInputRuntime spins up N Kafka consumers (one per topic) and coordinates
// marker processing through opb.DynamicNInputOperator with partition-level channels.
func runMultiInputRuntime(cfg Config) error {
	topics := splitCSV(cfg.MultiInputTopics)
	if len(topics) == 0 {
		return fmt.Errorf("multi-input: no topics provided")
	}
	log.Printf("mi event=start topics=%v", topics)

	// --- Admin HTTP and Snapshot Cut Control ---
	type snapshotCutRequest struct {
		cutType string
		prev    *manifest.Manifest
	}
	type barrierCutContext struct {
		id      string
		cutType string
		prev    *manifest.Manifest
	}
	cutReqCh := make(chan snapshotCutRequest, 8)
	activeCuts := struct {
		mu sync.Mutex
		m  map[string]*barrierCutContext
	}{m: make(map[string]*barrierCutContext)}

	// --- State store, snapshotter, manifest publisher ---
	snapFormat, err := snapshot.ParseFormat(cfg.SnapshotFormat)
	if err != nil {
		return err
	}
	if cfg.SnapshotShards < 1 {
		cfg.SnapshotShards = 1
	}
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
	snap := snapshot.NewFilesystemSnapshotter(cfg.SnapshotDir, snapFormat, cfg.SnapshotShards)
	maniFS := manifest.NewFilesystemManifest(cfg.SnapshotDir)
	var mani manifest.Publisher = maniFS
	if (cfg.ManifestSink == "kafka" || cfg.ManifestSink == "both") && cfg.KafkaBootstrap != "" {
		maniK := manifest.NewKafkaManifest(cfg.KafkaBootstrap, cfg.TopicSnapshots, "opb-manifest-latest")
		if cfg.ManifestSink == "kafka" {
			mani = maniK
		} else {
			mani = manifest.MultiPublisher(maniFS, maniK)
		}
	}

	// --- Build consumers, one per topic ---
	type input struct {
		topic string
		c     *ck.Consumer
	}
	inputs := make([]input, 0, len(topics))
	assign := struct { // assignment cache by topic
		mu sync.RWMutex
		m  map[string][]int32
	}{m: make(map[string][]int32)}

	// Pause/Resume helpers across all inputs (guarded)
	var pauseMu sync.Mutex
	pauseAll := func() {
		pauseMu.Lock()
		defer pauseMu.Unlock()
		for _, in := range inputs {
			ass, _ := in.c.Assignment()
			if len(ass) > 0 {
				_ = in.c.Pause(ass)
			}
		}
	}
	resumeAll := func() {
		pauseMu.Lock()
		defer pauseMu.Unlock()
		for _, in := range inputs {
			ass, _ := in.c.Assignment()
			if len(ass) > 0 {
				_ = in.c.Resume(ass)
			}
		}
	}

	// Import-once control
	var importOnce sync.Once

	for i, topic := range topics {
		c, err := ck.NewConsumer(&ck.ConfigMap{
			"bootstrap.servers":             cfg.KafkaBootstrap,
			"group.id":                      fmt.Sprintf("%s-mi-%d", cfg.GroupID, i),
			"enable.auto.commit":            false,
			"isolation.level":               "read_committed",
			"auto.offset.reset":             "earliest",
			"partition.assignment.strategy": "cooperative-sticky",
			"client.id":                     fmt.Sprintf("%s-mi-%d", cfg.InstanceID, i),
			"session.timeout.ms":            cfg.SessionTimeoutMs,
			"heartbeat.interval.ms":         cfg.HeartbeatIntervalMs,
		})
		if err != nil {
			return fmt.Errorf("multi-input: consumer %d init: %w", i, err)
		}
		rebalanceCb := func(c *ck.Consumer, event ck.Event) error {
			switch ev := event.(type) {
			case ck.AssignedPartitions:
				if err := c.IncrementalAssign(ev.Partitions); err != nil {
					log.Printf("mi event=rebalance action=assign err=%v", err)
				}
				parts := make([]int32, 0, len(ev.Partitions))
				for _, tp := range ev.Partitions {
					parts = append(parts, tp.Partition)
				}
				assign.mu.Lock()
				assign.m[topic] = parts
				assign.mu.Unlock()
				log.Printf("mi event=rebalance action=assigned topic=%s parts=%v", topic, parts)
				// Best-effort state import from a peer when enabled (run once)
				if cfg.RebalanceImportState && cfg.PeersCSV != "" {
					importOnce.Do(func() {
						go func() {
							pauseAll()
							peer := firstPeerOtherThanSelf(cfg.HTTPAddr, cfg.PeersCSV)
							if peer == "" {
								log.Printf("mi event=import status=skipped reason=no-peer")
								resumeAll()
								return
							}
							count, err := importStateFromPeer(peer, st)
							if err != nil {
								log.Printf("mi event=import status=error peer=%s err=%v", peer, err)
							} else {
								log.Printf("mi event=import status=ok peer=%s count=%d", peer, count)
							}
							resumeAll()
						}()
					})
				}
			case ck.RevokedPartitions:
				if err := c.IncrementalUnassign(ev.Partitions); err != nil {
					log.Printf("mi event=rebalance action=unassign err=%v", err)
				}
				assign.mu.Lock()
				assign.m[topic] = nil
				assign.mu.Unlock()
				log.Printf("mi event=rebalance action=revoked topic=%s count=%d", topic, len(ev.Partitions))
			}
			return nil
		}
		if err := c.SubscribeTopics([]string{topic}, rebalanceCb); err != nil {
			return fmt.Errorf("multi-input: subscribe %s: %w", topic, err)
		}
		inputs = append(inputs, input{topic: topic, c: c})
	}
	defer func() {
		for _, in := range inputs {
			in.c.Close()
		}
	}()

	// Producer for propagations (barrier markers) and admin injections
	pCfg := &ck.ConfigMap{
		"bootstrap.servers":  cfg.KafkaBootstrap,
		"enable.idempotence": true,
		"acks":               "all",
		"transactional.id":   fmt.Sprintf("opb-mi-%s", cfg.InstanceID),
		"linger.ms":          5,
		"compression.type":   "lz4",
	}
	prod, err := ck.NewProducer(pCfg)
	if err != nil {
		return fmt.Errorf("multi-input: producer: %w", err)
	}
	defer prod.Close()
	if err := prod.InitTransactions(context.TODO()); err != nil {
		return fmt.Errorf("multi-input: init tx: %w", err)
	}
	injP, injErr := ck.NewProducer(&ck.ConfigMap{"bootstrap.servers": cfg.KafkaBootstrap, "linger.ms": 5, "compression.type": "lz4"})
	if injErr != nil {
		log.Printf("mi event=injector status=error err=%v", injErr)
	}
	defer func() {
		if injP != nil {
			injP.Close()
		}
	}()

	// --- Operator wiring ---
	op := opb.NewDynamicNInputOperator()
	// Expected provider based on current assignment across all input topics
	op.Expected = func() []string {
		assign.mu.RLock()
		defer assign.mu.RUnlock()
		var keys []string
		for t, parts := range assign.m {
			for _, p := range parts {
				keys = append(keys, fmt.Sprintf("%s#%d", t, p))
			}
		}
		return keys
	}
	// Propagate barrier: on first marker, send to ALL partitions of output topic
	op.Propagate = func(m opb.Marker) {
		var md *ck.Metadata
		var merr error
		for i := 0; i < 3; i++ {
			md, merr = prod.GetMetadata(&cfg.OutputTopic, false, int((3 * time.Second).Milliseconds()))
			if merr == nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if merr != nil {
			log.Printf("mi event=propagate stage=metadata status=failed id=%s topic=%s err=%v", m.SnapshotID, cfg.OutputTopic, merr)
			return
		}
		tp, ok := md.Topics[cfg.OutputTopic]
		if !ok {
			log.Printf("mi event=propagate status=failed id=%s topic=%s err=%s", m.SnapshotID, cfg.OutputTopic, "not-found")
			return
		}
		h := opb.BarrierHeaders(m.SnapshotID)
		_ = prod.BeginTransaction()
		for _, part := range tp.Partitions {
			_ = prod.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &cfg.OutputTopic, Partition: int32(part.ID)}, Key: []byte("barrier"), Headers: h}, nil)
		}
		var cerr error
		for i := 0; i < 2; i++ {
			cerr = prod.CommitTransaction(context.TODO())
			if cerr == nil {
				break
			}
			time.Sleep(150 * time.Millisecond)
		}
		if cerr != nil {
			log.Printf("mi event=propagate stage=commit status=failed id=%s topic=%s err=%v", m.SnapshotID, cfg.OutputTopic, cerr)
			return
		}
		log.Printf("mi event=propagate status=committed id=%s topic=%s partitions=%d", m.SnapshotID, cfg.OutputTopic, len(tp.Partitions))
	}
	// Logging for block/unblock
	op.OnBlock = func(ch string) { log.Printf("mi event=block channel=%s cutId=%s", ch, op.CurCutID()) }
	op.OnUnblock = func() { log.Printf("mi event=unblock cutId=%s", op.CurCutID()) }
	// Complete: write snapshot from state view, persist inflight.json, publish manifest
	op.Complete = func(id string, inflight map[string][]opb.Event) {
		activeCuts.mu.Lock()
		cutCtx, ok := activeCuts.m[id]
		if ok {
			delete(activeCuts.m, id)
		}
		activeCuts.mu.Unlock()
		if !ok {
			log.Printf("mi event=complete status=error id=%s err=no_cut_context", id)
			return
		}

		var totalInflight int
		for _, evs := range inflight {
			totalInflight += len(evs)
		}
		log.Printf("mi event=complete id=%s type=%s channels=%d inflightEvents=%d", id, cutCtx.cutType, len(inflight), totalInflight)

		// Build current changelog offsets (needed for delta dirty-keys scan)
		var offInfo *manifest.OffsetsInfo
		if cfg.KafkaBootstrap != "" && cfg.TopicChangelog != "" {
			if offs, parts, err := kafkautil.CollectChangelogOffsets(cfg.KafkaBootstrap, cfg.TopicChangelog); err == nil {
				offInfo = &manifest.OffsetsInfo{Topic: cfg.TopicChangelog, Partitions: parts, Offsets: offs}
			} else {
				log.Printf("mi event=snapshot stage=collect-offsets status=error id=%s err=%v", id, err)
			}
		}

		// Snapshot from a point-in-time view
		view, err := st.NewSnapshotView()
		if err != nil {
			log.Printf("mi event=snapshot stage=view status=error id=%s err=%v", id, err)
			return
		}
		defer view.Close()

		var meta snapshot.Result
		var serr error
		mtype := manifest.SnapshotTypeFull
		var baseID, parentID string
		var dseq int

		t0 := time.Now()
		if cutCtx.cutType == manifest.SnapshotTypeDelta {
			// Validate prev manifest and offsets
			if cutCtx.prev == nil || cutCtx.prev.Changelog == nil || offInfo == nil || offInfo.Topic == "" || cutCtx.prev.Changelog.Topic == "" {
				log.Printf("mi event=snapshot stage=delta-skip id=%s reason=missing-prev-or-offsets", id)
				meta, serr = snap.WriteSnapshotFromView(id, view)
				mtype = manifest.SnapshotTypeFull
			} else {
				// Determine base/parent and sequence
				parentID = cutCtx.prev.SnapshotID
				if cutCtx.prev.SnapshotType == manifest.SnapshotTypeDelta && cutCtx.prev.BaseSnapshotID != "" {
					baseID = cutCtx.prev.BaseSnapshotID
					dseq = cutCtx.prev.DeltaSequence + 1
				} else {
					baseID = cutCtx.prev.SnapshotID
					dseq = 1
				}
				// Scan dirty keys between prev and current changelog offsets
				keys, kerr := kafkautil.ScanDirtyKeysKafka([]string{cfg.KafkaBootstrap}, cutCtx.prev.Changelog.Topic, cutCtx.prev.Changelog.Offsets, offInfo.Offsets, 0, 1500*time.Millisecond)
				if kerr != nil {
					log.Printf("mi event=snapshot stage=dirty-scan status=error id=%s err=%v", id, kerr)
					meta, serr = snap.WriteSnapshotFromView(id, view)
					mtype = manifest.SnapshotTypeFull
				} else {
					log.Printf("mi event=snapshot stage=delta-start id=%s dirtyKeys=%d", id, len(keys))
					meta, serr = snap.WriteDeltaSnapshotFromView(id, view, keys)
					mtype = manifest.SnapshotTypeDelta
				}
			}
		} else {
			meta, serr = snap.WriteSnapshotFromView(id, view)
		}
		durMs := time.Since(t0).Milliseconds()

		if serr != nil {
			log.Printf("mi event=snapshot stage=write status=error id=%s type=%s err=%v", id, mtype, serr)
			return
		}
		st.MarkSnapshotDone()

		var channels []string
		for ch := range inflight {
			channels = append(channels, ch)
		}
		inflightRecords := make(map[string][]inflightRecord, len(inflight))
		for ch, evs := range inflight {
			for _, ev := range evs {
				rec := inflightRecord{Key: ev.Key}
				if ev.VC != nil {
					rec.VC = ev.VC.Copy()
				}
				inflightRecords[ch] = append(inflightRecords[ch], rec)
			}
		}
		relInflight, inflightCount, inflightErr := writeInflightSnapshot(cfg.SnapshotDir, id, channels, inflightRecords)
		if inflightErr != nil {
			log.Printf("mi event=inflight stage=write status=error id=%s err=%v", id, inflightErr)
		} else if inflightCount > 0 {
			log.Printf("mi event=inflight stage=write status=ok id=%s count=%d", id, inflightCount)
		}

		// Log delta size for tuning (if delta)
		if mtype == manifest.SnapshotTypeDelta {
			var totalBytes int64
			if meta.Shards <= 1 {
				fp := filepath.Join(cfg.SnapshotDir, id, meta.Format.FileNameDelta())
				if fi, err := os.Stat(fp); err == nil {
					totalBytes += fi.Size()
				}
			} else {
				for i := 0; i < meta.Shards; i++ {
					fp := filepath.Join(cfg.SnapshotDir, id, meta.Format.FileNameDeltaForShard(i, meta.Shards))
					if fi, err := os.Stat(fp); err == nil {
						totalBytes += fi.Size()
					}
				}
			}
			log.Printf("mi event=snapshot stage=delta-metrics id=%s keys=%d bytes=%d durMs=%d", id, meta.Keys, totalBytes, durMs)
		}

		m := manifest.Manifest{
			SnapshotID:           id,
			SnapshotFormat:       meta.Format.String(),
			SnapshotShards:       meta.Shards,
			SnapshotKeys:         meta.Keys,
			SnapshotType:         mtype,
			BaseSnapshotID:       baseID,
			ParentSnapshotID:     parentID,
			DeltaSequence:        dseq,
			CreatedAtEpochSecond: time.Now().UTC().Unix(),
			Changelog:            offInfo,
			Channels:             channels,
			InflightFile:         relInflight,
			InflightEvents:       inflightCount,
		}
		// Publish manifest with one retry
		publishOnce := func() error {
			if fp, ok := mani.(manifest.FullPublisher); ok {
				return fp.Publish(m)
			}
			return mani.PublishLatest(id, 0)
		}
		perr := publishOnce()
		if perr != nil {
			time.Sleep(200 * time.Millisecond)
			perr = publishOnce()
		}
		if perr != nil {
			log.Printf("mi event=manifest stage=publish status=error id=%s err=%v", id, perr)
			return
		}
		log.Printf("mi event=manifest stage=publish status=ok id=%s type=%s shards=%d keys=%d", id, mtype, meta.Shards, meta.Keys)
	}

	// Goroutine to handle cut requests and inject barriers
	go func() {
		for req := range cutReqCh {
			assign.mu.RLock()
			am := make(map[string][]int32, len(assign.m))
			for t, ps := range assign.m {
				if len(ps) > 0 {
					cp := make([]int32, len(ps))
					copy(cp, ps)
					am[t] = cp
				}
			}
			assign.mu.RUnlock()
			if len(am) == 0 {
				log.Printf("mi event=admin-cut status=skipped reason=no-assignment")
				continue
			}

			id := fmt.Sprintf("cut-%d", time.Now().UnixNano())
			cutCtx := &barrierCutContext{id: id, cutType: req.cutType, prev: req.prev}
			activeCuts.mu.Lock()
			activeCuts.m[id] = cutCtx
			activeCuts.mu.Unlock()

			h := opb.BarrierHeaders(id)
			var injected []string
			for topic, parts := range am {
				for _, p := range parts {
					if err := injP.Produce(&ck.Message{TopicPartition: ck.TopicPartition{Topic: &topic, Partition: p}, Key: []byte("barrier"), Headers: h}, nil); err == nil {
						injected = append(injected, fmt.Sprintf("%s#%d", topic, p))
					}
				}
			}
			remaining := injP.Flush(2000)
			log.Printf("mi event=admin-cut status=injected id=%s type=%s count=%d partitions=%v remaining=%d", id, req.cutType, len(injected), injected, remaining)
		}
	}()

	go func(addr string) {
		mux := http.NewServeMux()
		maniReader := rf.NewFilesystemReader(cfg.SnapshotDir) // For delta cut checks

		mux.HandleFunc("/admin/snapshot-cut-multi", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			q := r.URL.Query()
			stype := strings.ToLower(strings.TrimSpace(q.Get("type")))
			if stype == "" {
				stype = manifest.SnapshotTypeFull
			}
			if stype != manifest.SnapshotTypeFull && stype != manifest.SnapshotTypeDelta {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid type (use full|delta)"})
				return
			}

			var prev *manifest.Manifest
			if stype == manifest.SnapshotTypeDelta {
				m, err := maniReader.ReadLatest()
				if err != nil || m.SnapshotID == "" || m.Changelog == nil || len(m.Changelog.Offsets) == 0 || m.Changelog.Topic == "" {
					w.WriteHeader(http.StatusBadRequest)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": "delta cut requires existing manifest with per-partition offsets"})
					return
				}
				prev = &m
			}

			select {
			case cutReqCh <- snapshotCutRequest{cutType: stype, prev: prev}:
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted", "type": stype})
			default:
				w.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "busy"})
			}
		})

		// Simple rate-limited cut across all topics

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
		// Admin: import state from NDJSON {key,state}
		mux.HandleFunc("/admin/state/load", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			scanner := bufio.NewScanner(r.Body)
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
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
			st.LoadAll(buf)
			_ = json.NewEncoder(w).Encode(map[string]any{"loaded": len(buf)})
		})
		_ = http.ListenAndServe(addr, mux)
	}(cfg.HTTPAddr)

	// --- Reader goroutines per consumer -> central channel ---
	type item struct {
		key string
		msg *ck.Message
	}
	chMsgs := make(chan item, 1024)
	stop := make(chan struct{})
	for _, in := range inputs {
		in := in
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				msg, err := in.c.ReadMessage(250 * time.Millisecond)
				if err != nil {
					continue
				}
				key := fmt.Sprintf("%s#%d", in.topic, msg.TopicPartition.Partition)
				chMsgs <- item{key: key, msg: msg}
			}
		}()
	}

	// --- Main loop ---
	for {
		select {
		case it := <-chMsgs:
			if ok, bid := opb.IsBarrier(it.msg.Headers); ok {
				m := opb.Marker{SnapshotID: bid, VC: opb.ExtractVectorClock(it.msg.Headers)}
				op.OnIn(it.key, opb.Event{Marker: &m})
				continue
			}
			// data event: forward basic envelope with key+vc
			k := string(it.msg.Key)
			vc := opb.ExtractVectorClock(it.msg.Headers)
			op.OnIn(it.key, opb.Event{Key: k, VC: vc})
		case <-time.After(5 * time.Second):
			if cfg.InstanceID != "" {
				log.Printf("mi event=idle instance=%s", cfg.InstanceID)
			}
		}
	}
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// importStateFromPeer fetches NDJSON of {key,state} from peer and loads into state store.
func importStateFromPeer(peerBase string, st state.Store) (int, error) {
	cli := &http.Client{Timeout: 15 * time.Second}
	resp, err := cli.Get(strings.TrimRight(peerBase, "/") + "/admin/state/export")
	if err != nil {
		return 0, err
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
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}
	st.LoadAll(buf)
	return len(buf), nil
}

// firstPeerOtherThanSelf returns the first peer different from self http address.
func firstPeerOtherThanSelf(httpAddr string, peersCSV string) string {
	mkSelf := func() string {
		addr := strings.TrimSpace(httpAddr)
		if strings.HasPrefix(addr, ":") {
			return "http://127.0.0.1" + addr
		}
		if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
			return addr
		}
		return "http://" + addr
	}
	self := mkSelf()
	for _, p := range strings.Split(peersCSV, ",") {
		p = strings.TrimSpace(p)
		if p == "" || p == self {
			continue
		}
		return p
	}
	return ""
}
