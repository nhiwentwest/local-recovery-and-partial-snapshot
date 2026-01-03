package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"hpb/internal/manifest"
	"hpb/internal/opb"
	rf "hpb/internal/restorefs"
	"hpb/internal/snapshot"
	"hpb/internal/state"
)

type pebbleSnapshotViewAdapter struct {
	snap  *snapshot.PebbleSnapshotter
	store state.Store
}

func (a pebbleSnapshotViewAdapter) WriteSnapshotFromView(id string, _ state.SnapshotView) (snapshot.Result, error) {
	return a.snap.WriteSnapshot(id, a.store)
}

func (a pebbleSnapshotViewAdapter) WriteDeltaSnapshotFromView(id string, _ state.SnapshotView, keys []string) (snapshot.Result, error) {
	if len(keys) == 0 {
		return snapshot.Result{}, fmt.Errorf("pebble delta snapshot requires dirty keys")
	}
	return a.snap.WriteDeltaSnapshot(id, a.store, keys)
}

// runMultiInputRuntime spins up N Kafka consumers (one per topic) and coordinates
// marker processing through opb.DynamicNInputOperator with partition-level channels.
func runMultiInputRuntime(cfg Config) error {
	topics := splitCSV(cfg.MultiInputTopics)
	if len(topics) == 0 {
		return fmt.Errorf("multi-input: no topics provided")
	}
	log.Printf("mi event=start topics=%v", topics)

	// --- Admin HTTP and Snapshot Cut Control ---
	cutReqCh := make(chan snapshotCutRequest, 8)
	activeCuts := &activeCutsMap{m: make(map[string]*barrierCutContext)}

	// --- State store, snapshotter, manifest publisher --- (extracted helper)
	ctx, err := initMiStoreSnapshot(cfg)
	if err != nil {
		return err
	}
	st := ctx.st
	snap := ctx.snap
	mani := ctx.mani
	// Ensure Pebble store closes on function exit
	if ps, ok := st.(*state.PebbleStore); ok {
	defer ps.Close()
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

	// Producer for propagations (barrier markers) and admin injections (extracted helper)
	prod, injP, err := initMiProducers(cfg)
	if err != nil {
		return fmt.Errorf("multi-input: %w", err)
	}
	defer prod.Close()
	if injP == nil {
		log.Printf("mi event=injector status=error err=%v", fmt.Errorf("init failed"))
	} else {
		defer injP.Close()
		}

	// --- Operator wiring ---
	op := opb.NewDynamicNInputOperator()
	// Non-snapshot wiring extracted (Expected / Propagate / Block / Unblock)
	wireOperatorBasics(op, prod, cfg, &assign)
	wireOperatorComplete(op, cfg, st, snap, mani, activeCuts)

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
