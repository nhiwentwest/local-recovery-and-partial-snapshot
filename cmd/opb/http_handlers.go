package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"hpb/internal/manifest"
	"hpb/internal/metrics"
	"hpb/internal/opb"
	rf "hpb/internal/restorefs"
	"hpb/internal/snapshot"
	"hpb/internal/state"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// httpHandlersDeps contains all dependencies needed for HTTP handlers.
type httpHandlersDeps struct {
	cfg                    Config
	st                     state.Store
	appStatus              *opb.StatusManager
	mreg                   *metrics.Registry
	zoneIdx                *opb.ZoneIndex
	maniReader             rf.Reader
	snapshotCutReq         chan snapshotCutRequest
	ingestCtrl             chan ingestCommand
	ingestPaused           *atomic.Bool
	ingestControlEnabled   bool
	injP                   *ck.Producer
	injErr                 error
	injLast                map[string]time.Time
	pauseMu                *sync.Mutex
	resolveSnapshotFormat  func(string) snapshot.Format
	resolveSnapshotShards  func(int) int
	readSnapshotManifest   func(string) (manifest.Manifest, error)
	snapshotSizeBytes      func(string, snapshot.Format, int) float64
	deltaSnapshotSizeBytes func(string, snapshot.Format, int) float64
}

// setupHTTPHandlers sets up all HTTP handlers and starts the HTTP server.
func setupHTTPHandlers(addr string, deps httpHandlersDeps) {
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
		return sendIngestCommand(deps.ingestCtrl, pause)
	}

	mkSelf := func() string {
		return makeSelfURL(deps.cfg.HTTPAddr)
	}
	peersList := func() []string {
		self := mkSelf()
		seen := map[string]bool{self: true}
		urls := []string{self}
		if deps.cfg.PeersCSV != "" {
			for _, p := range strings.Split(deps.cfg.PeersCSV, ",") {
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
	_ = vizTTL // used in buildCluster closure
	// Self updater
	go func() {
		t := time.NewTicker(vizInt)
		defer t.Stop()
		for range t.C {
			st := deps.appStatus.Load()
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
	mux.Handle("/metrics", deps.mreg.Handler())
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
			m, err := deps.maniReader.ReadLatest()
			if err != nil || m.SnapshotID == "" {
				resolved = manifest.SnapshotTypeFull
			} else {
				// If delta disabled by config
				if deps.cfg.SnapMaxDeltas <= 0 {
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
							cm, e2 := deps.readSnapshotManifest(cur.SnapshotID)
							if e2 == nil {
								format := deps.resolveSnapshotFormat(cm.SnapshotFormat)
								shards := deps.resolveSnapshotShards(cm.SnapshotShards)
								deltaBytes += deps.deltaSnapshotSizeBytes(cur.SnapshotID, format, shards)
							}
							if cur.ParentSnapshotID == "" || strings.ToLower(cur.SnapshotType) != manifest.SnapshotTypeDelta {
								break
							}
							pm, e3 := deps.readSnapshotManifest(cur.ParentSnapshotID)
							if e3 != nil {
								break
							}
							cur = pm
						}
					}
					// apply thresholds
					if deltaCount >= deps.cfg.SnapMaxDeltas {
						resolved = manifest.SnapshotTypeFull
					} else if deps.cfg.SnapMaxDeltaMB > 0 && (deltaBytes/1024.0/1024.0) >= float64(deps.cfg.SnapMaxDeltaMB) {
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
			m, err := deps.maniReader.ReadLatest()
			if err != nil || m.SnapshotID == "" || m.Changelog == nil || len(m.Changelog.Offsets) == 0 || m.Changelog.Topic == "" {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "delta cut requires existing manifest with per-partition offsets"})
				return
			}
			prev = &m
		}
		req := snapshotCutRequest{cutType: resolved, prev: prev}
		select {
		case deps.snapshotCutReq <- req:
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted", "type": resolved})
		default:
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "busy"})
		}
	})

	// Admin: trigger snapshot GC (best-effort). POST only.
	gc := snapshot.NewGarbageCollector(deps.cfg.SnapshotDir, deps.cfg.SnapRetentionCount, deps.cfg.SnapRetentionDays, deps.maniReader)
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
		view, err := deps.st.NewSnapshotView()
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
		if !deps.ingestControlEnabled {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "ingest control not available"})
			return
		}
		if deps.ingestPaused.Load() {
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
		if !deps.ingestControlEnabled {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "ingest control not available"})
			return
		}
		if !deps.ingestPaused.Load() {
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
		if err := deps.st.Range(matchFunc); err != nil && !errors.Is(err, errLimit) {
			log.Printf("prune-state: range error: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
			return
		}
		deleted := 0
		if !req.DryRun {
			for _, key := range selected {
				if err := deps.st.Delete(key); err != nil {
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
	mux.Handle("/viz/heatmap", opb.NewHeatmapHandler(deps.st, deps.cfg.WindowSizeSec, deps.cfg.InstanceID))
	mux.Handle("/api/zone-details", opb.NewZoneDetailsHandler(deps.st, deps.zoneIdx, deps.cfg.WindowSizeSec, deps.cfg.InstanceID, opb.RealClock{}))
	mux.HandleFunc("/api/inject-test-data", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if deps.injP == nil || deps.injErr != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "injector unavailable"})
			return
		}
		// Rate limit: 1 req per 2s per client
		ip := r.RemoteAddr
		now := time.Now()
		deps.pauseMu.Lock()
		last, ok := deps.injLast[ip]
		deps.pauseMu.Unlock()
		if ok && now.Sub(last) < 2*time.Second {
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "rate limit: 1 req per 2s"})
			return
		}
		deps.pauseMu.Lock()
		deps.injLast[ip] = now
		deps.pauseMu.Unlock()
		var job struct {
			StoreID   string `json:"storeId"`
			ProductID string `json:"productId"`
			WS        int64  `json:"ws"`
			Mode      string `json:"mode"`
			N         int    `json:"n"`
		}
		if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid json"})
			return
		}
		if job.N <= 0 {
			job.N = 1
		}
		if job.N > 100 {
			job.N = 100
		}
		if job.Mode == "" {
			job.Mode = "single"
		}
		// Inject test data
		topic := deps.cfg.TopicEnriched
		for i := 0; i < job.N; i++ {
			var value string
			if job.Mode == "burst" {
				value = fmt.Sprintf(`{"orderId":"test-%d-%d","productId":"%s","price":10000,"qty":1,"storeId":"%s","ts":%d,"validated":true,"normTs":%d}`,
					time.Now().UnixNano(), i, job.ProductID, job.StoreID, job.WS, job.WS)
			} else {
				value = fmt.Sprintf(`{"orderId":"test-%d","productId":"%s","price":10000,"qty":1,"storeId":"%s","ts":%d,"validated":true,"normTs":%d}`,
					time.Now().UnixNano(), job.ProductID, job.StoreID, job.WS, job.WS)
			}
			err := deps.injP.Produce(&ck.Message{
				TopicPartition: ck.TopicPartition{Topic: &topic, Partition: ck.PartitionAny},
				Key:            []byte(fmt.Sprintf("%s#%s", job.StoreID, job.ProductID)),
				Value:          []byte(value),
			}, nil)
			if err != nil {
				log.Printf("inject: produce error: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
				return
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"injected": job.N})
	})
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
		if rec, ok := deps.st.Get(key); ok {
			_ = json.NewEncoder(w).Encode(map[string]any{"found": true, "sumQty": rec.SumQty, "sumAmount": rec.SumAmount, "lastSeq": rec.LastSeq, "lastUpdatedBy": rec.LastUpdatedBy, "key": key})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"found": false, "key": key})
	})
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
		_ = deps.st.Range(func(key string, rs state.RecordState) error {
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
		if latest, err := deps.maniReader.ReadLatest(); err == nil && latest.SnapshotID != "" {
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
				if next, err := deps.readSnapshotManifest(cur.ParentSnapshotID); err == nil {
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
		metricsPath := filepath.Join(deps.cfg.StateDir, "restore-metrics.json")
		if rm, err := readRestoreMetrics(metricsPath); err == nil {
			res := rm
			resp["restoreMetrics"] = res
			resp["restoreInstance"] = deps.cfg.InstanceID
			resp["restoreSource"] = deps.cfg.InstanceID
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
			restoreFleet[deps.cfg.InstanceID] = *localRestore
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
			base = deps.cfg.PromURL
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
		fmt.Fprintf(w, "<div style='display:none'>")
		fmt.Fprintf(w, "<div style='display:none'>")
		fmt.Fprintf(w, "<!-- recovery summary removed -->")
		fmt.Fprintf(w, "</div>")
		fmt.Fprintf(w, "</div>")
		// Fetch API
		cli := &http.Client{Timeout: 2 * time.Second}
		addr := strings.TrimSpace(deps.cfg.HTTPAddr)
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
		fmt.Fprintf(w, "<script>\n(async function(){\n  async function loadStatus(){\n    try{\n      const res = await fetch('/status', {cache:'no-store'});\n      const j = await res.json();\n      const el = null;\n      if(!el) return;\n      const ttr = (j.ttrMs!==undefined? j.ttrMs+' ms':'N/A');\n      const snap = (j.restoringSnapshotId||'N/A');\n      const off = (j.lastChangelogOffset!==undefined? j.lastChangelogOffset:'N/A');\n      const ap = (j.lastRestoreApplied!==undefined? j.lastRestoreApplied:'N/A');\n      const sk = (j.lastRestoreSkipped!==undefined? j.lastRestoreSkipped:'N/A');\n      el.innerHTML = `<div>ttrMs: <b>${ttr}</b></div>`+\n                     `<div>snapshotId: <span class='muted'>${snap}</span></div>`+\n                     `<div>lastChangelogOffset: <span class='muted'>${off}</span></div>`+\n                     `<div>restore applied/skipped: <span class='muted'>${ap}</span>/<span class='muted'>${sk}</span></div>`;\n      const wsInput = document.getElementById('pf-ws');\n      if(wsInput && !wsInput.value && j.windowSizeSec){\n        const now = Math.floor(Date.now()/1000);\n        const ws = Math.floor(now / j.windowSizeSec) * j.windowSizeSec;\n        wsInput.value = String(ws);\n      }\n    }catch(e){\n      const el = null;\n      if(el) el.textContent = 'N/A';\n    }\n  }\n  function setupProbe(){\n    const btn = document.getElementById('pf-run');\n    if(!btn) return;\n    btn.addEventListener('click', async function(){\n      const s = document.getElementById('pf-store').value.trim();\n      const p = document.getElementById('pf-prod').value.trim();\n      const w = document.getElementById('pf-ws').value.trim();\n      if(!s || !p || !w){ return; }\n      const url = `/viz/zone-data?id=${encodeURIComponent(s)}&productId=${encodeURIComponent(p)}&ws=${encodeURIComponent(w)}`;\n      const pu = document.getElementById('probe-url');\n      if(pu) pu.textContent = url;\n      const fr = document.getElementById('probe-frame');\n      if(fr) fr.src = url;\n    });\n  }\n  await loadStatus();\n  setupProbe();\n})();\n</script>")
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
		sumA, sumQ, _ := deps.zoneIdx.Snapshot(id)
		fmt.Fprintf(w, "<h4>Store mode (aggregates)</h4>")
		fmt.Fprintf(w, "<pre>{\n  \"storeId\": \"%s\",\n  \"sumAmount\": %d,\n  \"sumQty\": %d\n}</pre>", id, sumA, sumQ)
		fmt.Fprintf(w, "<div class='small muted'>Heatmap total = sumQty=%d (hiện tại). sumQty được tạo bởi số product active × số events trên mỗi product.</div>", sumQ)
		var totalSumQty, totalSumAmount int64
		var maxLastSeq int64
		var lastUpdatedBy string
		_ = deps.st.Range(func(key string, rs state.RecordState) error {
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
		fmt.Fprintf(w, "<h4>Recent windows · windowSize=%ds</h4>", deps.cfg.WindowSizeSec)
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
				if rec, ok := deps.st.Get(key); ok {
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
		if deps.appStatus.Load().Status != "healthy" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": deps.appStatus.Load().Status})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		st := deps.appStatus.Load()
		// Snapshot fast-path EOS counters from metrics registry
		// Prometheus client counters do not expose read, so we only include fields already in AppStatus for now.
		// Optionally, these counters could be mirrored into StatusManager if REST snapshots are required.
		_ = json.NewEncoder(w).Encode(st)
	})
	_ = http.ListenAndServe(addr, mux)
}

