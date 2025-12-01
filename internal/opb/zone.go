package opb

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"hpb/internal/state"
)

// ZoneAgg holds per-store aggregates (transient, for quick API).
type ZoneAgg struct {
	SumAmount  int64           `json:"sumAmount"`
	SumQty     int64           `json:"sumQty"`
	Instances  map[string]bool `json:"-"`
	lastResp   []byte          // cached JSON response bytes (store mode)
	lastRespAt int64           // unix seconds of last cache build
}

type ZoneIndex struct {
	mu   sync.RWMutex
	data map[string]*ZoneAgg // storeId -> agg
}

func NewZoneIndex() *ZoneIndex { return &ZoneIndex{data: make(map[string]*ZoneAgg)} }

// OnApplied updates per-store aggregates when an event is applied.
func (z *ZoneIndex) OnApplied(storeID string, deltaAmount, deltaQty int64, instanceID string) {
	z.mu.Lock()
	agg, ok := z.data[storeID]
	if !ok {
		agg = &ZoneAgg{Instances: make(map[string]bool)}
		z.data[storeID] = agg
	}
	agg.SumAmount += deltaAmount
	agg.SumQty += deltaQty
	if instanceID != "" {
		agg.Instances[instanceID] = true
	}
	// Invalidate cache
	agg.lastRespAt = 0
	agg.lastResp = nil
	z.mu.Unlock()
}

// Snapshot returns a copy of aggregates (no caching) for external composition.
func (z *ZoneIndex) Snapshot(storeID string) (sumAmount, sumQty int64, related []string) {
	z.mu.RLock()
	agg := z.data[storeID]
	if agg == nil {
		z.mu.RUnlock()
		return 0, 0, nil
	}
	sumAmount = agg.SumAmount
	sumQty = agg.SumQty
	for s := range agg.Instances {
		related = append(related, s)
	}
	z.mu.RUnlock()
	return
}

// StoreResponse returns a cached JSON response for store mode with 1s TTL.
func (z *ZoneIndex) StoreResponse(storeID, instanceID string, nowUnix int64) []byte {
	z.mu.Lock()
	agg := z.data[storeID]
	if agg == nil {
		agg = &ZoneAgg{Instances: make(map[string]bool)}
		z.data[storeID] = agg
	}
	if agg.lastResp != nil && nowUnix-agg.lastRespAt < 1 {
		// Return cached bytes (copy to avoid races)
		b := make([]byte, len(agg.lastResp))
		copy(b, agg.lastResp)
		z.mu.Unlock()
		return b
	}
	// Rebuild response
	related := make([]string, 0, len(agg.Instances))
	for s := range agg.Instances {
		related = append(related, s)
	}
	payload := map[string]any{
		"mode":      "store",
		"storeId":   storeID,
		"sumAmount": agg.SumAmount,
		"sumQty":    agg.SumQty,
		"instances": related,
		"instance":  instanceID,
	}
	b, _ := json.Marshal(payload)
	agg.lastResp = b
	agg.lastRespAt = nowUnix
	// return copy
	out := make([]byte, len(b))
	copy(out, b)
	z.mu.Unlock()
	return out
}

// ZoneDetailsHandler supports two modes:
// - Exact key: supply productId and ws to fetch state.Get(key) O(1)
// - Store mode: supply only id (storeId) to return per-store aggregates from ZoneIndex (with 1s TTL cache)
func NewZoneDetailsHandler(st state.Store, idx *ZoneIndex, windowSizeSec int, instanceID string, clock Clock) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		storeID := q.Get("id")
		if storeID == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "missing id (storeId)"})
			return
		}
		productID := q.Get("productId")
		wsStr := q.Get("ws")
		if productID != "" && wsStr != "" {
			ws, err := strconv.ParseInt(wsStr, 10, 64)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "invalid ws"})
				return
			}
			key := OutputKey(storeID, productID, ws)
			if rec, ok := st.Get(key); ok {
				// Build per-source breakdown for this exact key.
				sourceBreakdown := map[string]any{}
				if len(rec.Sources) > 0 {
					for sk, stats := range rec.Sources {
						if stats.SumQty == 0 && stats.SumAmount == 0 {
							continue
						}
						sourceBreakdown[string(sk)] = map[string]any{
							"sumAmount": stats.SumAmount,
							"sumQty":    stats.SumQty,
						}
					}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"mode":            "exact",
					"storeId":         storeID,
					"productId":       productID,
					"ws":              ws,
					"sumAmount":       rec.SumAmount,
					"sumQty":          rec.SumQty,
					"lastSeq":         rec.LastSeq,
					"lastUpdatedBy":   rec.LastUpdatedBy,
					"instance":        instanceID,
					"sourceBreakdown": sourceBreakdown,
				})
				return
			}
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "key not found", "key": key})
			return
		}
		// Store mode: aggregate from state store (like heatmap total) to include old data
		// This ensures consistency with heatmap total mode
		var totalSumQty, totalSumAmount int64
		sourceTotals := make(map[state.SourceKind]state.SourceStats)
		var maxLastSeq int64
		var lastUpdatedBy string
		_ = st.Range(func(key string, rs state.RecordState) error {
			parts := strings.Split(key, "#")
			if len(parts) == 3 && parts[0] == storeID {
				totalSumQty += rs.SumQty
				totalSumAmount += rs.SumAmount
				if rs.LastSeq > maxLastSeq {
					maxLastSeq = rs.LastSeq
					lastUpdatedBy = rs.LastUpdatedBy
				}
				if len(rs.Sources) > 0 {
					for sk, stats := range rs.Sources {
						if stats.SumQty == 0 && stats.SumAmount == 0 {
							continue
						}
						cur := sourceTotals[sk]
						cur.SumAmount += stats.SumAmount
						cur.SumQty += stats.SumQty
						sourceTotals[sk] = cur
					}
				}
			}
			return nil
		})
		// Convert sourceTotals to a JSON-friendly map keyed by string.
		sourceBreakdown := map[string]any{}
		for sk, stats := range sourceTotals {
			if stats.SumQty == 0 && stats.SumAmount == 0 {
				continue
			}
			sourceBreakdown[string(sk)] = map[string]any{
				"sumAmount": stats.SumAmount,
				"sumQty":    stats.SumQty,
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode":            "store-total",
			"storeId":         storeID,
			"sumAmount":       totalSumAmount,
			"sumQty":          totalSumQty,
			"lastSeq":         maxLastSeq,
			"lastUpdatedBy":   lastUpdatedBy,
			"instance":        instanceID,
			"sourceBreakdown": sourceBreakdown,
		})
	})
}
