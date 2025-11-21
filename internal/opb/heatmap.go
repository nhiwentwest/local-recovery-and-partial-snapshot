package opb

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"hpb/internal/state"
)

type HeatCell struct {
	StoreID  string `json:"storeId"`
	Value    int64  `json:"value"`
	Instance string `json:"instance,omitempty"`
}

// BuildHeatmap groups state by storeId for a given windowStart.
// If windowStart < 0, aggregates all windows (total mode).
// metric: "sumQty", "sumAmount", or "total". limit<=0 means no explicit cap.
func BuildHeatmap(st state.Store, windowStart int64, prefix string, metric string, limit int) []HeatCell {
	agg := make(map[string]int64)
	_ = st.Range(func(key string, rs state.RecordState) error {
		parts := strings.Split(key, "#")
		if len(parts) != 3 {
			return nil
		}
		if windowStart >= 0 {
			if parts[2] != strconv.FormatInt(windowStart, 10) {
				return nil
			}
		}
		store := parts[0]
		if prefix != "" && !strings.HasPrefix(store, prefix) {
			return nil
		}
		switch metric {
		case "sumAmount":
			agg[store] += rs.SumAmount
		case "total":
			agg[store] += rs.SumQty
		default:
			agg[store] += rs.SumQty
		}
		return nil
	})
	// Build and sort cells by value desc, then storeId desc (stable for ties)
	out := make([]HeatCell, 0, len(agg))
	for s, v := range agg {
		out = append(out, HeatCell{StoreID: s, Value: v})
	}
	// sort
	sort.Slice(out, func(i, j int) bool {
		if out[i].Value == out[j].Value {
			return out[i].StoreID > out[j].StoreID
		}
		return out[i].Value > out[j].Value
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// NewHeatmapHandler returns an http.Handler that serves JSON heatmap data from the state store.
func NewHeatmapHandler(st state.Store, windowSizeSec int, instanceID string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Disable caching to ensure UI/scripts always see latest totals
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		q := r.URL.Query()
		// ws param (optional); default current window
		var ws int64
		prefix := q.Get("prefix")
		metric := q.Get("metric")
		switch metric {
		case "sumQty", "sumAmount", "total":
		default:
			metric = "total"
		}
		if metric != "total" {
			if wsStr := q.Get("ws"); wsStr != "" {
				if v, err := strconv.ParseInt(wsStr, 10, 64); err == nil {
					ws = v
				}
			}
			if ws == 0 {
				now := time.Now().Unix()
				if windowSizeSec <= 0 {
					windowSizeSec = 60
				}
				ws = WindowStart(now, windowSizeSec)
			}
		} else {
			ws = -1
		}
		limit := 500
		if limStr := q.Get("limit"); limStr != "" {
			if v, err := strconv.Atoi(limStr); err == nil {
				limit = v
			}
		}
		if limit <= 0 {
			limit = 500
		}
		if limit > 2000 {
			limit = 2000
		}

		cells := BuildHeatmap(st, ws, prefix, metric, limit)
		// attach instance label
		for i := range cells {
			cells[i].Instance = instanceID
		}
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"ws":       ws,
			"metric":   metric,
			"instance": instanceID,
			"cells":    cells,
		}
		if metric == "total" {
			resp["note"] = "total sum across all windows"
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
}
