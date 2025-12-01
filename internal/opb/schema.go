package opb

import (
	"fmt"
	"time"
)

// OrderSource indicates which logical phase/batch produced a given event.
// It is intentionally a string so that JSON payloads remain readable and
// backwards compatible (unknown/empty values simply mean "legacy/unspecified").
type OrderSource string

const (
	// SourceUnspecified is used for legacy data where the generator did not
	// explicitly tag the origin. UI should treat this as "mixed/unknown".
	SourceUnspecified OrderSource = ""
	// SourceBaseline covers the long-running baseline/backfill segment.
	SourceBaseline OrderSource = "baseline"
	// SourceDelta is for the focused delta batch before the snapshot cut.
	SourceDelta OrderSource = "delta"
	// SourcePostCut marks events that arrived strictly after the cut.
	SourcePostCut OrderSource = "postCut"
	// SourceSeed is reserved for small priming injections (e.g. PRE_CUT_PRIME).
	SourceSeed OrderSource = "seed"
)

// OrderEnriched mirrors schema v1 used by OpA output and by the generator in Phase 1.
type OrderEnriched struct {
	OrderID   string      `json:"orderId"`
	ProductID string      `json:"productId"`
	Price     int64       `json:"price"`
	Qty       int64       `json:"qty"`
	StoreID   string      `json:"storeId"`
	TS        int64       `json:"ts"`
	Validated bool        `json:"validated"`
	NormTS    int64       `json:"normTs"`
	Source    OrderSource `json:"source,omitempty"`
	// Optional ride-like attributes for more realistic demos
	DistanceKm float64 `json:"distanceKm,omitempty"`
	FareBase   int64   `json:"fareBase,omitempty"`
	FarePerKm  int64   `json:"farePerKm,omitempty"`
	Surge      float64 `json:"surge,omitempty"`
	Currency   string  `json:"currency,omitempty"`
}

// OutputKey returns the composite key storeId#productId#windowStart.
func OutputKey(storeID string, productID string, windowStart int64) string {
	return fmt.Sprintf("%s#%s#%d", storeID, productID, windowStart)
}

// WindowStart returns floor(normTs / windowSizeSec) * windowSizeSec.
func WindowStart(normTS int64, windowSizeSec int) int64 {
	if windowSizeSec <= 0 {
		windowSizeSec = 300
	}
	w := int64(windowSizeSec)
	return (normTS / w) * w
}

// OutputRecord represents aggregated state for emission to orders.output.
type OutputRecord struct {
	Key         string `json:"key"`
	SumAmount   int64  `json:"sumAmount"`
	SumQty      int64  `json:"sumQty"`
	WindowStart int64  `json:"windowStart"`
	StoreID     string `json:"storeId"`
	ProductID   string `json:"productId"`
	UpdatedAt   int64  `json:"updatedAt"`
}

// NowUnix returns current time in epoch seconds. Split for testability.
var NowUnix = func() int64 { return time.Now().UTC().Unix() }
