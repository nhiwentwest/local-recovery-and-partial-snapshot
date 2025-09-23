package opb

import (
	"fmt"
	"time"
)

// OrderEnriched mirrors schema v1 used by OpA output and by the generator in Phase 1.
type OrderEnriched struct {
	OrderID   string `json:"orderId"`
	ProductID string `json:"productId"`
	Price     int64  `json:"price"`
	Qty       int64  `json:"qty"`
	StoreID   string `json:"storeId"`
	TS        int64  `json:"ts"`
	Validated bool   `json:"validated"`
	NormTS    int64  `json:"normTs"`
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
