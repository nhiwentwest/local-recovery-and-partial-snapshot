package model

// Order represents input record for OpA.
type Order struct {
	OrderID   string `json:"orderId"`
	ProductID string `json:"productId"`
	Price     int64  `json:"price"`
	Qty       int64  `json:"qty"`
	StoreID   string `json:"storeId"`
	TS        int64  `json:"ts"`
}

// OrderEnriched is the normalized output from OpA.
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

// Normalize converts Order to OrderEnriched (v1 schema).
func Normalize(o Order) OrderEnriched {
	return OrderEnriched{
		OrderID:   o.OrderID,
		ProductID: o.ProductID,
		Price:     o.Price,
		Qty:       o.Qty,
		StoreID:   o.StoreID,
		TS:        o.TS,
		Validated: true,
		NormTS:    o.TS,
	}
}
