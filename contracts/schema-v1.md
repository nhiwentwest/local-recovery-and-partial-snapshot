# Schema v1

## orders (input)
- key: `orderId` (string)
- value (JSON):
```json
{
  "orderId": "o1",
  "productId": "p1",
  "price": 10000,
  "qty": 1,
  "storeId": "A",
  "ts": 1694500000
}
```

## orders.enriched (output of OpA)
- key: `orderId` (string)
- value (JSON):
```json
{
  "orderId": "o1",
  "productId": "p1",
  "price": 10000,
  "qty": 1,
  "storeId": "A",
  "ts": 1694500000,
  "validated": true,
  "normTs": 1694500000
}
```

Notes:
- `validated`: boolean gate for downstream aggregation
- `normTs`: normalized timestamp used for windowing in OpB

