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

## orders.output (output of OpB)
- key: `storeId#productId#windowStart` (string)
- value (JSON):
```json
{
  "key": "A#p1#1694500000",
  "sumAmount": 120000,
  "sumQty": 12,
  "windowStart": 1694500000,
  "storeId": "A",
  "productId": "p1",
  "updatedAt": 1761597000
}
```

## opb-changelog (delta for recovery)
- key: `storeId#productId#windowStart` (string)
- value (JSON):
```json
{
  "key": "A#p1#1694500000",
  "seq": 2,
  "delta": 20000,
  "deltaQty": 2,
  "TS": 1761597000
}
```
- Ghi chú: `seq` dùng bảo đảm idempotency khi replay; topic nên `cleanup.policy=compact`.

## opb-snapshots (manifest)
- key: `opb-manifest-latest`
- value (JSON):
```json
{
  "snapshotId": "2025-10-29T07:56:28Z",
  "lastChangelogOffset": 12912,
  "createdAt": "2025-10-29T07:56:30Z"
}
```
- Ghi chú: topic nên `cleanup.policy=compact`.

## Headers (optional, phục vụ đo đạc)
- `t0`: thời điểm gửi ở producer (UnixNano) trên `orders.enriched`.
- `t1`: thời điểm OpB emit (UnixNano) trên `orders.output` và/hoặc `opb-changelog`.
- Cho phép tính latency e2e bằng `t1 - t0` ổn định, không phụ thuộc timing consumer.

## Partitioning & Keys
- Kafka dùng Murmur2; giữ khóa ổn định để đảm bảo thứ tự trong partition.
- `orders.enriched`: khóa thực tế là `orderId`; khi benchmark có thể dùng dạng `storeId#productId#windowStart` để phân tán đều partitions.
- `orders.output`/`opb-changelog`: khóa `storeId#productId#windowStart` để gom theo cửa sổ.

