# Task plan

Đây là task plan tạm thời, có thể thay đổi.

Deadline 29/9 -> Chốt kết quả vào ngày 25/9 và làm slide. 

Báo cáo lần 1: họp buổi đầu ngày 19 hoặc ngày 20  - mọi người báo cáo mình đã làm những gì thôi, không cần phải xong hết. Có vấn đề gì thì có thể nhắn trong group hoặc đợi lúc báo cáo cũng được, nhưng mình sẽ xem tiến độ mỗi người ở buổi báo cáo này để điều chỉnh khối lượng công việc cho phù hợp với từng người, đặt mục tiêu để ngày 24/9 mọi người báo cáo lần nữa (lần này qua tin nhắn là okay).

```
├─ README.md
├─ docker-compose.yml
├─ Makefile
├─ contracts/
│  └─ schema-v1.md
├─ cmd/
│  ├─ opa/           # Người 1: OpA (EOS KIP-98)
│  │  └─ main.go
│  ├─ opb/           # Người 2: OpB (agg + changelog + snapshot + recovery + EOS)
│  │  └─ main.go
│  └─ genorders/     # generator mẫu (TÙY CHỌN, chỉ để demo sau khi ghép; GĐ1 mỗi người dùng tool riêng trong nhánh của mình)
│     └─ main.go
├─ internal/
│  ├─ eos/           # Người 1: helper Kafka transactions
│  ├─ model/         # Người 1: struct + normalize cho orders.enriched
│  ├─ state/         # Người 2: KV + key schema + seq/idempotency
│  ├─ snapshot/      # Người 2: backup/restore + materialize
│  ├─ manifest/      # Người 2: publish/read manifest (topic compacted)
│  ├─ recovery/      # Người 3: boot OpB (load manifest -> restore -> replay)
│  └─ metrics/       # Người 3: Prometheus (TTR, lag, applied/skipped)
└─ scripts/
   └─ topics.sh
```

## Ghi chú công nghệ

* Ngôn ngữ: Go 1.21+
* Kafka client: confluent-kafka-go (hỗ trợ transactions và SendOffsetsToTransaction)
* State store: BadgerDB cho OpB
* Metrics: prometheus/client\_golang
---

## 0) Schema chung 


### Topics và key

* `orders` (input thô)
* `orders.enriched` (output của OpA) — key = `orderId`
* `orders.output` (output của OpB) — key = `storeId#productId#windowStart`
* `opb-changelog` (compacted) — key = `storeId#productId#windowStart`
* `opb-snapshots` (manifest; compacted) — key = hằng `opb-manifest-latest`

### Mẫu bản ghi

`orders.enriched`

```json
{ "orderId":"o1","productId":"p1","price":10000,"qty":1,"storeId":"A",
  "ts":1694500000,"validated":true,"normTs":1694500000 }
```

`opb-changelog` (delta)

```json
{ "key":"A#p1#1694500000","seq":12,"delta":10000,"ts":1694500010 }
```

`opb-snapshots` (manifest)

```json
{ "snapshotId":"2025-09-12T10:00:00Z","lastChangelogOffset":7534221,"createdAt":1694499600 }
```

---

## Giai đoạn 1 

Nguyên tắc: mỗi người dùng cụm Kafka riêng hoặc topic prefix riêng (`p1.*`, `p2.*`, `p3.*`) để tránh va chạm; vẫn tuân schema v1.

### Người 1 — exactly-once (KIP-98) cho OpA và một wrapper OpB-output tạm

* Mục tiêu: chứng minh read→process→write transactional; `read_committed` không thấy aborted; không double-count.
* Công việc:

  * OpA (stateless): consume `p1.orders`, normalize, transactional produce `p1.orders.enriched` và gửi offsets vào transaction.
  * Wrapper tạm: consume `p1.orders.enriched`, transactional produce `p1.orders.output` (chưa stateful).
  * Thử crash ở ba thời điểm: trước, giữa, sau commit.
* Minh chứng mong đợi:

  * Mô tả ngắn các trường hợp commit/abort và chênh lệch giữa `read_committed` và `read_uncommitted`.
  * Tổng vào/ra không trùng đếm ở `p1.orders.output` khi có crash.

### Người 2 — partial snapshot cho OpB (stateful + changelog + snapshot) với dữ liệu giả

* Mục tiêu: aggregate stateful, ghi delta vào changelog, snapshot + manifest, replay đúng; chưa cần OpA thật.
* Công việc:

  * Generator xuất `p2.orders.enriched` theo schema v1.
  * OpB: cập nhật state (Badger), append delta vào `p2.opb-changelog`, xuất `p2.orders.output`, định kỳ snapshot và publish manifest lên `p2.opb-snapshots`.
  * Hỗ trợ chế độ on/off changelog để so sánh thời gian và kích thước.
* Minh chứng mong đợi:

  * Một manifest gần nhất (json) kèm đường dẫn snapshot.
  * Vài con số so sánh on/off: `snapshot_time_ms`, `snapshot_bytes`, `replay_bytes`.
  * Giải thích ngắn cơ chế idempotent theo `seq` (ví dụ có bao nhiêu bản ghi trùng bị bỏ qua khi replay).

### Người 3 — local recovery và quan trắc trên artefact tự tạo

* Mục tiêu: khởi động lại từ snapshot + replay changelog; đo time to recover (TTR); lag về 0; chưa cần artefact của người 2.
* Công việc:

  * Tạo seed snapshot/manifest/changelog theo schema v1 (toy OpB hoặc tool nhỏ).
  * Recovery boot: đọc manifest, restore snapshot, replay changelog từ offset, sau đó mới nối vào input.
  * Failure injector (kill -9 ngẫu nhiên) và metrics (TTR, replay rate, consumer lag).
* Minh chứng mong đợi:

  * Ba lần TTR bằng con số, ba dòng log cốt lõi: load manifest, restore ok, replay done (applied/skipped).
  * Nhận xét lag giảm về 0 sau mỗi lần khởi động lại.

---

## Giai đoạn 2 — ghép chung

Từ đây bỏ prefix cá nhân, dùng các topic thật: `orders`, `orders.enriched`, `orders.output`, `opb-changelog`, `opb-snapshots`.

### Bước I1 — OpA thật thay generator của người 2

* Trước I1: người 2 đang tự phát `p2.orders.enriched`.
* Sau I1:

  * Người 1 xuất `orders.enriched` transactional (OpA thật).
  * Người 2 ngừng generator của mình và đổi input sang `orders.enriched` từ người 1.
* Điều chỉnh kỹ thuật:

  * Người 2 cấu hình consumer với `isolation.level=read_committed` và group id chính.
  * Người 1 giữ nguyên transactional flow theo KIP-98.

Tóm tắt thay thế: generator `orders.enriched` (của người 2) → OpA thật (của người 1).

### Bước I2 — OpB thật thay wrapper của người 1 và thay artefact giả của người 3

* Trước I2:

  * Người 1 còn wrapper OpB-output tạm.
  * Người 3 dùng snapshot/manifest/changelog tự tạo để test.
* Sau I2:

  * Người 2 chạy OpB thật: aggregate + changelog + snapshot + manifest, xuất `orders.output`.
  * Người 1 bỏ wrapper, chuyển sang kiểm tra trên `orders.output` do OpB thật tạo.
  * Người 3 chuyển recovery sang artefact thật của người 2 (manifest, snapshot, changelog).
* Điều chỉnh kỹ thuật:

  * Người 1 đổi đích đọc sang `orders.output` và tiếp tục crash matrix ở OpA để kiểm chứng end-to-end.
  * Người 3 trỏ recovery boot vào `opb-snapshots`, `opb-changelog` và snapshot dir thật.

Tóm tắt thay thế:
wrapper `orders.output` tạm (của người 1) → OpB thật (của người 2)
seed snapshot/manifest/changelog giả (của người 3) → artefact thật (của người 2)

### Bước I3 — hoàn thiện end-to-end

* Không có thay thế lớn. Tối ưu cấu hình và đo đạc:

  * Người 1 tinh chỉnh timeout, retry, fencing cho transactional producer/consumer.
  * Người 2 tối ưu cadence snapshot, compaction, và kích thước replay.
  * Người 3 chạy fault-injection dày, tổng hợp bảng TTR, latency p95/p99, snapshot time/bytes, replay bytes.
