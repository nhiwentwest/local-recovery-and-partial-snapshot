# 1) Cơ sở & mục tiêu

Bối cảnh: Một thành phố được chia thành nhiều khu (Zone A, Zone B…). Mỗi phút, có rất nhiều yêu cầu gọi xe phát sinh: có cuốc được bấm gọi, được ghép tài xế, hoàn tất, hoặc bị huỷ. Hệ thống của chúng tôi giống như bảng điều độ trực tuyến: cứ 60 giây lại tổng hợp cho từng khu xem có bao nhiêu yêu cầu, bao nhiêu ghép thành công, bao nhiêu hoàn tất, bao nhiêu bị huỷ. Nhờ đó, người vận hành biết khu nào đang “nóng” để điều thêm xe hoặc điều chỉnh giá phù hợp. Điểm khác biệt của hệ thống là khi bộ phận “đếm & tổng hợp” gặp sự cố, các phần còn lại vẫn chạy bình thường. Khi bộ phận này bật lại, nó mở sổ ở lần lưu gần nhất rồi cộng bù các thay đổi vừa xảy ra để tiếp tục chính xác, không trùng và không thiếu. Vì thế, bảng điều độ không bị “đứng hình” và số liệu vẫn đúng ngay cả khi có lỗi xảy ra.

Ý tưởng: khi một operator stateful lỗi, chỉ phục hồi operator đó (local recovery), và dùng partial snapshot: thay vì chụp full state, ta log delta thay đổi để khôi phục bằng snapshot + replay. Đây là nội dung chính của paper Local recovery and partial snapshot in distributed stateful stream processing (KIIS, 30/06/2025). ([SpringerLink][1])
Triển khai dựa FLIP-158 (Flink) – state changelog / incremental checkpoints để log mọi thay đổi, và KIP-98 (Kafka) – transactions để bảo đảm read→process→write exactly-once (EOS). ([Apache Software Foundation][2])
Khung dự án: thêm 2 operator vào ApolloFlow (Go) – một task queue có mode Kafka/RabbitMQ, API/gRPC/WS cơ bản. ([GitHub][3])

- Ai? Đội điều phối dịch vụ gọi xe trong thành phố.
- Cần gì? Nhìn thấy nhịp nhu cầu theo khu mỗi phút để điều xe/giá.
- Vấn đề? Khi hệ thống lỗi, dữ liệu dễ sai hoặc gián đoạn.
- Giải pháp? Lưu lần chụp gần nhất và ghi nhật ký thay đổi; khi hồi phục, cộng bù để tiếp tục đúng & kịp thời.
- Lợi ích? Không đếm trùng, không dừng toàn hệ thống, phục hồi trong vài giây.

---

# 2) Kiến trúc tối thiểu (gắn vào ApolloFlow)

```
orders ──▶ OpA (stateless normalize, EOS) ──▶ orders.enriched ──▶ OpB (stateful aggregate, EOS) ──▶ orders.output
                                     └──────────────────────────▶ opb-changelog (compacted)
                                     └──────────────────────────▶ opb-snapshots (manifest; compacted)
```

* **OpA** (stateless): đọc `orders` (consumer `read_committed`), chuẩn hoá, **transactional produce** sang `orders.enriched` và `SendOffsetsToTransaction` (KIP-98). ([Apache Software Foundation][4])
* **OpB** (stateful): state local (Badger/RocksDB). **Mỗi cập nhật state** → **append delta** vào `opb-changelog` (compacted) theo tinh thần **FLIP-158**; định kỳ *materialize* snapshot + ghi **manifest** (chứa `lastChangelogOffset`) vào `opb-snapshots`. Xuất kết quả (EOS) sang `orders.output`. ([Apache Software Foundation][2])
* **Local recovery:** khi OpB crash, **chỉ OpB** restart → load snapshot mới nhất → **replay changelog từ `lastChangelogOffset`** → resume, trong khi OpA không dừng (đúng tinh thần paper). ([SpringerLink][1])

---

# 3) Đưa vào repo ApolloFlow 

ApolloFlow đã có skeleton API/worker & hỗ trợ Kafka → chỉ thêm 2 service trên, tái dùng hạ tầng build/run. ([GitHub][3])

## 3.2. Topics & cấu hình Kafka

```bash
# topics chính
kafka-topics --create --topic orders            --partitions 3 --replication-factor 1
kafka-topics --create --topic orders.enriched   --partitions 3 --replication-factor 1
kafka-topics --create --topic orders.output     --partitions 3 --replication-factor 1

# changelog & manifest (compacted)
kafka-topics --create --topic opb-changelog \
  --partitions 3 --replication-factor 1 \
  --config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.1

kafka-topics --create --topic opb-snapshots \
  --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact
```

## 3.3. OpA (stateless, EOS)

* **Consumer config:** `enable.auto.commit=false`, `isolation.level=read_committed`.
* **Producer config:** `enable.idempotence=true`, `acks=all`, `transactional.id=opA-<env>-<instance>`.
* **Flow:**
  `BeginTransaction()` → transform → `Produce(orders.enriched)` → `SendOffsetsToTransaction()` → `CommitTransaction()`; nếu lỗi: `AbortTransaction()`. (Chuẩn **KIP-98**.) ([Apache Software Foundation][4])

## 3.4. OpB (stateful, changelog + snapshot + EOS)

* **State store:** Badger (Go) – nhẹ & nhanh trên laptop.
* **Bản ghi delta changelog (keyed & idempotent):**

  ```json
  key   = "storeId#productId#windowStart"
  value = { "seq": 12345, "delta": 19900, "ts": 1694500000 }
  ```

  `seq` tăng dần theo key để bỏ qua lặp khi replay. (Tinh thần **FLIP-158**: log mọi thay đổi; snapshot chỉ “kết tinh” định kỳ.) ([Apache Software Foundation][2])
* **Snapshot materializer:** mỗi T giây: freeze/flush Badger → tạo thư mục snapshot (copy hoặc export) → ghi **manifest**:

  ```json
  { "snapshotId": "2025-09-12T10:00:00Z",
    "lastChangelogOffset": 7534221,
    "createdAt": 1694499600 }
  ```

  manifest lưu trên `opb-snapshots` (compacted) để lấy **bản mới nhất** nhanh chóng.
* **Output EOS:** tương tự OpA, nhưng với `transactional.id=opB-...` và `SendOffsetsToTransaction()` cho `orders.enriched`. (Chuẩn **KIP-98**.) ([Apache Software Foundation][4])

## 3.5. Recovery OpB (local)

Khi khởi động:

1. **Tải manifest mới nhất** từ `opb-snapshots` → xác định `lastChangelogOffset`.
2. **Nạp snapshot** Badger tương ứng.
3. **Replay `opb-changelog`** từ `lastChangelogOffset+1` đến “now” (áp dụng theo `seq` để idempotent).
4. **Attach consumer** `orders.enriched` ở group cũ (read\_committed) và chạy tiếp.
   → Chỉ OpB phải làm quy trình này; OpA không dừng (đúng “local recovery” của paper). ([SpringerLink][1])

---

# 4) Mô hình dữ liệu demo

* `orders`: `{orderId, productId, price, qty, storeId, ts}`
* `orders.enriched`: thêm `validated`, `normTs`… (OpA tạo)
* **State OpB**: `(storeId#productId#windowStart) -> {sum, count}` (tumbling window 1 phút)
* `orders.output`: `{storeId, productId, windowStart, sum, count}`

---

# 5) Test & tiêu chí pass

1. **Local recovery:** kill -9 OpB ngẫu nhiên trong khi OpA vẫn chạy; kỳ vọng **TTR** (time-to-recover) nhỏ (ví dụ ≤ 5–10s) tính từ lúc OpB restart đến lúc lại có `orders.output`. (Theo paper, local recovery rút ngắn TTR đáng kể so với global rollback). ([SpringerLink][1])
2. **Partial snapshot vs no-changelog:** bật/tắt ghi delta → so **thời gian snapshot**, **kích thước snapshot**, **bytes replay**. (Ý tưởng FLIP-158/GIC: snapshot nhanh & ổn định nhờ changelog). ([Apache Flink][5])
3. **Exactly-once (KIP-98):** tạo 3 kịch bản crash (trước/giữa/sau commit). Downstream (`read_committed`) **không** thấy bản ghi “nửa vời”; không double-count ở `orders.output`. ([Apache Software Foundation][4])

**Chỉ số tối thiểu báo cáo:** TTR, p50/p95/p99 latency, throughput, snapshot size, bytes replay.

---

[1]: https://link.springer.com/journal/10115/online-first?page=2 "Online first articles | Knowledge and Information Systems"
[2]: https://cwiki.apache.org/confluence/display/FLINK/FLIP-158%3A%2BGeneralized%2Bincremental%2Bcheckpoints "FLIP-158: Generalized incremental checkpoints"
[3]: https://github.com/dattskoushik/apolloflow "GitHub - dattskoushik/apolloflow: This project is a distributed task queue implemented in Go, using RabbitMQ/Kafka for message passing. The system allows clients to submit tasks and receive real-time notifications via WebSockets or gRPC when their tasks have been completed"
[4]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98%2B-%2BExactly%2BOnce%2BDelivery%2Band%2BTransactional%2BMessaging "KIP-98 - Exactly Once Delivery and Transactional Messaging"
[5]: https://flink.apache.org/2022/05/30/improving-speed-and-stability-of-checkpointing-with-generic-log-based-incremental-checkpoints/ "Improving speed and stability of checkpointing with generic ..."
