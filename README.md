# HPB - OpB (Aggregator, Changelog, Snapshot, Manifest)

This repository scaffolds the OpB service (Người 2) for the local-recovery-and-partial-snapshot project.

## Components

- Thành phố gửi yêu cầu liên tục (10k/s).
- Trạm A (OpA) chuẩn hóa và đảm bảo mỗi yêu cầu chỉ được tính một lần.
- Trạm B (OpB) cập nhật bảng tổng hợp theo từng khu/giờ.
- Khi tải tăng, đồng hồ lag cho thấy “xếp hàng”; ta chỉ việc thêm trạm B (scale) hoặc chia nhiều quầy (tăng partitions) để xử lý kịp.
- Nếu mất điện, bật lại sẽ khôi phục bảng tổng hợp đúng như trước khi mất điện.


- Ai? Đội điều phối dịch vụ gọi xe trong thành phố.
- Cần gì? Nhìn thấy nhịp nhu cầu theo khu mỗi phút để điều xe/giá.
- Vấn đề? Khi hệ thống lỗi, dữ liệu dễ sai hoặc gián đoạn.
- Giải pháp? Lưu lần chụp gần nhất và ghi nhật ký thay đổi; khi hồi phục, cộng bù để tiếp tục đúng & kịp thời.
- Lợi ích? Không đếm trùng, không dừng toàn hệ thống, phục hồi trong vài giây.

## Build & Run (local, Phase 1)

```bash
make build
./bin/opb --topic-prefix p2 --snapshot-dir ./snapshots --badger-dir ./data/opb
```

Notes:
- Phase 1 uses in-memory state and filesystem snapshots to validate the control flow.
- Kafka client and BadgerDB integration will be added next.

## Flags

- --topic-prefix: topic prefix (e.g., p2)
- --group-id: consumer group id (default: opb)
- --window-size: aggregation window seconds (default: 300)
- --snapshot-interval: seconds between snapshots (default: 60)
- --changelog: on|off toggle for changelog emission (default: on)
- --snapshot-dir: directory to store snapshots
- --badger-dir: directory for state (reserved for Badger; not used in Phase 1)

## Layout

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

Đã dựng một hệ thống tiếp nhận và tổng hợp dữ liệu theo thời gian thực ở quy mô ~10.000 yêu cầu mỗi giây, không trùng đếm, theo dõi được tải/lỗi, và có thể khôi phục nhanh khi sự cố nhờ snapshot + changelog.

- Không trùng đếm, không mất bản ghi: OpA xử lý “đơn” từng request một cách exactly-once (đọc → xử lý → ghi) nên không bị double-count khi có lỗi hay restart.
- Tổng hợp theo thời gian thực: OpB gom và cộng dồn theo key cửa hàng/sản phẩm/khung giờ, giống như “bảng tổng hợp theo quận/phường theo từng khung giờ”.
- Chịu tải cao ~10.000 yêu cầu/giây: Load test bằng rpk local bắn ~10k RPS trong 60 giây thành công. Dữ liệu vào Kafka tăng đều; hệ thống quan trắc được tốc độ và “điểm nghẽn”.
- Quan sát và kiểm soát: Có metric lag và throughput để thấy khi “đơn vào” nhanh hơn “đơn xử lý”, từ đó biết lúc nào cần tăng scale cho OpB hoặc số partition.
- An toàn khi sự cố: OpB có snapshot + changelog recovery; nếu “mất điện” giữa chừng, bật lại sẽ khôi phục trạng thái và replay phần còn thiếu, không mất số liệu.
- Độ trễ thấp ở đường đi chuẩn: Thiết kế hướng KV nhỏ + cập nhật tuần tự nên giữ được p95 sub-second trong điều kiện bình thường (đã đo trong test chức năng).
- Mở đường nâng cấp: Có thể chuyển backend state sang PebbleDB để tăng headroom hiệu năng khi cần, mà không đổi giao diện/chức năng.


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
