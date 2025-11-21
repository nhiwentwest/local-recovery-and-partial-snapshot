# Local Recovery & Partial Snapshot — Tổng quan hệ thống (OpA + OpB + hạ tầng)

Dự án mô phỏng một hệ thống giám sát/tổng hợp dữ liệu đơn hàng theo thời gian thực, chứng minh:
- Exactly‑Once (không đếm trùng) ở đường đi chuẩn.
- Scale‑out tuyến OpB khi tải tăng.
- Khả dụng khi bản sao lỗi (rebalance, tiếp quản partitions).
- Phục hồi nhanh nhờ snapshot + changelog (local recovery, partial snapshot).

Thành phần chính
- OpA (Normalizer/EOS): tiêu thụ p1.orders → chuẩn hoá → xuất p1.orders.enriched (Exactly‑Once).
- OpB (Aggregator): tiêu thụ p1.orders.enriched → tổng hợp theo cửa sổ → xuất p1.orders.output; đồng thời ghi changelog, snapshot, manifest lên các topic opb-*.
- Hạ tầng: Kafka/Redpanda, Prometheus/Grafana; web viz: /viz/cluster, /viz/zone-data (có lớp heatmap nếu UI hỗ trợ).

Mục đích: giúp đội vận hành “nhìn thấy” nhịp đơn hàng theo khu/phút, vẫn đúng & liên tục khi có sự cố, và phục hồi trong vài giây.


## 1) Kiến trúc & Luồng (mô tả text)
- OpA: p1.orders → (chuẩn hoá/EOS) → p1.orders.enriched.
- OpB: p1.orders.enriched → (tổng hợp cửa sổ) → p1.orders.output.
  - Đồng thời xuất:
    - p1.opb-changelog (delta, append‑only)
    - p1.opb-snapshots (compacted, manifest snapshot mới nhất)
    - p1.opb-store-touch (compacted, dấu vết phiên bản/instance dùng cho viz)
- Quan trắc:
  - HTTP metrics: /metrics (Prom/Graf)
  - Web viz: /viz/cluster, /viz/zone-data?id=... (có heatmap nếu bật)

Topics mặc định (prefix p1.)
- p1.orders, p1.orders.enriched, p1.orders.output
- p1.opb-changelog, p1.opb-snapshots (compacted), p1.opb-store-touch (compacted)


## 2) Quickstart (local)
Prerequisites
- Kafka/Redpanda tại 127.0.0.1:9092; cổng HTTP rảnh: :8088 (OpA), :8089 (OpB)
- Go toolchain + make

Build
- make build

Chạy tối thiểu hạ tầng + pipeline
- scripts/run_infra.sh (khởi động broker, Prom, Grafana, web viz nếu có trong repo)
- scripts/run_opa.sh (OpA — chuẩn hoá, Exactly‑Once)
- scripts/run_opb.sh (OpB — tổng hợp + snapshot/changelog)
- scripts/start_pipeline.sh (bơm dữ liệu mẫu nếu cần)

Mở giao diện web
- Cluster overview: http://127.0.0.1:8089/viz/cluster
- Zone data (theo store): http://127.0.0.1:8089/viz/zone-data?id=STORE_PREFIX

Ghi chú
- Các topic opb-* có mục tiêu compacted/append như trên. Nếu cần tạo thủ công bằng rpk/kafka-topics, hãy dùng prefix p1.* và số partitions phù hợp với demo.


## 3) Demos (bám script — mỗi demo: Mục tiêu → Cách chạy → Verify → Links)

### Chuẩn bị trước mỗi demo
Để tránh xung đột giữa các kịch bản (đặc biệt khi OpB có replica B2 đang chạy), hãy làm sạch môi trường trước MỖI demo:

1. Dừng các tiến trình đang chạy  
   `pkill -f bin/opb || true`  
   `pkill -f bin/opa || true`  
   `pkill -f ':8090' || true` # đảm bảo OpB2 không giữ partition
2. Xoá và tạo lại topic sạch:  
   `PREFIX=p1 bash scripts/run_infra.sh`
3. Khởi động pipeline nền với Pebble + window 120s:  
   `STATE_BACKEND=pebble WINDOW_SIZE=120 bash scripts/start_pipeline.sh`
4. Đợi `http://127.0.0.1:8088/healthz` và `http://127.0.0.1:8089/healthz` trả `{"status":"ok"}` rồi mới chạy demo.

Lưu ý: `scripts/demo_suite.sh` sẽ tự khởi động B2 (port 8090) trong pha scale-out; sau khi demo kết thúc hãy dừng B2 (`pkill -f ':8090'`) trước khi chuyển sang demo khác.

### Demo 1 — Exactly‑Once (EOS)
Mục tiêu
- Không double‑count khi bơm bản ghi trùng (DUP) vào đường đi chuẩn.

Cách chạy (headless)
- INTERACTIVE=0 DEMO_ONLY=EOS bash scripts/demo_suite.sh

Verify
- sumQty giữ nguyên sau pha DUP (không tăng lần 2).
- Metrics opb_events_skipped_dedup_total tăng tương ứng số DUP.

Links
- /viz/cluster, /viz/zone-data?id=EOS-TEST-D-


### Demo 2 — Scale‑out (local)
Mục tiêu
- Tăng replica/số partitions để giảm lag, tăng throughput.

Cách chạy (tuỳ chọn A — 1 giai đoạn)
- INTERACTIVE=0 AUTO_Y=1 STORE=EOS-TEST-D- bash scripts/demo_scaleout.sh

Cách chạy (tuỳ chọn B — 2 giai đoạn, có auto reset)
- INTERACTIVE=0 AUTO_Y=1 \
  RESET_AFTER_SEC=60 RESET_PARTS=4 RESET_MODE=delete_recreate \
  STORE=EOS-TEST-D- bash scripts/demo_scaleout_2stage.sh

Verify
- Lag giảm, throughput tăng sau khi thêm replica/tăng partitions.
- Ngay sau khi join group, LagTotal có thể >0 một thời gian rồi về 0 khi tiêu thụ xong.

Links
- /viz/cluster, /viz/zone-data?id=STORE


### Demo 3 — Availability & Headroom (local)
Mục tiêu
- Một replica OpB bị kill tạm thời, hệ thống vẫn xử lý nhờ rebalance; sau đó replica trở lại và join nhóm.

Cách chạy (headless)
- INTERACTIVE=0 AUTO_Y=1 bash scripts/demo_availability_local.sh

Cách chạy (clean backlog + drain lag trước khi gây lỗi)
- INTERACTIVE=0 AUTO_Y=1 CLEAN_TOPICS=1 CLEAN_LAG=1 LAG_THRESH=0 LAG_TIMEOUT=180 bash scripts/demo_availability_local.sh
  - CLEAN_TOPICS=1: xoá và tạo lại topics demo (enriched/output = 4 partitions; các topic compacted giữ cleanup.policy=compact)
  - CLEAN_LAG=1: đợi tổng lag trên cụm về ≤ LAG_THRESH (mặc định 0) trong tối đa LAG_TIMEOUT giây trước khi bắt đầu bơm tải/gây lỗi

Verify
- Khi B2 down: B1/B3 tiếp quản partitions của B2, hệ thống vẫn xử lý.
- Khi B2 phục hồi: partitions phân phối lại đều. Quan sát được trên /viz/cluster.

Links
- /viz/cluster


### Demo 4 — Recovery (local)
Mục tiêu
- Khởi động lại OpB, phục hồi nhanh nhờ manifest snapshot + replay changelog, và chứng minh thời gian khôi phục (TTR) dưới 10 giây sau tối ưu.

Cách chạy (headless)
- bash scripts/demo_recovery.sh
  - Script tự động:
    - Xoá rồi tạo lại hai topic phục hồi (`p1.opb-snapshots`, `p1.opb-changelog`) để tránh backlog từ các lần demo trước (cần CLI `kafka-topics`).
    - Bơm 1 000 bản ghi ban đầu → đợi manifest mới xuất hiện (poll log thay vì ngủ cố định).
    - Bơm thêm 500 bản ghi delta, chờ Exact cập nhật đủ lastSeq.
    - `kill -9` OpB, chạy lại hai giai đoạn: `--restore-on-start --restore-only` (foreground) rồi tiến trình thường.
    - Warmup + verify, sau cùng giữ tiến trình chạy đến khi nhấn Enter (INTERACTIVE=1) hoặc ngủ theo `SLEEP_BEFORE_SHUTDOWN`.

Verify
- Log sẽ in rõ thời điểm bắt đầu/hoàn tất restore (`restore ts: start=…`, `restore ts: done=…`) cùng `restore completed: applied=500 skipped=3` (demo mặc định).
- `/status` và file `data/opb-recovery/restore-metrics.json` phản ánh `ttrMs ≈ 9000`, `snapshotId`, `lastChangelogOffset=1200`, `lastRestoreApplied=500`, `lastRestoreSkipped=3`.
- `/viz/zone-data?id=RECOVERY-TEST&productId=p1&ws=<ws>`: `sumQty(after)=1501` (500 delta + 1 warmup) và `lastSeq(after) >= 1500`.
- Nếu cần so khớp offset, dùng `bin/count_changelog -topic p1.opb-changelog` sau khi script chạy xong.

Links
- /viz/cluster, /viz/zone-data?id=RECOVERY-TEST


## 4) HEATMAP (bổ sung)
Mục đích
- Quan sát phân bố “điểm nóng” theo zone/key để thấy tải/độ tập trung theo thời gian.

Cách xem
- Mở http://127.0.0.1:8089/viz/zone-data?id=STORE_PREFIX
- Nếu UI hỗ trợ heatmap, bật lớp heatmap trong trang zone‑data (layer/toggle). Khi bơm tải dàn trải, heatmap sẽ đều; khi dồn vào một số zone/key, khu vực đó sáng đậm hơn.


## 5) Flags & Topics (chuẩn hoá theo THÀNH PHẦN)

OpA (theo scripts/run_opa.sh)
- -bootstrap: địa chỉ bootstrap (vd 127.0.0.1:9092)
- -group-id: consumer group OpA
- -topic-in: p1.orders
- -topic-out: p1.orders.enriched
- -tx-id (transactional.id): bật EOS khi ghi ra enriched
- -http: địa chỉ HTTP (vd :8088)
- (tuỳ chọn test) -crash-mode: before|mid|after

OpB (theo scripts/run_opb.sh)
- --state-backend: memory|pebble (mặc định pebble)
- --state-dir: thư mục state (vd ./data/opb)
- --snapshot-dir: nơi lưu snapshot (vd ./snapshots)
- --kafka-bootstrap: bootstrap servers
- --group-id: consumer group OpB
- --input-source: kafka
- --topic-enriched: input (p1.orders.enriched)
- --output-topic: output (p1.orders.output)
- --changelog-sink: none|kafka|fs|both
- --manifest-sink: kafka|fs|both
- --topic-changelog: p1.opb-changelog
- --topic-snapshots: p1.opb-snapshots
- --window-size: giây cho cửa sổ gom
- --snapshot-interval: chu kỳ snapshot
- --tx-batch-size, --tx-linger-ms: tinh chỉnh giao dịch/ghi
- --http: địa chỉ HTTP (vd :8089)
- (tuỳ chọn) --output-tx-id nếu binary hỗ trợ

Shared
- Kafka bootstrap: 127.0.0.1:9092
- Topics chỉ dùng prefix p1.* trong README (KHÔNG dùng p2.*)
- Partitions: chọn theo demo (4, 8, ...)


## 6) Phụ lục & Liên kết
- KIP-98: Exactly‑Once & Transactional Messaging
- KIP-429: Cooperative Rebalancing; KIP-345: Static Membership
- FLIP-158: Generalized incremental checkpoints (định hướng log‑based snapshot)

Trích đoạn manifest (mẫu)
```
{ "snapshotId": "2025-09-12T10:00:00Z",
  "lastChangelogOffset": 7534221,
  "createdAt": 1694499600 }
```


## 7) Ghi chú phạm vi & đồng bộ
- README này chỉ mô tả, không thay đổi code/scripts.
- Ngôn ngữ: Tiếng Việt; giữ thuật ngữ kỹ thuật tiếng Anh khi cần.
- Demos, topics, flags đã được chuẩn hoá về p1.* và bám đúng tên script:
  - run_infra.sh, run_opa.sh, run_opb.sh, start_pipeline.sh
  - demo_suite.sh (DEMO_ONLY=EOS)
  - demo_scaleout.sh, demo_scaleout_2stage.sh
  - demo_availability_local.sh
  - demo_recovery.sh
- Không thêm mục Troubleshooting trong lần này.
