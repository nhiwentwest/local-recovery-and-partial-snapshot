# Local Recovery & Partial Snapshot — Tổng quan hệ thống (OpA + OpB + hạ tầng)

Dự án mô phỏng một hệ thống giám sát/tổng hợp dữ liệu đơn hàng theo thời gian thực, chứng minh:
- Exactly‑Once (không đếm trùng) ở đường đi chuẩn.
- Scale‑out tuyến OpB khi tải tăng.
- Khả dụng khi bản sao lỗi (rebalance, tiếp quản partitions).
- Phục hồi nhanh nhờ snapshot + changelog (local recovery, partial snapshot).

Thành phần chính
- OpA (Normalizer/EOS): tiêu thụ p1.orders → chuẩn hoá → xuất p1.orders.enriched (Exactly‑Once).
- OpB (Aggregator): tiêu thụ p1.orders.enriched → tổng hợp theo cửa sổ → xuất p1.orders.output; đồng thời ghi changelog, snapshot, manifest lên các topic opb-*.
- Hạ tầng: Kafka, Prometheus/Grafana; web viz: /viz/cluster, /viz/zone-data (có lớp heatmap nếu UI hỗ trợ).

Mục đích: giúp đội vận hành “nhìn thấy” nhịp đơn hàng theo khu/phút, vẫn đúng & liên tục khi có sự cố, và phục hồi trong vài giây.


## 1) Kiến trúc & Luồng (mô tả text)
- OpA: p1.orders → (chuẩn hoá/EOS) → p1.orders.enriched.
- OpB: p1.orders.enriched → (tổng hợp cửa sổ) → p1.orders.output.
  - Đồng thời xuất:
    - p1.opb-changelog (delta, append‑only)
    - p1.opb-snapshots (compacted, manifest snapshot mới nhất)
- Quan trắc:
  - HTTP metrics: /metrics (Prom/Graf)
  - Web viz: /viz/cluster, /viz/zone-data?id=... (có heatmap nếu bật)

Topics mặc định (prefix p1.)
- p1.orders, p1.orders.enriched, p1.orders.output
- p1.opb-changelog, p1.opb-snapshots (compacted)


## 2) Quickstart (local)
Prerequisites
- Kafka tại 127.0.0.1:9092; cổng HTTP rảnh: :8088 (OpA), :8089 (OpB)
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
    - Xoá rồi tạo lại hai topic phục hồi (`p1.opb-snapshots`, `p1.opb-changelog`) để tránh backlog từ các lần demo trước (cần CLI `kadmin`).
    - Bơm baseline + delta theo cấu hình, chụp snapshot bằng barrier-cut, có thể tạo causal inflight.
    - `kill -9` OpB, chạy lại hai giai đoạn: `--restore-on-start --restore-only` (foreground) rồi tiến trình thường.
    - Warmup + verify, in `/status` và các checkpoint heatmap/exact.

Verify
- Log sẽ in rõ thời điểm bắt đầu/hoàn tất restore (`restore ts: start=…`, `restore ts: done=…`), `restore completed: applied=… skipped=…`.
- `/status` phản ánh `ttrMs`, `snapshotId`, `lastChangelogOffset`, `lastRestoreApplied/Skipped`, `causalReplayTotal`, `causalInflight`.
- `/viz/zone-data?id=RECOVERY-TEST&productId=p1&ws=<ws>`: so sánh sumQty/lastSeq trước–sau crash.

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

OpB (theo scripts/run_opb.sh và demo*\*)
- `--kafka-bootstrap`: bootstrap servers
- `--group-id`, `--instance-id`: nhận diện consumer & replica
- `--state-backend` (memory|pebble), `--state-dir`: nơi lưu state
- `--snapshot-dir`, `--snapshot-format` (json|msgpack), `--snapshot-shards`: cấu hình snapshot
- `--snapshot-interval`, `--window-size`: thông số thời gian
- `--input-source` (sample|kafka), `--topic-enriched`, `--output-topic`
- `--changelog-sink` (file|kafka|both|none), `--manifest-sink` (file|kafka|both)
- `--changelog-source`, `--manifest-source` (file|kafka) + `--changelog-dir` khi dùng file-mode
- `--topic-changelog`, `--topic-snapshots`
- `--tx-batch-size`, `--tx-linger-ms`: tinh chỉnh transactional batching
- `--peers`: danh sách HTTP peer (dạng `http://host:port`, lấy từ OPB_PEERS)
- `--session-timeout-ms`, `--heartbeat-interval-ms`: tuning consumer group (demo HA)
- `--restore-on-start`, `--restore-only`: điều khiển restore khi khởi động
- `--http`: địa chỉ HTTP (vd :8089)

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


## 8) Đo TTR với Barrier-based Non-blocking Snapshot
Mục tiêu
- Đo chính xác thời gian khôi phục (Time-To-Recover) của OpB khi khởi động lại, dựa trên snapshot + replay changelog.
- Làm rõ hai góc nhìn đo: “nội bộ” (in-app) và “ngoại vi” (wall‑clock), đồng thời tránh hiểu nhầm “bypass” khi không có backlog.

Khái niệm đo
- In-app (nội bộ):
  - Trường `ttrMs` trên `/status` đo phần cốt lõi: restore snapshot + replay changelog (nếu có backlog), đến khi hoàn tất khôi phục state.
  - Log “restore phases” (ManifestMs, SnapshotTotalMs, ChangelogMs, MetricsMs, TotalMs) giúp soi chi tiết từng pha; `TotalMs` là tổng thời gian khối restore (bao gồm ManifestMs).
- Wall-clock (ngoại vi):
  - `scripts/measure_ttr.sh` đo thời gian từ khi chạy `opb --restore-only` đến khi tiến trình thoát. Bao gồm overhead start/stop tiến trình + IO log/metrics → gần với trải nghiệm restart thực tế.

Barrier-based Non-blocking snapshot là gì?
- Khi gọi `/admin/snapshot-cut`, OpB inject “barrier” vào tất cả partitions của topic input, đợi thấy barrier trên từng partition được assign, chụp snapshot qua Pebble SnapshotView (không chặn writer), rồi ghi `manifest` kèm offsets per‑partition của changelog.
- Khôi phục: đọc `manifest.changelog.offsets` và chỉ replay phần “sau” snapshot.
- Nếu ngay sau cut chưa có backlog (watermarks ≈ offsets), phần replay ≈ 0 → TTR nhỏ là hợp lý theo thiết kế (không phải bypass).

Cách chạy benchmark TTR
- Mặc định non‑blocking snapshot dùng `scripts/measure_ttr.sh`:
  ```bash
  BOOTSTRAP=127.0.0.1:9092 \
  HTTP_ADDR=:8089 RESTORE_HTTP_ADDR=:8099 \
  SNAPSHOT_DIR=./snapshots \
  STATE_DIR=./data/opb RESTORE_STATE_DIR=./data/opb-restore-only \
  PUMP_AFTER_CUT=20000 \
  bash scripts/measure_ttr.sh
  ```
  - Script sẽ:
    1) Gọi `/admin/snapshot-cut` → chờ `manifest` có `.changelog.offsets[]` (barrier ready).
    2) “Pin” manifest để giữ mốc đo.
    3) (Tuỳ chọn) Bơm thêm tải sau cut (`PUMP_AFTER_CUT`) → đợi backlog hình thành dựa trên watermarks vs offsets.
    4) Chạy `opb --restore-only` và in thời gian wall‑clock; đồng thời đọc “restore phases” trong log.
- Ép có replay rõ ràng (tuỳ chọn):
  - Đặt `STRIP_OFFSETS=1` để xoá `.changelog` khỏi manifest pinned (replay từ đầu hoặc từ `lastChangelogOffset`), hoặc tăng `PUMP_AFTER_CUT` + `WAIT_BACKLOG_SEC`.

Báo cáo nên công bố 2 con số
- TTR‑snapshot‑only: khi không có backlog (hoặc deliberately strip offsets để tách riêng snapshot). Dựa trên `ttrMs` và `SnapshotTotalMs`/`TotalMs`; kèm wall‑clock.
- TTR‑snapshot+replay(N): tạo backlog cỡ N; xác nhận wm.high − manifest.offsets ≥ N trước restore. Báo `ChangelogMs`, `TotalMs`, `ttrMs` và wall‑clock, kèm số bản ghi áp dụng/skipped để đối chiếu.

Lưu ý để phép đo ổn định
- Chạy nhiều lần và lấy p50/p95 (hoặc min) để giảm nhiễu IO/GC.
- Cố định môi trường: `--snapshot-interval 0` cho tiến trình ingest, dùng `RESTORE_STATE_DIR` riêng, tránh job nền.
- Với Kafka: tăng `WAIT_BACKLOG_SEC` nếu bơm lớn sau cut; xác thực backlog qua watermarks.

Thông điệp quan trọng khi trình bày
- TTR nhỏ khi không có backlog sau barrier là mục tiêu của kỹ thuật (không phải bypass). Để công bằng, luôn bổ sung kịch bản có backlog và công bố `ChangelogMs`/`applied`.


## 9) Kỹ thuật Recovery & Snapshot (nâng cao) — trạng thái hiện tại
- Barrier‑based Non‑blocking Snapshot (đã có): manifest chứa offsets per‑partition; cut không chặn writer nhờ SnapshotView + barrier marker trên từng partition.
- Incremental Snapshots (đã có):
  - Chính sách auto full|delta qua `--snap-max-deltas`, `--snap-max-delta-mb`.
  - Dirty‑key tracking bằng Kafka scan giữa manifest.prev.offsets → offsets hiện tại để chỉ snapshot phần thay đổi.
  - Manifest chain: `snapshotType=delta`, `baseSnapshotId`, `parentSnapshotId`, `deltaSequence`.
- Beaver‑style Causal Snapshot (đã có):
  - Ghi channel‑state (inflight) trong giai đoạn barrier propagation; file `inflight.json` được tham chiếu bởi `manifest.inflightFile` và `inflightEvents`.
  - Khôi phục theo thứ tự: Restore snapshot → Replay inflight (nếu có) → Replay changelog Kafka nếu còn backlog beyond manifest offsets (có thể skip hoàn toàn).
  - Web: `/viz/zone-data` hiển thị “Live Causal Cut” (id/phase/markers/inflight).
- Skip Kafka replay khi không có backlog (đã có): kiểm tra watermark vs `manifest.changelog.offsets` để quyết định bỏ qua `ReplayChangelogKafkaParallel`.
- Peer‑assisted State Migration (đã có bản đơn giản):
  - B2 có thể import state từ B1 qua `/admin/state/export` (NDJSON) khi rebalance; tạm thời best‑effort cho LAN demo.
- Snapshot GC/Retention (đã có): `/admin/snapshot-gc` + cờ `--snap-retention-*` để duy trì dung lượng.


## 10) Admin/API/Web — quick reference
Admin
- POST `/admin/snapshot-cut?type=full|delta|auto`
- POST `/admin/ingest/pause` ; POST `/admin/ingest/resume`
- POST `/admin/snapshot-gc`
- POST `/admin/prune-state` (body: storeId/productId/windowStartBefore/limit/dryRun)
- GET  `/admin/state/export` (NDJSON {key,state})

Data/Diag
- POST `/api/inject-test-data`
- GET  `/api/zone-details?id=STORE[&productId&ws]`
- GET  `/api/debug-store-keys?storeId=STORE`
- GET  `/api/exact?storeId&productId&ws`
- GET  `/api/cluster`

Observability
- GET `/status` (ttrMs, restoringSnapshotId, lastChangelogOffset, lastRestoreApplied/Skipped, causalReplayTotal, causalInflight, partitions, lagTotal, …)
- GET `/metrics` (Prometheus)
- Web: `/viz/cluster` (Instances/Assignment + Recovery summary), `/viz/zone-data` (Store total + Exact + Live Causal Cut), `/viz/heatmap`


## 11) Dọn repo & đẩy lên Git
Các thư mục sau là dữ liệu sinh ra trong lúc chạy demo (KHÔNG nên commit):
- `data/`, `logs/`, `snapshots*/`, `changelog*/`, `bin/`

`.gitignore` mẫu đã thêm trong repo:
```gitignore
# Build outputs
bin/

# Runtime state / generated data
data/
logs/

# Snapshots & changelogs (generated)
snapshots/
snapshots-*/
snapshots-recovery/
changelog/
changelog-*/
changelog-recovery/

# Restore metrics artifacts
**/restore-metrics.json

# OS/editor junk
.DS_Store
*.swp
*.swo
.idea/
.vscode/
```

Dọn & untrack nếu lỡ commit
```bash
# Xoá file/thư mục sinh ra tại local
rm -rf snapshots* changelog* data logs bin || true

# Bỏ theo dõi nếu đã từng commit các thư mục này
git rm -r --cached snapshots* changelog* data logs bin 2>/dev/null || true

# Stage & commit thay đổi .gitignore/README
git add .gitignore README.md
git add -A
git commit -m "chore: add .gitignore; docs: update recovery/snapshot techniques; clean generated state"

# Push
# Thay <branch> bằng nhánh của bạn (main/master/dev)
git push origin <branch>
```

Gợi ý: Các script demo sẽ tự tạo lại thư mục khi chạy nên việc xoá là an toàn. Nếu cần giữ mẫu nhỏ cho báo cáo, hãy lưu dưới `docs/examples/` (không dùng cho runtime).
