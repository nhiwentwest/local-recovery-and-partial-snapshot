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
- Kafka tại 127.0.0.1:9092 (Homebrew: `brew services start kafka`; kiểm tra `brew services list | grep kafka`)
- Go toolchain + make (build `bin/opb`, `bin/opbtool`, `bin/kadmin`)
- Kafka tại 127.0.0.1:9092 (Homebrew: `brew services start kafka`; kiểm tra `brew services list | grep kafka`)
- Go toolchain + make (build `bin/opb`, `bin/opbtool`, `bin/kadmin`)

Build
- make build

Chạy nhanh demo **Local Recovery & Causal Freeze**
- Khởi động Prometheus với cấu hình trong repo:
  ```bash
  cd hpb
  prometheus --config.file=./prometheus.yml --web.listen-address=:9090
  ```
- Chạy demo recovery (headless, tự reset topics/state, crash+restore B3):
  ```bash
  cd hpb
  AUTO_Y=1 INTERACTIVE=0 SCENARIO=freeze CAUSAL_FREEZE_MODE=1 ./scripts/demo_recovery.sh
  ```
- Mở viz: `http://127.0.0.1:8089/viz/`  
  - Ô “Prometheus URL”: nhập `http://127.0.0.1:9090` → panel *Causal inflight (last 5m)* & *Last Restore Summary* sẽ có dữ liệu.
Chạy nhanh demo **Local Recovery & Causal Freeze**
- Khởi động Prometheus với cấu hình trong repo:
  ```bash
  cd hpb
  prometheus --config.file=./prometheus.yml --web.listen-address=:9090
  ```
- Chạy demo recovery (headless, tự reset topics/state, crash+restore B3):
  ```bash
  cd hpb
  AUTO_Y=1 INTERACTIVE=0 SCENARIO=freeze CAUSAL_FREEZE_MODE=1 ./scripts/demo_recovery.sh
  ```
- Mở viz: `http://127.0.0.1:8089/viz/`  
  - Ô “Prometheus URL”: nhập `http://127.0.0.1:9090` → panel *Causal inflight (last 5m)* & *Last Restore Summary* sẽ có dữ liệu.

Chạy hạ tầng/pipeline (tuỳ chọn)
- scripts/run_infra.sh (Kafka, Prom, Grafana nếu cần)
- scripts/run_opa.sh (OpA)
- scripts/run_opb.sh (OpB)
- scripts/start_pipeline.sh (bơm dữ liệu mẫu)
Chạy hạ tầng/pipeline (tuỳ chọn)
- scripts/run_infra.sh (Kafka, Prom, Grafana nếu cần)
- scripts/run_opa.sh (OpA)
- scripts/run_opb.sh (OpB)
- scripts/start_pipeline.sh (bơm dữ liệu mẫu)

Mở giao diện web
- Cluster overview: http://127.0.0.1:8089/viz/cluster
- Zone data: http://127.0.0.1:8089/viz/zone-data?id=STORE_PREFIX
- Zone data: http://127.0.0.1:8089/viz/zone-data?id=STORE_PREFIX

Ghi chú
- Topics opb-* dùng prefix `p1.*`, changelog `cleanup.policy=delete`, snapshots `cleanup.policy=compact`.


## 3) Demo Recovery (local)

### Demo — Recovery (local)
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
- Script tự check `verify_pebble_manifest/restore/atomic_import`; có thể bổ sung `opbtool inspect snapshots/<SNAPSHOT_ID>` để xem danh sách SSTable, incremental files, checksum.

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
- `--state-backend` (pebble-only), `--state-dir`: nơi lưu state
- `--snapshot-dir`, `--snapshot-shards`: cấu hình snapshot Pebble (full + incremental)
- `--snapshot-interval`, `--window-size`: thông số thời gian
- `--input-source` (sample|kafka), `--topic-enriched`, `--output-topic`
- `--changelog-sink` (file|kafka|both|none), `--manifest-sink` (file|kafka|both)
- `--changelog-source`, `--manifest-source` (file|kafka) + `--changelog-dir` khi dùng file-mode
- `--topic-changelog`, `--topic-snapshots`
- `--tx-batch-size`, `--tx-linger-ms`: tinh chỉnh transactional batching
- `--enable-pebble-phase3`: bật incremental SSTable shipping (mặc định phase 2)
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
Đây là phiên bản thu gọn nhưng vẫn giữ đầy đủ chi tiết kỹ thuật:

---

## 7) Kiến trúc thư mục project

### Cấu trúc tổng quan
```
hpb/
├── cmd/                    # Các ứng dụng chính (entry points)
├── internal/              # Packages nội bộ (core logic)
├── scripts/               # Scripts demo và setup
├── web/                   # Web UI (visualization)
└── tools/                 # Công cụ tiện ích
```

### cmd/ — Entry points & Applications
**Trọng tâm:**
- **`cmd/opb/`** — OpB Aggregator (ứng dụng chính)
  - `main.go` — Entry point, orchestration, HTTP server setup
  - `http_handlers.go` — Tất cả HTTP endpoints (admin, API, visualization)
  - `restore_handler.go` — Logic restore từ snapshot + changelog
  - `restore_helpers.go` — Helper functions cho snapshot/manifest
  - `causal_inflight.go` — Causal inflight event tracking & replay
  - `multi_runtime.go` — Multi-instance runtime cho HA/rebalance
  - `mi_*.go` — Multi-instance consumers/producers/operators
- **`cmd/opa/`** — OpA Normalizer (EOS producer)
- **`cmd/opbtool/`** — CLI tool để inspect snapshots, verify state
- **`cmd/kadmin/`** — Kafka admin utilities

### internal/ — Core packages

#### internal/opb/ — OpB core logic
**Trọng tâm:**
- **`operator_n.go`** — N-input operator với barrier-based snapshot (Chandy-Lamport)
- **`aggregate.go`** — Windowed aggregation logic, idempotent apply
- **`snapcut/`** — Snapshot cut orchestration (barrier injection, manifest generation)
- **`tx.go`** — Transactional logic, vector clock, epoch fencing
- **`zone.go`** — Zone/store state management
- **`heatmap.go`** — Heatmap calculation cho visualization
- **`vector_clock.go`** — Vector clock implementation cho causal ordering

#### internal/state/ — State management
**Trọng tâm:**
- **`pebble_store.go`** — PebbleDB-backed state store
  - `ExportDeltaSSTables()` — Export dirty keys thành external SSTable (zero seqnum)
  - `IngestDeltaSSTables()` — Ingest external SSTable vào Pebble
  - `ExportIncrementalSSTables()` — Phase 3 incremental checkpoint
- **`state.go`** — State interface và implementations

#### internal/snapshot/ — Snapshot generation
**Trọng tâm:**
- **`snapshot.go`** — Snapshot writer (Pebble format, JSON format)
- **`pebble_snapshotter.go`** — Pebble-specific snapshot logic
- **`gc.go`** — Snapshot garbage collection (ref-count based)

#### internal/restore/ — Restore & recovery
**Trọng tâm:**
- **`restore.go`** — Main restore pipeline
  - `RestoreFromSnapshotWithFormat()` — Restore từ snapshot chain
  - `ReplayChangelog()` — Replay changelog từ Kafka/file
- **`restore_pebble_test.go`** — Tests cho Pebble restore
- **`restore_ttr_test.go`** — TTR measurement tests

#### internal/manifest/ — Manifest management
**Trọng tâm:**
- **`manifest.go`** — Manifest schema và serialization
  - Chứa metadata: `PebbleSSTFiles`, `PebbleIncrementalFiles`, `InflightFile`, offsets per partition

#### internal/changelog/ — Changelog handling
**Trọng tâm:**
- **`changelog.go`** — Changelog writer/reader (file-based, Kafka-based)

#### internal/restorefs/ & internal/restorekafka/
- **`restorefs/`** — File-based restore implementation
- **`restorekafka/`** — Kafka-based restore implementation

### scripts/ — Demo & setup scripts
**Trọng tâm:**
- **`demo_recovery.sh`** — Demo recovery scenario (crash + restore)
- **`run_opb.sh`** — Script khởi động OpB với cấu hình mặc định
- **`run_opa.sh`** — Script khởi động OpA
- **`start_pipeline.sh`** — Bơm dữ liệu mẫu vào pipeline

### web/viz/ — Web visualization
- HTML/JS/CSS cho cluster overview, zone data, heatmap visualization

### Luồng dữ liệu chính (theo code)
1. **Ingest**: `cmd/opb/main.go` → `internal/opb/operator_n.go` → `internal/opb/aggregate.go` → `internal/state/pebble_store.go`
2. **Snapshot**: `cmd/opb/main.go` (HTTP handler) → `internal/opb/snapcut/` → `internal/snapshot/` → `internal/state/pebble_store.go` (export SSTable)
3. **Restore**: `cmd/opb/restore_handler.go` → `internal/restore/restore.go` → `internal/state/pebble_store.go` (import SSTable) → `cmd/opb/causal_inflight.go` (replay inflight) → `internal/restore/restore.go` (replay changelog)

### Điểm quan trọng
- **Bắt đầu từ**: `cmd/opb/main.go` để hiểu entry point và orchestration
- **State management**: `internal/state/pebble_store.go` — nơi state được lưu và export/import
- **Snapshot logic**: `internal/opb/snapcut/` + `internal/snapshot/` — barrier cut và SSTable generation
- **Restore logic**: `cmd/opb/restore_handler.go` + `internal/restore/restore.go` — pipeline khôi phục
- **Tests**: Mỗi package có `*_test.go` files để hiểu behavior và edge cases

---

## 8) Kỹ thuật chính & Liên hệ KIP / FLIP / EOS

- **Bundle 2: EOS & Idempotent Replay (KIP-98 / KIP-447)**  
  OpA dùng transactional producer, OpB dedup theo `eventID=orderId#ws`, track `LastSeq` và epoch fencing → mỗi event chỉ cập nhật state một lần. Khi khôi phục, engine luôn áp dụng `snapshot → inflight → changelog`, nên state sau recover giữ nguyên EOS.

- **Barrier-based partial snapshot (Chandy–Lamport)**  
  `internal/opb/operator_n.go` + `cmd/opb/main.go` cài đặt barrier-cut: inject marker, ghi offsets per partition, scan dirty keys và ghi full/delta snapshot mà không block ingest.

- **Bundle 3: Causal Safety (Inflight, Freeze & Epoch Fencing) (Beaver-style / FLIP-158)**  
  `cmd/opb/causal_inflight.go` ghi `inflight.json` với `{key, payload, vectorClock}`, manifest chứa `InflightFile`, `InflightEvents`, `SnapshotVectorClock`. Khi restore, `replayInflightEvents` chạy giữa snapshot và Kafka tail để tránh "effect without cause".

- **Pebble SSTable shipping & incremental checkpoint (Phase 2/3)**  
  `internal/snapshot/*` xuất snapshot thành Pebble SSTable (full hoặc incremental), `internal/restore/restore.go` import trực tiếp (`CheckpointCapable.ImportSSTables`). Manifest lưu `PebbleSSTFiles`, `PebbleIncrementalFiles`, `PebbleAllFiles` để mô tả chain và phục vụ GC.

- **Manifest-driven restore & selective replay**  
  Restore pipeline đọc manifest.latest, khôi phục chain full+delta, sau đó đối chiếu offsets với Kafka head: nếu `ReplayRequired=false` hoặc không backlog thì skip Kafka tail (Causal Freeze). Đây là lý do demo đạt `replay_s≈0`.

- **Peer-assisted state migration (KIP-319 / KIP-345 / KIP-429)**  
  Với `--rebalance-import-state=true`, replica mới pause ingest, gọi `importStateFromPeer` để copy snapshot từ peer, phù hợp cooperative rebalance/static membership/sticky assignor. Tính năng này hỗ trợ scale-out/HA mặc dù demo mặc định không bật.

- **Observability & đo lường**  
  `/status`, `/viz/heatmap`, `/viz/zone-data`, `/viz/snapshot-insights` cùng Prometheus gauges `opb_last_restore_*`, `opb_causal_inflight`, `opb_causal_replay_total` giúp theo dõi toàn bộ cut → restore → replay.


## 9) Kỹ thuật Recovery & Snapshot (nâng cao) — trạng thái hiện tại
- **Barrier‑based non‑blocking snapshot**: mọi manifest chứa offsets per‑partition + inflight metadata; cut dựa trên SnapshotView + barrier marker nên không nghẽn writer, vẫn đạt Exactly‑Once.
- **Pebble SSTable shipping (Phase 2)**: full snapshot = Pebble checkpoint; delta = SSTable chứa dirty keys; manifest lưu `pebbleSstFiles`, `pebbleSstChecksums`, `pebbleFormatVersion`. Restore import trực tiếp SSTable → bỏ qua JSON hoàn toàn.
- **Incremental checkpoint (Phase 3)**: `--enable-pebble-phase3` + `PebbleIncrementalFiles`/`PebbleAllFiles` cho phép ship “new SSTables only”, chain được GC bằng ref-count; `scripts/demo_incremental.sh` minh hoạ base + nhiều incremental cut.
- **Causal + inflight delta**: lưu vector inflight, causal markers (Beaver-style) → áp dụng lại đúng thứ tự: snapshot → inflight → changelog.
- **Skip Kafka replay thông minh**: nếu watermark ≤ manifest offsets → chỉ cần snapshot + inflight, giảm TTR. Có thể ép replay bằng `STRIP_OFFSETS=1` trong `measure_ttr.sh`.
- **Peer-assisted state migration**: OpB peer có thể import state của nhau (pebble SSTable) khi rebalance; tận dụng cùng cơ chế checkpoint/import → phù hợp KIP-319/KIP-345/KIP-429 (cooperative rebalance, static membership, sticky assignor).
- **Snapshot GC + retention aware Pebble**: `/admin/snapshot-gc` theo chain + ref-count `PebbleAllFiles`, bảo vệ file dùng chung giữa incremental cut; tích hợp metrics `opb_snapshot_incremental_*`.
- **Tooling & verify**: `opbtool inspect <snapshotDir>` xem SSTable, checksum, sample key; `scripts/demo_recovery.sh` tự verify checksum/atomic import; `scripts/measure_ttr.sh` đo TTR wall-clock; Prometheus có gauge `opb_snapshot_incremental_bytes/files`.
- **Exactly-Once đường đi chuẩn**: pipeline OpA → OpB tận dụng transactional producer (KIP-98), idempotent sinks, và manifest offsets để tránh double-apply.
- **FLIP roadmap alignment**: Phase 3 incremental checkpoint lấy cảm hứng trực tiếp từ Flink FLIP-158 (Generalized Incremental Checkpoints) và các đề xuất FLIP khác cho shuffle-less/local recovery; công cụ GC/ref-count & inspect giúp chứng minh tính toàn vẹn tương tự FLIP-147/FLIP-199.


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


---

### Nhóm A – Exactly‑Once & Idempotency (EOS đường đi chuẩn)

- **Transactional producer + idempotent sink (OpA → OpB)**  
  - `cmd/opa` + `cmd/opb/main.go`: dùng transactional producer, `eventID = orderId#ws`, kiểm tra `dedupSeen` và `LastSeq` để tránh double‑apply.
  - `internal/opb/aggregate_idempotency_test.go`, `internal/opb/exactly_once_batch_test.go`: test đảm bảo không double‑count.
- **Vector clock + epoch fencing**  
  - `internal/opb/tx.go`: `BuildHeadersWithEpochAndVC`, `ExtractVectorClock`, header `epoch`, `vc`.  
  - `cmd/opb/main.go`: đọc VC từ header, tick VC cho operator, dùng epoch để drop message “ngoài epoch”.
- Đây là “kỹ thuật EOS nâng cao” → nên gom thành 1 mục riêng trong báo cáo.

---

### Nhóm B – Barrier‑based Partial Snapshot (Chandy‑Lamport style)

- **N‑input operator & marker logic**  
  - `internal/opb/operator_n.go`, `operator_dyn.go`, `operator_poc.go`: tổng quát hoá Chandy‑Lamport cho N input; record inflight giữa 2 marker.
- **Barrier injection & cut pipeline**  
  - `cmd/opb/main.go`:
    - `/admin/snapshot-cut` + worker `snapshotCutReq` + `barrierCut` struct.  
    - Inject marker vào tất cả partition `p1.orders.enriched`, pause consumer, commit offset, gọi `snapcut.PerformBarrierCut`.
- Phần này là “non‑blocking barrier snapshot” – nền tảng cho partial snapshot/local recovery.

---

### Nhóm C – Causal inflight snapshot (Beaver‑style) + vector clock

- **Ghi inflight snapshot**  
  - `cmd/opb/causal_inflight.go`: `inflightRecord{key, payload, vectorClock}`, `writeInflightSnapshot` → ghi `inflight.json`.
  - `recordInflightEvent` trong `cmd/opb/main.go`: copy payload + VC cho từng event; merge VC vào `barrierCut.vectorClock`.
- **Wiring vào manifest**  
  - `internal/opb/snapcut/snapcut.go`: `CausalInfo{Channels, InflightFile, InflightEvents, VectorClock}`; gán vào:
    - `manifest.InflightFile`, `InflightEvents`, `SnapshotVectorClock`.
- **Replay inflight khi restore**  
  - `replayInflightEvents` trong `cmd/opb/causal_inflight.go`: đọc `inflight.json`, apply lại `OrderEnriched` theo channel order.

Đây chính là “Causal inflight + multi‑vector” – dùng trong `demo_recovery.sh` và `demo_causal_snapshot.sh` (dù script sau anh đã xoá, nhưng kỹ thuật vẫn nằm trong core).

---

### Nhóm D – Pebble SSTable shipping & Incremental snapshot (Phase 2/3)

- **Full + incremental Pebble snapshot**  
  - `internal/snapshot/snapshot.go`: `FormatPebble`, `FilesystemSnapshotter.WriteSnapshotFromView`, `WriteDeltaSnapshotFromView`.
  - `internal/restore/restore.go`: `RestoreFromSnapshotWithFormat` có nhánh `FormatPebble` → gọi `CheckpointCapable.ImportSSTables`.
- **Manifest metadata**  
  - `internal/manifest/manifest.go`: `PebbleSSTFiles`, `PebbleSSTChecksums`, `PebbleIncrementalFiles`, `PebbleAllFiles`.
- **GC & retention**  
  - `internal/snapshot/gc.go` (nếu có) + `/admin/snapshot-gc`: dọn chain snapshot dựa trên ref‑count `PebbleAllFiles`.

Đây là kỹ thuật “local recovery bằng shipping SSTable” + incremental checkpoint, hỗ trợ partial snapshot rất mạnh.

---

### Nhóm E – Manifest‑driven Restore & Local Recovery

- **Restore pipeline chính**  
  - `cmd/opb/main.go` đoạn `if cfg.RestoreOnStart`:  
    - Đọc manifest.latest → restore snapshot chain (full+delta) bằng Pebble shipping.  
    - Đọc `inflight.json` → `replayInflightEvents`.  
    - Quyết định replay Kafka tail hay không (changelog) dựa trên:
      - `ReplayRequired` + `ChangelogHasBacklog`.
- **Freeze / epoch closed**  
  - Field `ReplayRequired` trong `internal/manifest/manifest.go`.  
  - Flag `--restore-trust-manifest` + hàm `manifestAllowsReplaySkip` trong `cmd/opb/main.go`.  
  - `scripts/demo_recovery.sh`: `freeze_epoch_after_cut` set `replayRequired=false` trong manifest + archived manifest → restore bỏ Kafka replay tail.

Đây là kỹ thuật “local recovery guided by manifest”: snapshot+inflight đủ thì không cần replay tail, đạt `replay_s≈0`.

---

### Nhóm F – Peer‑assisted state migration (rebalance/import từ peer)

- **Import state từ peer khi rebalance**  
  - `cmd/opb/main.go` và `cmd/opb/multi_runtime.go`: trong rebalance callback của Kafka consumer, nếu `--rebalance-import-state=true`:
    - Pause ingest, chọn một peer từ `--peers`, gọi `importStateFromPeer`, sau đó resume ingest.
  - `internal/state` + `state.CheckpointCapable`: export/import snapshot từ peer (pebble SST).
- **Scripts test**  
  - `scripts/test_state_import.sh`: kịch bản kiểm tra state import.  
  - Một số logic này không nằm trong `demo_recovery.sh` mà trong demo khác (giờ anh đã xoá scripts, nhưng kỹ thuật vẫn có trong code).

Đây là kỹ thuật “peer‑assisted state migration” giống KIP‑319/KIP‑345.

---

### Nhóm G – Đo TTR, metrics & viz cho địa phương hoá recovery

- **Restore metrics & Prometheus**  
  - `internal/metrics/metrics.go`: `opb_last_restore_*`, `opb_causal_inflight`, `opb_causal_replay_total`.  
  - `cmd/opb/main.go`: sau restore, ghi `restore-metrics.json` + set gauge Prom cho Last Restore Summary.
- **Đo TTR file‑based**  
  - `scripts/measure_ttr.sh` + `internal/restore/restore_ttr_test.go`: đo TTR coarse‑grained khi dùng file manifest + JSON changelog.

Phần này giúp “lượng hoá” hiệu quả các kỹ thuật trên.

