#!/usr/bin/env bash
set -euo pipefail

# Kịch bản chứng minh Exactly-Once Semantics (EOS) - không double-count
# Phiên bản dùng Go pump (bin/pump), không phụ thuộc Kafka CLI.

BOOTSTRAP=${BOOTSTRAP:-127.0.0.1:9092}
TOPIC=${TOPIC:-p1.orders}               # đầu vào của OpA
STORE_ID_PREFIX=${STORE_ID_PREFIX:-EOS-TEST-}
N=${N:-10}                              # số lượng event cho test
HTTP_OPB=${HTTP_OPB:-:8089}
WINDOW_SIZE=${WINDOW_SIZE:-60}          # phải khớp với window-size của OpB

say() { printf "\n\e[1;33m[%s] %s\e[0m\n" "$(date +%H:%M:%S)" "$*"; }

require_pump() {
  if [ ! -x ./bin/pump ]; then
    say "Building pump..."
    go build -o bin/pump ./cmd/pump
  fi
}

wait_opb_ready() {
  say "Đợi OpB /healthz sẵn sàng..."
  for i in {1..60}; do
    if curl -sf "http://127.0.0.1${HTTP_OPB}/healthz" | grep -q 'ok'; then
      return
    fi
    sleep 1
  done
  echo "ERROR: OpB không sẵn sàng sau 60s" >&2
  exit 1
}

# Sinh N dòng dữ liệu raw cố định (deterministic) cho 1 store và 1 window
produce_fixed_events() {
  local count=$1
  local ws=$2
  local store=$3
  say "Sinh ${count} event thô (raw) cho khu '${store}' vào window ${ws}..."
  ./bin/pump \
    -bootstrap "$BOOTSTRAP" \
    -topic "$TOPIC" \
    -mode raw \
    -n "$count" \
    -parallel 1 \
    -eos-store "$store" \
    -eos-window-start "$ws" \
    -eos-count "$count"
  say "Bơm xong."
}

# Lấy giá trị sumQty của khu từ heatmap (theo ws), trả về số
get_heatmap_value() {
  local ws=$1
  local store_prefix=$2
  for i in {1..30}; do
    local response
    response=$(curl -sf "http://127.0.0.1${HTTP_OPB}/viz/heatmap?prefix=${store_prefix}&ws=${ws}" || true)
    if [[ -z "$response" ]]; then
      sleep 2; continue
    fi
    # Lấy tổng giá trị các cell khớp prefix (an toàn nếu nhiều product)
    local value
    value=$(echo "$response" | python3 - "$store_prefix" <<'PY'
import json,sys
try:
    resp=json.load(sys.stdin)
except Exception:
    print(0)
    sys.exit(0)
prefix=sys.argv[1]
val=0
for c in resp.get('cells',[]):
    if c.get('storeId','').startswith(prefix):
        try:
            val+=int(c.get('value',0))
        except Exception:
            pass
print(val)
PY
    )
    if [[ "$value" =~ ^[0-9]+$ ]]; then
      echo "$value"; return
    fi
    sleep 2
  done
  echo "0"
}

main() {
  require_pump
  wait_opb_ready
  local now=$(date +%s)
  local ws=$(( (now / WINDOW_SIZE) * WINDOW_SIZE ))
  local store="${STORE_ID_PREFIX}$(date +%s)-"  # tránh dính dữ liệu cũ

  say "BƯỚC 1: Bơm $N event lần đầu vào window $ws..."
  produce_fixed_events "$N" "$ws" "$store"
  say "Đợi 35 giây để OpA và OpB consume và xử lý..."
  sleep 35

  val1=$(get_heatmap_value "$ws" "$store")
  say "KẾT QUẢ 1: Giá trị sumQty của '$store' là: $val1"
  if [ "$val1" -ne "$N" ]; then
    echo -e "\e[1;31m LỖI: Giá trị mong đợi là $N, nhưng nhận được $val1. Dừng test.\e[0m"
    exit 1
  fi

  say "BƯỚC 2: Bơm lại y hệt $N event đó vào cùng window $ws..."
  produce_fixed_events "$N" "$ws" "$store"
  say "Đợi 20 giây để OpA và OpB consume và xử lý..."
  sleep 20

  val2=$(get_heatmap_value "$ws" "$store")
  say "KẾT QUẢ 2: Giá trị sumQty của '$store' là: $val2"

  echo ""
  if [ "$val1" -eq "$val2" ]; then
    echo -e "\e[1;32m✅ THÀNH CÔNG! Giá trị không đổi ($val1 == $val2). Hệ thống đã bỏ qua event trùng lặp (không double-count).\e[0m"
  else
    echo -e "\e[1;31m❌ THẤT BẠI! Giá trị đã đổi từ $val1 sang $val2.\e[0m"
    exit 2
  fi
}

main "$@"
