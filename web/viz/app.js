const grid = document.getElementById('grid');
const metricSel = document.getElementById('metric');
const prefixIn = document.getElementById('prefix');
const limitIn = document.getElementById('limit');
const legend = document.getElementById('legend');
const wsSelect = document.getElementById('ws-select');
document.getElementById('reload').onclick = () => load();
document.getElementById('refresh-ws').onclick = () => refreshWindows();
wsSelect.addEventListener('change', () => load());
metricSel.addEventListener('change', () => {
  toggleWsSelector();
  load();
});
toggleWsSelector();

// Ghi nhớ giá trị lần trước để hiển thị xu hướng ▲/▼ theo phút
const prevValues = new Map();

async function load(){
  const params = new URLSearchParams();
  params.set('metric', metricSel.value);
  if (prefixIn.value) params.set('prefix', prefixIn.value);
  if (limitIn.value) params.set('limit', limitIn.value);
  if (metricSel.value !== 'total' && wsSelect.value) params.set('ws', wsSelect.value);
  const res = await fetch(`/viz/heatmap?${params.toString()}&_ts=${Date.now()}`);
  const data = await res.json();
  render(data);
}

async function refreshWindows(){
  if (metricSel.value === 'total') {
    return;
  }
  try {
    const res = await fetch('/status', {cache:'no-store'});
    const status = await res.json();
    const win = status.windowSizeSec || 60;
    const now = Math.floor(Date.now()/1000);
    const currentWs = Math.floor(now / win) * win;
    const candidates = [currentWs, currentWs - win, currentWs - 2*win];
    const existing = new Set();
    Array.from(wsSelect.options).forEach(opt => existing.add(opt.value));
    candidates.forEach(ws => {
      if (!existing.has(String(ws))) {
        const opt = document.createElement('option');
        opt.value = ws;
        opt.textContent = ws;
        wsSelect.appendChild(opt);
      }
    });
  } catch (err) {
    console.warn('refresh windows error', err);
  }
}

function toggleWsSelector(){
  const isTotal = metricSel.value === 'total';
  wsSelect.disabled = isTotal;
}

function render(data){
  const cells = data.cells || [];
  const values = cells.map(c=>c.value);
  const max = Math.max(1, ...values);
  const min = Math.min(...values);
  const total = values.reduce((a,b)=>a+b, 0);
  const wsLabel = data.ws < 0 ? 'ALL' : data.ws;
  legend.textContent = `ws=${wsLabel} instance=${data.instance} metric=${data.metric} max=${max} min=${min} total=${total} cells=${cells.length}`;
  // update dropdown with latest window (avoid duplicates) when in windowed mode
  if (data.ws >= 0 && !Array.from(wsSelect.options).some(opt => opt.value === String(data.ws))) {
    const opt = document.createElement('option');
    opt.value = data.ws;
    opt.textContent = data.ws;
    wsSelect.appendChild(opt);
  }
  
  // Tạo map theo storeId và sắp xếp cố định A, B, C, D...
  const cellMap = new Map();
  cells.forEach(c => {
    cellMap.set(c.storeId, c);
  });
  
  // Thứ tự ưu tiên cố định (A-P), nhưng bổ sung bất kỳ store nào xuất hiện trong dữ liệu
  const baseOrder = ['A-', 'B-', 'C-', 'D-', 'E-', 'F-', 'G-', 'H-', 'I-', 'J-', 'K-', 'L-', 'M-', 'N-', 'O-', 'P-'];
  const dynamicOrder = [...baseOrder];
  cells.forEach(c => {
    if (!dynamicOrder.includes(c.storeId)) {
      dynamicOrder.push(c.storeId);
    }
  });

  grid.innerHTML='';
  dynamicOrder.forEach(storeId => {
    const c = cellMap.get(storeId);
    const el = document.createElement('div');
    el.className='cell';
    let valueDisplay = '0';
    
    if (!c || c.value === 0) {
      // Không có data: màu xám
      el.classList.add('no-data');
      el.style.background = '#e0e0e0';
      el.title = `${storeId}: no data`;
    } else {
      valueDisplay = Number(c.value).toLocaleString('en-US');
      const ratio = (c.value - min) / (max - min || 1); // Normalize 0-1
      // Gradient đơn giản: xanh lá (thấp) → vàng (trung) → đỏ (cao)
      let r, g, b;
      if (ratio < 0.5) {
        // Xanh lá (0-50%): xanh đậm → xanh nhạt
        const subRatio = ratio / 0.5;
        r = Math.round(34 + 221 * subRatio);  // 34->255
        g = Math.round(139 + 116 * subRatio); // 139->255
        b = Math.round(34);                     // 34
      } else {
        // Vàng → Đỏ (50-100%): vàng → đỏ đậm
        const subRatio = (ratio - 0.5) / 0.5;
        r = Math.round(255);                    // 255
        g = Math.round(255 - 215 * subRatio);  // 255->40
        b = Math.round(0);                      // 0
      }
      el.style.background = `rgb(${r},${g},${b})`;
      // Xu hướng theo lần hiển thị trước
      const prev = prevValues.get(storeId) ?? c.value;
      const delta = c.value - prev;
      if (Math.abs(delta) > Math.max(5, 0.02 * prev)) {
        // tăng: viền xanh đậm; giảm: viền đỏ đậm
        el.classList.add(delta > 0 ? 'up' : 'down');
        const trend = document.createElement('span');
        trend.className = 'trend';
        trend.textContent = delta > 0 ? '▲' : '▼';
        el.appendChild(trend);
      }
      el.title = `${c.storeId}: ${c.value}${delta ? ` (${delta>0?'+':''}${delta})` : ''}`;
      prevValues.set(storeId, c.value);
    }
    
    // Label storeId (luôn hiển thị)
    const label = document.createElement('span');
    label.className = 'cell-label';
    label.textContent = storeId.replace('-', '');
    el.appendChild(label);

    // Numerical value to chứng minh EOS (sumQty)
    const val = document.createElement('span');
    val.className = 'cell-value';
    val.textContent = valueDisplay;
    el.appendChild(val);

    // Clickable to zone details
    el.style.cursor = 'pointer';
    el.onclick = () => {
      const id = encodeURIComponent(storeId);
      // Link to server-rendered page to avoid any JS/cache issues
      window.location.href = `/viz/zone-data?id=${id}`;
    };
    
    grid.appendChild(el);
  });
}

load();
setInterval(load, 2000);


