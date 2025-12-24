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

// Remember previous values to show trend ▲/▼ per minute
const prevValues = new Map();

// Prometheus helpers
const promUrlInput = document.getElementById('prom-url');
const promSaveBtn = document.getElementById('save-prom');
const promRefreshBtn = document.getElementById('prom-refresh');
const promCausalStatus = document.getElementById('prom-causal-status');
const timelineEl = document.getElementById('snapshot-timeline');
const timelineStatus = document.getElementById('snapshot-timeline-status');
const restorePhasesStatus = document.getElementById('restore-phases-status');
const incrementalStatus = document.getElementById('incremental-files-status');
const storedProm = localStorage.getItem('opbPromBase');
const defaultProm = storedProm || `${window.location.protocol}//${window.location.hostname}:9090`;
if (promUrlInput) {
  promUrlInput.value = defaultProm;
}
const getPromBase = () => (promUrlInput ? promUrlInput.value.trim().replace(/\/$/, '') : '');
if (promSaveBtn) {
  promSaveBtn.onclick = () => {
    const base = getPromBase();
    if (base) {
      localStorage.setItem('opbPromBase', base);
      refreshPromPanels();
    }
  };
}
if (promRefreshBtn) {
  promRefreshBtn.onclick = () => {
    refreshPromPanels(true);
    refreshSnapshotPanels(true);
  };
}

function shortStoreId(id){
  // Prefer numeric suffix if present, e.g. RECOVERY-0042 -> R-042
  const m = id.match(/(?:^|-)0*(\d+)$/);
  if (m && m[1]) return `R-${m[1].padStart(3,'0')}`;
  // Otherwise, compact common prefix RECOVERY -> R-
  const compact = id.replace(/^RECOVERY-?/, 'R-');
  // Truncate very long ids to keep label readable in small cells
  return compact.length > 8 ? compact.slice(0,8) : compact;
}

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
  
  grid.innerHTML='';
  if (cells.length === 0) {
    const empty = document.createElement('div');
    empty.className = 'cell no-data';
    empty.textContent = 'No data';
    grid.appendChild(empty);
    return;
  }

  cells.forEach(c => {
    const storeId = c.storeId;
    const el = document.createElement('div');
    el.className='cell';
    let valueDisplay = '0';
    
    if (!c.value) {
      // No data: gray background
      el.classList.add('no-data');
      el.style.background = '#e0e0e0';
      el.title = `${storeId}: no data`;
    } else {
      valueDisplay = Number(c.value).toLocaleString('en-US');
      const ratio = (c.value - min) / (max - min || 1); // Normalize 0-1
      // Simple gradient: green (low) -> yellow (mid) -> red (high)
      let r, g, b;
      if (ratio < 0.5) {
        // Green (0-50%): dark green -> light green
        const subRatio = ratio / 0.5;
        r = Math.round(34 + 221 * subRatio);  // 34->255
        g = Math.round(139 + 116 * subRatio); // 139->255
        b = Math.round(34);                     // 34
      } else {
        // Yellow -> Red (50-100%): yellow -> dark red
        const subRatio = (ratio - 0.5) / 0.5;
        r = Math.round(255);                    // 255
        g = Math.round(255 - 215 * subRatio);  // 255->40
        b = Math.round(0);                      // 0
      }
      el.style.background = `rgb(${r},${g},${b})`;
      // Trend based on previous render
      const prev = prevValues.get(storeId) ?? c.value;
      const delta = c.value - prev;
      if (Math.abs(delta) > Math.max(5, 0.02 * prev)) {
        // increase: dark green border; decrease: dark red border
        el.classList.add(delta > 0 ? 'up' : 'down');
        const trend = document.createElement('span');
        trend.className = 'trend';
        trend.textContent = delta > 0 ? '▲' : '▼';
        el.appendChild(trend);
      }
      el.title = `${c.storeId}: ${c.value}${delta ? ` (${delta>0?'+':''}${delta})` : ''}`;
      prevValues.set(storeId, c.value);
    }
    
    // Shortened storeId label for readability
    const label = document.createElement('span');
    label.className = 'cell-label';
    label.textContent = shortStoreId(storeId);
    el.appendChild(label);

    // Numerical value to prove EOS (sumQty)
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

async function queryPromRange(query, seconds = 300) {
  const params = new URLSearchParams({ query, seconds });
  const base = getPromBase();
  if (base) {
    params.set('base', base);
  }
  const res = await fetch(`/viz/prom-range?${params.toString()}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return data.values || [];
}

function drawPromSeries(canvasId, values, color) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  if (!values.length) {
    ctx.fillStyle = '#999';
    ctx.fillText('No data', 10, 20);
    return;
  }
  const nums = values.map(v => Number(v[1]));
  const min = Math.min(...nums);
  const max = Math.max(...nums);
  const span = max - min || 1;
  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.beginPath();
  values.forEach((v, idx) => {
    const x = (idx / (values.length - 1 || 1)) * canvas.width;
    const y = canvas.height - ((Number(v[1]) - min) / span) * canvas.height;
    if (idx === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
  ctx.fillStyle = '#555';
  ctx.font = '10px sans-serif';
  ctx.fillText(`min=${min.toFixed(2)} max=${max.toFixed(2)}`, 4, canvas.height - 4);
}

async function renderPromPanel(canvasId, statusEl, query, color) {
  if (!promUrlInput) return;
  try {
    const values = await queryPromRange(query);
    drawPromSeries(canvasId, values, color);
    if (statusEl) statusEl.textContent = values.length ? `points=${values.length}` : 'no series';
    if (statusEl) statusEl.classList.remove('err');
  } catch (err) {
    if (statusEl) {
      statusEl.textContent = `error: ${err.message}`;
      statusEl.classList.add('err');
    }
  }
}

async function refreshLastRestoreSummary() {
  const statusEl = document.getElementById('lrs-status');
  const elTtr = document.getElementById('lrs-ttr');
  const elRestore = document.getElementById('lrs-restore');
  const elReplay = document.getElementById('lrs-replay');
  const elSnap = document.getElementById('lrs-snapshot');
  const elInfl = document.getElementById('lrs-inflight');
  const elEOS = document.getElementById('lrs-eos');
  if (!promUrlInput || !elTtr) return;
  const base = getPromBase();
  if (!base) {
    if (statusEl) { statusEl.textContent = 'Set Prometheus URL'; statusEl.classList.add('err'); }
    return;
  }
  try {
    const seconds = 7200;
    const [ttrVals, restoreMsVals, replaySecsVals, replayEvVals, snapBytesVals, sstVals, incVals, inflVals, eosVals] = await Promise.all([
      queryPromRange('max(opb_last_restore_ttr_seconds)', seconds),
      queryPromRange('max(opb_last_restore_restore_only_ms)', seconds),
      queryPromRange('max(opb_last_restore_replay_seconds)', seconds),
      queryPromRange('max(opb_last_restore_replay_events)', seconds),
      queryPromRange('max(opb_last_restore_snapshot_bytes)', seconds),
      queryPromRange('max(opb_last_restore_sst_files_total)', seconds),
      queryPromRange('max(opb_last_restore_incremental_files)', seconds),
      queryPromRange('max(opb_last_restore_inflight_replayed)', seconds),
      queryPromRange('max(opb_last_restore_eos_ok)', seconds)
    ]);
    const last = arr => (arr && arr.length ? Number(arr[arr.length-1][1]) : NaN);
    const ttr = last(ttrVals);
    const restMs = last(restoreMsVals);
    const replayS = last(replaySecsVals);
    const replayEv = last(replayEvVals);
    const snapB = last(snapBytesVals);
    const sst = last(sstVals);
    const inc = last(incVals);
    const infl = last(inflVals);
    const eos = last(eosVals);
    const rate = (replayS > 0) ? (replayEv / replayS) : 0;
    if (!isNaN(ttr)) elTtr.textContent = `TTR: ${ttr.toFixed(2)}s`;
    if (!isNaN(restMs)) elRestore.textContent = `Restore: ${Math.round(restMs)} ms`;
    if (!isNaN(replayS)) elReplay.textContent = `Replay: ${replayS.toFixed(2)} s @ ${rate ? rate.toFixed(1) : '0'} eps (N=${isNaN(replayEv)?'0':Math.round(replayEv)})`;
    if (!isNaN(snapB) || !isNaN(sst)) {
      const humanB = isNaN(snapB) ? '?' : (snapB >= 1e9 ? (snapB/1e9).toFixed(2)+' GB' : snapB >= 1e6 ? (snapB/1e6).toFixed(1)+' MB' : Math.round(snapB)+' B');
      elSnap.textContent = `Snapshot: ${humanB}${isNaN(sst)?'':`, ${Math.round(sst)} SSTs`} ${isNaN(inc)||inc<=0?'':`(Δ files=${Math.round(inc)})`}`;
    }
    if (!isNaN(infl)) elInfl.textContent = `Inflight replayed: ${Math.round(infl)}`;
    if (!isNaN(eos)) elEOS.textContent = `EOS: ${eos >= 0.5 ? 'OK' : 'FAIL'}`;
    if (statusEl) { statusEl.textContent = 'updated'; statusEl.classList.remove('err'); }
  } catch (err) {
    if (statusEl) { statusEl.textContent = `error: ${err.message}`; statusEl.classList.add('err'); }
  }
}

function refreshPromPanels(manual = false) {
  if (!promUrlInput) return;
  const base = getPromBase();
  if (!base) {
    if (manual && promCausalStatus) promCausalStatus.textContent = 'Set Prometheus URL';
    return;
  }
  renderPromPanel('prom-causal', promCausalStatus, 'sum(opb_causal_inflight)', '#f39c12');
}

refreshPromPanels();
refreshLastRestoreSummary();
setInterval(refreshPromPanels, 15000);
setInterval(refreshLastRestoreSummary, 15000);

async function refreshSnapshotPanels(manual = false) {
  try {
    const res = await fetch(`/viz/snapshot-insights?_ts=${Date.now()}`, { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    renderTimeline(data.timeline || []);
    renderRestorePhases(data.restorePhases || {}, data.restoreSource || '');
    renderIncrementalPanel(data.latest || null);
    if (timelineStatus) {
      timelineStatus.textContent = data.timeline && data.timeline.length ? `entries=${data.timeline.length}` : 'no snapshots';
      timelineStatus.classList.remove('err');
    }
  } catch (err) {
    if (timelineStatus) {
      timelineStatus.textContent = `error: ${err.message}`;
      timelineStatus.classList.add('err');
    }
    if (restorePhasesStatus) {
      restorePhasesStatus.textContent = 'restore data unavailable';
      restorePhasesStatus.classList.add('err');
    }
    if (incrementalStatus) {
      incrementalStatus.textContent = 'incremental data unavailable';
      incrementalStatus.classList.add('err');
    }
  }
}

function renderTimeline(entries) {
  if (!timelineEl) return;
  timelineEl.innerHTML = '';
  if (!entries.length) {
    const empty = document.createElement('div');
    empty.className = 'timeline-entry';
    empty.textContent = 'no snapshots yet';
    timelineEl.appendChild(empty);
    return;
  }
  entries.forEach((entry, idx) => {
    const div = document.createElement('div');
    div.className = 'timeline-entry';
    const label = entry.type || (entry.deltaSequence > 0 ? 'delta' : 'full');
    const seq = entry.deltaSequence ? ` Δ#${entry.deltaSequence}` : '';
    const totalFilesText = entry.totalFiles != null ? entry.totalFiles : '?';
    const inc = entry.incrementalFiles ? `${entry.incrementalFiles} new / ${totalFilesText}` : `${totalFilesText} files`;
    const time = entry.createdAtIso || '';
    const title = idx === 0 ? `Latest · ${entry.snapshotId || '(unknown)'}` : (entry.snapshotId || '(unknown)');
    div.innerHTML = `<strong>${title}</strong>
      <div>${label}${seq}</div>
      <div class="muted">${time}</div>
      <div class="muted">files: ${inc}</div>`;
    timelineEl.appendChild(div);
  });
}

function renderRestorePhases(phases, sourceLabel = '') {
  const canvas = document.getElementById('restore-phases');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  const segments = [
    ['manifestMs', '#74c0fc', 'Manifest'],
    ['snapshotTotalMs', '#2ecc71', 'Snapshot'],
    ['changelogMs', '#1abc9c', 'Changelog'],
    ['metricsMs', '#f39c12', 'Metrics'],
  ].map(([field, color, label]) => ({
    field,
    color,
    label,
    value: Number(phases[field] || 0),
  })).filter(seg => seg.value > 0);
  const total = segments.reduce((sum, seg) => sum + seg.value, 0);
  if (!segments.length) {
    ctx.fillStyle = '#999';
    ctx.fillText('no restore data', 10, 20);
    if (restorePhasesStatus) {
      restorePhasesStatus.textContent = 'waiting for restore run';
      restorePhasesStatus.classList.add('muted');
    }
    return;
  }
  let offset = 0;
  segments.forEach(seg => {
    const width = (seg.value / total) * canvas.width;
    ctx.fillStyle = seg.color;
    ctx.fillRect(offset, 20, width, 60);
    ctx.fillStyle = '#fff';
    ctx.font = '10px sans-serif';
    ctx.fillText(`${seg.label} ${seg.value}ms`, offset + 4, 50);
    offset += width;
  });
  ctx.strokeStyle = '#333';
  ctx.strokeRect(0, 20, canvas.width, 60);
  if (restorePhasesStatus) {
    const summary = segments.map(seg => `${seg.label}:${seg.value}ms`).join('  ');
    const prefix = sourceLabel ? `source=${sourceLabel} · ` : '';
    restorePhasesStatus.textContent = `${prefix}total=${total}ms | ${summary}`;
    restorePhasesStatus.classList.remove('err');
    restorePhasesStatus.classList.remove('muted');
  }
}

function renderIncrementalPanel(latest) {
  const canvas = document.getElementById('incremental-files');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  if (!latest || !latest.snapshotId) {
    ctx.fillStyle = '#999';
    ctx.fillText('no incremental snapshot yet', 10, 20);
    if (incrementalStatus) {
      incrementalStatus.textContent = 'waiting for incremental snapshot';
      incrementalStatus.classList.add('muted');
    }
    return;
  }
  const incremental = latest.incrementalFiles || 0;
  const total = latest.totalFiles || incremental;
  const ratio = total ? incremental / total : 0;
  ctx.fillStyle = '#e0e0e0';
  ctx.fillRect(20, 30, canvas.width - 40, 30);
  ctx.fillStyle = '#ff6b6b';
  ctx.fillRect(20, 30, (canvas.width - 40) * ratio, 30);
  ctx.strokeStyle = '#333';
  ctx.strokeRect(20, 30, canvas.width - 40, 30);
  ctx.fillStyle = '#333';
  ctx.font = '12px sans-serif';
  ctx.fillText(`Δ files: ${incremental} / ${total}`, 20, 25);
  ctx.fillText(latest.snapshotId, 20, 80);
  if (incrementalStatus) {
    incrementalStatus.textContent = `snapshot=${latest.snapshotId} | incremental=${incremental} | total=${total}`;
    incrementalStatus.classList.remove('err');
  }
}

refreshSnapshotPanels();
setInterval(refreshSnapshotPanels, 15000);

// ---- Exact key compare (auto ws) ----
const cmpStore = document.getElementById('cmp-store');
const cmpProd  = document.getElementById('cmp-prod');
const cmpWs    = document.getElementById('cmp-ws');
const cmpLoad  = document.getElementById('cmp-load');
const cmpMarkB = document.getElementById('cmp-mark-before');
const cmpMarkA = document.getElementById('cmp-mark-after');
const cmpCurEl = document.getElementById('cmp-current');
const cmpBEl   = document.getElementById('cmp-before');
const cmpAEl   = document.getElementById('cmp-after');
const cmpRes   = document.getElementById('cmp-result');

async function defaultWs() {
  try {
    const r = await fetch('/status', {cache:'no-store'});
    const j = await r.json();
    const win = j.windowSizeSec || 60;
    const now = Math.floor(Date.now()/1000);
    return Math.floor(now / win) * win;
  } catch { return 0; }
}

function showJson(el, obj) {
  if (!el) return;
  el.textContent = obj ? JSON.stringify(obj, null, 2) : '';
  el.classList.toggle('muted', !obj);
}

function parsePre(el){
  try { return el && el.textContent ? JSON.parse(el.textContent) : null; } catch { return null; }
}

async function loadExact() {
  if (!cmpStore || !cmpProd) return null;
  const s = cmpStore.value.trim();
  const p = cmpProd.value.trim();
  let w = (cmpWs && cmpWs.value) ? Number(cmpWs.value) : 0;
  if (!w) { w = await defaultWs(); if (cmpWs) cmpWs.value = String(w||''); }
  if (!s || !p || !w) { showJson(cmpCurEl, {error:'missing params'}); return null; }
  const url = `/api/exact?${new URLSearchParams({storeId:s,productId:p,ws:String(w)}).toString()}`;
  const r = await fetch(url, {cache:'no-store'});
  const j = await r.json();
  showJson(cmpCurEl, j);
  return j;
}

function saveLocal(key, obj){ try{ localStorage.setItem(key, JSON.stringify(obj)); }catch{} }
function loadLocal(key){ try{ return JSON.parse(localStorage.getItem(key)||'null'); }catch{ return null; }}

function compareExact(a,b){
  if (!a || !b || !a.found || !b.found) return {ok:false, reason:'not-found or missing'};
  const same = (a.sumQty===b.sumQty) && (a.sumAmount===b.sumAmount) && (a.lastSeq===b.lastSeq);
  return {ok:same, a:{sq:a.sumQty,sa:a.sumAmount,ls:a.lastSeq}, b:{sq:b.sumQty,sa:b.sumAmount,ls:b.lastSeq}};
}

async function ensureBeforeAfterUI(){
  const B = loadLocal('opbCmpBefore');
  const A = loadLocal('opbCmpAfter');
  showJson(cmpBEl, B);
  showJson(cmpAEl, A);
  if (B && A) {
    const res = compareExact(B, A);
    if (cmpRes) cmpRes.textContent = res.ok ? 'OK: BEFORE == AFTER' : `DIFF: ${JSON.stringify(res)}`;
  } else if (cmpRes) {
    cmpRes.textContent = '';
  }
}

if (cmpLoad) {
  cmpLoad.onclick = async () => { await loadExact(); };
}
if (cmpMarkB) {
  cmpMarkB.onclick = async () => {
    const cur = await loadExact();
    if (cur) { saveLocal('opbCmpBefore', cur); await ensureBeforeAfterUI(); }
  };
}
if (cmpMarkA) {
  cmpMarkA.onclick = async () => {
    const cur = await loadExact();
    if (cur) { saveLocal('opbCmpAfter', cur); await ensureBeforeAfterUI(); }
  };
}

// Prefill ws for compare panel on first load
(async function prefillWs(){
  if (cmpWs && !cmpWs.value) { const w = await defaultWs(); if (w) cmpWs.value = String(w); }
  ensureBeforeAfterUI();
})();



