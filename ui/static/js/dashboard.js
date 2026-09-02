/* ============================================================
   MeshWeaver — Operations Console
   Talks to the existing Flask API (unchanged):
     GET  /api/status
     POST /api/node/start   {host, port}
     POST /api/node/stop     {}
     POST /api/peers/ping    {host, port}
     POST /api/tasks/submit  {operation, first, second, host, port}
   Snapshot: {running, node_id, address, peers[], events[], tasks[]}
   ============================================================ */

const SIGNAL = { tx: '#FFB454', rx: '#38E1D0', ok: '#5FE08A', err: '#FF6E85', sys: '#8E7BFF', line: '#223058', faint: '#5F6D9C' };
const PEER_COLORS = [SIGNAL.rx, SIGNAL.tx, SIGNAL.sys, SIGNAL.ok];
const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

const $ = (sel) => document.querySelector(sel);
const toast = $('.toast');
const state = { status: null };

/* ---------- helpers ---------- */
function showToast(message, error = false) {
  toast.textContent = message;
  toast.classList.toggle('error', error);
  toast.classList.add('visible');
  clearTimeout(showToast._t);
  showToast._t = setTimeout(() => toast.classList.remove('visible'), 3200);
}

async function api(path, data) {
  const response = await fetch(path, {
    method: data ? 'POST' : 'GET',
    headers: data ? { 'Content-Type': 'application/json' } : {},
    body: data ? JSON.stringify(data) : undefined,
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || 'The request could not be completed.');
  return payload;
}

function esc(value) {
  return String(value).replace(/[&<>'"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[c]));
}
const pad2 = (n) => String(n).padStart(2, '0');
function setOnline(el, on) { if (el) el.dataset.online = on ? 'true' : 'false'; }

/* ============================================================
   Render snapshot into the DOM
   ============================================================ */
function render(status) {
  state.status = status;
  const online = !!status.running;

  const netState = $('#network-state');
  netState.innerHTML = `<span class="beacon"></span> ${online ? `Node online · <code>${esc(status.node_id)}</code>` : 'Node offline'}`;
  setOnline(netState, online);

  $('#side-status').textContent = online ? 'Mesh node online' : 'Node offline';
  $('#side-address').textContent = status.address || 'No transport bound';
  setOnline($('#rail-transport'), online);

  const chip = $('#transport-chip');
  setOnline(chip, online);
  $('#chip-address').textContent = status.address || 'offline';

  const control = $('#node-control');
  control.textContent = online ? 'Stop node' : 'Start node';
  setOnline(control, online);

  $('#peer-count').textContent = pad2(status.peers.length);
  $('#task-count').textContent = pad2(status.tasks.length);
  $('#event-count').textContent = pad2(status.events.length);
  $('#node-id').textContent = status.node_id || '—';
  $('#node-address').textContent = status.address || '—';

  const core = $('#radar-core');
  setOnline(core, online);
  $('#radar-core-id').textContent = online ? (status.node_id || 'local') : 'idle';
  $('#radar-core-sub').textContent = online ? 'local node' : 'no transport';

  // Peers
  $('#peer-list').innerHTML = status.peers.length
    ? status.peers.map((peer, i) => {
        const initial = esc((peer.name || '?').charAt(0).toUpperCase() || '?');
        return `<div class="peer-row">
          <span class="node-orb orb-${i % 4}">${initial}</span>
          <div><strong>${esc(peer.name)}</strong><code>${esc(peer.host)}:${esc(peer.port)}</code></div>
          <button class="peer-ping" data-host="${esc(peer.host)}" data-port="${esc(peer.port)}">Ping</button>
          <span class="pill pill-online">${esc(peer.status)}</span>
        </div>`;
      }).join('')
    : '<p class="empty-state">No peers yet. Start your node, then ping one to bring it onto the mesh.</p>';

  // Activity
  const GLYPH = { send: '↑', peer: '↓', success: '✓', error: '!', system: '●' };
  $('#activity-list').innerHTML = status.events.length
    ? status.events.map((e) => {
        const kind = GLYPH[e.type] ? e.type : 'system';
        return `<div class="activity">
          <span class="activity-symbol sym-${kind}">${GLYPH[kind] || '●'}</span>
          <div class="activity-body"><strong>${esc(e.type)}</strong><p>${esc(e.message)}</p></div>
          <time>${esc(e.time)}</time>
        </div>`;
      }).join('')
    : '<p class="empty-state">Nothing on the wire yet. Node events stream in here as they happen.</p>';

  // Tasks
  $('#task-list').innerHTML = status.tasks.length
    ? status.tasks.map((t) => `<tr>
        <td><code>${esc(t.name)}</code></td>
        <td>${esc(t.target)}</td>
        <td>${esc(t.time)}</td>
        <td><span class="pill pill-busy">${esc(t.status)}</span></td>
      </tr>`).join('')
    : '<tr><td colspan="4" class="empty-cell">No tasks dispatched this session.</td></tr>';

  radar.sync(status);
}

async function refresh(silent = false) {
  try { render(await api('/api/status')); }
  catch (error) { if (!silent) showToast(error.message, true); }
}

/* ============================================================
   Mesh radar — the signature. Local node at centre, peers in
   orbit, packets travelling the links driven by real events.
   ============================================================ */
const radar = (() => {
  const stage = $('#radar-stage');
  const canvas = $('#mesh-radar');
  const ctx = canvas.getContext('2d');
  const dpr = Math.min(window.devicePixelRatio || 1, 2);

  let W = 0, H = 0;
  let online = false;
  let peers = [];
  let packets = [];
  let pulse = null;            // centre ripple {t0, color}
  let sweep = 0;               // radar sweep angle
  let lastEventKey;            // to detect new events
  let acc = 0, last = 0;       // ambient spawn accumulator / frame time

  function resize() {
    const r = stage.getBoundingClientRect();
    W = r.width; H = r.height;
    canvas.width = Math.round(W * dpr);
    canvas.height = Math.round(H * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    if (reduceMotion) draw(0);
  }

  function geometry() {
    const cx = W / 2, cy = H / 2;
    const radius = Math.max(60, Math.min(W, H) * 0.37);
    const n = peers.length;
    const nodes = peers.map((p, i) => {
      const a = -Math.PI / 2 + (i * 2 * Math.PI) / Math.max(n, 1);
      return { x: cx + Math.cos(a) * radius, y: cy + Math.sin(a) * radius, color: PEER_COLORS[i % 4], label: String(p.port), host: p.host, port: String(p.port) };
    });
    return { cx, cy, radius, nodes };
  }

  function spawn(linkIndex, dir, color) {
    if (packets.length > 60) return;
    packets.push({ link: linkIndex, dir, color, t: 0, speed: 0.55 + Math.random() * 0.25 });
  }
  function ripple(color) { pulse = { t0: performance.now(), color }; }

  function trigger(event, nodes) {
    if (!event) return;
    let idx = -1;
    if (event.host != null && event.port != null) {
      idx = nodes.findIndex((n) => n.host === event.host && n.port === String(event.port));
    }
    const links = idx >= 0 ? [idx] : nodes.map((_, i) => i);
    switch (event.type) {
      case 'send':
        links.forEach((i) => spawn(i, 'out', SIGNAL.tx));
        ripple(SIGNAL.tx);
        break;
      case 'peer':
        links.forEach((i) => spawn(i, 'in', SIGNAL.rx));
        ripple(SIGNAL.rx);
        break;
      case 'success':
        links.forEach((i) => spawn(i, 'in', SIGNAL.ok));
        break;
      case 'error':
        links.forEach((i) => spawn(i, 'in', SIGNAL.err));
        ripple(SIGNAL.err);
        break;
      default:
        ripple(SIGNAL.sys);
    }
  }

  function sync(status) {
    online = !!status.running;
    peers = status.peers || [];
    const top = status.events && status.events.length ? status.events[0] : null;
    const key = top ? `${top.type}|${top.message}|${top.time}` : null;
    if (lastEventKey === undefined) {
      lastEventKey = key;                    // first load: prime, don't animate
    } else if (key && key !== lastEventKey) {
      lastEventKey = key;
      if (!reduceMotion) trigger(top, geometry().nodes);
    }
    if (reduceMotion) draw(0);
  }

  function draw(dt) {
    const { cx, cy, radius, nodes } = geometry();
    ctx.clearRect(0, 0, W, H);

    // range rings
    for (let k = 1; k <= 3; k++) {
      ctx.beginPath();
      ctx.arc(cx, cy, (radius * k) / 3, 0, Math.PI * 2);
      ctx.strokeStyle = 'rgba(56,225,208,0.05)';
      ctx.lineWidth = 1;
      ctx.stroke();
    }
    // crosshair
    ctx.strokeStyle = 'rgba(90,105,150,0.10)';
    ctx.beginPath();
    ctx.moveTo(cx - radius, cy); ctx.lineTo(cx + radius, cy);
    ctx.moveTo(cx, cy - radius); ctx.lineTo(cx, cy + radius);
    ctx.stroke();

    // sweep
    if (!reduceMotion) {
      sweep += dt * 0.55;
      const grad = ctx.createConicGradient ? ctx.createConicGradient(sweep, cx, cy) : null;
      if (grad) {
        grad.addColorStop(0, 'rgba(56,225,208,0.14)');
        grad.addColorStop(0.08, 'rgba(56,225,208,0)');
        grad.addColorStop(1, 'rgba(56,225,208,0)');
        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.moveTo(cx, cy);
        ctx.arc(cx, cy, radius, 0, Math.PI * 2);
        ctx.fill();
      }
    }

    // links + peers
    if (online) {
      nodes.forEach((node) => {
        ctx.beginPath();
        ctx.moveTo(cx, cy); ctx.lineTo(node.x, node.y);
        ctx.strokeStyle = 'rgba(120,135,180,0.16)';
        ctx.lineWidth = 1;
        ctx.stroke();
      });

      // packets
      packets = packets.filter((p) => p.t <= 1);
      packets.forEach((p) => {
        p.t += dt * p.speed;
        const node = nodes[p.link];
        if (!node) { p.t = 2; return; }
        const from = p.dir === 'out' ? { x: cx, y: cy } : node;
        const to = p.dir === 'out' ? node : { x: cx, y: cy };
        const t = Math.min(p.t, 1);
        const x = from.x + (to.x - from.x) * t;
        const y = from.y + (to.y - from.y) * t;
        ctx.beginPath();
        ctx.arc(x, y, 3, 0, Math.PI * 2);
        ctx.fillStyle = p.color;
        ctx.shadowColor = p.color;
        ctx.shadowBlur = 10;
        ctx.fill();
        ctx.shadowBlur = 0;
      });

      // peer nodes
      nodes.forEach((node) => {
        ctx.beginPath();
        ctx.arc(node.x, node.y, 9, 0, Math.PI * 2);
        ctx.strokeStyle = node.color;
        ctx.globalAlpha = 0.35;
        ctx.lineWidth = 2;
        ctx.stroke();
        ctx.globalAlpha = 1;
        ctx.beginPath();
        ctx.arc(node.x, node.y, 4.5, 0, Math.PI * 2);
        ctx.fillStyle = node.color;
        ctx.shadowColor = node.color;
        ctx.shadowBlur = 12;
        ctx.fill();
        ctx.shadowBlur = 0;
        ctx.fillStyle = SIGNAL.faint;
        ctx.font = '10px "JetBrains Mono", monospace';
        ctx.textAlign = 'center';
        ctx.fillText(node.label, node.x, node.y + 22);
      });
    }

    // centre ripple
    if (pulse) {
      const age = (performance.now() - pulse.t0) / 800;
      if (age >= 1) { pulse = null; }
      else {
        ctx.beginPath();
        ctx.arc(cx, cy, 10 + age * 42, 0, Math.PI * 2);
        ctx.strokeStyle = pulse.color;
        ctx.globalAlpha = 1 - age;
        ctx.lineWidth = 2;
        ctx.stroke();
        ctx.globalAlpha = 1;
      }
    }
  }

  function loop(now) {
    const dt = Math.min((now - last) / 1000, 0.05);
    last = now;
    // ambient liveness
    if (online && peers.length) {
      acc += dt;
      if (acc > 1.4) { acc = 0; spawn(Math.floor(Math.random() * peers.length), Math.random() > 0.5 ? 'out' : 'in', Math.random() > 0.5 ? SIGNAL.tx : SIGNAL.rx); }
    }
    draw(dt);
    requestAnimationFrame(loop);
  }

  resize();
  if ('ResizeObserver' in window) new ResizeObserver(resize).observe(stage);
  else window.addEventListener('resize', resize);
  if (!reduceMotion) requestAnimationFrame((t) => { last = t; loop(t); });

  return { sync };
})();

/* ============================================================
   Controls, modals, forms
   ============================================================ */
function openModal(id) { $(id).classList.add('open'); const f = $(id).querySelector('input, select'); if (f) f.focus(); }
function closeModals() { document.querySelectorAll('.modal').forEach((m) => m.classList.remove('open')); }

$('#node-control').addEventListener('click', async () => {
  if (state.status?.running) {
    try { render(await api('/api/node/stop', {})); showToast('Node stopped.'); }
    catch (error) { showToast(error.message, true); }
  } else openModal('#node-modal');
});
$('#open-task').addEventListener('click', () => openModal('#task-modal'));
$('#open-peer').addEventListener('click', () => openModal('#peer-modal'));
$('#refresh-status').addEventListener('click', () => refresh());
$('#menu-button').addEventListener('click', () => $('#rail').classList.toggle('open'));

document.querySelectorAll('.close-modal, .modal').forEach((el) => {
  el.addEventListener('click', (e) => { if (e.target === el) closeModals(); });
});
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeModals(); });

$('#node-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  try { render(await api('/api/node/start', Object.fromEntries(new FormData(e.currentTarget)))); closeModals(); showToast('Node started.'); }
  catch (error) { showToast(error.message, true); }
});
$('#peer-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  try { render(await api('/api/peers/ping', Object.fromEntries(new FormData(e.currentTarget)))); closeModals(); showToast('PING sent — waiting for PONG.'); }
  catch (error) { showToast(error.message, true); }
});
$('#task-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  try { render(await api('/api/tasks/submit', Object.fromEntries(new FormData(e.currentTarget)))); closeModals(); showToast('Task dispatched to peer.'); }
  catch (error) { showToast(error.message, true); }
});

function syncTaskForm() {
  const select = $('#task-form select');
  const isEcho = select.value === 'echo';
  $('.second-value').hidden = isEcho;
  const first = $('#task-form [name="first"]');
  first.type = isEcho ? 'text' : 'number';
  first.value = isEcho ? 'Hello from MeshWeaver' : '0';
}
$('#task-form select').addEventListener('change', syncTaskForm);

$('#peer-list').addEventListener('click', async (e) => {
  const button = e.target.closest('.peer-ping');
  if (!button) return;
  try { render(await api('/api/peers/ping', { host: button.dataset.host, port: button.dataset.port })); showToast('PING sent.'); }
  catch (error) { showToast(error.message, true); }
});

/* ---------- boot ---------- */
syncTaskForm();
refresh();
setInterval(() => refresh(true), 2500);
