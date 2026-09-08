/* ============================================================
   MeshWeaver — Next-Gen Operations Console JavaScript Engine
   Features:
     - Asynchronous API communication with Flask Broker backend
     - High-Performance Cyber Canvas Radar with packet pulse rays
     - Live SVG Sparkline telemetry curves
     - Interactive Modal Management with Payload Previews
     - Quick Command Palette (Ctrl+K) & Keyboard Shortcuts
     - Filterable real-time wire activity feed
   ============================================================ */

(function () {
  'use strict';

  // Signal Color Matrix
  const COLORS = {
    cyan: '#00F0FF',
    amber: '#F59E0B',
    purple: '#8B5CF6',
    emerald: '#10B981',
    rose: '#F43F5E',
    grid: 'rgba(142, 123, 255, 0.12)',
    textDim: '#94A3B8',
  };

  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

  // Global State
  const state = {
    status: null,
    activityFilter: 'all',
    packetPulses: [],
    history: {
      peers: [0, 0, 0, 0, 0, 0],
      tasks: [0, 0, 0, 0, 0, 0],
      events: [0, 0, 0, 0, 0, 0],
    },
    isPolling: true,
  };

  /* ==================== TOAST & NOTIFICATIONS ==================== */
  const toast = $('#toast');
  let toastTimeout = null;

  function showToast(title, message, isError = false) {
    if (!toast) return;
    const titleEl = toast.querySelector('.toast-title');
    const msgEl = toast.querySelector('.toast-msg');
    const iconEl = toast.querySelector('.toast-icon');

    if (titleEl) titleEl.textContent = title;
    if (msgEl) msgEl.textContent = message;
    if (iconEl) iconEl.textContent = isError ? '✕' : '✓';
    toast.classList.toggle('error', isError);
    toast.classList.add('visible');

    clearTimeout(toastTimeout);
    toastTimeout = setTimeout(() => {
      toast.classList.remove('visible');
    }, 3600);
  }

  /* ==================== API CLIENT ==================== */
  async function api(path, data = null) {
    try {
      const response = await fetch(path, {
        method: data ? 'POST' : 'GET',
        headers: data ? { 'Content-Type': 'application/json' } : {},
        body: data ? JSON.stringify(data) : undefined,
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || 'Server error processing request.');
      }
      return payload;
    } catch (err) {
      throw err;
    }
  }

  function esc(val) {
    return String(val ?? '').replace(/[&<>'"]/g, (c) => ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      "'": '&#39;',
      '"': '&quot;',
    }[c]));
  }

  function pad2(n) {
    return String(n ?? 0).padStart(2, '0');
  }

  // Safe truncation helper for ANY input type (string, number, null, undefined)
  function truncateMiddle(val, front = 10, back = 8) {
    if (val === null || val === undefined) return '—';
    const str = String(val).trim();
    if (!str || str === '—') return '—';
    if (str.length <= front + back) return str;
    return `${str.slice(0, front)}...${str.slice(-back)}`;
  }

  /* ==================== COPY UTILITY ==================== */
  function copyText(text, label = 'Value') {
    if (!text || text === '—') return;
    navigator.clipboard.writeText(String(text)).then(
      () => showToast('Copied', `${label} copied to clipboard!`),
      () => showToast('Copy Failed', 'Unable to copy text.', true)
    );
  }

  /* ==================== SPARKLINE GENERATOR ==================== */
  function updateSparkline(svgSelector, dataPoints, strokeColor) {
    const svg = $(svgSelector);
    if (!svg) return;
    const maxVal = Math.max(...dataPoints, 1);
    const width = 100;
    const height = 30;
    const step = width / (dataPoints.length - 1);

    const points = dataPoints.map((val, idx) => {
      const x = (idx * step).toFixed(1);
      const y = (height - (val / maxVal) * (height - 8) - 4).toFixed(1);
      return `${x},${y}`;
    });

    const pathD = `M${points.join(' L')}`;
    svg.innerHTML = `
      <defs>
        <linearGradient id="grad-${strokeColor.replace(/[^a-zA-Z0-9]/g, '')}" x1="0%" y1="0%" x2="0%" y2="100%">
          <stop offset="0%" stop-color="${strokeColor}" stop-opacity="0.3"/>
          <stop offset="100%" stop-color="${strokeColor}" stop-opacity="0.0"/>
        </linearGradient>
      </defs>
      <path d="${pathD}" fill="none" stroke="${strokeColor}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>
    `;
  }

  /* ==================== UI STATE RENDERER ==================== */
  function render(status) {
    if (!status) return;
    state.status = status;
    const isOnline = !!status.running;

    // Header & Sidebar Online indicators
    const sideCard = $('#sidebar-node-card');
    if (sideCard) sideCard.dataset.online = isOnline ? 'true' : 'false';
    const sideStatusText = $('#side-status-text');
    if (sideStatusText) sideStatusText.textContent = isOnline ? 'Node Active' : 'Node Offline';
    const sideAddrText = $('#side-address-text');
    if (sideAddrText) sideAddrText.textContent = isOnline ? (status.address || 'Bound') : 'No active socket bound';

    const headerBadge = $('#header-status-pill');
    if (headerBadge) {
      headerBadge.dataset.online = isOnline ? 'true' : 'false';
      const label = $('#header-status-label');
      if (label) {
        label.textContent = isOnline
          ? `UDP RUNNING · ${status.address || 'Bound'}`
          : 'SOCKET DISCONNECTED';
      }
    }

    // Toggle Button States
    $$('[data-node-control]').forEach((btn) => {
      btn.dataset.online = isOnline ? 'true' : 'false';
      const label = btn.querySelector('.node-control-text') || btn.querySelector('span') || btn;
      label.textContent = isOnline ? 'Stop Node' : 'Start Node';
    });

    $$('[data-requires-node]').forEach((btn) => {
      btn.disabled = !isOnline;
    });

    // Metrics & Specs
    const peerCount = (status.peers && Array.isArray(status.peers)) ? status.peers.length : 0;
    const onlinePeerCount = (status.peers || []).filter((peer) => String(peer.status).toLowerCase() === 'online').length;
    const summary = status.task_summary || {};
    const taskCount = Object.values(summary).reduce((total, value) => total + Number(value || 0), 0)
      || ((status.tasks && Array.isArray(status.tasks)) ? status.tasks.length : 0);
    const eventCount = (status.events && Array.isArray(status.events)) ? status.events.length : 0;

    const peerCountEl = $('#peer-count');
    if (peerCountEl) peerCountEl.textContent = pad2(peerCount);
    const taskCountEl = $('#task-count');
    if (taskCountEl) taskCountEl.textContent = pad2(taskCount);
    const eventCountEl = $('#event-count');
    if (eventCountEl) eventCountEl.textContent = pad2(eventCount);

    const sidePeerCounter = $('#side-peer-count');
    if (sidePeerCounter) sidePeerCounter.textContent = peerCount;

    // Shift History & Update Sparklines
    state.history.peers.push(peerCount);
    state.history.peers.shift();
    state.history.tasks.push(taskCount);
    state.history.tasks.shift();
    state.history.events.push(eventCount);
    state.history.events.shift();

    updateSparkline('#spark-peers svg', state.history.peers, COLORS.cyan);
    updateSparkline('#spark-tasks svg', state.history.tasks, COLORS.amber);
    updateSparkline('#spark-events svg', state.history.events, COLORS.purple);

    // Node Specification Card
    const nodeIdEl = $('#node-id');
    const nodeAddrEl = $('#node-address');
    const copyIdBtn = $('#btn-copy-id');
    const copyAddrBtn = $('#btn-copy-addr');
    const transportBadge = $('#transport-online-badge');

    if (transportBadge) {
      transportBadge.textContent = isOnline ? 'CONNECTED' : 'OFFLINE';
      transportBadge.classList.toggle('active', isOnline);
    }

    if (isOnline) {
      const formattedNodeId = String(status.node_id || '');
      if (nodeIdEl) {
        nodeIdEl.textContent = formattedNodeId || '—';
        nodeIdEl.title = formattedNodeId;
      }
      if (nodeAddrEl) nodeAddrEl.textContent = status.address || '—';
      if (copyIdBtn) copyIdBtn.disabled = false;
      if (copyAddrBtn) copyAddrBtn.disabled = false;
    } else {
      if (nodeIdEl) nodeIdEl.textContent = '—';
      if (nodeAddrEl) nodeAddrEl.textContent = '—';
      if (copyIdBtn) copyIdBtn.disabled = true;
      if (copyAddrBtn) copyAddrBtn.disabled = true;
    }

    const localMetrics = status.local_metrics || {};
    const localMetricsEl = $('#local-metrics');
    if (localMetricsEl) {
      localMetricsEl.textContent = isOnline
        ? `${Number(localMetrics.cpu_percent ?? 0).toFixed(1)}% / ${Number(localMetrics.memory_percent ?? 0).toFixed(1)}%`
        : '— / —';
    }
    const taskSummaryEl = $('#task-summary');
    if (taskSummaryEl) {
      taskSummaryEl.textContent = `${summary.PENDING || 0} pending · ${summary.DISPATCHED || 0} running`;
    }
    const securityState = $('#security-status');
    if (securityState) {
      const enabled = !!status.security?.signing_enabled;
      securityState.dataset.enabled = enabled ? 'true' : 'false';
      securityState.textContent = enabled ? 'HMAC ON' : 'HMAC OFF';
    }

    // Radar Center Hub
    const radarCore = $('#radar-core');
    if (radarCore) {
      radarCore.dataset.online = isOnline ? 'true' : 'false';
      const coreId = $('#radar-core-id');
      if (coreId) coreId.textContent = isOnline ? truncateMiddle(status.node_id, 8, 6) : 'LOCAL NODE';
      const coreSub = $('#radar-core-sub');
      if (coreSub) coreSub.textContent = isOnline ? `BOUND : ${status.address || 'UDP'}` : 'SOCKET UNBOUND';
    }

    const radarCaption = $('#radar-caption');
    if (radarCaption) {
      if (!isOnline) {
        radarCaption.textContent = 'Transport offline. Launch node to initialize mesh topology.';
      } else if (peerCount === 0) {
        radarCaption.textContent = `Node active on ${status.address}. Ping a peer (e.g. 127.0.0.1:4801) to link onto radar.`;
      } else {
        radarCaption.textContent = `${onlinePeerCount} online / ${peerCount} known peer(s). Heartbeat and packet telemetry streaming.`;
      }
    }

    // Reachable Peers Hub Render
    renderPeers(status.peers || []);

    // Activity Feed Render
    renderActivity(status.events || []);

    // Task Execution Log Render
    renderTasks(status.tasks || []);

    // Update Radar Visualization State
    radarEngine.sync(status);
  }

  /* ==================== PEERS HUB RENDERER ==================== */
  function renderPeers(peers) {
    const list = $('#peer-list');
    if (!list) return;

    if (!peers || peers.length === 0) {
      list.innerHTML = `
        <div class="empty-state-box">
          <div class="empty-icon-halo">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="12" cy="12" r="10"/><path d="M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20"/><path d="M2 12h20"/></svg>
          </div>
          <h3>No Peers Discovered Yet</h3>
          <p>Start your node and send a UDP PING to bring neighbouring nodes onto the mesh radar.</p>
          <button class="btn-action btn-secondary btn-sm" data-open-peer ${!state.status?.running ? 'disabled' : ''}>Ping 127.0.0.1:4801</button>
        </div>
      `;
      return;
    }

    list.innerHTML = peers
      .map((peer) => {
        const rawName = String(peer.name ?? 'P');
        const initial = rawName.charAt(0).toUpperCase() || 'P';
        const displayId = truncateMiddle(rawName, 8, 6);
        const isOnline = String(peer.status || 'Online').toLowerCase() === 'online';
        const cpu = peer.cpu_percent == null ? '—' : `${Number(peer.cpu_percent).toFixed(1)}%`;
        const ram = peer.memory_percent == null ? '—' : `${Number(peer.memory_percent).toFixed(1)}%`;
        const seen = peer.last_seen_ago == null ? 'never' : `${Number(peer.last_seen_ago).toFixed(1)}s ago`;
        return `
        <div class="peer-card-item">
          <div class="peer-main-info">
            <div class="peer-avatar">${esc(initial)}</div>
            <div class="peer-meta">
              <div class="peer-name-row">
                <span class="peer-node-id" title="${esc(rawName)}">${esc(displayId)}</span>
                <span class="peer-online-tag ${isOnline ? '' : 'offline'}">${isOnline ? 'ONLINE' : 'OFFLINE'}</span>
              </div>
              <span class="peer-endpoint">${esc(peer.host)}:${esc(peer.port)}</span>
              <span class="peer-telemetry">
                <span>CPU <b>${esc(cpu)}</b></span>
                <span>RAM <b>${esc(ram)}</b></span>
                <span class="peer-heartbeat ${isOnline ? '' : 'offline'}">HB ${esc(seen)}</span>
              </span>
            </div>
          </div>
          <div class="peer-actions-row">
            <button class="btn-peer-ping" data-ping-host="${esc(peer.host)}" data-ping-port="${esc(peer.port)}">Ping</button>
            <button class="btn-micro" data-task-host="${esc(peer.host)}" data-task-port="${esc(peer.port)}">Dispatch</button>
          </div>
        </div>
      `;
      })
      .join('');
  }

  /* ==================== ACTIVITY STREAM RENDERER ==================== */
  function renderActivity(events) {
    const list = $('#activity-list');
    if (!list) return;

    if (!events || events.length === 0) {
      list.innerHTML = `
        <div class="empty-state-box">
          <div class="empty-icon-halo">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
          </div>
          <h3>No Activity On The Wire</h3>
          <p>Datagram events, heartbeats, and task completions will stream here in real time.</p>
        </div>
      `;
      return;
    }

    const filtered = events.filter((e) => {
      if (state.activityFilter === 'all') return true;
      if (state.activityFilter === 'send') return e.type === 'send';
      if (state.activityFilter === 'peer') return e.type === 'peer';
      if (state.activityFilter === 'tasks') return e.type === 'success' || e.type === 'error';
      return true;
    });

    const GLYPHS = {
      send: '↑',
      peer: '↓',
      success: '✓',
      error: '!',
      system: '●',
    };

    list.innerHTML = filtered
      .map((e) => {
        const kind = GLYPHS[e.type] ? e.type : 'system';
        return `
        <div class="activity-item">
          <div class="activity-symbol sym-${kind}">${GLYPHS[kind] || '●'}</div>
          <div class="activity-body">
            <div class="activity-type-title">${esc(e.type)}</div>
            <div class="activity-msg">${esc(e.message)}</div>
          </div>
          <time class="activity-time">${esc(e.time)}</time>
        </div>
      `;
      })
      .join('');
  }

  /* ==================== TASK LOG RENDERER ==================== */
  function renderTasks(tasks) {
    const tbody = $('#task-list');
    if (!tbody) return;

    if (!tasks || tasks.length === 0) {
      tbody.innerHTML = `
        <tr>
          <td colspan="5" class="table-empty">
            <div class="empty-table-prompt">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><polygon points="5 3 19 12 5 21 5 3"/></svg>
              <span>No workloads executed this session. Dispatch an echo, add, or multiply task.</span>
            </div>
          </td>
        </tr>
      `;
      return;
    }

    tbody.innerHTML = tasks
      .map((t) => {
        const statusStr = String(t.status || 'Dispatched');
        const isCompleted = statusStr.toLowerCase() === 'completed';
        const isFailed = statusStr.toLowerCase() === 'failed';
        const resultDisplay = (t.result !== undefined && t.result !== null)
          ? `<strong style="color: var(--color-emerald); font-family: var(--font-mono); font-size: 12px;">➔ ${esc(t.result)}</strong>`
          : (isFailed && t.error)
            ? `<strong style="color: var(--color-rose); font-family: var(--font-mono); font-size: 12px;">${esc(t.error)}</strong>`
            : `<span style="color: var(--color-cyan); font-size: 11px;">${esc(t.args ? `args(${t.args})` : `op='${t.name}'`)}</span>`;
        const attempts = `${Number(t.attempts || 0)}/${Number(t.max_attempts || 0)}`;
        const routeStatus = t.route_status ? String(t.route_status).toUpperCase() : '—';
        const history = Array.isArray(t.history) ? t.history : [];
        const historyDisplay = history.length
          ? `<details class="task-history"><summary>${esc(attempts)} attempts · history</summary><ul class="task-history-list">${history.map((item) => `<li>${esc(item)}</li>`).join('')}</ul></details>`
          : '';
        return `
        <tr>
          <td>
            <span class="op-pill">
              <svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"/></svg>
              ${esc(t.name)}()
            </span>
          </td>
          <td><code>${esc(t.target)}</code></td>
          <td>${esc(t.time)}</td>
          <td>
            <div class="task-route-detail">
              <div class="task-result-line">${resultDisplay}</div>
              <span class="task-route-line">route: ${esc(routeStatus)} · attempts: ${esc(attempts)}</span>
              ${historyDisplay}
            </div>
          </td>
          <td>
            <span class="status-glow-pill ${isCompleted ? 'completed' : (isFailed ? 'failed' : '')}">${esc(statusStr)}</span>
          </td>
        </tr>
      `;
      })
      .join('');
  }

  /* ==================== CYBER RADAR CANVAS ENGINE ==================== */
  const radarEngine = (() => {
    const stage = $('#radar-stage');
    const canvas = $('#mesh-radar');
    if (!canvas || !stage) return { sync: () => {}, addPulse: () => {} };

    const ctx = canvas.getContext('2d');
    let width = 0,
      height = 0,
      cx = 0,
      cy = 0;
    let angle = 0;
    let peers = [];
    let pulses = [];

    function resize() {
      const rect = stage.getBoundingClientRect();
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      width = rect.width;
      height = rect.height;
      canvas.width = width * dpr;
      canvas.height = height * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      cx = width / 2;
      cy = height / 2;
    }

    window.addEventListener('resize', resize);
    setTimeout(resize, 50);

    function sync(status) {
      if (!status || !status.peers || !Array.isArray(status.peers)) {
        peers = [];
        return;
      }
      const count = status.peers.length;
      const radius = Math.min(width, height) * 0.36;

      peers = status.peers.map((peer, i) => {
        const theta = (i / count) * Math.PI * 2 - Math.PI / 2;
        const nameStr = String(peer.name ?? 'P');
        return {
          ...peer,
          x: cx + Math.cos(theta) * radius,
          y: cy + Math.sin(theta) * radius,
          online: String(peer.status || 'Online').toLowerCase() === 'online',
          initial: nameStr.charAt(0).toUpperCase() || 'P',
        };
      });
    }

    function addPulse(fromX, fromY, toX, toY, color) {
      pulses.push({
        fromX,
        fromY,
        toX,
        toY,
        progress: 0,
        color,
      });
    }

    function draw() {
      ctx.clearRect(0, 0, width, height);

      if (width === 0) {
        resize();
      }

      const isOnline = !!state.status?.running;
      const maxR = Math.min(width, height) * 0.44;

      // 1. Concentric Sonar Rings
      [0.25, 0.5, 0.75, 1.0].forEach((ratio) => {
        ctx.beginPath();
        ctx.arc(cx, cy, maxR * ratio, 0, Math.PI * 2);
        ctx.strokeStyle = isOnline ? 'rgba(142, 123, 255, 0.15)' : 'rgba(255, 255, 255, 0.04)';
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.stroke();
        ctx.setLineDash([]);
      });

      // 2. Crosshair Grid Rays
      ctx.beginPath();
      ctx.moveTo(cx - maxR, cy);
      ctx.lineTo(cx + maxR, cy);
      ctx.moveTo(cx, cy - maxR);
      ctx.lineTo(cx, cy + maxR);
      ctx.strokeStyle = isOnline ? 'rgba(142, 123, 255, 0.08)' : 'rgba(255, 255, 255, 0.03)';
      ctx.lineWidth = 1;
      ctx.stroke();

      // 3. Rotating Radar Sweep Beam
      if (isOnline) {
        ctx.save();
        const sweepGradient = ctx.createConicGradient(angle, cx, cy);
        sweepGradient.addColorStop(0, 'rgba(0, 240, 255, 0.18)');
        sweepGradient.addColorStop(0.12, 'rgba(0, 240, 255, 0.0)');
        sweepGradient.addColorStop(1, 'rgba(0, 240, 255, 0.0)');

        ctx.fillStyle = sweepGradient;
        ctx.beginPath();
        ctx.arc(cx, cy, maxR, 0, Math.PI * 2);
        ctx.fill();

        // Laser Leading Line
        ctx.beginPath();
        ctx.moveTo(cx, cy);
        ctx.lineTo(cx + Math.cos(angle) * maxR, cy + Math.sin(angle) * maxR);
        ctx.strokeStyle = 'rgba(0, 240, 255, 0.5)';
        ctx.lineWidth = 1.5;
        ctx.stroke();
        ctx.restore();

        angle += 0.022;
        if (angle > Math.PI * 2) angle = 0;
      }

      // 4. Peer Links & Orbiting Nodes
      peers.forEach((peer) => {
        // Link Beam Line
        ctx.beginPath();
        ctx.moveTo(cx, cy);
        ctx.lineTo(peer.x, peer.y);
        ctx.strokeStyle = isOnline ? 'rgba(0, 240, 255, 0.25)' : 'rgba(255, 255, 255, 0.08)';
        ctx.lineWidth = 1.5;
        ctx.setLineDash([2, 4]);
        ctx.stroke();
        ctx.setLineDash([]);

        // Peer Glowing Outer Halo
        ctx.beginPath();
        ctx.arc(peer.x, peer.y, 18, 0, Math.PI * 2);
        ctx.fillStyle = 'rgba(0, 240, 255, 0.12)';
        ctx.fill();
        ctx.strokeStyle = peer.online ? COLORS.cyan : COLORS.rose;
        ctx.lineWidth = 1.5;
        ctx.stroke();

        // Node Inner Dot
        ctx.beginPath();
        ctx.arc(peer.x, peer.y, 7, 0, Math.PI * 2);
        ctx.fillStyle = peer.online ? COLORS.cyan : COLORS.rose;
        ctx.fill();

        // Node Label
        ctx.font = '600 10px "JetBrains Mono", monospace';
        ctx.fillStyle = '#FFFFFF';
        ctx.textAlign = 'center';
        ctx.fillText(`${peer.host}:${peer.port}`, peer.x, peer.y + 32);
      });

      // 5. Animated Packet Pulses
      for (let i = pulses.length - 1; i >= 0; i--) {
        const p = pulses[i];
        p.progress += 0.035;

        const curX = p.fromX + (p.toX - p.fromX) * p.progress;
        const curY = p.fromY + (p.toY - p.fromY) * p.progress;

        ctx.beginPath();
        ctx.arc(curX, curY, 4, 0, Math.PI * 2);
        ctx.fillStyle = p.color;
        ctx.shadowColor = p.color;
        ctx.shadowBlur = 10;
        ctx.fill();
        ctx.shadowBlur = 0;

        if (p.progress >= 1) {
          pulses.splice(i, 1);
        }
      }

      // Random spontaneous packet pulse on active nodes
      if (isOnline && peers.length > 0 && Math.random() < 0.03) {
        const randomPeer = peers[Math.floor(Math.random() * peers.length)];
        if (Math.random() > 0.5) {
          addPulse(cx, cy, randomPeer.x, randomPeer.y, COLORS.amber); // TX
        } else {
          addPulse(randomPeer.x, randomPeer.y, cx, cy, COLORS.cyan); // RX
        }
      }

      requestAnimationFrame(draw);
    }

    requestAnimationFrame(draw);

    return { sync, addPulse };
  })();

  /* ==================== MODALS & USER ACTIONS ==================== */
  function openModal(modalId) {
    const modal = $(modalId);
    if (!modal) return;
    modal.classList.add('active');
    modal.setAttribute('aria-hidden', 'false');
    const firstInput = modal.querySelector('input');
    if (firstInput) setTimeout(() => firstInput.focus(), 50);
  }

  function closeModal(modal) {
    if (!modal) return;
    modal.classList.remove('active');
    modal.setAttribute('aria-hidden', 'true');
  }

  function setupModals() {
    // Open Triggers
    $$('[data-open-peer]').forEach((btn) => {
      btn.addEventListener('click', () => openModal('#peer-modal'));
    });

    $$('[data-open-task]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const host = btn.dataset.taskHost;
        const port = btn.dataset.taskPort;
        if (host && port) {
          const hostInput = $('#task-host');
          const portInput = $('#task-port');
          if (hostInput) hostInput.value = host;
          if (portInput) portInput.value = port;
        }
        updateTaskPreview();
        openModal('#task-modal');
      });
    });

    // Close Triggers
    $$('.modal-backdrop').forEach((backdrop) => {
      backdrop.addEventListener('click', (e) => {
        if (e.target === backdrop) closeModal(backdrop);
      });
      const closeBtn = backdrop.querySelector('.modal-close');
      if (closeBtn) closeBtn.addEventListener('click', () => closeModal(backdrop));
      const cancelBtn = backdrop.querySelector('.modal-cancel');
      if (cancelBtn) cancelBtn.addEventListener('click', () => closeModal(backdrop));
    });

    // Preset Port Buttons in Modals
    $$('[data-set-port]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const portInput = $('#node-port');
        if (portInput) portInput.value = btn.dataset.setPort;
      });
    });

    $$('[data-set-peer]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const [host, port] = btn.dataset.setPeer.split(':');
        const hostInput = $('#peer-host');
        const portInput = $('#peer-port');
        if (hostInput) hostInput.value = host;
        if (portInput) portInput.value = port;
      });
    });

    // Dynamic Task Form Adaptation
    const taskOp = $('#task-op');
    const firstInput = $('#task-first');
    const secondInput = $('#task-second');
    const secondBox = $('#second-val-box');
    const firstLabel = $('#label-first');
    const routingSelect = $('#task-routing');
    const targetRow = $('#task-target-row');
    const targetInputs = [$('#task-host'), $('#task-port')].filter(Boolean);

    function updateTaskPreview() {
      if (!taskOp || !firstInput) return;
      const op = taskOp.value;
      const first = firstInput.value || '';
      const second = secondInput?.value || '';
      const host = $('#task-host')?.value || '127.0.0.1';
      const port = $('#task-port')?.value || '4801';
      const routing = routingSelect?.value || 'direct';
      const previewCode = $('#task-preview-code');

      if (targetRow) targetRow.style.display = routing === 'least-loaded' ? 'none' : 'flex';
      targetInputs.forEach((input) => { input.required = routing !== 'least-loaded'; });
      const destination = routing === 'least-loaded' ? 'router.select_least_loaded_peer()' : `'${host}:${port}'`;

      if (op === 'echo') {
        if (secondBox) secondBox.style.display = 'none';
        if (firstLabel) firstLabel.textContent = 'Echo Text Message';
        if (previewCode) previewCode.textContent = `meshweaver.dispatch(echo, args=('${first}',), route=${destination})`;
      } else {
        if (secondBox) secondBox.style.display = 'flex';
        if (firstLabel) firstLabel.textContent = 'Left Value (Number)';
        if (previewCode) previewCode.textContent = `meshweaver.dispatch(${op}, args=(${first || 0}, ${second || 0}), route=${destination})`;
      }
    }

    if (taskOp && firstInput) {
      taskOp.addEventListener('change', () => {
        if (taskOp.value === 'echo') {
          firstInput.value = 'Hello MeshWeaver 🚀';
        } else if (taskOp.value === 'add') {
          firstInput.value = '15';
          if (secondInput) secondInput.value = '27';
        } else if (taskOp.value === 'multiply') {
          firstInput.value = '6';
          if (secondInput) secondInput.value = '7';
        }
        updateTaskPreview();
      });
      firstInput.addEventListener('input', updateTaskPreview);
      if (secondInput) secondInput.addEventListener('input', updateTaskPreview);
      $('#task-host')?.addEventListener('input', updateTaskPreview);
      $('#task-port')?.addEventListener('input', updateTaskPreview);
      routingSelect?.addEventListener('change', updateTaskPreview);
      updateTaskPreview();
    }

    // Node Form Submit
    const nodeForm = $('#node-form');
    if (nodeForm) {
      nodeForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const host = ($('#node-host')?.value || '127.0.0.1').trim();
        const port = ($('#node-port')?.value || '4800').trim();
        try {
          const res = await api('/api/node/start', { host, port });
          closeModal($('#node-modal'));
          render(res);
          showToast('Node Online', `Listening on ${res.address || `${host}:${port}`}`);
        } catch (err) {
          showToast('Initialization Error', err.message, true);
        }
      });
    }

    // Peer Form Submit
    const peerForm = $('#peer-form');
    if (peerForm) {
      peerForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const host = ($('#peer-host')?.value || '127.0.0.1').trim();
        const port = ($('#peer-port')?.value || '4801').trim();
        try {
          const res = await api('/api/peers/ping', { host, port });
          closeModal($('#peer-modal'));
          render(res);
          showToast('PING Transmitted', `Datagram dispatched to ${host}:${port}`);
        } catch (err) {
          showToast('Ping Failed', err.message, true);
        }
      });
    }

    // Task Form Submit
    const taskForm = $('#task-form');
    if (taskForm) {
      taskForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const payload = {
          operation: taskOp?.value || 'echo',
          first: (firstInput?.value || '').trim(),
          second: (secondInput?.value || '').trim(),
          routing: routingSelect?.value || 'direct',
          host: ($('#task-host')?.value || '127.0.0.1').trim(),
          port: ($('#task-port')?.value || '4801').trim(),
        };
        try {
          const res = await api('/api/tasks/submit', payload);
          closeModal($('#task-modal'));
          render(res);
          const destination = payload.routing === 'least-loaded' ? 'the least-loaded online peer' : `${payload.host}:${payload.port}`;
          showToast('Task Dispatched', `${payload.operation}() routed to ${destination}`);
        } catch (err) {
          showToast('Task Dispatch Error', err.message, true);
        }
      });
    }
  }

  /* ==================== GLOBAL CONTROLS ==================== */
  function setupGlobalControls() {
    // Start/Stop Node Toggle Buttons
    $$('[data-node-control]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        const isOnline = !!state.status?.running;
        if (isOnline) {
          try {
            const res = await api('/api/node/stop', {});
            render(res);
            showToast('Node Stopped', 'Socket successfully unbound.');
          } catch (err) {
            showToast('Stop Failed', err.message, true);
          }
        } else {
          openModal('#node-modal');
        }
      });
    });

    // Refresh Status Button
    const refreshBtn = $('#refresh-status');
    if (refreshBtn) {
      refreshBtn.addEventListener('click', async () => {
        try {
          const res = await api('/api/status');
          render(res);
          showToast('Refreshed', 'Telemetry snapshot updated.');
        } catch (err) {
          showToast('Sync Error', err.message, true);
        }
      });
    }

    // Copy Buttons
    $('#btn-copy-id')?.addEventListener('click', () => {
      copyText(state.status?.node_id, 'Node ID');
    });

    $('#btn-copy-addr')?.addEventListener('click', () => {
      copyText(state.status?.address, 'Bound Address');
    });

    // Activity Stream Filters
    $$('#activity-filters .pill-btn').forEach((btn) => {
      btn.addEventListener('click', () => {
        $$('#activity-filters .pill-btn').forEach((b) => b.classList.remove('active'));
        btn.classList.add('active');
        state.activityFilter = btn.dataset.filter;
        renderActivity(state.status?.events || []);
      });
    });

    // Dynamic Delegate for Peer Card Actions
    document.addEventListener('click', async (e) => {
      const pingBtn = e.target.closest('[data-ping-host]');
      if (pingBtn) {
        const host = pingBtn.dataset.pingHost;
        const port = pingBtn.dataset.pingPort;
        try {
          const res = await api('/api/peers/ping', { host, port });
          render(res);
          showToast('Ping Transmitted', `PING sent to ${host}:${port}`);
        } catch (err) {
          showToast('Ping Failed', err.message, true);
        }
      }

      const taskBtn = e.target.closest('[data-task-host]');
      if (taskBtn) {
        const hostInput = $('#task-host');
        const portInput = $('#task-port');
        const routingSelect = $('#task-routing');
        if (hostInput) hostInput.value = taskBtn.dataset.taskHost;
        if (portInput) portInput.value = taskBtn.dataset.taskPort;
        if (routingSelect) {
          routingSelect.value = 'direct';
          routingSelect.dispatchEvent(new Event('change'));
        }
        openModal('#task-modal');
      }
    });

    // Mobile Sidebar Toggle
    const mobileBtn = $('#mobile-menu-btn');
    const sidebar = $('#sidebar');
    if (mobileBtn && sidebar) {
      mobileBtn.addEventListener('click', () => {
        sidebar.classList.toggle('open');
      });
    }
  }

  /* ==================== COMMAND PALETTE (CTRL+K) ==================== */
  function setupCommandPalette() {
    const trigger = $('#cmd-palette-trigger');
    const modal = $('#cmd-modal');
    const input = $('#cmd-input');

    if (!modal || !trigger) return;

    trigger.addEventListener('click', () => {
      openModal('#cmd-modal');
      if (input) input.value = '';
    });

    // Keyboard Shortcuts
    window.addEventListener('keydown', (e) => {
      // Ctrl+K or Cmd+K
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        modal.classList.contains('active') ? closeModal(modal) : openModal('#cmd-modal');
      }

      // Escape key closes modals
      if (e.key === 'Escape') {
        $$('.modal-backdrop.active').forEach((m) => closeModal(m));
      }

      // Quick hotkeys when not typing in an input
      if (document.activeElement.tagName !== 'INPUT' && document.activeElement.tagName !== 'SELECT') {
        if (e.key.toLowerCase() === 'n') {
          e.preventDefault();
          $('[data-node-control]')?.click();
        } else if (e.key.toLowerCase() === 'p') {
          e.preventDefault();
          if (state.status?.running) openModal('#peer-modal');
        } else if (e.key.toLowerCase() === 't') {
          e.preventDefault();
          if (state.status?.running) openModal('#task-modal');
        } else if (e.key.toLowerCase() === 'r') {
          e.preventDefault();
          $('#refresh-status')?.click();
        }
      }
    });

    // Command Item Click Dispatcher
    $$('.cmd-item').forEach((item) => {
      item.addEventListener('click', () => {
        const action = item.dataset.action;
        closeModal(modal);

        if (action === 'toggle-node') {
          $('[data-node-control]')?.click();
        } else if (action === 'open-ping') {
          if (!state.status?.running) {
            showToast('Action Blocked', 'Please start the local node first.', true);
          } else {
            openModal('#peer-modal');
          }
        } else if (action === 'open-task') {
          if (!state.status?.running) {
            showToast('Action Blocked', 'Please start the local node first.', true);
          } else {
            openModal('#task-modal');
          }
        } else if (action === 'refresh') {
          $('#refresh-status')?.click();
        }
      });
    });

    // Real-time Search Filter in Command Palette
    if (input) {
      input.addEventListener('input', () => {
        const q = input.value.toLowerCase().trim();
        $$('.cmd-item').forEach((item) => {
          const text = item.textContent.toLowerCase();
          item.style.display = text.includes(q) ? 'flex' : 'none';
        });
      });
    }
  }

  /* ==================== REAL-TIME TELEMETRY POLLING ==================== */
  async function pollStatus() {
    if (!state.isPolling) return;
    try {
      const res = await api('/api/status');
      render(res);
    } catch (_) {
      // Background poll failure handled silently
    }
  }

  document.addEventListener('visibilitychange', () => {
    state.isPolling = !document.hidden;
  });

  /* ==================== INITIALIZATION ==================== */
  document.addEventListener('DOMContentLoaded', () => {
    setupModals();
    setupGlobalControls();
    setupCommandPalette();

    // Initial snapshot fetch
    pollStatus();

    // 1.5s Polling Interval for crisp live real-time updates
    setInterval(pollStatus, 1500);
  });
})();
