(function () {
  // =====================================================
  // Config & state
  // =====================================================
  const CITIES = ["san_francisco", "new_york", "austin", "seattle", "chicago", "miami"];
  const STATUS_COLORS = {
    requested: "#F5A623",
    accepted: "#3ED6C6",
    in_progress: "#3ED6C6",
    completed: "#35D399",
    cancelled: "#FF5C5C"
  };

  // Hosted API the dashboard connects to on load. Override with ?api=<url>,
  // or open with ?demo=1 to stay on the simulated stream.
  const DEFAULT_API = "https://fleetline-api.onrender.com";

  let mode = "demo"; // "demo" | "live"
  let apiBase = "";
  let pollTimer = null;
  let demoTimer = null;     // demo ticks while the live API is waking up
  let liveConnected = false;
  let pollInFlight = false;
  let seenEvents = new Set();

  let trendSeries = []; // { t, events }
  let statusCounts = { requested: 0, accepted: 0, in_progress: 0, completed: 0, cancelled: 0 };
  let cityCounts = {};
  CITIES.forEach(c => cityCounts[c] = 0);

  // Active "trips" for the route map animation
  let activeTrips = [];
  let pulseRings = [];

  // =====================================================
  // DOM refs
  // =====================================================
  const settingsBtn = document.getElementById('settingsBtn');
  const settingsPanel = document.getElementById('settingsPanel');
  const connectBtn = document.getElementById('connectBtn');
  const disconnectBtn = document.getElementById('disconnectBtn');
  const apiUrlInput = document.getElementById('apiUrl');
  const modeDot = document.getElementById('modeDot');
  const modeLabel = document.getElementById('modeLabel');
  const feedEl = document.getElementById('feed');
  const donutLegend = document.getElementById('donutLegend');
  const cityBarsEl = document.getElementById('cityBars');
  const healthListEl = document.getElementById('healthList');

  settingsBtn.addEventListener('click', () => settingsPanel.classList.toggle('open'));

  connectBtn.addEventListener('click', () => {
    const val = apiUrlInput.value.trim();
    if (!val) return;
    settingsPanel.classList.remove('open');
    goLive(val);
  });

  disconnectBtn.addEventListener('click', () => {
    mode = "demo";
    liveConnected = false;
    setStatus("DEMO STREAM", true);
    settingsPanel.classList.remove('open');
    restartPolling();
  });

  function setStatus(text, isDemo) {
    modeLabel.textContent = text;
    modeDot.classList.toggle('demo', isDemo);
  }

  function goLive(url) {
    apiBase = url.replace(/\/$/, "");
    apiUrlInput.value = apiBase;
    mode = "live";
    liveConnected = false;
    seenEvents = new Set();
    setStatus("CONNECTING…", true);
    restartPolling();
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  // =====================================================
  // Feed
  // =====================================================
  function pushFeedItem(event) {
    const item = document.createElement('div');
    item.className = 'feed-item';
    const fareText = event.fare ? `$${Number(event.fare).toFixed(2)}` : '';
    item.innerHTML = `
      <span class="tag ${event.status}">${event.status.replace('_', ' ')}</span>
      <span class="feed-text">${escapeHtml(event.city.replace('_', ' '))} · trip ${escapeHtml(String(event.trip_id).slice(0, 8))}</span>
      <span class="feed-meta">${fareText}</span>
    `;
    feedEl.prepend(item);
    while (feedEl.children.length > 30) feedEl.removeChild(feedEl.lastChild);
  }

  // =====================================================
  // Stat cards
  // =====================================================
  function renderSummary(s) {
    document.getElementById('statTrips24h').textContent = (s.trips_24h ?? 0).toLocaleString();
    document.getElementById('statActive').textContent = (s.trips_active ?? 0).toLocaleString();
    document.getElementById('statCompleted').textContent = (s.completed_24h ?? 0).toLocaleString();
    document.getElementById('statCancelled').textContent = (s.cancelled_24h ?? 0).toLocaleString();
    document.getElementById('statFare').textContent = s.avg_fare_24h != null ? `$${Number(s.avg_fare_24h).toFixed(2)}` : '–';
  }

  // =====================================================
  // Donut (status breakdown)
  // =====================================================
  function renderDonut(counts) {
    const svg = document.getElementById('statusDonut');
    // remove previously drawn arcs (keep the first base circle)
    [...svg.querySelectorAll('circle.arc')].forEach(el => el.remove());

    const total = Object.values(counts).reduce((a, b) => a + b, 0) || 1;
    const circumference = 2 * Math.PI * 52;
    const order = ["requested", "in_progress", "completed", "cancelled"];
    let offset = 0;

    donutLegend.innerHTML = '';
    order.forEach(key => {
      const val = (counts[key] || 0) + (key === 'in_progress' ? (counts.accepted || 0) : 0);
      const fraction = val / total;
      const len = fraction * circumference;

      const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      circle.setAttribute("class", "arc");
      circle.setAttribute("cx", "65");
      circle.setAttribute("cy", "65");
      circle.setAttribute("r", "52");
      circle.setAttribute("fill", "none");
      circle.setAttribute("stroke", STATUS_COLORS[key]);
      circle.setAttribute("stroke-width", "16");
      circle.setAttribute("stroke-dasharray", `${len} ${circumference - len}`);
      circle.setAttribute("stroke-dashoffset", String(-(offset * circumference)));
      circle.setAttribute("transform", "rotate(-90 65 65)");
      svg.appendChild(circle);

      offset += fraction;

      const row = document.createElement('div');
      row.className = 'row';
      row.innerHTML = `<span class="swatch" style="background:${STATUS_COLORS[key]}"></span><span class="label">${key.replace('_', ' ')}</span><span class="value">${val}</span>`;
      donutLegend.appendChild(row);
    });
  }

  // =====================================================
  // City bars
  // =====================================================
  function renderCityBars(counts) {
    const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    const max = Math.max(1, ...entries.map(e => e[1]));
    cityBarsEl.innerHTML = '';
    entries.forEach(([city, count]) => {
      const row = document.createElement('div');
      row.className = 'hbar-row';
      row.innerHTML = `
        <span class="hbar-city">${city.replace('_', ' ')}</span>
        <span class="hbar-track"><span class="hbar-fill" style="width:${(count / max * 100)}%"></span></span>
        <span class="hbar-count">${count}</span>
      `;
      cityBarsEl.appendChild(row);
    });
  }

  // =====================================================
  // Pipeline health
  // =====================================================
  function renderHealth(components) {
    healthListEl.innerHTML = '';
    components.forEach(c => {
      const row = document.createElement('div');
      row.className = 'health-row';
      row.innerHTML = `
        <span class="health-name">${c.component.replace('_', ' ')}</span>
        <span class="health-status ${c.status}">${c.status}</span>
      `;
      healthListEl.appendChild(row);
    });
    const worst = components.some(c => c.status === 'down') ? null :
                  components.some(c => c.status === 'degraded') ? 99.5 : 99.9;
    if (worst) document.getElementById('statUptime').textContent = worst.toFixed(1) + '%';
  }

  // =====================================================
  // Trend chart (canvas line chart, responsive)
  // =====================================================
  const trendCanvas = document.getElementById('trendChart');
  const trendCtx = trendCanvas.getContext('2d');

  function resizeCanvasToContainer(canvas) {
    const rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * devicePixelRatio;
    canvas.height = rect.height * devicePixelRatio;
    const ctx = canvas.getContext('2d');
    ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
    return rect;
  }

  function drawTrend() {
    const rect = trendCanvas.getBoundingClientRect();
    trendCtx.clearRect(0, 0, rect.width, rect.height);
    if (trendSeries.length < 2) return;

    const values = trendSeries.map(d => d.events);
    const maxVal = Math.max(1, ...values);
    const padding = 8;
    const w = rect.width - padding * 2;
    const h = rect.height - padding * 2;
    const step = w / (trendSeries.length - 1);

    // grid lines
    trendCtx.strokeStyle = 'rgba(167,159,191,0.12)';
    trendCtx.lineWidth = 1;
    for (let i = 0; i <= 3; i++) {
      const y = padding + (h / 3) * i;
      trendCtx.beginPath();
      trendCtx.moveTo(padding, y);
      trendCtx.lineTo(rect.width - padding, y);
      trendCtx.stroke();
    }

    // area fill
    trendCtx.beginPath();
    trendCtx.moveTo(padding, padding + h - (values[0] / maxVal) * h);
    values.forEach((v, i) => {
      const x = padding + i * step;
      const y = padding + h - (v / maxVal) * h;
      trendCtx.lineTo(x, y);
    });
    trendCtx.lineTo(padding + (values.length - 1) * step, padding + h);
    trendCtx.lineTo(padding, padding + h);
    trendCtx.closePath();
    const gradient = trendCtx.createLinearGradient(0, padding, 0, padding + h);
    gradient.addColorStop(0, 'rgba(124,111,255,0.35)');
    gradient.addColorStop(1, 'rgba(124,111,255,0.02)');
    trendCtx.fillStyle = gradient;
    trendCtx.fill();

    // line
    trendCtx.beginPath();
    values.forEach((v, i) => {
      const x = padding + i * step;
      const y = padding + h - (v / maxVal) * h;
      if (i === 0) trendCtx.moveTo(x, y); else trendCtx.lineTo(x, y);
    });
    trendCtx.strokeStyle = '#7C6FFF';
    trendCtx.lineWidth = 2;
    trendCtx.stroke();
  }

  // =====================================================
  // Route map (signature animated element)
  // =====================================================
  const mapCanvas = document.getElementById('routeMap');
  const mapCtx = mapCanvas.getContext('2d');
  let gridLines = [];

  function buildGrid(rect) {
    gridLines = [];
    const cols = Math.max(4, Math.floor(rect.width / 90));
    const rows = Math.max(3, Math.floor(rect.height / 60));
    for (let i = 1; i < cols; i++) {
      const x = (rect.width / cols) * i + (Math.random() - 0.5) * 10;
      gridLines.push({ type: 'v', pos: x });
    }
    for (let j = 1; j < rows; j++) {
      const y = (rect.height / rows) * j + (Math.random() - 0.5) * 8;
      gridLines.push({ type: 'h', pos: y });
    }
  }

  function spawnTrip(rect) {
    const vLines = gridLines.filter(g => g.type === 'v').map(g => g.pos);
    const hLines = gridLines.filter(g => g.type === 'h').map(g => g.pos);
    if (!vLines.length || !hLines.length) return;
    const horizontal = Math.random() > 0.5;
    const fixedPos = horizontal ? hLines[Math.floor(Math.random() * hLines.length)] : vLines[Math.floor(Math.random() * vLines.length)];
    activeTrips.push({
      horizontal,
      fixedPos,
      progress: 0,
      speed: 0.003 + Math.random() * 0.004,
      color: Math.random() > 0.3 ? '#3ED6C6' : '#35D399',
      length: rect.width
    });
  }

  function spawnPulse(rect) {
    pulseRings.push({
      x: Math.random() * rect.width,
      y: Math.random() * rect.height,
      radius: 2,
      alpha: 1
    });
  }

  function drawRouteMap() {
    const rect = mapCanvas.getBoundingClientRect();
    mapCtx.clearRect(0, 0, rect.width, rect.height);

    // grid roads
    mapCtx.strokeStyle = 'rgba(167,159,191,0.14)';
    mapCtx.lineWidth = 1;
    gridLines.forEach(g => {
      mapCtx.beginPath();
      if (g.type === 'v') { mapCtx.moveTo(g.pos, 0); mapCtx.lineTo(g.pos, rect.height); }
      else { mapCtx.moveTo(0, g.pos); mapCtx.lineTo(rect.width, g.pos); }
      mapCtx.stroke();
    });

    // active trip dots traveling along roads
    activeTrips.forEach(trip => {
      trip.progress += trip.speed;
      const pos = trip.progress * rect.width;
      const x = trip.horizontal ? pos : trip.fixedPos;
      const y = trip.horizontal ? trip.fixedPos : pos;

      mapCtx.beginPath();
      mapCtx.fillStyle = trip.color;
      mapCtx.shadowColor = trip.color;
      mapCtx.shadowBlur = 8;
      mapCtx.arc(x, y, 3.2, 0, Math.PI * 2);
      mapCtx.fill();
      mapCtx.shadowBlur = 0;
    });
    activeTrips = activeTrips.filter(t => t.progress < 1.05);

    // pulsing pickup request rings
    pulseRings.forEach(p => {
      p.radius += 0.6;
      p.alpha -= 0.02;
      mapCtx.beginPath();
      mapCtx.strokeStyle = `rgba(245,166,35,${Math.max(0, p.alpha)})`;
      mapCtx.lineWidth = 2;
      mapCtx.arc(p.x, p.y, p.radius, 0, Math.PI * 2);
      mapCtx.stroke();
    });
    pulseRings = pulseRings.filter(p => p.alpha > 0);

    requestAnimationFrame(drawRouteMap);
  }

  function resizeMap() {
    const rect = resizeCanvasToContainer(mapCanvas);
    buildGrid(rect);
  }

  window.addEventListener('resize', () => {
    resizeMap();
    resizeCanvasToContainer(trendCanvas);
    drawTrend();
  });

  // =====================================================
  // Demo data generation
  // =====================================================
  const STATUS_SEQUENCE = ["requested", "in_progress", "completed"];

  function tickDemo() {
    const city = CITIES[Math.floor(Math.random() * CITIES.length)];
    const roll = Math.random();
    let status;
    if (roll < 0.35) status = "requested";
    else if (roll < 0.7) status = "in_progress";
    else if (roll < 0.92) status = "completed";
    else status = "cancelled";

    const event = {
      trip_id: Math.random().toString(36).slice(2, 10),
      city,
      status,
      fare: status === "completed" ? (8 + Math.random() * 55).toFixed(2) : null
    };

    statusCounts[status] = (statusCounts[status] || 0) + 1;
    cityCounts[city] = (cityCounts[city] || 0) + 1;

    pushFeedItem(event);
    renderDonut(statusCounts);
    renderCityBars(cityCounts);

    const totalTrips = Object.values(statusCounts).reduce((a, b) => a + b, 0);
    renderSummary({
      trips_24h: totalTrips,
      trips_active: statusCounts.in_progress || 0,
      completed_24h: statusCounts.completed || 0,
      cancelled_24h: statusCounts.cancelled || 0,
      avg_fare_24h: 24.5 + Math.random() * 4
    });

    trendSeries.push({ t: Date.now(), events: 1 });
    // collapse into a rolling per-tick counter for a smoother trend line
    if (trendSeries.length > 40) trendSeries.shift();
    // aggregate into a synthetic smoothed value for nicer visuals
    const smoothed = trendSeries.map((d, i) => ({
      t: d.t,
      events: 8 + Math.sin(i / 3) * 4 + Math.random() * 3
    }));
    drawTrendWith(smoothed);

    if (status === 'requested') {
      const rect = mapCanvas.getBoundingClientRect();
      spawnPulse(rect);
    }
    if (status === 'in_progress' && Math.random() < 0.6) {
      const rect = mapCanvas.getBoundingClientRect();
      spawnTrip(rect);
    }
  }

  function drawTrendWith(series) {
    trendSeries = series;
    drawTrend();
  }

  function seedDemoHealth() {
    renderHealth([
      { component: 'kafka', status: 'healthy' },
      { component: 'spark_streaming', status: 'healthy' },
      { component: 'emr_cluster', status: 'healthy' },
      { component: 'postgres', status: 'healthy' }
    ]);
  }

  // =====================================================
  // Live API polling
  // =====================================================
  async function fetchJson(path) {
    // Render's free tier can take ~50s to wake up, so allow a generous timeout.
    const res = await fetch(apiBase + path, { signal: AbortSignal.timeout(70000) });
    if (!res.ok) throw new Error('bad response: ' + path);
    return res.json();
  }

  async function pollLive() {
    if (pollInFlight) return;
    pollInFlight = true;
    try {
      const [summary, series, cities, statuses, events, health] = await Promise.all([
        fetchJson('/api/summary'),
        fetchJson('/api/timeseries?minutes=60'),
        fetchJson('/api/by-city'),
        fetchJson('/api/status-breakdown'),
        fetchJson('/api/recent-events?limit=25'),
        fetchJson('/api/pipeline-health')
      ]);

      renderSummary(summary);

      const seriesFormatted = series.map(r => ({ t: new Date(r.window_start).getTime(), events: r.events_total }));
      drawTrendWith(seriesFormatted);

      const statusMap = {};
      statuses.forEach(s => { statusMap[s.status] = Number(s.count); });
      renderDonut(statusMap);

      const cityMap = {};
      cities.forEach(c => { cityMap[c.city] = Number(c.trip_count); });
      renderCityBars(cityMap);

      feedEl.innerHTML = '';
      events.slice().reverse().forEach(e => pushFeedItem(e));

      if (health.length) renderHealth(health);

      animateLive(events, summary);

      if (!liveConnected) {
        liveConnected = true;
        if (demoTimer) { clearInterval(demoTimer); demoTimer = null; }
        setStatus("LIVE API", false);
      }
    } catch (e) {
      console.warn('Live API unreachable:', e.message);
      if (liveConnected) {
        setStatus("API OFFLINE · RETRYING", true);
      } else {
        setStatus("API WAKING UP · DEMO", true);
      }
    } finally {
      pollInFlight = false;
    }
  }

  // Drive the route map from live data: a pulse for each newly seen request,
  // a moving dot for each trip that started, and enough ambient dots to
  // reflect how many trips are active right now.
  function animateLive(events, summary) {
    const rect = mapCanvas.getBoundingClientRect();
    const firstPoll = seenEvents.size === 0;
    events.forEach(e => {
      const key = e.trip_id + ':' + e.status;
      if (seenEvents.has(key)) return;
      seenEvents.add(key);
      if (firstPoll) return;
      if (e.status === 'requested') spawnPulse(rect);
      if (e.status === 'in_progress' || e.status === 'accepted') spawnTrip(rect);
    });
    if (seenEvents.size > 2000) seenEvents = new Set([...seenEvents].slice(-500));
    const target = Math.min(Number(summary.trips_active) || 0, 25);
    for (let i = activeTrips.length; i < target; i++) spawnTrip(rect);
  }

  function restartPolling() {
    if (pollTimer) clearInterval(pollTimer);
    if (demoTimer) { clearInterval(demoTimer); demoTimer = null; }
    if (mode === 'live') {
      // Keep the demo animating until the first live response arrives.
      if (!liveConnected) demoTimer = setInterval(tickDemo, 900);
      pollLive();
      pollTimer = setInterval(pollLive, 4000);
    } else {
      pollTimer = setInterval(tickDemo, 900);
    }
  }

  // =====================================================
  // Init
  // =====================================================
  resizeMap();
  resizeCanvasToContainer(trendCanvas);
  seedDemoHealth();
  for (let i = 0; i < 15; i++) tickDemo();
  requestAnimationFrame(drawRouteMap);

  const params = new URLSearchParams(location.search);
  if (params.has('demo')) {
    restartPolling();
  } else {
    goLive(params.get('api') || DEFAULT_API);
  }
})();
