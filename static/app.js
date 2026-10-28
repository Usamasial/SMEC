// -- tabs --
document.querySelectorAll(".tab-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
    document.querySelectorAll(".tab-panel").forEach(p => p.classList.remove("active"));
    btn.classList.add("active");
    document.getElementById("tab-" + btn.dataset.tab).classList.add("active");
    if (btn.dataset.tab === "history") loadHistory();
    if (btn.dataset.tab === "queue") loadQueue();
    if (btn.dataset.tab === "connection") loadConnection();
  });
});

function metric(label, value, cls) {
  return `<div class="metric"><div class="k">${label}</div><div class="v ${cls}">${value}</div></div>`;
}

function statusChip(id, ok, okText, badText) {
  const el = document.getElementById(id);
  el.className = "chip " + (ok === null ? "chip-unknown" : ok ? "chip-ok" : "chip-bad");
  el.textContent = ok === null ? "checking..." : ok ? okText : badText;
}

async function loadDashboard() {
  const d = await (await fetch("/api/dashboard")).json();
  statusChip("plcChip", d.plc_connected, "PLC: Connected", "PLC: Disconnected");
  const qEl = document.getElementById("queueChip");
  qEl.className = "chip " + (d.queue_size > 0 ? "chip-bad" : "chip-ok");
  qEl.textContent = `Queue: ${d.queue_size}`;

  // Process-flow strip: the real physical sequence of the line (Station 1 -> 5),
  // each dot reflecting that station's live status at a glance.
  const flow = document.getElementById("flowStrip");
  if (flow) {
    flow.innerHTML = d.stations.map((s, idx) => {
      const dotCls = s.uptime_status === 1 ? "on" : s.uptime_status === 0 ? "off" : "";
      return `<div class="flow-step">
        <span class="flow-num">${String(idx + 1).padStart(2, "0")}</span>
        <span class="flow-dot ${dotCls}"></span>
        <div class="flow-label">
          <div class="name">${s.code.replace("_", " ")}</div>
          <div class="task">${s.label}</div>
        </div>
      </div>`;
    }).join("");
  }

  const grid = document.getElementById("stationGrid");
  grid.innerHTML = d.stations.map(s => {
    const running = s.uptime_status === 1;
    const known = s.uptime_status === 1 || s.uptime_status === 0;
    const cardCls = known ? (running ? "st-on" : "st-off") : "";
    const dotCls = known ? (running ? "on" : "off") : "";
    const statusText = running ? "RUNNING" : known ? "DOWN" : "--";
    const statusCls = known ? (running ? "on" : "off") : "";
    const plcCls = s.plc_ok ? "on" : "off";

    // Only Station 5 produces measurable output (Count/Reject) -- hardcoded here
    // on the frontend only, matching the real fact that no other station has
    // these values, without needing any backend field for it.
    const hasOutput = s.code === "STATION_5";
    const outputBlock = hasOutput ? `
      <div class="output-section">
        <div class="section-label">OUTPUT</div>
        <div class="output-grid">
          <div class="output-tile"><div class="num">${s.count ?? 0}</div><div class="lbl">Count</div></div>
          <div class="output-tile reject"><div class="num">${s.reject_count ?? 0}</div><div class="lbl">Reject</div></div>
        </div>
      </div>` : "";

    return `<div class="station-card ${cardCls}">
      <div class="station-head">
        <h3>${s.code.replace("_", " ")}</h3>
      </div>
      <div class="label">${s.label}</div>
      <div class="status-row">
        <span class="status-dot ${dotCls}"></span>
        <span class="status-text ${statusCls}">${statusText}</span>
      </div>
      <div class="station-metrics">
        ${metric("PLC Link", s.plc_ok ? "OK" : "LOST", plcCls)}
        ${metric("Last Success", s.last_success_at ? s.last_success_at.split(" ")[1] || s.last_success_at : "Never", s.last_success_at ? "na" : "off")}
        ${metric("Last Attempt", s.last_attempt_at ? s.last_attempt_at.split(" ")[1] || s.last_attempt_at : "--", "na")}
      </div>
      ${outputBlock}
    </div>`;
  }).join("");
}

let histOffset = 0;
const HIST_LIMIT = 50;
async function loadHistory() {
  const station = document.getElementById("histStation").value;
  const signal = document.getElementById("histSignal").value;
  const status = document.getElementById("histStatus").value;
  const qs = new URLSearchParams({ station_code: station, signal, status, limit: HIST_LIMIT, offset: histOffset });
  const r = await (await fetch(`/api/history?${qs}`)).json();
  const items = r.items.filter(i => i.signal !== "production");
  document.getElementById("historyBody").innerHTML = items.map(i => `<tr>
    <td>${i.datetime_str}</td><td>${i.station_code}</td><td>${i.signal}</td><td>${i.value}</td>
    <td>${i.mc_no}</td><td style="font-size:11px">${i.msg_no}</td>
    <td class="status-${i.status}">${i.status}</td><td>${i.http_status ?? ""}</td>
    <td style="font-size:11px">${i.error ?? ""}</td>
  </tr>`).join("");
  document.getElementById("historyPager").innerHTML =
    `<button onclick="histPrev()" ${histOffset === 0 ? "disabled" : ""}>Prev</button>
     <span>${histOffset + 1}-${histOffset + r.items.length} of ${r.total}</span>
     <button onclick="histNext(${r.total})">Next</button>`;
}
function histPrev() { histOffset = Math.max(0, histOffset - HIST_LIMIT); loadHistory(); }
function histNext(total) { if (histOffset + HIST_LIMIT < total) { histOffset += HIST_LIMIT; loadHistory(); } }

async function loadQueue() {
  const r = await (await fetch("/api/queue?limit=200")).json();
  document.getElementById("queueBody").innerHTML = r.items.map(i => `<tr>
    <td>${i.created_at}</td><td>${i.station_code}</td><td>${i.signal}</td>
    <td style="font-size:11px">${i.msg_no}</td><td>${i.attempts}</td>
  </tr>`).join("");
}

async function loadConnection() {
  const d = await (await fetch("/api/config")).json();

  const ic = d.input_connection;
  document.getElementById("inputConnGrid").innerHTML = `
    <div class="kv-item"><div class="k">Protocol</div><div class="v">${ic.protocol}</div></div>
    <div class="kv-item"><div class="k">Host</div><div class="v">${ic.host}</div></div>
    <div class="kv-item"><div class="k">Port</div><div class="v">${ic.port}</div></div>
    <div class="kv-item"><div class="k">Mode</div><div class="v">${ic.mode}</div></div>
    <div class="kv-item"><div class="k">DA1 (PLC node)</div><div class="v">${ic.da1}</div></div>
    <div class="kv-item"><div class="k">SA1 (this app)</div><div class="v">${ic.sa1}</div></div>
    <div class="kv-item"><div class="k">Timeout</div><div class="v">${ic.timeout_seconds}s</div></div>
  `;

  const rel = d.reliability;
  const b = d.reliability_bounds;
  document.getElementById("reliabilityGrid").innerHTML = `
    <div class="edit-item"><div class="k">Max retry attempts (${b.max_retry_attempts[0]}-${b.max_retry_attempts[1]})</div>
      <input type="number" id="setMaxRetryAttempts" value="${rel.max_retry_attempts}" min="${b.max_retry_attempts[0]}" max="${b.max_retry_attempts[1]}" step="1"></div>
    <div class="edit-item"><div class="k">Retry interval, seconds (${b.retry_interval_seconds[0]}-${b.retry_interval_seconds[1]})</div>
      <input type="number" id="setRetryInterval" value="${rel.retry_interval_seconds}" min="${b.retry_interval_seconds[0]}" max="${b.retry_interval_seconds[1]}" step="0.5"></div>
    <div class="edit-item"><div class="k">Max queue size (${b.max_queue_size[0]}-${b.max_queue_size[1]})</div>
      <input type="number" id="setMaxQueueSize" value="${rel.max_queue_size}" min="${b.max_queue_size[0]}" max="${b.max_queue_size[1]}" step="10"></div>
    <div class="edit-item"><div class="k">Response check (fixed)</div>
      <div style="font-size:12px;color:#6b7280;padding-top:6px">${rel.response_check}</div></div>
    <div class="edit-item"><div class="k">msg_no sequence (fixed)</div>
      <div style="font-size:12px;color:#6b7280;padding-top:6px">${rel.sequence_reset}</div></div>
  `;

  document.getElementById("outboundBody").innerHTML = d.outbound_apis.map(a => `<tr>
    <td>${a.signal}</td><td style="font-family:ui-monospace,monospace">${a.url}</td>
    <td>${a.package_no}</td>
    <td>${a.confirmed ? '<span class="badge-confirmed">Confirmed</span>' : '<span class="badge-assumed">Assumed</span>'}</td>
  </tr>`).join("");

  const pb = d.poll_interval_bounds;
  document.getElementById("stationConfigBody").innerHTML = d.stations.map(s => `<tr>
    <td>${s.code.replace("_", " ")}</td><td>${s.mc_no}</td>
    <td style="font-family:ui-monospace,monospace">${s.start}</td>
    <td style="font-family:ui-monospace,monospace">${s.sensor2}</td>
    <td style="font-family:ui-monospace,monospace">${s.stop}</td>
    <td style="font-family:ui-monospace,monospace">${s.reject}</td>
    <td><input class="poll-input" type="number" id="poll_${s.code}" value="${s.poll_interval}" min="${pb[0]}" max="${pb[1]}" step="0.01"></td>
    <td><button class="row-save-btn" onclick="savePollInterval('${s.code}')">Save</button>
      <span id="pollMsg_${s.code}" class="save-msg"></span></td>
  </tr>`).join("");
}

async function saveReliability() {
  const body = {
    max_retry_attempts: Number(document.getElementById("setMaxRetryAttempts").value),
    retry_interval_seconds: Number(document.getElementById("setRetryInterval").value),
    max_queue_size: Number(document.getElementById("setMaxQueueSize").value),
  };
  const r = await fetch("/api/settings", { method: "PUT", headers: {"Content-Type":"application/json"}, body: JSON.stringify(body) });
  const msgEl = document.getElementById("reliabilitySaveMsg");
  if (r.ok) {
    msgEl.textContent = "Saved -- applied immediately, no restart needed.";
    msgEl.className = "save-msg ok";
  } else {
    const err = await r.json();
    msgEl.textContent = "Error: " + (err.detail || "could not save");
    msgEl.className = "save-msg err";
  }
  setTimeout(() => { msgEl.textContent = ""; }, 4000);
}

async function savePollInterval(code) {
  const val = Number(document.getElementById(`poll_${code}`).value);
  const r = await fetch(`/api/stations/${encodeURIComponent(code)}/poll-interval`, {
    method: "PUT", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ poll_interval: val })
  });
  const msgEl = document.getElementById(`pollMsg_${code}`);
  if (r.ok) {
    msgEl.textContent = "Saved";
    msgEl.className = "save-msg ok";
  } else {
    const err = await r.json();
    msgEl.textContent = err.detail || "Error";
    msgEl.className = "save-msg err";
  }
  setTimeout(() => { msgEl.textContent = ""; }, 3000);
}

async function initStationFilter() {
  const d = await (await fetch("/api/stations")).json();
  const sel = document.getElementById("histStation");
  d.forEach(s => {
    const opt = document.createElement("option");
    opt.value = s.code; opt.textContent = s.code.replace("_", " ");
    sel.appendChild(opt);
  });
}

initStationFilter();
loadDashboard();
setInterval(loadDashboard, 2000);
