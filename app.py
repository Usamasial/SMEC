"""
SMEC Line Bridge -- polls 5 stations on one Omron PLC via FINS/UDP,
computes Uptime / Count / Reject per station (Production Status is
NOT sent -- confirmed not needed by the client's developer),
and forwards each to its HTTP API endpoint. Direct-send-first, with
a small SQLite retry queue for when the API is unreachable, plus a
permanent history log of everything sent or attempted.

Everything is hardcoded on purpose (fixed station list, fixed PLC
address map, fixed API endpoints) -- there is no settings UI.
"""
import json, logging, os, signal, socket, sqlite3, struct, threading, time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import requests
import uvicorn
from fastapi import Body, FastAPI, WebSocket
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("smec")

DB_PATH = Path("smec_bridge.db")
WEB_PORT = 8000

# ==================================================================== HARDCODED CONFIG

# -- PLC connection (Omron FINS over UDP, confirmed from Node-RED FINS Connection node) --
PLC_HOST = "10.57.10.10"
PLC_PORT = 9600
FINS_DA1 = 10     # destination node (the PLC)
FINS_SA1 = 4      # source node (this app)
# Timeout kept short on purpose: at fast polling intervals, one dropped UDP packet
# should cost a fraction of a second, not stall the whole loop for seconds.
FINS_TIMEOUT = 0.5
DOWNTIME_GRACE_SECONDS = 180  # 3 minutes -- matches Node-RED's countdown logic
EDGE_DEBOUNCE_SECONDS = 1.0   # ignore bouncing re-triggers within this window (Uptime/Production)
# Count/Reject use a MUCH longer debounce on purpose -- the original Node-RED flow
# rate-limits Count to at most 1 per MINUTE (a "delay" node with rate=1/minute, drop
# excess). Without this, a Start bit that pulses/bounces faster than once a minute
# at the PLC level gets miscounted as multiple finished units.
COUNT_DEBOUNCE_SECONDS = 60.0

# -- Outbound HTTP API. Developer confirmed only Uptime, Count, and Reject are
# NEEDED for their dashboard -- Production Status is intentionally NOT surfaced
# anywhere (no UI, no Connection Info listing, no dedicated history filter).
# It IS still sent internally, purely because their backend requires Production
# to be registered before it will accept a Count -- confirmed by the real error
# "Machine Currently Not Running In Production (System)!" appearing on Count
# once Production stopped being sent. This is a backend-side dependency we
# don't control, not something we chose to expose.
API_BASE = "http://10.57.10.4:8080/pna/public/api"
ENDPOINTS = {
    "uptime":     f"{API_BASE}/machine_up_time",
    "production": f"{API_BASE}/production",   # internal-only, unblocks Count -- see note above
    "count":      f"{API_BASE}/machine_count",
    "reject":     f"{API_BASE}/machine_reject",  # ASSUMED -- confirm with client, easy to change here
}
PACKAGE_NO = {"uptime": "002", "production": "003", "count": "005", "reject": "006"}
PAYLOAD_FIELD = {"uptime": "uptimeStatus", "production": "prodStatus", "count": "count", "reject": "rejectCount"}
HTTP_TIMEOUT = 5
# Their backend needs time to internally register Uptime/Production before it'll
# accept Count for the same station -- 2s was tried on-site and wasn't enough
# (confirmed by exact timestamps in real logs), 5s has real margin.
PRODUCTION_SEND_DELAY_SECONDS = 5.0
# -- Line-level idle watchdog. If NO station shows a running signal (Start/Sensor 2)
# for LINE_IDLE_SECONDS, send one "production stopped" packet for the whole line
# (mc_no "SMEC", prodStatus 0) to the production endpoint. Sent once per idle
# episode; re-arms as soon as any station signal comes back. Change the delay here
# (60 = 1 min, 120 = 2 min, 180 = 3 min).
LINE_IDLE_SECONDS = 180
LINE_MC_NO = "SMEC"
# Reliability settings (max retry attempts, retry interval, queue cap) and per-station
# poll interval are now USER-EDITABLE from the Connection Info tab -- they live in the
# settings table (see DEFAULT_SETTINGS below), not as fixed constants here.

# -- Station map (from client PDF "System Layout" + confirmed FINS Read nodes) --
# start/stop = (word, bit) in the Work (W) area.
# sensor2 = ONLY Station 1 has this. Per the client's PDF: "SENSOR 2 X1.12/W11.12 --
# Used to trigger production status in NR" -- i.e. Station 1's Uptime is driven by
# Start (W11.11) OR Sensor 2 (W11.12), either one counts. Confirmed directly in the
# original Node-RED source: W11.12's chain feeds the exact same output link as
# W11.11's chain. Every other station has only a single trigger bit.
# poll_interval = how often THIS station's bits are read, in seconds. Each station
# has its own value here -- this is the one place to change polling speed per station.
# has_count / has_reject: only Station 5 (last station -- Laser Engraving) produces a
# real finished-unit count and is where rejects are noticed, per client confirmation.
STATIONS = [
    dict(code="STATION_1", mc_no="STATION_1", label="Stacking Bottom Cover Casing",
         start=(11, 11), stop=(14, 0), sensor2=(11, 12), poll_interval=0.05, has_count=False, has_reject=False, reject=None),
    dict(code="STATION_2", mc_no="STATION_2", label="Stacking Top Cover Casing",
         start=(14, 1), stop=(14, 4), sensor2=None, poll_interval=0.05, has_count=False, has_reject=False, reject=None),
    dict(code="STATION_3", mc_no="STATION_3", label="Press Lock Cover Casing",
         start=(11, 0), stop=(11, 2), sensor2=None, poll_interval=0.05, has_count=False, has_reject=False, reject=None),
    dict(code="STATION_4", mc_no="STATION_4", label="Cobot Pick-up Cover Casing to Rack Tray",
         start=(11, 3), stop=(11, 4), sensor2=None, poll_interval=0.05, has_count=False, has_reject=False, reject=None),
    dict(code="STATION_5", mc_no="STATION_5", label="Laser Engraving Card Cover",
         # start CONFIRMED via live bit scan on-site: real bit is W11.06, not W11.05
         # as the PDF documented (off by one) -- confirmed by repeated toggling that
         # matched physical triggers exactly. stop (W11.09) not yet independently
         # re-verified -- app doesn't act on stop for any logic, so left as documented.
         start=(11, 6), stop=(11, 9), sensor2=None, poll_interval=0.05, has_count=True, has_reject=True,
         # reject is a DM WORD address (D272 -- TOGO_DisplayValue_Judgment_NG),
         # CONFIRMED directly in Sysmac Studio's own Global Variables table +
         # Search Results ("TOGO_DisplayValue_Judgment_NG AT %D272 : INT"), then
         # live-tested on-site reading real values. The xlsx's earlier claim of
         # D262 was simply wrong. NOT a (word,bit) tuple like start/stop/sensor2 --
         # it holds the PLC's own running NG/reject count as a real number, read
         # via read_word() (DM area), not read_bit() (Work area). We mirror this
         # number directly rather than edge-counting a bit ourselves, since the
         # PLC already maintains the count.
         reject=272),
]
STATIONS_BY_CODE = {s["code"]: s for s in STATIONS}
TICK_SECONDS = 0.01  # loop wake-up granularity -- matches POLL_INTERVAL_BOUNDS' floor, so
                      # a station set to the fastest allowed interval isn't silently capped
                      # by a coarser tick rate

def now_myt_string() -> str:
    """Malaysia time (UTC+8), formatted like the Node-RED Date/Time Formatter."""
    t = datetime.utcnow() + timedelta(hours=8)
    return t.strftime("%Y-%m-%d %H:%M:%S")

def now_myt_line_string() -> str:
    """Malaysia time in the DD-MM-YYYY HH:MM:SS format the line-level packet uses."""
    t = datetime.utcnow() + timedelta(hours=8)
    return t.strftime("%d-%m-%Y %H:%M:%S")

def now_s() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def now_ms() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

# ==================================================================== FINS/UDP CLIENT
class FinsError(Exception):
    pass

class FinsClient:
    """Minimal Omron FINS/UDP client -- Memory Area Read only, single-bit reads
    from the Work (W) area. Matches the Node-RED FINS Connection node config:
    ICF=0x80, GCT=0x02, DNA=0, DA1=<PLC node>, DA2=0, SNA=0, SA1=<our node>, SA2=0."""
    AREA_WORK_BIT = 0x31
    AREA_DM_WORD = 0x82

    def __init__(self, host, port, da1, sa1, timeout=2.0):
        self.host, self.port = host, port
        self.da1, self.sa1 = da1, sa1
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self.lock = threading.Lock()
        self._sid = 0

    def connect(self):
        self.close()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(self.timeout)
        self.sock.bind(("", 0))

    def close(self):
        if self.sock:
            try: self.sock.close()
            except Exception: pass
            self.sock = None

    def _next_sid(self):
        self._sid = (self._sid + 1) % 256
        return self._sid

    def read_bit(self, word: int, bit: int) -> bool:
        with self.lock:
            if not self.sock:
                self.connect()
            sid = self._next_sid()
            header = bytes([0x80, 0x00, 0x02, 0x00, self.da1, 0x00, 0x00, self.sa1, 0x00, sid])
            command = bytes([0x01, 0x01])  # Memory Area Read
            params = bytes([self.AREA_WORK_BIT]) + struct.pack(">H", word) + bytes([bit]) + struct.pack(">H", 1)
            frame = header + command + params
            self.sock.sendto(frame, (self.host, self.port))
            data, _ = self.sock.recvfrom(1024)
            if len(data) < 15:
                raise FinsError(f"Response too short ({len(data)} bytes)")
            end_code = data[12:14]
            if end_code != b"\x00\x00":
                raise FinsError(f"FINS end code {end_code.hex()} (word={word} bit={bit})")
            return data[14] != 0

    def read_word(self, word: int) -> int:
        """Reads a single 16-bit DM-area word (a real number, e.g. the PLC's own
        running Reject count) -- different memory area and response shape from
        read_bit, which only ever returns a single 0/1. Verified separately
        in check_d262.py against a mock FINS server before being folded in here."""
        with self.lock:
            if not self.sock:
                self.connect()
            sid = self._next_sid()
            header = bytes([0x80, 0x00, 0x02, 0x00, self.da1, 0x00, 0x00, self.sa1, 0x00, sid])
            command = bytes([0x01, 0x01])  # Memory Area Read
            # word-read address format: area code + word(2 bytes) + bit(1 byte, 0x00 for word access) + count(2 bytes)
            params = bytes([self.AREA_DM_WORD]) + struct.pack(">H", word) + bytes([0x00]) + struct.pack(">H", 1)
            frame = header + command + params
            self.sock.sendto(frame, (self.host, self.port))
            data, _ = self.sock.recvfrom(1024)
            if len(data) < 16:
                raise FinsError(f"Response too short ({len(data)} bytes)")
            end_code = data[12:14]
            if end_code != b"\x00\x00":
                raise FinsError(f"FINS end code {end_code.hex()} (DM word={word})")
            return struct.unpack(">H", data[14:16])[0]

fins = FinsClient(PLC_HOST, PLC_PORT, FINS_DA1, FINS_SA1, FINS_TIMEOUT)

# ==================================================================== DATA LAYER
_SCHEMA = """
CREATE TABLE IF NOT EXISTS history (
    id INTEGER PRIMARY KEY AUTOINCREMENT, station_code TEXT NOT NULL, signal TEXT NOT NULL,
    value REAL NOT NULL, mc_no TEXT NOT NULL, package_no TEXT NOT NULL, msg_no TEXT NOT NULL,
    datetime_str TEXT NOT NULL, status TEXT NOT NULL, http_status INTEGER, error TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_history_lookup ON history(station_code, signal, created_at);
CREATE TABLE IF NOT EXISTS retry_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT, station_code TEXT NOT NULL, signal TEXT NOT NULL,
    payload TEXT NOT NULL, url TEXT NOT NULL, msg_no TEXT NOT NULL UNIQUE,
    attempts INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS counters (
    key TEXT PRIMARY KEY, value INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS station_state (
    station_code TEXT PRIMARY KEY, uptime_status INTEGER NOT NULL DEFAULT 0,
    prod_status INTEGER NOT NULL DEFAULT 0, count INTEGER NOT NULL DEFAULT 0,
    reject_count INTEGER NOT NULL DEFAULT 0, plc_ok INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT, last_success_at TEXT,
    raw_start INTEGER, raw_stop INTEGER, raw_sensor2 INTEGER
);
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY, value TEXT NOT NULL
);
"""

# -- Editable defaults, seeded into the settings table on first run.
# These are the ONLY things meant to be changed from the UI -- PLC connection
# and station addresses stay hardcoded since those describe real wiring. --
DEFAULT_SETTINGS = {
    "max_retry_attempts": "10",
    "retry_interval_seconds": "5",
    "max_queue_size": "5000",
}
POLL_INTERVAL_BOUNDS = (0.01, 60.0)     # seconds -- user-adjustable floor, see TICK_SECONDS below
RETRY_ATTEMPTS_BOUNDS = (1, 100)
RETRY_INTERVAL_BOUNDS = (1, 300)        # seconds
QUEUE_SIZE_BOUNDS = (10, 100000)

class Store:
    def __init__(self, path: Path):
        # RLock, not Lock: several methods call other locked methods on the same
        # thread (e.g. bump_total -> get_total, enqueue -> get_setting) -- a plain
        # Lock would deadlock on that re-entry.
        self.lock = threading.RLock()
        self.c = sqlite3.connect(str(path), check_same_thread=False)
        self.c.execute("PRAGMA journal_mode=WAL;")
        self.c.executescript(_SCHEMA)
        for s in STATIONS:
            self.c.execute("INSERT OR IGNORE INTO station_state(station_code) VALUES(?)", (s["code"],))
        for k, v in DEFAULT_SETTINGS.items():
            self.c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k, v))
        for s in STATIONS:
            self.c.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)",
                (f"poll_interval:{s['code']}", str(s["poll_interval"])))
        self.c.commit()

    # -- generic settings (reliability + per-station poll interval) --
    def get_setting(self, key, default=None):
        with self.lock:
            r = self.c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
            return r[0] if r else default

    def set_setting(self, key, value):
        with self.lock:
            self.c.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)))
            self.c.commit()

    def get_reliability_settings(self):
        return {
            "max_retry_attempts": int(self.get_setting("max_retry_attempts", DEFAULT_SETTINGS["max_retry_attempts"])),
            "retry_interval_seconds": float(self.get_setting("retry_interval_seconds", DEFAULT_SETTINGS["retry_interval_seconds"])),
            "max_queue_size": int(self.get_setting("max_queue_size", DEFAULT_SETTINGS["max_queue_size"])),
        }

    def get_poll_interval(self, station_code, fallback):
        return float(self.get_setting(f"poll_interval:{station_code}", fallback))

    # -- monotonic per-station-per-signal sequence number, used in msg_no.
    # Resets to 1 each day (matches Node-RED's midnight "Reset message no." inject) --
    def next_seq(self, station_code, signal):
        day = datetime.now().strftime("%Y%m%d")
        key = f"{station_code}:{signal}:seq"
        day_key = f"{station_code}:{signal}:seq_day"
        with self.lock:
            stored_day = self.c.execute("SELECT value FROM counters WHERE key=?", (day_key,)).fetchone()
            r = self.c.execute("SELECT value FROM counters WHERE key=?", (key,)).fetchone()
            current = r[0] if r else 0
            if stored_day is None or str(stored_day[0]) != day:
                current = 0  # new day -- start the sequence over
                self.c.execute("INSERT INTO counters(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (day_key, int(day)))
            n = current + 1
            self.c.execute("INSERT INTO counters(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, n))
            self.c.commit()
        return n

    # -- persisted running totals (count / reject survive restarts) --
    def get_total(self, station_code, signal):
        key = f"{station_code}:{signal}:total"
        with self.lock:
            r = self.c.execute("SELECT value FROM counters WHERE key=?", (key,)).fetchone()
            return r[0] if r else 0

    def bump_total(self, station_code, signal):
        key = f"{station_code}:{signal}:total"
        with self.lock:
            n = self.get_total(station_code, signal) + 1
            self.c.execute("INSERT INTO counters(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, n))
            self.c.commit()
        return n

    def set_total(self, station_code, signal, value):
        """Like bump_total, but sets an exact value rather than incrementing --
        for signals where an external system (the PLC's own DM counter, for
        Reject) already maintains the real count and we just mirror it."""
        key = f"{station_code}:{signal}:total"
        with self.lock:
            self.c.execute("INSERT INTO counters(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
            self.c.commit()
        return value

    # -- station live state (for dashboard) --
    def update_state(self, station_code, **fields):
        cols = ", ".join(f"{k}=?" for k in fields)
        with self.lock:
            self.c.execute(f"UPDATE station_state SET {cols} WHERE station_code=?", (*fields.values(), station_code))
            self.c.commit()

    def get_states(self):
        with self.lock:
            rows = self.c.execute("""SELECT station_code,uptime_status,prod_status,count,reject_count,plc_ok,
                last_attempt_at,last_success_at,raw_start,raw_stop,raw_sensor2 FROM station_state""").fetchall()
        return {r[0]: {"uptime_status": r[1], "prod_status": r[2], "count": r[3],
                       "reject_count": r[4], "plc_ok": bool(r[5]), "last_attempt_at": r[6], "last_success_at": r[7],
                       "raw_start": r[8], "raw_stop": r[9], "raw_sensor2": r[10]} for r in rows}

    # -- history --
    def log_history(self, station_code, signal, value, mc_no, package_no, msg_no, datetime_str,
                     status, http_status=None, error=None):
        with self.lock:
            self.c.execute("""INSERT INTO history(station_code,signal,value,mc_no,package_no,msg_no,
                datetime_str,status,http_status,error,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (station_code, signal, value, mc_no, package_no, msg_no, datetime_str, status, http_status, error, now_ms()))
            self.c.commit()

    def update_history_status(self, msg_no, status, http_status=None, error=None):
        with self.lock:
            self.c.execute("UPDATE history SET status=?, http_status=?, error=? WHERE msg_no=?",
                (status, http_status, error, msg_no))
            self.c.commit()

    def history_page(self, station_code=None, signal=None, status=None, limit=100, offset=0):
        cl, pr = [], []
        if station_code: cl.append("station_code=?"); pr.append(station_code)
        if signal: cl.append("signal=?"); pr.append(signal)
        if status: cl.append("status=?"); pr.append(status)
        where = "WHERE " + " AND ".join(cl) if cl else ""
        with self.lock:
            total = self.c.execute(f"SELECT COUNT(*) FROM history {where}", pr).fetchone()[0]
            rows = self.c.execute(f"""SELECT station_code,signal,value,mc_no,package_no,msg_no,datetime_str,
                status,http_status,error,created_at FROM history {where}
                ORDER BY id DESC LIMIT ? OFFSET ?""", pr + [limit, offset]).fetchall()
        items = [{"station_code": r[0], "signal": r[1], "value": r[2], "mc_no": r[3], "package_no": r[4],
                  "msg_no": r[5], "datetime_str": r[6], "status": r[7], "http_status": r[8],
                  "error": r[9], "created_at": r[10]} for r in rows]
        return items, total

    # -- retry queue --
    def enqueue(self, station_code, signal, payload: dict, url: str, msg_no: str):
        with self.lock:
            # Cap enforcement, matches the spirit of Node-RED's 9000-row CSV limit --
            # drop the oldest queued item rather than growing forever if the API stays down.
            n = self.c.execute("SELECT COUNT(*) FROM retry_queue").fetchone()[0]
            max_q = int(self.get_setting("max_queue_size", DEFAULT_SETTINGS["max_queue_size"]))
            if n >= max_q:
                oldest = self.c.execute("SELECT id, msg_no FROM retry_queue ORDER BY created_at ASC LIMIT 1").fetchone()
                if oldest:
                    self.c.execute("DELETE FROM retry_queue WHERE id=?", (oldest[0],))
                    self.c.execute("UPDATE history SET status='dropped', error='queue full, dropped oldest' WHERE msg_no=?", (oldest[1],))
            self.c.execute("""INSERT OR IGNORE INTO retry_queue(station_code,signal,payload,url,msg_no,created_at)
                VALUES(?,?,?,?,?,?)""", (station_code, signal, json.dumps(payload), url, msg_no, now_ms()))
            self.c.commit()

    def oldest_queued(self):
        with self.lock:
            r = self.c.execute("""SELECT id,station_code,signal,payload,url,msg_no,attempts FROM retry_queue
                ORDER BY created_at ASC LIMIT 1""").fetchone()
        if not r: return None
        return {"id": r[0], "station_code": r[1], "signal": r[2], "payload": json.loads(r[3]),
                "url": r[4], "msg_no": r[5], "attempts": r[6]}

    def mark_attempt(self, row_id):
        with self.lock:
            self.c.execute("UPDATE retry_queue SET attempts=attempts+1 WHERE id=?", (row_id,)); self.c.commit()

    def dequeue(self, row_id):
        with self.lock:
            self.c.execute("DELETE FROM retry_queue WHERE id=?", (row_id,)); self.c.commit()

    def queue_count(self):
        with self.lock:
            return self.c.execute("SELECT COUNT(*) FROM retry_queue").fetchone()[0]

    def queue_page(self, limit=100, offset=0):
        total = self.queue_count()
        with self.lock:
            rows = self.c.execute("""SELECT station_code,signal,msg_no,attempts,created_at FROM retry_queue
                ORDER BY created_at ASC LIMIT ? OFFSET ?""", (limit, offset)).fetchall()
        items = [{"station_code": r[0], "signal": r[1], "msg_no": r[2], "attempts": r[3], "created_at": r[4]} for r in rows]
        return items, total

store = Store(DB_PATH)

# ==================================================================== EVENTS (for live dashboard)
import queue as _q
class Bus:
    def __init__(self): self.subs = []; self.lock = threading.Lock()
    def subscribe(self):
        q = _q.Queue(maxsize=500)
        with self.lock: self.subs.append(q)
        return q
    def unsubscribe(self, q):
        with self.lock:
            if q in self.subs: self.subs.remove(q)
    def publish(self, ch, payload):
        ev = {"channel": ch, "payload": payload}
        with self.lock: subs = list(self.subs)
        for q in subs:
            try: q.put_nowait(ev)
            except _q.Full:
                try: q.get_nowait(); q.put_nowait(ev)
                except _q.Empty: pass
bus = Bus()

# ==================================================================== OUTBOUND
_session = requests.Session()

def _check_response(r: requests.Response):
    """Decide real success/failure. Node-RED's backend wraps the true result inside
    the JSON body -- {"status": "200"/"500", "response": {"data": {"data": {...}}}} --
    so an HTTP 200 alone does not guarantee the server actually accepted the data.
    We check the wrapped status first; if the body isn't in that shape (or isn't
    JSON at all), we safely fall back to the plain HTTP status code.
    Returns (ok: bool, detail: str|None)."""
    http_ok = 200 <= r.status_code < 300
    try:
        body = r.json()
    except ValueError:
        return http_ok, (None if http_ok else r.text[:300])
    if isinstance(body, dict) and "status" in body:
        wrapped_ok = str(body.get("status")) == "200"
        detail = None if wrapped_ok else json.dumps(body)[:300]
        return wrapped_ok, detail
    return http_ok, (None if http_ok else r.text[:300])

def send_signal(station_code, mc_no, signal, value):
    """Direct-send-first. On failure, drop into the retry queue -- msg_no and
    payload are fixed at this point so a retry re-sends the identical message."""
    package_no = PACKAGE_NO[signal]
    seq = store.next_seq(station_code, signal)
    dt = now_myt_string()
    msg_no = f"{mc_no}/{package_no}_{dt}_{seq}"
    payload = {"data": {"data": {"datetime": dt, "mc_no": mc_no, PAYLOAD_FIELD[signal]: str(value),
                        "package_no": package_no, "msg_no": msg_no}}}
    url = ENDPOINTS[signal]
    store.log_history(station_code, signal, value, mc_no, package_no, msg_no, dt, status="sending")
    try:
        r = _session.post(url, json=payload, timeout=HTTP_TIMEOUT)
        ok, error = _check_response(r)
        http_status = r.status_code
    except requests.RequestException as e:
        ok, http_status, error = False, None, str(e)[:300]
    if ok:
        store.update_history_status(msg_no, "sent", http_status)
        bus.publish("sent", {"station": station_code, "signal": signal, "value": value})
    else:
        store.update_history_status(msg_no, "queued", http_status, error)
        store.enqueue(station_code, signal, payload, url, msg_no)
        bus.publish("queued", {"station": station_code, "signal": signal, "value": value, "error": error})

def send_line_idle():
    """Line-level 'production stopped' packet: {"data":{"data":{datetime, mc_no:"SMEC",
    prodStatus:0}}} -- exactly the shape the developer specified (no package_no/msg_no
    in the body). Same delivery path as send_signal: log, post, retry queue on failure.
    msg_no is only our own unique key for history/queue, it is not sent."""
    dt = now_myt_line_string()
    seq = store.next_seq(LINE_MC_NO, "line_idle")
    msg_no = f"{LINE_MC_NO}/{PACKAGE_NO['production']}_{dt}_{seq}"
    payload = {"data": {"data": {"datetime": dt, "mc_no": LINE_MC_NO, "prodStatus": 0}}}
    url = ENDPOINTS["production"]
    store.log_history(LINE_MC_NO, "line_idle", 0, LINE_MC_NO, PACKAGE_NO["production"], msg_no, dt, status="sending")
    try:
        r = _session.post(url, json=payload, timeout=HTTP_TIMEOUT)
        ok, error = _check_response(r)
        http_status = r.status_code
    except requests.RequestException as e:
        ok, http_status, error = False, None, str(e)[:300]
    if ok:
        store.update_history_status(msg_no, "sent", http_status)
        bus.publish("sent", {"station": LINE_MC_NO, "signal": "line_idle", "value": 0})
    else:
        store.update_history_status(msg_no, "queued", http_status, error)
        store.enqueue(LINE_MC_NO, "line_idle", payload, url, msg_no)
        bus.publish("queued", {"station": LINE_MC_NO, "signal": "line_idle", "value": 0, "error": error})

class LineIdleWatchdog:
    """Tracks the last time ANY station showed a running signal. Fires once when the
    whole line has been silent for LINE_IDLE_SECONDS, and re-arms on the next signal.
    Starts 'just active' so an app restart waits the full delay before the first send."""
    def __init__(self):
        self.last_activity = time.monotonic()
        self.fired = False

    def note_activity(self):
        self.last_activity = time.monotonic()
        self.fired = False

    def due(self) -> bool:
        if not self.fired and time.monotonic() - self.last_activity >= LINE_IDLE_SECONDS:
            self.fired = True
            return True
        return False

line_watchdog = LineIdleWatchdog()

def retry_loop():
    """Same defensive wrapping as poll_forever -- an uncaught exception here must
    never permanently kill retry processing in the background."""
    while True:
        try:
            rel = store.get_reliability_settings()
            row = store.oldest_queued()
            if row:
                store.mark_attempt(row["id"])
                try:
                    r = _session.post(row["url"], json=row["payload"], timeout=HTTP_TIMEOUT)
                    ok, error = _check_response(r)
                    http_status = r.status_code
                except requests.RequestException as e:
                    ok, http_status, error = False, None, str(e)[:300]
                if ok:
                    store.dequeue(row["id"])
                    store.update_history_status(row["msg_no"], "sent", http_status)
                    bus.publish("sent", {"station": row["station_code"], "signal": row["signal"]})
                elif row["attempts"] + 1 >= rel["max_retry_attempts"]:
                    store.dequeue(row["id"])
                    store.update_history_status(row["msg_no"], "failed", http_status, error or "max attempts exceeded")
                    bus.publish("failed", {"station": row["station_code"], "signal": row["signal"], "error": error})
                # else: stays in queue, tried again next cycle
            sleep_for = rel["retry_interval_seconds"]
        except Exception as e:
            log.error("retry_loop error (continuing): %s", e)
            sleep_for = RETRY_INTERVAL_BOUNDS[0]
        time.sleep(sleep_for)

# ==================================================================== SIGNAL LOGIC (per station)
class EdgePipeline:
    """One independent instance per (station, signal-kind). Mirrors the Node-RED
    behaviour: rising edge (0->1) is sent immediately; falling edge (1->0) is only
    sent after it has genuinely stayed 0 for DOWNTIME_GRACE_SECONDS (avoids
    flagging brief blips as downtime). Unlike the literal Node-RED flow, we only
    SEND on an actual state change, not on every poll cycle."""
    def __init__(self):
        self.value = 0
        self.pending_off_at: Optional[float] = None

    def feed(self, raw_bit: bool):
        """Returns (new_value, changed) if a send should happen, else (None, False)."""
        now = time.monotonic()
        if raw_bit:
            self.pending_off_at = None
            if self.value != 1:
                self.value = 1
                return 1, True
            return None, False
        else:
            if self.value == 1 and self.pending_off_at is None:
                self.pending_off_at = now + DOWNTIME_GRACE_SECONDS
            if self.pending_off_at is not None and now >= self.pending_off_at:
                self.pending_off_at = None
                if self.value != 0:
                    self.value = 0
                    return 0, True
            return None, False

class CountPipeline:
    """Rising-edge pulse counter with a long debounce (60s, matches the real
    Node-RED system's rate limiter). Used for Count (Station 5's Start bit).
    NOT used for Reject -- that's a DM word we mirror directly, not a bit we
    edge-count ourselves; see the reject handling in poll_station."""
    def __init__(self):
        self.last_raw = False
        self.last_edge_at = 0.0

    def feed(self, raw_bit: bool) -> bool:
        """Returns True exactly once per genuine rising edge, at most once per COUNT_DEBOUNCE_SECONDS."""
        now = time.monotonic()
        edge = raw_bit and not self.last_raw and (now - self.last_edge_at) >= COUNT_DEBOUNCE_SECONDS
        self.last_raw = raw_bit
        if edge:
            self.last_edge_at = now
            return True
        return False

class StationRuntime:
    def __init__(self, cfg, resume_uptime=0, resume_prod=0, resume_reject=None):
        """resume_uptime/resume_prod seed the pipelines from the last known state in
        the database, so an app RESTART does not blindly assume the machine is OFF
        when it may genuinely still be running. Without this, every restart looked
        like a fresh "rising edge" to us and sent a duplicate Start -- which the
        real backend correctly rejects with "Machine Already Started!"/"Already In
        Production!" if it never saw a matching Stop in between.

        resume_reject seeds last_reject_value from the last known mirrored total,
        so a restart doesn't see D272's already-known count as a brand new change
        and re-send it."""
        self.cfg = cfg
        self.uptime_pipe = EdgePipeline()
        self.uptime_pipe.value = 1 if resume_uptime else 0
        self.prod_pipe = EdgePipeline()
        self.prod_pipe.value = 1 if resume_prod else 0
        self.count_pipe = CountPipeline() if cfg["has_count"] else None
        # Production is sent PRODUCTION_SEND_DELAY_SECONDS after Uptime, but Count fires on the
        # same Start edge. The backend rejects Count until Production is registered, so Counts
        # that fire while a Production send is pending wait here and go out right after it.
        self.prod_lock = threading.Lock()
        self.prod_pending = False
        self.deferred_counts = []
        # Reject mirrors a DM word (D272) directly -- not a bit, so no CountPipeline.
        self.last_reject_value = resume_reject if (cfg["has_reject"] and cfg["reject"]) else None

# Only trust a resumed "already running" state if it was confirmed genuinely
# recently (within RESUME_STALENESS_SECONDS). Otherwise the very first real
# Start pulse after a restart gets silently swallowed -- the app thinks
# "already running" from stale/old data and only catches the SECOND pulse.
# This matters a lot during active testing, where the app gets restarted often
# and old test data can easily be stale by the time of a new restart.
RESUME_STALENESS_SECONDS = 120

def _is_resume_state_fresh(last_success_at_str):
    if not last_success_at_str:
        return False
    try:
        last = datetime.strptime(last_success_at_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return False
    return (datetime.now() - last).total_seconds() <= RESUME_STALENESS_SECONDS

_resume_states = store.get_states()
def _resume_value(code, field):
    st = _resume_states.get(code, {})
    if st.get(field) and _is_resume_state_fresh(st.get("last_success_at")):
        return 1
    return 0

runtimes = {s["code"]: StationRuntime(s,
    resume_uptime=_resume_value(s["code"], "uptime_status"),
    resume_prod=_resume_value(s["code"], "prod_status"),
    resume_reject=store.get_total(s["code"], "reject"))
    for s in STATIONS}

# ==================================================================== POLLING LOOP
_next_due: dict = {s["code"]: 0.0 for s in STATIONS}  # monotonic time each station is next due

def send_production_then_counts(rt, code, mc_no, value):
    """Timer target: send Production, then release any Counts that were held back
    waiting for it (see StationRuntime.deferred_counts). Counts are sent AFTER
    Production, so if Production lands in the retry queue the Count queues behind it
    and the FIFO retry loop still delivers them in the right order."""
    try:
        send_signal(code, mc_no, "production", value)
    except Exception as e:
        log.error("production send error for %s (continuing): %s", code, e)
    with rt.prod_lock:
        rt.prod_pending = False
        held, rt.deferred_counts = rt.deferred_counts, []
    for total in held:
        send_signal(code, mc_no, "count", total)

def poll_station(s):
    code, mc_no = s["code"], s["mc_no"]
    rt = runtimes[code]
    try:
        start_raw = fins.read_bit(*s["start"])
        stop_raw = fins.read_bit(*s["stop"])
        sensor2_raw = fins.read_bit(*s["sensor2"]) if s.get("sensor2") else False
        reject_value = fins.read_word(s["reject"]) if (s.get("has_reject") and s.get("reject")) else None
    except (FinsError, OSError, socket.timeout) as e:
        log.warning("FINS read failed for %s: %s", code, e)
        fins.close()  # force a fresh connect next attempt
        store.update_state(code, plc_ok=0, last_attempt_at=now_s())
        return False

    # Uptime -- driven by the Start bit, OR'd with Sensor 2 when the station has one
    # (only Station 1 -- either bit firing counts as "running", per the client's PDF).
    effective_trigger = start_raw or sensor2_raw
    if effective_trigger:
        line_watchdog.note_activity()
    uv, uchanged = rt.uptime_pipe.feed(effective_trigger)
    if uchanged:
        send_signal(code, mc_no, "uptime", uv)

    # Production -- sent internally only, NOT surfaced anywhere in the UI. Exists
    # purely to satisfy the backend's requirement that Production be registered
    # before it will accept Count for the same station. Delayed after Uptime for
    # the same reason (their backend needs time to register Uptime first).
    pv, pchanged = rt.prod_pipe.feed(effective_trigger)
    if pchanged:
        with rt.prod_lock:
            rt.prod_pending = True
        threading.Timer(PRODUCTION_SEND_DELAY_SECONDS, send_production_then_counts,
                        args=(rt, code, mc_no, pv)).start()

    # Count: only Station 5 -- Start bit rising edge = one finished unit. Sent
    # immediately, EXCEPT when this same poll cycle (or an earlier one) has a
    # Production send still pending: the backend rejects Count with "Machine
    # Currently Not Running In Production" until Production is registered, and
    # Count fires on the same edge that triggers Production. In that case the Count
    # is held and sent right after Production (send_production_then_counts).
    if rt.count_pipe and rt.count_pipe.feed(start_raw):
        total = store.bump_total(code, "count")
        with rt.prod_lock:
            hold = rt.prod_pending
            if hold:
                rt.deferred_counts.append(total)
        if not hold:
            send_signal(code, mc_no, "count", total)

    # Reject: only Station 5. D272 is a DM WORD holding the PLC's own running
    # NG/reject count (TOGO_DisplayValue_Judgment_NG) -- confirmed directly in
    # Sysmac Studio's own Global Variables table, then live-tested on-site
    # reading real values. We mirror that number directly (read_word, not
    # read_bit/edge-counting), sending only when it actually changes from what
    # we last saw -- the PLC does the counting, we just relay it.
    if reject_value is not None:
        if rt.last_reject_value is None or reject_value != rt.last_reject_value:
            store.set_total(code, "reject", reject_value)
            send_signal(code, mc_no, "reject", reject_value)
            rt.last_reject_value = reject_value

    store.update_state(code, uptime_status=rt.uptime_pipe.value, prod_status=rt.prod_pipe.value,
        count=store.get_total(code, "count"), reject_count=store.get_total(code, "reject"),
        plc_ok=1, last_attempt_at=now_s(), last_success_at=now_s(),
        raw_start=int(start_raw), raw_stop=int(stop_raw), raw_sensor2=int(sensor2_raw))
    bus.publish("station", {"station": code, "uptime": rt.uptime_pipe.value,
        "start_raw": start_raw, "stop_raw": stop_raw})
    return True

def poll_forever():
    """Each station is polled on its own schedule (settings key poll_interval:<code>,
    editable from the Connection Info tab), not all in lockstep -- so changing one
    station's rate never affects the others.

    The whole loop body is wrapped in try/except: this thread runs unsupervised in
    the background, and an uncaught exception here silently kills polling FOREVER
    with no visible sign except a traceback in the console -- every station would
    appear stuck/DOWN with no indication why. That exact failure happened once
    (a SQLite thread-safety bug); this guard means a future bug degrades to "log
    it and keep going" instead of "the whole app quietly stops working"."""
    while True:
        try:
            now = time.monotonic()
            for s in STATIONS:
                if now >= _next_due[s["code"]]:
                    poll_station(s)
                    interval = store.get_poll_interval(s["code"], s["poll_interval"])
                    _next_due[s["code"]] = time.monotonic() + interval
            # Only judge "idle" while the PLC is actually answering -- if reads are
            # failing we can't know the line is idle, so don't report it as stopped.
            if all(st["plc_ok"] for st in store.get_states().values()) and line_watchdog.due():
                threading.Thread(target=send_line_idle, daemon=True).start()
        except Exception as e:
            log.error("poll_forever loop error (continuing): %s", e)
        time.sleep(TICK_SECONDS)

# ==================================================================== WEB APP
@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=poll_forever, daemon=True).start()
    threading.Thread(target=retry_loop, daemon=True).start()
    yield

app = FastAPI(title="SMEC Line Bridge", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

def bad(msg, code=400): return JSONResponse(status_code=code, content={"detail": msg})

@app.get("/", response_class=HTMLResponse)
def index():
    return open("templates/index.html").read()

@app.get("/api/stations")
def api_stations():
    states = store.get_states()
    out = []
    for s in STATIONS:
        st = states.get(s["code"], {})
        out.append({"code": s["code"], "label": s["label"], "mc_no": s["mc_no"],
                    "has_count": s["has_count"], "has_reject": s["has_reject"],
                    "reject_wired": bool(s["reject"]), **st})
    return out

@app.get("/api/config")
def api_config():
    """PLC connection and outbound API endpoints are fixed (real wiring, shown read-only).
    Reliability settings and per-station poll interval ARE editable -- see PUT /api/settings
    and PUT /api/stations/{code}/poll-interval below."""
    rel = store.get_reliability_settings()
    return {
        "input_connection": {
            "protocol": "FINS over UDP", "host": PLC_HOST, "port": PLC_PORT,
            "mode": "NX", "da1": FINS_DA1, "sa1": FINS_SA1, "timeout_seconds": FINS_TIMEOUT,
        },
        "reliability": {
            "response_check": "HTTP status + wrapped body status field (falls back to HTTP status if body isn't wrapped)",
            "sequence_reset": "daily at midnight (per station/signal)",
            **rel,
        },
        "reliability_bounds": {
            "max_retry_attempts": RETRY_ATTEMPTS_BOUNDS, "retry_interval_seconds": RETRY_INTERVAL_BOUNDS,
            "max_queue_size": QUEUE_SIZE_BOUNDS,
        },
        "outbound_apis": [
            {"signal": "Uptime", "url": ENDPOINTS["uptime"], "package_no": PACKAGE_NO["uptime"], "confirmed": True},
            {"signal": "Count", "url": ENDPOINTS["count"], "package_no": PACKAGE_NO["count"], "confirmed": True},
            {"signal": "Reject", "url": ENDPOINTS["reject"], "package_no": PACKAGE_NO["reject"], "confirmed": False},
        ],
        "stations": [{"code": s["code"], "label": s["label"], "mc_no": s["mc_no"],
                      "start": f"W{s['start'][0]}.{s['start'][1]:02d}", "stop": f"W{s['stop'][0]}.{s['stop'][1]:02d}",
                      "sensor2": (f"W{s['sensor2'][0]}.{s['sensor2'][1]:02d}" if s.get("sensor2") else "--"),
                      "reject": (f"D{s['reject']}" if s["reject"] else "Not wired yet"),
                      "poll_interval": store.get_poll_interval(s["code"], s["poll_interval"])} for s in STATIONS],
        "poll_interval_bounds": POLL_INTERVAL_BOUNDS,
    }

@app.put("/api/settings")
def api_update_settings(payload: dict = Body(...)):
    """Edit reliability settings: max_retry_attempts, retry_interval_seconds, max_queue_size.
    Takes effect on the retry loop's next cycle -- no restart needed."""
    lo, hi = RETRY_ATTEMPTS_BOUNDS
    if "max_retry_attempts" in payload:
        v = int(payload["max_retry_attempts"])
        if not (lo <= v <= hi): return bad(f"max_retry_attempts must be between {lo} and {hi}.")
        store.set_setting("max_retry_attempts", v)
    lo, hi = RETRY_INTERVAL_BOUNDS
    if "retry_interval_seconds" in payload:
        v = float(payload["retry_interval_seconds"])
        if not (lo <= v <= hi): return bad(f"retry_interval_seconds must be between {lo} and {hi}.")
        store.set_setting("retry_interval_seconds", v)
    lo, hi = QUEUE_SIZE_BOUNDS
    if "max_queue_size" in payload:
        v = int(payload["max_queue_size"])
        if not (lo <= v <= hi): return bad(f"max_queue_size must be between {lo} and {hi}.")
        store.set_setting("max_queue_size", v)
    return {"ok": True, "reliability": store.get_reliability_settings()}

@app.put("/api/stations/{code}/poll-interval")
def api_update_poll_interval(code: str, payload: dict = Body(...)):
    """Edit a single station's poll interval (seconds). Takes effect on that
    station's next scheduled poll -- no restart needed."""
    if code not in STATIONS_BY_CODE:
        return bad("Unknown station.", 404)
    lo, hi = POLL_INTERVAL_BOUNDS
    try:
        v = float(payload["poll_interval"])
    except (KeyError, TypeError, ValueError):
        return bad("poll_interval must be a number.")
    if not (lo <= v <= hi):
        return bad(f"poll_interval must be between {lo} and {hi} seconds.")
    store.set_setting(f"poll_interval:{code}", v)
    return {"ok": True, "station": code, "poll_interval": v}

@app.get("/api/dashboard")
def api_dashboard():
    states = store.get_states()
    plc_connected = all(v["plc_ok"] for v in states.values()) if states else False
    return {"plc_connected": plc_connected, "queue_size": store.queue_count(),
            "stations": [{"code": s["code"], "label": s["label"], **states.get(s["code"], {})} for s in STATIONS]}

@app.get("/api/history")
def api_history(station_code: str = "", signal: str = "", status: str = "", limit: int = 100, offset: int = 0):
    items, total = store.history_page(station_code or None, signal or None, status or None, limit, offset)
    return {"items": items, "total": total}

@app.get("/api/queue")
def api_queue(limit: int = 100, offset: int = 0):
    items, total = store.queue_page(limit, offset)
    return {"items": items, "total": total}

@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    sub = bus.subscribe()
    try:
        while True:
            try:
                ev = await __import__("asyncio").get_event_loop().run_in_executor(None, sub.get, True, 1.0)
            except _q.Empty:
                continue
            await ws.send_json(ev)
    except Exception:
        pass
    finally:
        bus.unsubscribe(sub)

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
    uvicorn.run(app, host="0.0.0.0", port=WEB_PORT)
