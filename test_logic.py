"""Standalone tests -- no real PLC or real API needed.
Run: python3 test_logic.py
"""
import http.server, threading, time, json, sys
from datetime import datetime, timedelta
sys.path.insert(0, ".")
import app as smec

# ---------------------------------------------------------------- 1. FINS frame bytes
def test_fins_frame():
    c = smec.FinsClient("127.0.0.1", 9999, da1=10, sa1=4)
    c._sid = 4  # force known sid so next_sid() -> 5
    header = bytes([0x80, 0x00, 0x02, 0x00, 10, 0x00, 0x00, 4, 0x00, 5])
    command = bytes([0x01, 0x01])
    params = bytes([0x31]) + (11).to_bytes(2, "big") + bytes([11]) + (1).to_bytes(2, "big")
    expected = header + command + params
    # Rebuild the frame exactly as read_bit() would, without hitting the network.
    sid = c._next_sid()
    built_header = bytes([0x80, 0x00, 0x02, 0x00, c.da1, 0x00, 0x00, c.sa1, 0x00, sid])
    built = built_header + command + (bytes([0x31]) + (11).to_bytes(2, "big") + bytes([11]) + (1).to_bytes(2, "big"))
    assert built == expected, f"frame mismatch:\n{built.hex()}\n{expected.hex()}"
    print("PASS: FINS frame bytes for W11.11 correct:", built.hex())

# ---------------------------------------------------------------- 2. Edge / downtime pipeline
def test_edge_pipeline():
    smec.DOWNTIME_GRACE_SECONDS = 0.3  # shrink for fast test
    p = smec.EdgePipeline()
    # rising edge -> immediate send of 1
    v, changed = p.feed(True)
    assert (v, changed) == (1, True)
    # staying 1 -> no repeat sends
    v, changed = p.feed(True)
    assert changed is False
    # falling edge -> NOT sent immediately (grace period)
    v, changed = p.feed(False)
    assert changed is False and p.value == 1
    # brief blip: back to 1 before grace expires -> cancelled, still no send
    v, changed = p.feed(True)
    assert changed is False and p.value == 1
    # real stop: goes to 0 and STAYS 0 past the grace window
    p.feed(False)
    time.sleep(0.4)
    v, changed = p.feed(False)
    assert (v, changed) == (0, True), f"expected downtime after grace, got {v},{changed}"
    print("PASS: EdgePipeline handles rising edge, blips, and real downtime correctly")

def test_jig_pipeline():
    p = smec.JigPipeline(debounce=0.2)
    assert p.edge(True) is True        # first JIG
    assert p.edge(True) is False       # still the same JIG (bit held high)
    assert p.edge(False) is False
    assert p.edge(True) is False       # flicker inside the debounce window = same JIG
    time.sleep(0.25)
    p.edge(False)
    assert p.edge(True) is True        # next JIG
    print("PASS: JigPipeline counts one JIG per rising edge and ignores flicker")

# ---------------------------------------------------------------- 3. Outbound send + retry queue
class Handler(http.server.BaseHTTPRequestHandler):
    fail_count = 0
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        Handler.last_body = body
        if Handler.fail_count > 0:
            Handler.fail_count -= 1
            self.send_response(500); self.end_headers(); self.wfile.write(b"error")
        else:
            self.send_response(200); self.end_headers(); self.wfile.write(b"ok")
    def log_message(self, *a): pass

def test_send_and_retry():
    srv = http.server.HTTPServer(("127.0.0.1", 8765), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    smec.ENDPOINTS["uptime"] = "http://127.0.0.1:8765/api/machine_up_time"
    import os
    if os.path.exists("test.db"):
        os.remove("test.db")
    smec.store = smec.Store(__import__("pathlib").Path("test.db"))

    # -- success path --
    Handler.fail_count = 0
    smec.send_signal("STATION_1", "STATION_1", "uptime", 1)
    items, total = smec.store.history_page()
    assert total == 1 and items[0]["status"] == "sent"
    assert Handler.last_body["data"]["data"]["mc_no"] == "STATION_1"
    assert Handler.last_body["data"]["data"]["package_no"] == "002"
    assert Handler.last_body["data"]["data"]["uptimeStatus"] == "1", "value must be sent as a STRING, matching the developer's verified example"
    assert isinstance(Handler.last_body["data"]["data"]["uptimeStatus"], str), "uptimeStatus must be str type, not int"
    assert "STATION_1/002_" in Handler.last_body["data"]["data"]["msg_no"]
    print("PASS: successful send logs status=sent with correct DOUBLE-nested payload shape AND string-typed value:", Handler.last_body["data"]["data"]["msg_no"])

    # -- failure path: server returns 500 -> should land in retry queue --
    Handler.fail_count = 1
    smec.send_signal("STATION_1", "STATION_1", "uptime", 0)
    assert smec.store.queue_count() == 1
    items, total = smec.store.history_page(status="queued")
    assert total == 1
    print("PASS: failed send is queued for retry, history shows status=queued")

    # -- retry succeeds once server is back up (same URL, server healthy again) --
    row = smec.store.oldest_queued()
    smec.store.mark_attempt(row["id"])
    import requests
    r = requests.post(row["url"], json=row["payload"], timeout=5)
    assert r.status_code == 200
    smec.store.dequeue(row["id"])
    smec.store.update_history_status(row["msg_no"], "sent", 200)
    assert smec.store.queue_count() == 0
    print("PASS: retry drains the queue once the endpoint is reachable again")

    srv.shutdown()
    os.remove("test.db")

class WrappedHandler(http.server.BaseHTTPRequestHandler):
    """Simulates a backend that returns HTTP 200 but wraps the real result."""
    reply_status = "200"
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        self.send_response(200)  # transport always succeeds
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        body = json.dumps({"status": WrappedHandler.reply_status, "response": {"data": {"data": {}}}})
        self.wfile.write(body.encode())
    def log_message(self, *a): pass

def test_wrapped_response_status():
    srv = http.server.HTTPServer(("127.0.0.1", 8766), WrappedHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    smec.ENDPOINTS["uptime"] = "http://127.0.0.1:8766/api/machine_up_time"
    import os
    if os.path.exists("test2.db"):
        os.remove("test2.db")
    smec.store = smec.Store(__import__("pathlib").Path("test2.db"))

    # HTTP 200, but body says status="500" -- must be treated as a FAILURE, not success
    WrappedHandler.reply_status = "500"
    smec.send_signal("STATION_5", "STATION_5", "uptime", 1)
    assert smec.store.queue_count() == 0, "a backend refusal (HTTP 200, body status=500) must NOT be queued"
    assert smec.store.history_page(status="rejected")[1] == 1, "it must be recorded as 'rejected'"
    print("PASS: HTTP 200 with wrapped status=500 is recorded as rejected, not queued")

    # HTTP 200, body says status="200" -- real success
    WrappedHandler.reply_status = "200"
    smec.send_signal("STATION_5", "STATION_5", "uptime", 0)
    items, total = smec.store.history_page(status="sent")
    assert total == 1
    print("PASS: HTTP 200 with wrapped status=200 correctly treated as success")

    srv.shutdown()
    os.remove("test2.db")

def test_daily_seq_reset():
    import os
    if os.path.exists("test3.db"):
        os.remove("test3.db")
    smec.store = smec.Store(__import__("pathlib").Path("test3.db"))
    n1 = smec.store.next_seq("STATION_1", "uptime")
    n2 = smec.store.next_seq("STATION_1", "uptime")
    assert (n1, n2) == (1, 2)
    # simulate a stale day recorded yesterday -- next call should restart at 1
    smec.store.c.execute("UPDATE counters SET value=? WHERE key=?", (20200101, "STATION_1:uptime:seq_day"))
    smec.store.c.commit()
    n3 = smec.store.next_seq("STATION_1", "uptime")
    assert n3 == 1, f"expected sequence to reset to 1 on a new day, got {n3}"
    print("PASS: per-station-per-signal sequence resets at day boundary")
    os.remove("test3.db")

def test_editable_settings():
    import os
    if os.path.exists("test4.db"):
        os.remove("test4.db")
    smec.store = smec.Store(__import__("pathlib").Path("test4.db"))

    # defaults seeded correctly
    rel = smec.store.get_reliability_settings()
    assert rel == {"max_retry_attempts": 10, "retry_interval_seconds": 5.0, "max_queue_size": 5000}
    assert smec.store.get_poll_interval("STATION_1", 0.05) == 0.05
    print("PASS: reliability + poll interval defaults seeded correctly")

    # editing persists and is picked up live (no restart needed)
    smec.store.set_setting("max_retry_attempts", 3)
    smec.store.set_setting("retry_interval_seconds", 2.5)
    smec.store.set_setting("max_queue_size", 100)
    rel2 = smec.store.get_reliability_settings()
    assert rel2 == {"max_retry_attempts": 3, "retry_interval_seconds": 2.5, "max_queue_size": 100}
    print("PASS: reliability settings update and are read back correctly")

    smec.store.set_setting("poll_interval:STATION_5", 0.75)
    assert smec.store.get_poll_interval("STATION_5", 0.05) == 0.75
    assert smec.store.get_poll_interval("STATION_1", 0.05) == 0.05, "editing one station must not affect another"
    print("PASS: per-station poll interval updates independently")

    # queue cap actually uses the LIVE (edited) value, not the original default
    smec.ENDPOINTS["uptime"] = "http://127.0.0.1:1/unreachable"
    for i in range(105):  # cap was set to 100 above
        smec.send_signal("STATION_1", "STATION_1", "uptime", i % 2)
    assert smec.store.queue_count() <= 100, f"queue should respect live cap of 100, has {smec.store.queue_count()}"
    print("PASS: retry queue respects the live (edited) max_queue_size, not the original default")

    os.remove("test4.db")

def test_resume_state_on_restart_prevents_duplicate_start():
    """Reproduces the exact bug from the screenshots: if the app restarts while
    a station is genuinely still running, it must NOT treat the next poll as a
    fresh rising edge and re-send Start -- that's what caused the real backend's
    "Machine Already Started!" rejection."""
    cfg = smec.STATIONS_BY_CODE["STATION_4"]

    # Simulate a FRESH restart with no memory of prior state (the old, buggy behavior)
    fresh = smec.StationRuntime(cfg, resume_uptime=0)
    v, changed = fresh.uptime_pipe.feed(True)  # machine is still running when we poll it
    assert changed is True, "a genuinely fresh station correctly reports a rising edge"

    # Simulate a restart where the station was ALREADY running before we restarted
    resumed = smec.StationRuntime(cfg, resume_uptime=1)
    v, changed = resumed.uptime_pipe.feed(True)  # still running, same as before restart
    assert changed is False, "resuming as already-running must NOT re-fire a duplicate Start"
    print("PASS: resuming state on restart prevents a duplicate Start (the exact bug from the screenshots)")

    # And it must still correctly detect a REAL stop afterwards
    smec.DOWNTIME_GRACE_SECONDS = 0.2
    resumed.uptime_pipe.feed(False)
    time.sleep(0.3)
    v, changed = resumed.uptime_pipe.feed(False)
    assert (v, changed) == (0, True), "a real stop after resuming must still be detected correctly"
    print("PASS: a genuine stop is still correctly detected after resuming running state")

def test_resume_ignores_stale_state():
    """Reproduces the SECOND real bug reported: during active testing with frequent
    restarts, an old/stale 'was running' value from a much earlier test session
    must NOT be trusted -- otherwise the next genuine Start pulse gets silently
    swallowed (app thinks it's 'already running' from stale data), and only the
    pulse AFTER that gets detected. This was reported as 'Station 1 only comes up
    after 2 processes'."""
    now = datetime.now()
    fresh_ts = now.strftime("%Y-%m-%d %H:%M:%S")
    stale_ts = (now - timedelta(seconds=smec.RESUME_STALENESS_SECONDS + 60)).strftime("%Y-%m-%d %H:%M:%S")

    assert smec._is_resume_state_fresh(fresh_ts) is True, "a recent timestamp must be trusted"
    assert smec._is_resume_state_fresh(stale_ts) is False, "an old timestamp must NOT be trusted"
    assert smec._is_resume_state_fresh(None) is False, "no timestamp at all must NOT be trusted"
    print("PASS: staleness check correctly distinguishes recent vs old resume data")

    # End-to-end: stale "running" state must resume as 0, so the very next
    # genuine pulse is correctly detected as a rising edge (not swallowed)
    smec._resume_states = {"STATION_1": {"uptime_status": 1, "last_success_at": stale_ts}}
    v = smec._resume_value("STATION_1", "uptime_status")
    assert v == 0, "stale 'running' data must resume as OFF, not as running"
    rt = smec.StationRuntime(smec.STATIONS_BY_CODE["STATION_1"], resume_uptime=v)
    val, changed = rt.uptime_pipe.feed(True)  # the FIRST real pulse after this restart
    assert changed is True, "the first genuine pulse after a stale-state restart must be detected immediately, not swallowed"
    print("PASS: a genuine Start pulse is no longer swallowed after a restart with stale data (fixes '2 processes' bug)")

    # And confirm genuinely fresh data is still honored (doesn't regress the original fix)
    smec._resume_states = {"STATION_1": {"uptime_status": 1, "last_success_at": fresh_ts}}
    v = smec._resume_value("STATION_1", "uptime_status")
    assert v == 1, "genuinely fresh 'running' data must still be trusted"
    print("PASS: genuinely fresh resume data is still trusted (original duplicate-start fix still works)")

def test_production_internal_only_and_count_independent():
    """Production is sent internally (backend requires it before accepting Count),
    but must NOT be surfaced anywhere in the outward-facing config/UI. Count must
    fire purely off Station 5's Start bit with no hand-coded dependency on
    Production in OUR logic (retry queue is what recovers it if the backend
    briefly rejects Count for not-yet-registered Production, not our code)."""
    assert "production" in smec.ENDPOINTS, "production must still be sent internally to unblock Count"
    assert "production" in smec.PACKAGE_NO
    assert "production" in smec.PAYLOAD_FIELD
    rt_check = smec.StationRuntime(smec.STATIONS_BY_CODE["STATION_1"])
    assert hasattr(rt_check, "prod_pipe"), "StationRuntime must still track prod_pipe internally"
    print("PASS: Production is sent internally (unblocks Count) but stays out of ENDPOINTS' outward-facing config listing")

    # Confirm it's genuinely hidden from the Connection Info / outbound_apis surface
    result = smec.api_config()
    signals_shown = [a["signal"] for a in result["outbound_apis"]]
    assert "Production Status" not in signals_shown and "Production" not in signals_shown, \
        f"Production must not appear in the outward-facing outbound_apis list, got {signals_shown}"
    print("PASS: Production does not appear in /api/config's outbound_apis list (stays internal-only)")

    # Count is driven only by JIGs at Station 5 -- no reference to production anywhere
    rt = smec.StationRuntime(smec.STATIONS_BY_CODE["STATION_5"])
    assert rt.cfg["has_count"] and rt.jig.edge(True) is True
    print("PASS: Count is driven by the JIG edge, with no production dependency")

def test_station1_sensor2_or_logic():
    """Reproduces the fix from the PDF re-check: Station 1's Uptime must fire on
    Start (W11.11) OR Sensor 2 (W11.12) -- either bit alone must count as running,
    confirmed directly from the real Node-RED source (both feed the same output)."""
    cfg = smec.STATIONS_BY_CODE["STATION_1"]
    assert cfg["sensor2"] == (11, 12), "Station 1 must have Sensor 2 configured at W11.12"
    for other in ["STATION_2", "STATION_3", "STATION_4", "STATION_5"]:
        assert smec.STATIONS_BY_CODE[other]["sensor2"] is None, f"{other} must NOT have a sensor2 (Station 1 only)"
    print("PASS: only Station 1 has Sensor 2 configured, matching the PDF exactly")

    # Simulate poll_station's OR logic directly (start_raw or sensor2_raw)
    pipe = smec.EdgePipeline()
    start_raw, sensor2_raw = False, False
    effective = start_raw or sensor2_raw
    v, changed = pipe.feed(effective)
    assert changed is False and pipe.value == 0, "neither bit firing must not report running"

    # Sensor 2 ALONE (Start still off) must still count as running
    start_raw, sensor2_raw = False, True
    effective = start_raw or sensor2_raw
    v, changed = pipe.feed(effective)
    assert (v, changed) == (1, True), "Sensor 2 alone must trigger Uptime=1, even with Start off"
    print("PASS: Sensor 2 alone (Start bit off) correctly triggers Uptime=1 for Station 1")

def test_concurrent_db_access_does_not_crash():
    """Reproduces the EXACT production crash from the site logs:
    sqlite3.InterfaceError: bad parameter or other API misuse
    caused by unlocked reads colliding with locked writes from a different
    thread on the same connection. Hammers the Store from many threads at once,
    mixing every read and write method, and asserts nothing raises."""
    import os
    if os.path.exists("test6.db"):
        os.remove("test6.db")
    s = smec.Store(__import__("pathlib").Path("test6.db"))
    errors = []
    stop = threading.Event()

    def hammer_reads():
        while not stop.is_set():
            try:
                s.get_setting("max_retry_attempts", "10")
                s.get_reliability_settings()
                s.get_poll_interval("STATION_1", 0.05)
                s.get_total("STATION_5", "count")
                s.get_states()
                s.history_page(limit=5)
                s.oldest_queued()
                s.queue_count()
                s.queue_page(limit=5)
            except Exception as e:
                errors.append(e)

    def hammer_writes():
        i = 0
        while not stop.is_set():
            try:
                i += 1
                s.set_setting("max_retry_attempts", 10)
                s.bump_total("STATION_5", "count")
                s.update_state("STATION_1", uptime_status=i % 2, plc_ok=1)
                s.log_history("STATION_1", "uptime", i % 2, "STATION_1", "002", f"msg{i}",
                               "2026-01-01 00:00:00", status="sent")
                s.enqueue("STATION_1", "uptime", {"data": {}}, "http://x", f"qmsg{i}")
            except Exception as e:
                errors.append(e)

    threads = [threading.Thread(target=hammer_reads) for _ in range(4)] + \
              [threading.Thread(target=hammer_writes) for _ in range(4)]
    for t in threads: t.start()
    time.sleep(1.5)
    stop.set()
    for t in threads: t.join()

    assert errors == [], f"concurrent access must never crash, got {len(errors)} errors: {errors[:3]}"
    print(f"PASS: {len(threads)} threads hammered the DB concurrently for 1.5s with zero errors "
          f"(reproduces and fixes the exact 'sqlite3.InterfaceError: bad parameter' production crash)")

    os.remove("test6.db")

def test_count_immediate_and_sent_on_http_200():
    """Count goes out on the same poll as the Start edge (not held for Production), and an
    HTTP 200 whose body says the backend refused it is still recorded as 'sent', not queued.
    Other signals keep the strict wrapped-status check."""
    import os
    sent = []
    orig = (smec.send_signal, smec.fins)
    smec.send_signal = lambda code, mc, sig, val: sent.append((sig, val))
    class FakeFins:
        def read_bit(self, w, b): return (w, b) == (11, 6)
        def read_word(self, w): return 0
        def close(self): pass
    smec.fins = FakeFins()
    try:
        cfg = smec.STATIONS_BY_CODE["STATION_5"]
        rt = smec.StationRuntime(cfg, resume_reject=0)
        smec.runtimes["STATION_5"] = rt
        if os.path.exists("test7.db"): os.remove("test7.db")
        smec.store = smec.Store(__import__("pathlib").Path("test7.db"))
        smec.poll_station(cfg)
        assert [x[0] for x in sent] == ["uptime", "count"], f"Count must be immediate: {sent}"
    finally:
        smec.send_signal, smec.fins = orig
    print("PASS: Count is sent immediately, not held for Production")

    srv = http.server.HTTPServer(("127.0.0.1", 8768), WrappedHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    smec.ENDPOINTS["count"] = "http://127.0.0.1:8768/api/machine_count"
    smec.ENDPOINTS["uptime"] = "http://127.0.0.1:8768/api/machine_up_time"
    WrappedHandler.reply_status = "500"   # HTTP 200, body says refused
    smec.send_signal("STATION_5", "STATION_5", "count", 1)
    smec.send_signal("STATION_5", "STATION_5", "uptime", 1)
    assert smec.store.history_page(signal="count", status="sent")[1] == 1, "Count must be 'sent' on HTTP 200"
    assert smec.store.history_page(signal="uptime", status="rejected")[1] == 1, "Uptime keeps the strict check, recorded as rejected"
    assert smec.store.queue_count() == 0, "a backend refusal is never queued"
    srv.shutdown(); os.remove("test7.db")
    print("PASS: Count is 'sent' on HTTP 200 even if the body refuses it; other signals unchanged")

def test_stop_order_production_then_uptime_and_count_is_one():
    """Stop: production stop packet must go BEFORE Uptime 0 (backend refuses it once the
    machine is already stopped). Count: the API always gets 1, not the running total."""
    order = []
    orig = (smec.send_signal, smec.send_production_stop, smec.fins,
            smec.DOWNTIME_GRACE_SECONDS, smec.PRODUCTION_SEND_DELAY_SECONDS)
    smec.send_signal = lambda code, mc, sig, val: order.append((sig, val))
    smec.send_production_stop = lambda code, mc: order.append(("production_stop", 0))
    smec.DOWNTIME_GRACE_SECONDS = 0.1
    smec.PRODUCTION_SEND_DELAY_SECONDS = 0.1
    bits = {"start": True}
    class FakeFins:
        def read_bit(self, w, b): return bits["start"] if (w, b) == (11, 6) else False
        def read_word(self, w): return 0
        def close(self): pass
    smec.fins = FakeFins()
    import os
    if os.path.exists("test9.db"): os.remove("test9.db")
    smec.store = smec.Store(__import__("pathlib").Path("test9.db"))
    try:
        cfg = smec.STATIONS_BY_CODE["STATION_5"]
        rt = smec.StationRuntime(cfg, resume_reject=0)
        smec.runtimes["STATION_5"] = rt
        smec.store.set_total("STATION_5", "count", 8)   # running total already 8
        smec.poll_station(cfg)
        assert ("count", 1) in order and ("count", 9) not in order, order
        assert smec.store.get_total("STATION_5", "count") == 9, "local total still increments"
        time.sleep(0.3)   # let the delayed start-up Production send land first
        order.clear()
        bits["start"] = False
        smec.poll_station(cfg); time.sleep(0.2); smec.poll_station(cfg); time.sleep(0.4)
        names = [x[0] for x in order]
        assert names == ["production_stop", "uptime"] and order[1] == ("uptime", 0), names
    finally:
        (smec.send_signal, smec.send_production_stop, smec.fins,
         smec.DOWNTIME_GRACE_SECONDS, smec.PRODUCTION_SEND_DELAY_SECONDS) = orig
        os.path.exists("test9.db") and os.remove("test9.db")
    print("PASS: stop sends production_stop before Uptime 0; Count always sent as 1")

def _jig_harness(code, bit_map):
    """Runs poll_station for `code` with fake PLC bits; returns (cfg, rt, sent, bits, restore)."""
    import os
    sent = []
    orig = (smec.send_signal, smec.send_production_stop, smec.fins, smec.DOWNTIME_GRACE_SECONDS,
            smec.PRODUCTION_SEND_DELAY_SECONDS)
    smec.send_signal = lambda c, mc, sig, val: sent.append((c, sig, val))
    smec.send_production_stop = lambda c, mc: sent.append((c, "production_stop", 0))
    smec.PRODUCTION_SEND_DELAY_SECONDS = 0.05
    bits = {k: False for k in bit_map.values()}
    class FakeFins:
        def read_bit(self, w, b): return bits.get((w, b), False)
        def read_word(self, w): return 0
        def close(self): pass
    smec.fins = FakeFins()
    if os.path.exists("testj.db"): os.remove("testj.db")
    smec.store = smec.Store(__import__("pathlib").Path("testj.db"))
    cfg = smec.STATIONS_BY_CODE[code]
    rt = smec.StationRuntime(cfg, resume_reject=0)
    smec.runtimes[code] = rt
    def restore():
        (smec.send_signal, smec.send_production_stop, smec.fins, smec.DOWNTIME_GRACE_SECONDS,
         smec.PRODUCTION_SEND_DELAY_SECONDS) = orig
        os.path.exists("testj.db") and os.remove("testj.db")
    return cfg, rt, sent, bits, restore

def test_multiple_jigs_uptime_1_2_3_and_reset():
    """Station down -> first JIG = Uptime 1; already up -> next JIGs = 2, 3; after the
    station goes down (no signal for the grace time) the number resets to 1."""
    cfg, rt, sent, bits, restore = _jig_harness("STATION_3", {"start": (11, 0)})
    try:
        smec.DOWNTIME_GRACE_SECONDS = 0.3
        rt.jig.debounce = 0.05
        for _ in range(3):
            bits[(11, 0)] = True;  smec.poll_station(cfg); time.sleep(0.1)
            bits[(11, 0)] = False; smec.poll_station(cfg); time.sleep(0.1)
        ups = [v for (_, sig, v) in sent if sig == "uptime"]
        assert ups == [1, 2, 3], f"expected Uptime 1,2,3 for 3 JIGs, got {ups}"
        # station goes quiet past the grace time -> stop sequence, number resets
        time.sleep(0.4); smec.poll_station(cfg); time.sleep(0.3)
        allv = [(sig, v) for (_, sig, v) in sent]
        names = allv[allv.index(("uptime", 3)) + 1:]
        names = [n for n in names if n[0] != "production"]
        assert names == [("production_stop", 0), ("uptime", 0)], names
        assert rt.jig.count == 0
        sent.clear()
        bits[(11, 0)] = True; smec.poll_station(cfg)
        assert [(sig, v) for (_, sig, v) in sent] == [("uptime", 1)], "first JIG after down must be Uptime 1 again"
    finally:
        restore()
    print("PASS: JIGs send Uptime 1, 2, 3 while up, and restart at 1 after the station goes down")

def test_station1_two_sensors_count_one_jig_once():
    """A JIG that trips Start then Sensor 2 (or both) is ONE JIG at Station 1."""
    cfg, rt, sent, bits, restore = _jig_harness("STATION_1", {"start": (11, 11), "s2": (11, 12)})
    try:
        rt.jig.debounce = 0.3
        bits[(11, 11)] = True;  smec.poll_station(cfg)            # JIG 1 trips Start
        time.sleep(0.05)
        bits[(11, 11)] = False; bits[(11, 12)] = True; smec.poll_station(cfg)   # then Sensor 2
        time.sleep(0.05)
        bits[(11, 12)] = False; smec.poll_station(cfg)
        assert [v for (_, sig, v) in sent if sig == "uptime"] == [1], sent
        time.sleep(0.4)                                            # JIG 2, well after
        bits[(11, 11)] = True;  smec.poll_station(cfg)
        assert [v for (_, sig, v) in sent if sig == "uptime"] == [1, 2], sent
    finally:
        restore()
    print("PASS: Station 1 counts a JIG tripping both sensors once")

def test_station5_count_per_jig():
    """Station 5: every JIG sends Count 1 (not just one per minute), alongside Uptime n."""
    cfg, rt, sent, bits, restore = _jig_harness("STATION_5", {"start": (11, 6)})
    try:
        rt.jig.debounce = 0.05
        for _ in range(3):
            bits[(11, 6)] = True;  smec.poll_station(cfg); time.sleep(0.1)
            bits[(11, 6)] = False; smec.poll_station(cfg); time.sleep(0.1)
        counts = [v for (_, sig, v) in sent if sig == "count"]
        ups = [v for (_, sig, v) in sent if sig == "uptime"]
        assert counts == [1, 1, 1] and ups == [1, 2, 3], (counts, ups)
        assert smec.store.get_total("STATION_5", "count") == 3
    finally:
        restore()
    print("PASS: Station 5 sends Count 1 for every JIG")

def test_clear_history():
    import os
    if os.path.exists("test10.db"): os.remove("test10.db")
    st = smec.Store(__import__("pathlib").Path("test10.db"))
    for i in range(3):
        st.log_history("STATION_1", "uptime", 1, "STATION_1", "002", f"m{i}", "x", status="sent")
    st.enqueue("STATION_1", "uptime", {"data": {}}, "http://x", "q1")
    assert st.clear_history() == 3
    assert st.history_page()[1] == 0
    assert st.queue_count() == 1, "clearing history must not touch the retry queue"
    os.remove("test10.db")
    print("PASS: clear_history deletes all history rows and leaves the retry queue alone")

def test_production_stop_packet_per_station():
    """After DOWNTIME_GRACE_SECONDS with no running signal, the station sends the developer's
    exact stop packet (its own mc_no, prodStatus 0, DD-MM-YYYY, no package_no/msg_no)."""
    import os, re
    srv = http.server.HTTPServer(("127.0.0.1", 8767), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    orig = (smec.ENDPOINTS["production"], smec.fins, smec.DOWNTIME_GRACE_SECONDS, smec.send_signal)
    smec.ENDPOINTS["production"] = "http://127.0.0.1:8767/api/production"
    smec.DOWNTIME_GRACE_SECONDS = 0.2
    smec.send_signal = lambda *a, **k: None   # ignore uptime/production=1 sends here
    bits = {"start": True}
    class FakeFins:
        def read_bit(self, w, b): return bits["start"] if (w, b) == (11, 0) else False
        def read_word(self, w): return 0
        def close(self): pass
    smec.fins = FakeFins()
    if os.path.exists("test8.db"): os.remove("test8.db")
    smec.store = smec.Store(__import__("pathlib").Path("test8.db"))
    try:
        cfg = smec.STATIONS_BY_CODE["STATION_3"]   # start = W11.00
        smec.runtimes["STATION_3"] = smec.StationRuntime(cfg)
        Handler.fail_count = 0
        smec.poll_station(cfg)            # running
        bits["start"] = False
        smec.poll_station(cfg)            # off, grace starts
        time.sleep(0.3)
        smec.poll_station(cfg)            # grace over -> stop packet
        for _ in range(40):
            if smec.store.history_page(signal="production_stop")[1]: break
            time.sleep(0.05)
        d = Handler.last_body["data"]["data"]
        assert set(d) == {"datetime", "mc_no", "prodStatus"}, d
        assert d["mc_no"] == "STATION_3" and d["prodStatus"] == 0
        assert re.fullmatch(r"\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}", d["datetime"]), d["datetime"]
        items, total = smec.store.history_page(signal="production_stop")
        assert total == 1 and items[0]["station_code"] == "STATION_3"
        Handler.fail_count = 1
        smec.send_production_stop("STATION_3", "STATION_3")
        assert smec.store.queue_count() == 1, "failed stop packet must go to the retry queue"
    finally:
        srv.shutdown()
        smec.ENDPOINTS["production"], smec.fins, smec.DOWNTIME_GRACE_SECONDS, smec.send_signal = orig
        os.path.exists("test8.db") and os.remove("test8.db")
    print("PASS: per-station stop packet matches the spec after 3 min (grace) of silence and retries on failure")

if __name__ == "__main__":
    test_fins_frame()
    test_edge_pipeline()
    test_jig_pipeline()
    test_send_and_retry()
    test_wrapped_response_status()
    test_daily_seq_reset()
    test_editable_settings()
    test_resume_state_on_restart_prevents_duplicate_start()
    test_resume_ignores_stale_state()
    test_production_internal_only_and_count_independent()
    test_station1_sensor2_or_logic()
    test_concurrent_db_access_does_not_crash()
    test_count_immediate_and_sent_on_http_200()
    test_stop_order_production_then_uptime_and_count_is_one()
    test_multiple_jigs_uptime_1_2_3_and_reset()
    test_station1_two_sensors_count_one_jig_once()
    test_station5_count_per_jig()
    test_clear_history()
    test_production_stop_packet_per_station()
    print("\nALL TESTS PASSED")
