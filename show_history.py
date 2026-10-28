"""
Prints the most recent history rows INCLUDING Production (hidden in the web UI),
with the backend's error text, plus the current retry queue. Read-only.

Usage:
    venv\\Scripts\\python show_history.py [how_many]
"""
import sqlite3, sys

n = int(sys.argv[1]) if len(sys.argv) > 1 else 40
c = sqlite3.connect("file:smec_bridge.db?mode=ro", uri=True)
print(f"=== last {n} history rows (newest first) ===")
for r in c.execute("""SELECT created_at, station_code, signal, value, status, http_status, error
                      FROM history ORDER BY id DESC LIMIT ?""", (n,)):
    print(" | ".join("" if x is None else str(x) for x in r))
print("\n=== retry queue (oldest first) ===")
for r in c.execute("SELECT created_at, station_code, signal, attempts, msg_no FROM retry_queue ORDER BY created_at"):
    print(" | ".join(str(x) for x in r))
