"""
Live view of every station's Start / Stop / Sensor2 bit straight from the PLC,
using the SAME addresses and FINS settings as app.py -- so you can see whether
the PLC itself is raising a station's bit, independent of the rest of the app.

Usage (stop the SmecBridge service first -- or it's fine to run alongside):
    venv\\Scripts\\python check_bits.py

Press Ctrl+C to stop. A station that "can't run" but shows Start=1 here means the
app is the problem; Start=0 here means the PLC is not raising that bit.
"""
import time
import app as smec

def main():
    print("Reading all station bits every 0.5s. Ctrl+C to stop.\n")
    last = None
    try:
        while True:
            parts = []
            for s in smec.STATIONS:
                try:
                    st = int(smec.fins.read_bit(*s["start"]))
                    sp = int(smec.fins.read_bit(*s["stop"]))
                    s2 = int(smec.fins.read_bit(*s["sensor2"])) if s.get("sensor2") else "-"
                    parts.append(f"{s['code'][-1]}: start={st} stop={sp} s2={s2}")
                except Exception as e:
                    parts.append(f"{s['code'][-1]}: ERROR({e})")
            line = "  |  ".join(parts)
            if line != last:
                print(time.strftime("%H:%M:%S"), line)
                last = line
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        smec.fins.close()

if __name__ == "__main__":
    main()
