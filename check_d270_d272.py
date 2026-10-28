"""
Reads D270 (OK/Good count) and D272 (NG/Reject count) side by side, live,
repeatedly -- these are the REAL addresses, confirmed directly in Sysmac
Studio's own Global Variables table + Search Results:

    TOGO_DisplayValue_Judgment_OK  AT %D270 : INT
    TOGO_DisplayValue_Judgment_NG  AT %D272 : INT

(The xlsx's earlier claim of D261/D262 was simply wrong -- this replaces
check_d261_d262.py, same logic, corrected addresses.)

Same FINS connection settings already proven working in app.py, same DM-area
WORD read protocol already validated against a mock server.

Usage:
    venv\\Scripts\\python.exe check_d270_d272.py

Press Ctrl+C to stop.
"""
import socket, struct, threading, time

PLC_HOST = "10.57.10.10"
PLC_PORT = 9600
FINS_DA1 = 10
FINS_SA1 = 4
FINS_TIMEOUT = 0.5

DM_WORDS = {"D270 (OK count)": 270, "D272 (NG count)": 272}
POLL_DELAY = 0.5  # seconds between reads

class FinsClient:
    AREA_DM_WORD = 0x82
    def __init__(self, host, port, da1, sa1, timeout=2.0):
        self.host, self.port = host, port
        self.da1, self.sa1 = da1, sa1
        self.timeout = timeout
        self.sock = None
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
    def read_word(self, word):
        with self.lock:
            if not self.sock:
                self.connect()
            sid = self._next_sid()
            header = bytes([0x80, 0x00, 0x02, 0x00, self.da1, 0x00, 0x00, self.sa1, 0x00, sid])
            command = bytes([0x01, 0x01])
            params = bytes([self.AREA_DM_WORD]) + struct.pack(">H", word) + bytes([0x00]) + struct.pack(">H", 1)
            frame = header + command + params
            self.sock.sendto(frame, (self.host, self.port))
            data, _ = self.sock.recvfrom(1024)
            if len(data) < 16:
                raise RuntimeError(f"short response ({len(data)} bytes)")
            if data[12:14] != b"\x00\x00":
                raise RuntimeError(f"FINS error code {data[12:14].hex()}")
            value = struct.unpack(">H", data[14:16])[0]
            return value

def main():
    fins = FinsClient(PLC_HOST, PLC_PORT, FINS_DA1, FINS_SA1, FINS_TIMEOUT)
    print(f"Reading {', '.join(DM_WORDS.keys())} every {POLL_DELAY}s. Press Ctrl+C to stop.")
    print("Let the line run normally -- watch for D270 to climb as good parts pass,")
    print("and watch D272 during a real reject.\n")
    last_values = {name: None for name in DM_WORDS}
    try:
        while True:
            parts = []
            for name, word in DM_WORDS.items():
                try:
                    value = fins.read_word(word)
                    marker = ""
                    if last_values[name] is not None and value != last_values[name]:
                        marker = " <<< CHANGED"
                    parts.append(f"{name} = {value}{marker}")
                    last_values[name] = value
                except Exception as e:
                    parts.append(f"{name} = ERROR ({e})")
            print("  |  ".join(parts))
            time.sleep(POLL_DELAY)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        fins.close()

if __name__ == "__main__":
    main()
