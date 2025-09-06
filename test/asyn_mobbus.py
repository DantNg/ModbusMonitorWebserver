# thread_modbus_50.py  -- Python 3.13, threads + barrier + scheduler
import math, time, threading
from dataclasses import dataclass
from typing import List
from flask import Flask, render_template_string
from flask_socketio import SocketIO
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException

# ===================== CẤU HÌNH =====================
GATEWAY_HOST = "127.0.0.1"
GATEWAY_PORT = 502
NUM_UNITS     = 30       # device_id 1..50
INTERVAL_SEC  = 1      # chu kỳ đọc mỗi thiết bị (ví dụ 0.2s nếu muốn nhanh)
TIMEOUT_SEC   = 0.5      # timeout mỗi lần đọc (đặt < INTERVAL_SEC)
READ_COUNT    = 6        # số thanh ghi đọc mỗi lần (giảm để nhanh hơn)

# Nếu bạn muốn CHẠY BÙ (không bỏ tick): đặt True (có thể dồn đọc nếu bị trễ)
# Nếu False: sẽ bắt nhịp bằng cách nhảy qua tick trễ (nhanh nhưng có thể "nhảy số")
DRAIN_BACKLOG = False

# ===================== DATA MODEL =====================
@dataclass
class Device:
    id: str
    device_id: int
    interval: float
    timeout: float

DEVICES: List[Device] = [
    Device(id=f"dev{u}", device_id=u, interval=INTERVAL_SEC, timeout=TIMEOUT_SEC)
    for u in range(1, NUM_UNITS + 1)
]

# ===================== WEB + SOCKET.IO =====================
app = Flask(__name__)
app.config["SECRET_KEY"] = "secret!"
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

INDEX_HTML = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Modbus Live Dashboard (50 units)</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <script src="https://cdn.socket.io/4.7.5/socket.io.min.js" crossorigin="anonymous"></script>
  <style>
    :root { --ok:#10b981; --err:#ef4444; --muted:#9ca3af; --card:#111827; --bg:#0b1020; --fg:#e5e7eb; }
    body { background:var(--bg); color:var(--fg); font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin:0; }
    .wrap { max-width: 1600px; margin: 24px auto; padding: 0 16px; }
    h1 { font-size: 20px; font-weight: 700; margin: 0 0 12px; }
    .grid { display:grid; grid-template-columns: repeat(10, minmax(140px, 1fr)); gap: 12px; align-items:stretch; }
    .card { background:var(--card); border-radius: 14px; padding: 10px; box-shadow: 0 8px 24px rgba(0,0,0,.25); position: relative; overflow: hidden; }
    .title { font-weight: 700; font-size: 14px; margin-bottom: 6px; display:flex; align-items:center; gap:8px;}
    .pill { display:inline-flex; align-items:center; gap:6px; font-size:12px; padding:3px 8px; border-radius:999px; background:#1f2937; color:#d1d5db; }
    .pill.ok { background: rgba(16,185,129,.15); color:#a7f3d0; }
    .pill.err { background: rgba(239,68,68,.15); color:#fecaca; }
    .meta { font-size:11px; color:#9ca3af; display:flex; gap:10px; flex-wrap:wrap; margin-bottom: 6px; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size:12px; }
    .regs { margin-top: 6px; padding:6px; background:#0b1222; border-radius:10px; min-height: 38px; }
    .spark { position:absolute; left:0; bottom:0; height:3px; width:0; background: linear-gradient(90deg, #22d3ee, #8b5cf6, #22c55e); transition: width .2s ease; }
    .ts { font-variant-numeric: tabular-nums; }
    .muted { color: var(--muted); }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Modbus Live Dashboard — 30 units on {{host}}:{{port}}</h1>
    <div class="grid" id="cards">
      <script>
        for (let u = 1; u <= 30; u++) {
          document.write(`
            <div class="card" id="card-dev${u}">
              <div class="title">
                <span>dev${u}</span>
                <span class="pill" id="pill-dev${u}">waiting…</span>
              </div>
              <div class="meta">
                <div>device_id: <b>${u}</b></div>
                <div>latency: <b id="lat-dev${u}">—</b> ms</div>
                <div>updated: <span class="ts" id="ts-dev${u}">—</span></div>
                <div>seq: <b id="seq-dev${u}">—</b></div>
              </div>
              <div class="regs mono" id="regs-dev${u}">
                <span class="muted">[no data]</span>
              </div>
              <div class="spark" id="spark-dev${u}"></div>
            </div>
          `);
        }
      </script>
    </div>
  </div>

  <script>
    const socket = io("/", { transports: ["websocket"] });
    function fmtTs(t) {
      const d = new Date(t*1000);
      const pad = n => String(n).padStart(2,"0");
      return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${String(d.getMilliseconds()).padStart(3,"0")}`;
    }
    function pulse(id) {
      const el = document.getElementById(`spark-${id}`);
      if (!el) return;
      el.style.width = "100%";
      setTimeout(() => el.style.width = "0%", 160);
    }
    function setPill(id, ok, text) {
      const el = document.getElementById(`pill-${id}`);
      if (!el) return;
      el.classList.remove("ok","err");
      if (ok === true) { el.classList.add("ok"); el.textContent = "OK"; }
      else if (ok === false) { el.classList.add("err"); el.textContent = text || "ERR"; }
      else { el.textContent = "waiting…"; }
    }
    socket.on("connect", () => console.log("connected"));
    socket.on("modbus_update", (msg) => {
      const id = msg.device_id;
      const regsEl = document.getElementById(`regs-${id}`);
      const tsEl = document.getElementById(`ts-${id}`);
      const latEl = document.getElementById(`lat-${id}`);
      const seqEl = document.getElementById(`seq-${id}`);
      if (msg.ok) {
        setPill(id, true);
        if (Array.isArray(msg.data) && msg.data.length) regsEl.textContent = msg.data.join(", ");
        else regsEl.textContent = "[empty]";
      } else {
        setPill(id, false, "ERR");
        regsEl.textContent = msg.error || "error";
      }
      if (typeof msg.latency_ms === "number") latEl.textContent = msg.latency_ms;
      if (typeof msg.ts === "number") tsEl.textContent = fmtTs(msg.ts);
      if (typeof msg.seq === "number") seqEl.textContent = msg.seq;
      pulse(id);
    });
  </script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(INDEX_HTML, host=GATEWAY_HOST, port=GATEWAY_PORT)

# ===================== WORKER (1 thread / device) =====================
def reader(dev: Device, start_epoch: float, barrier: threading.Barrier):
    """
    - Đồng bộ xuất phát bằng Barrier.
    - Lịch chống trôi theo mốc chung (start_epoch).
    - Bắt nhịp: bỏ tick trễ (mặc định) hoặc chạy bù (nếu DRAIN_BACKLOG=True).
    """
    client = ModbusTcpClient(host=GATEWAY_HOST, port=GATEWAY_PORT, timeout=dev.timeout)
    client.connect()
    try:
        # 1) Đợi tất cả thread sẵn sàng
        barrier.wait()

        # 2) Chờ tới mốc xuất phát chung
        now = time.monotonic()
        if now < start_epoch:
            barrier.wait(timeout=start_epoch - now)

        next_run = start_epoch
        seq = 0
        while True:
            now = time.monotonic()
            if now < next_run:
                time.sleep(next_run - now)

            t0 = time.perf_counter()
            ok, data, err = False, None, None
            try:
                # API theo bạn cung cấp: device_id=
                rr = client.read_holding_registers(
                    0, count=READ_COUNT, slave              =dev.device_id
                )
                if rr.isError():
                    err = str(rr)
                else:
                    ok = True
                    data = rr.registers
            except Exception as e:
                err = repr(e)

            seq += 1
            socketio.emit("modbus_update", {
                "device_id": dev.id,
                "unit": dev.device_id,
                "ok": ok,
                "data": data,
                "error": err,
                "seq": seq,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "ts": time.time(),
            }, namespace="/")

            # 3) Lên lịch tick tiếp theo
            next_run += dev.interval

            # 4) Bắt kịp lịch
            if DRAIN_BACKLOG:
                # chạy bù (có thể dồn khi thiết bị chậm)
                while time.monotonic() >= next_run:
                    # chạy ngay một vòng "bù"
                    t0 = time.perf_counter()
                    ok, data, err = False, None, None
                    try:
                        rr = client.read_holding_registers(
                            0, count=READ_COUNT, device_id=dev.device_id
                        )
                        if rr.isError():
                            err = str(rr)
                        else:
                            ok = True
                            data = rr.registers
                    except Exception as e:
                        err = repr(e)

                    seq += 1
                    socketio.emit("modbus_update", {
                        "device_id": dev.id,
                        "unit": dev.device_id,
                        "ok": ok,
                        "data": data,
                        "error": err,
                        "seq": seq,
                        "latency_ms": int((time.perf_counter() - t0) * 1000),
                        "ts": time.time(),
                    }, namespace="/")

                    next_run += dev.interval
            else:
                # bỏ qua các mốc đã trễ để bắt nhịp nhanh
                while time.monotonic() >= next_run:
                    next_run += dev.interval
    finally:
        try: client.close()
        except: pass

def start_poller():
    # Mốc xuất phát chung (giây kế tiếp) + Barrier cho 50 thiết bị
    start_epoch = math.ceil(time.monotonic()) + 1
    barrier = threading.Barrier(len(DEVICES))
    for dev in DEVICES:
        threading.Thread(target=reader, args=(dev, start_epoch, barrier), daemon=True).start()

# ===================== MAIN =====================
if __name__ == "__main__":
    start_poller()
    # Lưu ý: với nhiều thread + SocketIO, để production hãy cân nhắc Redis message_queue
    socketio.run(app, host="0.0.0.0", port=5000, debug=True)
