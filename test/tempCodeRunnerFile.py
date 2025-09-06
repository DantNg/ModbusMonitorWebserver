# async_modbus_30.py
import asyncio, math, time
from dataclasses import dataclass
from collections import defaultdict
from contextlib import asynccontextmanager

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

import socketio  # python-socketio (ASGI)
from starlette.applications import Starlette
from starlette.responses import HTMLResponse
from starlette.routing import Route
from uvicorn import run as uvicorn_run


# ===================== CẤU HÌNH =====================
GATEWAY_HOST = "127.0.0.1"
GATEWAY_PORT = 502
NUM_UNITS = 30                 # unit_id 1..30
INTERVAL_SEC = 1.0             # mỗi thiết bị đọc mỗi 1s (đổi tùy nhu cầu)
TIMEOUT_SEC = 0.7              # timeout mỗi lần đọc
PER_HOST_LIMIT = 1             # 1 request một lúc cho TCP->RS485 gateway

# ===================== MÔ TẢ THIẾT BỊ =====================
@dataclass
class Device:
    id: str
    host: str
    port: int
    unit: int
    interval: float
    timeout: float

DEVICES: list[Device] = [
    Device(
        id=f"dev{u}", host=GATEWAY_HOST, port=GATEWAY_PORT,
        unit=u, interval=INTERVAL_SEC, timeout=TIMEOUT_SEC
    )
    for u in range(1, NUM_UNITS+1)
]


# ===================== SOCKET.IO + WEB =====================
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")

# 10 phần tử mỗi hàng + UI giống dashboard trước
INDEX_HTML = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Modbus Live Dashboard (30 units)</title>
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
    <h1>Modbus Live Dashboard — 30 units on {{HOST}}:{{PORT}}</h1>
    <div class="grid" id="cards">
      <!-- tạo 30 card -->
      <script>
        const host = "{{HOST}}"; const port = "{{PORT}}";
        for (let u = 1; u <= 30; u++) {
          document.write(`
            <div class="card" id="card-dev${u}">
              <div class="title">
                <span>dev${u}</span>
                <span class="pill" id="pill-dev${u}">waiting…</span>
              </div>
              <div class="meta">
                <div>unit: <b>${u}</b></div>
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
        if (Array.isArray(msg.data) && msg.data.length) {
          regsEl.textContent = msg.data.join(", ");
        } else {
          regsEl.textContent = "[empty]";
        }
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

# Starlette route "/" trả HTML
async def index(_request):
    html = INDEX_HTML.replace("{{HOST}}", GATEWAY_HOST).replace("{{PORT}}", str(GATEWAY_PORT))
    return HTMLResponse(html)

starlette_app = Starlette(routes=[Route("/", index)])
app = socketio.ASGIApp(sio, other_asgi_app=starlette_app)


# ===================== QUEUE PHÁT RA WEB =====================
broadcast_q: asyncio.Queue = asyncio.Queue(maxsize=5000)

async def broadcaster():
    while True:
        event, payload = await broadcast_q.get()
        try:
            await sio.emit(event, payload)  # phát cho tất cả client
        finally:
            broadcast_q.task_done()


# ===================== POOL THEO HOST =====================
class HostPool:
    def __init__(self, host, port, max_concurrent=1):
        self.host = host
        self.port = port
        self.sem = asyncio.Semaphore(max_concurrent)
        self.client: AsyncModbusTcpClient | None = None
        self.lock = asyncio.Lock()

    @asynccontextmanager
    async def session(self):
        # đảm bảo có client (lazy connect)
        async with self.lock:
            if self.client is None:
                self.client = AsyncModbusTcpClient(host=self.host, port=self.port)
                await self.client.connect()
        try:
            yield self.client
        except Exception:
            try:
                await self.client.close()
            except:
                pass
            self.client = None
            raise

host_pools: dict[tuple[str, int], HostPool] = {}

def get_pool(host, port, per_host_limit=1) -> HostPool:
    key = (host, port)
    if key not in host_pools:
        host_pools[key] = HostPool(host, port, max_concurrent=per_host_limit)
    return host_pools[key]


# ===================== LỊCH CHỐNG TRÔI + PHASE =====================
def init_next_run(interval: float, unit: int) -> float:
    # phase nhỏ vài ms để tránh dồn xung cùng lúc trên gateway
    phase = min(0.02, interval / 10.0) * ((unit % 10) / 10.0)
    now = time.monotonic()
    base = math.floor((now - phase) / interval) * interval + phase
    return base if base > now else base + interval


# ===================== ĐỌC 1 LẦN (đổi tùy nhu cầu) =====================
async def read_once(client: AsyncModbusTcpClient, unit: int):
    rr = await client.read_holding_registers(0, 6, unit=unit)  # đọc 6 thanh ghi
    if rr.isError():
        raise ModbusException(str(rr))
    return rr.registers


# ===================== TASK CHO MỖI THIẾT BỊ =====================
async def poll_device(dev: Device, per_host_limit=1):
    pool = get_pool(dev.host, dev.port, per_host_limit=per_host_limit)
    next_run = init_next_run(dev.interval, dev.unit)
    seq = 0
    while True:
        now = time.monotonic()
        if now < next_run:
            await asyncio.sleep(next_run - now)

        t0 = time.perf_counter()
        ok, data, err = False, None, None
        try:
            async with pool.sem:            # giới hạn song song theo host
                async with pool.session() as client:
                    async with asyncio.timeout(dev.timeout):
                        data = await read_once(client, dev.unit)
                        ok = True
        except Exception as e:
            err = repr(e)

        seq += 1
        payload = {
            "device_id": dev.id,
            "host": dev.host,
            "unit": dev.unit,
            "ok": ok,
            "data": data,
            "error": err,
            "seq": seq,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "ts": time.time(),
        }

        # đẩy ra broadcaster (nếu đầy, giữ mẫu mới nhất)
        try:
            broadcast_q.put_nowait(("modbus_update", payload))
        except asyncio.QueueFull:
            _ = await broadcast_q.get()
            broadcast_q.task_done()
            await broadcast_q.put(("modbus_update", payload))

        # lịch tick kế tiếp + bắt kịp nếu trễ
        next_run += dev.interval
        while time.monotonic() >= next_run:
            next_run += dev.interval


# ===================== MAIN =====================
async def main():
    # chạy broadcaster
    asyncio.create_task(broadcaster())

    # tạo task cho 30 thiết bị
    tasks = [asyncio.create_task(poll_device(d, PER_HOST_LIMIT)) for d in DEVICES]

    # chạy ASGI server
    server = asyncio.create_task(asyncio.to_thread(
        uvicorn_run, app, host="0.0.0.0", port=8000, log_level="info"
    ))

    await asyncio.gather(*tasks, server)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
