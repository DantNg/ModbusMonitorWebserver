# hybrid_modbus_flags.py — Python 3.13
# Bật/tắt:
#  - USE_WORKER_THREADS: thêm thread cho callback/emit (sio.start_background_task)
#  - USE_SHARDS: chia N thiết bị thành SHARDS nhóm, mỗi nhóm chạy event loop riêng trong 1 thread
# Lưu ý: nếu là TCP->RS485 gateway, để PER_HOST_LIMIT=1 và thường không cần USE_SHARDS.

import asyncio, time, math, threading
from dataclasses import dataclass
from typing import List
from contextlib import asynccontextmanager
import janus

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

import socketio
from starlette.applications import Starlette
from starlette.responses import HTMLResponse
from starlette.routing import Route
from uvicorn import run as uvicorn_run

# ================== CẤU HÌNH ==================
HOST, PORT = "127.0.0.1", 502

NUM_DEVICES = 30         # dev1..devN (device_id = 1..N)
INTERVAL     = 0.2       # 200 ms / device
TIMEOUT      = 0.08      # 80 ms / read (nên < INTERVAL)
READ_COUNT   = 2         # đọc ít thanh ghi nhất có thể để nhanh hơn

# Hạn chế song song theo host (để 1 nếu là TCP->RS485 gateway)
PER_HOST_LIMIT = 1       # PLC TCP thật có thể 2..8 (tuỳ thiết bị)

# A) Thread cho callback/emit
USE_WORKER_THREADS = True
WORKERS            = 6   # số thread emit

# B) Thread cho request (shard mỗi thread = 1 event loop)
USE_SHARDS = False
SHARDS     = 4           # chỉ bật khi PLC hỗ trợ đồng thời (không phải RS485)

# Queue & batch emit
QUEUE_MAX     = 10_000   # hàng đợi ra UI
BATCH_EMIT_MS = 8        # gom gói trong ~8ms rồi emit 1 lần (khi KHÔNG dùng worker)

# ================== MODEL ==================
@dataclass
class Dev:
    id: str
    device_id: int
    interval: float
    timeout: float

ALL_DEVS: List[Dev] = [Dev(f"dev{u}", u, INTERVAL, TIMEOUT) for u in range(1, NUM_DEVICES+1)]

# ================== WEB / SOCKET ==================
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")

HTML = """<!doctype html>
<meta charset="utf-8"><title>Hybrid Modbus (flags)</title>
<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
<style>
  body{background:#0b1020;color:#e5e7eb;font-family:system-ui;margin:0;padding:16px}
  .grid{display:grid;grid-template-columns:repeat(10,minmax(140px,1fr));gap:12px}
  .card{background:#111827;border-radius:14px;padding:10px}
  .title{display:flex;gap:8px;align-items:center;font-weight:700}
  .dot{width:10px;height:10px;border-radius:999px;background:#f59e0b}
  .ok{background:#10b981}.err{background:#ef4444}
  .kv{display:grid;grid-template-columns:70px 1fr;gap:6px;font-size:12px}
  .mono{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px}
  .regs{padding:6px;background:#0b1222;border-radius:10px;min-height:32px;margin-top:6px}
</style>
<h3>Hybrid Modbus — {N} devices @ {HOST}:{PORT} | workers={W} | shards={S} | per_host={PHL}</h3>
<div class="grid" id="g"></div>
<script>
const N={N};
const g=document.getElementById('g');
for(let u=1;u<=N;u++){
  g.insertAdjacentHTML('beforeend', `
    <div class="card" id="card-dev${u}">
      <div class="title"><span class="dot" id="dot-dev${u}"></span><span>dev${u}</span></div>
      <div class="kv">
        <div>unit:</div><div id="unit-dev${u}">${u}</div>
        <div>seq:</div><div id="seq-dev${u}">—</div>
        <div>lat:</div><div id="lat-dev${u}">— ms</div>
        <div>time:</div><div id="ts-dev${u}">—</div>
      </div>
      <div class="regs mono" id="regs-dev${u}"><span style="opacity:.6">[no data]</span></div>
    </div>`);
}
const sock=io("/",{transports:["websocket"]});
function fmt(t){const d=new Date(t*1000);const p=n=>String(n).padStart(2,"0");return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${String(d.getMilliseconds()).padStart(3,"0")}`}

function applyOne(m){
  const id=m.device_id;
  const dot=document.getElementById(`dot-${id}`), seq=document.getElementById(`seq-${id}`),
        lat=document.getElementById(`lat-${id}`), ts=document.getElementById(`ts-${id}`),
        regs=document.getElementById(`regs-${id}`);
  if(dot){dot.classList.remove('ok','err'); dot.classList.add(m.ok?'ok':'err');}
  if(seq) seq.textContent = m.seq ?? "—";
  if(lat) lat.textContent = (m.latency_ms ?? "—")+" ms";
  if(ts)  ts.textContent  = typeof m.ts==="number" ? fmt(m.ts) : "—";
  if(regs){
    if(m.ok && Array.isArray(m.data)) regs.textContent = m.data.slice(0,6).join(", ")+(m.data.length>6?" …":"");
    else regs.textContent = (m.error||"error");
  }
}
sock.on("modbus_update", applyOne);
sock.on("modbus_batch", arr => { for(const m of arr) applyOne(m); });
</script>
""".replace("{N}", str(NUM_DEVICES)).replace("{HOST}", HOST).replace("{PORT}", str(PORT))\
   .replace("{W}", str(WORKERS)).replace("{S}", str(SHARDS)).replace("{PHL}", str(PER_HOST_LIMIT))

async def index(_): return HTMLResponse(HTML)
app_star = Starlette(routes=[Route("/", index)])
app = socketio.ASGIApp(sio, other_asgi_app=app_star)

# ================== HÀNG ĐỢI TRUNG TÂM ==================
# Dùng janus để bridge giữa async và thread an toàn
jq = janus.Queue(maxsize=QUEUE_MAX)

# ================== EMIT: 2 CHẾ ĐỘ ==================
async def broadcaster_async_batch():
    """Emit trong main loop, gom batch ~BATCH_EMIT_MS (khi KHÔNG dùng worker threads)."""
    while True:
        event, payload = await jq.async_q.get()
        batch = [(event, payload)]
        t0 = time.perf_counter()
        while (time.perf_counter() - t0) < (BATCH_EMIT_MS/1000.0):
            try:
                batch.append(jq.async_q.get_nowait())
                jq.async_q.task_done()
            except asyncio.QueueEmpty:
                break
        updates = [p for (e,p) in batch if e=="modbus_update"]
        others  = [(e,p) for (e,p) in batch if e!="modbus_update"]
        if len(updates)==1:
            await sio.emit("modbus_update", updates[0])
        elif len(updates)>1:
            await sio.emit("modbus_batch", updates)
        for e,p in others:
            await sio.emit(e,p)
        jq.async_q.task_done()

def worker_emit(sync_q: "janus.SyncQueue"):
    """Emit từ nhiều thread (khi USE_WORKER_THREADS=True). Không cần event loop trong thread."""
    while True:
        event, payload = sync_q.get()
        try:
            # Đưa emit về event loop nội bộ của Socket.IO (thread-safe)
            sio.start_background_task(sio.emit, event, payload)
        finally:
            sync_q.task_done()

# ================== POOL 1 KẾT NỐI/SHARD ==================
class HostPool:
    """Giữ 1 AsyncModbusTcpClient dùng lại + Semaphore giới hạn song song."""
    def __init__(self, host: str, port: int, per_host_limit: int):
        self.host, self.port = host, port
        self.sem = asyncio.Semaphore(per_host_limit)
        self._client: AsyncModbusTcpClient | None = None
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def session(self):
        async with self._lock:
            if self._client is None:
                self._client = AsyncModbusTcpClient(host=self.host, port=self.port)
                ok = await self._client.connect()
                if not ok:
                    raise ConnectionError(f"connect_fail {self.host}:{self.port}")
        try:
            yield self._client
        except Exception:
            try: await self._client.close()
            except: pass
            self._client = None
            raise

# ================== ĐỌC 1 LẦN ==================
async def read_once(cli: AsyncModbusTcpClient, device_id: int):
    rr = await cli.read_holding_registers(0, count=READ_COUNT, device_id=device_id)
    if rr.isError(): raise ModbusException(str(rr))
    return rr.registers

# ================== TASK MỖI THIẾT BỊ ==================
async def poll_device(dev: Dev, pool: HostPool, put_async):
    # Lịch chống trôi + phase nhỏ để tránh dồn xung
    phase = min(0.02, dev.interval/10.0) * ((dev.device_id % 10)/10.0)
    next_run = (math.floor((time.monotonic()-phase)/dev.interval)*dev.interval + phase)
    if next_run <= time.monotonic(): next_run += dev.interval
    seq = 0

    while True:
        now = time.monotonic()
        if now < next_run:
            await asyncio.sleep(next_run - now)

        t0 = time.perf_counter()
        ok, data, err = True, None, None
        try:
            async with pool.sem:
                async with pool.session() as cli:
                    async with asyncio.timeout(dev.timeout):
                        data = await read_once(cli, dev.device_id)
        except Exception as e:
            ok, err = False, repr(e)

        seq += 1
        msg = {
            "device_id": dev.id, "unit": dev.device_id,
            "ok": ok, "data": data, "error": err,
            "seq": seq, "latency_ms": int((time.perf_counter()-t0)*1000),
            "ts": time.time(),
        }

        # put non-blocking; nếu đầy -> drop oldest
        try:
            put_async(("modbus_update", msg))
        except asyncio.QueueFull:
            try:
                _ = jq.async_q.get_nowait(); jq.async_q.task_done()
            except Exception:
                pass
            # đảm bảo put thành công
            await jq.async_q.put(("modbus_update", msg))

        next_run += dev.interval
        while time.monotonic() >= next_run:
            next_run += dev.interval

# ================== SHARD (mỗi thread = 1 event loop) ==================
def start_shard_thread(devs_subset: List[Dev], per_host_limit: int):
    """Mỗi shard có event loop riêng + HostPool riêng. LƯU Ý: tổng song song = SHARDS * per_host_limit."""
    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        pool = HostPool(HOST, PORT, per_host_limit=per_host_limit)

        # put_async cho shard thread: dùng jq.sync_q (thread-safe, không đòi hỏi loop)
        def put_async(item):
            try:
                jq.sync_q.put_nowait(item)
            except Exception:
                jq.sync_q.put(item)

        async def shard_main():
            tasks = [asyncio.create_task(poll_device(d, pool, put_async))
                     for d in devs_subset]
            await asyncio.gather(*tasks)

        loop.run_until_complete(shard_main())

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t

# ================== CHIA NHÓM ==================
def chunk(lst, n):
    m = max(1, (len(lst)+n-1)//n)
    return [lst[i:i+m] for i in range(0, len(lst), m)]

# ================== MAIN ==================
async def main():
    # A) Worker cho emit (tuỳ chọn)
    if USE_WORKER_THREADS:
        for _ in range(WORKERS):
            threading.Thread(target=worker_emit, args=(jq.sync_q,), daemon=True).start()
    else:
        asyncio.create_task(broadcaster_async_batch())

    if USE_SHARDS:
        # B) Request sharded qua nhiều thread/loop
        groups = chunk(ALL_DEVS, SHARDS)
        for g in groups:
            start_shard_thread(g, PER_HOST_LIMIT)
        # main loop chỉ chạy web server
        server = asyncio.create_task(asyncio.to_thread(
            uvicorn_run, socketio.ASGIApp(sio, other_asgi_app=app_star),
            host="0.0.0.0", port=8000, log_level="info"
        ))
        await server
    else:
        # Không sharding: tất cả device chạy trong main loop với 1 HostPool
        pool = HostPool(HOST, PORT, per_host_limit=PER_HOST_LIMIT)

        # put_async cho main loop
        def put_async(item):
            # item = (event, payload)
            try:
                jq.async_q.put_nowait(item)
            except asyncio.QueueFull:
                # drop oldest
                _ = jq.async_q.get_nowait(); jq.async_q.task_done()
                jq.async_q.put_nowait(item)

        # tạo task cho N thiết bị
        tasks = [asyncio.create_task(poll_device(d, pool, put_async)) for d in ALL_DEVS]
        # web server
        server = asyncio.create_task(asyncio.to_thread(
            uvicorn_run, socketio.ASGIApp(sio, other_asgi_app=app_star),
            host="0.0.0.0", port=8000, log_level="info"
        ))
        await asyncio.gather(*tasks, server)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
