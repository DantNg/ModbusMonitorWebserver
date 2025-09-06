# app.py
import math, time, threading
from flask import Flask, render_template
from flask_socketio import SocketIO
from pymodbus.client import ModbusTcpClient

# ==== CẤU HÌNH (cùng IP, khác unit_id) ====
GATEWAY = {"host": "127.0.0.1", "port": 502, "timeout": 1}
DEVICES = [
    {"id": "dev1",  "unit": 1,  "interval": 1.0},
    {"id": "dev2",  "unit": 2,  "interval": 1.0},
    {"id": "dev3",  "unit": 3,  "interval": 1.0},
    {"id": "dev4",  "unit": 4,  "interval": 1.0},
    {"id": "dev5",  "unit": 5,  "interval": 1.0},
    {"id": "dev6",  "unit": 6,  "interval": 1.0},
    {"id": "dev7",  "unit": 7,  "interval": 1.0},
    {"id": "dev8",  "unit": 8,  "interval": 1.0},
    {"id": "dev9",  "unit": 9,  "interval": 1.0},
    {"id": "dev10", "unit": 10, "interval": 1.0},
    {"id": "dev11", "unit": 11, "interval": 1.0},
    {"id": "dev12", "unit": 12, "interval": 1.0},
    {"id": "dev13", "unit": 13, "interval": 1.0},
    {"id": "dev14", "unit": 14, "interval": 1.0},
    {"id": "dev15", "unit": 15, "interval": 1.0},
    {"id": "dev16", "unit": 16, "interval": 1.0},
    {"id": "dev17", "unit": 17, "interval": 1.0},
    {"id": "dev18", "unit": 18, "interval": 1.0},
    {"id": "dev19", "unit": 19, "interval": 1.0},
    {"id": "dev20", "unit": 20, "interval": 1.0},
    {"id": "dev21", "unit": 21, "interval": 1.0},
    {"id": "dev22", "unit": 22, "interval": 1.0},
    {"id": "dev23", "unit": 23, "interval": 1.0},
    {"id": "dev24", "unit": 24, "interval": 1.0},
    {"id": "dev25", "unit": 25, "interval": 1.0},
    {"id": "dev26", "unit": 26, "interval": 1.0},
    {"id": "dev27", "unit": 27, "interval": 1.0},
    {"id": "dev28", "unit": 28, "interval": 1.0},
    {"id": "dev29", "unit": 29, "interval": 1.0},
    {"id": "dev30", "unit": 30, "interval": 1.0},
]

# ==== FLASK + SOCKET.IO ====
app = Flask(__name__)
app.config["SECRET_KEY"] = "secret!"
socketio = SocketIO(app, async_mode="threading", cors_allowed_origins="*")

@app.route("/")
def index():
    return render_template("index.html", devices=DEVICES, gateway=GATEWAY)

# ---- Worker cho từng device (1 thread/device) ----
def reader(dev, start_epoch, timeout, barrier):
    did   = dev["id"]
    unit  = int(dev["unit"])
    itv   = float(dev.get("interval", 1.0))

    # Tạo client riêng cho device
    client = ModbusTcpClient(host=GATEWAY["host"], port=GATEWAY["port"], timeout=timeout)
    client.connect()

    try:
        # Chờ tất cả thread sẵn sàng rồi cùng nổ
        barrier.wait()

        # Chờ tới mốc xuất phát chung
        now = time.monotonic()
        if now < start_epoch:
            time.sleep(start_epoch - now)

        k = 0
        while True:
            # Lịch chống trôi theo mốc chung
            t_sched = start_epoch + k * itv
            now = time.monotonic()
            if now < t_sched:
                time.sleep(t_sched - now)

            t0 = time.perf_counter()
            ok, data, err = False, None, None
            try:
                # pymodbus 3.x: dùng "slave=" cho unit_id (Modbus TCP)
                rr = client.read_holding_registers(address=0, count=6, slave=unit)
                if rr.isError():
                    err = str(rr)
                else:
                    ok, data = True, rr.registers
            except Exception as e:
                err = str(e)

            payload = {
                "device_id": did,
                "unit": unit,
                "ok": ok,
                "data": data,
                "error": err,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "ts": time.time(),
            }
            socketio.emit("modbus_update", payload, namespace="/")

            # CATCH-UP: nhảy k tới mốc tương lai gần nhất để không trôi tích lũy
            now2 = time.monotonic()
            k = math.floor((now2 - start_epoch) / itv) + 1
    finally:
        try:
            client.close()
        except:
            pass

def start_poller():
    # Mốc xuất phát chung (giây kế tiếp) + barrier đồng bộ
    start_epoch = math.ceil(time.monotonic()) + 1
    barrier = threading.Barrier(len(DEVICES))

    for dev in DEVICES:
        threading.Thread(
            target=reader,
            args=(dev, start_epoch, GATEWAY["timeout"], barrier),
            daemon=True
        ).start()

if __name__ == "__main__":
    start_poller()
    socketio.run(app, host="0.0.0.0", port=5000, debug=True)
