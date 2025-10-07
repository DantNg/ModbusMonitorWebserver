import time, socket, json, threading
from typing import Optional
from flask import Flask, render_template_string
from flask_socketio import SocketIO
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusIOException
from multiprocessing import Process, Queue

# ==== Config ====
HOST = "127.0.0.1"   # App Modbus slave của bạn
PORT = 502
UNIT = 1
START = 0
COUNT = 1
CYCLE_MS = 300       # 200–500 ms

# ==== Flask app & Socket.IO ====
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")  # dùng eventlet

# HTML đơn giản hiển thị gói gần nhất
INDEX_HTML = """
<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8" />
  <title>Modbus Stream (Flask)</title>
  <script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
  <style>
    body { font-family: system-ui, sans-serif; margin: 2rem; }
    #status { padding:.25rem .5rem; border-radius:.25rem; display:inline-block }
    .ok { background:#d1fae5 }
    .bad { background:#fee2e2 }
    pre { background:#f5f5f5; padding:1rem; border-radius:.5rem; overflow:auto }
  </style>
</head>
<body>
  <h1>Modbus Stream (Flask)</h1>
  <div>Socket: <span id="status" class="bad">Đang kết nối…</span></div>
  <div><small>Poll {{cycle}} ms | unit {{unit}} | addr {{addr}} | count {{count}}</small></div>
  <h2>Gói gần nhất</h2>
  <pre id="out">{}</pre>
  <script>
    const out = document.getElementById('out');
    const statusEl = document.getElementById('status');
    const socket = io(); // cùng host/port Flask

    socket.on('connect', () => {
      statusEl.textContent = 'Đã kết nối';
      statusEl.classList.remove('bad'); statusEl.classList.add('ok');
    });
    socket.on('disconnect', () => {
      statusEl.textContent = 'Mất kết nối';
      statusEl.classList.remove('ok'); statusEl.classList.add('bad');
    });
    socket.on('modbus', (payload) => {
      out.textContent = JSON.stringify(payload, null, 2);
    });
  </script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(INDEX_HTML, cycle=CYCLE_MS, unit=UNIT, addr=START, count=COUNT)

modbus_queue = Queue()

# ==== Modbus poll (sync) chạy trong thread nền ====
def _set_sock_opts(cli: ModbusTcpClient):
    if cli.socket is None:
        return
    sock: socket.socket = cli.socket
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)   # tắt Nagle
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 131072)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
    except OSError:
        pass

def start_background_poll():
    # Start Modbus TCP worker process
    global modbus_queue
    p = Process(target=modbus_tcp_worker, args=(modbus_queue,), daemon=True)
    p.start()

    def socketio_forward_loop():
        while True:
            payload = modbus_queue.get()
            with app.app_context():
                socketio.emit("modbus", payload)
    t = threading.Thread(target=socketio_forward_loop, name="modbus-forward", daemon=True)
    t.start()

# ==== Main ====
if __name__ == "__main__":
    from modbus_tcp_worker import modbus_tcp_worker
    start_background_poll()
    socketio.run(app, host="0.0.0.0", port=5000)
