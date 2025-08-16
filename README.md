# Flask Modbus Monitor (Starter)

## Quickstart
```bash
python -m venv .venv && . .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
# open http://127.0.0.1:5000
```
1) Add a device (ModbusTCP or RTU).
2) Open the device and add tags.
3) Create a Data Logger that includes those tags.
4) The background poller (every 5s) will read tags and the Dashboard will live-refresh.

> Addresses accept 40001-style; internally converted to zero-based.
