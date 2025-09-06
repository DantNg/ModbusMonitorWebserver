from __future__ import annotations
import threading, time, math
from typing import Dict, List
from modbus_monitor.database import db as dbsync
from modbus_monitor.services.common import LatestCache, utc_now
from modbus_monitor.services.notification_service import *
from modbus_monitor.extensions import socketio
def _cmp(v: float, op: str, th: float) -> bool:
    if math.isnan(v): return False
    return {
        ">": v > th, "<": v < th, ">=": v >= th, "<=": v <= th, "==": v == th, "!=": v != th
    }.get(op, False)

class AlarmService(threading.Thread):
    """Định kỳ quét rules theo device và evaluate trên cache."""
    def __init__(self, cache: LatestCache, period_sec: float = 0.5):
        super().__init__(name="alarm-service", daemon=True)
        self.cache = cache
        self.period = period_sec
        self._stop = threading.Event()
        self._state = {}    # trạng thái mở rộng: on_since/off_since
        self._active: Dict[int, bool] = {}
        self._since: Dict[int, float] = {}
    def start_send_email_thread(self, to_email, subject, body):
        threading.Thread(
            target=send_email,
            args=(to_email, subject, body),
            daemon=True
        ).start()

    def start_send_sms_thread(self, phone_number, message):
        threading.Thread(
            target=send_sms,
            args=(phone_number, message),
            daemon=True
        ).start()
    def run(self):
        while not self._stop.is_set():
            try:
                devs = dbsync.list_devices()
                now = time.time()
                for d in devs:
                    rules = dbsync.list_alarm_rules_for_device(d["id"]) or []
                    for r in rules:
                        if not r.get("enabled", True):
                            continue

                        tag_id = int(r["target"])
                        th = float(r.get("threshold") or 0.0)
                        op = r.get("operator") or ">"
                        on_s = int(r.get("on_stable_sec") or 0)
                        off_s = int(r.get("off_stable_sec") or 0)
                        to_email = r.get("email")
                        to_sms = r.get("sms")

                        rec = self.cache.get(tag_id)
                        if not rec:
                            continue
                        _, val = rec
                        cond = _cmp(float(val), op, th)

                        # ---- state lưu trữ cho từng rule ----
                        state = self._state.setdefault(r["id"], {
                            "active": False,
                            "on_since": None,
                            "off_since": None
                        })

                        # ---- Nếu điều kiện thỏa ----
                        if cond:
                            state["off_since"] = None
                            if not state["active"]:
                                if state["on_since"] is None:
                                    state["on_since"] = now
                                # kiểm tra đã ổn định đủ lâu chưa
                                if now - state["on_since"] >= on_s:
                                    # Bật alarm
                                    dbsync.insert_alarm_event(
                                        utc_now(),
                                        r.get("name", "Alarm"),
                                        r.get("level", "Critical"),
                                        r.get("target"),
                                        float(val),
                                        f"Alarm INCOMING (> {th})"
                                    )
                                    socketio.emit('alarm_event', {
                                        'title': f"ALARM: '{r.get('name', 'Alarm')}'",
                                        'message': (
                                            f"Alarm '{r.get('name', 'Alarm')}' triggered for device '{d.get('name', '')}'.\n"
                                            f"Threshold: {th}, Value: {val}, Operator: {op}"
                                        ),
                                        'status': "On",
                                        'level': r.get('level', 'Critical'),
                                        'device': d.get('name', ''),
                                        'tag': r.get('name', 'Unknown'),
                                        'value': val,
                                        'time': utc_now().strftime('%d/%m/%Y %H:%M:%S')
                                    })
                                    
                                    try:
                                        # self.start_send_email_thread(
                                        #     to_email=to_email,
                                        #     subject=f"Alarm Triggered: {r.get('name', 'Alarm')}",
                                        #     body=(
                                        #         f"DateTime: {utc_now().strftime('%d/%m/%Y %H:%M:%S')}\n"
                                        #         f"Tag: {r.get('name', 'Unknown')}\n"
                                        #         f"Value: {val}\n"
                                        #         f"High Level: {r.get('high_level', th)}\n"
                                        #         f"Low Level: {r.get('low_level', '')}\n"
                                        #         f"Status: {'High Alarm' if op in ('>', '>=') else 'Low Alarm'}"
                                        #     )
                                        # )
                                        # self.start_send_sms_thread(
                                        #     phone_number=to_sms,
                                        #     message=(
                                        #         f"Alarm '{r.get('name', 'Alarm')}' triggered for device {d['name']}.\n"
                                        #         f"Threshold: {th}, Value: {val}, Operator: {op}"
                                        #     )
                                        # )
                                        pass
                                    except Exception as e:
                                        print(f"Notification error: {e}")
                                    state["active"] = True
                                    
                                state["off_since"] = None  # reset off timer
                        else:
                            state["on_since"] = None
                            if state["active"]:
                                if state["off_since"] is None:
                                    state["off_since"] = now
                                # đủ Off Stable Time
                                if now - state["off_since"] >= off_s:
                                    # Ghi nhận alarm outcome
                                    dbsync.insert_alarm_event(
                                        utc_now(),
                                        r.get("name", "Alarm"),
                                        r.get("level", "Critical"),
                                        r.get("target"),
                                        float(val),
                                        f"Alarm OUTCOME (< {th})"
                                    )
                                    state["active"] = False

            except Exception as e:
                print("AlarmService error:", e)
            time.sleep(self.period)


    def stop(self):
        self._stop.set()

    