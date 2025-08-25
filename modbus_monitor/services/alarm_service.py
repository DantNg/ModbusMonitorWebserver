from __future__ import annotations
import threading, time, math
from typing import Dict, List
from modbus_monitor.database import db as dbsync
from modbus_monitor.services.common import LatestCache, utc_now
from modbus_monitor.services.notification_service import *
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
        # state lưu active để phát hiện chuyển trạng thái
        self._active: Dict[int, bool] = {}
        self._since: Dict[int, float] = {}

    def run(self):
        while not self._stop.is_set():
            try:
                devs = dbsync.list_devices()
                now = time.time()
                for d in devs:
                    rules = dbsync.list_alarm_rules_for_device(d["id"]) or []
                    for r in rules:
                        if not r.get("enabled", True): continue
                        tag_id = int(r["target"])
                        th = float(r.get("threshold") or 0.0)
                        op = r.get("operator") or ">"
                        on_s  = int(r.get("on_stable_sec") or 0)
                        off_s = int(r.get("off_stable_sec") or 0)
                        to_email = r.get("email")
                        to_sms = r.get("sms")
                        rec = self.cache.get(tag_id)
                        if not rec: 
                            continue
                        _, val = rec
                        cond = _cmp(float(val), op, th)

                        cur_active = cond
                        prev_active = self._active.get(r["id"], False)
                        self._active[r["id"]] = cur_active

                        if cur_active and not prev_active:
                            # Only insert when alarm transitions from inactive to active
                            dbsync.insert_alarm_event(
                                utc_now(),
                                r.get("name", "Alarm"),
                                r.get("level", "Critical"),
                                r.get("target"),
                                float(val),
                                f"Rule {r['id']} triggered"
                            )
                            try:
                                send_email(
                                    to_email=to_email,
                                    subject=f"Alarm Triggered: {r.get('name', 'Alarm')}",
                                    body=(
                                        f"DateTime: {utc_now().strftime('%d/%m/%Y %H:%M:%S')}\n"
                                        f"Tag: {r.get('name', 'Unknown')}\n"
                                        f"Value: {val}\n"
                                        f"High Level: {r.get('high_level', th)}\n"
                                        f"Low Level: {r.get('low_level', '')}\n"
                                        f"Status: {'High Alarm' if op in ('>', '>=') else 'Low Alarm'}"
                                    )
                                )

                                # Send SMS notification
                                send_sms(
                                    phone_number=to_sms,
                                    message=f"Alarm '{r.get('name', 'Alarm')}' triggered for device {d['name']}.\n"
                                            f"Threshold: {th}, Value: {val}, Operator: {op}"
                                )
                            except Exception as e:
                                print(f"Notification error: {e}")
            except Exception as e:
                print("AlarmService error:", e)
            time.sleep(self.period)

    def stop(self):
        self._stop.set()
