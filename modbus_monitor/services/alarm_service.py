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

                        tag_id = int(r.get("target", 0))
                        if tag_id == 0:
                            print(f"Warning: Invalid tag_id for alarm rule: {r}")
                            continue
                            
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
                        rule_id = r.get("id", f"rule_{tag_id}_{hash(str(r))}")
                        state = self._state.setdefault(rule_id, {
                            "active": False,
                            "on_since": None,
                            "off_since": None
                        })

                        # ---- Nếu điều kiện thỏa (alarm condition met) ----
                        if cond:
                            # Reset off timer ngay khi trở lại điều kiện alarm
                            state["off_since"] = None
                            
                            if not state["active"]:
                                # Bắt đầu đếm on stable time
                                if state["on_since"] is None:
                                    state["on_since"] = now
                                    print(f"Alarm {r.get('name', 'Unknown')} - Started ON stable timer")
                                
                                # Kiểm tra đã ổn định đủ lâu chưa
                                if now - state["on_since"] >= on_s:
                                    print(f"Alarm {r.get('name', 'Unknown')} - ON stable time reached ({on_s}s), triggering alarm")
                                    # Bật alarm - gửi INCOMING event
                                    dbsync.insert_alarm_event(
                                        utc_now(),
                                        r.get("name", "Alarm"),
                                        r.get("level", "Critical"),
                                        r.get("target"),
                                        float(val),
                                        f"Alarm INCOMING ({op} {th})",
                                        event_type="INCOMING",
                                        operator=op,
                                        threshold=th
                                    )
                                    # Emit alarm event to dashboard and relevant subdashboards
                                    alarm_event_data = {
                                        'title': f"🚨 ALARM: '{r.get('name', 'Alarm')}'",
                                        'message': (
                                            f"Device: {d.get('name', '')}\n"
                                            f"Tag: {r.get('name', 'Unknown')}\n"
                                            f"Value: {val} {op} {th}\n"
                                            f"Level: {r.get('level', 'Critical')}"
                                        ),
                                        'status': "INCOMING",
                                        'level': r.get('level', 'Critical'),
                                        'device': d.get('name', ''),
                                        'tag': r.get('name', 'Unknown'),
                                        'value': val,
                                        'threshold': th,
                                        'operator': op,
                                        'time': utc_now().strftime('%d/%m/%Y %H:%M:%S')
                                    }
                                    
                                    # Emit to main dashboard
                                    socketio.emit('alarm_event', alarm_event_data)
                                    
                                    # Emit to relevant subdashboards that contain this tag
                                    try:
                                        subdashboards = dbsync.list_subdashboards() or []
                                        for subdash in subdashboards:
                                            subdash_id = subdash.get('id')
                                            if not subdash_id:
                                                continue
                                                
                                            # Get tags in this subdashboard
                                            subdash_tag_ids = [t['id'] for t in dbsync.get_subdashboard_tags(subdash_id) or []]
                                            
                                            # If this alarm's tag is in the subdashboard, emit event
                                            if tag_id in subdash_tag_ids:
                                                socketio.emit('alarm_event', alarm_event_data, room=f"subdashboard_{subdash_id}")
                                                print(f"Emitted alarm to subdashboard_{subdash_id}")
                                    except Exception as e:
                                        print(f"Error emitting alarm to subdashboards: {e}")
                                    
                                    try:
                                        print(f"Alarm {r.get('name', 'Unknown')} triggered - sending notifications...")
                                        
                                        # Send Email notification if configured
                                        if to_email and to_email.strip():
                                            self.start_send_email_thread(
                                                to_email=to_email.strip(),
                                                subject=f"🚨 ALARM TRIGGERED: {r.get('name', 'Alarm')}",
                                                body=(
                                                    f"ALARM NOTIFICATION\n"
                                                    f"==================\n\n"
                                                    f"DateTime: {utc_now().strftime('%d/%m/%Y %H:%M:%S')}\n"
                                                    f"Device: {d.get('name', 'Unknown Device')}\n"
                                                    f"Alarm Name: {r.get('name', 'Unknown Alarm')}\n"
                                                    f"Tag Value: {val}\n"
                                                    f"Threshold: {th}\n"
                                                    f"Condition: {op}\n"
                                                    f"Level: {r.get('level', 'Critical')}\n"
                                                    f"Status: {'High Alarm' if op in ('>', '>=') else 'Low Alarm' if op in ('<', '<=') else 'Alarm'}\n\n"
                                                    f"Please check the system immediately."
                                                )
                                            )
                                        
                                        # Send SMS notification if configured
                                        if to_sms and to_sms.strip():
                                            self.start_send_sms_thread(
                                                phone_number=to_sms.strip(),
                                                message=(
                                                    f"🚨 ALARM: '{r.get('name', 'Alarm')}' triggered for device '{d.get('name', 'Unknown')}'.\n"
                                                    f"Value: {val}, Threshold: {op} {th}, Level: {r.get('level', 'Critical')}\n"
                                                    f"Time: {utc_now().strftime('%d/%m/%Y %H:%M:%S')}"
                                                )
                                            )
                                        
                                    except Exception as e:
                                        print(f"Notification error: {e}")
                                    
                                    state["active"] = True
                                    state["on_since"] = None  # Reset on timer sau khi trigger
                                else:
                                    remaining = on_s - (now - state["on_since"])
                                    print(f"Alarm {r.get('name', 'Unknown')} - ON stable: {remaining:.1f}s remaining")
                            else:
                                # Alarm đã active, chỉ cần reset off timer
                                pass
                                # print(f"Alarm {r.get('name', 'Unknown')} - Still active, condition continues")
                        
                        # ---- Nếu điều kiện không thỏa (alarm condition not met) ----
                        else:
                            # Reset on timer ngay khi thoát điều kiện alarm  
                            state["on_since"] = None
                            
                            if state["active"]:
                                # Bắt đầu đếm off stable time
                                if state["off_since"] is None:
                                    state["off_since"] = now
                                    print(f"Alarm {r.get('name', 'Unknown')} - Started OFF stable timer")
                                
                                # Kiểm tra đã ổn định đủ lâu chưa
                                if now - state["off_since"] >= off_s:
                                    print(f"Alarm {r.get('name', 'Unknown')} - OFF stable time reached ({off_s}s), clearing alarm")
                                    # Tắt alarm - gửi OUTGOING event
                                    dbsync.insert_alarm_event(
                                        utc_now(),
                                        r.get("name", "Alarm"),
                                        r.get("level", "Critical"),
                                        r.get("target"),
                                        float(val),
                                        f"Alarm OUTGOING ({op} {th})",
                                        event_type="OUTGOING", 
                                        operator=op,
                                        threshold=th
                                    )
                                    # Emit alarm clear event to dashboard and relevant subdashboards
                                    alarm_clear_data = {
                                        'title': f"✅ CLEAR: '{r.get('name', 'Alarm')}'",
                                        'message': (
                                            f"Device: {d.get('name', '')}\n"
                                            f"Tag: {r.get('name', 'Unknown')}\n"
                                            f"Value: {val} (Normal)\n" 
                                            f"Alarm cleared"
                                        ),
                                        'status': "OUTGOING",
                                        'level': "Normal",
                                        'device': d.get('name', ''),
                                        'tag': r.get('name', 'Unknown'),
                                        'value': val,
                                        'threshold': th,
                                        'operator': op,
                                        'time': utc_now().strftime('%d/%m/%Y %H:%M:%S')
                                    }
                                    
                                    # Emit to main dashboard
                                    socketio.emit('alarm_event', alarm_clear_data)
                                    
                                    # Emit to relevant subdashboards that contain this tag
                                    try:
                                        subdashboards = dbsync.list_subdashboards() or []
                                        for subdash in subdashboards:
                                            subdash_id = subdash.get('id')
                                            if not subdash_id:
                                                continue
                                                
                                            # Get tags in this subdashboard
                                            subdash_tag_ids = [t['id'] for t in dbsync.get_subdashboard_tags(subdash_id) or []]
                                            
                                            # If this alarm's tag is in the subdashboard, emit event
                                            if tag_id in subdash_tag_ids:
                                                socketio.emit('alarm_event', alarm_clear_data, room=f"subdashboard_{subdash_id}")
                                                print(f"Emitted alarm clear to subdashboard_{subdash_id}")
                                    except Exception as e:
                                        print(f"Error emitting alarm clear to subdashboards: {e}")
                                    
                                    try:
                                        print(f"Alarm {r.get('name', 'Unknown')} cleared - sending clear notifications...")
                                        
                                        # Send Email clear notification if configured
                                        if to_email and to_email.strip():
                                            self.start_send_email_thread(
                                                to_email=to_email.strip(),
                                                subject=f"✅ ALARM CLEARED: {r.get('name', 'Alarm')}",
                                                body=(
                                                    f"ALARM CLEAR NOTIFICATION\n"
                                                    f"========================\n\n"
                                                    f"DateTime: {utc_now().strftime('%d/%m/%Y %H:%M:%S')}\n"
                                                    f"Device: {d.get('name', 'Unknown Device')}\n"
                                                    f"Alarm Name: {r.get('name', 'Unknown Alarm')}\n"
                                                    f"Tag Value: {val}\n"
                                                    f"Threshold: {th}\n"
                                                    f"Condition: {op}\n"
                                                    f"Status: NORMAL\n\n"
                                                    f"The alarm condition has been resolved."
                                                )
                                            )
                                        
                                        # Send SMS clear notification if configured
                                        if to_sms and to_sms.strip():
                                            self.start_send_sms_thread(
                                                phone_number=to_sms.strip(),
                                                message=(
                                                    f"✅ CLEAR: '{r.get('name', 'Alarm')}' for device '{d.get('name', 'Unknown')}'.\n"
                                                    f"Value: {val} (Normal), Time: {utc_now().strftime('%d/%m/%Y %H:%M:%S')}"
                                                )
                                            )
                                        
                                    except Exception as e:
                                        print(f"Clear notification error: {e}")
                                    
                                    state["active"] = False
                                    state["off_since"] = None  # Reset off timer sau khi clear
                                else:
                                    remaining = off_s - (now - state["off_since"])
                                    # print(f"Alarm {r.get('name', 'Unknown')} - OFF stable: {remaining:.1f}s remaining")
                            else:
                                # Alarm chưa active, không cần làm gì
                                pass
                                # print(f"Alarm {r.get('name', 'Unknown')} - Normal condition, not active")

            except Exception as e:
                print("AlarmService error:", e)
            time.sleep(self.period)


    def stop(self):
        self._stop.set()

    