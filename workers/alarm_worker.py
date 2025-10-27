"""
Alarm Worker - Dedicated process for alarm evaluation and notifications
"""
import os
import sys
import time
import math
import json
import logging
import threading
from multiprocessing import Process, Queue
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime

import socketio

# Email imports with fallback
try:
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    EMAIL_AVAILABLE = True
except ImportError:
    try:
        import smtplib
        from email.MIMEText import MIMEText
        from email.MIMEMultipart import MIMEMultipart
        EMAIL_AVAILABLE = True
    except ImportError:
        print("⚠️ Email modules not available")
        EMAIL_AVAILABLE = False
        MIMEText = None
        MIMEMultipart = None

# SMS imports
try:
    import serial
    SMS_AVAILABLE = True
except ImportError:
    print("⚠️ Serial module not available - SMS functionality disabled")
    SMS_AVAILABLE = False
    serial = None

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'webapp'))

logger = logging.getLogger(__name__)
sio = socketio.Client()
def get_local_ip():
    """Get the local IP address of the current machine."""
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Doesn't have to be reachable
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

server_ip = get_local_ip()
sio.connect(f'http://{server_ip}:5000')
@dataclass
class AlarmConfig:
    """Configuration for alarm worker"""
    check_interval: float = 1.0  # How often to check alarms (seconds)
    enable_notifications: bool = True
    database_timeout: float = 5.0  # Database operation timeout

class AlarmWorker:
    """Dedicated worker process for alarm evaluation"""
    
    def __init__(self, config: AlarmConfig, data_queue: Queue, command_queue: Queue, log_queue: Queue, shared_state):
        self.config = config
        self.data_queue = data_queue
        self.command_queue = command_queue
        self.log_queue = log_queue
        self.shared_state = shared_state
        
        # Load SMTP and SMS configuration
        self._load_notification_config()
        
        # Database imports
        try:
            from modbus_monitor.database import db
            self.db = db
            self.db_available = True
            self.log("INFO", "Database module loaded successfully")
        except Exception as e:
            self.db = None
            self.db_available = False
            self.log("ERROR", f"Failed to load database module: {e}")
        
        # Alarm state tracking
        self._alarm_states: Dict[int, bool] = {}  # rule_id -> active
        self._alarm_since: Dict[int, float] = {}  # rule_id -> timestamp when state changed
        self._last_notification: Dict[int, Dict[str, float]] = {}  # rule_id -> {"incoming": ts, "outgoing": ts}
        
        # Operator mapping for faster evaluation
        self._operator_map = {
            ">": lambda v, t: v > t,
            "<": lambda v, t: v < t,
            ">=": lambda v, t: v >= t,
            "<=": lambda v, t: v <= t,
            "==": lambda v, t: v == t,
            "!=": lambda v, t: v != t
        }
        
        # Runtime state
        self.running = False
        self.seq = 0
        self.command_thread = None
        
        
    def _load_notification_config(self):
        """Load SMTP and SMS configuration from config file"""
        try:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'SMTP_config.json')
            with open(config_path, 'r') as f:
                config_data = json.load(f)
            
            # SMTP configuration
            smtp_config = config_data.get('SMTPSettings', {})
            self.smtp_config = {
                'host': smtp_config.get('Host', 'smtp.gmail.com'),
                'port': smtp_config.get('Port', 587),
                'username': smtp_config.get('Username', ''),
                'password': smtp_config.get('Password', ''),
                'default_subject': smtp_config.get('Subject', 'Alarm Notification')
            }
            
            # SMS configuration
            sms_config = config_data.get('SMSSettings', {})
            self.sms_config = {
                'com_port': sms_config.get('COMPort', 'COM2'),
                'baud_rate': sms_config.get('BaudRate', 9600),
                'data_bits': sms_config.get('DataBits', 8),
                'parity': sms_config.get('Parity', 'N'),
                'stop_bits': sms_config.get('StopBits', 1),
                'pin': sms_config.get('PIN', '0000'),
                'timeout': 10
            }
            
            self.notification_enabled = EMAIL_AVAILABLE or SMS_AVAILABLE
            self.log("INFO", f"Notification config loaded - Email: {EMAIL_AVAILABLE}, SMS: {SMS_AVAILABLE}")
            
        except Exception as e:
            self.log("ERROR", f"Failed to load notification config: {e}")
            self.notification_enabled = False
            self.smtp_config = {}
            self.sms_config = {}
        
    def log(self, level, message):
        """Send log message to main process"""
        try:
            self.log_queue.put({
                "worker_id": "alarm_worker",
                "level": level,
                "message": message,
                "timestamp": time.time()
            }, block=False)
        except:
            pass  # Queue full
    
    def _compare_value(self, value: float, operator: str, threshold: float) -> bool:
        """Compare value with threshold using operator"""
        if math.isnan(value):
            return False
        
        # Use operator map for faster evaluation
        compare_fn = self._operator_map.get(operator)
        if compare_fn:
            return compare_fn(value, threshold)
        return False
    
    def _should_send_notification(self, rule_id: int, notification_type: str, stable_time_sec: int) -> bool:
        """Check if notification should be sent based on debounce timer"""
        now = time.time()
        # Debounce time: max(5s, 2*stable_time) as requested
        debounce_time = max(5.0, 2 * stable_time_sec)
        
        if rule_id not in self._last_notification:
            self._last_notification[rule_id] = {}
        
        last_sent = self._last_notification[rule_id].get(notification_type, 0)
        return (now - last_sent) >= debounce_time
    
    def create_alarm_email_body(self, device_name: str, alarm_name: str, tag_value: float,
                                threshold: float, operator: str, alarm_level: str,
                                notification_type: str = "incoming") -> str:
        """Create a simplified, client-preferred email body.
        Removed banner and Level; keep essential fields only.
        """
        now = datetime.now()

        # Determine status label
        if notification_type == "outgoing":
            status = "CLEARED"
        else:
            status = "Alarm"

        body = (
            f"DateTime: {now.strftime('%d/%m/%Y %H:%M:%S')}\n"
            f"Alarm Name: {alarm_name}\n"
            f"Tag Value: {tag_value}\n"
            f"Threshold: {threshold}\n"
            f"Condition: {operator}\n"
            f"Status: {status}"
        )

        return body

    def create_alarm_sms_text(self, alarm_name: str, tag_value: float, threshold: float,
                              operator: str, notification_type: str = "incoming") -> str:
        """Compact SMS text aligned with simplified email content (<=160 chars)."""
        now = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        status = "CLEARED" if notification_type == "outgoing" else "Alarm"
        text = f"{status}: {alarm_name} | Val:{tag_value} Thr:{threshold} Cond:{operator} @ {now}"
        return text[:160]
    
    def _send_email_notification(self, to_email: str, subject: str, body: str) -> bool:
        """Send email notification using SMTP configuration"""
        if not EMAIL_AVAILABLE or not self.notification_enabled:
            self.log("INFO", f"📧 Email disabled - would send to {to_email}: {subject}")
            return False
            
        try:
            self.log("INFO", f"📧 Sending email to {to_email}")
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.smtp_config['username']
            msg['To'] = to_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            
            # Connect and send
            with smtplib.SMTP(self.smtp_config['host'], self.smtp_config['port']) as server:
                server.starttls()
                server.login(self.smtp_config['username'], self.smtp_config['password'])
                server.send_message(msg)
            
            self.log("INFO", f"✅ Email sent to {to_email}: {subject}")
            return True
            
        except Exception as e:
            self.log("ERROR", f"❌ Failed to send email to {to_email}: {e}")
            return False
    
    def _send_sms_notification(self, phone: str, message: str) -> bool:
        """Send SMS notification via COM port (GSM modem/AT commands)"""
        if not SMS_AVAILABLE or not self.notification_enabled:
            self.log("INFO", f"📱 SMS disabled - would send to {phone}: {message}")
            return False
            
        try:
            self.log("INFO", f"📱 Sending SMS to {phone}")
            
            # Connect to COM port for GSM modem
            with serial.Serial(
                port=self.sms_config['com_port'],
                baudrate=self.sms_config['baud_rate'],
                parity=self.sms_config['parity'],
                stopbits=self.sms_config['stop_bits'],
                bytesize=self.sms_config['data_bits'],
                timeout=self.sms_config['timeout']
            ) as ser:
                # Helpers
                def read_until(tokens=("OK","ERROR","+CMS ERROR"), timeout=10.0):
                    end = time.time() + timeout
                    buf = b""
                    while time.time() < end:
                        chunk = ser.read(ser.in_waiting or 1)
                        if chunk:
                            buf += chunk
                            txt = buf.decode(errors="ignore")
                            for t in tokens:
                                if t in txt:
                                    return txt
                        else:
                            time.sleep(0.05)
                    return buf.decode(errors="ignore")

                def at(cmd: str, wait_tokens=("OK","ERROR","+CMS ERROR"), timeout=5.0):
                    ser.write((cmd if cmd.endswith("\r") else cmd+"\r").encode("utf-8", errors="ignore"))
                    time.sleep(0.1)
                    return read_until(wait_tokens, timeout)

                def is_ascii_printable(s: str) -> bool:
                    try:
                        s.encode('ascii')
                        # avoid control characters except common newline (not used here)
                        return all(32 <= ord(c) <= 126 for c in s)
                    except UnicodeEncodeError:
                        return False

                # Wake and set text mode
                at("AT", timeout=2.0)
                resp = at("AT+CMGF=1", timeout=3.0)
                if "ERROR" in resp:
                    raise Exception(f"CMGF set failed: {resp}")

                # Decide charset based on content; strip non-printable
                msg = (message or "").replace("\r", " ").replace("\n", " ")
                msg = "".join(ch for ch in msg if 9 <= ord(ch) <= 126)

                use_ucs2 = not is_ascii_printable(msg)
                if use_ucs2:
                    # UCS2 mode
                    resp = at('AT+CSCS="UCS2"', timeout=3.0)
                    if "ERROR" in resp:
                        raise Exception(f"Failed to set UCS2 charset: {resp}")
                    # Encode phone and message as UCS2 hex (UTF-16BE hex without 0x)
                    phone_enc = ''.join(f"{ord(c):04X}" for c in phone)
                    msg_enc = ''.join(f"{ord(c):04X}" for c in msg)
                    resp = at(f'AT+CMGS="{phone_enc}"', wait_tokens=(">","ERROR","+CMS ERROR"), timeout=5.0)
                    if ">" not in resp:
                        raise Exception(f"No prompt after CMGS: {resp}")
                    ser.write(bytes.fromhex(msg_enc))
                    time.sleep(0.2)
                    ser.write(b"\x1A")
                    final = read_until(("OK","ERROR","+CMS ERROR"), timeout=20.0)
                else:
                    # GSM 7-bit basic via ASCII content
                    at('AT+CSCS="GSM"', timeout=2.0)
                    # Optional: ensure default text params
                    at('AT+CSMP=17,167,0,0', timeout=2.0)
                    resp = at(f'AT+CMGS="{phone}"', wait_tokens=(">","ERROR","+CMS ERROR"), timeout=5.0)
                    if ">" not in resp:
                        raise Exception(f"No prompt after CMGS: {resp}")
                    ser.write(msg.encode("ascii", errors="ignore"))
                    time.sleep(0.2)
                    ser.write(b"\x1A")
                    final = read_until(("OK","ERROR","+CMS ERROR"), timeout=20.0)

                if "+CMS ERROR" in final or "ERROR" in final and "OK" not in final:
                    raise Exception(final.strip())
            
            self.log("INFO", f"✅ SMS sent to {phone}")
            return True
            
        except Exception as e:
            self.log("ERROR", f"❌ Failed to send SMS to {phone}: {e}")
            return False
    
    def _send_notification(self, rule_id: int, rule_name: str, tag_name: str, 
                          value: float, threshold: float, operator: str, 
                          notification_type: str, device_name: str = "Unknown Device",
                          alarm_level: str = "Critical", rule: dict = None):
        """Send alarm notification via email/SMS using threads to avoid blocking"""
        try:
            # Get notification contacts from alarm rule
            contacts = self._get_notification_contacts(rule) if rule else []
            
            if not contacts:
                self.log("WARNING", f"No notification contacts configured for alarm: {rule_name}")
                return
            
            # Create notification message using the standard format
            subject = f"🚨 ALARM TRIGGERED: {rule_name}" if notification_type == "incoming" else f"✅ ALARM CLEARED: {rule_name}"
            body = self.create_alarm_email_body(
                device_name=device_name,
                alarm_name=rule_name,
                tag_value=value,
                threshold=threshold,
                operator=operator,
                alarm_level=alarm_level,
                notification_type=notification_type
            )
            
            # Send notifications to configured contacts using threads
            for contact in contacts:
                if contact.get("enabled", True):
                    # Send email if contact has email (non-blocking)
                    email = contact.get("email")
                    if email and email.strip():
                        email_thread = threading.Thread(
                            target=self._send_email_async,
                            args=(email.strip(), subject, body, rule_id),
                            daemon=True
                        )
                        email_thread.start()
                    
                    # Send SMS if contact has phone (non-blocking)
                    phone = contact.get("phone")
                    if phone and phone.strip():
                        sms_message = self.create_alarm_sms_text(
                            alarm_name=rule_name,
                            tag_value=value,
                            threshold=rule.get("threshold"),
                            operator=rule.get("operator"),
                            notification_type=notification_type,
                        )
                        sms_thread = threading.Thread(
                            target=self._send_sms_async,
                            args=(phone.strip(), sms_message, rule_id),
                            daemon=True
                        )
                        sms_thread.start()
            
            # Update last notification timestamp
            self._last_notification[rule_id][notification_type] = time.time()
            
        except Exception as e:
            self.log("ERROR", f"Failed to send notification for rule {rule_id}: {e}")
    
    def _send_email_async(self, email: str, subject: str, body: str, rule_id: int):
        """Async wrapper for email sending"""
        try:
            success = self._send_email_notification(email, subject, body)
            if success:
                self.log("INFO", f"📧 Email notification sent to {email}")
            else:
                self.log("WARNING", f"📧 Email notification failed to {email}")
        except Exception as e:
            self.log("ERROR", f"📧 Email thread error for {email}: {e}")
    
    def _send_sms_async(self, phone: str, message: str, rule_id: int):
        """Async wrapper for SMS sending"""
        try:
            success = self._send_sms_notification(phone, message)
            if success:
                self.log("INFO", f"📱 SMS notification sent to {phone}")
            else:
                self.log("WARNING", f"📱 SMS notification failed to {phone}")
        except Exception as e:
            self.log("ERROR", f"📱 SMS thread error for {phone}: {e}")
    
    def _get_notification_contacts(self, rule: dict) -> List[dict]:
        """Get notification contacts from alarm rule email/sms fields"""
        contacts = []
        
        try:
            # Get email from alarm rule
            email = rule.get("email")
            if email and email.strip():
                contacts.append({
                    "id": f"email_{rule.get('id')}",
                    "name": f"Alarm {rule.get('name', 'Unknown')} Email",
                    "email": email.strip(),
                    "phone": None,
                    "enabled": True
                })
                self.log("DEBUG", f"Added email contact from rule: {email.strip()}")
            
            # Get SMS from alarm rule
            sms = rule.get("sms")
            if sms and sms.strip():
                contacts.append({
                    "id": f"sms_{rule.get('id')}",
                    "name": f"Alarm {rule.get('name', 'Unknown')} SMS",
                    "email": None,
                    "phone": sms.strip(),
                    "enabled": True
                })
                self.log("DEBUG", f"Added SMS contact from rule: {sms.strip()}")
            
            if not contacts:
                self.log("WARNING", f"No email or SMS configured for alarm rule {rule.get('id')}: {rule.get('name')}")
            else:
                self.log("DEBUG", f"Found {len(contacts)} notification contacts for rule {rule.get('id')}")
            
            return contacts
            
        except Exception as e:
            self.log("ERROR", f"Failed to get notification contacts from rule {rule.get('id')}: {e}")
            return []
    
    def _load_alarm_rules(self) -> List[dict]:
        """Load enabled alarm rules from database"""
        if not self.db_available:
            return []
            
        try:
            rules = self.db.list_alarm_rules()
            enabled_rules = [rule for rule in rules if rule.get("enabled", True)]
            
            # Enrich with tag and device information
            enriched_rules = []
            for rule in enabled_rules:
                tag_id = rule.get("target")  # target field contains tag_id
                if tag_id:
                    tag_info = self.db.get_tag(tag_id)
                    if tag_info:
                        rule["tag_name"] = tag_info.get("name", f"Tag {tag_id}")
                        rule["tag_id"] = tag_id
                        
                        # Get device information
                        device_id = tag_info.get("device_id")
                        if device_id:
                            device_info = self.db.get_device(device_id)
                            if device_info:
                                rule["device_name"] = device_info.get("name", f"Device {device_id}")
                        
                        enriched_rules.append(rule)
            
            return enriched_rules
            
        except Exception as e:
            self.log("ERROR", f"Failed to load alarm rules: {e}")
            return []
    
    def _get_tag_value(self, tag_id: int) -> Optional[float]:
        """Get current tag value from tag_latest_values table"""
        if not self.db_available:
            return None
            
        try:
            result = self.db.get_latest_tag_value(tag_id)
            if result:
                # get_latest_tag_value returns tuple (value, timestamp)
                if isinstance(result, tuple) and len(result) >= 1:
                    return result[0]  # Return value part
                elif isinstance(result, dict):
                    return result.get("value")
                else:
                    return result
            return None
        except Exception as e:
            self.log("ERROR", f"Failed to get tag {tag_id} value: {e}")
            return None
    
    def _evaluate_alarm_rule(self, rule: dict, tag_value: float) -> bool:
        """Evaluate a single alarm rule with given tag value"""
        try:
            operator = rule.get("operator", ">")
            threshold_str = rule.get("threshold", "0")
            
            # Parse threshold (can be single value or range like "min,max")
            if "," in str(threshold_str):
                # Range threshold for between operations
                min_val, max_val = map(float, str(threshold_str).split(","))
                threshold = (min_val, max_val)
            else:
                threshold = float(threshold_str)
            
            if tag_value is None:
                return False
            
            # Handle range operations
            if isinstance(threshold, tuple):
                min_val, max_val = threshold
                if operator == "between":
                    return min_val <= tag_value <= max_val
                elif operator == "not_between":
                    return not (min_val <= tag_value <= max_val)
                else:
                    # Use first value for single operations
                    return self._compare_value(tag_value, operator, min_val)
            else:
                return self._compare_value(tag_value, operator, threshold)
            
        except Exception as e:
            self.log("ERROR", f"Failed to evaluate alarm rule {rule.get('id', 'unknown')}: {e}")
            return False
    def _process_alarm_rule(self, rule: dict):
        """Process a single alarm rule and handle state changes"""
        try:
            rule_id = rule["id"]
            rule_name = rule.get("name", f"Rule {rule_id}")
            tag_id = rule.get("tag_id")
            tag_name = rule.get("tag_name", f"Tag {tag_id}")
            device_name = rule.get("device_name", "Unknown Device")
            alarm_level = rule.get("level", "Critical")
            on_stable_sec = rule.get("on_stable_sec", 0)
            off_stable_sec = rule.get("off_stable_sec", 0)
            
            # Read tag value once per rule as requested
            current_value = self._get_tag_value(tag_id)
            if current_value is None:
                self.log("DEBUG", f"No value available for tag {tag_id} in rule {rule_name}")
                return
            
            # Evaluate current condition using the new function with tag value parameter
            current_condition = self._evaluate_alarm_rule(rule, current_value)
            previous_active = self._alarm_states.get(rule_id, False)
            now = time.time()
            
            # Handle state changes with stability check
            if current_condition and not previous_active:
                # Potential alarm activation
                if rule_id not in self._alarm_since:
                    self._alarm_since[rule_id] = now
                
                # Check if stable time reached
                stable_time = now - self._alarm_since[rule_id]
                if stable_time >= on_stable_sec:
                    self._alarm_states[rule_id] = True
                    
                    # Log alarm activation and save to database
                    self.log("WARNING", f"Alarm ACTIVATED: {rule_name} - {tag_name} = {current_value}")
                    
                    # Save alarm event to database
                    if self.db_available:
                        try:
                            threshold_val = rule.get("threshold", "0")
                            if "," in str(threshold_val):
                                threshold_num = float(str(threshold_val).split(",")[0])
                            else:
                                threshold_num = float(threshold_val)
                                
                            ts_now = datetime.now()
                            event_id = self.db.insert_alarm_event(
                                ts=ts_now,
                                name=rule_name,
                                level=alarm_level,
                                target=tag_id,
                                value=current_value,
                                note=f"Alarm activated: {rule.get('operator', '>')} {threshold_val}",
                                event_type="INCOMING",
                                operator=rule.get("operator", ">"),
                                threshold=threshold_num
                            )
                            data = {
                                "title": f"Alarm Triggered: {rule_name}",
                                "message": f"Alarm activated: {rule.get('operator', '>')} {threshold_val}",
                                "level": alarm_level,
                                "tag_id": tag_id,
                                "tag_name": tag_name,
                                "value": current_value,
                                "status": "INCOMING",
                                "created_at": ts_now.isoformat(),
                                "alarm_event_id": event_id,
                            }
                            sio.emit('alarm_event', data)
                        except Exception as e:
                            self.log("ERROR", f"Failed to save alarm event: {e}")
                    
                    # Send notifications if enabled
                    if self.config.enable_notifications:
                        if self._should_send_notification(rule_id, "incoming", on_stable_sec):
                            self._send_notification(
                                rule_id, rule_name, tag_name, current_value, 
                                rule.get("threshold"), rule.get("operator"),
                                "incoming", device_name, alarm_level, rule
                            )
                            
            elif not current_condition and previous_active:
                # Potential alarm deactivation
                if rule_id not in self._alarm_since:
                    self._alarm_since[rule_id] = now
                
                # Check if stable time reached
                stable_time = now - self._alarm_since[rule_id]
                if stable_time >= off_stable_sec:
                    self._alarm_states[rule_id] = False
                    
                    # Log alarm deactivation and save to database
                    self.log("INFO", f"Alarm CLEARED: {rule_name} - {tag_name} = {current_value}")
                    
                    # Save alarm event to database
                    if self.db_available:
                        try:
                            threshold_val = rule.get("threshold", "0")
                            if "," in str(threshold_val):
                                threshold_num = float(str(threshold_val).split(",")[0])
                            else:
                                threshold_num = float(threshold_val)
                                
                            ts_now = datetime.now()
                            event_id = self.db.insert_alarm_event(
                                ts=ts_now,
                                name=rule_name,
                                level=alarm_level,
                                target=tag_id,
                                value=current_value,
                                note=f"Alarm cleared: {rule.get('operator', '>')} {threshold_val}",
                                event_type="OUTGOING",
                                operator=rule.get("operator", ">"),
                                threshold=threshold_num
                            )
                            data = {
                                "title": f"Alarm Triggered: {rule_name}",
                                "message": f"Alarm cleared: {rule.get('operator', '>')} {threshold_val}",
                                "level": alarm_level,
                                "tag_id": tag_id,
                                "tag_name": tag_name,
                                "value": current_value,
                                "status": "OUTGOING",
                                "created_at": ts_now.isoformat(),
                                "alarm_event_id": event_id,
                            }
                            sio.emit('alarm_event', data)
                        except Exception as e:
                            self.log("ERROR", f"Failed to save alarm clear event: {e}")
                    
                    # Send clear notifications if enabled
                    if self.config.enable_notifications:
                        if self._should_send_notification(rule_id, "outgoing", off_stable_sec):
                            self._send_notification(
                                rule_id, rule_name, tag_name, current_value,
                                rule.get("threshold"), rule.get("operator"),
                                "outgoing", device_name, alarm_level, rule
                            )
            
            # Reset stability timer if condition changed
            if current_condition != previous_active:
                if rule_id in self._alarm_since:
                    # Keep tracking for stability, don't reset
                    pass
            else:
                # Condition stable, can reset timer for next change
                if rule_id in self._alarm_since:
                    del self._alarm_since[rule_id]
            
        except Exception as e:
            self.log("ERROR", f"Failed to process alarm rule {rule.get('id', 'unknown')}: {e}")
    
    def _alarm_loop(self):
        """Main alarm evaluation loop"""
        self.log("INFO", "Starting alarm evaluation loop")
        
        while self.running:
            try:
                # Load current alarm rules
                alarm_rules = self._load_alarm_rules()
                
                if alarm_rules:
                    self.log("DEBUG", f"Evaluating {len(alarm_rules)} alarm rules")
                    
                    # Process each enabled alarm rule
                    for rule in alarm_rules:
                        if rule.get("enabled", True):
                            self._process_alarm_rule(rule)
                
                # Update sequence number
                self.seq += 1
                
                # Update shared state with worker status - total_rules = len(alarm_rules) as requested
                if self.seq % 10 == 0:  # Update every 10 cycles to reduce overhead
                    active_alarms = sum(1 for active in self._alarm_states.values() if active)
                    self.shared_state["alarm_worker_status"] = {
                        "running": self.running,
                        "seq": self.seq,
                        "active_alarms": active_alarms,
                        "total_rules": len(alarm_rules),  # total_rules = len(alarm_rules) as requested
                        "last_update": time.time()
                    }
                
                # Sleep until next check
                time.sleep(self.config.check_interval)
                
            except Exception as e:
                self.log("ERROR", f"Error in alarm loop: {e}")
                time.sleep(1)  # Prevent rapid error loops
    
    def _handle_commands(self):
        """Handle commands from main process"""
        try:
            while not self.command_queue.empty():
                command = self.command_queue.get_nowait()
                
                if command["type"] == "stop":
                    self.log("INFO", "Received stop command")
                    self.running = False
                    
                elif command["type"] == "reload_config":
                    self.log("INFO", "Received reload config command")
                    # Reload configuration if needed
                    self._load_notification_config()
                    
                elif command["type"] == "get_status":
                    # Send status back via shared state
                    self.shared_state["alarm_worker_status"] = {
                        "running": self.running,
                        "seq": self.seq,
                        "active_alarms": len([k for k, v in self._alarm_states.items() if v]),
                        "total_rules": len(self._alarm_states),
                        "last_update": time.time()
                    }
                    
        except Exception as e:
            if "Empty" not in str(e):  # Ignore empty queue errors
                self.log("ERROR", f"Error handling commands: {e}")
    
    def run(self):
        """Main worker process entry point"""
        self.log("INFO", "Alarm worker starting")
        self.running = True
        
        # Start command handler thread after running=True as requested
        def command_handler():
            while self.running:
                self._handle_commands()
                time.sleep(0.1)
        
        self.command_thread = threading.Thread(target=command_handler, daemon=True)
        self.command_thread.start()
        self.log("INFO", "Command handler thread started")
        
        try:
            # Start alarm evaluation loop
            self._alarm_loop()
            
        except Exception as e:
            self.log("ERROR", f"Fatal error in alarm worker: {e}")
        finally:
            self.running = False
            self.log("INFO", "Alarm worker stopped")

def alarm_worker_main(config, data_queue, command_queue, log_queue, shared_state):
    """Main entry point for alarm worker process"""
    worker = AlarmWorker(config, data_queue, command_queue, log_queue, shared_state)
    
    # Run main worker
    worker.run()

def create_alarm_worker_process(config: AlarmConfig, data_queue: Queue, command_queue: Queue, 
                               log_queue: Queue, shared_state) -> Process:
    """Create and return alarm worker process"""
    return Process(
        target=alarm_worker_main, 
        args=(config, data_queue, command_queue, log_queue, shared_state),
        name="AlarmWorker"
    )