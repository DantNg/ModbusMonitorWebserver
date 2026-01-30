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
import queue as pyqueue
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
try:
    sio.connect(f'http://{server_ip}:5000')
    print(f"✅ Socket.IO connected to http://{server_ip}:5000")
except Exception as e:
    print(f"❌ Failed to connect Socket.IO: {e}")
    
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
        
        # Database imports (robust across source and PyInstaller EXE)
        try:
            # Prefer explicit package path available at build-time
            try:
                from webapp.modbus_monitor.database import db as _db
                self.db = _db
            except Exception:
                # Fallback when running from within webapp/ working dir
                from modbus_monitor.database import db as _db
                self.db = _db
            self.db_available = True
            self.log("INFO", "Database module loaded successfully")
        except Exception as e:
            # Final attempt: if frozen, try adding exe_dir/webapp to sys.path then retry
            try:
                exe_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else None
                if exe_dir:
                    webapp_dir = os.path.join(exe_dir, 'webapp')
                    if os.path.isdir(webapp_dir) and webapp_dir not in sys.path:
                        sys.path.insert(0, webapp_dir)
                        from modbus_monitor.database import db as _db
                        self.db = _db
                        self.db_available = True
                        self.log("INFO", "Database module loaded successfully after sys.path adjustment")
                        e = None  # clear error
            except Exception:
                pass
            if e is not None:
                self.db = None
                self.db_available = False
                self.log("ERROR", f"Failed to load database module: {e}")
        
        # Alarm state tracking
        self._alarm_states: Dict[int, bool] = {}  # rule_id -> active
        self._alarm_since: Dict[int, float] = {}  # rule_id -> timestamp when state changed
        self._last_notification: Dict[int, Dict[str, float]] = {}  # rule_id -> {"incoming": ts, "outgoing": ts}
        self._alarm_types: Dict[str, str] = {}  # alarm_key -> "High" or "Low" (for quad alarms)
        
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

        # SMS sending pipeline: single-threaded sender to serialize COM access
        self.sms_queue = pyqueue.Queue(maxsize=500) if SMS_AVAILABLE else None
        self.sms_sender_thread = None
        self.sms_sender_stop = threading.Event()
        
        
    def _load_notification_config(self):
        """Load SMTP and SMS configuration from config file"""
        try:
            # Resolve config near executable first (frozen), then CWD, then project root, then _MEIPASS
            env_path = os.environ.get('SMTP_CONFIG_PATH')
            meipass = getattr(sys, '_MEIPASS', None)
            try:
                exe_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else None
            except Exception:
                exe_dir = None
            candidates = []
            if env_path:
                candidates.append(env_path)
            if exe_dir:
                candidates.append(os.path.join(exe_dir, 'config', 'SMTP_config.json'))
                candidates.append(os.path.join(exe_dir, 'SMTP_config.json'))
            cwd = os.getcwd()
            candidates.append(os.path.join(cwd, 'config', 'SMTP_config.json'))
            candidates.append(os.path.join(cwd, 'SMTP_config.json'))
            proj_root = os.path.dirname(os.path.dirname(__file__))
            candidates.append(os.path.join(proj_root, 'config', 'SMTP_config.json'))
            if meipass:
                candidates.append(os.path.join(meipass, 'config', 'SMTP_config.json'))
            config_path = None
            for p in candidates:
                if os.path.exists(p):
                    config_path = p
                    break
            if not config_path:
                raise FileNotFoundError('SMTP_config.json not found near exe, cwd, project root, or _MEIPASS')
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
        """SMS content mirroring the simplified email body (multiline)."""
        now = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        status = "CLEARED" if notification_type == "outgoing" else "Alarm"
        lines = [
            f"DateTime: {now}",
            f"Alarm Name: {alarm_name}",
            f"Tag Value: {tag_value}",
            f"Threshold: {threshold}",
            f"Condition: {operator}",
            f"Status: {status}",
        ]
        # Keep newlines to display as multiline SMS; let sender decide charset
        text = "\n".join(lines)
        return text
    
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

                def is_gsm7_compatible(s: str) -> bool:
                    # GSM 03.38 safe set approximation (allow newline/tab as well)
                    # Note: '_' is problematic on some modems (renders as '§'), treat as non-GSM to preserve exact char
                    bad = set('|^{}\\[]~€_')
                    try:
                        s.encode('ascii')
                    except UnicodeEncodeError:
                        return False
                    for ch in s:
                        if ch in ('\n', '\r', '\t'):
                            continue
                        if ch in bad:
                            return False
                        if not (32 <= ord(ch) <= 126):
                            return False
                    return True

                # Wake and set text mode
                at("AT", timeout=2.0)
                resp = at("AT+CMGF=1", timeout=3.0)
                if "ERROR" in resp:
                    raise Exception(f"CMGF set failed: {resp}")

                # Normalize original message; keep full Unicode for UCS2 path
                raw_msg = (message or "").replace("\r\n", "\n").replace("\r", "\n")
                use_ucs2 = not is_gsm7_compatible(raw_msg)
                self.log("DEBUG", f"SMS charset selected: {'UCS2' if use_ucs2 else 'GSM'}; length={len(raw_msg)}")
                if use_ucs2:
                    # Log first offending character for diagnostics
                    for ch in raw_msg:
                        if ch not in ('\n','\r','\t') and (ord(ch) < 32 or ord(ch) > 126 or ch in set('|^{}\\[]~€_')):
                            self.log("DEBUG", f"UCS2 reason: char='{ch}' code={ord(ch)}")
                            break

                # Helper: chunk text with preference for newline boundaries
                def chunk_text(text: str, max_len: int):
                    if len(text) <= max_len:
                        return [text]
                    parts = []
                    current = ""
                    for i, line in enumerate(text.split("\n")):
                        sep = "\n" if i < len(text.split("\n")) - 1 else ""
                        piece = line + sep
                        if len(current) + len(piece) <= max_len:
                            current += piece
                        else:
                            if current:
                                parts.append(current)
                            # Hard split if a single line is too long
                            while len(piece) > max_len:
                                parts.append(piece[:max_len])
                                piece = piece[max_len:]
                            current = piece
                    if current:
                        parts.append(current)
                    return parts
                if use_ucs2:
                    # UCS2 mode
                    resp = at('AT+CSCS="UCS2"', timeout=3.0)
                    if "ERROR" in resp:
                        raise Exception(f"Failed to set UCS2 charset: {resp}")
                    # Some modems require DCS=8 for UCS2
                    at('AT+CSMP=17,167,0,8', timeout=2.0)
                    # Use ASCII number first; many modems expect ASCII phone even in UCS2 text mode
                    phone_ascii = phone
                    # CRLF line breaks
                    msg_crlf = raw_msg.replace("\n", "\r\n")
                    # Segment for UCS2 limits: 70 chars single, ~67 when concatenated
                    ucs2_parts = chunk_text(msg_crlf, 67 if len(msg_crlf) > 70 else 70)
                    final = "OK"
                    for idx, part in enumerate(ucs2_parts, 1):
                        msg_enc = ''.join(f"{ord(c):04X}" for c in part)
                        resp = at(f'AT+CMGS="{phone_ascii}"', wait_tokens=(">","ERROR","+CMS ERROR"), timeout=7.0)
                        if ">" not in resp:
                            # Try with UCS2-encoded phone number
                            phone_hex = ''.join(f"{ord(c):04X}" for c in phone)
                            resp = at(f'AT+CMGS="{phone_hex}"', wait_tokens=(">","ERROR","+CMS ERROR"), timeout=7.0)
                            if ">" not in resp:
                                raise Exception(f"No prompt after CMGS (UCS2): {resp}")
                        # Send ASCII HEX pairs for UCS2 message
                        ser.write(msg_enc.encode("ascii", errors="ignore"))
                        time.sleep(0.2)
                        ser.write(b"\x1A")
                        final = read_until(("OK","ERROR","+CMS ERROR"), timeout=25.0)
                        if "+CMS ERROR" in final or ("ERROR" in final and "OK" not in final):
                            self.log("WARNING", f"UCS2 part {idx}/{len(ucs2_parts)} failed; trying alternate CSMP")
                            at('AT+CSMP=49,167,0,8', timeout=2.0)
                            resp = at(f'AT+CMGS="{phone_ascii}"', wait_tokens=(">","ERROR","+CMS ERROR"), timeout=7.0)
                            if ">" not in resp:
                                phone_hex = ''.join(f"{ord(c):04X}" for c in phone)
                                resp = at(f'AT+CMGS="{phone_hex}"', wait_tokens=(">","ERROR","+CMS ERROR"), timeout=7.0)
                                if ">" not in resp:
                                    raise Exception(f"No prompt after CMGS (UCS2 alt CSMP): {resp}")
                            ser.write(msg_enc.encode("ascii", errors="ignore"))
                            time.sleep(0.2)
                            ser.write(b"\x1A")
                            final = read_until(("OK","ERROR","+CMS ERROR"), timeout=25.0)
                            if "+CMS ERROR" in final or ("ERROR" in final and "OK" not in final):
                                raise Exception(final.strip())
                else:
                    # GSM 7-bit basic via ASCII content
                    at('AT+CSCS="GSM"', timeout=2.0)
                    # Optional: ensure default text params
                    at('AT+CSMP=17,167,0,0', timeout=2.0)
                    # Convert LF to CRLF for some modems to render line breaks
                    gsm_msg = ''.join(ch for ch in raw_msg if ch == "\n" or ch == "\t" or 32 <= ord(ch) <= 126)
                    payload = gsm_msg.replace("\n", "\r\n")
                    # Split long GSM texts to avoid errors
                    gsm_parts = chunk_text(payload, 153 if len(payload) > 160 else 160)
                    final = "OK"
                    for part in gsm_parts:
                        resp = at(f'AT+CMGS="{phone}"', wait_tokens=(">","ERROR","+CMS ERROR"), timeout=5.0)
                        if ">" not in resp:
                            raise Exception(f"No prompt after CMGS: {resp}")
                        ser.write(part.encode("ascii", errors="ignore"))
                        time.sleep(0.2)
                        ser.write(b"\x1A")
                        final = read_until(("OK","ERROR","+CMS ERROR"), timeout=20.0)
                
                if "+CMS ERROR" in final or "ERROR" in final and "OK" not in final:
                    # If GSM path yielded +CMS ERROR: 305, retry once with UCS2
                    if (not use_ucs2) and ("+CMS ERROR: 305" in final or " 305" in final):
                        self.log("WARNING", "GSM text mode returned +CMS ERROR 305; retrying once with UCS2")
                        # Switch to UCS2 and resend
                        resp = at('AT+CSCS="UCS2"', timeout=3.0)
                        if "ERROR" in resp:
                            raise Exception(f"Failed to set UCS2 charset on retry: {resp}")
                        at('AT+CSMP=17,167,0,8', timeout=2.0)
                        # Retry in UCS2 with segmentation
                        msg_crlf = raw_msg.replace("\n", "\r\n")
                        parts = chunk_text(msg_crlf, 67 if len(msg_crlf) > 70 else 70)
                        phone_ascii = phone
                        for idx, part in enumerate(parts, 1):
                            msg_enc = ''.join(f"{ord(c):04X}" for c in part)
                            resp = at(f'AT+CMGS="{phone_ascii}"', wait_tokens=(">","ERROR","+CMS ERROR"), timeout=7.0)
                            if ">" not in resp:
                                phone_hex = ''.join(f"{ord(c):04X}" for c in phone)
                                resp = at(f'AT+CMGS="{phone_hex}"', wait_tokens=(">","ERROR","+CMS ERROR"), timeout=7.0)
                                if ">" not in resp:
                                    raise Exception(f"No prompt after CMGS (retry UCS2): {resp}")
                            ser.write(msg_enc.encode("ascii", errors="ignore"))
                            time.sleep(0.2)
                            ser.write(b"\x1A")
                            final = read_until(("OK","ERROR","+CMS ERROR"), timeout=25.0)
                            if "+CMS ERROR" in final or ("ERROR" in final and "OK" not in final):
                                self.log("WARNING", f"UCS2 retry part {idx}/{len(parts)} failed; trying alternate CSMP")
                                at('AT+CSMP=49,167,0,8', timeout=2.0)
                                resp = at(f'AT+CMGS="{phone_ascii}"', wait_tokens=(">","ERROR","+CMS ERROR"), timeout=7.0)
                                if ">" not in resp:
                                    phone_hex = ''.join(f"{ord(c):04X}" for c in phone)
                                    resp = at(f'AT+CMGS="{phone_hex}"', wait_tokens=(">","ERROR","+CMS ERROR"), timeout=7.0)
                                    if ">" not in resp:
                                        raise Exception(f"No prompt after CMGS (retry UCS2 alt CSMP): {resp}")
                                ser.write(msg_enc.encode("ascii", errors="ignore"))
                                time.sleep(0.2)
                                ser.write(b"\x1A")
                                final = read_until(("OK","ERROR","+CMS ERROR"), timeout=25.0)
                                if "+CMS ERROR" in final or ("ERROR" in final and "OK" not in final):
                                    raise Exception(final.strip())
                    else:
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
                    
                    # Send SMS if contact has phone (queued, serialized)
                    phone = contact.get("phone")
                    if phone and phone.strip():
                        sms_message = self.create_alarm_sms_text(
                            alarm_name=rule_name,
                            tag_value=value,
                            threshold=rule.get("threshold"),
                            operator=rule.get("operator"),
                            notification_type=notification_type,
                        )
                        # Enqueue instead of spawning threads to avoid COM contention
                        try:
                            if self.sms_queue is not None:
                                self.sms_queue.put_nowait((phone.strip(), sms_message, rule_id))
                                self.log("DEBUG", f"📥 Queued SMS to {phone.strip()}")
                            else:
                                # Fallback to direct send if queue unavailable
                                self._send_sms_async(phone.strip(), sms_message, rule_id)
                        except Exception as qe:
                            self.log("ERROR", f"Failed to queue SMS to {phone.strip()}: {qe}")
            
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
        """Queue-based wrapper for SMS sending to serialize COM access"""
        try:
            if self.sms_queue is not None:
                self.sms_queue.put_nowait((phone, message, rule_id))
                self.log("DEBUG", f"📥 Queued SMS to {phone}")
            else:
                # Fallback to direct send if queue unavailable
                success = self._send_sms_notification(phone, message)
                if success:
                    self.log("INFO", f"📱 SMS notification sent to {phone}")
                else:
                    self.log("WARNING", f"📱 SMS notification failed to {phone}")
        except Exception as e:
            self.log("ERROR", f"📱 SMS queue error for {phone}: {e}")
    
    def _get_notification_contacts(self, rule: dict) -> List[dict]:
        """Get notification contacts from alarm rule email/sms fields"""
        contacts = []

        try:
            def split_list(raw: str) -> List[str]:
                if not raw:
                    return []
                # Accept comma, semicolon, whitespace, newline as separators
                seps = [',', ';', '\n', '\r', '\t', ' ']
                s = raw
                for sp in seps:
                    s = s.replace(sp, ',')
                items = [x.strip() for x in s.split(',') if x and x.strip()]
                # de-duplicate preserving order
                seen = set()
                result = []
                for x in items:
                    if x not in seen:
                        seen.add(x)
                        result.append(x)
                return result

            # Emails (list)
            emails = split_list((rule.get("email") or ""))
            for idx, em in enumerate(emails, 1):
                contacts.append({
                    "id": f"email_{rule.get('id')}_{idx}",
                    "name": f"Alarm {rule.get('name', 'Unknown')} Email #{idx}",
                    "email": em,
                    "phone": None,
                    "enabled": True,
                })
            if emails:
                self.log("DEBUG", f"Added {len(emails)} email contact(s) from rule {rule.get('id')}")

            # Phones (list)
            phones = split_list((rule.get("sms") or ""))
            for idx, ph in enumerate(phones, 1):
                contacts.append({
                    "id": f"sms_{rule.get('id')}_{idx}",
                    "name": f"Alarm {rule.get('name', 'Unknown')} SMS #{idx}",
                    "email": None,
                    "phone": ph,
                    "enabled": True,
                })
            if phones:
                self.log("DEBUG", f"Added {len(phones)} SMS contact(s) from rule {rule.get('id')}")

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
    
    def _evaluate_quad_conditions(self):
        """Evaluate all active quad tag conditions"""
        if not self.db_available:
            return
        
        try:
            # Get all active quad conditions
            quad_conditions = self.db.get_all_active_quad_conditions()
            
            if not quad_conditions:
                return
            
            self.log("DEBUG", f"Evaluating {len(quad_conditions)} quad tag conditions")
            
            for condition in quad_conditions:
                try:
                    self._process_quad_condition(condition)
                except Exception as e:
                    self.log("ERROR", f"Failed to process quad condition {condition.get('quad_card_id')}: {e}")
                    
        except Exception as e:
            self.log("ERROR", f"Failed to load quad conditions: {e}")
    
    def _process_quad_condition(self, condition: dict):
        """Process a single quad tag condition"""
        quad_id = condition.get("quad_card_id")
        
        # Get quad card info to get tag IDs
        try:
            quad_card = self.db.get_quad_card_by_id(quad_id)
            if not quad_card:
                return
            
            # Get tag values for the quad card (4 tags)
            tag1_value = self._get_tag_value(quad_card.get("tag1_id"))
            tag2_value = self._get_tag_value(quad_card.get("tag2_id"))
            tag3_value = self._get_tag_value(quad_card.get("tag3_id"))
            tag4_value = self._get_tag_value(quad_card.get("tag4_id"))
            
            # Evaluate left column (tag1 top and tag3 bottom)
            self._evaluate_quad_column(
                condition, "left", 
                tag1_value, tag3_value,
                quad_card.get("tag1_id"), quad_card.get("tag3_id"),
                quad_id
            )
            
            # Evaluate right column (tag2 top and tag4 bottom)
            self._evaluate_quad_column(
                condition, "right",
                tag2_value, tag4_value,
                quad_card.get("tag2_id"), quad_card.get("tag4_id"),
                quad_id
            )
            
        except Exception as e:
            self.log("ERROR", f"Failed to process quad condition for quad {quad_id}: {e}")
    
    def _evaluate_quad_column(self, condition: dict, column: str, 
                             tag1_value: float, tag2_value: float,
                             tag1_id: int, tag2_id: int, quad_id: int):
        """Evaluate conditions for one column (left or right) of quad card"""
        
        # Build alarm key for tracking state
        alarm_key = f"quad_{quad_id}_{column}"
        
        # Get condition parameters
        high_op = condition.get(f"{column}_high_operator")
        high_compare_type = condition.get(f"{column}_high_compare_type")
        high_value = condition.get(f"{column}_high_value")
        high_compare_tag_id = condition.get(f"{column}_high_compare_tag_id")
        high_on_stable = condition.get(f"{column}_high_on_stable", 10)
        high_off_stable = condition.get(f"{column}_high_off_stable", 30)
        
        low_op = condition.get(f"{column}_low_operator")
        low_compare_type = condition.get(f"{column}_low_compare_type")
        low_value = condition.get(f"{column}_low_value")
        low_compare_tag_id = condition.get(f"{column}_low_compare_tag_id")
        low_on_stable = condition.get(f"{column}_low_on_stable", 10)
        low_off_stable = condition.get(f"{column}_low_off_stable", 30)
        
        email = condition.get(f"{column}_email", "")
        sms = condition.get(f"{column}_sms", "")
        description = condition.get(f"{column}_description", "")
        
        # Check HIGH threshold
        high_condition_met = False
        if high_op and (high_value is not None or high_compare_tag_id):
            # Determine threshold value
            if high_compare_type == "tag" and high_compare_tag_id:
                high_threshold = self._get_tag_value(high_compare_tag_id)
            else:
                high_threshold = high_value
            
            if high_threshold is not None:
                # Compare both tags against threshold
                if tag1_value is not None and tag2_value is not None:
                    high_condition_met = (
                        self._compare_value(tag1_value, high_op, high_threshold) ## only compare PV tag
                    )
        
        # Check LOW threshold
        low_condition_met = False
        if low_op and (low_value is not None or low_compare_tag_id):
            # Determine threshold value
            if low_compare_type == "tag" and low_compare_tag_id:
                low_threshold = self._get_tag_value(low_compare_tag_id)
            else:
                low_threshold = low_value
            
            if low_threshold is not None:
                # Compare both tags against threshold
                if tag1_value is not None and tag2_value is not None:
                    low_condition_met = (
                        self._compare_value(tag1_value, low_op, low_threshold) ## only compare PV tag
                    )
        
        # Determine overall alarm state
        alarm_triggered = high_condition_met or low_condition_met
        
        # Determine which alarm type is currently triggering
        current_alarm_type = "High" if high_condition_met else ("Low" if low_condition_met else None)
        
        # Process alarm state with stability
        self._process_quad_alarm_state(
            alarm_key, alarm_triggered, quad_id, column,
            tag1_value, tag2_value, tag1_id, tag2_id,
            high_condition_met, low_condition_met,
            high_on_stable if high_condition_met else low_on_stable,
            high_off_stable if high_condition_met else low_off_stable,
            email, sms, description,
            high_value if high_condition_met else low_value,
            high_op if high_condition_met else low_op,
            current_alarm_type
        )
    
    def _process_quad_alarm_state(self, alarm_key: str, current_condition: bool,
                                   quad_id: int, column: str,
                                   tag1_value: float, tag2_value: float,
                                   tag1_id: int, tag2_id: int,
                                   high_met: bool, low_met: bool,
                                   on_stable_sec: int, off_stable_sec: int,
                                   email: str, sms: str, description: str,
                                   threshold: float, operator: str,
                                   current_alarm_type: str):
        """Process quad alarm state with stability timers"""
        
        now = time.time()
        previous_active = self._alarm_states.get(alarm_key, False)
        stored_alarm_type = self._alarm_types.get(alarm_key)
        
        # Debug logging with state tracking
        timer_exists = alarm_key in self._alarm_since
        timer_value = now - self._alarm_since[alarm_key] if timer_exists else 0
        self.log("DEBUG", f"Quad {quad_id} {column}: condition={current_condition}, prev_active={previous_active}, timer={timer_value:.1f}s, current_type={current_alarm_type}, stored_type={stored_alarm_type}, tag1={tag1_value}, tag2={tag2_value}, threshold={threshold}, op={operator}")
        
        # Check if alarm type changed (LOW->HIGH or HIGH->LOW transition)
        alarm_type_changed = (previous_active and current_condition and 
                             stored_alarm_type and current_alarm_type and 
                             stored_alarm_type != current_alarm_type)
        
        if alarm_type_changed:
            # Alarm type changed - treat as clear old + trigger new
            self.log("INFO", f"🔄 Quad Alarm TYPE CHANGED: Quad {quad_id} {column} - {stored_alarm_type} → {current_alarm_type}")
            
            # Clear old alarm immediately (no stable time needed for type change)
            self._alarm_states[alarm_key] = False
            
            # Emit OUTGOING for old alarm type
            try:
                event_data = {
                    "quad_id": quad_id,
                    "column": column,
                    "alarm_type": stored_alarm_type,
                    "status": "OUTGOING",
                    "tag1_value": tag1_value,
                    "tag2_value": tag2_value,
                    "threshold": threshold,
                    "operator": operator,
                    "timestamp": datetime.now().isoformat()
                }
                self.data_queue.put({"type": "quad_alarm_event", "data": event_data})
                try:
                    sio.emit('quad_alarm_event', event_data)
                except Exception:
                    pass
            except Exception as e:
                self.log("ERROR", f"Failed to emit alarm type change OUTGOING: {e}")
            
            # Reset timer and start fresh for new alarm type
            self._alarm_since[alarm_key] = now
            self._alarm_types[alarm_key] = current_alarm_type
            self.log("DEBUG", f"Quad {quad_id} {column}: Reset timer for new alarm type {current_alarm_type}")
            return
        
        if current_condition and not previous_active:
            # Potential alarm activation
            if alarm_key not in self._alarm_since:
                self._alarm_since[alarm_key] = now
                self.log("DEBUG", f"Quad {quad_id} {column}: Started stability timer for activation")
            
            # Check if stable time reached
            stable_time = now - self._alarm_since[alarm_key]
            self.log("DEBUG", f"Quad {quad_id} {column}: Stable time = {stable_time:.1f}s / {on_stable_sec}s required")
            
            if stable_time >= on_stable_sec:
                self._alarm_states[alarm_key] = True
                
                # Determine which tag triggered by checking which one violates the threshold
                triggered_tag_id = tag1_id
                triggered_value = tag1_value
                triggered_tag_name = "Tag 1"
                
                # Check if tag1 violates threshold
                tag1_violates = tag1_value is not None and self._compare_value(tag1_value, operator, threshold)
                # Check if tag2 violates threshold
                tag2_violates = tag2_value is not None and self._compare_value(tag2_value, operator, threshold)
                
                # Determine which tag to report (prefer the one with more severe violation)
                if tag2_violates:
                    if not tag1_violates or abs(tag2_value - threshold) > abs(tag1_value - threshold):
                        triggered_tag_id = tag2_id
                        triggered_value = tag2_value
                        triggered_tag_name = "Tag 2"
                
                # Log alarm activation
                alarm_name = f"Quad {quad_id} - {column.capitalize()} Column"
                if high_met:
                    alarm_type = "High"
                    self.log("INFO", f"⚠️ Quad Alarm TRIGGERED: {alarm_name} - HIGH threshold - {triggered_tag_name} Value: {triggered_value} (threshold: {threshold})")
                else:
                    alarm_type = "Low"
                    self.log("INFO", f"⚠️ Quad Alarm TRIGGERED: {alarm_name} - LOW threshold - {triggered_tag_name} Value: {triggered_value} (threshold: {threshold})")
                
                # Store alarm type for OUTGOING event
                self._alarm_types[alarm_key] = alarm_type
                
                # Emit Socket.IO event for UI update
                try:
                    event_data = {
                        "quad_id": quad_id,
                        "column": column,
                        "alarm_type": alarm_type,
                        "status": "INCOMING",
                        "tag1_value": tag1_value,
                        "tag2_value": tag2_value,
                        "threshold": threshold,
                        "operator": operator,
                        "timestamp": datetime.now().isoformat()
                    }
                    self.log("INFO", f"📡 Emitting quad_alarm_event: {event_data}")
                    
                    # Send event via data_queue to main process for broadcasting
                    self.data_queue.put({
                        "type": "quad_alarm_event",
                        "data": event_data
                    })
                    
                    # Also try direct emit (fallback)
                    try:
                        sio.emit('quad_alarm_event', event_data)
                    except Exception as emit_err:
                        self.log("WARNING", f"Direct emit failed: {emit_err}")
                    
                    self.log("INFO", f"✅ Successfully queued quad_alarm_event")
                except Exception as e:
                    self.log("ERROR", f"Failed to emit quad alarm event: {e}")
                
                # Send notifications
                if email or sms:
                    self._send_quad_notification(
                        alarm_key, alarm_name, triggered_value, threshold,
                        operator, "incoming", email, sms, description,
                        on_stable_sec
                    )
        
        elif not current_condition and previous_active:
            # Potential alarm deactivation
            if alarm_key not in self._alarm_since:
                self._alarm_since[alarm_key] = now
                self.log("DEBUG", f"Quad {quad_id} {column}: Started stability timer for deactivation")
            
            # Check if stable time reached
            stable_time = now - self._alarm_since[alarm_key]
            self.log("DEBUG", f"Quad {quad_id} {column}: Deactivation stable time = {stable_time:.1f}s / {off_stable_sec}s required")
            
            if stable_time >= off_stable_sec:
                self._alarm_states[alarm_key] = False
                
                # Get stored alarm type
                alarm_type = self._alarm_types.get(alarm_key, "High")
                
                # Log alarm deactivation
                alarm_name = f"Quad {quad_id} - {column.capitalize()} Column"
                self.log("INFO", f"✅ Quad Alarm CLEARED: {alarm_name} ({alarm_type}) - Value: {tag1_value}")
                
                # Emit Socket.IO event for UI update
                try:
                    event_data = {
                        "quad_id": quad_id,
                        "column": column,
                        "alarm_type": alarm_type,  # Include alarm type for proper CSS class removal
                        "status": "OUTGOING",
                        "tag1_value": tag1_value,
                        "tag2_value": tag2_value,
                        "threshold": threshold,
                        "operator": operator,
                        "timestamp": datetime.now().isoformat()
                    }
                    
                    # Send event via data_queue to main process for broadcasting
                    self.data_queue.put({
                        "type": "quad_alarm_event",
                        "data": event_data
                    })
                    
                    # Also try direct emit (fallback)
                    try:
                        sio.emit('quad_alarm_event', event_data)
                    except Exception as emit_err:
                        self.log("WARNING", f"Direct emit failed: {emit_err}")
                        
                except Exception as e:
                    self.log("ERROR", f"Failed to emit quad alarm clear event: {e}")
                
                # Send clear notifications
                if email or sms:
                    self._send_quad_notification(
                        alarm_key, alarm_name, tag1_value, threshold,
                        operator, "outgoing", email, sms, description,
                        off_stable_sec
                    )
                
                # Clean up stored alarm type
                if alarm_key in self._alarm_types:
                    del self._alarm_types[alarm_key]
                
                # Reset stability timer after clearing
                if alarm_key in self._alarm_since:
                    del self._alarm_since[alarm_key]
        
        # Reset timer when condition and state match (stable state, no transition)
        elif current_condition == previous_active:
            # Both match means stable state: either both active or both inactive
            # Reset timer so it's ready for next state change
            if alarm_key in self._alarm_since:
                del self._alarm_since[alarm_key]
                self.log("DEBUG", f"Quad {quad_id} {column}: Reset timer (stable state: condition={current_condition}, active={previous_active})")
    
    def _send_quad_notification(self, alarm_key: str, alarm_name: str,
                                value: float, threshold: float, operator: str,
                                notification_type: str, email: str, sms: str,
                                description: str, stable_time_sec: int):
        """Send notification for quad alarm"""
        
        # Check debounce
        if not self._should_send_notification(hash(alarm_key), notification_type, stable_time_sec):
            return
        
        # Create message
        subject = f"🚨 QUAD ALARM: {alarm_name}" if notification_type == "incoming" else f"✅ QUAD ALARM CLEARED: {alarm_name}"
        body = self.create_alarm_email_body(
            device_name="Quad Tag Card",
            alarm_name=alarm_name,
            tag_value=value,
            threshold=threshold,
            operator=operator,
            alarm_level="Warning",
            notification_type=notification_type
        )
        
        if description:
            body += f"\n\nDescription: {description}"
        
        # Send email
        if email and email.strip():
            email_thread = threading.Thread(
                target=self._send_email_async,
                args=(email.strip(), subject, body, hash(alarm_key)),
                daemon=True
            )
            email_thread.start()
        
        # Send SMS
        if sms and sms.strip():
            sms_message = self.create_alarm_sms_text(
                alarm_name=alarm_name,
                tag_value=value,
                threshold=threshold,
                operator=operator,
                notification_type=notification_type
            )
            if description:
                sms_message += f"\nNote: {description[:50]}"  # Limit description in SMS
            
            try:
                if self.sms_queue is not None:
                    self.sms_queue.put_nowait((sms.strip(), sms_message, hash(alarm_key)))
                    self.log("DEBUG", f"📥 Queued SMS for quad alarm {alarm_key}")
            except Exception as e:
                self.log("ERROR", f"Failed to queue SMS for quad alarm: {e}")
        
        # Update last notification time
        if hash(alarm_key) not in self._last_notification:
            self._last_notification[hash(alarm_key)] = {}
        self._last_notification[hash(alarm_key)][notification_type] = time.time()
    
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
                
                # Evaluate quad tag conditions
                try:
                    self._evaluate_quad_conditions()
                except Exception as e:
                    self.log("ERROR", f"Failed to evaluate quad conditions: {e}")
                
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
        
        # Start SMS sender thread (serialized COM access)
        if SMS_AVAILABLE and self.sms_queue is not None:
            def sms_sender_loop():
                self.log("INFO", "SMS sender thread started")
                while self.running and not self.sms_sender_stop.is_set():
                    try:
                        phone, message, rule_id = self.sms_queue.get(timeout=0.5)
                    except pyqueue.Empty:
                        continue
                    try:
                        ok = self._send_sms_notification(phone, message)
                        if ok:
                            self.log("INFO", f"📱 SMS notification sent to {phone} (queued)")
                        else:
                            self.log("WARNING", f"📱 SMS notification failed to {phone} (queued)")
                    except Exception as e:
                        self.log("ERROR", f"📱 SMS sender error for {phone}: {e}")
                    finally:
                        try:
                            self.sms_queue.task_done()
                        except Exception:
                            pass
                        # Small gap between messages to let modem settle
                        time.sleep(0.25)
                self.log("INFO", "SMS sender thread stopping")

            self.sms_sender_thread = threading.Thread(target=sms_sender_loop, daemon=True)
            self.sms_sender_thread.start()
        
        try:
            # Start alarm evaluation loop
            self._alarm_loop()
            
        except Exception as e:
            self.log("ERROR", f"Fatal error in alarm worker: {e}")
        finally:
            self.running = False
            # Stop SMS sender
            try:
                if self.sms_sender_thread is not None:
                    self.sms_sender_stop.set()
                    self.sms_sender_thread.join(timeout=3)
            except Exception:
                pass
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