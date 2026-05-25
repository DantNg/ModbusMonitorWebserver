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

# Strategy + Factory pattern cho qtag alarm evaluation
from workers.alarm_strategies import AlarmStrategyFactory, CardAlarmStrategy, ColumnDef

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


class SocketIOManager:
    """
    Manages the Socket.IO client connection with automatic reconnection.
    Solves the issue where after long uptime (~1 day), the connection drops
    silently and alarm events are never delivered to the browser.
    
    IMPORTANT: Always connects via 127.0.0.1 (localhost) since the alarm worker
    and Flask server run on the SAME machine. Using the LAN IP (e.g., 192.168.x.x)
    causes frequent ConnectTimeoutError when the network adapter sleeps or
    firewall blocks the connection.
    """

    # Default server URL — localhost is reliable since both processes run on same machine
    DEFAULT_SERVER_URL = 'http://127.0.0.1:5000'

    def __init__(self, reconnect_interval=30):
        self._sio = None
        self._server_url = self.DEFAULT_SERVER_URL
        self._connected = False
        self._lock = threading.Lock()
        self._reconnect_interval = reconnect_interval  # seconds between health checks
        self._last_connect_attempt = 0
        self._connect_cooldown = 5  # minimum seconds between connect attempts
        self._init_client()

    def _init_client(self):
        """Create a new socketio.Client with reconnection enabled."""
        self._sio = socketio.Client(
            reconnection=True,
            reconnection_attempts=0,  # unlimited
            reconnection_delay=2,
            reconnection_delay_max=30,
            logger=False,
            engineio_logger=False
        )
        # Register event handlers for connection lifecycle
        self._sio.on('connect', self._on_connect)
        self._sio.on('disconnect', self._on_disconnect)
        self._sio.on('connect_error', self._on_connect_error)

    def _on_connect(self):
        self._connected = True
        logger.info("[SocketIOManager] Connected to server: %s", self._server_url)
        print(f"✅ [SocketIOManager] Socket.IO connected to {self._server_url}")

    def _on_disconnect(self):
        self._connected = False
        logger.warning("[SocketIOManager] Disconnected from server")
        print("⚠️ [SocketIOManager] Socket.IO disconnected")

    def _on_connect_error(self, data=None):
        self._connected = False
        logger.warning(f"[SocketIOManager] Connection error: {data}")

    def connect(self, server_url=None):
        """Connect to the Socket.IO server.
        
        Always prefers 127.0.0.1 for reliability. Falls back to provided URL
        only if explicitly specified.
        """
        if server_url:
            self._server_url = server_url
        if not self._server_url:
            self._server_url = self.DEFAULT_SERVER_URL

        try:
            now = time.time()
            if now - self._last_connect_attempt < self._connect_cooldown:
                return False
            self._last_connect_attempt = now

            if self._sio.connected:
                self._connected = True
                return True

            self._sio.connect(self._server_url, wait_timeout=3)
            self._connected = True
            print(f"✅ Socket.IO connected to {self._server_url}")
            return True
        except Exception as e:
            self._connected = False
            print(f"❌ Failed to connect Socket.IO to {self._server_url}: {e}")
            return False

    def ensure_connected(self):
        """
        Check connection health and reconnect if needed.
        Call this periodically from the alarm loop (runs in background thread).
        """
        with self._lock:
            try:
                if self._sio and self._sio.connected:
                    self._connected = True
                    return True
            except Exception:
                pass

            # Connection lost - attempt reconnect
            self._connected = False
            logger.warning("[SocketIOManager] Connection lost, attempting reconnect to %s ...", self._server_url)
            try:
                # Disconnect cleanly first to reset internal state
                try:
                    self._sio.disconnect()
                except Exception:
                    pass
                # Re-create client to avoid stale internal state
                self._init_client()
                return self.connect()
            except Exception as e:
                logger.warning(f"[SocketIOManager] Reconnect failed: {e}")
                return False

    def emit(self, event, data):
        """
        Safely emit an event. Returns True if sent successfully.
        NON-BLOCKING: Does NOT attempt reconnection here.
        Reconnection is handled by the periodic health check in _alarm_loop.
        """
        try:
            if self._sio and self._sio.connected:
                self._sio.emit(event, data)
                self._connected = True
                return True
            else:
                self._connected = False
                logger.warning(f"[SocketIOManager] Cannot emit '{event}': not connected (will reconnect on next health check)")
                return False
        except Exception as e:
            self._connected = False
            logger.warning(f"[SocketIOManager] Emit failed for '{event}': {e}")
            return False

    @property
    def connected(self):
        """Check if currently connected."""
        try:
            return self._sio and self._sio.connected
        except Exception:
            return False


# Global socket.io manager instance (initialized per-process)
# Uses 127.0.0.1 by default — alarm worker and Flask server always run on the same machine.
# The initial connect may fail if Flask server hasn't started yet; the health check 
# in _alarm_loop will retry automatically every 10 seconds.
sio_manager = SocketIOManager()
sio_manager.connect()  # Connects to http://127.0.0.1:5000

# Backward-compatible alias: code that calls sio.emit() will use the manager
sio = sio_manager
    
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
        self._offline_skipped: set = set()  # alarm_keys skipped due to device offline (for reconnect re-emit)
        
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

        # Device status cache for quad tag evaluation
        self._device_status_cache: Dict[int, dict] = {}
        self._device_status_cache_ts = 0.0
        self._tag_device_map: Dict[int, Optional[int]] = {}
        
        # Performance caches to reduce DB queries per cycle
        self._cached_alarm_rules: Optional[List[dict]] = None
        self._cached_alarm_rules_ts = 0.0
        self._cached_alarm_rules_ttl = 10.0  # Reload rules from DB every 10 seconds
        
        self._cached_quad_conditions: Optional[List[dict]] = None
        self._cached_quad_conditions_ts = 0.0
        self._cached_quad_conditions_ttl = 5.0  # Reload quad conditions every 5 seconds
        
        self._quad_card_cache: Dict[int, dict] = {}  # quad_id -> {"data": ..., "ts": ...}
        self._quad_card_cache_ttl = 30.0  # Quad card info changes rarely
        
        self._tag_value_cache: Dict[int, Optional[float]] = {}  # Per-cycle tag value cache
        # Cache scale+offset cho từng tag để chuyển raw → engineering units khi so sánh alarm
        self._tag_transform_cache: Dict[int, tuple] = {}  # tag_id -> (scale, offset)
        self._tag_transform_cache_ts: Dict[int, float] = {}
        self._tag_transform_cache_ttl = 60.0  # reload sau 60 giây
        
        # Cache for quad-managed tag IDs (to skip regular alarms for these tags)
        # DEPRECATED: dùng _managed_tag_ids thay thế (thống nhất quad + card)
        self._quad_managed_tag_ids: set = set()
        self._quad_managed_tag_ids_ts = 0.0
        self._quad_managed_tag_ids_ttl = 10.0  # kept for backward compat

        # Card alarm conditions cache (Qtag6, Single3, PV Only, PV Dual, Qtag2, Qtag3, Qtag4)
        self._cached_card_conditions: Optional[List[dict]] = None
        self._cached_card_conditions_ts = 0.0
        self._cached_card_conditions_ttl = 5.0
        # Unified card info cache – dùng chung cho cả quad và card types (key = "{card_type}_{card_id}")
        self._card_info_cache: Dict[str, dict] = {}
        self._card_info_cache_ttl = 30.0
        # Unified managed tag IDs – thay thế cả _quad_managed_tag_ids và _card_managed_tag_ids
        self._managed_tag_ids: set = set()
        self._managed_tag_ids_ts = 0.0
        self._managed_tag_ids_ttl = 10.0
        # Kept for backward compat (alias to unified set)
        self._card_managed_tag_ids: set = self._managed_tag_ids
        self._card_managed_tag_ids_ts = 0.0
        self._card_managed_tag_ids_ttl = 10.0
        
        
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

    def _refresh_device_status_cache(self) -> Dict[int, dict]:
        """Refresh device status cache with short TTL to avoid frequent DB hits."""
        if not self.db_available:
            return {}
        now = time.time()
        if not self._device_status_cache or (now - self._device_status_cache_ts) > 3.0:
            try:
                self._device_status_cache = self.db.get_all_device_statuses_from_db() or {}
            except Exception as e:
                self.log("ERROR", f"Failed to refresh device statuses: {e}")
                self._device_status_cache = {}
            self._device_status_cache_ts = now
        return self._device_status_cache

    def _get_tag_device_id(self, tag_id: int) -> Optional[int]:
        """Resolve and cache device_id for a tag."""
        if tag_id in self._tag_device_map:
            return self._tag_device_map[tag_id]
        device_id = None
        try:
            tag = self.db.get_tag(tag_id)
            if tag:
                device_id = tag.get("device_id")
        except Exception as e:
            self.log("ERROR", f"Failed to resolve device for tag {tag_id}: {e}")
        self._tag_device_map[tag_id] = device_id
        return device_id

    def _is_tag_device_online(self, tag_id: int) -> Optional[bool]:
        """Return device online status for a tag. None if unknown."""
        if tag_id is None or not self.db_available:
            return None
        device_id = self._get_tag_device_id(tag_id)
        if not device_id:
            return None
        statuses = self._refresh_device_status_cache()
        status_info = statuses.get(device_id)
        if status_info is None:
            return None
        return status_info.get("is_online")
    
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
            subject = f"ALARM TRIGGERED: {rule_name}" if notification_type == "incoming" else f"✅ ALARM CLEARED: {rule_name}"
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
        """Load enabled alarm rules from database (cached with TTL to reduce DB queries)"""
        if not self.db_available:
            return []
        
        # Return cached rules if still fresh
        now = time.time()
        if self._cached_alarm_rules is not None and (now - self._cached_alarm_rules_ts) < self._cached_alarm_rules_ttl:
            return self._cached_alarm_rules
            
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
            
            # Update cache
            self._cached_alarm_rules = enriched_rules
            self._cached_alarm_rules_ts = now
            return enriched_rules
            
        except Exception as e:
            self.log("ERROR", f"Failed to load alarm rules: {e}")
            return self._cached_alarm_rules or []
    
    def _get_tag_transform(self, tag_id: int):
        """Trả về (scale, offset) cho tag, cache 60 giây để giảm query DB."""
        now = time.time()
        if (tag_id in self._tag_transform_cache and
                now - self._tag_transform_cache_ts.get(tag_id, 0) < self._tag_transform_cache_ttl):
            return self._tag_transform_cache[tag_id]
        sf, off = 1.0, 0.0
        try:
            if self.db_available:
                tag_info = self.db.get_tag(tag_id)
                if tag_info:
                    sf = float(tag_info.get('scale', 1) or 1)
                    off = float(tag_info.get('offset', 0) or 0)
        except Exception:
            pass
        self._tag_transform_cache[tag_id] = (sf, off)
        self._tag_transform_cache_ts[tag_id] = now
        return (sf, off)

    def _get_tag_value(self, tag_id: int) -> Optional[float]:
        """Get current tag value in engineering units (raw × scale + offset) from tag_latest_values table (per-cycle cache)"""
        if not self.db_available:
            return None
        
        # Per-cycle cache: avoid querying same tag multiple times within one alarm loop cycle
        if tag_id in self._tag_value_cache:
            return self._tag_value_cache[tag_id]
            
        try:
            result = self.db.get_latest_tag_value(tag_id)
            value = None
            if result:
                # get_latest_tag_value returns tuple (value, timestamp)
                if isinstance(result, tuple) and len(result) >= 1:
                    value = result[0]  # Return value part
                elif isinstance(result, dict):
                    value = result.get("value")
                else:
                    value = result
            # Apply scale+offset để chuyển raw register → engineering units trước khi so sánh threshold
            if value is not None:
                try:
                    sf, off = self._get_tag_transform(tag_id)
                    value = float(value) * sf + off
                except Exception:
                    pass
            self._tag_value_cache[tag_id] = value
            return value
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
                    
                    # Prepare common data
                    threshold_val = rule.get("threshold", "0")
                    if "," in str(threshold_val):
                        threshold_num = float(str(threshold_val).split(",")[0])
                    else:
                        threshold_num = float(threshold_val)
                    ts_now = datetime.now()
                    
                    # ── EMIT SOCKET EVENT FIRST for instant UI update ──
                    try:
                        data = {
                            "title": f"Alarm Triggered: {rule_name}",
                            "message": f"Alarm activated: {rule.get('operator', '>')} {threshold_val}",
                            "level": alarm_level,
                            "tag_id": tag_id,
                            "tag_name": tag_name,
                            "value": current_value,
                            "status": "INCOMING",
                            "created_at": ts_now.isoformat(),
                            "alarm_event_id": None,  # DB event_id not yet available
                        }
                        sio.emit('alarm_event', data)
                    except Exception as e:
                        self.log("WARNING", f"Socket emit failed for alarm {rule_name}: {e}")
                    
                    # ── THEN save to database (slower, but UI already updated) ──
                    if self.db_available:
                        try:
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
                    
                    # Prepare common data
                    threshold_val = rule.get("threshold", "0")
                    if "," in str(threshold_val):
                        threshold_num = float(str(threshold_val).split(",")[0])
                    else:
                        threshold_num = float(threshold_val)
                    ts_now = datetime.now()
                    
                    # ── EMIT SOCKET EVENT FIRST for instant UI update ──
                    try:
                        dashboard_title = (rule.get("dashboard_name") or device_name or rule_name)
                        data = {
                            "title": dashboard_title,
                            "message": f"Alarm cleared: {rule.get('operator', '>')} {threshold_val}",
                            "level": alarm_level,
                            "tag_id": tag_id,
                            "tag_name": tag_name,
                            "value": current_value,
                            "status": "OUTGOING",
                            "created_at": ts_now.isoformat(),
                            "alarm_event_id": None,  # DB event_id not yet available
                        }
                        sio.emit('alarm_event', data)
                    except Exception as e:
                        self.log("WARNING", f"Socket emit failed for alarm clear {rule_name}: {e}")
                    
                    # ── THEN save to database (slower, but UI already updated) ──
                    if self.db_available:
                        try:
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
    
    def _get_quad_managed_tag_ids(self) -> set:
        """DEPRECATED: Dùng _get_all_managed_tag_ids() thay thế. Giữ lại để backward compat."""
        return self._get_all_managed_tag_ids()

    def _evaluate_quad_conditions(self):
        """DEPRECATED: Logic đã được hợp nhất vào _evaluate_card_conditions() với Strategy Pattern.
        
        Giữ lại để không bị lỗi nếu có code nào vẫn gọi trực tiếp.
        """
        pass  # Handled inside _evaluate_card_conditions via quad strategy

    def _process_quad_condition(self, condition: dict):
        """DEPRECATED: Dùng _process_card_condition() với strategy='quad'."""
        normalized = dict(condition)
        normalized["card_type"] = "quad"
        normalized["card_id"] = condition.get("quad_card_id")
        self._process_card_condition(normalized)

    # ========== Card + Quad Alarm Evaluation – unified via Strategy Pattern ==========

    def _get_cached_card_info(self, strategy: CardAlarmStrategy, card_id: int) -> Optional[dict]:
        """Lấy card info từ cache hoặc DB, dùng chung cho mọi card type.

        Strategy cung cấp method fetch_card_info() đúng với từng loại
        (vd: get_card_info_for_alarm vs get_quad_card_by_id).
        Cache key: "{card_type}_{card_id}" – thống nhất cả quad lẫn card.
        """
        now = time.time()
        cache_key = f"{strategy.card_type}_{card_id}"
        cached = self._card_info_cache.get(cache_key)
        if cached and (now - cached["ts"]) < self._card_info_cache_ttl:
            return cached["data"]
        card_info = strategy.fetch_card_info(self.db, card_id)
        self._card_info_cache[cache_key] = {"data": card_info, "ts": now}
        return card_info

    def _get_all_managed_tag_ids(self) -> set:
        """Lấy tập hợp tag IDs được quản lý bởi tất cả qtag card (quad + card).

        Thay thế _get_quad_managed_tag_ids() và _get_card_managed_tag_ids() cũ.
        Dùng AlarmStrategyFactory để đảm bảo tự động bao gồm loại card mới.
        """
        now = time.time()
        if (now - self._managed_tag_ids_ts) < self._managed_tag_ids_ttl:
            return self._managed_tag_ids

        tag_ids: set = set()
        if not self.db_available:
            return tag_ids

        try:
            # --- Card conditions (qtag6, qtag4, ..., pv_dual) ---
            card_conditions = self.db.get_all_active_card_conditions() or []
            for condition in card_conditions:
                card_type = condition.get("card_type")
                card_id = condition.get("card_id")
                if not card_type or card_id is None:
                    continue
                strategy = AlarmStrategyFactory.get(card_type)
                if not strategy:
                    continue
                card_info = self._get_cached_card_info(strategy, card_id)
                if card_info:
                    tag_ids.update(strategy.get_managed_tag_ids(card_info))

            # --- Quad conditions (bảng riêng) ---
            quad_strategy = AlarmStrategyFactory.get("quad")
            if quad_strategy:
                quad_conditions = self.db.get_all_active_quad_conditions() or []
                for condition in quad_conditions:
                    quad_id = condition.get("quad_card_id")
                    if quad_id is None:
                        continue
                    card_info = self._get_cached_card_info(quad_strategy, quad_id)
                    if card_info:
                        tag_ids.update(quad_strategy.get_managed_tag_ids(card_info))

        except Exception as e:
            self.log("ERROR", f"Failed to get managed tag IDs: {e}")

        self._managed_tag_ids = tag_ids
        self._managed_tag_ids_ts = now
        # Cập nhật alias để backward compat
        self._card_managed_tag_ids = tag_ids
        self._card_managed_tag_ids_ts = now
        self._quad_managed_tag_ids = tag_ids
        self._quad_managed_tag_ids_ts = now
        return tag_ids

    def _get_card_managed_tag_ids(self) -> set:
        """Get all tag IDs managed by card alarm conditions to skip in regular evaluation."""
        now = time.time()
        if (now - self._managed_tag_ids_ts) < self._managed_tag_ids_ttl:
            return self._managed_tag_ids

        tag_ids = set()
        try:
            if not self.db_available:
                return tag_ids

            conditions = self.db.get_all_active_card_conditions()
            if not conditions:
                self._card_managed_tag_ids = tag_ids
                self._card_managed_tag_ids_ts = now
                return tag_ids

            for condition in conditions:
                card_type = condition.get("card_type")
                card_id = condition.get("card_id")
                if not card_type or not card_id:
                    continue

                cache_key = f"{card_type}_{card_id}"
                cached = self._card_info_cache.get(cache_key)
                if cached and (now - cached['ts']) < self._card_info_cache_ttl:
                    card_info = cached['data']
                else:
                    card_info = self.db.get_card_info_for_alarm(card_type, card_id)
                    self._card_info_cache[cache_key] = {'data': card_info, 'ts': now}

                if not card_info:
                    continue

                # Extract PV tag IDs based on card type
                pv_tags = self._get_card_pv_tag_ids(card_type, card_info)
                tag_ids.update(pv_tags)

            self._card_managed_tag_ids = tag_ids
            self._card_managed_tag_ids_ts = now

        except Exception as e:
            self.log("ERROR", f"Failed to get card managed tag IDs: {e}")

        return self._card_managed_tag_ids

    def _evaluate_card_conditions(self):
        """Evaluate tất cả qtag card conditions (cả card lẫn quad) dùng Strategy Pattern.

        Thay thế cả _evaluate_card_conditions cũ và _evaluate_quad_conditions.
        Factory tự động routing đúng strategy cho từng loại card.
        """
        if not self.db_available:
            return

        now = time.time()

        # --- Card conditions (qtag6, qtag4, qtag3, qtag2, single3, pv_only, pv_dual) ---
        try:
            if self._cached_card_conditions is None or (now - self._cached_card_conditions_ts) > self._cached_card_conditions_ttl:
                self._cached_card_conditions = self.db.get_all_active_card_conditions()
                self._cached_card_conditions_ts = now

            for condition in (self._cached_card_conditions or []):
                try:
                    self._process_card_condition(condition)
                except Exception as e:
                    self.log("ERROR", f"Failed to process card condition {condition.get('card_type')}/{condition.get('card_id')}: {e}")
        except Exception as e:
            self.log("ERROR", f"Failed to load card conditions: {e}")

        # --- Quad conditions (bảng riêng, normalize thành cùng format) ---
        try:
            if self._cached_quad_conditions is None or (now - self._cached_quad_conditions_ts) > self._cached_quad_conditions_ttl:
                self._cached_quad_conditions = self.db.get_all_active_quad_conditions()
                self._cached_quad_conditions_ts = now

            for condition in (self._cached_quad_conditions or []):
                try:
                    # Normalize quad condition: thêm card_type="quad" và card_id=quad_card_id
                    # để dùng chung _process_card_condition với Strategy pattern
                    normalized = dict(condition)
                    normalized["card_type"] = "quad"
                    normalized["card_id"] = condition.get("quad_card_id")
                    self._process_card_condition(normalized)
                except Exception as e:
                    self.log("ERROR", f"Failed to process quad condition {condition.get('quad_card_id')}: {e}")
        except Exception as e:
            self.log("ERROR", f"Failed to load quad conditions: {e}")

    def _process_card_condition(self, condition: dict):
        """Xử lý một card alarm condition dùng strategy tương ứng.

        Thay thế cả _process_card_condition cũ và _process_quad_condition.
        Factory tra cứu strategy đúng dựa trên card_type.
        """
        card_type = condition.get("card_type")
        card_id = condition.get("card_id")
        if not card_type or card_id is None:
            return

        # Factory tra cứu strategy – nếu không có thì card_type không được hỗ trợ
        strategy = AlarmStrategyFactory.get(card_type)
        if not strategy:
            self.log("WARNING", f"No alarm strategy registered for card_type='{card_type}' – skipping")
            return

        card_info = self._get_cached_card_info(strategy, card_id)
        if not card_info:
            return

        # Strategy cung cấp danh sách cột cần evaluate – không cần if/elif chain
        for col in strategy.get_columns(card_info):
            pv_value = self._get_tag_value(col.pv_tag_id)
            sv_value = self._get_tag_value(col.sv_tag_id) if col.sv_tag_id else None
            self._evaluate_card_column(strategy, condition, col, pv_value, sv_value, card_id, card_info)

    def _evaluate_card_column(self, strategy: CardAlarmStrategy, condition: dict,
                               col: ColumnDef, pv_value: float, sv_value: Optional[float],
                               card_id: int, card_info: dict):
        """Evaluate alarm conditions cho một cột của card.

        Thay thế cả _evaluate_card_column cũ và _evaluate_quad_column.
        Dùng strategy để tạo alarm_key và build event – không phụ thuộc card_type cụ thể.
        """
        column = col.name
        alarm_key = strategy.make_alarm_key(card_id, column)

        # Condition parameters cho cột này (format chuẩn cho mọi loại card)
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

        # Kiểm tra offline cho PV tag, SV tag và compare tags
        offline_tags: set = set()
        for tid in (col.pv_tag_id, col.sv_tag_id):
            if tid:
                online = self._is_tag_device_online(tid)
                if online is False:
                    offline_tags.add(tid)
        for tid in filter(None, [
            high_compare_tag_id if high_compare_type == "tag" else None,
            low_compare_tag_id if low_compare_type == "tag" else None,
        ]):
            online = self._is_tag_device_online(tid)
            if online is False:
                offline_tags.add(tid)

        if offline_tags:
            self.log("DEBUG", f"{strategy.card_type}/{card_id} {column}: skip – device offline tag(s) {sorted(offline_tags)}")
            self._offline_skipped.add(alarm_key)
            return

        # Kiểm tra reconnect (device vừa online lại)
        reconnected = alarm_key in self._offline_skipped
        if reconnected:
            self._offline_skipped.discard(alarm_key)
            self.log("INFO", f"{strategy.card_type}/{card_id} {column}: device back online, re-evaluating alarm state")

        # Evaluate HIGH threshold (chỉ so sánh PV value)
        high_threshold = None
        high_condition_met = False
        if high_op and (high_value is not None or high_compare_tag_id):
            if high_compare_type == "tag" and high_compare_tag_id:
                high_threshold = self._get_tag_value(high_compare_tag_id)
            else:
                high_threshold = high_value
            if high_threshold is not None and pv_value is not None:
                high_condition_met = self._compare_value(pv_value, high_op, high_threshold)

        # Evaluate LOW threshold (chỉ so sánh PV value)
        low_threshold = None
        low_condition_met = False
        if low_op and (low_value is not None or low_compare_tag_id):
            if low_compare_type == "tag" and low_compare_tag_id:
                low_threshold = self._get_tag_value(low_compare_tag_id)
            else:
                low_threshold = low_value
            if low_threshold is not None and pv_value is not None:
                low_condition_met = self._compare_value(pv_value, low_op, low_threshold)

        alarm_triggered = high_condition_met or low_condition_met
        current_alarm_type = "High" if high_condition_met else ("Low" if low_condition_met else None)

        stored_alarm_type = self._alarm_types.get(alarm_key)
        previous_active = self._alarm_states.get(alarm_key, False)

        # Chọn stable times dựa trên context (activation vs deactivation)
        if alarm_triggered:
            selected_on_stable = high_on_stable if high_condition_met else low_on_stable
            selected_off_stable = high_off_stable if high_condition_met else low_off_stable
        elif previous_active and stored_alarm_type:
            selected_on_stable = high_on_stable if stored_alarm_type == "High" else low_on_stable
            selected_off_stable = high_off_stable if stored_alarm_type == "High" else low_off_stable
        else:
            selected_on_stable = high_on_stable
            selected_off_stable = high_off_stable

        self._process_card_alarm_state(
            strategy=strategy,
            alarm_key=alarm_key,
            current_condition=alarm_triggered,
            card_id=card_id,
            column=column,
            pv_value=pv_value,
            pv_tag_id=col.pv_tag_id,
            sv_value=sv_value,
            high_met=high_condition_met,
            low_met=low_condition_met,
            on_stable_sec=selected_on_stable,
            off_stable_sec=selected_off_stable,
            email=email,
            sms=sms,
            description=description,
            threshold=high_threshold if high_condition_met else low_threshold,
            operator=high_op if high_condition_met else low_op,
            current_alarm_type=current_alarm_type,
            high_threshold=high_threshold,
            low_threshold=low_threshold,
            high_op=high_op,
            low_op=low_op,
            card_info=card_info,
            reconnected=reconnected,
        )

    def _process_card_alarm_state(
        self,
        strategy: CardAlarmStrategy,
        alarm_key: str,
        current_condition: bool,
        card_id: int,
        column: str,
        pv_value: float,
        pv_tag_id: int,
        sv_value: Optional[float],
        high_met: bool,
        low_met: bool,
        on_stable_sec: int,
        off_stable_sec: int,
        email: str,
        sms: str,
        description: str,
        threshold: float,
        operator: str,
        current_alarm_type: str,
        high_threshold: float,
        low_threshold: float,
        high_op: str,
        low_op: str,
        card_info: dict = None,
        reconnected: bool = False,
    ):
        """State machine alarm với stability timers – dùng chung cho mọi qtag type.

        Thay thế cả _process_card_alarm_state cũ và _process_quad_alarm_state.
        Strategy cung cấp: display name, socket event name/payload, DB methods.
        """
        now = time.time()
        previous_active = self._alarm_states.get(alarm_key, False)
        stored_alarm_type = self._alarm_types.get(alarm_key)

        # Display names qua strategy (không hardcode)
        alarm_name = strategy.get_display_name(card_id, card_info) if card_info else f"{strategy.card_type} {card_id}"
        column_display = strategy.get_column_display(column, card_info) if card_info else column.capitalize()

        # ── Alarm type change: HIGH → LOW hoặc LOW → HIGH ────────────────────
        alarm_type_changed = (
            previous_active and current_condition
            and stored_alarm_type and current_alarm_type
            and stored_alarm_type != current_alarm_type
        )
        if alarm_type_changed:
            self.log("INFO", f"🔄 Alarm TYPE CHANGED: {strategy.card_type}/{card_id} {column} - {stored_alarm_type} → {current_alarm_type}")
            self._alarm_states[alarm_key] = False
            try:
                event_data = strategy.build_event(
                    card_id=card_id, column=column, alarm_type=stored_alarm_type,
                    status="OUTGOING", pv_tag_id=pv_tag_id, pv_value=pv_value,
                    sv_value=sv_value, threshold=threshold, operator=operator,
                    timestamp=datetime.now().isoformat(),
                )
                self.data_queue.put({"type": strategy.socket_event_name, "data": event_data})
                try:
                    sio.emit(strategy.socket_event_name, event_data)
                except Exception:
                    pass
            except Exception as e:
                self.log("ERROR", f"Failed to emit alarm type change OUTGOING: {e}")
            self._alarm_since[alarm_key] = now
            self._alarm_types[alarm_key] = current_alarm_type
            return

        # ── INCOMING: điều kiện mới bắt đầu (potential activation) ──────────
        if current_condition and not previous_active:
            if alarm_key not in self._alarm_since:
                self._alarm_since[alarm_key] = now

            stable_time = now - self._alarm_since[alarm_key]
            if stable_time >= on_stable_sec:
                alarm_type = "High" if high_met else "Low"
                self._alarm_states[alarm_key] = True
                self._alarm_types[alarm_key] = alarm_type

                self.log("INFO", f"⚠️ Alarm TRIGGERED: {alarm_name} ({strategy.card_type}) - {alarm_type} - {column_display} PV: {pv_value} (threshold: {threshold})")

                # Emit socket event TRƯỚC để UI cập nhật ngay lập tức
                ts_now = datetime.now()
                event_data = strategy.build_event(
                    card_id=card_id, column=column, alarm_type=alarm_type,
                    status="INCOMING", pv_tag_id=pv_tag_id, pv_value=pv_value,
                    sv_value=sv_value, threshold=threshold, operator=operator,
                    timestamp=ts_now.isoformat(),
                )
                try:
                    sio.emit(strategy.socket_event_name, event_data)
                    self.data_queue.put({"type": strategy.socket_event_name, "data": event_data})
                except Exception as e:
                    self.log("WARNING", f"Socket emit failed: {e}")

                # Lưu vào DB sau (chậm hơn nhưng UI đã update rồi)
                if self.db_available:
                    try:
                        note_prefix = f"{column_display} - " if column_display else ""
                        event_id = self.db.insert_alarm_event(
                            ts=ts_now, name=alarm_name, level="Critical",
                            target=pv_tag_id, value=pv_value,
                            note=f"{note_prefix}Alarm activated ({alarm_type}): {operator} {threshold}",
                            event_type="INCOMING", operator=operator, threshold=threshold,
                        )
                        state_id = strategy.save_alarm_state(
                            self.db, card_id, column, alarm_type, pv_value, sv_value, threshold, operator
                        )
                        self.log("INFO", f"✅ Saved alarm INCOMING: event={event_id}, state={state_id}")
                    except Exception as e:
                        self.log("ERROR", f"Failed to save alarm event: {e}")

                if email or sms:
                    self._send_card_notification(
                        alarm_key, alarm_name, pv_value, threshold, operator,
                        "incoming", email, sms, description, on_stable_sec,
                        device_name=strategy.device_label,
                    )

        # ── OUTGOING: điều kiện hết (potential deactivation) ─────────────────
        elif not current_condition and previous_active:
            if alarm_key not in self._alarm_since:
                self._alarm_since[alarm_key] = now

            stable_time = now - self._alarm_since[alarm_key]
            if stable_time >= off_stable_sec:
                alarm_type = self._alarm_types.get(alarm_key, "High")
                stored_threshold = high_threshold if alarm_type == "High" else low_threshold
                stored_operator = high_op if alarm_type == "High" else low_op

                self._alarm_states[alarm_key] = False
                self.log("INFO", f"✅ Alarm CLEARED: {alarm_name} ({strategy.card_type}) - {alarm_type} - PV: {pv_value}")

                ts_now = datetime.now()
                event_data = strategy.build_event(
                    card_id=card_id, column=column, alarm_type=alarm_type,
                    status="OUTGOING", pv_tag_id=pv_tag_id, pv_value=pv_value,
                    sv_value=sv_value, threshold=stored_threshold, operator=stored_operator,
                    timestamp=ts_now.isoformat(),
                )
                try:
                    sio.emit(strategy.socket_event_name, event_data)
                    self.data_queue.put({"type": strategy.socket_event_name, "data": event_data})
                except Exception as e:
                    self.log("WARNING", f"Socket emit failed: {e}")

                if self.db_available:
                    try:
                        note_prefix = f"{column_display} - " if column_display else ""
                        event_id = self.db.insert_alarm_event(
                            ts=ts_now, name=alarm_name, level="Warning",
                            target=pv_tag_id, value=pv_value,
                            note=f"{note_prefix}Alarm cleared ({alarm_type}): {stored_operator} {stored_threshold}",
                            event_type="OUTGOING", operator=stored_operator, threshold=stored_threshold,
                        )
                        deleted = strategy.delete_alarm_state(self.db, card_id, column)
                        self.log("INFO", f"✅ Saved alarm OUTGOING: event={event_id}, deleted={deleted}")
                    except Exception as e:
                        self.log("ERROR", f"Failed to save alarm clear event: {e}")

                if email or sms:
                    self._send_card_notification(
                        alarm_key, alarm_name, pv_value, stored_threshold, stored_operator,
                        "outgoing", email, sms, description, off_stable_sec,
                        device_name=strategy.device_label,
                    )

                self._alarm_types.pop(alarm_key, None)
                self._alarm_since.pop(alarm_key, None)

        # ── Stable state: reset timer; re-emit sau reconnect ─────────────────
        elif current_condition == previous_active:
            self._alarm_since.pop(alarm_key, None)

            if reconnected and current_condition and previous_active:
                alarm_type = self._alarm_types.get(alarm_key, current_alarm_type or "High")
                self.log("INFO", f"🔄 {strategy.card_type}/{card_id} {column}: reconnected – re-emitting INCOMING (type={alarm_type})")
                try:
                    event_data = strategy.build_event(
                        card_id=card_id, column=column, alarm_type=alarm_type,
                        status="INCOMING", pv_tag_id=pv_tag_id, pv_value=pv_value,
                        sv_value=sv_value, threshold=threshold, operator=operator,
                        timestamp=datetime.now().isoformat(),
                    )
                    self.data_queue.put({"type": strategy.socket_event_name, "data": event_data})
                    try:
                        sio.emit(strategy.socket_event_name, event_data)
                    except Exception:
                        pass
                except Exception as e:
                    self.log("ERROR", f"Failed to re-emit alarm after reconnect: {e}")

    def _send_card_notification(self, alarm_key: str, alarm_name: str,
                                value: float, threshold: float, operator: str,
                                notification_type: str, email: str, sms: str,
                                description: str, stable_time_sec: int,
                                device_name: str = "Tag Card"):
        """Gửi email/SMS cho alarm – dùng chung cho mọi qtag type.

        Thay thế cả _send_card_notification cũ và _send_quad_notification.
        device_name được cung cấp bởi strategy.device_label.
        """
        if not self._should_send_notification(hash(alarm_key), notification_type, stable_time_sec):
            return

        subject = f"ALARM: {alarm_name}" if notification_type == "incoming" else f"✅ ALARM CLEARED: {alarm_name}"
        body = self.create_alarm_email_body(
            device_name=device_name,
            alarm_name=alarm_name,
            tag_value=value,
            threshold=threshold,
            operator=operator,
            alarm_level="Warning",
            notification_type=notification_type,
        )
        if description:
            body += f"\n\nDescription: {description}"

        if email and email.strip():
            email_thread = threading.Thread(
                target=self._send_email_async,
                args=(email.strip(), subject, body, hash(alarm_key)),
                daemon=True,
            )
            email_thread.start()

        if sms and sms.strip():
            sms_message = self.create_alarm_sms_text(
                alarm_name=alarm_name,
                tag_value=value,
                threshold=threshold,
                operator=operator,
                notification_type=notification_type,
            )
            if description:
                sms_message += f"\nNote: {description[:50]}"
            try:
                if self.sms_queue is not None:
                    self.sms_queue.put_nowait((sms.strip(), sms_message, hash(alarm_key)))
            except Exception as e:
                self.log("ERROR", f"Failed to queue SMS for alarm: {e}")

        if hash(alarm_key) not in self._last_notification:
            self._last_notification[hash(alarm_key)] = {}
        self._last_notification[hash(alarm_key)][notification_type] = time.time()

    def _restore_card_alarm_states(self):
        """Restore tất cả card + quad alarm states từ DB khi khởi động.

        Thay thế cả _restore_card_alarm_states cũ và _restore_quad_alarm_states.
        Dùng AlarmStrategyFactory.make_alarm_key để tạo đúng key cho từng loại.
        """
        if not self.db_available:
            return
        try:
            restored = 0

            # --- Card alarm states ---
            for alarm in (self.db.get_active_card_alarms() or []):
                card_type = alarm.get("card_type")
                card_id = alarm.get("card_id")
                column = alarm.get("column")
                alarm_type = alarm.get("alarm_type", "High")
                if card_type and card_id is not None and column:
                    strategy = AlarmStrategyFactory.get(card_type)
                    if strategy:
                        alarm_key = strategy.make_alarm_key(int(card_id), column)
                    else:
                        # Fallback để tương thích ngược nếu loại chưa register
                        alarm_key = f"card_{card_type}_{card_id}_{column}"
                    self._alarm_states[alarm_key] = True
                    self._alarm_types[alarm_key] = alarm_type
                    restored += 1
                    self.log("DEBUG", f"Restored card alarm state: {alarm_key} type={alarm_type}")

            # --- Quad alarm states ---
            quad_strategy = AlarmStrategyFactory.get("quad")
            for alarm in (self.db.get_active_quad_alarms() or []):
                quad_id = alarm.get("quad_id")
                column = alarm.get("column")
                alarm_type = alarm.get("alarm_type", "High")
                if quad_id is not None and column:
                    if quad_strategy:
                        alarm_key = quad_strategy.make_alarm_key(int(quad_id), column)
                    else:
                        alarm_key = f"quad_{quad_id}_{column}"
                    self._alarm_states[alarm_key] = True
                    self._alarm_types[alarm_key] = alarm_type
                    restored += 1
                    self.log("DEBUG", f"Restored quad alarm state: {alarm_key} type={alarm_type}")

            self.log("INFO", f"✅ Restored {restored} persisted alarm state(s) (card + quad) from database")
        except Exception as e:
            self.log("ERROR", f"Failed to restore alarm states: {e}")

    def _restore_quad_alarm_states(self):
        """Đã được hợp nhất vào _restore_card_alarm_states. Giữ lại để backward compat."""
        pass  # Logic đã chuyển vào _restore_card_alarm_states

    def _alarm_loop(self):
        """Main alarm evaluation loop"""
        self.log("INFO", "Starting alarm evaluation loop")
        _sio_health_check_counter = 0  # Check socket.io health every N cycles
        _sio_reconnecting = False  # Flag to avoid overlapping reconnect threads
        
        while self.running:
            try:
                # Clear per-cycle tag value cache at the start of each cycle
                self._tag_value_cache.clear()
                
                # Periodic Socket.IO connection health check (every ~10 cycles = ~10s at 1s interval)
                # Runs in background thread to avoid blocking alarm evaluation
                _sio_health_check_counter += 1
                if _sio_health_check_counter >= 10:
                    _sio_health_check_counter = 0
                    if not sio_manager.connected and not _sio_reconnecting:
                        def _bg_reconnect():
                            nonlocal _sio_reconnecting
                            _sio_reconnecting = True
                            try:
                                self.log("WARNING", "Socket.IO connection lost, attempting reconnect in background...")
                                if sio_manager.ensure_connected():
                                    self.log("INFO", "Socket.IO reconnected successfully")
                                else:
                                    self.log("WARNING", "Socket.IO reconnect failed, will retry next health check")
                            except Exception as e:
                                self.log("WARNING", f"Socket.IO health check error: {e}")
                            finally:
                                _sio_reconnecting = False
                        threading.Thread(target=_bg_reconnect, daemon=True).start()
                
                # Load current alarm rules
                alarm_rules = self._load_alarm_rules()
                
                # Get tag IDs managed by ALL card/quad alarms to avoid duplicate notifications
                managed_tags = self._get_all_managed_tag_ids()
                
                if alarm_rules:
                    # self.log("DEBUG", f"Evaluating {len(alarm_rules)} alarm rules")
                    
                    # Process each enabled alarm rule (skip tags already covered by card/quad alarms)
                    for rule in alarm_rules:
                        if rule.get("enabled", True):
                            tag_id = rule.get("tag_id") or rule.get("target")
                            if tag_id and int(tag_id) in managed_tags:
                                continue  # Skip - this tag is managed by card/quad alarm system
                            self._process_alarm_rule(rule)
                
                # Evaluate all qtag card + quad alarm conditions (unified via Strategy Pattern)
                try:
                    self._evaluate_card_conditions()
                except Exception as e:
                    self.log("ERROR", f"Failed to evaluate card/quad conditions: {e}")
                
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
                        "last_update": time.time(),
                        "sio_connected": sio_manager.connected  # Track socket health
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
            # Restore persisted alarm states from database before starting loop
            self._restore_quad_alarm_states()
            self._restore_card_alarm_states()
            
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