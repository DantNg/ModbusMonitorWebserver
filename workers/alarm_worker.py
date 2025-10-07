"""
Alarm Worker - Dedicated process for alarm evaluation and notifications
"""
import time
import math
import logging
import threading
from multiprocessing import Process, Queue
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class AlarmConfig:
    """Configuration for alarm worker"""
    check_interval: float = 0.5  # How often to check alarms
    enable_notifications: bool = True

class AlarmWorker:
    """Dedicated worker process for alarm evaluation"""
    
    def __init__(self, config: AlarmConfig, data_queue: Queue, command_queue: Queue, log_queue: Queue, shared_state):
        self.config = config
        self.data_queue = data_queue
        self.command_queue = command_queue
        self.log_queue = log_queue
        self.shared_state = shared_state
        
        # Alarm state tracking
        self._alarm_states: Dict[int, bool] = {}  # rule_id -> active
        self._alarm_since: Dict[int, float] = {}  # rule_id -> timestamp
        self._last_notification: Dict[int, Dict[str, float]] = {}  # rule_id -> {"incoming": ts, "outgoing": ts}
        
        # Runtime state
        self.running = False
        self.seq = 0
        
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
        
        operators = {
            ">": value > threshold,
            "<": value < threshold, 
            ">=": value >= threshold,
            "<=": value <= threshold,
            "==": value == threshold,
            "!=": value != threshold
        }
        return operators.get(operator, False)
    
    def _should_send_notification(self, rule_id: int, notification_type: str, stable_time_sec: int) -> bool:
        """Check if notification should be sent based on debounce timer"""
        now = time.time()
        debounce_time = stable_time_sec * 2  # Prevent notification spam
        
        if rule_id not in self._last_notification:
            self._last_notification[rule_id] = {}
        
        last_sent = self._last_notification[rule_id].get(notification_type, 0)
        return (now - last_sent) >= debounce_time
    
    def _send_notification(self, rule_id: int, rule_name: str, tag_name: str, 
                          value: float, threshold: float, operator: str, 
                          notification_type: str, contacts: List[dict]):
        """Send alarm notification via email/SMS"""
        try:
            # Create notification message
            status = "ACTIVE" if notification_type == "incoming" else "CLEARED"
            subject = f"Alarm {status}: {rule_name}"
            
            message = f"""
Alarm {status}: {rule_name}

Tag: {tag_name}
Current Value: {value}
Threshold: {threshold} (operator: {operator})
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """.strip()
            
            # Send notifications to configured contacts
            for contact in contacts:
                if contact.get("enabled", False):
                    if contact.get("email"):
                        self._send_email_notification(contact["email"], subject, message)
                    if contact.get("phone"):
                        self._send_sms_notification(contact["phone"], f"{subject}: {tag_name} = {value}")
            
            # Update last notification timestamp
            self._last_notification[rule_id][notification_type] = time.time()
            
        except Exception as e:
            self.log("ERROR", f"Failed to send notification for rule {rule_id}: {e}")
    
    def _send_email_notification(self, to_email: str, subject: str, body: str):
        """Send email notification (implement based on your SMTP config)"""
        # TODO: Implement email sending using SMTP config
        self.log("INFO", f"Email notification sent to {to_email}: {subject}")
    
    def _send_sms_notification(self, phone: str, message: str):
        """Send SMS notification (implement based on your SMS provider)"""
        # TODO: Implement SMS sending
        self.log("INFO", f"SMS notification sent to {phone}: {message[:50]}...")
    
    def _load_alarm_rules(self) -> List[dict]:
        """Load alarm rules from database"""
        try:
            # TODO: Load from shared database or cache
            # For now, return empty list
            return []
        except Exception as e:
            self.log("ERROR", f"Failed to load alarm rules: {e}")
            return []
    
    def _get_tag_value(self, tag_id: int) -> Optional[float]:
        """Get current tag value from shared data"""
        try:
            # Get latest value from shared state
            tag_data = self.shared_state.get(f"tag_{tag_id}", {})
            return tag_data.get("value")
        except Exception as e:
            self.log("ERROR", f"Failed to get tag {tag_id} value: {e}")
            return None
    
    def _evaluate_alarm_rule(self, rule: dict) -> bool:
        """Evaluate a single alarm rule"""
        try:
            tag_id = rule.get("tag_id")
            operator = rule.get("operator", ">")
            threshold = float(rule.get("threshold", 0))
            
            current_value = self._get_tag_value(tag_id)
            if current_value is None:
                return False
            
            return self._compare_value(current_value, operator, threshold)
            
        except Exception as e:
            self.log("ERROR", f"Failed to evaluate alarm rule {rule.get('id', 'unknown')}: {e}")
            return False
    
    def _process_alarm_rule(self, rule: dict):
        """Process a single alarm rule and handle state changes"""
        try:
            rule_id = rule["id"]
            rule_name = rule.get("name", f"Rule {rule_id}")
            tag_id = rule.get("tag_id")
            on_stable_sec = rule.get("on_stable_sec", 5)
            off_stable_sec = rule.get("off_stable_sec", 5)
            
            # Evaluate current condition
            current_condition = self._evaluate_alarm_rule(rule)
            previous_active = self._alarm_states.get(rule_id, False)
            now = time.time()
            
            # Handle state changes
            if current_condition and not previous_active:
                # Alarm becoming active
                self._alarm_since[rule_id] = now
                self._alarm_states[rule_id] = True
                
                # Check if stable time reached for notification
                if on_stable_sec <= 0 or (now - self._alarm_since[rule_id]) >= on_stable_sec:
                    if self._should_send_notification(rule_id, "incoming", on_stable_sec):
                        current_value = self._get_tag_value(tag_id)
                        contacts = rule.get("notification_contacts", [])
                        
                        self._send_notification(
                            rule_id, rule_name, rule.get("tag_name", f"Tag {tag_id}"),
                            current_value, rule.get("threshold"), rule.get("operator"),
                            "incoming", contacts
                        )
                        
            elif not current_condition and previous_active:
                # Alarm becoming inactive
                self._alarm_since[rule_id] = now
                self._alarm_states[rule_id] = False
                
                # Check if stable time reached for notification
                if off_stable_sec <= 0 or (now - self._alarm_since[rule_id]) >= off_stable_sec:
                    if self._should_send_notification(rule_id, "outgoing", off_stable_sec):
                        current_value = self._get_tag_value(tag_id)
                        contacts = rule.get("notification_contacts", [])
                        
                        self._send_notification(
                            rule_id, rule_name, rule.get("tag_name", f"Tag {tag_id}"),
                            current_value, rule.get("threshold"), rule.get("operator"),
                            "outgoing", contacts
                        )
            
        except Exception as e:
            self.log("ERROR", f"Failed to process alarm rule {rule.get('id', 'unknown')}: {e}")
    
    def _alarm_loop(self):
        """Main alarm evaluation loop"""
        self.log("INFO", "Starting alarm evaluation loop")
        
        while self.running:
            try:
                # Load current alarm rules
                alarm_rules = self._load_alarm_rules()
                
                # Process each alarm rule
                for rule in alarm_rules:
                    if rule.get("enabled", True):
                        self._process_alarm_rule(rule)
                
                # Update sequence number
                self.seq += 1
                
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
    
    # Handle commands periodically
    def command_handler():
        while worker.running:
            worker._handle_commands()
            time.sleep(0.1)
    
    # Start command handler thread
    command_thread = threading.Thread(target=command_handler, daemon=True)
    command_thread.start()
    
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