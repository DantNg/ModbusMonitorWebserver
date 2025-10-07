"""
Alarm Engine - Evaluates alarm rules and manages alarm states
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

class AlarmEngine:
    def __init__(self, db_manager, notification_manager, debug=False):
        self.db_manager = db_manager
        self.notification_manager = notification_manager
        self.debug = debug
        self.alarm_rules = []
    
    def load_alarm_rules(self) -> List[Dict]:
        """Load alarm rules from database"""
        try:
            rules = self.db_manager.execute_query(
                """SELECT id, enabled, code, name, level, target, operator, threshold, 
                          on_stable_sec, off_stable_sec, email, sms 
                   FROM alarm_rules 
                   WHERE enabled = 1"""
            )
            
            self.alarm_rules = []
            for rule in rules:
                rule_dict = {
                    'id': rule[0],
                    'enabled': rule[1],
                    'code': rule[2],
                    'name': rule[3],
                    'level': rule[4],
                    'target': rule[5],  # tag_id
                    'operator': rule[6],
                    'threshold': rule[7],
                    'on_stable_sec': rule[8],
                    'off_stable_sec': rule[9],
                    'email': rule[10],
                    'sms': rule[11]
                }
                self.alarm_rules.append(rule_dict)
            
            return self.alarm_rules
            
        except Exception as e:
            print(f"❌ Error loading alarm rules: {e}")
            return []
    
    def evaluate_rule(self, rule: Dict, collected_data: Dict, current_time: datetime) -> Optional[Dict]:
        """
        Evaluate a single alarm rule against collected data
        Returns alarm result if triggered, None otherwise
        """
        try:
            target_tag_id = int(rule['target'])
            
            # Find the device and tag value
            tag_value = None
            device_id = None
            device_name = None
            tag_name = None
            
            for dev_id, device_data in collected_data.items():
                for t_name, value in device_data['tags'].items():
                    # We need to match by tag_id, but we only have tag_name in collected_data
                    # Let's get tag info from database
                    tag_info = self.db_manager.execute_query(
                        "SELECT id, name, device_id FROM tags WHERE id = ?",
                        (target_tag_id,)
                    )
                    
                    if tag_info and tag_info[0][1] == t_name and tag_info[0][2] == dev_id:
                        tag_value = value
                        device_id = dev_id
                        device_name = device_data['name']
                        tag_name = t_name
                        break
                
                if tag_value is not None:
                    break
            
            if tag_value is None:
                if self.debug:
                    print(f"🔍 Tag {target_tag_id} not found in collected data")
                return None
            
            # Evaluate condition
            operator = rule['operator']
            threshold = float(rule['threshold'])
            is_triggered = False
            
            if operator == '>':
                is_triggered = tag_value > threshold
            elif operator == '>=':
                is_triggered = tag_value >= threshold
            elif operator == '<':
                is_triggered = tag_value < threshold
            elif operator == '<=':
                is_triggered = tag_value <= threshold
            elif operator == '==':
                is_triggered = tag_value == threshold
            elif operator == '!=':
                is_triggered = tag_value != threshold
            else:
                print(f"⚠️ Unknown operator: {operator}")
                return None
            
            if is_triggered:
                return {
                    'rule_id': rule['id'],
                    'rule_name': rule['name'],
                    'rule_code': rule['code'],
                    'level': rule['level'],
                    'device_id': device_id,
                    'device_name': device_name,
                    'tag_id': target_tag_id,
                    'tag_name': tag_name,
                    'value': tag_value,
                    'operator': operator,
                    'threshold': threshold,
                    'timestamp': current_time,
                    'message': f"{rule['name']}: {tag_name} ({tag_value}) {operator} {threshold}",
                    'email': rule.get('email'),
                    'sms': rule.get('sms')
                }
            
            return None
            
        except Exception as e:
            print(f"❌ Error evaluating rule {rule.get('name', 'Unknown')}: {e}")
            return None
    
    def should_renotify(self, rule: Dict, alarm_state: Dict, current_time: datetime) -> bool:
        """Determine if alarm should trigger notification again"""
        try:
            level = rule['level']
            last_notified = alarm_state['last_notified']
            
            # Renotification intervals by severity
            intervals = {
                'Low': timedelta(hours=1),
                'Medium': timedelta(minutes=30),
                'High': timedelta(minutes=15),
                'Critical': timedelta(minutes=5)
            }
            
            interval = intervals.get(level, timedelta(minutes=15))
            return (current_time - last_notified) >= interval
            
        except Exception as e:
            print(f"❌ Error checking renotification: {e}")
            return False
    
    def log_alarm_event(self, alarm_result: Dict):
        """Log alarm event to database"""
        try:
            self.db_manager.execute_update(
                """INSERT INTO alarm_events 
                   (ts, name, level, target, value, note, event_type, operator, threshold) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    alarm_result['timestamp'],
                    alarm_result['rule_name'],
                    alarm_result['level'],
                    alarm_result['tag_id'],
                    alarm_result['value'],
                    f"Alarm INCOMING: {alarm_result['message']}",
                    'INCOMING',
                    alarm_result['operator'],
                    alarm_result['threshold']
                )
            )
            
        except Exception as e:
            print(f"❌ Error logging alarm event: {e}")