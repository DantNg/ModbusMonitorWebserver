"""
Notification Manager - Handles email, SMS, webhook, and other notifications
"""

import smtplib
import json
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Dict, Any
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

class NotificationManager:
    def __init__(self, email_enabled=False, sms_enabled=False, webhook_enabled=False, slack_enabled=False):
        self.email_enabled = email_enabled
        self.sms_enabled = sms_enabled
        self.webhook_enabled = webhook_enabled
        self.slack_enabled = slack_enabled
        
        # Load configuration
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """Load notification configuration"""
        try:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'SMTP_config.json')
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    return json.load(f)
            else:
                print("⚠️ SMTP config file not found, using defaults")
                return {}
        except Exception as e:
            print(f"❌ Error loading config: {e}")
            return {}
    
    def send_alarm(self, alarm_result: Dict, is_repeat: bool = False):
        """Send alarm notifications via enabled channels"""
        try:
            subject = f"{'REPEAT ' if is_repeat else ''}ALARM: {alarm_result['rule_name']}"
            message = self._format_alarm_message(alarm_result, is_repeat)
            
            sent_channels = []
            
            # Email notification
            if self.email_enabled and alarm_result.get('email'):
                if self._send_email(alarm_result['email'], subject, message):
                    sent_channels.append('email')
            
            # SMS notification (placeholder)
            if self.sms_enabled and alarm_result.get('sms'):
                if self._send_sms(alarm_result['sms'], message):
                    sent_channels.append('sms')
            
            # Webhook notification
            if self.webhook_enabled:
                if self._send_webhook(alarm_result):
                    sent_channels.append('webhook')
            
            # Slack notification
            if self.slack_enabled:
                if self._send_slack(message):
                    sent_channels.append('slack')
            
            if sent_channels:
                print(f"📧 Alarm notifications sent via: {', '.join(sent_channels)}")
            else:
                print("⚠️ No notifications sent - check configuration")
                
        except Exception as e:
            print(f"❌ Error sending alarm notifications: {e}")
    
    def send_alarm_resolved(self, alarm_key: str, duration):
        """Send alarm resolution notification"""
        try:
            subject = f"RESOLVED: {alarm_key}"
            message = f"Alarm resolved after {duration}"
            
            print(f"✅ Alarm resolved notification: {subject}")
            # Add actual notification logic here
            
        except Exception as e:
            print(f"❌ Error sending resolution notification: {e}")
    
    def send_system_notification(self, title: str, message: str):
        """Send system-level notifications"""
        try:
            print(f"🔔 System notification: {title} - {message}")
            # Add actual notification logic here
            
        except Exception as e:
            print(f"❌ Error sending system notification: {e}")
    
    def _format_alarm_message(self, alarm_result: Dict, is_repeat: bool) -> str:
        """Format alarm message for notifications"""
        timestamp = alarm_result['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        
        message = f"""
{'REPEAT ' if is_repeat else ''}ALARM TRIGGERED

Alarm: {alarm_result['rule_name']} ({alarm_result.get('rule_code', 'N/A')})
Severity: {alarm_result['level']}
Device: {alarm_result['device_name']}
Tag: {alarm_result['tag_name']}
Current Value: {alarm_result['value']}
Condition: {alarm_result['tag_name']} {alarm_result['operator']} {alarm_result['threshold']}
Time: {timestamp}

Please investigate immediately.
"""
        return message.strip()
    
    def _send_email(self, to_email: str, subject: str, message: str) -> bool:
        """Send email notification"""
        try:
            if not self.config.get('smtp_server'):
                print("⚠️ SMTP server not configured")
                return False
            
            msg = MIMEMultipart()
            msg['From'] = self.config.get('smtp_from', 'noreply@modbus-monitor.local')
            msg['To'] = to_email
            msg['Subject'] = subject
            
            msg.attach(MIMEText(message, 'plain'))
            
            server = smtplib.SMTP(
                self.config.get('smtp_server', 'localhost'),
                self.config.get('smtp_port', 587)
            )
            
            if self.config.get('smtp_use_tls', True):
                server.starttls()
            
            if self.config.get('smtp_username') and self.config.get('smtp_password'):
                server.login(
                    self.config['smtp_username'],
                    self.config['smtp_password']
                )
            
            server.send_message(msg)
            server.quit()
            
            return True
            
        except Exception as e:
            print(f"❌ Email send error: {e}")
            return False
    
    def _send_sms(self, phone_number: str, message: str) -> bool:
        """Send SMS notification (placeholder - implement with your SMS provider)"""
        try:
            # Implement SMS sending logic here
            # Example: Twilio, AWS SNS, etc.
            print(f"📱 SMS to {phone_number}: {message[:50]}...")
            return True
            
        except Exception as e:
            print(f"❌ SMS send error: {e}")
            return False
    
    def _send_webhook(self, alarm_result: Dict) -> bool:
        """Send webhook notification"""
        try:
            webhook_url = self.config.get('webhook_url')
            if not webhook_url:
                return False
            
            payload = {
                'type': 'alarm',
                'timestamp': alarm_result['timestamp'].isoformat(),
                'alarm': alarm_result
            }
            
            response = requests.post(
                webhook_url,
                json=payload,
                timeout=10,
                headers={'Content-Type': 'application/json'}
            )
            
            return response.status_code == 200
            
        except Exception as e:
            print(f"❌ Webhook send error: {e}")
            return False
    
    def _send_slack(self, message: str) -> bool:
        """Send Slack notification"""
        try:
            slack_webhook = self.config.get('slack_webhook_url')
            if not slack_webhook:
                return False
            
            payload = {
                'text': message,
                'username': 'Modbus Monitor',
                'icon_emoji': ':warning:'
            }
            
            response = requests.post(
                slack_webhook,
                json=payload,
                timeout=10
            )
            
            return response.status_code == 200
            
        except Exception as e:
            print(f"❌ Slack send error: {e}")
            return False