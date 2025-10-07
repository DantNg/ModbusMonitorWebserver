#!/usr/bin/env python3
"""
Alarm Process - Handles alarm evaluation and notifications
Monitors device data and triggers alarms based on configured rules
Usage: python run_alarm.py --check-interval 10 --email --sms
"""

import sys
import os
import argparse
import time
import threading
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    parser = argparse.ArgumentParser(description='Alarm Process')
    parser.add_argument('--check-interval', type=int, default=10, help='Alarm check interval in seconds (default: 10)')
    parser.add_argument('--email', action='store_true', help='Enable email notifications')
    parser.add_argument('--sms', action='store_true', help='Enable SMS notifications')
    parser.add_argument('--webhook', action='store_true', help='Enable webhook notifications')
    parser.add_argument('--slack', action='store_true', help='Enable Slack notifications')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    print(f"🚨 Starting Alarm Process")
    print(f"⏱️  Check Interval: {args.check_interval}s")
    print(f"📧 Email: {'✅' if args.email else '❌'}")
    print(f"📱 SMS: {'✅' if args.sms else '❌'}")
    print(f"🔗 Webhook: {'✅' if args.webhook else '❌'}")
    print(f"💬 Slack: {'✅' if args.slack else '❌'}")
    print(f"🐛 Debug: {'✅' if args.debug else '❌'}")
    print("-" * 50)
    
    try:
        # Import after adding path
        from shared.database_manager import DatabaseManager
        from shared.alarm_engine import AlarmEngine
        from shared.notification_manager import NotificationManager
        from shared.data_collector import DataCollector
        
        # Initialize components
        db_manager = DatabaseManager()
        notification_manager = NotificationManager(
            email_enabled=args.email,
            sms_enabled=args.sms,
            webhook_enabled=args.webhook,
            slack_enabled=args.slack
        )
        alarm_engine = AlarmEngine(db_manager, notification_manager, debug=args.debug)
        data_collector = DataCollector()
        
        # Load alarm rules from database
        print("📋 Loading alarm rules...")
        alarm_rules = alarm_engine.load_alarm_rules()
        print(f"⚙️  Loaded {len(alarm_rules)} alarm rules")
        
        if args.debug:
            for rule in alarm_rules:
                print(f"   🔔 {rule['name']}: {rule['condition']} → {rule['severity']}")
        
        print("🚀 Starting alarm monitoring...")
        
        alarm_states = {}  # Track alarm states to prevent spam
        last_check = datetime.now()
        
        while True:
            try:
                current_time = datetime.now()
                
                # Collect current data
                collected_data = data_collector.collect_all_data()
                
                if collected_data:
                    # Evaluate each alarm rule
                    active_alarms = []
                    
                    for rule in alarm_rules:
                        try:
                            # Check if rule applies to current data
                            alarm_result = alarm_engine.evaluate_rule(rule, collected_data, current_time)
                            
                            if alarm_result:
                                rule_id = rule['id']
                                alarm_key = f"{rule_id}_{alarm_result['device_id']}_{alarm_result['tag_name']}"
                                
                                # Check if this is a new alarm or repeat
                                if alarm_key not in alarm_states:
                                    # New alarm
                                    alarm_states[alarm_key] = {
                                        'first_triggered': current_time,
                                        'last_notified': current_time,
                                        'count': 1
                                    }
                                    
                                    print(f"🚨 NEW ALARM: {alarm_result['message']}")
                                    
                                    # Send notifications
                                    notification_manager.send_alarm(alarm_result)
                                    
                                    # Log to database
                                    alarm_engine.log_alarm_event(alarm_result)
                                    
                                else:
                                    # Existing alarm - check if should re-notify
                                    alarm_state = alarm_states[alarm_key]
                                    alarm_state['count'] += 1
                                    
                                    # Re-notify based on severity and time
                                    should_renotify = alarm_engine.should_renotify(
                                        rule, alarm_state, current_time
                                    )
                                    
                                    if should_renotify:
                                        alarm_state['last_notified'] = current_time
                                        print(f"🔔 REPEAT ALARM: {alarm_result['message']} (Count: {alarm_state['count']})")
                                        notification_manager.send_alarm(alarm_result, is_repeat=True)
                                    
                                    if args.debug:
                                        print(f"🔍 Ongoing: {alarm_result['message']} (Count: {alarm_state['count']})")
                                
                                active_alarms.append(alarm_key)
                        
                        except Exception as e:
                            print(f"❌ Error evaluating rule {rule.get('name', 'Unknown')}: {e}")
                            if args.debug:
                                import traceback
                                traceback.print_exc()
                    
                    # Clear resolved alarms
                    resolved_alarms = []
                    for alarm_key in list(alarm_states.keys()):
                        if alarm_key not in active_alarms:
                            alarm_info = alarm_states[alarm_key]
                            duration = current_time - alarm_info['first_triggered']
                            print(f"✅ RESOLVED: {alarm_key} (Duration: {duration})")
                            
                            # Send resolution notification if configured
                            notification_manager.send_alarm_resolved(alarm_key, duration)
                            resolved_alarms.append(alarm_key)
                    
                    # Remove resolved alarms from tracking
                    for alarm_key in resolved_alarms:
                        del alarm_states[alarm_key]
                    
                    if args.debug:
                        print(f"🔍 Check completed: {len(active_alarms)} active, {len(resolved_alarms)} resolved")
                
                else:
                    if args.debug:
                        print(f"🔍 No data available for alarm checking at {current_time.strftime('%H:%M:%S')}")
                
            except Exception as e:
                print(f"❌ Alarm check error: {e}")
                if args.debug:
                    import traceback
                    traceback.print_exc()
            
            # Wait for next check
            time.sleep(args.check_interval)
            
    except KeyboardInterrupt:
        print("\n🛑 Alarm system stopped by user")
        # Send notification about alarm system shutdown
        try:
            notification_manager.send_system_notification("Alarm System Stopped", "Manual shutdown")
        except:
            pass
    except Exception as e:
        print(f"❌ Alarm system error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()