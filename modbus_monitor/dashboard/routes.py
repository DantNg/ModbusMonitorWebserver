from flask import render_template,request,jsonify,session,redirect,url_for,flash
from . import dashboard_bp
from modbus_monitor.database import db
from modbus_monitor.database.db import get_latest_tag_value
from datetime import datetime, timedelta
from modbus_monitor.extensions import socketio

@dashboard_bp.route("/dashboard")
def dashboard():
    print("Role:", session.get("role"))
    if "username" not in session:
        return redirect(url_for("auth_bp.login"))
    current_device = request.args.get("device", "__all__")
    devices = db.list_devices()
    
    # Only load tag data for visible devices to improve performance
    for dev in devices:
        # Only load tags if this device will be displayed
        if current_device == "__all__" or str(dev["id"]) == current_device:
            dev["tags"] = db.list_tags(dev["id"])
            for tag in dev["tags"]:
                value, ts = get_latest_tag_value(tag["id"])
                tag["value"] = value
                tag["ts"] = ts.strftime("%H:%M") if ts else "--:--"
                tag["alarm_status"] = "Normal"  # Or your logic
        else:
            dev["tags"] = []  # Empty for non-displayed devices
            
    return render_template(
        "dashboard.html",
        devices=devices,
        current_device=current_device,
        role=session.get("role")
    )

# Emit real-time tag updates
@dashboard_bp.route("/api/tags")
def api_tags():
    try:
        current_device = request.args.get("device", "__all__")
        # print(f"Current device: {current_device}")
        
        tags = []
        devices = db.list_devices()
        # print(f"Devices: {devices}")
        
        for dev in devices:
            if current_device != "__all__" and str(dev["id"]) != current_device:
                continue
            # print(f"Processing device: {dev}")
            
            for tag in db.list_tags(dev["id"]):
                # print(f"Processing tag: {tag}")
                value, ts = get_latest_tag_value(tag["id"])
                # print(f"Tag value: {value}, Timestamp: {ts}")
                
                tags.append({
                    "id": tag["id"],
                    "name": tag["name"],
                    "value": value,
                    "ts": ts.strftime("%H:%M") if ts else "--:--"
                })
        
        # print(f"Tags: {tags}")
        # socketio.emit("update_tags", {"tags": tags})
        return jsonify({"tags": tags})
    except Exception as e:
        print(f"Error in /api/tags: {e}")
        return jsonify({"error": str(e)}), 500

@dashboard_bp.route("/api/alarms/recent")
def api_recent_alarms():
    """API endpoint to get recent alarm events for notifications"""
    try:
        # Get recent alarm events (last hour)
        recent_time = datetime.now() - timedelta(hours=1)
        
        # Get alarm events from database
        alarms = db.get_recent_alarm_events(since=recent_time)
        
        # Format for notification system
        formatted_alarms = []
        for alarm in alarms:
            formatted_alarms.append({
                "id": alarm.get("id"),
                "tag_id": alarm.get("tag_id"),
                "tag_name": alarm.get("tag_name", "Unknown Tag"),
                "title": f"Alarm: {alarm.get('alarm_name', 'Alert')}",
                "message": alarm.get("message", "Alarm condition detected"),
                "level": alarm.get("level", "Medium"),
                "created_at": alarm.get("created_at", datetime.now()).isoformat(),
                "status": alarm.get("event_type", "Active")
            })
        
        return jsonify(formatted_alarms)
    except Exception as e:
        print(f"Error in /api/alarms/recent: {e}")
        return jsonify({"error": str(e)}), 500