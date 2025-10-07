from flask import render_template,request,jsonify,session,redirect,url_for,flash
from . import dashboard_bp
from modbus_monitor.database import db
from modbus_monitor.database.db import get_latest_tag_value, get_latest_tag_values_batch, safe_datetime_now
from datetime import datetime, timedelta
from modbus_monitor.extensions import socketio

@dashboard_bp.route("/dashboard")
def dashboard():
    print("Role:", session.get("role"))
    if "username" not in session:
        return redirect(url_for("auth_bp.login"))
    
    # Kiểm tra xem có subdashboards không
    subdashboards = db.list_subdashboards() if hasattr(db, "list_subdashboards") else []
    
    # Nếu có subdashboard, redirect đến subdashboard đầu tiên
    if subdashboards and len(subdashboards) > 0:
        first_subdash = subdashboards[0]
        return redirect(url_for("subdash_bp.subdash_detail", sid=first_subdash["id"]))
    
    # Nếu không có subdashboard, hiển thị page hướng dẫn
    return render_template("dashboard_guide.html")

# Emit real-time tag updates
@dashboard_bp.route("/api/tags")
def api_tags():
    # print("API tags called")
    try:
        current_device = request.args.get("device", "__all__")
        # print(f"Current device: {current_device}")
        
        # Thu thập tất cả tags và tag_ids trước
        all_tags = []
        tag_ids = []
        devices = db.list_devices()
        
        for dev in devices:
            if current_device != "__all__" and str(dev["id"]) != current_device:
                continue
            
            for tag in db.list_tags(dev["id"]):
                all_tags.append(tag)
                tag_ids.append(tag["id"])
        
        # Batch query để lấy latest values cho tất cả tags cùng lúc
        latest_values = get_latest_tag_values_batch(tag_ids)
        
        # Tạo response
        tags = []
        for tag in all_tags:
            tag_id = tag["id"]
            value, ts = latest_values.get(tag_id, (None, None))
            
            tags.append({
                "id": tag_id,
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
        recent_time = safe_datetime_now() - timedelta(hours=1)
        
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
                "created_at": alarm.get("created_at", safe_datetime_now()).isoformat(),
                "status": alarm.get("event_type", "Active")
            })
        
        return jsonify(formatted_alarms)
    except Exception as e:
        print(f"Error in /api/alarms/recent: {e}")
        return jsonify({"error": str(e)}), 500