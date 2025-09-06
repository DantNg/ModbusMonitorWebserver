from flask import render_template,request,jsonify,session,redirect,url_for,flash
from . import dashboard_bp
from modbus_monitor.database import db
from modbus_monitor.database.db import get_latest_tag_value
from datetime import datetime
from modbus_monitor.extensions import socketio

@dashboard_bp.route("/dashboard")
def dashboard():
    print("Role:", session.get("role"))
    if "username" not in session:
        return redirect(url_for("auth_bp.login"))
    current_device = request.args.get("device", "__all__")
    devices = db.list_devices()
    for dev in devices:
        dev["tags"] = db.list_tags(dev["id"])
        for tag in dev["tags"]:
            value, ts = get_latest_tag_value(tag["id"])
            tag["value"] = value
            tag["ts"] = ts.strftime("%H:%M") if ts else "--:--"
            tag["alarm_status"] = "Normal"  # Or your logic
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
        
        print(f"Tags: {tags}")
        # socketio.emit("update_tags", {"tags": tags})
        return jsonify({"tags": tags})
    except Exception as e:
        print(f"Error in /api/tags: {e}")
        return jsonify({"error": str(e)}), 500