from flask import render_template,request,jsonify
from . import dashboard_bp
from modbus_monitor.database import db
from modbus_monitor.database.db import get_latest_tag_value
from datetime import datetime
@dashboard_bp.route("/")
@dashboard_bp.route("/dashboard")
def dashboard():
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
        current_device=current_device
    )


@dashboard_bp.route("/api/tags")
def api_tags():
    device_id = request.args.get("device", "__all__")
    result = []
    if device_id == "__all__":
        devices = db.list_devices()
        for dev in devices:
            tags = db.list_tags(dev["id"])
            for tag in tags:
                tag["device_id"] = dev["id"]
                tag["device_name"] = dev["name"]
                result.append(tag)
    else:
        try:
            did = int(device_id)
            tags = db.list_tags(did)
            for tag in tags:
                tag["device_id"] = did
            result = tags
        except Exception:
            result = []
    return jsonify(result)