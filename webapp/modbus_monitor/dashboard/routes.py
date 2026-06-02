from flask import render_template,request,jsonify,session,redirect,url_for,flash
from . import dashboard_bp
from ..database import db
from ..database.db import (
    get_latest_tag_value,
    get_latest_tag_values_batch,
    safe_datetime_now,
    get_notifications,
    get_user_by_username,
    mark_user_notifications_read,
    # new dismiss function
    mark_user_notifications_dismissed,
)
from datetime import datetime, timedelta
from ..extensions import socketio

# Import shared formatting để tạo display_value nhất quán với workers
try:
    from shared.formatting import format_tag_value as _fmt_tag_value, TagFormatMetadata as _TagFmtMeta
    _FORMATTER_AVAILABLE = True
except ImportError:
    _FORMATTER_AVAILABLE = False

@dashboard_bp.route("/dashboard")
def dashboard():
    # Thêm logic để track user access
    print("=== USER ACCESSING DASHBOARD ===")
    print(f"Username: {session.get('username')}")
    print(f"User ID: {session.get('user_id')}")
    print(f"Role: {session.get('role')}")
    print("================================")
    
    if "username" not in session:
        return redirect(url_for("auth_bp.login"))
    
    user = get_user_by_username(session.get("username"))
    notifications = get_notifications(user['id'])
    print(f"User notifications: {notifications}")
    
    # Kiểm tra xem có subdashboards không
    subdashboards = db.list_subdashboards() if hasattr(db, "list_subdashboards") else []
    
    # Nếu có subdashboard, redirect đến subdashboard đầu tiên
    if subdashboards and len(subdashboards) > 0:
        first_subdash = subdashboards[0]
        # Lưu notifications vào session để truyền sang subdashboard
        session['notifications_count'] = len([n for n in notifications if not n.get('is_read', False)])
        return redirect(url_for("subdash_bp.subdash_detail", sid=first_subdash["id"]))
    
    # Nếu không có subdashboard, hiển thị page hướng dẫn với notifications
    return render_template("dashboard_guide.html", notifications=notifications)

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

            # Tạo display_value qua shared/formatting (nhất quán với workers)
            if _FORMATTER_AVAILABLE and value is not None:
                try:
                    _meta = _TagFmtMeta(
                        tag_id=tag_id,
                        scale=float(tag.get("scale") or 1.0),
                        offset=float(tag.get("offset") or 0.0),
                        unit=tag.get("unit") or "",
                        datatype=tag.get("datatype") or "unsigned",
                    )
                    display_value = _fmt_tag_value(float(value), _meta).display_text
                except Exception:
                    display_value = str(value) if value is not None else "--"
            else:
                display_value = str(value) if value is not None else "--"

            tags.append({
                "id": tag_id,
                "name": tag["name"],
                "value": value,
                "display_value": display_value,
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

@dashboard_bp.route("/api/notifications")
def api_notifications():
    """API endpoint to get user notifications"""
    try:
        if "username" not in session:
            return jsonify([])
        
        user = get_user_by_username(session.get("username"))
        notifications = get_notifications(user['id'])
        
        # Format notifications for frontend
        formatted_notifications = []
        for notification in notifications:
            created_at = notification.get("created_at")
            # Ensure datetime is serialized to ISO string
            if hasattr(created_at, "isoformat"):
                created_at = created_at.isoformat()

            formatted_notifications.append({
                "id": notification.get("id") or notification.get("notification_id"),
                "title": notification.get("title") or notification.get("name", "Notification"),
                "message": notification.get("message") or notification.get("note", ""),
                "level": notification.get("level", "Medium"),
                "created_at": created_at,
                "is_read": notification.get("is_read", False) or (notification.get("status") != "unread"),
                "type": notification.get("type", "system"),
                "tag_name": notification.get("tag_name") or notification.get("target"),
                # provide alarm event id and tag id if present to help deduplicate client-side
                "alarm_event_id": notification.get("alarm_event_id"),
                "tag_id": notification.get("tag_id"),
                "event_type": notification.get("event_type", "INCOMING"),
            })
        
        return jsonify(formatted_notifications)
    except Exception as e:
        print(f"Error in /api/notifications: {e}")
        return jsonify({"error": str(e)}), 500

@dashboard_bp.route("/api/notifications/mark-read", methods=["POST"])
def api_mark_notifications_read():
    """API endpoint to mark user notifications as read"""
    try:
        if "username" not in session:
            return jsonify({"success": True, "count": 0})
        
        user = get_user_by_username(session.get("username"))
        
        # Gọi function để đánh dấu tất cả notifications của user đã đọc
        db.mark_user_notifications_read(user['id'])
        
        return jsonify({"success": True})
    except Exception as e:
        print(f"Error marking notifications as read: {e}")
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/api/notifications/clear", methods=["POST"])
def api_clear_notifications():
    """Dismiss all notifications for the current user (won't be returned next fetch)."""
    try:
        if "username" not in session:
            return jsonify({"success": True, "count": 0})

        user = get_user_by_username(session.get("username"))
        db.mark_user_notifications_dismissed(user['id'])
        return jsonify({"success": True})
    except Exception as e:
        print(f"Error clearing notifications: {e}")
        return jsonify({"error": str(e)}), 500