from flask import jsonify, render_template, request, redirect, url_for, flash, session
from . import reports_bp
from datetime import datetime,timedelta
from modbus_monitor.database import db
from modbus_monitor.database.db import safe_datetime_now, get_data_logger, get_data_logger_tag_ids, get_tag_values_with_interval

# Import shared formatting để format giá trị trên BE (single source of truth)
_FORMATTER_AVAILABLE = False
try:
    from shared.formatting import format_tag_value, TagFormatMetadata as _TagFmtMeta
    _FORMATTER_AVAILABLE = True
except ImportError:
    pass


def _format_report_items(items: list, tag_meta_full: dict) -> list:
    """Format tất cả giá trị số trong report items theo shared/formatting rules.
    Tag values trong DB là raw float, cần format đúng số thập phân trước khi gửi FE.

    QUAN TRỌNG: mọi giá trị số phải được gửi về FE dưới dạng STRING. Nếu để dạng
    JSON number, trailing zero bị mất khi JS gọi String(89.0) -> "89" (mất ".0").
    Vì vậy ngay cả khi không có metadata / formatter, vẫn ép str(val) để giữ nguyên
    biểu diễn thập phân.
    """
    for item in items:
        for col, val in item.items():
            if col == 'timestamp' or val is None or val == '':
                continue
            meta = tag_meta_full.get(col) if _FORMATTER_AVAILABLE else None
            if not meta:
                # Fallback: giữ nguyên biểu diễn thập phân dạng string (str(89.0) == "89.0")
                item[col] = str(val)
                continue
            try:
                fmt_meta = _TagFmtMeta(
                    tag_id=0,
                    scale=float(meta['scale']),
                    offset=0.0,   # đã áp tại nguồn (worker), không áp lại
                    unit=str(meta['unit']),
                    datatype=str(meta['datatype']),
                )
                item[col] = format_tag_value(float(val), fmt_meta).display_text
            except Exception:
                item[col] = str(val)
    return items

@reports_bp.get("/reports")
def reports():
    loggers = db.list_data_loggers()
    logger_arg = request.args.get("logger") or (loggers[0]["id"] if loggers else 0)

    if logger_arg == "all":
        current_logger_id = "all"
        current_logger_name = "ALL DATALOGGERS"
    else:
        try:
            current_logger_id = int(logger_arg)
        except Exception:
            current_logger_id = loggers[0]["id"] if loggers else 0
        current_logger_name = next((l["name"] for l in loggers if l["id"] == current_logger_id), "Logger")

    # Handle time filter like alarm system
    time_filter = request.args.get("time_filter", "today")
    now = safe_datetime_now()
    
    if time_filter == "today":
        from_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        to_dt = now
    elif time_filter == "yesterday":
        yesterday = now - timedelta(days=1)
        from_dt = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        to_dt = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif time_filter == "last7days":
        from_dt = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        to_dt = now
    elif time_filter == "last30days":
        from_dt = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
        to_dt = now
    elif time_filter == "custom":
        from_dt = _parse_dt(request.args.get("from"))
        to_dt = _parse_dt(request.args.get("to"))
        if not to_dt:
            to_dt = now
        if not from_dt:
            from_dt = now - timedelta(days=1)
    else:  # "all"
        from_dt = now - timedelta(days=365)  # Last year
        to_dt = now
    print(f"Reports: Time filter {time_filter}, from {from_dt} to {to_dt}")

    # Only get columns for initial page load, no data
    if current_logger_id == "all":
        _, columns, _ = db.get_all_datalogger_data(dt_from=from_dt, dt_to=to_dt, limit=1)
    else:
        _, columns, _ = db.get_datalogger_data(current_logger_id, dt_from=from_dt, dt_to=to_dt, limit=1)

    if "timestamp" not in columns:
        columns = ["timestamp"] + [c for c in columns if c != "timestamp"]
    
    print(f"Reports: Initial page load for logger {current_logger_id}")
    return render_template(
        "reports/reports.html",
        loggers=loggers,
        current_logger_id=current_logger_id,
        current_logger_name=current_logger_name,
        time_filter=time_filter,
        from_dt=from_dt.strftime("%d/%m/%Y %H:%M") if time_filter == "custom" else "",
        to_dt=to_dt.strftime("%d/%m/%Y %H:%M") if time_filter == "custom" else "",
        columns=columns,
        items=[]  # No initial data, will be loaded via API
    )


@reports_bp.get("/reports/api/data")
def get_reports_data():
    """API endpoint for paginated report data"""
    loggers = db.list_data_loggers()
    logger_arg = request.args.get("logger") or (loggers[0]["id"] if loggers else 0)
    
    if logger_arg == "all":
        current_logger_id = "all"
    else:
        try:
            current_logger_id = int(logger_arg)
        except Exception:
            current_logger_id = loggers[0]["id"] if loggers else 0

    # Get pagination params
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 100))
    
    # Handle time filter
    time_filter = request.args.get("time_filter", "today")
    now = safe_datetime_now()
    
    if time_filter == "today":
        from_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        to_dt = now
    elif time_filter == "yesterday":
        yesterday = now - timedelta(days=1)
        from_dt = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        to_dt = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif time_filter == "last7days":
        from_dt = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        to_dt = now
    elif time_filter == "last30days":
        from_dt = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
        to_dt = now
    elif time_filter == "custom":
        from_dt = _parse_dt(request.args.get("from"))
        to_dt = _parse_dt(request.args.get("to"))
        if not to_dt:
            to_dt = now
        if not from_dt:
            from_dt = now - timedelta(days=1)
    else:  # "all"
        from_dt = now - timedelta(days=365)
        to_dt = now

    try:
        if current_logger_id == "all":
            items, columns, total_count = db.get_all_datalogger_data(dt_from=from_dt, dt_to=to_dt, offset=offset, limit=limit)
            tag_meta_full = db.get_all_logger_tag_full_metadata()
        else:
            items, columns, total_count = db.get_datalogger_data(current_logger_id, dt_from=from_dt, dt_to=to_dt, offset=offset, limit=limit)
            tag_meta_full = db.get_logger_tag_full_metadata(current_logger_id)

        # Format giá trị trên BE — FE chỉ hiển thị string đã được format
        items = _format_report_items(items, tag_meta_full)

        print(f"Reports API: Loaded {len(items)} rows of {total_count} total (offset={offset}, limit={limit})")
        return jsonify({
            "success": True,
            "items": items,
            "columns": columns,
            "offset": offset,
            "limit": limit,
            "total": total_count,
            "has_more": offset + len(items) < total_count
        })
    except Exception as e:
        print(f"Error loading report data: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


def _parse_dt(s):
    """Parse datetime string - hỗ trợ nhiều format:
    - dd/mm/yyyy HH:MM (flatpickr format)
    - YYYY-MM-DDTHH:MM (datetime-local format cũ)
    - YYYY-MM-DD HH:MM
    """
    if not s:
        return None
    s = s.strip()
    # Thử từng format theo thứ tự ưu tiên
    formats = [
        "%d/%m/%Y %H:%M",    # dd/mm/yyyy HH:MM (flatpickr)
        "%d/%m/%Y %H:%M:%S", # dd/mm/yyyy HH:MM:SS
        "%Y-%m-%dT%H:%M",    # ISO datetime-local (cũ)
        "%Y-%m-%dT%H:%M:%S", # ISO với giây
        "%Y-%m-%d %H:%M",    # YYYY-MM-DD HH:MM
        "%Y-%m-%d %H:%M:%S", # YYYY-MM-DD HH:MM:SS
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None
# Giữ tương thích: /data-loggers -> redirect sang /reports
@reports_bp.route("/data-loggers", endpoint="data_loggers")
def data_loggers_alias():
    return redirect(url_for("reports_bp.reports"))

@reports_bp.post("/clear_all_data")
def clear_all_report_data():
    """Clear all report data - admin only"""
    if session.get("role") != "admin":
        flash("Access denied. Admin role required.", "error")
        return redirect(url_for("reports_bp.reports"))
    
    try:
        deleted_count = db.clear_report_data()
        flash(f"Successfully cleared {deleted_count} report records.", "success")
    except Exception as e:
        flash(f"Error clearing report data: {str(e)}", "error")
    
    return redirect(url_for("reports_bp.reports"))

@reports_bp.post("/clear_logger_data/<int:logger_id>")
def clear_logger_report_data(logger_id: int):
    """Clear report data for specific logger - admin only"""
    print(f"Request to clear data for logger ID {logger_id} with user role {session.get('role')}")
    if session.get("role") != "admin":
        flash("Access denied. Admin role required.", "error")
        return redirect(url_for("reports_bp.reports"))
    
    try:
        deleted_count = db.clear_report_data_by_logger(logger_id)
        logger_name = next((l["name"] for l in db.list_data_loggers() if l["id"] == logger_id), f"Logger {logger_id}")
        flash(f"Successfully cleared {deleted_count} records for {logger_name}.", "success")
    except Exception as e:
        flash(f"Error clearing logger data: {str(e)}", "error")
    
    return redirect(url_for("reports_bp.reports", logger=logger_id))


# ===== Comparison Config API Endpoints =====

@reports_bp.post("/api/comparison-config/save")
def save_comparison_config():
    """Save user's comparison configuration to database - admin only"""
    try:
        user_id = session.get("user_id")
        if not user_id:
            return jsonify({"success": False, "error": "User not logged in"}), 401
        
        # Only admin can save comparison config
        if session.get("role") != "admin":
            return jsonify({"success": False, "error": "Only admin can modify comparison config"}), 403
        
        data = request.get_json()
        if not data or "config" not in data:
            return jsonify({"success": False, "error": "No config provided"}), 400
        
        # Lấy logger_id từ request (mặc định là 'all')
        logger_id = data.get("logger_id", "all")
        
        import json
        config_json = json.dumps(data["config"])
        
        success = db.save_user_comparison_config(user_id, logger_id, config_json)
        
        if success:
            return jsonify({"success": True, "message": "Config saved successfully"})
        else:
            return jsonify({"success": False, "error": "Failed to save config"}), 500
            
    except Exception as e:
        print(f"Error saving comparison config: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@reports_bp.get("/api/comparison-config/load")
def load_comparison_config():
    """Load comparison configuration from database.
    User role always loads admin's config."""
    try:
        user_id = session.get("user_id")
        role = session.get("role", "user")
        if not user_id:
            return jsonify({"success": False, "error": "User not logged in"}), 401
        
        # Lấy logger_id từ query parameter (mặc định là 'all')
        logger_id = request.args.get("logger_id", "all")
        print(f"[comparison-load] user_id={user_id}, role={role}, logger_id={logger_id}")
        
        config_json = None
        
        if role == "admin":
            # Admin loads their own config
            config_json = db.get_user_comparison_config(user_id, logger_id)
            print(f"[comparison-load] Admin config found: {config_json is not None}")
        else:
            # User role always loads admin's comparison config
            admin_users = [u for u in db.list_users() if u.get("role") == "admin"]
            print(f"[comparison-load] Found {len(admin_users)} admin users")
            for admin_user in admin_users:
                config_json = db.get_user_comparison_config(admin_user["id"], logger_id)
                print(f"[comparison-load] Checking admin id={admin_user['id']}, logger={logger_id}, found={config_json is not None}")
                if config_json:
                    break
        
        if config_json:
            import json
            config = json.loads(config_json)
            print(f"[comparison-load] Returning {len(config)} comparison pairs")
            return jsonify({"success": True, "config": config})
        else:
            print(f"[comparison-load] No config found, returning empty")
            return jsonify({"success": True, "config": []})
            
    except Exception as e:
        print(f"Error loading comparison config: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@reports_bp.delete("/api/comparison-config/delete")
def delete_comparison_config():
    """Delete comparison configuration from database - admin only"""
    try:
        user_id = session.get("user_id")
        if not user_id:
            return jsonify({"success": False, "error": "User not logged in"}), 401
        
        # Only admin can delete comparison config
        if session.get("role") != "admin":
            return jsonify({"success": False, "error": "Only admin can modify comparison config"}), 403
        
        # Lấy logger_id từ query parameter (mặc định là 'all')
        logger_id = request.args.get("logger_id", "all")
        
        success = db.delete_user_comparison_config(user_id, logger_id)
        
        if success:
            return jsonify({"success": True, "message": "Config deleted successfully"})
        else:
            return jsonify({"success": True, "message": "No config to delete"})
            
    except Exception as e:
        print(f"Error deleting comparison config: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

