from flask import jsonify, render_template, request, redirect, url_for, flash, session
from . import reports_bp
from datetime import datetime,timedelta
from modbus_monitor.database import db
from modbus_monitor.database.db import safe_datetime_now

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
    if current_logger_id == "all":
        # mới: lấy dữ liệu cho tất cả logger
        items, columns = db.get_all_logger_rows(dt_from=from_dt, dt_to=to_dt)
    else:
        items, columns = db.get_logger_rows(current_logger_id, from_dt, to_dt)

    if "timestamp" not in columns:
        columns = ["timestamp"] + [c for c in columns if c != "timestamp"]
    print(f"Reports: Loaded {len(items)} rows for logger {current_logger_id} from {from_dt} to {to_dt}")
    return render_template(
        "reports/reports.html",
        loggers=loggers,
        current_logger_id=current_logger_id,
        current_logger_name=current_logger_name,
        time_filter=time_filter,
        from_dt=from_dt.strftime("%Y-%m-%dT%H:%M") if time_filter == "custom" else "",
        to_dt=to_dt.strftime("%Y-%m-%dT%H:%M") if time_filter == "custom" else "",
        columns=columns,
        items=items
    )


def _parse_dt(s):
    """nhận 'YYYY-MM-DDTHH:MM' -> datetime hoặc None"""
    if not s: 
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M")
    except Exception:
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

