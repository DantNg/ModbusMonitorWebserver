from flask import jsonify, render_template, request, redirect, url_for
from . import reports_bp
from datetime import datetime,timedelta
from modbus_monitor.database import db

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

    from_dt = _parse_dt(request.args.get("from"))
    to_dt   = _parse_dt(request.args.get("to"))
    now = datetime.now()
    if not to_dt:
        to_dt = now
    if not from_dt:
        from_dt = now - timedelta(days=1)

    if current_logger_id == "all":
        # mới: lấy dữ liệu cho tất cả logger
        items, columns = db.get_all_logger_rows(dt_from=from_dt, dt_to=to_dt)
    else:
        items, columns = db.get_logger_rows(current_logger_id, from_dt, to_dt)

    if "timestamp" not in columns:
        columns = ["timestamp"] + [c for c in columns if c != "timestamp"]

    return render_template(
        "reports/reports.html",
        loggers=loggers,
        current_logger_id=current_logger_id,
        current_logger_name=current_logger_name,
        from_dt=from_dt.strftime("%Y-%m-%dT%H:%M"),
        to_dt=to_dt.strftime("%Y-%m-%dT%H:%M"),
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

