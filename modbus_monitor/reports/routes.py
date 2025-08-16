from flask import render_template, request, redirect, url_for
from . import reports_bp
from datetime import datetime,timedelta
from modbus_monitor.database import db

@reports_bp.get("/reports")
def reports():
    # 1) lấy danh sách logger từ DB
    loggers = db.list_data_loggers()  # trả về [{"id":1,"name":"DataLogger1"}, ...]

    # 2) id logger đang chọn
    try:
        current_logger_id = int(request.args.get("logger") or (loggers[0]["id"] if loggers else 0))
    except Exception:
        current_logger_id = loggers[0]["id"] if loggers else 0

    current_logger_name = next((l["name"] for l in loggers if l["id"] == current_logger_id), "Logger")

    # 3) time range
    from_dt = request.args.get("from") or ""
    to_dt   = request.args.get("to") or ""
    # chuyển về datetime để truy vấn (tuỳ DB của bạn)
    dt_from = _parse_dt(from_dt)
    dt_to   = _parse_dt(to_dt)
    now = datetime.now()
    if not dt_to:
        dt_to = now
        to_dt = dt_to.strftime("%Y-%m-%dT%H:%M")
    if not dt_from:
        dt_from = now - timedelta(days=1)
        from_dt = dt_from.strftime("%Y-%m-%dT%H:%M")
    # 4) lấy dữ liệu logger từ DB
    # db.get_logger_rows phải trả về list[dict], có "timestamp" + các cột
    items, columns = db.get_logger_rows(current_logger_id, dt_from, dt_to)
    # đảm bảo "timestamp" là cột đầu
    if "timestamp" not in columns:
        columns = ["timestamp"] + [c for c in columns if c != "timestamp"]
    print(f"Current Logger ID: {current_logger_id}")
    print(f"Items returned: {len(items)}")
    print(f"Columns: {columns}")


    return render_template(
        "reports/reports.html",
        loggers=loggers,
        current_logger_id=current_logger_id,
        current_logger_name=current_logger_name,
        from_dt=from_dt, to_dt=to_dt,
        columns=columns, items=items
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
