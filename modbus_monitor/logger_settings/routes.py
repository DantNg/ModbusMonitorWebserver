from flask import render_template, request, redirect, url_for, flash
from typing import List as list
from typing import Tuple as tuple
from typing import Optional
from . import logger_settings_bp
from modbus_monitor.database.db import (
    list_data_loggers, get_data_logger, get_data_logger_tag_ids,
    add_data_logger_row, update_data_logger_row, delete_data_logger_row,
    list_all_tags
)
from modbus_monitor.services.runner import restart_services

# Danh sách Data Logger (settings)
@logger_settings_bp.route("/data-logger-settings", endpoint="datalogger_settings")
def data_logger_settings():
    items = list_data_loggers()
    return render_template("data_loggers/data_loggers.html", items=items)

# Add
@logger_settings_bp.route("/data-loggers/add", methods=["GET", "POST"], endpoint="datalogger_add")
def datalogger_add():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        interval_sec = request.form.get("interval_sec") or "60"
        enabled = request.form.get("enabled") in ("1","true","on","yes")
        description = (request.form.get("description") or "").strip() or None

        errors = {}
        if not name:
            errors["name"] = "Name is required."
        try:
            interval_sec = float(interval_sec)
            if interval_sec <= 0:
                errors["interval_sec"] = "Interval must be > 0."
        except Exception:
            errors["interval_sec"] = "Interval must be a valid number."

        # tag_ids: mảng checkbox
        tag_ids = []
        for v in request.form.getlist("tag_ids"):
            try:
                tag_ids.append(int(v))
            except Exception:
                pass

        if errors:
            tags = _tags_for_form()
            flash("; ".join(errors.values()), "danger")
            return render_template("data_loggers/logger_form.html",
                                   form=request.form, tags=tags)

        new_id = add_data_logger_row(
            {"name": name, "interval_sec": interval_sec, "enabled": enabled, "description": description},
            tag_ids
        )
        
        # Restart services to pick up new datalogger
        restart_services()
        
        flash("Data Logger created.", "success")
        return redirect(url_for("logger_settings_bp.datalogger_settings"))

    # GET
    tags = _tags_for_form()
    return render_template("data_loggers/logger_form.html", form={}, tags=tags)

# Edit
@logger_settings_bp.route("/data-loggers/<int:lid>/edit", methods=["GET","POST"])
def datalogger_edit(lid):
    logger = get_data_logger(lid)
    if not logger:
        flash("Data Logger not found.", "warning")
        return redirect(url_for("logger_settings_bp.datalogger_settings"))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        interval_sec = request.form.get("interval_sec") or "60"
        enabled = request.form.get("enabled") in ("1","true","on","yes")
        description = (request.form.get("description") or "").strip() or None

        errors = {}
        if not name:
            errors["name"] = "Name is required."
        try:
            interval_sec = float(interval_sec)
            if interval_sec <= 0:
                errors["interval_sec"] = "Interval must be > 0."
        except Exception:
            errors["interval_sec"] = "Interval must be a valid number."

        tag_ids = []
        for v in request.form.getlist("tag_ids"):
            try:
                tag_ids.append(int(v))
            except Exception:
                pass

        if errors:
            tags = _tags_for_form(selected=get_data_logger_tag_ids(lid))
            flash("; ".join(errors.values()), "danger")
            return render_template("data_loggers/logger_form.html",
                                   form=request.form, tags=tags, editing=True, logger=logger)

        update_data_logger_row(
            lid,
            {"name": name, "interval_sec": interval_sec, "enabled": enabled, "description": description},
            tag_ids
        )
        flash("Data Logger updated.", "success")
        return redirect(url_for("logger_settings_bp.datalogger_settings"))

    # GET: prefill
    selected = get_data_logger_tag_ids(lid)
    tags = _tags_for_form(selected=selected)
    # đưa logger -> form cho tiện prefill
    form = dict(logger)
    form["enabled"] = "on" if logger.get("enabled") else ""
    return render_template("data_loggers/logger_form.html",
                           form=form, tags=tags, editing=True, logger=logger)

# Delete
@logger_settings_bp.route("/data-loggers/<int:lid>/delete", methods=["POST"])
def datalogger_delete(lid):
    cnt = delete_data_logger_row(lid)
    
    if cnt:
        # Restart services to remove deleted datalogger
        restart_services()
    
    flash("Data Logger deleted." if cnt else "Data Logger not found.", "success" if cnt else "warning")
    return redirect(url_for("logger_settings_bp.datalogger_settings"))

# ---------- helpers ----------
def _tags_for_form(selected: Optional[list[int]] = None):
    """
    Trả về danh sách tag có cấu trúc phù hợp với logger_form.html hiện tại:
      { id, name, device: { name } }
    """
    rows = list_all_tags()
    selected = set(selected or [])
    shaped = []
    for r in rows:
        shaped.append({
            "id": r["id"],
            "name": r["tag_name"],
            "device": {"name": r["device_name"]},
            "checked": (r["id"] in selected),
        })
    return shaped
