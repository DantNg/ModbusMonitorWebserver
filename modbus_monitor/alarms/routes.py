from flask import render_template, request, redirect, url_for, flash

from modbus_monitor.database import db
from . import alarms_bp
from modbus_monitor.database.db import (
    list_alarm_report,
    list_all_tags,
    list_alarm_rules, get_alarm_rule,
    add_alarm_rule_row, update_alarm_rule_row, delete_alarm_rule_row,
    list_alarm_events, clear_alarm_events,
    delete_alarm_event_row
)
from flask import jsonify
from datetime import datetime
# /alarms/events/<id>/delete
@alarms_bp.route("/alarms/events/<int:eid>/delete", methods=["POST"])
def delete_alarm_event(eid):
    cnt = delete_alarm_event_row(eid)
    flash("Alarm event deleted." if cnt else "Event not found.", "success" if cnt else "warning")
    return redirect(url_for("alarms_bp.alarm_events"))

@alarms_bp.post("/events/delete-selected")
def delete_selected_alarm_events():
    ids_str = request.form.get("ids")  # "1,2,3"
    if ids_str:
        ids = [int(i) for i in ids_str.split(",") if i.strip()]
        db.delete_alarm_events(ids)
        flash(f"Deleted {len(ids)} alarm events.")
    return redirect(url_for("alarms_bp.alarm_events"))


# /alarms/events (history)
@alarms_bp.route("/alarms/events")
def alarm_events():
    items = list_alarm_events()
    return render_template("alarms/alarm_events.html", items=items)

# /alarms/events/clear
@alarms_bp.route("/alarms/events/clear", methods=["POST"])
def clear_alarm_events_route():
    cnt = clear_alarm_events()
    flash(f"Cleared {cnt} alarm events.", "success")
    return redirect(url_for("alarms_bp.alarm_events"))

# /alarms (history / list)
@alarms_bp.route("/alarms")
def alarms():
    items = list_alarm_rules()
    return render_template("alarms/alarms.html", items=items)

# /alarms/add-rule (form thêm rule)
@alarms_bp.route("/alarms/add-rule", methods=["GET", "POST"])
def add_alarm_rule():
    if request.method == "POST":
        data = _parse_alarm_form(request.form)
        errors = _validate_alarm_form(data)
        print(errors)
        if errors:
            flash("; ".join(errors.values()), "danger")
            tags = list_all_tags()
            return render_template("alarms/alarm_form.html", form=request.form,tags= tags)

        new_id = add_alarm_rule_row(data)
        flash("Alarm created.", "success")
        return redirect(url_for("alarms_bp.alarm_settings"))
    # GET
    tags = list_all_tags()
    return render_template("alarms/alarm_form.html",form={},tags= tags)

# /alarms/<id>/edit
@alarms_bp.route("/alarms/<int:aid>/edit", methods=["GET", "POST"])
def edit_alarm_rule(aid):
    alarm = get_alarm_rule(aid)
    if not alarm:
        flash("Alarm not found.", "warning")
        return redirect(url_for("alarms_bp.alarms"))

    if request.method == "POST":
        data = _parse_alarm_form(request.form)
        errors = _validate_alarm_form(data)
        if errors:
            flash("; ".join(errors.values()), "danger")
            # Change this line:
            # return render_template("alarms/alarm_form.html", form=request.form, editing=True, alarm=alarm)
            # To:
            return render_template("alarms/alarm_form.html", form=dict(request.form), editing=True, alarm=alarm)
        update_alarm_rule_row(aid, data)
        flash("Alarm updated.", "success")
        return redirect(url_for("alarms_bp.alarm_events"))

    # GET: prefill
    class F: pass
    f = F()
    for k, v in alarm.items():
        setattr(f, k, v)
    tags = list_all_tags()
    return render_template("alarms/alarm_form.html", form=alarm,tags = tags, editing=True, alarm=alarm)

# /alarms/<id>/delete
@alarms_bp.route("/alarms/<int:aid>/delete", methods=["POST"])
def delete_alarm_rule(aid):
    cnt = delete_alarm_rule_row(aid)
    flash("Alarm deleted." if cnt else "Alarm not found.", "success" if cnt else "warning")
    return redirect(url_for("alarms_bp.alarm_settings"))

# /alarm-settings (nếu bạn dùng trang cấu hình riêng)
@alarms_bp.route("/alarm-settings")
def alarm_settings():
    items = list_alarm_rules()
    return render_template("alarms/alarm_settings.html", items=items)


@alarms_bp.route("/api/alarm-report")
def alarm_report():
    items = list_alarm_report()
    print(items)
    return jsonify({"items": items})

# --------- helpers ----------
def _parse_alarm_form(f):
    # normalise operator from html entities / unicode
    raw_op = (f.get("operator") or "").strip()
    print(raw_op)
    op = (raw_op
          .replace("&gt;=", ">=")
          .replace("&lt;=", "<=")
          .replace("&gt;", ">")
          .replace("&lt;", "<")
          .replace("≥", ">=")
          .replace("≤", "<=")
          )

    return {
        "enabled": (f.get("enabled") in ("1", "true", "on", "yes")),
        "code": (f.get("code") or "").strip() or None,
        "name": (f.get("name") or "").strip() or None,
        "level": (f.get("level") or "High").strip(),
        # Lưu tag_id là chuỗi (sau này convert sang int khi evaluate)
        "target": (f.get("target") or "").strip(),
        "operator": op,
        "threshold": (f.get("threshold") or "").strip(),
        "on_stable_sec": _to_int(f.get("on_stable"), 0),  # Fixed: get "on_stable" from form
        "off_stable_sec": _to_int(f.get("off_stable"), 0), # Fixed: get "off_stable" from form
        "email": (f.get("email") or "").strip(),  # Correct field name
        "sms": (f.get("sms") or "").strip(),   
    }

def _validate_alarm_form(data: dict):
    errors = {}
    if not data["target"]:
        errors["target"] = "Target is required."
    if not data["operator"]:
        errors["operator"] = "Operator is required."
    if data["threshold"] == "":
        errors["threshold"] = "Threshold is required."

    allowed = {">","<",">=","<=","==","!="}
    if data["operator"] not in allowed:
        errors["operator"] = "Operator must be one of >,<,>=,<=,==,!=."
    if data["level"] not in {"Low","Medium","High","Critical"}:
        errors["level"] = "Level must be one of Low/Medium/High/Critical."
    return errors

def _to_int(s, default=None):
    try:
        return int(s) if s not in (None, "") else default
    except Exception:
        return default
@alarms_bp.route("/api/alarm-events")
def api_alarm_events():
    items = list_alarm_events()  # Should return a list of dicts
    # Optionally format datetime
    for e in items:
        if isinstance(e["ts"], datetime):
            e["ts"] = e["ts"].strftime("%Y-%m-%d %H:%M:%S")
    return jsonify({"items": items})