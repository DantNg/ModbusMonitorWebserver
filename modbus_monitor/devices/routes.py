from flask import render_template, request, redirect, url_for, flash
from . import devices_bp
from modbus_monitor.database.db import (
    add_device_row, list_devices, get_device, list_tags,
    update_device_row, delete_device_row, get_tag, add_tag_row,
    update_tag_row, delete_tag_row
)
from modbus_monitor.services.runner import restart_services
from datetime import datetime

# List Devices (giữ nguyên nếu bạn đã có)
@devices_bp.route("/devices")
def devices():
    items = list_devices()
    return render_template("devices/devices.html", items=items)

# (tuỳ chọn) trang chi tiết để các link "Open/View" không lỗi
@devices_bp.route("/devices/<int:did>")
def device_detail(did):
    dev = get_device(did)
    if not dev:
        flash("Device not found", "warning")
        return redirect(url_for("devices_bp.devices"))
    tags = list_tags(did)
    return render_template("devices/device_detail.html", device=dev, tags=tags)

# ---------- ADD DEVICE ----------
@devices_bp.route("/devices/add", methods=["GET", "POST"])
def add_device():
    """
    Hỗ trợ 2 giao thức:
      - ModbusTCP: name, host, port, unit_id, timeout_ms, description
      - ModbusRTU: name, serial_port, baudrate, parity, stopbits, bytesize, unit_id, timeout_ms, description
    protocol được lấy từ ?protocol=ModbusTCP|ModbusRTU (mặc định ModbusTCP)
    """
    protocol = (request.args.get("protocol") or request.form.get("protocol") or "ModbusTCP").strip()

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        description = (request.form.get("description") or "").strip()

        errors = {}

        if not name:
            errors["name"] = "Name is required."

        # Chuẩn hoá số nguyên an toàn
        def to_int(val, default=None, field=None):
            if val is None or val == "":
                return default
            try:
                return int(val)
            except ValueError:
                if field:
                    errors[field] = "Must be an integer."
                return default

        # Base fields
        unit_id = to_int(request.form.get("unit_id"), 1, "unit_id")
        timeout_ms = to_int(request.form.get("timeout_ms"), 2000, "timeout_ms")
        default_function_code = to_int(request.form.get("default_function_code"), 3, "default_function_code")
        
        # Validate function code
        if default_function_code not in [1, 2, 3, 4]:
            errors["default_function_code"] = "Function code must be 1, 2, 3, or 4."

        data = {
            "name": name,
            "protocol": "ModbusTCP" if protocol == "ModbusTCP" else "ModbusRTU",
            "unit_id": unit_id,
            "timeout_ms": timeout_ms,
            "default_function_code": default_function_code,
            "description": description or None,
        }

        if protocol == "ModbusTCP":
            host = (request.form.get("host") or "").strip()
            port = to_int(request.form.get("port"), 502, "port")
            if not host:
                errors["host"] = "Host is required for ModbusTCP."
            data.update({
                "host": host or None,
                "port": port,
                # RTU fields để None
                "serial_port": None, "baudrate": None, "parity": None,
                "stopbits": None, "bytesize": None
            })

        else:  # ModbusRTU
            serial_port = (request.form.get("serial_port") or "").strip()
            baudrate = to_int(request.form.get("baudrate"), None, "baudrate")
            parity = (request.form.get("parity") or "N").upper()
            stopbits = to_int(request.form.get("stopbits"), None, "stopbits")
            bytesize = to_int(request.form.get("bytesize"), None, "bytesize")

            if not serial_port:
                errors["serial_port"] = "Serial port is required for ModbusRTU."
            if parity not in ("N", "E", "O"):
                errors["parity"] = "Parity must be N, E or O."
            if stopbits not in (1, 2):
                errors["stopbits"] = "Stop bits must be 1 or 2."
            if bytesize not in (7, 8):
                errors["bytesize"] = "Byte size must be 7 or 8."

            data.update({
                "serial_port": serial_port or None,
                "baudrate": baudrate,
                "parity": parity,
                "stopbits": stopbits,
                "bytesize": bytesize,
                # TCP fields để None
                "host": None, "port": None
            })

        # Nếu có lỗi -> render lại form kèm thông báo
        if errors:
            return render_template("devices/device_form.html",
                                   protocol=protocol,
                                   form=request.form,
                                   errors=errors)

        # Insert DB
        new_id = add_device_row(data)
        
        # Restart services to pick up new device
        restart_services()
        
        flash("Device created successfully.", "success")
        return redirect(url_for("devices_bp.device_detail", did=new_id))

    # GET: mở form theo protocol (giữ param ?protocol=)
    return render_template("devices/device_form.html", protocol=protocol)

@devices_bp.route("/devices/<int:did>/tags/add", methods=["GET", "POST"])
def add_tag(did):
    device = get_device(did)
    if not device:
        flash("Device not found", "warning")
        return redirect(url_for("devices_bp.devices"))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        address = request.form.get("address")
        datatype = (request.form.get("datatype") or "Word").strip()
        unit = (request.form.get("unit") or "").strip() or None
        scale = request.form.get("scale") or 1.0
        offset = request.form.get("offset") or 0.0
        grp = (request.form.get("grp") or "Group1").strip()
        function_code = request.form.get("function_code")
        description = (request.form.get("description") or "").strip() or None

        errors = {}
        if not name:
            errors["name"] = "Name is required."
        try:
            address = int(address)
        except Exception:
            errors["address"] = "Address must be an integer."
        
        # Validate function code if provided
        if function_code:
            try:
                function_code = int(function_code)
                if function_code not in [1, 2, 3, 4]:
                    errors["function_code"] = "Function code must be 1, 2, 3, or 4."
            except ValueError:
                errors["function_code"] = "Function code must be a valid integer."
        else:
            function_code = None

        if errors:
            return render_template(
                "devices/tag_form.html",
                device=device,
                errors=errors,
                form=request.form
            )

        add_tag_row(did, {
            "name": name,
            "address": address,
            "datatype": datatype,
            "unit": unit,
            "scale": float(scale),
            "offset": float(offset),
            "grp": grp,
            "function_code": function_code,
            "description": description,
        })
        
        # Restart services to pick up new tag
        restart_services()
        
        flash("Tag added.", "success")
        return redirect(url_for("devices_bp.device_detail", did=did))

    # GET
    return render_template("devices/tag_form.html", device=device)

@devices_bp.route("/devices/<int:did>/edit", methods=["GET", "POST"])
def edit_device(did):
    dev = get_device(did)
    if not dev:
        flash("Device not found.", "warning")
        return redirect(url_for("devices_bp.devices"))

    protocol = dev.get("protocol") or "ModbusTCP"

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        description = (request.form.get("description") or "").strip()

        errors = {}
        if not name:
            errors["name"] = "Name is required."

        def to_int(val, default=None, field=None):
            if val is None or val == "":
                return default
            try:
                return int(val)
            except ValueError:
                if field:
                    errors[field] = "Must be integer."
                return default

        unit_id = to_int(request.form.get("unit_id"), 1, "unit_id")
        timeout_ms = to_int(request.form.get("timeout_ms"), 2000, "timeout_ms")
        default_function_code = to_int(request.form.get("default_function_code"), 3, "default_function_code")
        
        # Validate function code
        if default_function_code not in [1, 2, 3, 4]:
            errors["default_function_code"] = "Function code must be 1, 2, 3, or 4."

        data = {
            "name": name,
            "protocol": protocol,  # không cho đổi protocol trong edit (đơn giản)
            "unit_id": unit_id,
            "timeout_ms": timeout_ms,
            "default_function_code": default_function_code,
            "description": description or None,
        }

        if protocol == "ModbusTCP":
            host = (request.form.get("host") or "").strip()
            port = to_int(request.form.get("port"), 502, "port")
            if not host:
                errors["host"] = "Host is required for ModbusTCP."
            data.update({
                "host": host or None,
                "port": port,
                "serial_port": None, "baudrate": None, "parity": None,
                "stopbits": None, "bytesize": None
            })
        else:
            serial_port = (request.form.get("serial_port") or "").strip()
            baudrate = to_int(request.form.get("baudrate"), None, "baudrate")
            parity = (request.form.get("parity") or "N").upper()
            stopbits = to_int(request.form.get("stopbits"), None, "stopbits")
            bytesize = to_int(request.form.get("bytesize"), None, "bytesize")

            if not serial_port:
                errors["serial_port"] = "Serial port is required for ModbusRTU."
            if parity not in ("N","E","O"):
                errors["parity"] = "Parity must be N, E or O."
            if stopbits not in (1,2):
                errors["stopbits"] = "Stop bits must be 1 or 2."
            if bytesize not in (7,8):
                errors["bytesize"] = "Byte size must be 7 or 8."

            data.update({
                "serial_port": serial_port or None,
                "baudrate": baudrate,
                "parity": parity,
                "stopbits": stopbits,
                "bytesize": bytesize,
                "host": None, "port": None
            })

        if errors:
            # render lại form với dữ liệu hiện tại
            return render_template("devices/device_form.html",
                                   protocol=protocol,
                                   form=request.form,
                                   errors=errors,
                                   editing=True,
                                   device_id=did)

        update_device_row(did, data)
        flash("Device updated.", "success")
        return redirect(url_for("devices_bp.device_detail", did=did))

    # GET: prefill form từ dev
    class F: pass
    f = F()
    for k, v in dev.items():
        setattr(f, k, v)
    return render_template("devices/device_form.html",
                           protocol=protocol,
                           form=f,
                           editing=True,
                           device_id=did)

@devices_bp.route("/devices/<int:did>/delete", methods=["POST"])
def delete_device(did):
    cnt = delete_device_row(did)
    if cnt:
        # Restart services to remove deleted device
        restart_services()
        flash("Device deleted.", "success")
    else:
        flash("Device not found.", "warning")
    return redirect(url_for("devices_bp.devices"))

@devices_bp.route("/devices/<int:did>/tags/<int:tid>/edit", methods=["GET","POST"])
def edit_tag(did, tid):
    device = get_device(did)
    tag = get_tag(tid)
    if not device or not tag or tag["device_id"] != did:
        flash("Tag not found.", "warning")
        return redirect(url_for("devices_bp.device_detail", did=did))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        datatype = (request.form.get("datatype") or "Word").strip()
        unit = (request.form.get("unit") or "").strip() or None
        grp = (request.form.get("grp") or "Group1").strip()
        description = (request.form.get("description") or "").strip() or None

        errors = {}
        try:
            address = int(request.form.get("address"))
        except Exception:
            errors["address"] = "Address must be integer."
            address = None
        try:
            scale = float(request.form.get("scale") or 1.0)
            offset = float(request.form.get("offset") or 0.0)
        except Exception:
            errors["scale"] = "Scale/Offset must be number."

        if not name:
            errors["name"] = "Name is required."

        if errors:
            return render_template("devices/tag_form.html",
                                   device=device, tag=tag, form=request.form,
                                   errors=errors, editing=True)

        update_tag_row(tid, {
            "name": name, "address": address, "datatype": datatype,
            "unit": unit, "scale": scale, "offset": offset,
            "grp": grp, "description": description
        })
        flash("Tag updated.", "success")
        return redirect(url_for("devices_bp.device_detail", did=did))

    # GET: prefill
    class F: pass
    f = F()
    for k, v in tag.items():
        setattr(f, k, v)
    return render_template("devices/tag_form.html",
                           device=device, tag=tag, form=f, editing=True)

@devices_bp.route("/devices/<int:did>/tags/<int:tid>/delete", methods=["POST"])
def delete_tag(did, tid):
    tag = get_tag(tid)
    if not tag or tag["device_id"] != did:
        flash("Tag not found.", "warning")
        return redirect(url_for("devices_bp.device_detail", did=did))
    delete_tag_row(tid)
    
    # Restart services to remove deleted tag
    restart_services()
    
    flash("Tag deleted.", "success")
    return redirect(url_for("devices_bp.device_detail", did=did))

@devices_bp.route("/devices/<int:did>/tags/<int:tid>/write", methods=["POST"])
def write_tag(did, tid):
    """Write a value to a specific tag."""
    from modbus_monitor.services import runner
    
    tag = get_tag(tid)
    if not tag or tag["device_id"] != did:
        return {"success": False, "error": "Tag not found"}, 404
    
    try:
        value = float(request.form.get("value") or request.json.get("value", 0))
    except (ValueError, TypeError):
        return {"success": False, "error": "Invalid value format"}, 400
    
    success = runner.write_tag_value(tid, value)
    
    if success:
        return {"success": True, "message": f"Successfully wrote {value} to {tag['name']}"}
    else:
        return {"success": False, "error": "Failed to write to tag"}, 500

@devices_bp.route("/api/tags/<int:tid>/write", methods=["POST"])
def api_write_tag(tid):
    """API endpoint to write a value to a tag (accepts JSON)."""
    from modbus_monitor.services import runner
    
    tag = get_tag(tid)
    if not tag:
        return {"success": False, "error": "Tag not found"}, 404
    
    data = request.get_json()
    if not data or "value" not in data:
        return {"success": False, "error": "Value is required"}, 400
    
    try:
        value = float(data["value"])
    except (ValueError, TypeError):
        return {"success": False, "error": "Invalid value format"}, 400
    
    success = runner.write_tag_value(tid, value)
    
    if success:
        return {"success": True, "message": f"Successfully wrote {value} to {tag['name']}", "tag_name": tag['name']}
    else:
        return {"success": False, "error": "Failed to write to tag"}, 500