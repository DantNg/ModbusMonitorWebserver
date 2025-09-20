from flask import render_template, request, redirect, url_for, flash, jsonify
from . import devices_bp
from modbus_monitor.database.db import (
    list_devices, list_tags  # Keep these for now as fallback
)
from modbus_monitor.services.runner import restart_services
from modbus_monitor.services.config_cache import get_config_cache
from datetime import datetime
import time

# Get config cache instance
config_cache = get_config_cache()

# List Devices (using cache)
@devices_bp.route("/devices")
def devices():
    # Use cache for listing devices
    cached_devices = config_cache.get_all_devices()
    device_statuses = config_cache.get_all_device_statuses()
    
    items = []
    for device in cached_devices.values():
        status_info = device_statuses.get(device.id, {})
        status = status_info.get("status", "unknown")
        last_seen = status_info.get("last_seen")
        
        # Determine online status based on status and last_seen
        is_online = None
        if status == "connected":
            # Consider online if last seen within 30 seconds
            if last_seen and (time.time() - last_seen) < 30:
                is_online = True
            else:
                is_online = False
        elif status == "disconnected":
            is_online = False
        
        items.append({
            **device.__dict__,
            "created_at": datetime.now(),  # Mock for template compatibility
            "updated_at": datetime.now(),
            "is_online": is_online,
            "status": status,
            "last_seen": last_seen
        })
    
    # Fallback to DB if cache is empty
    if not items:
        items = list_devices()
    
    return render_template("devices/devices.html", items=items)

# Device detail (using cache)
@devices_bp.route("/devices/<int:did>")
def device_detail(did):
    # Use cache first
    dev = config_cache.get_device(did)
    if dev:
        dev_dict = dev.__dict__.copy()
        dev_dict.update({
            "created_at": datetime.now(),  # Mock for template compatibility
            "updated_at": datetime.now(),
            "is_online": False  # Mock for template compatibility
        })
    else:
        # Fallback to DB
        dev_dict = config_cache.get_device(did)
        if not dev_dict:
            flash("Device not found", "warning")
            return redirect(url_for("devices_bp.devices"))
    
    # Get tags from cache
    cached_tags = config_cache.get_device_tags(did)
    tags = [tag.__dict__ for tag in cached_tags] if cached_tags else list_tags(did)
    
    return render_template("devices/device_detail.html", device=dev_dict, tags=tags)

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
            byte_order = (request.form.get("byte_order") or "BigEndian").strip()
            
            if not host:
                errors["host"] = "Host is required for ModbusTCP."
            if byte_order not in ("BigEndian", "LittleEndian"):
                errors["byte_order"] = "Byte order must be BigEndian or LittleEndian."
                
            data.update({
                "host": host or None,
                "port": port,
                "byte_order": byte_order,
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
            byte_order = (request.form.get("byte_order") or "BigEndian").strip()

            if not serial_port:
                errors["serial_port"] = "Serial port is required for ModbusRTU."
            if parity not in ("N", "E", "O"):
                errors["parity"] = "Parity must be N, E or O."
            if stopbits not in (1, 2):
                errors["stopbits"] = "Stop bits must be 1 or 2."
            if bytesize not in (7, 8):
                errors["bytesize"] = "Byte size must be 7 or 8."
            if byte_order not in ("BigEndian", "LittleEndian"):
                errors["byte_order"] = "Byte order must be BigEndian or LittleEndian."

            data.update({
                "serial_port": serial_port or None,
                "baudrate": baudrate,
                "parity": parity,
                "stopbits": stopbits,
                "bytesize": bytesize,
                "byte_order": byte_order,
                # TCP fields để None
                "host": None, "port": None
            })

        # Nếu có lỗi -> render lại form kèm thông báo
        if errors:
            return render_template("devices/device_form.html",
                                   protocol=protocol,
                                   form=request.form,
                                   errors=errors)

        # Insert to cache and DB
        new_id = config_cache.add_device(data)
        if not new_id:
            flash("Failed to create device.", "error")
            return render_template("devices/device_form.html",
                                   protocol=protocol,
                                   form=request.form,
                                   errors={"general": "Database error"})
        
        # Restart services to pick up new device
        restart_services()
        
        flash("Device created successfully.", "success")
        return redirect(url_for("devices_bp.device_detail", did=new_id))

    # GET: mở form theo protocol (giữ param ?protocol=)
    return render_template("devices/device_form.html", protocol=protocol)

@devices_bp.route("/devices/<int:did>/tags/add", methods=["GET", "POST"])
def add_tag(did):
    device = config_cache.get_device(did)
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

        tag_data = {
            "name": name,
            "address": address,
            "datatype": datatype,
            "unit": unit,
            "scale": float(scale),
            "offset": float(offset),
            "grp": grp,
            "function_code": function_code,
            "description": description,
        }
        
        # Add tag to cache and DB
        new_tag_id = config_cache.add_tag(did, tag_data)
        if not new_tag_id:
            flash("Failed to add tag.", "error")
            return render_template(
                "devices/tag_form.html",
                device=device,
                errors={"general": "Database error"},
                form=request.form
            )
        
        # Force reload configs instead of restarting services
        try:
            from modbus_monitor.services.runner import get_modbus_service
            modbus_service = get_modbus_service()
            if modbus_service:
                modbus_service.reload_configs()
                
            # Also force reload config cache for immediate effect
            config_cache.reload_configs()
            
            # Force invalidate subdashboard cache to ensure new tags appear
            config_cache.invalidate_subdashboard_cache()
            
            flash("Tag added and configs reloaded.", "success")
        except Exception as e:
            flash(f"Tag added but config reload failed: {e}", "warning")
        
        return redirect(url_for("devices_bp.device_detail", did=did))

    # GET
    return render_template("devices/tag_form.html", device=device)

@devices_bp.route("/devices/<int:did>/edit", methods=["GET", "POST"])
def edit_device(did):
    dev = config_cache.get_device(did)
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

        # Update device in cache and DB
        if not config_cache.update_device(did, data):
            flash("Failed to update device.", "error")
            return render_template("devices/device_form.html",
                                   protocol=protocol,
                                   form=request.form,
                                   errors={"general": "Database error"},
                                   editing=True,
                                   device_id=did)
        
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
    if config_cache.delete_device(did):
        # Restart services to remove deleted device
        restart_services()
        flash("Device deleted.", "success")
    else:
        flash("Device not found.", "warning")
    return redirect(url_for("devices_bp.devices"))

@devices_bp.route("/devices/<int:did>/tags/<int:tid>/edit", methods=["GET","POST"])
def edit_tag(did, tid):
    device = config_cache.get_device(did)
    tag = config_cache.get_tag(tid)
    if not device or not tag or tag.device_id != did:
        flash("Tag not found.", "warning")
        return redirect(url_for("devices_bp.device_detail", did=did))

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        datatype = (request.form.get("datatype") or "Word").strip()
        unit = (request.form.get("unit") or "").strip() or None
        grp = (request.form.get("grp") or "Group1").strip()
        description = (request.form.get("description") or "").strip() or None
        function_code = request.form.get("function_code")

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

        if not name:
            errors["name"] = "Name is required."

        if errors:
            return render_template("devices/tag_form.html",
                                   device=device, tag=tag, form=request.form,
                                   errors=errors, editing=True)

        tag_data = {
            "name": name, "address": address, "datatype": datatype,
            "unit": unit, "scale": scale, "offset": offset,
            "grp": grp, "function_code": function_code, "description": description
        }
        
        # Update tag in cache and DB
        if not config_cache.update_tag(tid, tag_data):
            flash("Failed to update tag.", "error")
            return render_template("devices/tag_form.html",
                                   device=device, tag=tag, form=request.form,
                                   errors={"general": "Database error"}, editing=True)
        
        # Restart services to pick up tag changes
        restart_services()
        
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
    tag = config_cache.get_tag(tid)
    if not tag or tag.device_id != did:
        flash("Tag not found.", "warning")
        return redirect(url_for("devices_bp.device_detail", did=did))
    
    if config_cache.delete_tag(tid):
        # Restart services to remove deleted tag
        restart_services()
        flash("Tag deleted.", "success")
    else:
        flash("Failed to delete tag.", "error")
    
    return redirect(url_for("devices_bp.device_detail", did=did))

@devices_bp.route("/devices/<int:did>/tags/<int:tid>/write", methods=["POST"])
def write_tag(did, tid):
    """Write a value to a specific tag."""
    from modbus_monitor.services import runner
    
    tag = config_cache.get_tag(tid)
    if not tag or tag.device_id != did:
        return {"success": False, "error": "Tag not found"}, 404
    
    try:
        value = float(request.form.get("value") or request.json.get("value", 0))
    except (ValueError, TypeError):
        return {"success": False, "error": "Invalid value format"}, 400
    
    success = runner.write_tag_value(tid, value)
    
    if success:
        return {"success": True, "message": f"Successfully wrote {value} to {tag.name}"}
    else:
        return {"success": False, "error": "Failed to write to tag"}, 500

@devices_bp.route("/api/tags/<int:tid>/write", methods=["POST"])
def api_write_tag(tid):
    """API endpoint to write a value to a tag (accepts JSON)."""
    from modbus_monitor.services import runner
    
    tag = config_cache.get_tag(tid)
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
        return {"success": True, "message": f"Successfully wrote {value} to {tag.name}", "tag_name": tag.name}
    else:
        return {"success": False, "error": "Failed to write to tag"}, 500

@devices_bp.route("/api/devices/status", methods=["GET"])
def get_devices_status():
    """API endpoint to get all device status information"""
    try:
        statuses = config_cache.get_all_device_statuses()
        return jsonify({"success": True, "devices": statuses})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@devices_bp.route("/api/devices/<int:device_id>/status", methods=["GET"])  
def get_device_status(device_id):
    """API endpoint to get specific device status"""
    try:
        status = config_cache.get_device_status(device_id)
        return jsonify({"success": True, "device": status})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500