from flask import render_template, request, redirect, url_for, flash, jsonify, current_app
from . import devices_bp
from modbus_monitor.database.db import (
    list_devices, list_tags  # Keep these for now as fallback
)
from modbus_monitor.services.config_cache import get_config_cache
from datetime import datetime
import time
from types import SimpleNamespace
# Get config cache instance
config_cache = get_config_cache()

def get_process_manager():
    """Get ProcessManager instance from app context (disabled in webapp-only mode)"""
    return None  # Always return None for webapp-only mode

def group_devices_by_connection():
    """Group devices by their connection (TCP host:port or COM port)"""
    try:
        cached_devices = config_cache.get_all_devices()
        print(f"DEBUG: cached_devices type: {type(cached_devices)}")
        print(f"DEBUG: cached_devices content: {cached_devices}")
        
        connections = {}
        
        # Handle case where cached_devices might be None or empty
        if not cached_devices:
            print("DEBUG: cached_devices is empty or None")
            return connections
            
        # Handle case where cached_devices is a list instead of dict
        if isinstance(cached_devices, list):
            print("DEBUG: cached_devices is a list, converting to dict")
            # Convert list to dict if needed
            cached_devices_dict = {}
            for device in cached_devices:
                if hasattr(device, 'id'):
                    cached_devices_dict[device.id] = device
                else:
                    print(f"DEBUG: device missing id: {device}")
            cached_devices = cached_devices_dict
        
        for device in cached_devices.values():
            print(f"DEBUG: Processing device {device.id} ({device.name}): protocol={device.protocol}")
            # Determine connection identifier
            if device.protocol == "TCP" or device.protocol == "ModbusTCP":
                connection_id = f"TCP_{device.host}_{device.port}"
                connection_name = f"TCP {device.host}:{device.port}"
                print(f"DEBUG: TCP device -> {connection_id}")
            else:  # RTU
                connection_id = f"RTU_{device.serial_port}"
                connection_name = f"RTU {device.serial_port}"
                print(f"DEBUG: RTU device -> {connection_id}")
            
            if connection_id not in connections:
                connections[connection_id] = {
                    "id": connection_id,
                    "name": connection_name,
                    "protocol": "TCP" if (device.protocol == "TCP" or device.protocol == "ModbusTCP") else "RTU",
                    "host": getattr(device, 'host', None),
                    "port": getattr(device, 'port', None),
                    "serial_port": getattr(device, 'serial_port', None),
                    "devices": []
                }
            
            connections[connection_id]["devices"].append(device)
        
        return connections
    except Exception as e:
        print(f"DEBUG: Error in group_devices_by_connection: {e}")
        import traceback
        traceback.print_exc()
        return {}

def reload_all_configs():
    """
    Reload all configurations without restarting the entire service
    """
    try:
        # Reload config cache first
        config_cache.reload_configs()
        
        # NOTE: Worker reload is handled by independent processes
        # Webapp only manages database and configuration
        print("💡 Config reloaded. Workers will pick up changes automatically.")
        
        # Force invalidate subdashboard cache to ensure UI updates
        config_cache.invalidate_subdashboard_cache()
        
        return True
    except Exception as e:
        print(f"❌ Config reload failed: {e}")
        return False

# List Devices (using cache) with Worker Information
@devices_bp.route("/devices")
def devices():
    try:
        # Group devices by connection
        connections = group_devices_by_connection()
        
        # Ensure connections is always a dict
        if not isinstance(connections, dict):
            print(f"DEBUG: connections is not dict, type: {type(connections)}")
            connections = {}
        
        # Get worker status - in webapp-only mode, show as "external_process"
        pm = get_process_manager()
        worker_statuses = []
        # ProcessManager is disabled in webapp-only mode
        print("💡 Workers are running as independent processes")
        
        # Add default worker status to each connection
        for connection_id, connection in connections.items():
            conn_info = connection.get('serial_port') or f"{connection.get('host')}:{connection.get('port')}"
            print(f"DEBUG: Processing connection {connection_id}: {connection.get('protocol')} - {conn_info}")
            # In webapp-only mode, assume workers are external processes
            connection["worker_status"] = "external_process"
            connection["worker_info"] = {"status": "external_process", "message": "Run as independent process"}
            connection["worker_id"] = f"EXTERNAL_{connection_id}"
            
            print(f"DEBUG: Final connection status: {connection['worker_status']}")
        
        # Get individual device statuses from database
        from modbus_monitor.database.db import get_all_device_statuses_from_db
        device_statuses = get_all_device_statuses_from_db()
        
        # Ensure device_statuses is dict
        if not isinstance(device_statuses, dict):
            device_statuses = {}
        
        # Add status info to devices
        for connection in connections.values():
            for device in connection["devices"]:
                status_info = device_statuses.get(device.id, {})
                # Map database columns to status format
                device.status = "connected" if status_info.get("is_online") else "disconnected"
                device.last_seen = status_info.get("updated_at")  # Use updated_at as last_seen
                device.is_online = bool(status_info.get("is_online", False))
                device.latency_ms = status_info.get("latency_ms")  # If available in future
        
        return render_template("devices/devices.html", 
                             connections=connections,
                             # Keep legacy format for backward compatibility
                             items=[])
        
    except Exception as e:
        print(f"ERROR in devices route: {e}")
        import traceback
        traceback.print_exc()
        flash(f"Error loading devices: {e}", "error")
        return render_template("devices/devices.html", connections={}, items=[])

@devices_bp.route("/debug/status")
def debug_device_status():
    """Debug endpoint to check device statuses"""
    try:
        device_statuses = config_cache.get_all_device_statuses()
        cached_devices = config_cache.get_all_devices()
        
        # Ensure cached_devices is dict
        if not isinstance(cached_devices, dict):
            cached_devices = {}
        
        # Ensure device_statuses is dict  
        if not isinstance(device_statuses, dict):
            device_statuses = {}
        
        debug_info = {
            "device_count": len(cached_devices),
            "status_count": len(device_statuses),
            "devices": {device_id: device.__dict__ for device_id, device in cached_devices.items()},
            "statuses": device_statuses,
            "timestamp": time.time()
        }
        
        return jsonify(debug_info)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# Device detail (using cache)
@devices_bp.route("/devices/<int:did>")
def device_detail(did):
    # Use cache first
    dev = None
    # print(list_devices())
    for item in list_devices():
        if item['id'] == did:
            dev = item
            break
    dev = SimpleNamespace(**dev) if dev else config_cache.get_device(did)
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
    
    # Get tags from DB
    tags = list_tags(did)
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
        read_interval_ms = to_int(request.form.get("read_interval_ms"), 1000, "read_interval_ms")
        default_function_code = to_int(request.form.get("default_function_code"), 3, "default_function_code")
        
        # Validate reading interval
        if read_interval_ms < 50 or read_interval_ms > 10000:
            errors["read_interval_ms"] = "Reading interval must be between 50ms and 10000ms."
        
        # Validate function code
        if default_function_code not in [1, 2, 3, 4]:
            errors["default_function_code"] = "Function code must be 1, 2, 3, or 4."

        data = {
            "name": name,
            "protocol": "ModbusTCP" if protocol == "ModbusTCP" else "ModbusRTU",
            "unit_id": unit_id,
            "timeout_ms": timeout_ms,
            "read_interval_ms": read_interval_ms,
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
        
        # Reload configs instead of restarting services
        if reload_all_configs():
            flash("Device created and configs reloaded successfully.", "success")
        else:
            flash("Device created but config reload failed. You may need to restart manually.", "warning")
        
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
        
        # Force reload configs via ProcessManager
        try:
            from flask import current_app
            pm = getattr(current_app, 'process_manager', None)
            if pm:
                # Send reload to all workers
                for worker_id in pm.command_queues:
                    pm.command_queues[worker_id].put({"type": "reload_config"})
                
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

    protocol = dev.protocol or "ModbusTCP"

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
        read_interval_ms = to_int(request.form.get("read_interval_ms"), 1000, "read_interval_ms")
        default_function_code = to_int(request.form.get("default_function_code"), 3, "default_function_code")
        
        # Validate reading interval
        if read_interval_ms < 50 or read_interval_ms > 10000:
            errors["read_interval_ms"] = "Reading interval must be between 50ms and 10000ms."
        
        # Validate function code
        if default_function_code not in [1, 2, 3, 4]:
            errors["default_function_code"] = "Function code must be 1, 2, 3, or 4."

        data = {
            "name": name,
            "protocol": protocol,  # không cho đổi protocol trong edit (đơn giản)
            "unit_id": unit_id,
            "timeout_ms": timeout_ms,
            "read_interval_ms": read_interval_ms,
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
                "serial_port": None, "baudrate": None, "parity": None,
                "stopbits": None, "bytesize": None
            })
        else:
            serial_port = (request.form.get("serial_port") or "").strip()
            baudrate = to_int(request.form.get("baudrate"), None, "baudrate")
            parity = (request.form.get("parity") or "N").upper()
            stopbits = to_int(request.form.get("stopbits"), None, "stopbits")
            bytesize = to_int(request.form.get("bytesize"), None, "bytesize")
            byte_order = (request.form.get("byte_order") or "BigEndian").strip()

            if not serial_port:
                errors["serial_port"] = "Serial port is required for ModbusRTU."
            if parity not in ("N","E","O"):
                errors["parity"] = "Parity must be N, E or O."
            if stopbits not in (1,2):
                errors["stopbits"] = "Stop bits must be 1 or 2."
            if bytesize not in (7,8):
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
        
        # Reload configs to pick up device changes
        if reload_all_configs():
            flash("Device updated and configs reloaded successfully.", "success")
        else:
            flash("Device updated but config reload failed. You may need to restart manually.", "warning")
        
        return redirect(url_for("devices_bp.device_detail", did=did))

    # GET: prefill form từ dev
    class F: pass
    f = F()
    # Convert dataclass to dict-like object for template
    for k in ['name', 'protocol', 'host', 'port', 'serial_port', 'baudrate', 'parity', 'stopbits', 'bytesize', 'unit_id', 'timeout_ms', 'read_interval_ms', 'default_function_code', 'byte_order', 'description']:
        setattr(f, k, getattr(dev, k, None))
    return render_template("devices/device_form.html",
                           protocol=protocol,
                           form=f,
                           editing=True,
                           device_id=did)

@devices_bp.route("/devices/<int:did>/delete", methods=["POST"])
def delete_device(did):
    if config_cache.delete_device(did):
        # Reload configs instead of restarting services
        if reload_all_configs():
            flash("Device deleted and configs reloaded successfully.", "success")
        else:
            flash("Device deleted but config reload failed. You may need to restart manually.", "warning")
    else:
        flash("Device not found.", "warning")
    return redirect(url_for("devices_bp.devices"))

@devices_bp.route("/devices/<int:did>/tags/<int:tid>/edit", methods=["GET","POST"])
def edit_tag(did, tid):
    device = config_cache.get_device(did)
    tag = None
    for item in list_tags(did):
        if item['id'] == tid:
            tag = item
            tag = SimpleNamespace(**tag)
            break
    if tag == None:
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
        
        # Reload configs instead of restarting services
        if reload_all_configs():
            flash("Tag updated and configs reloaded successfully.", "success")
        else:
            flash("Tag updated but config reload failed. You may need to restart manually.", "warning")
        
        return redirect(url_for("devices_bp.device_detail", did=did))

    # GET: prefill
    class F: pass
    f = F()
    # Convert dataclass to dict-like object for template
    for k in ['name', 'address', 'datatype', 'unit', 'scale', 'offset', 'grp', 'function_code', 'description']:
        setattr(f, k, getattr(tag, k, None))
    return render_template("devices/tag_form.html",
                           device=device, tag=tag, form=f, editing=True)

@devices_bp.route("/devices/<int:did>/tags/<int:tid>/delete", methods=["POST"])
def delete_tag(did, tid):
    tag = config_cache.get_tag(tid)
    if not tag or tag.device_id != did:
        flash("Tag not found.", "warning")
        return redirect(url_for("devices_bp.device_detail", did=did))
    
    if config_cache.delete_tag(tid):
        # Reload configs instead of restarting services
        if reload_all_configs():
            flash("Tag deleted and configs reloaded successfully.", "success")
        else:
            flash("Tag deleted but config reload failed. You may need to restart manually.", "warning")
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

@devices_bp.route("/api/devices/mysql-sync", methods=["POST"])
def sync_device_status_to_mysql():
    """API endpoint to sync device status from config_cache to MySQL database"""
    try:
        from modbus_monitor.database.db import sync_device_status_to_mysql
        
        result = sync_device_status_to_mysql()
        
        if result.get("success"):
            return jsonify({
                "success": True, 
                "message": f"Device status synced to MySQL successfully",
                "updated_count": result.get("updated_count", 0),
                "skipped_count": result.get("skipped_count", 0),
                "total_processed": result.get("total_processed", 0)
            })
        else:
            return jsonify({
                "success": False, 
                "error": result.get("error", "Unknown error")
            }), 500
            
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@devices_bp.route("/api/devices/mysql-sync/force", methods=["POST"])
def force_sync_mysql():
    """API endpoint to force immediate device status sync to MySQL - deprecated"""
    # Device sync now handled by workers
    return jsonify({
        "success": True,
        "message": "Device sync is now handled by workers automatically",
        "updated_count": 0
    })

@devices_bp.route("/api/devices/mysql-sync/compare", methods=["GET"])
def compare_device_status():
    """API endpoint to compare device status between config_cache and MySQL"""
    try:
        from modbus_monitor.database.db import get_device_status_comparison
        
        result = get_device_status_comparison()
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@devices_bp.route("/api/devices/mysql-sync/stats", methods=["GET"])
def get_mysql_sync_stats():
    """API endpoint to get MySQL sync service stats (external process mode)"""
    try:
        # In webapp-only mode, assume datalogger runs as external process
        stats = {
            "worker_running": True,  # Assume external process is running
            "worker_pid": "external",
            "last_activity": "external_process",
            "log_entries_count": "N/A",
            "message": "MySQL sync is handled by external datalogger process"
        }
        return jsonify({"success": True, "stats": stats})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@devices_bp.route("/debug/mysql-sync", methods=["GET"])
def debug_mysql_sync():
    """Debug endpoint để kiểm tra trạng thái sync (external process mode)"""
    try:
        from modbus_monitor.database.db import get_device_status_comparison
        from modbus_monitor.services.config_cache import get_config_cache
        
        # In webapp-only mode, assume datalogger runs as external process
        service_info = {
            "running": True,  # Assume external process is running
            "stats": {
                "worker_pid": "external",
                "last_activity": "external_process",
                "message": "MySQL sync handled by external datalogger process"
            }
        }
        
        # Lấy thông tin config_cache
        config_cache = get_config_cache()
        cache_statuses = config_cache.get_all_device_statuses()
        
        # So sánh với database
        comparison = get_device_status_comparison()
        
        debug_data = {
            "timestamp": time.time(),
            "service": service_info,
            "cache_devices": len(cache_statuses),
            "cache_statuses": cache_statuses,
            "comparison": comparison
        }
        
        return jsonify(debug_data)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500