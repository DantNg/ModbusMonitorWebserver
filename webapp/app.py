import eventlet
eventlet.monkey_patch()

import os
import sys
import atexit
import threading
import time
import logging
import traceback
from datetime import datetime
from logging.handlers import RotatingFileHandler

# Add current directory and project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, current_dir)
sys.path.insert(0, project_root)

from flask import redirect, url_for, request, jsonify, session
from modbus_monitor import create_app
from modbus_monitor.extensions import socketio
from modbus_monitor.database import db

app = create_app()

# --------------------------------------------------
# Logging configuration — use centralized app_logger
# Auto-creates logs/ folder, works with both Python & EXE (PyInstaller)
# --------------------------------------------------
from shared.app_logger import get_logger as _get_app_logger, log_exception
logger = _get_app_logger("webapp", log_filename="webapp_errors.log")

# Log absolute static folder path for diagnostics
logger.info("Static folder: %s (exists=%s)", app.static_folder, os.path.isdir(app.static_folder or ''))

# --------------------------------------------------
# Compatibility Publisher — register socketio instance so publisher
# can be used by other modules when USE_COMPATIBILITY_PUBLISHER=true.
# This call is always safe (no behavior change when flag is false).
# --------------------------------------------------
try:
    from modbus_monitor.services.socket_emission_manager import publisher as _compat_publisher
    _compat_publisher.set_socketio(socketio)
    logger.info("CompatibilityPublisher: socketio instance registered")
except Exception as _cp_exc:
    logger.warning("CompatibilityPublisher init failed (non-fatal): %s", _cp_exc)

# Import feature flags and publisher for use in route/event handlers below
try:
    from shared.feature_flags import flags as _flags
    from modbus_monitor.services.socket_emission_manager import publisher as _publisher
except Exception as _ff_exc:
    logger.warning("feature_flags/publisher import failed (non-fatal): %s", _ff_exc)

    class _FlagsFallback:
        USE_COMPATIBILITY_PUBLISHER = False

    _flags = _FlagsFallback()
    _publisher = None

# ProcessManager disabled in webapp-only mode
process_manager = None
data_queue_thread = None

def initialize_process_manager():
    """Initialize ProcessManager - disabled in webapp-only mode"""
    global process_manager
    # Always return None in webapp-only mode
    process_manager = None
    app.process_manager = None  # Explicitly set to None
    return None

@app.route("/")
def root():
    logger.info("Root accessed, redirecting to login")
    return redirect(url_for("auth_bp.login"))

@app.route("/tags/<int:tag_id>/update-unit", methods=["POST"])
def update_tag_unit(tag_id):
    """Update unit for a specific tag"""
    # Check if user is admin
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    
    try:
        unit = request.form.get("unit", "").strip()
        
        # Update unit in database
        result = db.update_tag_unit(tag_id, unit)
        
        if result:
            return jsonify({"success": True, "message": "Unit updated successfully"})
        else:
            return jsonify({"success": False, "message": "Failed to update unit"})
            
    except Exception as e:
        logger.error("Error updating tag unit: %s\n%s", e, traceback.format_exc())
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/api/emit-tag-update", methods=["POST"])
def emit_tag_update():
    """API endpoint for workers to emit tag updates via SocketIO"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "message": "No data provided"}), 400
        
        tag_id = data.get("tag_id")
        value = data.get("value")
        timestamp = data.get("timestamp")
        worker_id = data.get("worker_id")
        worker_type = data.get("worker_type", "unknown")
        device_id = data.get("device_id")
        device_name = data.get("device_name")
        
        if tag_id is None or value is None:
            return jsonify({"success": False, "message": "tag_id and value required"}), 400
        
        # Prepare SocketIO data
        socket_data = {
            "tag_id": tag_id,
            "value": value,
            "timestamp": timestamp,
            "worker_id": worker_id,
            "worker_type": worker_type,
            "device_id": device_id,
            "device_name": device_name
        }
        
        # Determine target rooms for emission
        rooms_to_emit = []
        
        # 1. Dashboard device room
        if device_id:
            dashboard_room = f"subdashboard_19"
            rooms_to_emit.append(dashboard_room)
        
        # 2. Subdashboard rooms (get from database if tag belongs to subdashboards)
        try:
            # You might want to query which subdashboards contain this tag
            # For now, we'll use a simple approach
            subdash_rooms = db.get_subdashboard_rooms_for_tag(tag_id) if hasattr(db, 'get_subdashboard_rooms_for_tag') else []
            rooms_to_emit.extend(subdash_rooms)
        except:
            pass
        
        # Emit modbus_update to specific rooms + tag_update broadcast
        if _flags.USE_COMPATIBILITY_PUBLISHER and _publisher:
            _publisher.publish_modbus_update(socket_data, rooms=rooms_to_emit if rooms_to_emit else None)
            _publisher.publish_tag_update(socket_data)
        else:
            for room in rooms_to_emit:
                socketio.emit('modbus_update', socket_data, room=room)
            # Also emit general tag_update for broader listeners
            socketio.emit('tag_update', socket_data, broadcast=True)
        
        return jsonify({
            "success": True, 
            "message": f"Tag update emitted to {len(rooms_to_emit)} rooms",
            "rooms": rooms_to_emit
        })
        
    except Exception as e:
        logger.error("Error in emit_tag_update: %s\\n%s", e, traceback.format_exc())
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/api/modbus-update", methods=["POST"])
def modbus_update():
    """API endpoint for workers to send modbus updates with proper room handling"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "message": "No data provided"}), 400
        
        tag_id = data.get("tag_id")
        value = data.get("value")
        timestamp = data.get("timestamp")
        worker_id = data.get("worker_id")
        worker_type = data.get("worker_type", "unknown")
        device_id = data.get("device_id")
        device_name = data.get("device_name")
        tag_name = data.get("tag_name", f"tag_{tag_id}")
        
        if tag_id is None or value is None:
            return jsonify({"success": False, "message": "tag_id and value required"}), 400
        
        # Prepare modbus_update data (format expected by frontend)
        modbus_data = {
            "tags": [{
                "id": tag_id,
                "name": tag_name,
                "value": value,
                "timestamp": timestamp
            }],
            "device_id": device_id,
            "device_name": device_name,
            "worker_id": worker_id,
            "worker_type": worker_type,
            "ok": True
        }
        
        # Determine target rooms
        rooms_emitted = []

        # 1. Dashboard device room
        if device_id:
            dashboard_room = f"dashboard_device_{device_id}"
            rooms_emitted.append(dashboard_room)

        # 2. Get subdashboard rooms for this tag
        try:
            subdash_rooms = db.get_subdashboard_rooms_for_tag(tag_id) if hasattr(db, 'get_subdashboard_rooms_for_tag') else []
            rooms_emitted.extend(subdash_rooms)
        except Exception as subdash_error:
            logger.warning("Could not get subdashboard rooms: %s", subdash_error)

        # Emit via publisher or direct socketio
        if _flags.USE_COMPATIBILITY_PUBLISHER and _publisher:
            _publisher.publish_modbus_update(modbus_data, rooms=rooms_emitted if rooms_emitted else None)
        else:
            for room in rooms_emitted:
                socketio.emit('modbus_update', modbus_data, room=room)
        
        return jsonify({
            "success": True, 
            "message": f"Modbus update sent to {len(rooms_emitted)} rooms",
            "rooms": rooms_emitted
        })
        
    except Exception as e:
        logger.error("Error in modbus_update endpoint: %s\\n%s", e, traceback.format_exc())
        return jsonify({"success": False, "message": str(e)}), 500
# Cache subdashboards để tránh query mỗi request
_subdashboards_cache = None
_cache_timestamp = 0

@app.context_processor
def inject_subdashboards():
    global _subdashboards_cache, _cache_timestamp
    import time
    current_time = time.time()
    
    # Cache 30 giây
    if _subdashboards_cache is None or (current_time - _cache_timestamp) > 30:
        try:
            _subdashboards_cache = db.list_subdashboards() if hasattr(db, "list_subdashboards") else []
            logger.info("Loaded %d subdashboards: %s", len(_subdashboards_cache), [s.get('name') for s in _subdashboards_cache])
            _cache_timestamp = current_time
        except Exception as e:
            logger.error("Error loading subdashboards: %s\n%s", e, traceback.format_exc())
            _subdashboards_cache = []
    
    return dict(subdashboards=_subdashboards_cache)

def clear_subdashboards_cache():
    """Clear subdashboards cache to force reload"""
    global _subdashboards_cache
    _subdashboards_cache = None
    logger.info("Subdashboards cache cleared")

# Make function available globally
app.clear_subdashboards_cache = clear_subdashboards_cache

# Socket.IO event handlers
from flask_socketio import join_room, leave_room

# Socket.IO error handler for connection issues
@socketio.on_error_default
def default_error_handler(e):
    """Handle Socket.IO errors globally"""
    # Log the error but don't crash the server
    if isinstance(e, (ConnectionAbortedError, ConnectionResetError, BrokenPipeError)):
        logger.warning(f"Client connection lost: {type(e).__name__}: {e}")
    else:
        logger.error(f"Socket.IO error: {type(e).__name__}: {e}")
        logger.error("Traceback:\n" + traceback.format_exc())

@socketio.on('connect')
def handle_connect():
    try:
        client_ip = request.environ.get('REMOTE_ADDR', 'unknown')
        logger.info(f"Client connected from {client_ip}: {request.sid}")
    except Exception as e:
        logger.warning(f"Error handling connect event: {e}")

@socketio.on('leave')
def on_leave(data):
    room = data.get('room')
    if room:
        leave_room(room)
        logger.info(f"Client {request.sid} left room: {room}")
        
        # Notify workers about room leave
        socketio.emit('room_left', {
            'room': room,
            'client_id': request.sid,
            'timestamp': datetime.now().isoformat()
        })
        
        logger.info(f"Room leave broadcasted: {room}")
            
@socketio.on('disconnect')
def handle_disconnect(auth=None):
    try:
        client_ip = request.environ.get('REMOTE_ADDR', 'unknown')
        logger.info(f"Client disconnected from {client_ip}: {request.sid}")
    except Exception as e:
        logger.warning(f"Error handling disconnect event: {e}")
    # ProcessManager disabled in webapp-only mode

@socketio.on('join')
def on_join(data):
    room = data.get('room')
    if room:
        join_room(room)  # Join the socket.io room
        logger.info(f"Client {request.sid} joined room: {room}")
        
        # Notify workers about room join for targeted updates
        socketio.emit('room_joined', {
            'room': room,
            'client_id': request.sid,
            'timestamp': datetime.now().isoformat()
        })
        
        logger.info(f"Room join broadcasted: {room}")
            
        socketio.emit('join_ack', {'room': room}, to=request.sid)

@socketio.on('modbus_update')
def on_modbus_update(data):
    """
    Handle modbus update from workers and emit to appropriate rooms.
    Data comes directly from TCP workers via SocketIO client.
    """
    try:
        tags = data.get("tags") or []
        source = data.get("source", "unknown")
        
        # Log occasionally to avoid spam
        if data.get("seq", 0) % 20 == 0:
            print(f"[srv] modbus_update from {source}: device={data.get('device_id')} tags={len(tags)} seq={data.get('seq')}")

        # For TCP worker emissions, broadcast to all rooms for now
        # Later we can implement more sophisticated room filtering
        if _flags.USE_COMPATIBILITY_PUBLISHER and _publisher:
            # Resolve target rooms from payload, then publish via canonical publisher
            if source == 'tcp_worker':
                _publisher.publish_modbus_update(data)
            else:
                target_rooms = data.get("target_rooms") or []
                if not target_rooms:
                    room = data.get("room")
                    if not room:
                        subdash_id = data.get("subdash_id")
                        if subdash_id:
                            room = f"subdashboard_{subdash_id}"
                    if room:
                        target_rooms = [room]
                _publisher.publish_modbus_update(data, rooms=target_rooms if target_rooms else None)
        elif source == 'tcp_worker':
            # Broadcast to all connected clients
            socketio.emit('modbus_update', data)
        else:
            # Handle legacy format with target_rooms or room
            target_rooms = data.get("target_rooms") or []
            
            if target_rooms:
                for room in target_rooms:
                    socketio.emit('modbus_update', data, room=room)
            else:
                # Fallback room determination (for backward compatibility)
                room = data.get("room")
                if not room:
                    subdash_id = data.get("subdash_id")
                    if subdash_id:
                        room = f"subdashboard_{subdash_id}"
                
                if room:
                    socketio.emit('modbus_update', data, room=room)
                else:
                    # Last resort - broadcast to all
                    socketio.emit('modbus_update', data)

        # Store data in database if needed
        if data.get('ok', True) and 'tag_id' in data and 'value' in data:
            try:
                db.update_tag_latest_value(
                    data['tag_id'], 
                    data['value'], 
                    data.get('timestamp', time.time())
                )
            except Exception as e:
                print(f"Error storing tag value: {e}")

    except Exception as e:
        print(f"on_modbus_update error: {e}")

@socketio.on('modbus_write_command')
def on_modbus_write_command(data):
    """
    Handle write command from frontend and forward to appropriate worker.
    This acts as a relay between frontend and workers.
    """
    try:
        print(f"📝 Received write command from frontend: {data}")
        
        tag_id = data.get('tag_id')
        if not tag_id:
            # Send error response back to frontend
            socketio.emit('modbus_write_response', {
                'tag_id': None,
                'success': False,
                'error': 'Missing tag_id in write command',
                'timestamp': datetime.now().isoformat()
            }, to=request.sid)
            return
        
        # Look up tag and device from database to determine which worker should handle this
        try:
            from modbus_monitor.database.db import get_tag, get_device
            
            tag = get_tag(tag_id)
            if not tag:
                print(f"❌ Tag {tag_id} not found in database")
                socketio.emit('modbus_write_response', {
                    'tag_id': tag_id,
                    'success': False,
                    'error': f'Tag {tag_id} not found',
                    'timestamp': datetime.now().isoformat()
                }, to=request.sid)
                return
            
            print(f"📖 Found tag: {tag}")
            
            device_id = tag.get('device_id')
            if not device_id:
                print(f"❌ Tag {tag_id} has no device_id")
                socketio.emit('modbus_write_response', {
                    'tag_id': tag_id,
                    'success': False,
                    'error': f'Tag {tag_id} has no device association',
                    'timestamp': datetime.now().isoformat()
                }, to=request.sid)
                return
            
            device = get_device(device_id)
            if not device:
                print(f"❌ Device {device_id} not found in database")
                socketio.emit('modbus_write_response', {
                    'tag_id': tag_id,
                    'success': False,
                    'error': f'Device for tag {tag_id} not found',
                    'timestamp': datetime.now().isoformat()
                }, to=request.sid)
                return
            
            print(f"📖 Found device: {device}")
            
            # Add device info to write command
            write_data = data.copy()
            write_data.update({
                'device_id': device.get('id'),
                'device_name': device.get('name'),
                'connection_type': device.get('connection_type', 'tcp'),
                'frontend_client_id': request.sid  # So worker knows who to respond to
            })
            
            # Forward to all workers (they will filter by their tags)
            # In a more sophisticated setup, you might want to target specific workers
            socketio.emit('modbus_write_command', write_data)
            print(f"📤 Forwarded write command to workers: tag={tag_id}, device={device.get('name')}")
            
        except Exception as e:
            print(f"❌ Database lookup error for write command: {e}")
            import traceback
            traceback.print_exc()
            socketio.emit('modbus_write_response', {
                'tag_id': tag_id,
                'success': False,
                'error': f'Database error: {str(e)}',
                'timestamp': datetime.now().isoformat()
            }, to=request.sid)
            
    except Exception as e:
        print(f"on_modbus_write_command error: {e}")
        socketio.emit('modbus_write_response', {
            'tag_id': data.get('tag_id'),
            'success': False,
            'error': f'Server error: {str(e)}',
            'timestamp': datetime.now().isoformat()
        }, to=request.sid)

@socketio.on('modbus_write_response')
def on_modbus_write_response(data):
    """
    Handle write response from workers and forward to appropriate frontend client.
    Workers will send responses here, and we forward to the original requester.
    """
    try:
        print(f"📥 Received write response from worker: {data}")
        
        # Forward response to the original frontend client
        frontend_client_id = data.get('frontend_client_id')
        if frontend_client_id:
            socketio.emit('modbus_write_response', data, to=frontend_client_id)
            print(f"📤 Forwarded write response to frontend client: {frontend_client_id}")
        else:
            # Fallback: broadcast to all clients (less ideal but works)
            socketio.emit('modbus_write_response', data)
            print("📤 Broadcasted write response to all clients")
            
    except Exception as e:
        print(f"on_modbus_write_response error: {e}")

@socketio.on('alarm_event')
def handle_alarm_event(data):
    print('Received alarm_event from client:', data)
    if _flags.USE_COMPATIBILITY_PUBLISHER and _publisher:
        _publisher.publish_alarm_event(data)
    else:
        socketio.emit('alarm_event', data)

@socketio.on('quad_alarm_event')
def handle_quad_alarm_event(data):
    """
    Handle quad alarm events from alarm worker and broadcast to all browser clients.
    This receives events emitted by alarm_worker.py via socketio.Client().
    """
    try:
        print('📡 Received quad_alarm_event from worker:', data)
        
        # Extract quad_id to determine which subdashboard this belongs to
        quad_id = data.get('quad_id')
        
        # Broadcast to all clients (they will filter by quad_id on frontend)
        if _flags.USE_COMPATIBILITY_PUBLISHER and _publisher:
            _publisher.publish_quad_alarm_event(data)
        else:
            socketio.emit('quad_alarm_event', data)
        print(f'✅ Broadcasted quad_alarm_event to all clients: quad_id={quad_id}')
        
    except Exception as e:
        print(f'❌ Error handling quad_alarm_event: {e}')
        import traceback
        traceback.print_exc()

@socketio.on('card_alarm_event')
def handle_card_alarm_event(data):
    """
    Handle card alarm events (qtag6, qtag4, qtag3, qtag2, single3, pv_only, pv_dual)
    from alarm worker and broadcast to all browser clients.
    This receives events emitted by alarm_worker.py via socketio.Client().
    """
    try:
        card_type = data.get('card_type')
        card_id = data.get('card_id')
        status = data.get('status')
        print(f'📡 Received card_alarm_event from worker: {card_type}/{card_id} [{status}]')

        # Broadcast to all clients (frontend filters by card_type/card_id)
        if _flags.USE_COMPATIBILITY_PUBLISHER and _publisher:
            _publisher.publish_card_alarm_event(data)
        else:
            socketio.emit('card_alarm_event', data)
        print(f'✅ Broadcasted card_alarm_event to all clients: {card_type}/{card_id}')

    except Exception as e:
        print(f'❌ Error handling card_alarm_event: {e}')
        import traceback
        traceback.print_exc()
    
def start_data_queue_processor():
    """Background thread to process data from workers (disabled in webapp-only mode)"""
    print("INFO: Data queue processor disabled in webapp-only mode")
    return None

def cleanup_on_exit():
    """Cleanup function for graceful shutdown (disabled in webapp-only mode)"""
    print("INFO: Cleanup disabled in webapp-only mode")

if __name__ == "__main__":
    print("🌐 Starting Flask Modbus Monitor - Webapp Only")
    print("� This process only handles web interface")
    print("� Access at: http://localhost:5000")
    print("--------------------------------------------------")
    
    # Initialize ProcessManager (disabled in webapp-only mode)
    initialize_process_manager()
    
    try:
        # Use socketio.run() for proper eventlet integration
        socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)
    except KeyboardInterrupt:
        print("\n🛑 Server stopped")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
