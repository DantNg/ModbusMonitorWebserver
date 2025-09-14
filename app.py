import eventlet
eventlet.monkey_patch()

import os
from flask import redirect, url_for, request, jsonify, session
from modbus_monitor import create_app
from modbus_monitor.extensions import socketio
from modbus_monitor.services.runner import start_services
from modbus_monitor.database import db
app = create_app()

@app.route("/")
def root():
    print("Start login")
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
        print(f"Error updating tag unit: {e}")
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
            print(f"🔄 Loaded {len(_subdashboards_cache)} subdashboards from DB: {[s.get('name') for s in _subdashboards_cache]}")
            _cache_timestamp = current_time
        except Exception as e:
            print(f"❌ Error loading subdashboards: {e}")
            _subdashboards_cache = []
    
    return dict(subdashboards=_subdashboards_cache)

def clear_subdashboards_cache():
    """Clear subdashboards cache to force reload"""
    global _subdashboards_cache
    _subdashboards_cache = None
    print("🔄 Subdashboards cache cleared")

# Make function available globally
app.clear_subdashboards_cache = clear_subdashboards_cache

if __name__ == "__main__":
    try:
        # Luôn start services trong production
        start_services()

        # Debug=False để cải thiện performance
        socketio.run(app, host="0.0.0.0", port=5000, debug=False)

    except Exception as e:
        print(f"Error starting the application: {e}")
