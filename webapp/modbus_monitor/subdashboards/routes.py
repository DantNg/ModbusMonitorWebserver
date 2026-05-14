from flask import jsonify, render_template, request, redirect, url_for, flash, session
from sqlalchemy import select
from . import subdash_bp
from datetime import datetime,timedelta
from ..database import db
from ..services.socket_emission_manager import get_emission_manager

@subdash_bp.get("/")
def list_subdash():
    # Lấy danh sách subdashboard từ DB (demo: chưa có bảng riêng thì hardcode)
    print("List subdashboards")
    dashboards = db.list_subdashboards() if hasattr(db, "list_subdashboards") else []
    print(f"🔍 Found {len(dashboards)} subdashboards: {[d.get('name') for d in dashboards]}")
    return render_template("subdashboards/list.html", items=dashboards)

@subdash_bp.route("/add", methods=["GET", "POST"])
def add_subdash():
    # Chỉ admin mới có thể tạo subdashboard
    if session.get("role") != "admin":
        flash("Access denied. Admin role required.", "error")
        return redirect(url_for("subdash_bp.list_subdash"))
    
    if request.method == "POST":
        name = request.form.get("name")
        description = request.form.get("description")
        tag_ids = request.form.getlist("tag_ids")
        sid = db.add_subdashboard_row({"name": name, "description": description}, [int(t) for t in tag_ids])
        
        # Clear cache để navigation update
        from flask import current_app
        if hasattr(current_app, 'clear_subdashboards_cache'):
            current_app.clear_subdashboards_cache()
        
        return redirect(url_for("subdash_bp.subdash_detail", sid=sid))
    all_tags = db.list_all_tags() if hasattr(db, "list_all_tags") else []
    return render_template("subdashboards/add.html", all_tags=all_tags)

@subdash_bp.get("/<int:sid>")
def subdash_detail(sid):
    from flask import make_response
    from ..services.config_cache import get_config_cache

    # Fetch subdashboard, tags, and all tags
    subdash = db.get_subdashboard(sid) if hasattr(db, "get_subdashboard") else {"id": sid, "name": "Demo"}
    tags = db.get_subdashboard_tags(sid) if hasattr(db, "get_subdashboard_tags") else []
    all_tags = db.list_all_tags() if hasattr(db, "list_all_tags") else []

    # Prefer fresh statuses from DB to avoid stale cache on page refresh
    device_statuses = {}
    if hasattr(db, "get_all_device_statuses_from_db"):
        try:
            device_statuses = db.get_all_device_statuses_from_db() or {}
        except Exception as e:
            print(f"⚠️ Could not load device statuses from DB: {e}")

    config_cache = get_config_cache()

    def _get_device_status(device_id: int):
        """Return (status_str, last_seen) where status_str in connected/disconnected/unknown.
        DB value preferred; fallback to config_cache if DB missing.
        """
        info = device_statuses.get(device_id) if isinstance(device_statuses, dict) else None
        if isinstance(info, dict) and "is_online" in info:
            status_str = 'connected' if info.get('is_online') else 'disconnected'
            last_seen = info.get('updated_at')
            return status_str, last_seen
        # Fallback to cache
        cache_info = config_cache.get_device_status(device_id) or {}
        return cache_info.get('status', 'unknown'), cache_info.get('last_seen')

    def enrich_tag(tag):
        device_id = tag.get('device_id')
        if device_id:
            status_str, last_seen = _get_device_status(device_id)
            tag['device_status'] = status_str
            tag['device_last_seen'] = last_seen
        else:
            tag['device_status'] = 'unknown'
            tag['device_last_seen'] = None
        if tag.get('device_status') == 'disconnected':
            tag['value'] = 0
        return tag

    tags = [enrich_tag(tag) for tag in tags]

    # Get groups for this subdashboard
    groups = [dict(g) for g in db.list_subdash_groups_for_dashboard(sid)] if hasattr(db, "list_subdash_groups_for_dashboard") else []
    for g in groups:
        g["tags"] = [enrich_tag(tag) for tag in db.get_tags_of_group(g["id"])]
        # Add quad cards to group
        g["quad_cards"] = db.get_quad_cards_for_group(g["id"]) if hasattr(db, "get_quad_cards_for_group") else []
        # Enrich device_status for quad card tags (fix: quad tags were missing status on page load)
        for qc in g["quad_cards"]:
            for pos in [1, 2, 3, 4]:
                tag_data = qc.get(f'tag{pos}')
                if tag_data and isinstance(tag_data, dict):
                    device_id = tag_data.get('device_id')
                    if device_id:
                        status_str, last_seen = _get_device_status(device_id)
                        tag_data['device_status'] = status_str
                        tag_data['device_last_seen'] = last_seen
                    else:
                        tag_data['device_status'] = 'unknown'
                        tag_data['device_last_seen'] = None
                    # Force value to 0 if device is disconnected
                    if tag_data.get('device_status') == 'disconnected':
                        tag_data['last_value'] = 0

        # ===== Load Qtag6 cards =====
        g["qtag6_cards"] = db.get_qtag6_cards_for_group(g["id"]) if hasattr(db, "get_qtag6_cards_for_group") else []
        for qc in g["qtag6_cards"]:
            for pos in range(1, 7):
                tag_data = qc.get(f'tag{pos}')
                if tag_data and isinstance(tag_data, dict):
                    device_id = tag_data.get('device_id')
                    if device_id:
                        status_str, last_seen = _get_device_status(device_id)
                        tag_data['device_status'] = status_str
                        tag_data['device_last_seen'] = last_seen
                    else:
                        tag_data['device_status'] = 'unknown'
                        tag_data['device_last_seen'] = None
                    if tag_data.get('device_status') == 'disconnected':
                        tag_data['last_value'] = 0

        # ===== Load Qtag4 cards =====
        g["qtag4_cards"] = db.get_qtag4_cards_for_group(g["id"]) if hasattr(db, "get_qtag4_cards_for_group") else []
        for qc in g["qtag4_cards"]:
            for pos in range(1, 5):
                tag_data = qc.get(f'tag{pos}')
                if tag_data and isinstance(tag_data, dict):
                    device_id = tag_data.get('device_id')
                    if device_id:
                        status_str, last_seen = _get_device_status(device_id)
                        tag_data['device_status'] = status_str
                        tag_data['device_last_seen'] = last_seen
                    else:
                        tag_data['device_status'] = 'unknown'
                        tag_data['device_last_seen'] = None
                    if tag_data.get('device_status') == 'disconnected':
                        tag_data['last_value'] = 0

        # ===== Load Qtag3 cards =====
        g["qtag3_cards"] = db.get_qtag3_cards_for_group(g["id"]) if hasattr(db, "get_qtag3_cards_for_group") else []
        for qc in g["qtag3_cards"]:
            for pos in range(1, 4):
                tag_data = qc.get(f'tag{pos}')
                if tag_data and isinstance(tag_data, dict):
                    device_id = tag_data.get('device_id')
                    if device_id:
                        status_str, last_seen = _get_device_status(device_id)
                        tag_data['device_status'] = status_str
                        tag_data['device_last_seen'] = last_seen
                    else:
                        tag_data['device_status'] = 'unknown'
                        tag_data['device_last_seen'] = None
                    if tag_data.get('device_status') == 'disconnected':
                        tag_data['last_value'] = 0

        # ===== Load Qtag2 cards =====
        g["qtag2_cards"] = db.get_qtag2_cards_for_group(g["id"]) if hasattr(db, "get_qtag2_cards_for_group") else []
        for qc in g["qtag2_cards"]:
            for pos in range(1, 3):
                tag_data = qc.get(f'tag{pos}')
                if tag_data and isinstance(tag_data, dict):
                    device_id = tag_data.get('device_id')
                    if device_id:
                        status_str, last_seen = _get_device_status(device_id)
                        tag_data['device_status'] = status_str
                        tag_data['device_last_seen'] = last_seen
                    else:
                        tag_data['device_status'] = 'unknown'
                        tag_data['device_last_seen'] = None
                    if tag_data.get('device_status') == 'disconnected':
                        tag_data['last_value'] = 0

        # ===== Load Qtag Single3 cards =====
        g["qtag_single3_cards"] = db.get_qtag_single3_cards_for_group(g["id"]) if hasattr(db, "get_qtag_single3_cards_for_group") else []
        for qc in g["qtag_single3_cards"]:
            pv_tag = qc.get('pv_tag')
            if pv_tag and isinstance(pv_tag, dict):
                device_id = pv_tag.get('device_id')
                if device_id:
                    status_str, last_seen = _get_device_status(device_id)
                    pv_tag['device_status'] = status_str
                    pv_tag['device_last_seen'] = last_seen
                else:
                    pv_tag['device_status'] = 'unknown'
                    pv_tag['device_last_seen'] = None
                if pv_tag.get('device_status') == 'disconnected':
                    pv_tag['last_value'] = 0

        # ===== Load Qtag PV Only cards =====
        g["qtag_pv_cards"] = db.get_qtag_pv_cards_for_group(g["id"]) if hasattr(db, "get_qtag_pv_cards_for_group") else []
        for qc in g["qtag_pv_cards"]:
            pv_tag = qc.get('pv_tag')
            if pv_tag and isinstance(pv_tag, dict):
                device_id = pv_tag.get('device_id')
                if device_id:
                    status_str, last_seen = _get_device_status(device_id)
                    pv_tag['device_status'] = status_str
                    pv_tag['device_last_seen'] = last_seen
                else:
                    pv_tag['device_status'] = 'unknown'
                    pv_tag['device_last_seen'] = None
                if pv_tag.get('device_status') == 'disconnected':
                    pv_tag['last_value'] = 0

        # ===== Load Qtag PV Dual cards =====
        g["qtag_pv_dual_cards"] = db.get_qtag_pv_dual_cards_for_group(g["id"]) if hasattr(db, "get_qtag_pv_dual_cards_for_group") else []
        for qc in g["qtag_pv_dual_cards"]:
            for side in ('left_tag', 'right_tag'):
                tag_data = qc.get(side)
                if tag_data and isinstance(tag_data, dict):
                    device_id = tag_data.get('device_id')
                    if device_id:
                        status_str, last_seen = _get_device_status(device_id)
                        tag_data['device_status'] = status_str
                        tag_data['device_last_seen'] = last_seen
                    else:
                        tag_data['device_status'] = 'unknown'
                        tag_data['device_last_seen'] = None
                    if tag_data.get('device_status') == 'disconnected':
                        tag_data['last_value'] = 0

    # Get active quad alarms for this subdashboard
    active_quad_alarms = []
    if hasattr(db, "get_active_quad_alarms_by_subdash"):
        try:
            active_quad_alarms = db.get_active_quad_alarms_by_subdash(sid)
            print(f"🔔 Found {len(active_quad_alarms)} active quad alarms for subdashboard {sid}")
        except Exception as e:
            print(f"⚠️ Could not load active quad alarms: {e}")

    # Get active per-tag alarms for all Qtag cards in this subdashboard
    active_tag_alarms = []
    if hasattr(db, "get_active_tag_alarms_for_subdash"):
        try:
            active_tag_alarms = db.get_active_tag_alarms_for_subdash(sid)
            # Serialize datetime for JSON embedding in template
            for alarm in active_tag_alarms:
                if 'triggered_at' in alarm and alarm['triggered_at'] is not None:
                    try:
                        alarm['triggered_at'] = alarm['triggered_at'].isoformat()
                    except Exception:
                        alarm['triggered_at'] = str(alarm['triggered_at'])
            print(f"🔔 Found {len(active_tag_alarms)} active tag alarms for subdashboard {sid}")
        except Exception as e:
            print(f"⚠️ Could not load active tag alarms: {e}")

    # Get active card alarms (Qtag6, Qtag4, Qtag3, Qtag2, Single3, PV Only, PV Dual)
    active_card_alarms = []
    if hasattr(db, "get_active_card_alarms_by_subdash"):
        try:
            active_card_alarms = db.get_active_card_alarms_by_subdash(sid)
            for alarm in active_card_alarms:
                if 'triggered_at' in alarm and alarm['triggered_at'] is not None:
                    try:
                        alarm['triggered_at'] = alarm['triggered_at'].isoformat()
                    except Exception:
                        alarm['triggered_at'] = str(alarm['triggered_at'])
            print(f"🔔 Found {len(active_card_alarms)} active card alarms for subdashboard {sid}")
        except Exception as e:
            print(f"⚠️ Could not load active card alarms: {e}")

    current_group = request.args.get('group', '__all__')

    response = make_response(render_template(
        "subdashboards/detail.html",
        subdash=subdash,
        all_tags=all_tags,
        groups=groups,
        current_group=current_group,
        active_quad_alarms=active_quad_alarms,
        active_tag_alarms=active_tag_alarms,
        active_card_alarms=active_card_alarms
    ))
    response.headers['Cache-Control'] = 'no-cache'
    response.headers['Pragma'] = 'no-cache'
    return response

@subdash_bp.route("/<int:sid>/add_tag", methods=["POST"])
def add_tag_to_subdash(sid):
    """Add tag to subdashboard with optional group assignment"""
    tag_id = request.form.get("tag_id")
    target_group = request.form.get("target_group")  # Existing group ID
    new_group_name = request.form.get("new_group_name")  # New group name
    
    if not tag_id:
        return jsonify({"success": False, "error": "Please select a tag"}), 400
    
    try:
        # Get tag info for response
        tag = db.get_tag(int(tag_id))
        tag_name = tag.get("name", "Unknown") if tag else "Unknown"
        
        # Add tag to subdashboard first
        db.add_tag_to_subdashboard(sid, int(tag_id))
        
        # Force refresh subdashboard cache for real-time updates
        try:
            emission_manager = get_emission_manager()
            emission_manager.force_refresh_subdash_cache()
        except Exception as e:
            print(f"Warning: Could not refresh subdashboard cache: {e}")
        
        # Handle group assignment
        group_id = None
        message = f"Tag '{tag_name}' added successfully"
        
        if new_group_name and new_group_name.strip():
            # Create new group
            group_data = {
                "dashboard_id": sid,
                "name": new_group_name.strip(),
                "order": 0
            }
            group_id = db.add_subdash_group(group_data)
            message = f"Tag '{tag_name}' added successfully and new group '{new_group_name}' created"
        elif target_group:
            # Use existing group
            group_id = int(target_group)
            group = db.get_subdash_group(group_id)
            group_name = group.get("name", "Unknown") if group else "Unknown"
            message = f"Tag '{tag_name}' added successfully to group '{group_name}'"
        
        # Add tag to group if group is specified
        if group_id:
            db.add_tag_to_subdash_group(group_id, int(tag_id))
            
        return jsonify({"success": True, "message": message})
            
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@subdash_bp.route("/<int:sid>/rename", methods=["POST"])
def rename_subdash(sid):
    # Chỉ admin mới có thể rename subdashboard
    if session.get("role") != "admin":
        return jsonify({"success": False, "error": "Access denied. Admin role required."}), 403
    
    try:
        new_name = request.form.get("new_name", "").strip()
        
        if not new_name:
            return jsonify({"success": False, "error": "Subdashboard name cannot be empty"}), 400
        
        # Check if subdashboard exists
        subdash = db.get_subdashboard(sid) if hasattr(db, "get_subdashboard") else None
        if not subdash:
            return jsonify({"success": False, "error": "Subdashboard not found"}), 404
        
        old_name = subdash.get("name", "Unknown")
        
        # Update the subdashboard name
        db.update_subdashboard_row(sid, {"name": new_name})
        
        # Clear cache để navigation update
        from flask import current_app
        if hasattr(current_app, 'clear_subdashboards_cache'):
            current_app.clear_subdashboards_cache()
        
        return jsonify({
            "success": True,
            "message": f"Subdashboard renamed from '{old_name}' to '{new_name}' successfully"
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@subdash_bp.post("/<int:sid>/delete")
def delete_subdash(sid):
    # Chỉ admin mới có thể xóa subdashboard
    if session.get("role") != "admin":
        flash("Access denied. Admin role required.", "error")
        return redirect(url_for("subdash_bp.list_subdash"))
    
    try:
        # Lấy tên subdashboard trước khi xóa
        subdash = db.get_subdashboard(sid) if hasattr(db, "get_subdashboard") else {"name": "Unknown"}
        subdash_name = subdash.get("name", "Unknown")
        
        db.delete_subdashboard_row(sid)
        
        # Clear cache để navigation update
        from flask import current_app
        if hasattr(current_app, 'clear_subdashboards_cache'):
            current_app.clear_subdashboards_cache()
        
        flash(f"Subdashboard '{subdash_name}' has been deleted successfully.", "success")
    except Exception as e:
        flash(f"Error deleting subdashboard: {str(e)}", "error")
    
    # Redirect to dashboard to refresh navigation completely  
    return redirect(url_for("dashboard_bp.dashboard"))

@subdash_bp.route("/<int:sid>/add_group", methods=["POST"])
def add_group_to_subdash(sid):
    try:
        group_name = request.form.get("group_name")
        tag_ids = request.form.getlist("group_tags")
        
        if not group_name or not tag_ids:
            return jsonify({"success": False, "error": "Please provide group name and select at least one tag"}), 400
        
        # Add group to subdash_tag_groups
        group_id = db.add_subdash_group({"dashboard_id": sid, "name": group_name})
        
        # Add tags to subdash_group_tags
        if tag_ids:
            with db.init_engine().begin() as con:
                con.execute(
                    db.subdash_group_tags.insert(),
                    [{"group_id": group_id, "tag_id": int(tid)} for tid in tag_ids]
                )
        
        return jsonify({
            "success": True,
            "message": f"Group '{group_name}' created successfully with {len(tag_ids)} tags"
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@subdash_bp.route("/<int:sid>/group/<int:gid>/delete", methods=["POST"])
def delete_group(sid, gid):
    """Delete a specific group and all its tag associations."""
    try:
        # First check if the group exists and belongs to this subdashboard
        group = db.get_subdash_group(gid)
        if not group or group["dashboard_id"] != sid:
            return jsonify({"success": False, "error": "Group not found or doesn't belong to this dashboard"}), 404
        
        group_name = group.get("name", "Unknown")
        
        # Delete the group (this will cascade to delete group_tags due to ON DELETE CASCADE)
        db.delete_subdash_group(gid)
        
        # Force refresh subdashboard cache for real-time updates
        try:
            emission_manager = get_emission_manager()
            emission_manager.force_refresh_subdash_cache()
        except Exception as e:
            print(f"Warning: Could not refresh subdashboard cache: {e}")
        
        return jsonify({
            "success": True,
            "message": f"Group '{group_name}' deleted successfully"
        })
        
        # If this is an AJAX request, return JSON
        if request.headers.get('Content-Type') == 'application/json' or request.args.get('ajax') == '1':
            return {"success": True, "message": f"Group '{group['name']}' deleted successfully"}
        
        # Otherwise add flash message and redirect back to the subdashboard
        flash(f"Group '{group['name']}' deleted successfully.", "success")
        return redirect(url_for("subdash_bp.subdash_detail", sid=sid))
        
    except Exception as e:
        if request.headers.get('Content-Type') == 'application/json' or request.args.get('ajax') == '1':
            return {"success": False, "error": str(e)}, 500
        else:
            # Add flash message and redirect for regular form submission
            flash(f"Error deleting group: {str(e)}", "danger")
            return redirect(url_for("subdash_bp.subdash_detail", sid=sid))

@subdash_bp.route("/<int:sid>/group/<int:gid>/rename", methods=["POST"])
def rename_group(sid, gid):
    """Rename a specific group."""
    try:
        new_name = request.form.get("new_name", "").strip()
        
        if not new_name:
            return jsonify({"success": False, "error": "Group name cannot be empty"}), 400
        
        # First check if the group exists and belongs to this subdashboard
        group = db.get_subdash_group(gid)
        if not group or group["dashboard_id"] != sid:
            return jsonify({"success": False, "error": "Group not found or doesn't belong to this dashboard"}), 404
        
        old_name = group.get("name", "Unknown")
        
        # Update the group name
        db.update_subdash_group(gid, {"name": new_name})
        
        # Force refresh subdashboard cache for real-time updates
        try:
            emission_manager = get_emission_manager()
            emission_manager.force_refresh_subdash_cache()
        except Exception as e:
            print(f"Warning: Could not refresh subdashboard cache: {e}")
        
        return jsonify({
            "success": True,
            "message": f"Group renamed from '{old_name}' to '{new_name}' successfully"
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@subdash_bp.route("/<int:sid>/remove_tag", methods=["POST"])
def remove_tag_from_group(sid):
    """Remove a tag from a specific group in subdashboard."""
    try:
        tag_id = request.form.get("tag_id")
        group_id = request.form.get("group_id")
        
        if not tag_id or not group_id:
            return jsonify({"success": False, "error": "Missing tag ID or group ID"}), 400
        
        # Get tag and group info for response message
        tag = db.get_tag(int(tag_id))
        group = db.get_subdash_group(int(group_id))
        
        if not group or group["dashboard_id"] != sid:
            return jsonify({"success": False, "error": "Group not found or doesn't belong to this subdashboard"}), 404
        
        # Remove tag from group
        db.remove_tag_from_subdash_group(int(group_id), int(tag_id))
        
        # Force refresh subdashboard cache for real-time updates
        try:
            emission_manager = get_emission_manager()
            emission_manager.force_refresh_subdash_cache()
        except Exception as e:
            print(f"Warning: Could not refresh subdashboard cache: {e}")
        
        tag_name = tag.get("name", "Unknown") if tag else "Unknown"
        group_name = group.get("name", "Unknown")
        
        return jsonify({
            "success": True, 
            "message": f"Tag '{tag_name}' removed from group '{group_name}' successfully"
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

from flask import jsonify

@subdash_bp.get("/debug")
def debug_subdashboards():
    """Debug route to check subdashboards"""
    dashboards = db.list_subdashboards() if hasattr(db, "list_subdashboards") else []
    return jsonify({
        "count": len(dashboards),
        "dashboards": dashboards,
        "cache_info": "Check server logs for cache details"
    })

@subdash_bp.get("/<int:sid>/api/active_quad_alarms")
def api_active_quad_alarms(sid):
    """API endpoint to fetch current active quad alarms for a subdashboard.
    Used by client-side JS to periodically re-sync alarm visual state."""
    try:
        active_alarms = []
        if hasattr(db, "get_active_quad_alarms_by_subdash"):
            active_alarms = db.get_active_quad_alarms_by_subdash(sid)
        # Serialize datetime objects for JSON
        for alarm in active_alarms:
            if 'triggered_at' in alarm and alarm['triggered_at'] is not None:
                alarm['triggered_at'] = alarm['triggered_at'].isoformat()
        return jsonify({"success": True, "alarms": active_alarms})
    except Exception as e:
        print(f"⚠️ Error fetching active quad alarms: {e}")
        return jsonify({"success": False, "alarms": [], "error": str(e)}), 500

@subdash_bp.get("/<int:sid>/api/active_tag_alarms")
def api_active_tag_alarms(sid):
    """API endpoint to fetch current active per-tag alarms for all Qtag cards in a subdashboard.
    Reuses existing alarm_events table - returns tags whose latest alarm event is INCOMING."""
    try:
        active_alarms = []
        if hasattr(db, "get_active_tag_alarms_for_subdash"):
            active_alarms = db.get_active_tag_alarms_for_subdash(sid)
        for alarm in active_alarms:
            if 'triggered_at' in alarm and alarm['triggered_at'] is not None:
                try:
                    alarm['triggered_at'] = alarm['triggered_at'].isoformat()
                except Exception:
                    alarm['triggered_at'] = str(alarm['triggered_at'])
        return jsonify({"success": True, "alarms": active_alarms})
    except Exception as e:
        print(f"⚠️ Error fetching active tag alarms: {e}")
        return jsonify({"success": False, "alarms": [], "error": str(e)}), 500

@subdash_bp.get("/api/tags")
def api_tags_for_subdash():
    print("API request for subdashboard tags")
    try:
        sid = request.args.get("subdash", type=int)
        if not sid:
            return jsonify({"tags": []})
        
        # Get tag IDs for this subdashboard
        tag_ids = [t["id"] for t in db.get_subdashboard_tags(sid)]
        # Tối ưu: gom luôn info tag và value vào 1 query
        from ..database.db import tag_latest_values, tags as tags_table, init_engine
        tags = []
        with init_engine().connect() as con:
            rows = con.execute(
                select(
                    tag_latest_values.c.tag_id,
                    tag_latest_values.c.value,
                    tag_latest_values.c.ts,
                    tags_table.c.name,
                    tags_table.c.description,
                    tags_table.c.datatype,
                    tags_table.c.unit
                ).select_from(
                    tag_latest_values.join(tags_table, tag_latest_values.c.tag_id == tags_table.c.id)
                ).where(tag_latest_values.c.tag_id.in_(tag_ids))
            ).mappings().all()
            for row in rows:
                value = row['value']
                datatype = row['datatype']
                # Format giá trị theo datatype
                if datatype in ["Word", "Short", "DWord", "DInt", "Bit", "Signed", "Unsigned", "Long", "Long_inverse", "Hex", "Binary"]:
                    try:
                        if float(value).is_integer():
                            formatted_value = int(value)
                        else:
                            formatted_value = value
                    except (ValueError, TypeError):
                        formatted_value = value
                else:
                    formatted_value = value
                tag_info = {
                    "id": row["tag_id"],
                    "name": row["name"],
                    "description": row.get("description", ""),
                    "datatype": datatype,
                    "unit": row.get("unit", ""),
                    "value": formatted_value,
                    "ts": row["ts"].strftime("%H:%M") if row["ts"] else "--:--",
                    "alarm_status": "Normal",
                }
                tags.append(tag_info)
        # Đối với các tag không có data, vẫn trả về info
        missing_ids = set(tag_ids) - {t["id"] for t in tags}
        for tag_id in missing_ids:
            tag = db.get_tag(tag_id)
            if not tag:
                continue
            tag_info = {
                "id": tag_id,
                "name": tag["name"],
                "description": tag.get("description", ""),
                "datatype": tag.get("datatype", ""),
                "unit": tag.get("unit", ""),
                "value": None,
                "ts": "--:--",
                "alarm_status": "Normal",
            }
            tags.append(tag_info)
        return jsonify({"tags": tags})
    except Exception as e:
        print(f"Error in subdashboard /api/tags: {e}")
        return jsonify({"error": str(e)}), 500

@subdash_bp.route("/debug/<int:sid>")
def debug_subdash(sid):
    """Debug endpoint to check subdashboard data"""
    try:
        subdash = db.get_subdashboard(sid)
        tags = db.get_subdashboard_tags(sid)
        groups = db.list_subdashboard_groups(sid)
        
        debug_info = {
            "subdashboard": subdash,
            "tags": tags,
            "groups": groups,
            "tag_count": len(tags) if tags else 0,
            "group_count": len(groups) if groups else 0,
            "tag_ids": [t.get('id') for t in tags] if tags else []
        }
        
        return jsonify(debug_info)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@subdash_bp.route("/tags/<int:tag_id>/update-unit", methods=["POST"])
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

# ========== QUAD TAG ROUTES ==========

@subdash_bp.route("/<int:sid>/add_quad_tag", methods=["POST"])
def add_quad_tag_card(sid):
    """Add a new quad tag card to subdashboard (supports fix value mode for SV tags)"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    
    try:
        tag1_id = request.form.get("tag1_id")
        tag2_id = request.form.get("tag2_id") 
        group_id = request.form.get("group_id")
        new_group_name = request.form.get("new_group_name")
        card_title = request.form.get("card_title", "").strip()
        left_title = request.form.get("left_title", "").strip()
        right_title = request.form.get("right_title", "").strip()
        
        # PV tags are required
        if not all([tag1_id, tag2_id]):
            return jsonify({"success": False, "message": "Please select both PV tags"}), 400

        # SV fix value handling
        sv_left_type = request.form.get("sv_left_type", "tag")
        sv_right_type = request.form.get("sv_right_type", "tag")

        tag3_id = None
        sv_left_fixed = None
        if sv_left_type == 'fixed':
            fv = request.form.get("sv_left_fixed")
            sv_left_fixed = float(fv) if fv else None
        else:
            tag3_id_raw = request.form.get("tag3_id")
            if not tag3_id_raw:
                return jsonify({"success": False, "message": "SV Left tag is required when mode is 'tag'"}), 400
            tag3_id = int(tag3_id_raw)

        tag4_id = None
        sv_right_fixed = None
        if sv_right_type == 'fixed':
            fv = request.form.get("sv_right_fixed")
            sv_right_fixed = float(fv) if fv else None
        else:
            tag4_id_raw = request.form.get("tag4_id")
            if not tag4_id_raw:
                return jsonify({"success": False, "message": "SV Right tag is required when mode is 'tag'"}), 400
            tag4_id = int(tag4_id_raw)
            
        # Handle group assignment
        if new_group_name and new_group_name.strip():
            group_data = {
                "dashboard_id": sid,
                "name": new_group_name.strip(),
                "order": 0
            }
            group_id = db.add_subdash_group(group_data)
            message_suffix = f" in new group '{new_group_name}'"
        elif group_id:
            group_id = int(group_id)
            group = db.get_subdash_group(group_id)
            group_name = group.get("name", "Unknown") if group else "Unknown"
            message_suffix = f" in group '{group_name}'"
        else:
            return jsonify({"success": False, "message": "Please select a group or enter a new group name"}), 400
        
        quad_card_id = db.add_quad_tag_card(
            group_id, 
            int(tag1_id), int(tag2_id),
            tag3_id=tag3_id, tag4_id=tag4_id,
            card_title=card_title if card_title else None,
            left_title=left_title if left_title else None,
            right_title=right_title if right_title else None,
            sv_left_type=sv_left_type, sv_left_fixed=sv_left_fixed,
            sv_right_type=sv_right_type, sv_right_fixed=sv_right_fixed,
        )
        
        try:
            emission_manager = get_emission_manager()
            emission_manager.force_refresh_subdash_cache()
        except Exception as e:
            print(f"Warning: Could not refresh subdashboard cache: {e}")
        
        return jsonify({
            "success": True, 
            "message": f"Quad tag card added successfully{message_suffix}",
            "quad_card_id": quad_card_id
        })
        
    except Exception as e:
        print(f"Error adding quad tag card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@subdash_bp.route("/<int:sid>/delete_quad_card/<int:quad_id>", methods=["DELETE"])
def delete_quad_tag_card(sid, quad_id):
    """Delete a quad tag card"""
    # Check if user is admin
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    
    try:
        # Verify quad card exists and belongs to this subdashboard
        quad_card = db.get_quad_card_by_id(quad_id)
        if not quad_card:
            return jsonify({"success": False, "message": "Quad card not found"}), 404
        
        # Delete the quad card
        result = db.delete_quad_card(quad_id)
        
        if result:
            # Force refresh subdashboard cache for real-time updates
            try:
                emission_manager = get_emission_manager()
                emission_manager.force_refresh_subdash_cache()
            except Exception as e:
                print(f"Warning: Could not refresh subdashboard cache: {e}")
            
            return jsonify({"success": True, "message": "Quad tag card deleted successfully"})
        else:
            return jsonify({"success": False, "message": "Failed to delete quad card"}), 500
            
    except Exception as e:
        print(f"Error deleting quad tag card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@subdash_bp.route("/<int:sid>/update_quad_tags/<int:quad_id>", methods=["POST"])
def update_quad_tags(sid, quad_id):
    """Update tags in a quad card (supports fix value mode for SV tags)"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    
    try:
        tag1_id = request.form.get("tag1_id")
        tag2_id = request.form.get("tag2_id")
        
        if not all([tag1_id, tag2_id]):
            return jsonify({"success": False, "message": "Both PV tags are required"}), 400
        
        tag1_id = int(tag1_id)
        tag2_id = int(tag2_id)

        # SV fix value handling
        sv_left_type = request.form.get("sv_left_type", "tag")
        sv_right_type = request.form.get("sv_right_type", "tag")

        tag3_id = None
        sv_left_fixed = None
        if sv_left_type == 'fixed':
            fv = request.form.get("sv_left_fixed")
            sv_left_fixed = float(fv) if fv else None
        else:
            t = request.form.get("tag3_id")
            if not t:
                return jsonify({"success": False, "message": "SV Left tag is required when mode is 'tag'"}), 400
            tag3_id = int(t)

        tag4_id = None
        sv_right_fixed = None
        if sv_right_type == 'fixed':
            fv = request.form.get("sv_right_fixed")
            sv_right_fixed = float(fv) if fv else None
        else:
            t = request.form.get("tag4_id")
            if not t:
                return jsonify({"success": False, "message": "SV Right tag is required when mode is 'tag'"}), 400
            tag4_id = int(t)
        
        quad_card = db.get_quad_card_by_id(quad_id)
        if not quad_card:
            return jsonify({"success": False, "message": "Quad card not found"}), 404
        
        result = db.update_quad_card(
            quad_id, tag1_id, tag2_id,
            tag3_id=tag3_id, tag4_id=tag4_id,
            sv_left_type=sv_left_type, sv_left_fixed=sv_left_fixed,
            sv_right_type=sv_right_type, sv_right_fixed=sv_right_fixed,
        )
        
        if result:
            try:
                emission_manager = get_emission_manager()
                emission_manager.force_refresh_subdash_cache()
            except Exception as e:
                print(f"Warning: Could not refresh subdashboard cache: {e}")
            
            return jsonify({"success": True, "message": "Quad tags updated successfully"})
        else:
            return jsonify({"success": False, "message": "Failed to update quad tags"}), 500
            
    except ValueError as e:
        return jsonify({"success": False, "message": "Invalid tag IDs"}), 400
    except Exception as e:
        print(f"Error updating quad tags: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@subdash_bp.route("/<int:sid>/update_quad_card/<int:quad_id>", methods=["POST"])
def update_quad_tag_card(sid, quad_id):
    """Update a quad tag card (supports fix value mode for SV tags)"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    
    try:
        tag1_id = request.form.get("tag1_id")
        tag2_id = request.form.get("tag2_id")
        
        if not all([tag1_id, tag2_id]):
            return jsonify({"success": False, "message": "Please select both PV tags"}), 400

        # SV fix value handling
        sv_left_type = request.form.get("sv_left_type", "tag")
        sv_right_type = request.form.get("sv_right_type", "tag")

        tag3_id = None
        sv_left_fixed = None
        if sv_left_type == 'fixed':
            fv = request.form.get("sv_left_fixed")
            sv_left_fixed = float(fv) if fv else None
        else:
            t = request.form.get("tag3_id")
            if not t:
                return jsonify({"success": False, "message": "SV Left tag is required"}), 400
            tag3_id = int(t)

        tag4_id = None
        sv_right_fixed = None
        if sv_right_type == 'fixed':
            fv = request.form.get("sv_right_fixed")
            sv_right_fixed = float(fv) if fv else None
        else:
            t = request.form.get("tag4_id")
            if not t:
                return jsonify({"success": False, "message": "SV Right tag is required"}), 400
            tag4_id = int(t)
        
        card_title = request.form.get("card_title", "").strip() or None
        left_title = request.form.get("left_title", "").strip() or None
        right_title = request.form.get("right_title", "").strip() or None
        
        quad_card = db.get_quad_card_by_id(quad_id)
        if not quad_card:
            return jsonify({"success": False, "message": "Quad card not found"}), 404
        
        result = db.update_quad_card(
            quad_id, int(tag1_id), int(tag2_id),
            tag3_id=tag3_id, tag4_id=tag4_id,
            card_title=card_title, left_title=left_title, right_title=right_title,
            sv_left_type=sv_left_type, sv_left_fixed=sv_left_fixed,
            sv_right_type=sv_right_type, sv_right_fixed=sv_right_fixed,
        )
        
        if result:
            try:
                emission_manager = get_emission_manager()
                emission_manager.force_refresh_subdash_cache()
            except Exception as e:
                print(f"Warning: Could not refresh subdashboard cache: {e}")
            
            return jsonify({"success": True, "message": "Quad tag card updated successfully"})
        else:
            return jsonify({"success": False, "message": "Failed to update quad card"}), 500
            
    except Exception as e:
        print(f"Error updating quad tag card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


# ========== QUAD TAG CONDITIONS ROUTES ==========

@subdash_bp.route("/<int:sid>/quad_condition/<int:quad_id>", methods=["GET"])
def get_quad_condition(sid, quad_id):
    """Lấy điều kiện alarm của quad tag card"""
    try:
        condition = db.get_quad_condition(quad_id)
        if condition:
            return jsonify({"success": True, "condition": condition})
        else:
            # Trả về cấu trúc rỗng nếu chưa có condition
            return jsonify({"success": True, "condition": None})
    except Exception as e:
        print(f"Error getting quad condition: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/quad_condition/<int:quad_id>", methods=["POST"])
def save_quad_condition(sid, quad_id):
    """Lưu điều kiện alarm cho quad tag card"""
    try:
        data = request.get_json()
        
        # Chuẩn bị dữ liệu để lưu
        conditions_data = {
            "enabled": data.get("enabled", True),
            
            # Left column
            "left_high_operator": data.get("left_high_operator"),
            "left_high_compare_type": data.get("left_high_compare_type"),
            "left_high_value": data.get("left_high_value"),
            "left_high_compare_tag_id": data.get("left_high_compare_tag_id"),
            "left_high_on_stable": data.get("left_high_on_stable", 10),
            "left_high_off_stable": data.get("left_high_off_stable", 30),
            
            "left_low_operator": data.get("left_low_operator"),
            "left_low_compare_type": data.get("left_low_compare_type"),
            "left_low_value": data.get("left_low_value"),
            "left_low_compare_tag_id": data.get("left_low_compare_tag_id"),
            "left_low_on_stable": data.get("left_low_on_stable", 10),
            "left_low_off_stable": data.get("left_low_off_stable", 30),
            
            "left_email": data.get("left_email"),
            "left_sms": data.get("left_sms"),
            "left_description": data.get("left_description"),
            
            # Right column
            "right_high_operator": data.get("right_high_operator"),
            "right_high_compare_type": data.get("right_high_compare_type"),
            "right_high_value": data.get("right_high_value"),
            "right_high_compare_tag_id": data.get("right_high_compare_tag_id"),
            "right_high_on_stable": data.get("right_high_on_stable", 10),
            "right_high_off_stable": data.get("right_high_off_stable", 30),
            
            "right_low_operator": data.get("right_low_operator"),
            "right_low_compare_type": data.get("right_low_compare_type"),
            "right_low_value": data.get("right_low_value"),
            "right_low_compare_tag_id": data.get("right_low_compare_tag_id"),
            "right_low_on_stable": data.get("right_low_on_stable", 10),
            "right_low_off_stable": data.get("right_low_off_stable", 30),
            
            "right_email": data.get("right_email"),
            "right_sms": data.get("right_sms"),
            "right_description": data.get("right_description"),
        }
        
        # Lưu vào database
        condition_id = db.save_quad_condition(quad_id, conditions_data)

        # Nếu disable condition → xóa alarm states để tránh alarm cũ vẫn hiển thị
        if not conditions_data.get("enabled", True):
            try:
                cleared = db.clear_quad_alarm_states_for_card(quad_id)
                print(f"🧹 Cleared {cleared} quad alarm state(s) for quad {quad_id} (condition disabled)")
            except Exception as e:
                print(f"Warning: Could not clear quad alarm states: {e}")
        
        return jsonify({
            "success": True,
            "message": "Điều kiện đã được lưu thành công",
            "condition_id": condition_id
        })
        
    except Exception as e:
        print(f"Error saving quad condition: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/quad_condition/<int:quad_id>", methods=["DELETE"])
def delete_quad_condition_route(sid, quad_id):
    """Xóa điều kiện alarm của quad tag card"""
    try:
        result = db.delete_quad_condition(quad_id)
        if result:
            return jsonify({"success": True, "message": "Điều kiện đã được xóa"})
        else:
            return jsonify({"success": False, "message": "Không tìm thấy điều kiện"}), 404
    except Exception as e:
        print(f"Error deleting quad condition: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


# ========== CARD ALARM CONDITIONS ROUTES (Qtag6, Single3, PV Only, PV Dual) ==========

VALID_CARD_TYPES = {'qtag6', 'single3', 'pv_only', 'pv_dual'}

@subdash_bp.route("/<int:sid>/card_condition/<card_type>/<int:card_id>", methods=["GET"])
def get_card_condition(sid, card_type, card_id):
    """Get alarm condition for a card"""
    if card_type not in VALID_CARD_TYPES:
        return jsonify({"success": False, "message": "Invalid card type"}), 400
    try:
        condition = db.get_card_alarm_condition(card_type, card_id)
        return jsonify({"success": True, "condition": condition})
    except Exception as e:
        print(f"Error getting card condition: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/card_condition/<card_type>/<int:card_id>", methods=["POST"])
def save_card_condition(sid, card_type, card_id):
    """Save alarm condition for a card"""
    if card_type not in VALID_CARD_TYPES:
        return jsonify({"success": False, "message": "Invalid card type"}), 400
    try:
        data = request.get_json()
        conditions_data = {
            "enabled": data.get("enabled", True),
            # Left column
            "left_high_operator": data.get("left_high_operator"),
            "left_high_compare_type": data.get("left_high_compare_type"),
            "left_high_value": data.get("left_high_value"),
            "left_high_compare_tag_id": data.get("left_high_compare_tag_id"),
            "left_high_on_stable": data.get("left_high_on_stable", 10),
            "left_high_off_stable": data.get("left_high_off_stable", 30),
            "left_low_operator": data.get("left_low_operator"),
            "left_low_compare_type": data.get("left_low_compare_type"),
            "left_low_value": data.get("left_low_value"),
            "left_low_compare_tag_id": data.get("left_low_compare_tag_id"),
            "left_low_on_stable": data.get("left_low_on_stable", 10),
            "left_low_off_stable": data.get("left_low_off_stable", 30),
            "left_email": data.get("left_email"),
            "left_sms": data.get("left_sms"),
            "left_description": data.get("left_description"),
            # Right column (only used for dual-column types)
            "right_high_operator": data.get("right_high_operator"),
            "right_high_compare_type": data.get("right_high_compare_type"),
            "right_high_value": data.get("right_high_value"),
            "right_high_compare_tag_id": data.get("right_high_compare_tag_id"),
            "right_high_on_stable": data.get("right_high_on_stable", 10),
            "right_high_off_stable": data.get("right_high_off_stable", 30),
            "right_low_operator": data.get("right_low_operator"),
            "right_low_compare_type": data.get("right_low_compare_type"),
            "right_low_value": data.get("right_low_value"),
            "right_low_compare_tag_id": data.get("right_low_compare_tag_id"),
            "right_low_on_stable": data.get("right_low_on_stable", 10),
            "right_low_off_stable": data.get("right_low_off_stable", 30),
            "right_email": data.get("right_email"),
            "right_sms": data.get("right_sms"),
            "right_description": data.get("right_description"),
        }
        condition_id = db.save_card_alarm_condition(card_type, card_id, conditions_data)

        # Nếu disable condition → xóa alarm states để tránh alarm cũ vẫn hiển thị
        if not conditions_data.get("enabled", True):
            try:
                cleared = db.clear_card_alarm_states(card_type, card_id)
                print(f"🧹 Cleared {cleared} card alarm state(s) for {card_type}/{card_id} (condition disabled)")
            except Exception as e:
                print(f"Warning: Could not clear card alarm states: {e}")

        return jsonify({
            "success": True,
            "message": "Alarm condition saved successfully",
            "condition_id": condition_id
        })
    except Exception as e:
        print(f"Error saving card condition: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/card_condition/<card_type>/<int:card_id>", methods=["DELETE"])
def delete_card_condition(sid, card_type, card_id):
    """Delete alarm condition for a card"""
    if card_type not in VALID_CARD_TYPES:
        return jsonify({"success": False, "message": "Invalid card type"}), 400
    try:
        result = db.delete_card_alarm_condition(card_type, card_id)
        if result:
            return jsonify({"success": True, "message": "Alarm condition deleted"})
        else:
            return jsonify({"success": False, "message": "Condition not found"}), 404
    except Exception as e:
        print(f"Error deleting card condition: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.get("/<int:sid>/api/active_card_alarms")
def api_active_card_alarms(sid):
    """API endpoint to fetch active card alarm states for a subdashboard."""
    try:
        active_alarms = []
        if hasattr(db, "get_active_card_alarms_by_subdash"):
            active_alarms = db.get_active_card_alarms_by_subdash(sid)
        for alarm in active_alarms:
            if 'triggered_at' in alarm and alarm['triggered_at'] is not None:
                try:
                    alarm['triggered_at'] = alarm['triggered_at'].isoformat()
                except Exception:
                    alarm['triggered_at'] = str(alarm['triggered_at'])
        return jsonify({"success": True, "alarms": active_alarms})
    except Exception as e:
        print(f"⚠️ Error fetching active card alarms: {e}")
        return jsonify({"success": False, "alarms": [], "error": str(e)}), 500


# ========== QUAD TAG TITLES ROUTES ==========

@subdash_bp.route("/<int:sid>/quad_card/<int:quad_id>/rename", methods=["POST"])
def rename_quad_card_title(sid, quad_id):
    """Đổi tên các tiêu đề của quad tag card"""
    # Check if user is admin
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    
    try:
        data = request.get_json() or {}
        target = data.get("target")  # 'card', 'left', or 'right'
        new_title = data.get("title", "").strip()
        
        if not target or not new_title:
            return jsonify({"success": False, "message": "Target và title không được để trống"}), 400
        
        # Verify quad card exists
        quad_card = db.get_quad_card_by_id(quad_id)
        if not quad_card:
            return jsonify({"success": False, "message": "Quad card không tồn tại"}), 404
        
        # Update the appropriate title
        if target == "card":
            result = db.update_quad_card_titles(quad_id, card_title=new_title)
        elif target == "left":
            result = db.update_quad_card_titles(quad_id, left_title=new_title)
        elif target == "right":
            result = db.update_quad_card_titles(quad_id, right_title=new_title)
        else:
            return jsonify({"success": False, "message": "Target không hợp lệ"}), 400
        
        if result:
            # Force refresh subdashboard cache for real-time updates
            try:
                emission_manager = get_emission_manager()
                emission_manager.force_refresh_subdash_cache()
            except Exception as e:
                print(f"Warning: Could not refresh subdashboard cache: {e}")
            
            return jsonify({"success": True, "message": "Đã cập nhật tiêu đề thành công"})
        else:
            return jsonify({"success": False, "message": "Không thể cập nhật tiêu đề"}), 500
            
    except Exception as e:
        print(f"Error renaming quad card title: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.get("/demo-qtag6")
def demo_qtag6():
    """Demo page for Qtag 6 Tag layout (CRS-3B).
    Displays mockup UI with simulated realtime data for review before integration.
    """
    return render_template("subdashboards/demo_qtag6.html")


# ========== QTAG6 ROUTES (6 tags) ==========

@subdash_bp.route("/<int:sid>/add_qtag6", methods=["POST"])
def add_qtag6_card(sid):
    """Add a new qtag6 card to subdashboard (supports fix value mode for SV tags)"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        # PV tags (tag1, tag2) are required
        pv_ids = []
        for i in [1, 2]:
            t = request.form.get(f"tag{i}_id")
            if not t:
                return jsonify({"success": False, "message": f"PV Tag {i} is required"}), 400
            pv_ids.append(int(t))

        # SV type/fixed/tag_id mapping: tag position -> (form prefix, type col, fixed col)
        sv_config = {
            3: ('left_sv_high', 'left_sv_high_type', 'left_sv_high_fixed'),
            4: ('right_sv_high', 'right_sv_high_type', 'right_sv_high_fixed'),
            5: ('left_sv_low', 'left_sv_low_type', 'left_sv_low_fixed'),
            6: ('right_sv_low', 'right_sv_low_type', 'right_sv_low_fixed'),
        }

        sv_kwargs = {}
        sv_tag_ids = {}
        for pos, (prefix, type_col, fixed_col) in sv_config.items():
            sv_type = request.form.get(f"{prefix}_type", "tag")
            sv_kwargs[type_col] = sv_type
            if sv_type == 'fixed':
                fv = request.form.get(f"{prefix}_fixed")
                sv_kwargs[fixed_col] = float(fv) if fv else None
                sv_tag_ids[pos] = None
            else:
                t = request.form.get(f"tag{pos}_id")
                if not t:
                    return jsonify({"success": False, "message": f"SV Tag {pos} is required when mode is 'tag'"}), 400
                sv_tag_ids[pos] = int(t)
                sv_kwargs[fixed_col] = None

        group_id = request.form.get("group_id")
        new_group_name = request.form.get("new_group_name", "").strip()
        card_title = request.form.get("card_title", "").strip() or None
        left_title = request.form.get("left_title", "").strip() or None
        right_title = request.form.get("right_title", "").strip() or None

        if group_id:
            group_id = int(group_id)
        elif new_group_name:
            group_id = db.add_subdash_group({"dashboard_id": sid, "name": new_group_name, "order": 0})
        else:
            return jsonify({"success": False, "message": "Please select a group or enter a new group name"}), 400

        card_id = db.add_qtag6_card(
            group_id,
            tag1_id=pv_ids[0], tag2_id=pv_ids[1],
            tag3_id=sv_tag_ids.get(3), tag4_id=sv_tag_ids.get(4),
            tag5_id=sv_tag_ids.get(5), tag6_id=sv_tag_ids.get(6),
            card_title=card_title, left_title=left_title, right_title=right_title,
            **sv_kwargs
        )
        try:
            get_emission_manager().force_refresh_subdash_cache()
        except Exception:
            pass

        return jsonify({"success": True, "message": "Qtag6 card added successfully", "card_id": card_id})
    except Exception as e:
        print(f"Error adding qtag6 card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/update_qtag6/<int:card_id>", methods=["POST"])
def update_qtag6_card_route(sid, card_id):
    """Update a qtag6 card (supports fix value mode for SV tags)"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        existing = db.get_qtag6_card_by_id(card_id)
        if not existing:
            return jsonify({"success": False, "message": "Card not found"}), 404

        kwargs = {}
        # PV tags
        for i in [1, 2]:
            t = request.form.get(f"tag{i}_id")
            if t:
                kwargs[f'tag{i}_id'] = int(t)

        # SV fix value fields
        sv_config = {
            3: ('left_sv_high', 'left_sv_high_type', 'left_sv_high_fixed'),
            4: ('right_sv_high', 'right_sv_high_type', 'right_sv_high_fixed'),
            5: ('left_sv_low', 'left_sv_low_type', 'left_sv_low_fixed'),
            6: ('right_sv_low', 'right_sv_low_type', 'right_sv_low_fixed'),
        }
        for pos, (prefix, type_col, fixed_col) in sv_config.items():
            sv_type = request.form.get(f"{prefix}_type")
            if sv_type:
                kwargs[type_col] = sv_type
                if sv_type == 'fixed':
                    fv = request.form.get(f"{prefix}_fixed")
                    kwargs[fixed_col] = float(fv) if fv else None
                    kwargs[f'tag{pos}_id'] = None
                else:
                    t = request.form.get(f"tag{pos}_id")
                    kwargs[f'tag{pos}_id'] = int(t) if t else None
                    kwargs[fixed_col] = None

        for field in ('card_title', 'left_title', 'right_title'):
            v = request.form.get(field)
            if v is not None:
                kwargs[field] = v.strip() or None

        result = db.update_qtag6_card(card_id, **kwargs)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Qtag6 card updated successfully"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        print(f"Error updating qtag6 card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/delete_qtag6/<int:card_id>", methods=["DELETE"])
def delete_qtag6_card_route(sid, card_id):
    """Delete a qtag6 card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        if not db.get_qtag6_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        result = db.delete_qtag6_card(card_id)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Qtag6 card deleted successfully"})
        return jsonify({"success": False, "message": "Failed to delete"}), 500
    except Exception as e:
        print(f"Error deleting qtag6 card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/qtag6/<int:card_id>/rename", methods=["POST"])
def rename_qtag6_card(sid, card_id):
    """Rename qtag6 card titles"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        data = request.get_json() or {}
        target = data.get("target")
        new_title = data.get("title", "").strip()
        if not target or not new_title:
            return jsonify({"success": False, "message": "Target and title required"}), 400
        if not db.get_qtag6_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        kwargs = {}
        if target == "card":
            kwargs['card_title'] = new_title
        elif target == "left":
            kwargs['left_title'] = new_title
        elif target == "right":
            kwargs['right_title'] = new_title
        else:
            return jsonify({"success": False, "message": "Invalid target"}), 400
        result = db.update_qtag6_card(card_id, **kwargs)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Title updated"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


# ========== QTAG4 ROUTES (2 columns: PV + SV each) ==========

@subdash_bp.route("/<int:sid>/add_qtag4", methods=["POST"])
def add_qtag4_card(sid):
    """Add a new qtag4 card to subdashboard (supports fix value mode for SV tags)"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        pv_ids = []
        for i in [1, 2]:
            t = request.form.get(f"tag{i}_id")
            if not t:
                return jsonify({"success": False, "message": f"PV Tag {i} is required"}), 400
            pv_ids.append(int(t))

        sv_config = {
            3: ('left_sv', 'left_sv_type', 'left_sv_fixed'),
            4: ('right_sv', 'right_sv_type', 'right_sv_fixed'),
        }

        sv_kwargs = {}
        sv_tag_ids = {}
        for pos, (prefix, type_col, fixed_col) in sv_config.items():
            sv_type = request.form.get(f"{prefix}_type", "tag")
            sv_kwargs[type_col] = sv_type
            if sv_type == 'fixed':
                fv = request.form.get(f"{prefix}_fixed")
                sv_kwargs[fixed_col] = float(fv) if fv else None
                sv_tag_ids[pos] = None
            else:
                t = request.form.get(f"tag{pos}_id")
                if not t:
                    return jsonify({"success": False, "message": f"SV Tag {pos} is required when mode is 'tag'"}), 400
                sv_tag_ids[pos] = int(t)
                sv_kwargs[fixed_col] = None

        group_id = request.form.get("group_id")
        new_group_name = request.form.get("new_group_name", "").strip()
        card_title = request.form.get("card_title", "").strip() or None
        left_title = request.form.get("left_title", "").strip() or None
        right_title = request.form.get("right_title", "").strip() or None

        if group_id:
            group_id = int(group_id)
        elif new_group_name:
            group_id = db.add_subdash_group({"dashboard_id": sid, "name": new_group_name, "order": 0})
        else:
            return jsonify({"success": False, "message": "Please select a group or enter a new group name"}), 400

        card_id = db.add_qtag4_card(
            group_id,
            tag1_id=pv_ids[0], tag2_id=pv_ids[1],
            tag3_id=sv_tag_ids.get(3), tag4_id=sv_tag_ids.get(4),
            card_title=card_title, left_title=left_title, right_title=right_title,
            **sv_kwargs
        )
        try:
            get_emission_manager().force_refresh_subdash_cache()
        except Exception:
            pass
        return jsonify({"success": True, "message": "Qtag4 card added successfully", "card_id": card_id})
    except Exception as e:
        print(f"Error adding qtag4 card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/update_qtag4/<int:card_id>", methods=["POST"])
def update_qtag4_card_route(sid, card_id):
    """Update a qtag4 card (supports fix value mode for SV tags)"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        existing = db.get_qtag4_card_by_id(card_id)
        if not existing:
            return jsonify({"success": False, "message": "Card not found"}), 404

        kwargs = {}
        for i in [1, 2]:
            t = request.form.get(f"tag{i}_id")
            if t:
                kwargs[f'tag{i}_id'] = int(t)

        sv_config = {
            3: ('left_sv', 'left_sv_type', 'left_sv_fixed'),
            4: ('right_sv', 'right_sv_type', 'right_sv_fixed'),
        }
        for pos, (prefix, type_col, fixed_col) in sv_config.items():
            sv_type = request.form.get(f"{prefix}_type")
            if sv_type:
                kwargs[type_col] = sv_type
                if sv_type == 'fixed':
                    fv = request.form.get(f"{prefix}_fixed")
                    kwargs[fixed_col] = float(fv) if fv else None
                    kwargs[f'tag{pos}_id'] = None
                else:
                    t = request.form.get(f"tag{pos}_id")
                    kwargs[f'tag{pos}_id'] = int(t) if t else None
                    kwargs[fixed_col] = None

        for field in ('card_title', 'left_title', 'right_title'):
            v = request.form.get(field)
            if v is not None:
                kwargs[field] = v.strip() or None

        result = db.update_qtag4_card(card_id, **kwargs)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Qtag4 card updated successfully"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        print(f"Error updating qtag4 card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/delete_qtag4/<int:card_id>", methods=["DELETE"])
def delete_qtag4_card_route(sid, card_id):
    """Delete a qtag4 card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        if not db.get_qtag4_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        result = db.delete_qtag4_card(card_id)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Qtag4 card deleted successfully"})
        return jsonify({"success": False, "message": "Failed to delete"}), 500
    except Exception as e:
        print(f"Error deleting qtag4 card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/qtag4/<int:card_id>/rename", methods=["POST"])
def rename_qtag4_card(sid, card_id):
    """Rename qtag4 card titles"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        data = request.get_json() or {}
        target = data.get("target")
        new_title = data.get("title", "").strip()
        if not target or not new_title:
            return jsonify({"success": False, "message": "Target and title required"}), 400
        if not db.get_qtag4_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        kwargs = {}
        if target == "card":
            kwargs['card_title'] = new_title
        elif target == "left":
            kwargs['left_title'] = new_title
        elif target == "right":
            kwargs['right_title'] = new_title
        else:
            return jsonify({"success": False, "message": "Invalid target"}), 400
        result = db.update_qtag4_card(card_id, **kwargs)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Title updated"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


# ========== QTAG3 ROUTES (1 column: PV + SV HIGH + SV LOW, Qtag6-style) ==========

@subdash_bp.route("/<int:sid>/add_qtag3", methods=["POST"])
def add_qtag3_card(sid):
    """Add a new qtag3 card (1 column: PV + SV HIGH + SV LOW)"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        tag1_id = request.form.get("tag1_id")
        if not tag1_id:
            return jsonify({"success": False, "message": "PV Tag is required"}), 400
        tag1_id = int(tag1_id)

        # SV HIGH (tag2)
        sv_high_type = request.form.get("sv_high_type", "tag")
        sv_high_fixed = None
        tag2_id = None
        if sv_high_type == 'fixed':
            fv = request.form.get("sv_high_fixed")
            sv_high_fixed = float(fv) if fv else None
        else:
            t = request.form.get("tag2_id")
            tag2_id = int(t) if t else None

        # SV LOW (tag3)
        sv_low_type = request.form.get("sv_low_type", "tag")
        sv_low_fixed = None
        tag3_id = None
        if sv_low_type == 'fixed':
            fv = request.form.get("sv_low_fixed")
            sv_low_fixed = float(fv) if fv else None
        else:
            t = request.form.get("tag3_id")
            tag3_id = int(t) if t else None

        group_id = request.form.get("group_id")
        new_group_name = request.form.get("new_group_name", "").strip()
        card_title = request.form.get("card_title", "").strip() or None
        column_title = request.form.get("column_title", "").strip() or None

        if group_id:
            group_id = int(group_id)
        elif new_group_name:
            group_id = db.add_subdash_group({"dashboard_id": sid, "name": new_group_name, "order": 0})
        else:
            return jsonify({"success": False, "message": "Please select a group or enter a new group name"}), 400

        card_id = db.add_qtag3_card(
            group_id, tag1_id=tag1_id,
            tag2_id=tag2_id, tag3_id=tag3_id,
            card_title=card_title, column_title=column_title,
            sv_high_type=sv_high_type, sv_high_fixed=sv_high_fixed,
            sv_low_type=sv_low_type, sv_low_fixed=sv_low_fixed
        )
        try:
            get_emission_manager().force_refresh_subdash_cache()
        except Exception:
            pass
        return jsonify({"success": True, "message": "Qtag3 card added successfully", "card_id": card_id})
    except Exception as e:
        print(f"Error adding qtag3 card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/update_qtag3/<int:card_id>", methods=["POST"])
def update_qtag3_card_route(sid, card_id):
    """Update a qtag3 card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        existing = db.get_qtag3_card_by_id(card_id)
        if not existing:
            return jsonify({"success": False, "message": "Card not found"}), 404

        kwargs = {}
        t = request.form.get("tag1_id")
        if t:
            kwargs['tag1_id'] = int(t)

        # SV HIGH
        sv_high_type = request.form.get("sv_high_type")
        if sv_high_type:
            kwargs['sv_high_type'] = sv_high_type
            if sv_high_type == 'fixed':
                fv = request.form.get("sv_high_fixed")
                kwargs['sv_high_fixed'] = float(fv) if fv else None
                kwargs['tag2_id'] = None
            else:
                t = request.form.get("tag2_id")
                kwargs['tag2_id'] = int(t) if t else None
                kwargs['sv_high_fixed'] = None

        # SV LOW
        sv_low_type = request.form.get("sv_low_type")
        if sv_low_type:
            kwargs['sv_low_type'] = sv_low_type
            if sv_low_type == 'fixed':
                fv = request.form.get("sv_low_fixed")
                kwargs['sv_low_fixed'] = float(fv) if fv else None
                kwargs['tag3_id'] = None
            else:
                t = request.form.get("tag3_id")
                kwargs['tag3_id'] = int(t) if t else None
                kwargs['sv_low_fixed'] = None

        for field in ('card_title', 'column_title'):
            v = request.form.get(field)
            if v is not None:
                kwargs[field] = v.strip() or None

        result = db.update_qtag3_card(card_id, **kwargs)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Qtag3 card updated successfully"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        print(f"Error updating qtag3 card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/delete_qtag3/<int:card_id>", methods=["DELETE"])
def delete_qtag3_card_route(sid, card_id):
    """Delete a qtag3 card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        if not db.get_qtag3_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        result = db.delete_qtag3_card(card_id)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Qtag3 card deleted successfully"})
        return jsonify({"success": False, "message": "Failed to delete"}), 500
    except Exception as e:
        print(f"Error deleting qtag3 card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/qtag3/<int:card_id>/rename", methods=["POST"])
def rename_qtag3_card(sid, card_id):
    """Rename qtag3 card titles"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        data = request.get_json() or {}
        target = data.get("target")
        new_title = data.get("title", "").strip()
        if not target or not new_title:
            return jsonify({"success": False, "message": "Target and title required"}), 400
        if not db.get_qtag3_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        kwargs = {}
        if target == "card":
            kwargs['card_title'] = new_title
        elif target == "column":
            kwargs['column_title'] = new_title
        else:
            return jsonify({"success": False, "message": "Invalid target"}), 400
        result = db.update_qtag3_card(card_id, **kwargs)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Title updated"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


# ========== QTAG2 ROUTES (1 column: PV + SV, Qtag4-style) ==========

@subdash_bp.route("/<int:sid>/add_qtag2", methods=["POST"])
def add_qtag2_card(sid):
    """Add a new qtag2 card (1 column: PV + SV)"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        tag1_id = request.form.get("tag1_id")
        if not tag1_id:
            return jsonify({"success": False, "message": "PV Tag is required"}), 400
        tag1_id = int(tag1_id)

        # SV (tag2)
        sv_type = request.form.get("sv_type", "tag")
        sv_fixed = None
        tag2_id = None
        if sv_type == 'fixed':
            fv = request.form.get("sv_fixed")
            sv_fixed = float(fv) if fv else None
        else:
            t = request.form.get("tag2_id")
            tag2_id = int(t) if t else None

        group_id = request.form.get("group_id")
        new_group_name = request.form.get("new_group_name", "").strip()
        card_title = request.form.get("card_title", "").strip() or None
        column_title = request.form.get("column_title", "").strip() or None

        if group_id:
            group_id = int(group_id)
        elif new_group_name:
            group_id = db.add_subdash_group({"dashboard_id": sid, "name": new_group_name, "order": 0})
        else:
            return jsonify({"success": False, "message": "Please select a group or enter a new group name"}), 400

        card_id = db.add_qtag2_card(
            group_id, tag1_id=tag1_id,
            tag2_id=tag2_id,
            card_title=card_title, column_title=column_title,
            sv_type=sv_type, sv_fixed=sv_fixed
        )
        try:
            get_emission_manager().force_refresh_subdash_cache()
        except Exception:
            pass
        return jsonify({"success": True, "message": "Qtag2 card added successfully", "card_id": card_id})
    except Exception as e:
        print(f"Error adding qtag2 card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/update_qtag2/<int:card_id>", methods=["POST"])
def update_qtag2_card_route(sid, card_id):
    """Update a qtag2 card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        existing = db.get_qtag2_card_by_id(card_id)
        if not existing:
            return jsonify({"success": False, "message": "Card not found"}), 404

        kwargs = {}
        t = request.form.get("tag1_id")
        if t:
            kwargs['tag1_id'] = int(t)

        # SV
        sv_type = request.form.get("sv_type")
        if sv_type:
            kwargs['sv_type'] = sv_type
            if sv_type == 'fixed':
                fv = request.form.get("sv_fixed")
                kwargs['sv_fixed'] = float(fv) if fv else None
                kwargs['tag2_id'] = None
            else:
                t = request.form.get("tag2_id")
                kwargs['tag2_id'] = int(t) if t else None
                kwargs['sv_fixed'] = None

        for field in ('card_title', 'column_title'):
            v = request.form.get(field)
            if v is not None:
                kwargs[field] = v.strip() or None

        result = db.update_qtag2_card(card_id, **kwargs)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Qtag2 card updated successfully"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        print(f"Error updating qtag2 card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/delete_qtag2/<int:card_id>", methods=["DELETE"])
def delete_qtag2_card_route(sid, card_id):
    """Delete a qtag2 card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        if not db.get_qtag2_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        result = db.delete_qtag2_card(card_id)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Qtag2 card deleted successfully"})
        return jsonify({"success": False, "message": "Failed to delete"}), 500
    except Exception as e:
        print(f"Error deleting qtag2 card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/qtag2/<int:card_id>/rename", methods=["POST"])
def rename_qtag2_card(sid, card_id):
    """Rename qtag2 card titles"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        data = request.get_json() or {}
        target = data.get("target")
        new_title = data.get("title", "").strip()
        if not target or not new_title:
            return jsonify({"success": False, "message": "Target and title required"}), 400
        if not db.get_qtag2_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        kwargs = {}
        if target == "card":
            kwargs['card_title'] = new_title
        elif target == "column":
            kwargs['column_title'] = new_title
        else:
            return jsonify({"success": False, "message": "Invalid target"}), 400
        result = db.update_qtag2_card(card_id, **kwargs)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Title updated"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


# ========== QTAG SINGLE3 ROUTES (PV + SV HIGH/LOW) ==========

@subdash_bp.route("/<int:sid>/add_qtag_single3", methods=["POST"])
def add_qtag_single3_card(sid):
    """Add a new qtag single3 card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        pv_tag_id = request.form.get("pv_tag_id")
        if not pv_tag_id:
            return jsonify({"success": False, "message": "PV tag is required"}), 400

        group_id = request.form.get("group_id")
        new_group_name = request.form.get("new_group_name", "").strip()
        card_title = request.form.get("card_title", "").strip() or None

        sv_high_type = request.form.get("sv_high_type", "fixed")
        sv_high_tag_id = request.form.get("sv_high_tag_id")
        sv_high_fixed = request.form.get("sv_high_fixed")
        sv_low_type = request.form.get("sv_low_type", "fixed")
        sv_low_tag_id = request.form.get("sv_low_tag_id")
        sv_low_fixed = request.form.get("sv_low_fixed")

        # Priority: existing group > new group name (avoid duplicate groups)
        if group_id:
            group_id = int(group_id)
        elif new_group_name:
            group_id = db.add_subdash_group({"dashboard_id": sid, "name": new_group_name, "order": 0})
        else:
            return jsonify({"success": False, "message": "Please select a group or enter a new group name"}), 400

        card_id = db.add_qtag_single3_card(
            group_id=group_id,
            pv_tag_id=int(pv_tag_id),
            card_title=card_title,
            sv_high_type=sv_high_type,
            sv_high_tag_id=int(sv_high_tag_id) if sv_high_tag_id else None,
            sv_high_fixed=float(sv_high_fixed) if sv_high_fixed else None,
            sv_low_type=sv_low_type,
            sv_low_tag_id=int(sv_low_tag_id) if sv_low_tag_id else None,
            sv_low_fixed=float(sv_low_fixed) if sv_low_fixed else None,
        )
        try:
            get_emission_manager().force_refresh_subdash_cache()
        except Exception:
            pass
        return jsonify({"success": True, "message": "Qtag Single3 card added", "card_id": card_id})
    except Exception as e:
        print(f"Error adding qtag single3 card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/update_qtag_single3/<int:card_id>", methods=["POST"])
def update_qtag_single3_card_route(sid, card_id):
    """Update a qtag single3 card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        if not db.get_qtag_single3_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404

        kwargs = {}
        pv = request.form.get("pv_tag_id")
        if pv:
            kwargs['pv_tag_id'] = int(pv)
        for field in ('card_title',):
            v = request.form.get(field)
            if v is not None:
                kwargs[field] = v.strip() or None
        for prefix in ('sv_high', 'sv_low'):
            t = request.form.get(f"{prefix}_type")
            if t:
                kwargs[f'{prefix}_type'] = t
            tid = request.form.get(f"{prefix}_tag_id")
            kwargs[f'{prefix}_tag_id'] = int(tid) if tid else None
            fv = request.form.get(f"{prefix}_fixed")
            kwargs[f'{prefix}_fixed'] = float(fv) if fv else None

        result = db.update_qtag_single3_card(card_id, **kwargs)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Card updated"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        print(f"Error updating qtag single3: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/delete_qtag_single3/<int:card_id>", methods=["DELETE"])
def delete_qtag_single3_card_route(sid, card_id):
    """Delete a qtag single3 card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        if not db.get_qtag_single3_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        result = db.delete_qtag_single3_card(card_id)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Card deleted"})
        return jsonify({"success": False, "message": "Failed to delete"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/qtag_single3/<int:card_id>/rename", methods=["POST"])
def rename_qtag_single3_card(sid, card_id):
    """Rename qtag single3 card title"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        data = request.get_json() or {}
        new_title = data.get("title", "").strip()
        if not new_title:
            return jsonify({"success": False, "message": "Title required"}), 400
        if not db.get_qtag_single3_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        result = db.update_qtag_single3_card(card_id, card_title=new_title)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Title updated"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


# ========== QTAG PV ONLY ROUTES ==========

@subdash_bp.route("/<int:sid>/add_qtag_pv", methods=["POST"])
def add_qtag_pv_card(sid):
    """Add a new qtag PV only card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        pv_tag_id = request.form.get("pv_tag_id")
        if not pv_tag_id:
            return jsonify({"success": False, "message": "PV tag is required"}), 400

        group_id = request.form.get("group_id")
        new_group_name = request.form.get("new_group_name", "").strip()
        card_title = request.form.get("card_title", "").strip() or None
        description = request.form.get("description", "").strip() or None

        # Priority: existing group > new group name (avoid duplicate groups)
        if group_id:
            group_id = int(group_id)
        elif new_group_name:
            group_id = db.add_subdash_group({"dashboard_id": sid, "name": new_group_name, "order": 0})
        else:
            return jsonify({"success": False, "message": "Please select a group or enter a new group name"}), 400

        card_id = db.add_qtag_pv_card(group_id, int(pv_tag_id), card_title, description)
        try:
            get_emission_manager().force_refresh_subdash_cache()
        except Exception:
            pass
        return jsonify({"success": True, "message": "Qtag PV card added", "card_id": card_id})
    except Exception as e:
        print(f"Error adding qtag PV card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/update_qtag_pv/<int:card_id>", methods=["POST"])
def update_qtag_pv_card_route(sid, card_id):
    """Update a qtag PV only card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        if not db.get_qtag_pv_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        kwargs = {}
        pv = request.form.get("pv_tag_id")
        if pv:
            kwargs['pv_tag_id'] = int(pv)
        ct = request.form.get("card_title")
        if ct is not None:
            kwargs['card_title'] = ct.strip() or None
        desc = request.form.get("description")
        if desc is not None:
            kwargs['description'] = desc.strip() or None
        result = db.update_qtag_pv_card(card_id, **kwargs)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Card updated"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/delete_qtag_pv/<int:card_id>", methods=["DELETE"])
def delete_qtag_pv_card_route(sid, card_id):
    """Delete a qtag PV only card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        if not db.get_qtag_pv_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        result = db.delete_qtag_pv_card(card_id)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Card deleted"})
        return jsonify({"success": False, "message": "Failed to delete"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/qtag_pv/<int:card_id>/rename", methods=["POST"])
def rename_qtag_pv_card(sid, card_id):
    """Rename qtag PV card title"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        data = request.get_json() or {}
        new_title = data.get("title", "").strip()
        if not new_title:
            return jsonify({"success": False, "message": "Title required"}), 400
        if not db.get_qtag_pv_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        result = db.update_qtag_pv_card(card_id, card_title=new_title)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Title updated"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


# ========== QTAG PV DUAL CARD ROUTES ==========

@subdash_bp.route("/<int:sid>/add_qtag_pv_dual", methods=["POST"])
def add_qtag_pv_dual_card(sid):
    """Add a new qtag PV dual card (2 columns, 2 PV tags)"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        left_tag_id = request.form.get("left_tag_id")
        right_tag_id = request.form.get("right_tag_id")
        if not left_tag_id or not right_tag_id:
            return jsonify({"success": False, "message": "Both left and right PV tags are required"}), 400

        group_id = request.form.get("group_id")
        new_group_name = request.form.get("new_group_name", "").strip()
        card_title = request.form.get("card_title", "").strip() or None
        left_title = request.form.get("left_title", "").strip() or None
        right_title = request.form.get("right_title", "").strip() or None
        left_description = request.form.get("left_description", "").strip() or None
        right_description = request.form.get("right_description", "").strip() or None

        if group_id:
            group_id = int(group_id)
        elif new_group_name:
            group_id = db.add_subdash_group({"dashboard_id": sid, "name": new_group_name, "order": 0})
        else:
            return jsonify({"success": False, "message": "Please select a group or enter a new group name"}), 400

        card_id = db.add_qtag_pv_dual_card(group_id, int(left_tag_id), int(right_tag_id),
                                            card_title, left_title, right_title,
                                            left_description, right_description)
        try:
            get_emission_manager().force_refresh_subdash_cache()
        except Exception:
            pass
        return jsonify({"success": True, "message": "PV Dual card added", "card_id": card_id})
    except Exception as e:
        print(f"Error adding qtag PV dual card: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/update_qtag_pv_dual/<int:card_id>", methods=["POST"])
def update_qtag_pv_dual_card_route(sid, card_id):
    """Update a qtag PV dual card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        if not db.get_qtag_pv_dual_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        kwargs = {}
        left = request.form.get("left_tag_id")
        if left:
            kwargs['left_tag_id'] = int(left)
        right = request.form.get("right_tag_id")
        if right:
            kwargs['right_tag_id'] = int(right)
        ct = request.form.get("card_title")
        if ct is not None:
            kwargs['card_title'] = ct.strip() or None
        lt = request.form.get("left_title")
        if lt is not None:
            kwargs['left_title'] = lt.strip() or None
        rt = request.form.get("right_title")
        if rt is not None:
            kwargs['right_title'] = rt.strip() or None
        ld = request.form.get("left_description")
        if ld is not None:
            kwargs['left_description'] = ld.strip() or None
        rd = request.form.get("right_description")
        if rd is not None:
            kwargs['right_description'] = rd.strip() or None
        result = db.update_qtag_pv_dual_card(card_id, **kwargs)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Card updated"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/delete_qtag_pv_dual/<int:card_id>", methods=["DELETE"])
def delete_qtag_pv_dual_card_route(sid, card_id):
    """Delete a qtag PV dual card"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        if not db.get_qtag_pv_dual_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        result = db.delete_qtag_pv_dual_card(card_id)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Card deleted"})
        return jsonify({"success": False, "message": "Failed to delete"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/qtag_pv_dual/<int:card_id>/rename", methods=["POST"])
def rename_qtag_pv_dual_card(sid, card_id):
    """Rename qtag PV dual card (card title, left title, right title)"""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    try:
        data = request.get_json() or {}
        target = data.get("target", "card")
        new_title = data.get("title", "").strip()
        if not new_title:
            return jsonify({"success": False, "message": "Title required"}), 400
        if not db.get_qtag_pv_dual_card_by_id(card_id):
            return jsonify({"success": False, "message": "Card not found"}), 404
        kwargs = {}
        if target == "left":
            kwargs['left_title'] = new_title
        elif target == "right":
            kwargs['right_title'] = new_title
        else:
            kwargs['card_title'] = new_title
        result = db.update_qtag_pv_dual_card(card_id, **kwargs)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, "message": "Title updated"})
        return jsonify({"success": False, "message": "Failed to update"}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@subdash_bp.route("/<int:sid>/update_card_color", methods=["POST"])
def update_card_color(sid):
    """Update background color or sub-header color for any card type."""
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied"}), 403
    try:
        data = request.get_json() or {}
        card_type = data.get("card_type", "")
        card_id = data.get("card_id")
        color = data.get("color", "").strip()
        color_field = data.get("color_field", "card_color")

        if not card_type or not card_id:
            return jsonify({"success": False, "message": "card_type and card_id required"}), 400

        if card_type not in ("quad", "quad6", "qtag4", "qtag3", "qtag2", "single3", "pvonly", "pvdual"):
            return jsonify({"success": False, "message": "Invalid card_type"}), 400

        if color_field not in ("card_color", "sub_color", "sub_color_right"):
            return jsonify({"success": False, "message": "Invalid color_field"}), 400

        # Allow empty/null to reset to default
        result = db.update_card_color(card_type, int(card_id), color if color else None, color_field)
        if result:
            try:
                get_emission_manager().force_refresh_subdash_cache()
            except Exception:
                pass
            return jsonify({"success": True, color_field: color if color else None})
        return jsonify({"success": False, "message": "Card not found or update failed"}), 404
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500
