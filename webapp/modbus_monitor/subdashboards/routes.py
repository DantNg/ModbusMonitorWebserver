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

    # Get active quad alarms for this subdashboard
    active_quad_alarms = []
    if hasattr(db, "get_active_quad_alarms_by_subdash"):
        try:
            active_quad_alarms = db.get_active_quad_alarms_by_subdash(sid)
            print(f"🔔 Found {len(active_quad_alarms)} active quad alarms for subdashboard {sid}")
        except Exception as e:
            print(f"⚠️ Could not load active quad alarms: {e}")

    current_group = request.args.get('group', '__all__')

    response = make_response(render_template(
        "subdashboards/detail.html",
        subdash=subdash,
        all_tags=all_tags,
        groups=groups,
        current_group=current_group,
        active_quad_alarms=active_quad_alarms
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
    """Add a new quad tag card to subdashboard"""
    # Check if user is admin
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    
    try:
        tag1_id = request.form.get("tag1_id")
        tag2_id = request.form.get("tag2_id") 
        tag3_id = request.form.get("tag3_id")
        tag4_id = request.form.get("tag4_id")
        group_id = request.form.get("group_id")
        new_group_name = request.form.get("new_group_name")
        
        # Validate required fields
        if not all([tag1_id, tag2_id, tag3_id, tag4_id]):
            return jsonify({"success": False, "message": "Please select all 4 tags"}), 400
            
        # Convert to integers
        tag_ids = [int(tag1_id), int(tag2_id), int(tag3_id), int(tag4_id)]
        
        # Validate that all tags are different
        if len(set(tag_ids)) != 4:
            return jsonify({"success": False, "message": "All 4 tags must be different"}), 400
            
        # Handle group assignment
        if new_group_name and new_group_name.strip():
            # Create new group
            group_data = {
                "dashboard_id": sid,
                "name": new_group_name.strip(),
                "order": 0
            }
            group_id = db.add_subdash_group(group_data)
            message_suffix = f" in new group '{new_group_name}'"
        elif group_id:
            # Use existing group
            group_id = int(group_id)
            group = db.get_subdash_group(group_id)
            group_name = group.get("name", "Unknown") if group else "Unknown"
            message_suffix = f" in group '{group_name}'"
        else:
            return jsonify({"success": False, "message": "Please select a group or enter a new group name"}), 400
        
        # Add quad tag card
        quad_card_id = db.add_quad_tag_card(group_id, tag_ids[0], tag_ids[1], tag_ids[2], tag_ids[3])
        
        # Force refresh subdashboard cache for real-time updates
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
    """Update tags in a quad card"""
    # Check if user is admin
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    
    try:
        # Get form data
        tag1_id = request.form.get("tag1_id")
        tag2_id = request.form.get("tag2_id")
        tag3_id = request.form.get("tag3_id")
        tag4_id = request.form.get("tag4_id")
        
        # Validate all tags are present
        if not all([tag1_id, tag2_id, tag3_id, tag4_id]):
            return jsonify({"success": False, "message": "All 4 tags are required"}), 400
        
        # Convert to integers
        tag1_id = int(tag1_id)
        tag2_id = int(tag2_id)
        tag3_id = int(tag3_id)
        tag4_id = int(tag4_id)
        
        # Validate all tags are different
        tag_ids = [tag1_id, tag2_id, tag3_id, tag4_id]
        if len(set(tag_ids)) != 4:
            return jsonify({"success": False, "message": "All 4 tags must be different"}), 400
        
        # Verify quad card exists
        quad_card = db.get_quad_card_by_id(quad_id)
        if not quad_card:
            return jsonify({"success": False, "message": "Quad card not found"}), 404
        
        # Update the quad card tags
        result = db.update_quad_card(quad_id, tag1_id, tag2_id, tag3_id, tag4_id)
        
        if result:
            # Force refresh subdashboard cache for real-time updates
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
    """Update a quad tag card"""
    # Check if user is admin
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied. Admin role required."}), 403
    
    try:
        tag1_id = request.form.get("tag1_id")
        tag2_id = request.form.get("tag2_id")
        tag3_id = request.form.get("tag3_id")
        tag4_id = request.form.get("tag4_id")
        
        # Validate required fields
        if not all([tag1_id, tag2_id, tag3_id, tag4_id]):
            return jsonify({"success": False, "message": "Please select all 4 tags"}), 400
        
        # Convert to integers
        tag_ids = [int(tag1_id), int(tag2_id), int(tag3_id), int(tag4_id)]
        
        # Validate that all tags are different
        if len(set(tag_ids)) != 4:
            return jsonify({"success": False, "message": "All 4 tags must be different"}), 400
        
        # Verify quad card exists
        quad_card = db.get_quad_card_by_id(quad_id)
        if not quad_card:
            return jsonify({"success": False, "message": "Quad card not found"}), 404
        
        # Update the quad card
        result = db.update_quad_card(quad_id, tag_ids[0], tag_ids[1], tag_ids[2], tag_ids[3])
        
        if result:
            # Force refresh subdashboard cache for real-time updates
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
