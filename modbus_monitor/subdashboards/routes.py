from flask import jsonify, render_template, request, redirect, url_for, flash
from . import subdash_bp
from datetime import datetime,timedelta
from modbus_monitor.database import db

@subdash_bp.get("/")
def list_subdash():
    # Lấy danh sách subdashboard từ DB (demo: chưa có bảng riêng thì hardcode)
    print("List subdashboards")
    dashboards = db.list_subdashboards() if hasattr(db, "list_subdashboards") else []
    return render_template("subdashboards/list.html", items=dashboards)

@subdash_bp.route("/add", methods=["GET", "POST"])
def add_subdash():
    if request.method == "POST":
        name = request.form.get("name")
        description = request.form.get("description")
        tag_ids = request.form.getlist("tag_ids")
        sid = db.add_subdashboard_row({"name": name, "description": description}, [int(t) for t in tag_ids])
        return redirect(url_for("subdash_bp.subdash_detail", sid=sid))
    all_tags = db.list_all_tags() if hasattr(db, "list_all_tags") else []
    return render_template("subdashboards/add.html", all_tags=all_tags)

@subdash_bp.get("/<int:sid>")
def subdash_detail(sid):
    subdash = db.get_subdashboard(sid) if hasattr(db, "get_subdashboard") else {"id": sid, "name": "Demo"}
    tags = db.get_subdashboard_tags(sid) if hasattr(db, "get_subdashboard_tags") else []
    all_tags = db.list_all_tags() if hasattr(db, "list_all_tags") else []
    
    # Debug logging
    print(f"Subdash {sid}: Found {len(tags)} tags")
    for tag in tags[:5]:  # Show first 5 tags
        print(f"  Tag {tag.get('id')}: {tag.get('tag_name')} - {tag.get('device_name')}")
    
    # Get groups for this specific subdashboard
    if hasattr(db, "list_subdash_groups_for_dashboard"):
        groups = [dict(g) for g in db.list_subdash_groups_for_dashboard(sid)]
    else:
        groups = []
    
    # Handle group filtering
    current_group = request.args.get('group', '__all__')
    
    print("G: ",groups)
    for g in groups:
        g["tags"] = db.get_tags_of_group(g["id"])
    
    return render_template("subdashboards/detail.html", 
                         subdash=subdash, 
                         tags=tags, 
                         all_tags=all_tags, 
                         groups=groups,
                         current_group=current_group)

@subdash_bp.route("/<int:sid>/add_tag", methods=["POST"])
def add_tag_to_subdash(sid):
    """Add tag to subdashboard with optional group assignment"""
    tag_id = request.form.get("tag_id")
    target_group = request.form.get("target_group")  # Existing group ID
    new_group_name = request.form.get("new_group_name")  # New group name
    
    if not tag_id:
        flash("Please select a tag", "error")
        return redirect(url_for("subdash_bp.subdash_detail", sid=sid))
    
    try:
        # Add tag to subdashboard first
        db.add_tag_to_subdashboard(sid, int(tag_id))
        
        # Handle group assignment
        group_id = None
        if new_group_name and new_group_name.strip():
            # Create new group
            group_data = {
                "dashboard_id": sid,
                "name": new_group_name.strip(),
                "order": 0
            }
            group_id = db.add_subdash_group(group_data)
            flash(f"Tag added successfully and new group '{new_group_name}' created", "success")
        elif target_group:
            # Use existing group
            group_id = int(target_group)
            group = db.get_subdash_group(group_id)
            group_name = group.get("name", "Unknown") if group else "Unknown"
            flash(f"Tag added successfully to group '{group_name}'", "success")
        else:
            flash("Tag added successfully", "success")
        
        # Add tag to group if group is specified
        if group_id:
            db.add_tag_to_subdash_group(group_id, int(tag_id))
            
    except Exception as e:
        flash(f"Error adding tag: {str(e)}", "error")
    
    return redirect(url_for("subdash_bp.subdash_detail", sid=sid))

@subdash_bp.post("/<int:sid>/delete")
def delete_subdash(sid):
    db.delete_subdashboard_row(sid)
    return redirect(url_for("subdash_bp.list_subdash"))

@subdash_bp.route("/<int:sid>/add_group", methods=["POST"])
def add_group_to_subdash(sid):
    group_name = request.form.get("group_name")
    tag_ids = request.form.getlist("group_tags")
    # Add group to subdash_tag_groups
    group_id = db.add_subdash_group({"dashboard_id": sid, "name": group_name})
    # Add tags to subdash_group_tags
    if tag_ids:
        with db.init_engine().begin() as con:
            con.execute(
                db.subdash_group_tags.insert(),
                [{"group_id": group_id, "tag_id": int(tid)} for tid in tag_ids]
            )
    return redirect(url_for("subdash_bp.subdash_detail", sid=sid))

@subdash_bp.route("/<int:sid>/groups/<int:gid>/delete", methods=["POST"])
def delete_group(sid, gid):
    """Delete a specific group and all its tag associations."""
    try:
        # First check if the group exists and belongs to this subdashboard
        group = db.get_subdash_group(gid)
        if not group or group["dashboard_id"] != sid:
            return {"success": False, "error": "Group not found or doesn't belong to this dashboard"}, 404
        
        # Delete the group (this will cascade to delete group_tags due to ON DELETE CASCADE)
        db.delete_subdash_group(gid)
        
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

from flask import jsonify

@subdash_bp.get("/api/tags")
def api_tags_for_subdash():
    try:
        sid = request.args.get("subdash", type=int)
        if not sid:
            return jsonify({"tags": []})
        
        # Get tag IDs for this subdashboard
        tag_ids = [t["id"] for t in db.get_subdashboard_tags(sid)]
        tags = []
        
        for tag_id in tag_ids:
            tag = db.get_tag(tag_id)
            if not tag:
                continue
                
            value, ts = db.get_latest_tag_value(tag_id)
            tag_info = {
                "id": tag_id,
                "name": tag["name"],
                "description": tag.get("description", ""),
                "datatype": tag.get("datatype", ""),
                "unit": tag.get("unit", ""),
                "value": value,
                "ts": ts.strftime("%H:%M") if ts else "--:--",
                "alarm_status": "Normal",  # You can add alarm logic here
            }
            tags.append(tag_info)
            
        print(f"Subdashboard {sid} tags: {tags}")
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