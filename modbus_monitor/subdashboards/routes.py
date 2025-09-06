from flask import jsonify, render_template, request, redirect, url_for
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
    groups = db.list_subdash_groups() if hasattr(db, "list_subdash_groups") else []
    print("G: ",groups)
    groups = [dict(g) for g in groups]
    for g in groups:
        g["tags"] = db.get_tags_of_group(g["id"])
    return render_template("subdashboards/detail.html", subdash=subdash, tags=tags, all_tags=all_tags, groups=groups)

@subdash_bp.route("/<int:sid>/add_tag", methods=["POST"])
def add_tag_to_subdash(sid):
    tag_id = int(request.form.get("tag_id"))
    db.add_tag_to_subdashboard(sid, tag_id)  # <-- implement this function
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

from flask import jsonify

@subdash_bp.get("/api/tags")
def api_tags_for_subdash():
    sid = request.args.get("subdash", type=int)
    if not sid:
        return jsonify({"tags": []})
    # Get tag IDs for this subdashboard
    tag_ids = [t["id"] for t in db.get_subdashboard_tags(sid)]
    tags = []
    for tag_id in tag_ids:
        tag = db.get_tag(tag_id)
        value, ts = db.get_latest_tag_value(tag_id)
        tag_info = {
            "id": tag_id,
            "name": tag["name"],
            "description": tag.get("description"),
            "datatype": tag.get("datatype"),
            "unit": tag.get("unit"),
            "value": value,
            "ts": ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "",
            "alarm_status": "Normal",  # You can add alarm logic here
        }
        tags.append(tag_info)
    return jsonify({"tags": tags})