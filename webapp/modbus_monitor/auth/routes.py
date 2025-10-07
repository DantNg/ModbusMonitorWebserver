from flask import Blueprint, request, jsonify,redirect,url_for, render_template,session
from modbus_monitor.database import db
from . import auth_bp
from werkzeug.security import check_password_hash, generate_password_hash
@auth_bp.route("/")
def root():
    print("Hello")
    return redirect(url_for("auth_bp.login"))
@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        user = db.get_user_by_username(username)
        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            session["role"] = user["role"]
            return redirect(url_for("dashboard_bp.dashboard"))
        else:
            error = "Invalid username or password"
    return render_template("auth/login.html", error=error)
@auth_bp.route("/logout")
def logout():
    # Handle logout logic here
    return render_template("auth/login.html")

@auth_bp.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        email = request.form.get("email")
        # Logic to handle password reset (e.g., send email)
        return render_template("auth/forgot_password.html", success=True)
    return render_template("auth/forgot_password.html")


@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        email = request.form.get("email")
        # Logic to create a new user
        return redirect(url_for("auth_bp.login"))
    return render_template("auth/register.html")

@auth_bp.route("/user-management")
def user_management():
    users = db.list_users()
    return render_template("auth/user_management.html", users=users)

@auth_bp.route("/user-management/<username>/edit", methods=["GET", "POST"])
def edit_user(username):
    user = db.get_user_by_username(username)
    if not user:
        return "User not found", 404

    if request.method == "POST":
        data = {
            "username": request.form.get("username"),
            "role": request.form.get("role"),
        }
        password = request.form.get("password")
        if password:  # If a new password is provided, hash it and include it in the update
            from werkzeug.security import generate_password_hash
            data["password_hash"] = generate_password_hash(password)

        db.update_user_row(user["id"], data)
        return redirect(url_for("auth_bp.user_management"))

    return render_template("auth/edit_user.html", user=user)

@auth_bp.route("/user-management/<username>/delete", methods=["POST"])
def delete_user(username):
    user = db.get_user_by_username(username)
    if not user:
        return "User not found", 404

    if user["role"] == "admin":
        return "Cannot delete admin users.", 403

    db.delete_user_row(user["id"])
    return redirect(url_for("auth_bp.user_management"))


@auth_bp.route("/user-management/add", methods=["GET", "POST"])
def add_user():
    if request.method == "POST":
        data = {
            "username": request.form.get("username"),
            "password_hash": request.form.get("password_hash"),  # Hash password before saving
            "role": request.form.get("role"),
        }
        db.add_user_row(data)
        return redirect(url_for("auth_bp.user_management"))
    return render_template("auth/add_user.html")

### API
# @auth_bp.route("/api/users", methods=["GET"])
# def api_list_users():
#     return jsonify(db.list_users())

# @auth_bp.route("/api/users/<username>", methods=["GET"])
# def api_get_user(username):
#     user = db.get_user_by_username(username)
#     if not user:
#         return jsonify({"error": "User not found"}), 404
#     return jsonify(user)

@auth_bp.route("/api/users", methods=["POST"])
def api_add_user():
    data = request.json
    if not data or "username" not in data or "password_hash" not in data:
        return jsonify({"error": "Missing fields"}), 400
    user_id = db.add_user_row(data)
    return jsonify({"id": user_id}), 201

@auth_bp.route("/api/users/<int:user_id>", methods=["PUT"])
def api_update_user(user_id):
    data = request.json
    db.update_user_row(user_id, data)
    return jsonify({"status": "updated"})

@auth_bp.route("/api/users/<int:user_id>", methods=["DELETE"])
def api_delete_user(user_id):
    db.delete_user_row(user_id)
    return jsonify({"status": "deleted"})