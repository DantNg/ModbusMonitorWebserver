from flask import Blueprint, request, jsonify,redirect,url_for, render_template,session
from ..database import db
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
            session.clear()  # Clear old session data to prevent role confusion
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            session["role"] = user["role"]
            return redirect(url_for("dashboard_bp.dashboard"))
        else:
            error = "Invalid username or password"
    return render_template("auth/login.html", error=error)
@auth_bp.route("/logout")
def logout():
    session.clear()  # Clear ALL session data to prevent role confusion
    return redirect(url_for("auth_bp.login"))

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
    if session.get("role") != "admin":
        return "Unauthorized", 403
    users = db.list_users()
    return render_template("auth/user_management.html", users=users)

@auth_bp.route("/user-management/<username>/edit", methods=["GET", "POST"])
def edit_user(username):
    if session.get("role") != "admin":
        return "Unauthorized", 403
    user = db.get_user_by_username(username)
    if not user:
        return "User not found", 404

    if request.method == "POST":
        data = {
            "username": request.form.get("username"),
            "role": user["role"],  # Preserve existing role - only one admin allowed
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
    # Only admin can delete users
    if session.get("role") != "admin":
        return "Unauthorized", 403

    user = db.get_user_by_username(username)
    if not user:
        return "User not found", 404

    # Admin cannot delete their own account
    if user["id"] == session.get("user_id"):
        return "Cannot delete your own account.", 403

    db.delete_user_row(user["id"])
    return redirect(url_for("auth_bp.user_management"))


@auth_bp.route("/user-management/add", methods=["GET", "POST"])
def add_user():
    # Only admin can add users
    if session.get("role") != "admin":
        return redirect(url_for("auth_bp.user_management"))
    if request.method == "POST":
        data = {
            "username": request.form.get("username"),
            "password_hash": generate_password_hash(request.form.get("password_hash")),  # Hash password before saving
            "role": "user",  # Only one admin allowed, new accounts are always 'user'
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
    if session.get("role") != "admin":
        return jsonify({"error": "Unauthorized"}), 403
    data = request.json
    if not data or "username" not in data or "password_hash" not in data:
        return jsonify({"error": "Missing fields"}), 400
    user_id = db.add_user_row(data)
    return jsonify({"id": user_id}), 201

@auth_bp.route("/api/users/<int:user_id>", methods=["PUT"])
def api_update_user(user_id):
    if session.get("role") != "admin":
        return jsonify({"error": "Unauthorized"}), 403
    data = request.json
    db.update_user_row(user_id, data)
    return jsonify({"status": "updated"})

@auth_bp.route("/api/users/<int:user_id>", methods=["DELETE"])
def api_delete_user(user_id):
    if session.get("role") != "admin":
        return jsonify({"error": "Unauthorized"}), 403
    db.delete_user_row(user_id)
    return jsonify({"status": "deleted"})