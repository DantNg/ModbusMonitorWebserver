"""
license/routes.py
-----------------
Flask routes for license activation and management.

Endpoints:
    GET  /license/activate          – Page to enter a license key (no auth required)
    POST /license/activate          – Submit and validate a license key
    GET  /license/manage            – Admin management page
    POST /license/<int:id>/revoke   – Revoke a license (admin)
    POST /license/<int:id>/delete   – Delete a license (admin)
    GET  /license/machine-uid       – AJAX: return current machine HDD UID hash
"""

from flask import render_template, request, redirect, url_for, session, jsonify, flash
from datetime import datetime

from . import license_bp
from ..database import db
from ..license_manager import get_hdd_uid, hash_uid, verify_key, invalidate_cache


# ---------------------------------------------------------------------------
# Activation page – no login required (intentionally outside auth guard)
# ---------------------------------------------------------------------------

@license_bp.route("/activate", methods=["GET", "POST"])
def activate():
    """Show license activation form and process submitted keys."""
    error = None
    success = None

    if request.method == "POST":
        key = (request.form.get("license_key") or "").strip()

        if not key:
            error = "Please enter a license key."
        else:
            is_valid, message, expires_at = verify_key(key)

            if not is_valid:
                error = message
            else:
                # Get current HDD UID hash to store alongside the key
                hdd_hash = hash_uid(get_hdd_uid())
                is_permanent = expires_at is None
                note = request.form.get("note", "").strip()

                inserted_id = db.add_license(
                    license_key=key,
                    hdd_uid_hash=hdd_hash,
                    expires_at=expires_at,
                    is_permanent=is_permanent,
                    note=note,
                )

                if inserted_id == -1:
                    error = "This license key has already been activated or a database error occurred."
                else:
                    # Flush the validity cache so next request proceeds normally
                    invalidate_cache()
                    flash("License activated successfully!", "success")
                    return redirect(url_for("dashboard_bp.dashboard"))

    return render_template("license/activate.html", error=error, success=success)


# ---------------------------------------------------------------------------
# Management page – admin only
# ---------------------------------------------------------------------------

@license_bp.route("/manage")
def manage():
    """License management page (admin only)."""
    if session.get("role") != "admin":
        flash("Access denied. Admin role required.", "danger")
        return redirect(url_for("dashboard_bp.dashboard"))

    all_licenses = db.list_all_licenses()
    active_license = db.get_active_license()

    # Enrich each row with human-readable status
    now = datetime.now()
    for lic in all_licenses:
        if lic["status"] == "revoked":
            lic["display_status"] = "Revoked"
        elif lic["is_permanent"]:
            lic["display_status"] = "Active (Permanent)"
        elif lic["expires_at"] and lic["expires_at"] < now:
            lic["display_status"] = "Expired"
        else:
            lic["display_status"] = "Active"

    # Current machine UID for reference
    machine_uid = get_hdd_uid()
    machine_hash = hash_uid(machine_uid)

    return render_template(
        "license/manage.html",
        all_licenses=all_licenses,
        active_license=active_license,
        machine_uid=machine_uid,
        machine_hash=machine_hash,
    )


# ---------------------------------------------------------------------------
# Revoke / Delete (admin only, AJAX-friendly JSON responses)
# ---------------------------------------------------------------------------

@license_bp.route("/<int:license_id>/revoke", methods=["POST"])
def revoke(license_id: int):
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied."}), 403

    ok = db.revoke_license(license_id)
    if ok:
        invalidate_cache()
        return jsonify({"success": True, "message": "License revoked."})
    return jsonify({"success": False, "message": "License not found or already revoked."}), 404


@license_bp.route("/<int:license_id>/delete", methods=["POST"])
def delete_license_route(license_id: int):
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied."}), 403

    ok = db.delete_license(license_id)
    if ok:
        invalidate_cache()
        return jsonify({"success": True, "message": "License deleted."})
    return jsonify({"success": False, "message": "License not found."}), 404


# ---------------------------------------------------------------------------
# AJAX helper: return current machine HDD UID (admin diagnostic)
# ---------------------------------------------------------------------------

@license_bp.route("/machine-uid")
def machine_uid():
    if session.get("role") != "admin":
        return jsonify({"success": False, "message": "Access denied."}), 403

    uid = get_hdd_uid()
    return jsonify({
        "success": True,
        "uid": uid,
        "hash": hash_uid(uid),
    })
