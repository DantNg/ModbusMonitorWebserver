"""
license_manager.py
------------------
Core license logic: HDD UID reading, key generation, key verification.
This module is intentionally free of Flask/DB imports so it can be used
both by the webapp (before_request hook) and the standalone CLI tool.
"""

import subprocess
import hashlib
import hmac
import json
import base64
import platform
import time
from datetime import datetime
from typing import Optional, Tuple

# ---------------------------------------------------------------------------
# HMAC secret – must be identical in both the generator tool and the app.
# Change this value before the first production deployment; never change it
# after keys have been distributed, or all existing keys will become invalid.
# ---------------------------------------------------------------------------
_LICENSE_SECRET = "Mdb5-LcNs-Xk9P-qMrV-2025-zNf3-s8cW"

# Module-level cache so the DB is not hit on every request (60-second TTL)
_cache: dict = {"valid": None, "ts": 0.0}
_CACHE_TTL = 60  # seconds


def get_hdd_uid() -> str:
    """
    Read the serial number of the primary hard drive.
    Falls back to platform.node() (hostname) when the OS command fails.
    """
    uid = ""
    try:
        sys_name = platform.system()
        if sys_name == "Windows":
            result = subprocess.run(
                ["wmic", "diskdrive", "get", "SerialNumber", "/format:list"],
                capture_output=True, text=True, timeout=10
            )
            for line in result.stdout.splitlines():
                if line.upper().startswith("SERIALNUMBER="):
                    serial = line.split("=", 1)[1].strip()
                    if serial:
                        uid = serial
                        break
        elif sys_name == "Linux":
            result = subprocess.run(
                ["lsblk", "-d", "-o", "SERIAL", "--noheadings"],
                capture_output=True, text=True, timeout=10
            )
            for line in result.stdout.splitlines():
                serial = line.strip()
                if serial:
                    uid = serial
                    break
    except Exception:
        pass

    # Fallback: hostname (less unique but always available)
    if not uid:
        uid = platform.node()

    return uid.strip()


def hash_uid(uid: str) -> str:
    """Return the SHA-256 hex digest of the HDD UID."""
    return hashlib.sha256(uid.encode("utf-8")).hexdigest()


def generate_key(hdd_uid: str, expiry: Optional[str] = None) -> str:
    """
    Generate a license key for a specific HDD UID.

    Args:
        hdd_uid : Raw HDD serial number string (from get_hdd_uid()).
        expiry  : ISO date 'YYYY-MM-DD', or None / empty for permanent.

    Returns:
        URL-safe Base64-encoded license key string.
    """
    hdd_hash = hash_uid(hdd_uid)
    expiry_str = (expiry.strip() if expiry else "") or "PERMANENT"

    # Validate expiry format when not permanent
    if expiry_str != "PERMANENT":
        try:
            datetime.strptime(expiry_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid expiry format '{expiry_str}'. Use YYYY-MM-DD or leave empty for permanent.")

    # Build HMAC over "<hash>|<expiry>"
    msg = f"{hdd_hash}|{expiry_str}"
    sig = hmac.new(_LICENSE_SECRET.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()

    payload = json.dumps({"h": hdd_hash, "e": expiry_str, "s": sig}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("utf-8")


def verify_key(key: str) -> Tuple[bool, str, Optional[datetime]]:
    """
    Verify a license key against the current machine's HDD UID.

    Returns:
        (is_valid: bool, message: str, expires_at: Optional[datetime])
        expires_at is None for permanent licenses or invalid keys.
    """
    try:
        raw = base64.urlsafe_b64decode(key.encode("utf-8")).decode("utf-8")
        payload = json.loads(raw)
    except Exception:
        return False, "License key format is invalid.", None

    h = payload.get("h", "")
    e = payload.get("e", "")
    s = payload.get("s", "")

    if not (h and e and s):
        return False, "License key is incomplete.", None

    # Verify HMAC signature
    msg = f"{h}|{e}"
    expected_sig = hmac.new(
        _LICENSE_SECRET.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(s, expected_sig):
        return False, "License key signature is invalid.", None

    # Verify that the key was issued for this machine
    current_hash = hash_uid(get_hdd_uid())
    if not hmac.compare_digest(h, current_hash):
        return False, "This license key was issued for a different machine.", None

    # Check expiry
    if e == "PERMANENT":
        return True, "License is valid (permanent).", None

    try:
        expires_at = datetime.strptime(e, "%Y-%m-%d")
    except ValueError:
        return False, "License key contains an invalid expiry date.", None

    if datetime.now() > expires_at:
        return False, f"License expired on {e}.", expires_at

    return True, f"License is valid until {e}.", expires_at


def is_license_valid() -> bool:
    """
    Quick check (with 60-second cache) used in the before_request hook.
    Queries the DB for an active license row and verifies it cryptographically.
    """
    global _cache
    now = time.time()
    if _cache["valid"] is not None and (now - _cache["ts"]) < _CACHE_TTL:
        return _cache["valid"]

    result = _check_db_license()
    _cache = {"valid": result, "ts": now}
    return result


def invalidate_cache() -> None:
    """Call this whenever a license is added or removed."""
    global _cache
    _cache = {"valid": None, "ts": 0.0}


def _check_db_license() -> bool:
    """Internal: query DB and verify the stored license key."""
    try:
        from .database.db import get_active_license
        row = get_active_license()
        if not row:
            return False
        is_valid, _, _ = verify_key(row["license_key"])
        return is_valid
    except Exception:
        return False
