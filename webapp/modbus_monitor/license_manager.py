"""
license_manager.py
------------------
Core license logic: HDD UID reading, key generation, key verification.
This module is intentionally free of Flask/DB imports so it can be used
both by the webapp (before_request hook) and the standalone CLI tool.
"""

import os
import socket
import struct
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

# ---------------------------------------------------------------------------
# NTP + Anti-rollback configuration
# ---------------------------------------------------------------------------
_NTP_SERVERS = ["pool.ntp.org", "time.google.com", "time.cloudflare.com"]
_NTP_TIMEOUT = 3          # seconds per NTP server attempt

# Max tolerated difference (seconds) between current time and last-seen timestamp
# before treating it as a clock rollback.  1 hour covers legitimate NTP corrections
# while catching any intentional date rollback (days / months).
_ROLLBACK_TOLERANCE = 3600

# Path of the HMAC-signed "last seen" file (inside project's logs/ directory)
_MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
_LAST_SEEN_FILE = os.path.normpath(
    os.path.join(_MODULE_DIR, "..", "..", "logs", ".lts")
)

# Module-level cache so the DB is not hit on every request (60-second TTL)
_cache: dict = {"valid": None, "ts": 0.0}
_CACHE_TTL = 60  # seconds


# ---------------------------------------------------------------------------
# NTP helpers (raw UDP, no external libraries)
# ---------------------------------------------------------------------------

def _query_ntp_time() -> Optional[float]:
    """
    Query an NTP server via raw UDP socket.
    Returns authoritative Unix timestamp (float) or None when offline / all servers fail.
    No external libraries required – uses only stdlib socket + struct.
    """
    for host in _NTP_SERVERS:
        s = None
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(_NTP_TIMEOUT)
            # Minimal NTP request packet (mode 3 = client)
            s.sendto(b'\x1b' + 47 * b'\0', (host, 123))
            data, _ = s.recvfrom(1024)
            if len(data) >= 48:
                # Transmit Timestamp field starts at byte 40 (high 32-bit word = seconds)
                seconds_since_1900 = struct.unpack('!I', data[40:44])[0]
                # NTP epoch is 1900-01-01; subtract 70 years to get Unix epoch
                return float(seconds_since_1900 - 2208988800)
        except Exception:
            continue
        finally:
            if s:
                try:
                    s.close()
                except Exception:
                    pass
    return None


# ---------------------------------------------------------------------------
# Signed last-seen timestamp helpers (anti clock-rollback)
# ---------------------------------------------------------------------------

def _sign_ts(ts: float) -> str:
    """Encode a timestamp as a URL-safe base64 blob with an HMAC signature."""
    msg = f"{ts:.3f}"
    sig = hmac.new(
        _LICENSE_SECRET.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    payload = json.dumps({"ts": ts, "s": sig}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("utf-8")


def _verify_ts(data: str) -> Optional[float]:
    """Return the timestamp if the HMAC is intact, else None (tampered / invalid)."""
    try:
        raw = base64.urlsafe_b64decode(data.encode("utf-8")).decode("utf-8")
        payload = json.loads(raw)
        ts = float(payload["ts"])
        sig = payload["s"]
        msg = f"{ts:.3f}"
        expected = hmac.new(
            _LICENSE_SECRET.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        if hmac.compare_digest(sig, expected):
            return ts
    except Exception:
        pass
    return None


def _read_last_seen() -> Optional[float]:
    """Read and verify the persisted last-seen timestamp. Returns None if missing / tampered."""
    try:
        with open(_LAST_SEEN_FILE, "r", encoding="utf-8") as f:
            return _verify_ts(f.read().strip())
    except Exception:
        return None


def _write_last_seen(ts: float) -> None:
    """Persist a new signed last-seen timestamp (only if it advances the stored value)."""
    try:
        os.makedirs(os.path.dirname(_LAST_SEEN_FILE), exist_ok=True)
        with open(_LAST_SEEN_FILE, "w", encoding="utf-8") as f:
            f.write(_sign_ts(ts))
    except Exception:
        pass


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


def verify_key(key: str, _now: Optional[datetime] = None) -> Tuple[bool, str, Optional[datetime]]:
    """
    Verify a license key against the current machine's HDD UID.

    Args:
        key  : License key string.
        _now : Reference datetime used for expiry check.
               Pass NTP-derived time to prevent clock-rollback bypass.
               Defaults to datetime.now() when not provided.

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

    # Use caller-supplied reference time (NTP-derived) when available to prevent
    # clock-rollback bypass; fall back to system time only when no reference is given.
    check_time = _now if _now is not None else datetime.now()
    if check_time > expires_at:
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
    """
    Internal: query DB and verify the stored license key with anti-tamper time checks.

    Time-verification flow:
    1. Try NTP (authoritative) → prevents bypass while online.
    2. Fall back to system time if NTP is unreachable (offline mode).
    3. Anti-rollback: if reference time is more than _ROLLBACK_TOLERANCE seconds
       behind the last persisted last-seen timestamp, the system clock was rolled
       back → deny (covers the "disconnect + change date" attack vector).
    4. Verify key cryptographically using the reference time.
    5. On success, advance the signed last-seen file so future rollbacks are caught.
    """
    try:
        from .database.db import get_active_license
        row = get_active_license()
        if not row:
            return False

        # Step 1 & 2: get the most trustworthy time available
        ntp_ts = _query_ntp_time()
        ref_ts = ntp_ts if ntp_ts is not None else time.time()

        # Step 3: anti-rollback check (only meaningful when NTP is unavailable;
        # when NTP succeeds, the reference is already authoritative)
        if ntp_ts is None:
            last_seen = _read_last_seen()
            if last_seen is not None and ref_ts < last_seen - _ROLLBACK_TOLERANCE:
                print(
                    f"[license] Clock rollback detected: "
                    f"system={ref_ts:.0f}, last_seen={last_seen:.0f}, "
                    f"diff={last_seen - ref_ts:.0f}s"
                )
                return False

        # Step 4: cryptographic verification with reference time
        ref_dt = datetime.fromtimestamp(ref_ts)
        is_valid, msg, _ = verify_key(row["license_key"], _now=ref_dt)

        # Step 5: advance last-seen only when timestamp moves forward
        if is_valid:
            last_seen = _read_last_seen()
            if last_seen is None or ref_ts > last_seen:
                _write_last_seen(ref_ts)

        return is_valid
    except Exception:
        return False
