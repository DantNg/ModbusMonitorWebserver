#!/usr/bin/env python3
"""
generate_license.py
-------------------
Admin tool: generate license keys for the Modbus Monitor application.

Workflow:
  1. Run  read_machine_uid.exe  on the client machine  →  machine_uid.json
  2. Copy machine_uid.json to the admin machine.
  3. Run  generate_license.exe --uid-file machine_uid.json  →  license_<hostname>.txt
  4. Send the .txt file (or the key inside) to the client.

Usage:
    generate_license.exe --uid-file machine_uid.json
    generate_license.exe --uid-file machine_uid.json --expiry 2027-12-31
    generate_license.exe --uid "WD-WCC1T1234567" --expiry 2026-06-30
    generate_license.exe --uid "WD-WCC1T1234567" --permanent
    generate_license.exe                          (interactive mode)
"""

import sys
import os
import json
import argparse
import hashlib
import hmac
import base64
import subprocess
import platform
from datetime import datetime
from typing import Optional

# ---------------------------------------------------------------------------
# License crypto (standalone – no Flask/DB imports required)
# IMPORTANT: _LICENSE_SECRET must match the value in license_manager.py.
# ---------------------------------------------------------------------------
_LICENSE_SECRET = "Mdb5-LcNs-Xk9P-qMrV-2025-zNf3-s8cW"


def _get_hdd_uid() -> str:
    """Read primary hard-drive serial number, fallback to hostname."""
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
    return uid.strip() or platform.node()


def _hash_uid(uid: str) -> str:
    return hashlib.sha256(uid.encode("utf-8")).hexdigest()


def _generate_key(hdd_uid: str, expiry: Optional[str] = None) -> str:
    """
    Build a signed license key for the given HDD UID.

    Args:
        hdd_uid : Raw HDD serial number.
        expiry  : 'YYYY-MM-DD' string, or None / '' for permanent.

    Returns:
        URL-safe Base64 encoded license key.
    """
    uid_hash = _hash_uid(hdd_uid)
    expiry_str = (expiry.strip() if expiry else "") or "PERMANENT"

    if expiry_str != "PERMANENT":
        try:
            datetime.strptime(expiry_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid expiry '{expiry_str}'. Use YYYY-MM-DD or leave empty.")

    msg = f"{uid_hash}|{expiry_str}"
    sig = hmac.new(_LICENSE_SECRET.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).hexdigest()
    payload = json.dumps({"h": uid_hash, "e": expiry_str, "s": sig}, separators=(",", ":"))
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("utf-8")


# ---------------------------------------------------------------------------
# ANSI colour helpers
# ---------------------------------------------------------------------------

def _c(code: str, text: str) -> str:
    if sys.stdout.isatty():
        return f"\033[{code}m{text}\033[0m"
    return text

GREEN  = lambda t: _c("92", t)
YELLOW = lambda t: _c("93", t)
CYAN   = lambda t: _c("96", t)
BOLD   = lambda t: _c("1",  t)
RED    = lambda t: _c("91", t)


def _print_banner():
    print(BOLD("=" * 60))
    print(BOLD("  Modbus Monitor  –  License Key Generator"))
    print(BOLD("=" * 60))
    print()


def _save_license_file(key: str, uid: str, expiry: str, hostname: str) -> str:
    """Write the license key to a .txt file and return the file path."""
    safe_host = "".join(c if c.isalnum() or c in "-_" else "_" for c in hostname)
    date_str = datetime.now().strftime("%Y%m%d")
    filename = f"license_{safe_host}_{date_str}.txt"
    exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    filepath = os.path.join(exe_dir, filename)

    content = "\n".join([
        "=" * 60,
        "  Modbus Monitor – License Key",
        "=" * 60,
        f"  Hostname   : {hostname}",
        f"  HDD UID    : {uid}",
        f"  Expiry     : {expiry if expiry else 'PERMANENT'}",
        f"  Generated  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "  LICENSE KEY:",
        f"  {key}",
        "",
        "=" * 60,
    ])

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath


def _print_result(key: str, uid: str, expiry: str, hostname: str, out_file: str):
    print(BOLD("=" * 60))
    print(GREEN("  LICENSE KEY GENERATED"))
    print(BOLD("=" * 60))
    print(f"\n  Hostname   : {hostname}")
    print(f"  HDD UID    : {uid}")
    print(f"  Expiry     : {expiry if expiry else 'PERMANENT'}")
    print()
    print(BOLD("  LICENSE KEY:"))
    print(f"  {CYAN(key)}")
    print()
    print(BOLD("=" * 60))
    print(f"  Saved to   : {out_file}")
    print(BOLD("=" * 60))


# ---------------------------------------------------------------------------
# Execution modes
# ---------------------------------------------------------------------------

def _prompt_expiry() -> str:
    print("  1) Permanent  (no expiry)")
    print("  2) Time-limited")
    choice = input("  Select [1/2]: ").strip()
    if choice == "2":
        while True:
            raw = input("  Expiry date (YYYY-MM-DD): ").strip()
            try:
                exp = datetime.strptime(raw, "%Y-%m-%d")
                if exp < datetime.now():
                    print(RED("  [!] Date is in the past. Enter a future date."))
                    continue
                return raw
            except ValueError:
                print(RED("  [!] Invalid format. Use YYYY-MM-DD (e.g. 2027-06-30)."))
    return ""


def _run_with_uid_file(uid_file: str, expiry: Optional[str], permanent: bool):
    """Generate key from a machine_uid.json file produced by read_machine_uid."""
    _print_banner()

    try:
        with open(uid_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(RED(f"[ERROR] File not found: {uid_file}"))
        sys.exit(1)
    except json.JSONDecodeError:
        print(RED(f"[ERROR] Cannot parse JSON from: {uid_file}"))
        sys.exit(1)

    uid = data.get("uid", "").strip()
    hostname = data.get("hostname", "unknown")
    collected_at = data.get("collected_at", "")

    if not uid:
        print(RED("[ERROR] 'uid' field is missing or empty in the JSON file."))
        sys.exit(1)

    print(f"  Loaded from  : {uid_file}")
    print(f"  Hostname     : {hostname}")
    print(f"  HDD UID      : {uid}")
    if collected_at:
        print(f"  Collected at : {collected_at}")
    print()

    # Determine expiry
    if permanent:
        exp = ""
    elif expiry:
        try:
            datetime.strptime(expiry, "%Y-%m-%d")
        except ValueError:
            print(RED(f"[ERROR] Invalid expiry format '{expiry}'. Use YYYY-MM-DD."))
            sys.exit(1)
        exp = expiry
    else:
        print(CYAN("Choose license type:"))
        exp = _prompt_expiry()
        print()

    try:
        key = _generate_key(uid, exp)
    except ValueError as e:
        print(RED(f"[ERROR] {e}"))
        sys.exit(1)

    out_file = _save_license_file(key, uid, exp, hostname)
    _print_result(key, uid, exp, hostname, out_file)


def _run_interactive():
    """Fully interactive session (no --uid-file provided)."""
    _print_banner()

    # Step 1: Source of UID
    print(CYAN("Step 1: Machine UID source"))
    print("  1) Load from machine_uid.json file")
    print("  2) Auto-detect on this machine")
    print("  3) Enter UID manually")
    src = input("  Select [1/2/3]: ").strip()

    hostname = platform.node()

    if src == "1":
        uid_file = input("  Path to machine_uid.json: ").strip().strip('"')
        try:
            with open(uid_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            uid = data.get("uid", "").strip()
            hostname = data.get("hostname", hostname)
            if not uid:
                print(RED("  [!] 'uid' field is empty in the file."))
                sys.exit(1)
            print(f"  UID loaded  : {YELLOW(uid)}")
            print(f"  Hostname    : {hostname}")
        except Exception as e:
            print(RED(f"  [!] Cannot read file: {e}"))
            sys.exit(1)
    elif src == "2":
        uid = _get_hdd_uid()
        print(f"  Detected UID : {YELLOW(uid)}")
    else:
        uid = input("  Enter HDD UID manually: ").strip()
        if not uid:
            print(RED("  [!] UID cannot be empty."))
            sys.exit(1)
        hostname_input = input(f"  Hostname (optional, default: {hostname}): ").strip()
        if hostname_input:
            hostname = hostname_input

    print()

    # Step 2: Expiry
    print(CYAN("Step 2: License type"))
    exp = _prompt_expiry()
    print()

    try:
        key = _generate_key(uid, exp)
    except ValueError as e:
        print(RED(f"  [!] {e}"))
        sys.exit(1)

    out_file = _save_license_file(key, uid, exp, hostname)
    _print_result(key, uid, exp, hostname, out_file)


def main():
    parser = argparse.ArgumentParser(
        description="Generate license keys for Modbus Monitor.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--uid-file", metavar="JSON_FILE",
        help="Path to machine_uid.json produced by read_machine_uid on the client."
    )
    parser.add_argument(
        "--uid", metavar="SERIAL",
        help="HDD serial number (override instead of using --uid-file)."
    )
    parser.add_argument(
        "--expiry", metavar="YYYY-MM-DD",
        help="License expiry date (omit for permanent)."
    )
    parser.add_argument(
        "--permanent", action="store_true",
        help="Issue a permanent (no-expiry) license."
    )

    args = parser.parse_args()

    if args.uid_file:
        _run_with_uid_file(args.uid_file, args.expiry, args.permanent)
        return

    if args.uid:
        # Non-interactive with explicit UID
        _print_banner()
        uid = args.uid
        hostname = platform.node()
        exp = "" if args.permanent else (args.expiry or "")
        if args.expiry:
            try:
                datetime.strptime(args.expiry, "%Y-%m-%d")
            except ValueError:
                print(RED(f"[ERROR] Invalid expiry '{args.expiry}'. Use YYYY-MM-DD."))
                sys.exit(1)
        try:
            key = _generate_key(uid, exp)
        except ValueError as e:
            print(RED(f"[ERROR] {e}"))
            sys.exit(1)
        out_file = _save_license_file(key, uid, exp, hostname)
        _print_result(key, uid, exp, hostname, out_file)
        return

    # Fully interactive
    _run_interactive()


if __name__ == "__main__":
    main()

