#!/usr/bin/env python3
"""
read_machine_uid.py
-------------------
Run this tool on the CLIENT machine to collect the hardware fingerprint.
The output file (machine_uid.json) must be sent to the administrator
to generate a license key.

This script is standalone – no external dependencies beyond Python stdlib.
"""

import sys
import os
import json
import subprocess
import hashlib
import platform
from datetime import datetime


# ---------------------------------------------------------------------------
# HDD UID reading  (same logic as license_manager.py – kept in sync)
# ---------------------------------------------------------------------------

def _get_hdd_uid() -> str:
    """Read the primary hard-drive serial number, fallback to hostname."""
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

    if not uid:
        uid = platform.node()

    return uid.strip()


def _hash_uid(uid: str) -> str:
    return hashlib.sha256(uid.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _pause():
    """Keep console open when run by double-clicking the EXE."""
    print()
    input("  Press Enter to exit...")


def main():
    print("=" * 58)
    print("  Machine UID Reader  –  Modbus Monitor")
    print("=" * 58)
    print()

    uid = _get_hdd_uid()
    uid_hash = _hash_uid(uid)
    hostname = platform.node()
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    data = {
        "uid":          uid,
        "hash":         uid_hash,
        "hostname":     hostname,
        "collected_at": collected_at,
    }

    # Write JSON file next to the executable / script
    exe_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    output_path = os.path.join(exe_dir, "machine_uid.json")

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        saved_ok = True
    except OSError as err:
        saved_ok = False
        save_error = str(err)

    # Display collected info
    print(f"  Hostname     : {hostname}")
    print(f"  HDD Serial   : {uid}")
    print(f"  Fingerprint  : {uid_hash[:32]}…")
    print(f"  Collected at : {collected_at}")
    print()

    if saved_ok:
        print(f"  [OK] Saved to : {output_path}")
        print()
        print("  Please send the file  machine_uid.json  to your")
        print("  administrator to receive your license key.")
    else:
        print(f"  [ERROR] Could not save file: {save_error}")
        print()
        print("  Copy the Fingerprint value above and provide it")
        print("  to your administrator manually.")

    print()
    print("=" * 58)
    _pause()


if __name__ == "__main__":
    main()
