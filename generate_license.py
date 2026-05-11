#!/usr/bin/env python3
"""
generate_license.py
-------------------
Standalone CLI tool to generate license keys for the Modbus Monitor app.

Usage:
    python generate_license.py                  # Interactive mode
    python generate_license.py --uid <serial>   # Provide HDD UID manually
    python generate_license.py --expiry 2027-12-31   # Set expiry date
    python generate_license.py --permanent       # Permanent license
    python generate_license.py --read-uid        # Just read and print HDD UID

Examples:
    python generate_license.py --permanent
    python generate_license.py --expiry 2026-12-31
    python generate_license.py --uid "WD-WCC1T1234567" --expiry 2026-06-30
"""

import sys
import os
import argparse
from datetime import datetime

# ---------------------------------------------------------------------------
# Import the shared license_manager.  The tool can run from:
#   (a) project root   → webapp/modbus_monitor/license_manager.py
#   (b) same folder as generate_license.py
# ---------------------------------------------------------------------------
_this_dir = os.path.dirname(os.path.abspath(__file__))
_webapp_dir = os.path.join(_this_dir, "webapp")
if _webapp_dir not in sys.path:
    sys.path.insert(0, _webapp_dir)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

try:
    from modbus_monitor.license_manager import get_hdd_uid, hash_uid, generate_key
except ImportError as e:
    print(f"[ERROR] Cannot import license_manager: {e}")
    print("Make sure you run this script from the project root directory.")
    sys.exit(1)


# ── ANSI colour helpers ──────────────────────────────────────────────────────
def _c(code: str, text: str) -> str:
    """Wrap text in ANSI colour if stdout is a TTY."""
    if sys.stdout.isatty():
        return f"\033[{code}m{text}\033[0m"
    return text


GREEN  = lambda t: _c("92", t)
YELLOW = lambda t: _c("93", t)
CYAN   = lambda t: _c("96", t)
BOLD   = lambda t: _c("1",  t)
RED    = lambda t: _c("91", t)


def print_banner():
    print(BOLD("=" * 60))
    print(BOLD("  Modbus Monitor  –  License Key Generator"))
    print(BOLD("=" * 60))
    print()


def prompt_expiry() -> str:
    """Ask user to choose permanent or time-limited license."""
    print("License type:")
    print("  1) Permanent  (no expiry)")
    print("  2) Time-limited")
    choice = input("  Select [1/2]: ").strip()
    if choice == "2":
        while True:
            raw = input("  Expiry date (YYYY-MM-DD): ").strip()
            try:
                exp = datetime.strptime(raw, "%Y-%m-%d")
                if exp < datetime.now():
                    print(RED("  [!] Expiry date is in the past. Please enter a future date."))
                    continue
                return raw
            except ValueError:
                print(RED("  [!] Invalid format. Use YYYY-MM-DD (e.g. 2027-06-30)."))
    return ""  # permanent


def run_interactive():
    """Interactive CLI session."""
    print_banner()

    # Step 1: HDD UID
    print(CYAN("Step 1: HDD UID"))
    auto_uid = get_hdd_uid()
    print(f"  Auto-detected UID : {YELLOW(auto_uid)}")
    print(f"  SHA-256 hash      : {YELLOW(hash_uid(auto_uid))}")
    override = input("  Use this UID? [Y/n]: ").strip().lower()
    if override in ("n", "no"):
        uid = input("  Enter HDD UID manually: ").strip()
        if not uid:
            print(RED("  [!] UID cannot be empty."))
            sys.exit(1)
    else:
        uid = auto_uid

    print()

    # Step 2: Expiry
    print(CYAN("Step 2: License expiry"))
    expiry = prompt_expiry()
    print()

    # Step 3: Generate key
    try:
        key = generate_key(uid, expiry)
    except ValueError as e:
        print(RED(f"  [!] {e}"))
        sys.exit(1)

    # Display result
    print(BOLD("=" * 60))
    print(GREEN("  LICENSE KEY GENERATED"))
    print(BOLD("=" * 60))
    print(f"\n  HDD UID  : {uid}")
    print(f"  Expiry   : {expiry if expiry else 'PERMANENT'}")
    print()
    print(BOLD("  LICENSE KEY:"))
    print(f"  {CYAN(key)}")
    print()
    print(BOLD("=" * 60))
    print("  Copy the key above and paste it in the License tab of the app.")
    print(BOLD("=" * 60))


def run_args(args):
    """Non-interactive mode from CLI arguments."""
    # --read-uid: just print UID and exit
    if args.read_uid:
        uid = get_hdd_uid()
        print(f"HDD UID  : {uid}")
        print(f"SHA-256  : {hash_uid(uid)}")
        return

    uid = args.uid or get_hdd_uid()

    if args.permanent:
        expiry = ""
    elif args.expiry:
        # Validate
        try:
            exp_dt = datetime.strptime(args.expiry, "%Y-%m-%d")
        except ValueError:
            print(RED(f"[ERROR] Invalid expiry format '{args.expiry}'. Use YYYY-MM-DD."))
            sys.exit(1)
        expiry = args.expiry
    else:
        # Default: permanent
        expiry = ""

    try:
        key = generate_key(uid, expiry)
    except ValueError as e:
        print(RED(f"[ERROR] {e}"))
        sys.exit(1)

    print_banner()
    print(f"HDD UID  : {uid}")
    print(f"Expiry   : {expiry if expiry else 'PERMANENT'}")
    print()
    print(BOLD("LICENSE KEY:"))
    print(CYAN(key))
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Generate license keys for Modbus Monitor.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--uid",       help="Override HDD UID (default: auto-detect)")
    parser.add_argument("--expiry",    help="Expiry date YYYY-MM-DD (default: permanent)")
    parser.add_argument("--permanent", action="store_true", help="Force permanent license")
    parser.add_argument("--read-uid",  action="store_true", help="Print detected HDD UID and exit",
                        dest="read_uid")

    args = parser.parse_args()

    # If any meaningful arg provided → non-interactive
    if args.uid or args.expiry or args.permanent or args.read_uid:
        run_args(args)
    else:
        run_interactive()


if __name__ == "__main__":
    main()
