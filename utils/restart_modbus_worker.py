
import psutil
import subprocess
import sys
import os
from typing import List

# Có thể mở rộng nếu môi trường bạn khác
TERMINAL_NAMES = {
    # Windows
    "cmd.exe", "powershell.exe", "pwsh.exe", "conhost.exe", "wt.exe", "windowsterminal.exe",
    # Linux
    "bash", "zsh", "fish", "sh", "gnome-terminal", "konsole", "xterm", "xfce4-terminal", "alacritty",
    # macOS
    "Terminal", "iTerm2", "login"
}

def find_worker_process(cmdline_substring):
    """
    Tìm các tiến trình terminal có command line chứa chuỗi (không phân biệt hoa thường).
    Trả về list psutil.Process.
    """
    matches = []
    sub = cmdline_substring.lower()
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            name = (proc.info.get('name') or "").lower()
            cmdline = proc.info.get('cmdline') or []
            if name in {n.lower() for n in TERMINAL_NAMES} and cmdline:
                full_cmd = " ".join(cmdline)
                if sub in full_cmd.lower():
                    matches.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return matches

def find_processes_by_name(name_substring: str) -> List[psutil.Process]:
    """
    Find processes whose executable name OR full command line contains the given substring (case-insensitive).
    Returns a list of psutil.Process instances.
    """
    sub = (name_substring or '').lower()
    procs = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'exe']):
        try:
            name = (proc.info.get('name') or '').lower()
            exe = (proc.info.get('exe') or '').lower()
            cmdline = ' '.join(proc.info.get('cmdline') or []).lower()
            if sub and (sub in name or sub in exe or sub in cmdline):
                # Skip self
                if proc.pid == os.getpid():
                    continue
                procs.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return procs

def _highest_terminal_ancestor(proc):
    """
    Trả về terminal ancestor cao nhất (furthest ancestor) thuộc TERMINAL_NAMES.
    Nếu không tìm thấy, trả về chính proc.
    """
    highest = None
    try:
        for p in proc.parents():
            try:
                if (p.name() or "").lower() in {n.lower() for n in TERMINAL_NAMES}:
                    highest = p
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    return highest or proc

def _kill_process_tree(root_proc, timeout=3.0):
    """
    Kết liễu toàn bộ cây tiến trình của root_proc: terminate -> wait -> kill còn sót.
    """
    try:
        # Thu thập tất cả con cháu trước
        children = root_proc.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        children = []

    # Gửi terminate tới con cháu
    for p in children:
        try:
            p.terminate()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    # Đợi chúng thoát
    try:
        gone, alive = psutil.wait_procs(children, timeout=timeout)
    except Exception:
        alive = [c for c in children if c.is_running()]

    # Cưỡng bức kill nếu còn sống
    for p in alive:
        try:
            p.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    # Cuối cùng xử lý chính root (terminal)
    try:
        root_proc.terminate()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return

    try:
        root_proc.wait(timeout=timeout)
    except (psutil.NoSuchProcess, psutil.TimeoutExpired):
        try:
            root_proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

def stop_by_exe_name(name_substring: str) -> int:
    """Stop all processes matching the given exe name/substring (kill the process tree). Returns count stopped."""
    matches = find_processes_by_name(name_substring)
    count = 0
    for proc in matches:
        try:
            # kill the process and its children, not its parent terminal
            _kill_process_tree(proc, timeout=3.0)
            count += 1
            print(f"Stopped process PID {proc.pid} ({proc.name()}) matching '{name_substring}'")
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            print(f"Error stopping PID {getattr(proc, 'pid', '?')}: {e}")
    if count == 0:
        print(f"No process matching '{name_substring}' found.")
    return count

def stop_worker_util(cmdline_substring):
    """
    Dừng tất cả terminal có command line chứa chuỗi cho trước,
    đồng thời đóng luôn cửa sổ terminal bằng cách giết cả cây tiến trình của terminal ancestor cao nhất.
    """
    matches = find_worker_process(cmdline_substring)
    seen_terminal_pids = set()

    for proc in matches:
        try:
            term_ancestor = _highest_terminal_ancestor(proc)
            if term_ancestor.pid in seen_terminal_pids:
                # Tránh giết lặp lại cùng một cửa sổ/phiên terminal
                continue
            seen_terminal_pids.add(term_ancestor.pid)

            print(f"Stopping terminal tree PID {term_ancestor.pid} ({term_ancestor.name()}) "
                  f"for worker match '{cmdline_substring}'")

            # Một số trường hợp worker nằm dưới conhost.exe -> giết ancestor cao nhất (cmd/powershell/wt)
            _kill_process_tree(term_ancestor, timeout=3.0)

        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            print(f"Error handling PID {getattr(proc, 'pid', '?')}: {e}")

def start_worker_util(worker_script_path):
    """
    Start the worker script in a new terminal window. Accepts relative or absolute paths.
    """
    abs_path = os.path.abspath(worker_script_path)
    print(f"Starting worker: {abs_path}")
    if abs_path.endswith('.py'):
        subprocess.Popen(["cmd.exe", "/c", f"start cmd /c python {abs_path}"])
    else:
        subprocess.Popen(["cmd.exe", "/c", f"start cmd /c {abs_path}"])

def start_exe(exe_path: str, cwd: str = None, new_window: bool = True):
    """
    Start an executable. On Windows, new_window=True opens a new console window using 'start'.
    """
    exe_abs = os.path.abspath(exe_path)
    run_cwd = cwd or os.path.dirname(exe_abs) or None
    if os.name == 'nt':
        if new_window:
            # Use cmd start to get a new window, opened MINIMIZED so the console
            # does not pop up in front when a worker is reloaded.
            subprocess.Popen(["cmd.exe", "/c", "start", "", "/min", exe_abs], cwd=run_cwd)
        else:
            # Detached background
            DETACHED_PROCESS = 0x00000008
            subprocess.Popen([exe_abs], cwd=run_cwd, creationflags=DETACHED_PROCESS)
    else:
        # Unix-like: run in background
        subprocess.Popen([exe_abs], cwd=run_cwd)

def restart_worker_util(cmdline_substring, worker_script_path):
    """
    Stop then start the worker script, matching by command line substring.
    """
    stop_worker_util(cmdline_substring)
    # Optional: wait a moment for processes to close
    import time
    time.sleep(1)
    start_worker_util(worker_script_path)


# --------- Convenience for released EXEs (no batch scripts) ---------
PROCESS_MAP = {
    # logical name -> exe filename
    "orchestra": "orchestra_modbus.exe",
    "datalogger": "orchestra_datalogger.exe",
    "alarm": "orchestra_alarm.exe",
}

def _resolve_exe_path(exe_name_or_path: str) -> str:
    """
    If input is a file path, return it. Otherwise try to locate the exe by name near this script
    and in common locations (current dir, ../output).
    """
    candidate = exe_name_or_path
    if os.path.isabs(candidate) and os.path.isfile(candidate):
        return candidate
    # Relative path or just a name
    if os.path.isfile(candidate):
        return os.path.abspath(candidate)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    guesses = [
        os.path.join(script_dir, candidate),
        os.path.join(script_dir, '..', candidate),
        os.path.join(script_dir, '..', 'output', candidate),
        os.path.join(os.getcwd(), candidate),
        os.path.join(os.getcwd(), 'output', candidate),
    ]
    for g in guesses:
        if os.path.isfile(g):
            return os.path.abspath(g)
    return candidate  # fallback; let caller handle not-found

def stop(name: str):
    """Stop by logical name or exe substring."""
    exe_name = PROCESS_MAP.get(name, name)
    return stop_by_exe_name(exe_name)

def start(name: str, exe_path: str = None, new_window: bool = True):
    """Start by logical name using resolved exe path, or explicit exe_path."""
    exe_name = PROCESS_MAP.get(name, name)
    target = _resolve_exe_path(exe_path or exe_name)
    if not os.path.isfile(target):
        print(f"⚠️ Executable not found: {target}")
        return
    start_exe(target, cwd=os.path.dirname(target), new_window=new_window)
    print(f"Started {os.path.basename(target)}")

def restart(name: str, exe_path: str = None, new_window: bool = True):
    """Restart by logical name (stop all then start)."""
    stop(name)
    # give OS time to release ports/files
    try:
        import time
        time.sleep(1)
    except Exception:
        pass
    start(name, exe_path=exe_path, new_window=new_window)


if __name__ == '__main__':
    # Simple CLI: python restart_modbus_worker.py [stop|start|restart|list] <name> [--path exe_path] [--no-window]
    import argparse
    parser = argparse.ArgumentParser(description='Manage Modbus worker executables without batch scripts.')
    parser.add_argument('action', choices=['stop', 'start', 'restart', 'list'], help='Action to perform')
    parser.add_argument('name', nargs='?', help='Logical name (orchestra|datalogger|alarm) or exe substring')
    parser.add_argument('--path', dest='path', help='Explicit path to executable for start/restart')
    parser.add_argument('--no-window', action='store_true', help='Do not open a new console window on Windows')
    args = parser.parse_args()

    if args.action == 'list':
        # List running target processes
        targets = list(PROCESS_MAP.values())
        print('Running worker processes:')
        for t in targets:
            procs = find_processes_by_name(t)
            for p in procs:
                print(f"- {t}: PID {p.pid} (name={p.name()})")
        sys.exit(0)

    if not args.name:
        parser.error('name is required for stop/start/restart')

    if args.action == 'stop':
        stop(args.name)
    elif args.action == 'start':
        start(args.name, exe_path=args.path, new_window=not args.no_window)
    elif args.action == 'restart':
        restart(args.name, exe_path=args.path, new_window=not args.no_window)

    
# Example usage:
# for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
#     try:
#         if proc.info['name'] in terminal_names:
#             print(f"PID: {proc.info['pid']}, Name: {proc.info['name']}")
#             if proc.info['cmdline']:
#                 print("Command Line:", " ".join(proc.info['cmdline']))
#             print("-" * 40)
#     except (psutil.NoSuchProcess, psutil.AccessDenied):
#         continue
# stop_worker_util('start_orchestra_modbus.bat')
# # start_worker_util('C:/path/to/start_orchestra_modbus.bat')
# restart_worker_util('start_orchestra_modbus.bat', '../start_orchestra_modbus.bat')
