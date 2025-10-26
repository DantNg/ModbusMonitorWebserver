
import psutil
import subprocess
import sys
import os

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

def restart_worker_util(cmdline_substring, worker_script_path):
    """
    Stop then start the worker script, matching by command line substring.
    """
    stop_worker_util(cmdline_substring)
    # Optional: wait a moment for processes to close
    import time
    time.sleep(1)
    start_worker_util(worker_script_path)

    
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
