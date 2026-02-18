"""
single_instance.py - Ensure only one instance of each process is running.

Uses Windows Named Mutex (primary) with lock file fallback for cross-platform.
When built as EXE, prevents duplicate launches of the same service.

Usage:
    from utils.single_instance import SingleInstance

    instance = SingleInstance("orchestra_modbus")
    if not instance.acquire():
        print("Another instance is already running!")
        sys.exit(1)
    
    # ... run your app ...
    
    instance.release()  # Optional, auto-released on process exit
"""

import os
import sys
import atexit
import ctypes
import logging

logger = logging.getLogger(__name__)


class SingleInstance:
    """
    Ensures only one instance of a named process can run at a time.
    
    On Windows: Uses a Named Mutex (kernel object) - most reliable for EXE.
    Fallback: Uses a lock file with PID check.
    """

    def __init__(self, instance_name: str):
        """
        Args:
            instance_name: Unique identifier for the process 
                          (e.g., "orchestra_modbus", "modbus_webserver")
        """
        self.instance_name = instance_name
        self._mutex_handle = None
        self._lock_file_path = None
        self._lock_file = None
        self._acquired = False

    def acquire(self) -> bool:
        """
        Try to acquire single instance lock.
        
        Returns:
            True if this is the only instance (lock acquired).
            False if another instance is already running.
        """
        if self._acquired:
            return True

        # Try Windows Named Mutex first (most reliable for EXE)
        if sys.platform == "win32":
            result = self._acquire_mutex()
            if result is not None:
                self._acquired = result
                if result:
                    atexit.register(self.release)
                return result

        # Fallback to lock file mechanism
        result = self._acquire_lock_file()
        self._acquired = result
        if result:
            atexit.register(self.release)
        return result

    def release(self):
        """Release the single instance lock."""
        if not self._acquired:
            return

        # Release Windows Mutex
        if self._mutex_handle is not None:
            try:
                ctypes.windll.kernel32.ReleaseMutex(self._mutex_handle)
                ctypes.windll.kernel32.CloseHandle(self._mutex_handle)
                self._mutex_handle = None
            except Exception as e:
                logger.debug(f"Error releasing mutex: {e}")

        # Release lock file
        if self._lock_file is not None:
            try:
                self._lock_file.close()
                self._lock_file = None
            except Exception:
                pass

        if self._lock_file_path and os.path.exists(self._lock_file_path):
            try:
                os.remove(self._lock_file_path)
            except Exception:
                pass

        self._acquired = False

    def _acquire_mutex(self):
        """
        Try to acquire a Windows Named Mutex.
        
        Returns:
            True  - mutex acquired (no other instance)
            False - another instance holds the mutex
            None  - failed to use mutex mechanism (fallback needed)
        """
        try:
            # Mutex name must be globally unique
            mutex_name = f"Global\\ModbusMonitor_{self.instance_name}"

            # CreateMutexW(lpMutexAttributes, bInitialOwner, lpName)
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.CreateMutexW(None, True, mutex_name)

            if handle == 0:
                logger.warning(f"CreateMutexW failed for '{mutex_name}'")
                return None

            # ERROR_ALREADY_EXISTS = 183
            last_error = kernel32.GetLastError()
            if last_error == 183:
                # Another instance already created this mutex
                kernel32.CloseHandle(handle)
                logger.info(f"Another instance of '{self.instance_name}' is already running (mutex exists)")
                return False

            # Successfully created and owned the mutex
            self._mutex_handle = handle
            logger.info(f"Single instance lock acquired for '{self.instance_name}' (mutex)")
            return True

        except Exception as e:
            logger.warning(f"Mutex mechanism failed: {e}, falling back to lock file")
            return None

    def _acquire_lock_file(self) -> bool:
        """
        Fallback: Try to acquire a lock file with PID validation.
        
        Returns:
            True if lock acquired, False if another instance is running.
        """
        try:
            # Determine lock file location
            lock_dir = self._get_lock_dir()
            os.makedirs(lock_dir, exist_ok=True)
            self._lock_file_path = os.path.join(lock_dir, f"{self.instance_name}.lock")

            # Check if lock file exists and if the PID is still alive
            if os.path.exists(self._lock_file_path):
                try:
                    with open(self._lock_file_path, "r") as f:
                        old_pid = int(f.read().strip())

                    if self._is_pid_alive(old_pid):
                        logger.info(
                            f"Another instance of '{self.instance_name}' is running (PID {old_pid})"
                        )
                        return False
                    else:
                        # Stale lock file, process no longer exists
                        logger.info(
                            f"Removing stale lock file for '{self.instance_name}' (PID {old_pid} dead)"
                        )
                        os.remove(self._lock_file_path)
                except (ValueError, OSError):
                    # Corrupted lock file, remove it
                    try:
                        os.remove(self._lock_file_path)
                    except OSError:
                        pass

            # Write current PID to lock file
            with open(self._lock_file_path, "w") as f:
                f.write(str(os.getpid()))
            
            logger.info(f"Single instance lock acquired for '{self.instance_name}' (lock file)")
            return True

        except Exception as e:
            logger.warning(f"Lock file mechanism failed: {e}")
            # If we can't create a lock file, allow the process to run
            return True

    @staticmethod
    def _get_lock_dir() -> str:
        """Get directory for lock files."""
        # Use temp directory or project directory
        if sys.platform == "win32":
            temp = os.environ.get("TEMP", os.environ.get("TMP", ""))
            if temp:
                return os.path.join(temp, "modbus_monitor_locks")

        # Fallback: use project directory
        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return os.path.join(project_dir, ".locks")

    @staticmethod
    def _is_pid_alive(pid: int) -> bool:
        """Check if a process with given PID is still running."""
        if sys.platform == "win32":
            try:
                # Use OpenProcess to check if PID exists
                # PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(0x1000, False, pid)
                if handle:
                    kernel32.CloseHandle(handle)
                    return True
                return False
            except Exception:
                return False
        else:
            # Unix/Linux
            try:
                os.kill(pid, 0)
                return True
            except OSError:
                return False

    def __del__(self):
        """Cleanup on garbage collection."""
        try:
            self.release()
        except Exception:
            pass


def ensure_single_instance(instance_name: str) -> SingleInstance:
    """
    Convenience function: acquire single instance or exit immediately.
    
    Args:
        instance_name: Unique name for this process.
    
    Returns:
        SingleInstance object (lock held).
    
    Exits:
        sys.exit(0) if another instance is already running.
    """
    instance = SingleInstance(instance_name)
    if not instance.acquire():
        print(f"⚠️ {instance_name} is already running. Only one instance allowed.")
        print(f"   Exiting to prevent duplicate process.")
        sys.exit(0)
    return instance
