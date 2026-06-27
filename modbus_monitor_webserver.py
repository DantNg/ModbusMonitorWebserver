"""
Unified entrypoint to start the Flask webapp without calling webapp/app.py directly.

Usage:
  python modbus_monitor_webserver.py

This imports the Socket.IO app from webapp/app.py and runs it.
"""

import os
import sys
import logging
import queue
import threading
import atexit
from logging.handlers import RotatingFileHandler

# Ensure project root is on sys.path so `webapp` imports correctly
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
	sys.path.insert(0, PROJECT_ROOT)

# Tự thu nhỏ cửa sổ console ngay khi khởi động (bản --console). No-op nếu --noconsole.
try:
	from utils.console_window import minimize_console
	minimize_console()
except Exception:
	pass

# Use centralized app_logger (auto-creates logs/ folder, works with EXE)
from shared.app_logger import get_logger as _get_app_logger, log_exception, log_startup

# Setup logging for the main server process
def setup_main_logging():
	"""Setup logging for the main server process using centralized app_logger"""
	return _get_app_logger("modbus_webserver_main")

try:
	# Setup logging first
	logger = setup_main_logging()
	logger.info("Starting Modbus Monitor Webserver...")
	
	# Import pre-configured app and socketio from webapp
	from webapp.app import app, socketio, initialize_process_manager
	logger.info("Successfully imported webapp components")
except Exception as e:
	# Fallback: try adding the webapp folder explicitly
	try:
		WEBAPP_DIR = os.path.join(PROJECT_ROOT, 'webapp')
		if WEBAPP_DIR not in sys.path:
			sys.path.insert(0, WEBAPP_DIR)
		from webapp.app import app, socketio, initialize_process_manager  # type: ignore
		if 'logger' in locals():
			logger.info("Successfully imported webapp components (fallback method)")
	except Exception as fallback_error:
		if 'logger' in locals():
			logger.error(f"Failed to import webapp components: {fallback_error}")
		print(f"❌ Critical error importing webapp: {fallback_error}")
		import traceback
		traceback.print_exc()
		sys.exit(1)


def _env_flag(name: str, default: bool = False) -> bool:
	"""Parse boolean environment variable values safely."""
	value = os.environ.get(name)
	if value is None:
		return default
	return str(value).strip().lower() in ("1", "true", "yes", "on", "y")


class InProcessAlarmService:
	"""Run alarm worker inside the webserver process as a background thread."""

	def __init__(self, app, logger):
		self.app = app
		self.logger = logger
		self.config = None
		self.worker = None
		self.worker_thread = None
		self.log_thread = None
		self.running = False

		# Use stdlib queues for in-process mode.
		self.data_queue = queue.Queue()
		self.command_queue = queue.Queue()
		self.log_queue = queue.Queue()
		self.shared_state = {}

	def start(self):
		if self.running:
			return

		from workers.alarm_worker import AlarmConfig, AlarmWorker

		self.config = AlarmConfig(
			check_interval=1.0,
			enable_notifications=True,
			database_timeout=5.0,
		)
		self.worker = AlarmWorker(
			config=self.config,
			data_queue=self.data_queue,
			command_queue=self.command_queue,
			log_queue=self.log_queue,
			shared_state=self.shared_state,
		)

		self.worker_thread = threading.Thread(target=self.worker.run, name="InProcessAlarmWorker", daemon=True)
		self.worker_thread.start()

		self.log_thread = threading.Thread(target=self._drain_logs, name="InProcessAlarmLogPump", daemon=True)
		self.log_thread.start()

		# Expose queue for Flask routes to issue runtime commands (reload/stop/status).
		self.app.alarm_command_queue = self.command_queue
		self.app.inprocess_alarm_service = self
		self.running = True
		self.logger.info("Alarm service started in main process (ALARM_IN_MAIN_PROCESS=1)")

	def stop(self):
		if not self.running:
			return
		try:
			self.command_queue.put({"type": "stop"}, block=False)
		except Exception:
			pass

		if self.worker_thread is not None and self.worker_thread.is_alive():
			self.worker_thread.join(timeout=5)

		self.running = False
		self.logger.info("In-process alarm service stopped")

	def _drain_logs(self):
		while True:
			if not self.running and self.log_queue.empty():
				return
			try:
				log_entry = self.log_queue.get(timeout=0.5)
			except queue.Empty:
				continue
			except Exception:
				continue

			level = str(log_entry.get("level", "INFO")).upper()
			message = log_entry.get("message", "")
			worker_id = log_entry.get("worker_id", "alarm_worker")
			log_line = f"[{worker_id}] {message}"

			if level == "ERROR":
				self.logger.error(log_line)
			elif level == "WARNING":
				self.logger.warning(log_line)
			elif level == "DEBUG":
				self.logger.debug(log_line)
			else:
				self.logger.info(log_line)


def main():
	# --- Single Instance Check ---
	from utils.single_instance import ensure_single_instance
	_instance_lock = ensure_single_instance("modbus_webserver")

	logger.info("🌐 Starting Flask Modbus Monitor - via modbus_monitor_webserver.py")
	logger.info("• This process runs the web interface (Socket.IO enabled)")
	
	host = os.environ.get("FLASK_HOST", "0.0.0.0")
	try:
		port = int(os.environ.get("FLASK_PORT", "5000"))
	except ValueError:
		port = 5000
		logger.warning("Invalid FLASK_PORT in environment, using default 5000")

	logger.info(f"Server will listen on {host}:{port}")

	# Initialize (no-op in webapp-only mode, kept for compatibility)
	try:
		initialize_process_manager()
		logger.info("Process manager initialized successfully")
	except Exception as e:
		logger.warning(f"Process manager initialization failed (may be normal): {e}")

	# Default app hooks for alarm runtime control (used by Flask routes).
	if not hasattr(app, "alarm_command_queue"):
		app.alarm_command_queue = None

	inprocess_alarm = None
	if _env_flag("ALARM_IN_MAIN_PROCESS", default=False):
		inprocess_alarm = InProcessAlarmService(app, logger)
		inprocess_alarm.start()
		atexit.register(inprocess_alarm.stop)
	else:
		logger.info("Alarm in-process mode disabled. Use standalone alarm worker as before.")

	try:
		# Run with socketio (eventlet/monkey_patch is handled inside webapp/app.py)
		logger.info("Starting SocketIO server...")
		socketio.run(app, host=host, port=port, debug=False, allow_unsafe_werkzeug=True)
	except KeyboardInterrupt:
		logger.info("Server shutdown requested via keyboard interrupt")
		raise
	except Exception as e:
		logger.error(f"Fatal server error: {e}")
		import traceback
		logger.error("Full traceback:\n" + traceback.format_exc())
		raise


if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		if 'logger' in locals():
			logger.info("🛑 Server stopped by user")
		print("\n🛑 Server stopped")
	except Exception as e:
		if 'logger' in locals():
			logger.critical(f"❌ Critical error: {e}")
			import traceback
			logger.critical("Full traceback:\n" + traceback.format_exc())
		print(f"❌ Error: {e}")
		import traceback
		traceback.print_exc()
		sys.exit(1)

