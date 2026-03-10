"""
Unified entrypoint to start the Flask webapp without calling webapp/app.py directly.

Usage:
  python modbus_monitor_webserver.py

This imports the Socket.IO app from webapp/app.py and runs it.
"""

import os
import sys
import logging
from logging.handlers import RotatingFileHandler

# Ensure project root is on sys.path so `webapp` imports correctly
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
	sys.path.insert(0, PROJECT_ROOT)

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

