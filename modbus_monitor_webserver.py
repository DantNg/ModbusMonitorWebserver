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

# Setup logging for the main server process
def setup_main_logging():
	"""Setup logging for the main server process"""
	# Create logs directory
	logs_dir = os.path.join(PROJECT_ROOT, 'logs')
	os.makedirs(logs_dir, exist_ok=True)
	
	# Create main logger
	main_logger = logging.getLogger('modbus_webserver_main')
	main_logger.setLevel(logging.INFO)
	
	# Avoid duplicate handlers
	if main_logger.handlers:
		return main_logger
	
	# File handler with rotation
	log_file = os.path.join(logs_dir, 'webserver_main.log')
	file_handler = RotatingFileHandler(
		log_file, maxBytes=1024*1024, backupCount=5, encoding='utf-8'
	)
	file_handler.setLevel(logging.INFO)
	
	# Console handler
	console_handler = logging.StreamHandler(sys.stdout)
	console_handler.setLevel(logging.INFO)
	
	# Formatter
	formatter = logging.Formatter(
		'%(asctime)s [%(levelname)s] %(name)s: %(message)s',
		datefmt='%Y-%m-%d %H:%M:%S'
	)
	file_handler.setFormatter(formatter)
	console_handler.setFormatter(formatter)
	
	main_logger.addHandler(file_handler)
	main_logger.addHandler(console_handler)
	
	return main_logger

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

