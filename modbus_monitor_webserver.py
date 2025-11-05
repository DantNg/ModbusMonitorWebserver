"""
Unified entrypoint to start the Flask webapp without calling webapp/app.py directly.

Usage:
  python modbus_monitor_webserver.py

This imports the Socket.IO app from webapp/app.py and runs it.
"""

import os
import sys

# Ensure project root is on sys.path so `webapp` imports correctly
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
	sys.path.insert(0, PROJECT_ROOT)

try:
	# Import pre-configured app and socketio from webapp
	from webapp.app import app, socketio, initialize_process_manager
except Exception as e:
	# Fallback: try adding the webapp folder explicitly
	WEBAPP_DIR = os.path.join(PROJECT_ROOT, 'webapp')
	if WEBAPP_DIR not in sys.path:
		sys.path.insert(0, WEBAPP_DIR)
	from webapp.app import app, socketio, initialize_process_manager  # type: ignore


def main():
	print("🌐 Starting Flask Modbus Monitor - via modbus_monitor_webserver.py")
	print("• This process runs the web interface (Socket.IO enabled)")
	host = os.environ.get("FLASK_HOST", "0.0.0.0")
	try:
		port = int(os.environ.get("FLASK_PORT", "5000"))
	except ValueError:
		port = 5000

	# Initialize (no-op in webapp-only mode, kept for compatibility)
	try:
		initialize_process_manager()
	except Exception:
		pass

	# Run with socketio (eventlet/monkey_patch is handled inside webapp/app.py)
	socketio.run(app, host=host, port=port, debug=False, allow_unsafe_werkzeug=True)


if __name__ == "__main__":
	try:
		main()
	except KeyboardInterrupt:
		print("\n🛑 Server stopped")
	except Exception as e:
		print(f"❌ Error: {e}")
		import traceback
		traceback.print_exc()

