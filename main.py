#!/usr/bin/env python3
"""
Multi-Process Modbus Monitor - Main Entry Point
"""
import sys
import os
from multiprocessing import freeze_support

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

if __name__ == "__main__":
    # Required for Windows multiprocessing
    freeze_support()
    
    # Import main app from webapp
    from webapp.app import app, initialize_process_manager
    
    print("🚀 Starting Multi-Process Modbus Monitor...")
    print("📁 Project structure:")
    print("   ├── webapp/     - Flask web application")
    print("   ├── workers/    - Worker processes") 
    print("   ├── shared/     - Shared utilities")
    print("   └── config/     - Configuration files")
    print("")
    
    # Initialize ProcessManager
    process_manager = initialize_process_manager()
    
    try:
        # Run the Flask app with SocketIO
        from webapp.modbus_monitor.extensions import socketio
        print("Starting Flask Modbus Monitor on http://0.0.0.0:5000")
        
        # Initialize SocketIO properly with app
        socketio.init_app(app, cors_allowed_origins="*", async_mode="eventlet")
        socketio.run(app, debug=False, host="0.0.0.0", port=5000, allow_unsafe_werkzeug=True)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        # Cleanup ProcessManager
        if process_manager:
            process_manager.shutdown()
        print("Application stopped.")