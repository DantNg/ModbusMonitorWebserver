import eventlet
eventlet.monkey_patch()

import os
from flask import redirect, url_for
from modbus_monitor import create_app
from modbus_monitor.extensions import socketio
from modbus_monitor.services.runner import start_services

app = create_app()

@app.route("/")
def root():
    print("Start login")
    return redirect(url_for("auth_bp.login"))

if __name__ == "__main__":
    try:
        # Luôn start services trong production
        start_services()

        socketio.run(app, host="0.0.0.0", port=5000, debug=False)

    except Exception as e:
        print(f"Error starting the application: {e}")
