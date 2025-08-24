import eventlet
eventlet.monkey_patch()
import logging,os,sys
from flask import redirect, url_for
from modbus_monitor import create_app, socketio
from modbus_monitor.services.runner import start_services
from modbus_monitor.extensions import socketio
app = create_app()

@app.route("/")
def root():
    print("Start login")
    return redirect(url_for("auth_bp.login"))
if __name__ == "__main__":
    try:
        if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
            start_services()
            # app.logger.info("Services started (Modbus/Alarm/DataLogger).")

        socketio.run(app, debug=True,port=5000)
    except Exception as e:
        print(f"Error starting the application: {e}")
