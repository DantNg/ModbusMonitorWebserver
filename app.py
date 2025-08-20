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
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        start_services()
        # app.logger.info("Services started (Modbus/Alarm/DataLogger).")
    socketio.run(app, debug=True,port=5000)
    # app.run(debug=True, port=1234)
    
