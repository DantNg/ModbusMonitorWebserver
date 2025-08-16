import logging,os,sys
from modbus_monitor import create_app
from modbus_monitor.services.runner import start_services

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
app = create_app()

if __name__ == "__main__":
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        start_services()
        app.logger.info("Services started (Modbus/Alarm/DataLogger).")

    app.run(debug=True, port=1234)
