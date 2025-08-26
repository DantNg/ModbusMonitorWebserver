from flask import Flask
from .dashboard import dashboard_bp
from .alarms import alarms_bp
from .devices import devices_bp
from .reports import reports_bp
from .logger_settings import logger_settings_bp
from .auth import auth_bp
from .database.db import init_engine, create_schema
import os
import asyncio
import logging, sys
from .extensions import socketio
import json


# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
#     handlers=[logging.StreamHandler(sys.stdout)]
# )
def create_app():
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    init_engine()
    create_schema()
    with open("config/SMTP_config.json") as config_file:
        config = json.load(config_file)
    app.secret_key = config.get("SECRET_KEY")

    # Đăng ký các blueprint
    app.register_blueprint(auth_bp,url_prefix="/auth")
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(alarms_bp,url_prefix="/alarms")
    app.register_blueprint(devices_bp)
    app.register_blueprint(reports_bp)
    app.register_blueprint(logger_settings_bp)
    socketio.init_app(app)
    return app
