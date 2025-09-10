from flask import Flask
from .dashboard import dashboard_bp
from .alarms import alarms_bp
from .devices import devices_bp
from .reports import reports_bp
from .logger_settings import logger_settings_bp
from .auth import auth_bp
from .subdashboards import subdash_bp
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

    # Custom Jinja filters
    @app.template_filter('format_value')
    def format_value_filter(value, datatype=None):
        """Format numeric values based on datatype rules:
        - Float: show with 2 decimal places (.00)
        - Integer types (Word, DWord, Bit): show without decimals
        """
        if value is None or value == '':
            return '—'
        
        # Handle non-numeric values
        try:
            num_value = float(value)
        except (ValueError, TypeError):
            return str(value)
        
        # Check if it's NaN or infinite
        if not (num_value == num_value) or abs(num_value) == float('inf'):
            return '—'
        
        # Fix -0.0 display issue
        if num_value == 0.0:
            num_value = 0.0
        
        # Format based on datatype
        if datatype:
            datatype_lower = datatype.lower()
            # Float types: show with .00
            if datatype_lower in ('float', 'float32', 'real'):
                return f"{num_value:.2f}"
            # Integer types: show without decimals if whole number
            elif datatype_lower in ('word', 'short', 'dword', 'dint', 'bit', 'int16', 'int32', 'uint16', 'uint32', 'ushort', 'udint'):
                if num_value.is_integer():
                    return f"{int(num_value)}"
                else:
                    return f"{num_value:.2f}"  # Fallback for fractional values after scaling
        else:
            # Default behavior when no datatype specified
            if num_value.is_integer():
                return f"{int(num_value)}"
            else:
                return f"{num_value:.2f}"

    # Đăng ký các blueprint
    app.register_blueprint(auth_bp,url_prefix="/auth")
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(alarms_bp,url_prefix="/alarms")
    app.register_blueprint(devices_bp)
    app.register_blueprint(reports_bp)
    app.register_blueprint(logger_settings_bp)
    app.register_blueprint(subdash_bp)
    socketio.init_app(app)
    return app
