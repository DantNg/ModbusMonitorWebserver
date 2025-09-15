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
        - IEEE754 Float/Double: show with decimal places
        - Display formats: Hex, Binary, Raw
        - Integer types: show without decimals if whole number
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
            
            # IEEE754 Float types: show with 2 decimal places
            if datatype_lower in ('float', 'float32', 'real', 'float_inverse', 'floatinverse', 'float-inverse'):
                return f"{num_value:.2f}"
            # IEEE754 Double types: show with 4 decimal places for higher precision
            elif datatype_lower in ('double', 'float64', 'double_inverse', 'doubleinverse', 'double-inverse'):
                return f"{num_value:.4f}"
            # Display formats
            elif datatype_lower == 'hex':
                int_val = int(abs(num_value))
                return f"0x{int_val:X}"
            elif datatype_lower in ('binary', 'bit', 'bool', 'boolean'):
                int_val = int(abs(num_value))
                return f"0b{int_val:b}"
            elif datatype_lower == 'raw':
                return str(num_value)  # Raw unprocessed value
            # Integer types: show without decimals if whole number
            elif datatype_lower in ('signed', 'unsigned', 'word', 'short', 'dword', 'dint', 'long', 'long_inverse', 'longinverse', 'long-inverse', 'int16', 'int32', 'uint16', 'uint32', 'ushort', 'udint', 'int64'):
                if num_value.is_integer():
                    return f"{int(num_value)}"
                else:
                    return f"{num_value:.2f}"  # Fallback for fractional values after scaling
        
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
