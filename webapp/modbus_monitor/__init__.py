from flask import Flask
from .dashboard import dashboard_bp
from .alarms import alarms_bp
from .devices import devices_bp
from .reports import reports_bp
from .logger_settings import logger_settings_bp
from .auth import auth_bp
from .subdashboards import subdash_bp
from .datalogger import datalogger_bp
from .database.db import init_engine, create_schema
import os
import asyncio
import logging, sys
from .extensions import socketio
import json


def create_app():
    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    init_engine()
    create_schema()

    # Get project root directory for config path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    smtp_cfg_path = os.path.join(project_root, "config", "SMTP_config.json")

    with open(smtp_cfg_path) as config_file:
        smtp_cfg = json.load(config_file)
    app.secret_key = smtp_cfg.get("SECRET_KEY")

    # App naming configuration: support customizing brand/title, login and footer names
    # Sources (priority): root 'config.txt' then 'config/config.txt'
    # Supported formats:
    #  - Plain text: first line is the brand/title name
    #  - JSON: {"app_name":"...", "brand_name":"...", "login_name":"...", "footer_name":"..."}
    default_brand = "Modbus Monitor"
    brand_name = default_brand
    login_name = None
    footer_name = None

    def _parse_names(text: str):
        nonlocal brand_name, login_name, footer_name
        text = (text or '').strip()
        if not text:
            return
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                # Back-compat: app_name maps to brand_name
                if data.get('app_name') and not data.get('brand_name'):
                    data['brand_name'] = data['app_name']
                if data.get('brand_name'):
                    brand_name = str(data.get('brand_name')).strip() or brand_name
                if data.get('login_name'):
                    login_name = str(data.get('login_name')).strip() or login_name
                if data.get('footer_name'):
                    footer_name = str(data.get('footer_name')).strip() or footer_name
                return
        except Exception:
            # Not JSON -> treat as plain text brand name (first non-empty line)
            first_line = text.splitlines()[0].strip()
            if first_line:
                brand_name = first_line

    # Try root config.txt first
    root_cfg = os.path.join(project_root, 'config.txt')
    if os.path.exists(root_cfg):
        with open(root_cfg, 'r', encoding='utf-8') as f:
            _parse_names(f.read())
    else:
        # Fallback: config/config.txt
        alt_cfg = os.path.join(project_root, "config", "config.txt")
        if os.path.exists(alt_cfg):
            with open(alt_cfg, 'r', encoding='utf-8') as f:
                _parse_names(f.read())

    # Finalize configuration
    app.config['APP_BRAND_NAME'] = brand_name or default_brand
    app.config['APP_LOGIN_NAME'] = login_name or brand_name or default_brand
    app.config['APP_FOOTER_NAME'] = footer_name or brand_name or default_brand
    # Backward-compatible alias
    app.config['APP_NAME'] = app.config['APP_BRAND_NAME']

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
        try:
            num_value = float(value)
        except (ValueError, TypeError):
            return str(value)
        if not (num_value == num_value) or abs(num_value) == float('inf'):
            return '—'
        if num_value == 0.0:
            num_value = 0.0
        if datatype:
            datatype_lower = datatype.lower()
            if datatype_lower in ('float', 'float32', 'real', 'float_inverse', 'floatinverse', 'float-inverse'):
                return f"{num_value:.2f}"
            elif datatype_lower in ('double', 'float64', 'double_inverse', 'doubleinverse', 'double-inverse'):
                return f"{num_value:.4f}"
            elif datatype_lower == 'hex':
                int_val = int(abs(num_value))
                return f"0x{int_val:X}"
            elif datatype_lower in ('binary', 'bit', 'bool', 'boolean'):
                int_val = int(abs(num_value))
                return f"0b{int_val:b}"
            elif datatype_lower == 'raw':
                return str(num_value)
            elif datatype_lower in ('signed', 'unsigned', 'word', 'short', 'dword', 'dint', 'long', 'long_inverse', 'longinverse', 'long-inverse', 'int16', 'int32', 'uint16', 'uint32', 'ushort', 'udint', 'int64'):
                if num_value.is_integer():
                    return f"{int(num_value)}"
                else:
                    return f"{num_value:.2f}"
        if num_value.is_integer():
            return f"{int(num_value)}"
        else:
            return f"{num_value:.2f}"

    # Register blueprints
    app.register_blueprint(auth_bp, url_prefix="/auth")
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(alarms_bp, url_prefix="/alarms")
    app.register_blueprint(devices_bp)
    app.register_blueprint(reports_bp)
    app.register_blueprint(logger_settings_bp)
    app.register_blueprint(subdash_bp)
    app.register_blueprint(datalogger_bp)
    socketio.init_app(app)

    # Inject globals to all templates
    @app.context_processor
    def inject_app_globals():
        return dict(
            app_name=app.config.get('APP_BRAND_NAME', default_brand),  # backward alias
            app_brand=app.config.get('APP_BRAND_NAME', default_brand),
            app_login=app.config.get('APP_LOGIN_NAME', default_brand),
            app_footer=app.config.get('APP_FOOTER_NAME', default_brand),
        )

    return app
