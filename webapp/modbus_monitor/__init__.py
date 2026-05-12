from flask import Flask, request
from .dashboard import dashboard_bp
from .alarms import alarms_bp
from .devices import devices_bp
from .reports import reports_bp
from .logger_settings import logger_settings_bp
from .auth import auth_bp
from .subdashboards import subdash_bp
from .datalogger import datalogger_bp
from .license import license_bp
from .database.db import init_engine, create_schema
import os
import asyncio
import logging, sys
import time
from .extensions import socketio
import json


def create_app():
    # Use absolute paths for template_folder and static_folder.
    # Relative paths can break after extended uptime (10+ days) when
    # eventlet/PyInstaller alters the working directory or temp paths.
    #
    # PyInstaller EXE: __file__ resolves inside _MEIPASS temp dir which
    # already contains the bundled webapp/ tree, so the same logic works.
    # We also add a fallback: if _MEIPASS exists, prefer that base path.
    _meipass = getattr(sys, '_MEIPASS', None)
    if _meipass:
        # Running as PyInstaller EXE – webapp/ is bundled under _MEIPASS
        _webapp_dir = os.path.join(_meipass, 'webapp')
    else:
        # Running from source
        _pkg_dir = os.path.dirname(os.path.abspath(__file__))
        _webapp_dir = os.path.dirname(_pkg_dir)

    _abs_template = os.path.join(_webapp_dir, 'templates')
    _abs_static = os.path.join(_webapp_dir, 'static')

    app = Flask(
        __name__,
        template_folder=_abs_template,
        static_folder=_abs_static,
    )
    init_engine()
    create_schema()

    # Cache static files in browser for 1 hour (3600s).
    # Reduces load on eventlet file serving and prevents
    # "static not found" errors caused by file descriptor exhaustion.
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 3600

    # Get project root directory for config path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))

    # Robust resolution of SMTP_config.json for both source and PyInstaller EXE
    smtp_cfg_path = None
    env_override = os.environ.get("SMTP_CONFIG_PATH")
    meipass = getattr(sys, "_MEIPASS", None)
    try:
        exe_dir = os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else None
    except Exception:
        exe_dir = None

    candidates = []
    if env_override:
        candidates.append(env_override)
    if exe_dir:
        candidates.append(os.path.join(exe_dir, "config", "SMTP_config.json"))
        candidates.append(os.path.join(exe_dir, "SMTP_config.json"))
    # Current working directory
    candidates.append(os.path.join(os.getcwd(), "config", "SMTP_config.json"))
    candidates.append(os.path.join(os.getcwd(), "SMTP_config.json"))
    # Project root (when running from source)
    candidates.append(os.path.join(project_root, "config", "SMTP_config.json"))
    # PyInstaller temp folder (last resort)
    if meipass:
        candidates.append(os.path.join(meipass, "config", "SMTP_config.json"))

    for p in candidates:
        if p and os.path.exists(p):
            smtp_cfg_path = p
            break

    if not smtp_cfg_path:
        raise FileNotFoundError(
            "SMTP_config.json not found. Looked in: " + "; ".join(candidates)
        )

    with open(smtp_cfg_path, 'r', encoding='utf-8') as config_file:
        smtp_cfg = json.load(config_file)
    app.secret_key = smtp_cfg.get("SECRET_KEY")

    # App naming configuration: support customizing brand/title, login and footer names
    # Sources (priority): root 'web_config.txt' then 'config/web_config.txt'
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

    # Try root web_config.txt first
    root_cfg = os.path.join(project_root, 'web_config.txt')
    if os.path.exists(root_cfg):
        with open(root_cfg, 'r', encoding='utf-8') as f:
            _parse_names(f.read())
    else:
        # Fallback: config/web_config.txt
        alt_cfg = os.path.join(project_root, "config", "web_config.txt")
        if os.path.exists(alt_cfg):
            with open(alt_cfg, 'r', encoding='utf-8') as f:
                _parse_names(f.read())

    # Finalize configuration
    app.config['APP_BRAND_NAME'] = brand_name or default_brand
    app.config['APP_LOGIN_NAME'] = login_name or brand_name or default_brand
    app.config['APP_FOOTER_NAME'] = footer_name or brand_name or default_brand
    # Backward-compatible alias
    app.config['APP_NAME'] = app.config['APP_BRAND_NAME']
    app.config['ASSET_VERSION'] = int(time.time())

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
                    return f"{round(num_value, 2):g}"
        if num_value.is_integer():
            return f"{int(num_value)}"
        else:
            return f"{round(num_value, 2):g}"

    @app.template_filter('contrast_color')
    def contrast_color_filter(hex_color):
        """Return #1a1a1a (dark) or #ffffff (light) for readable text contrast on the given hex background."""
        if not hex_color:
            return ''
        h = hex_color.lstrip('#')
        if len(h) != 6:
            return '#ffffff'
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        yiq = (r * 299 + g * 587 + b * 114) / 1000
        return '#1a1a1a' if yiq >= 140 else '#ffffff'

    # Register blueprints
    app.register_blueprint(auth_bp, url_prefix="/auth")
    app.register_blueprint(license_bp, url_prefix="/license")
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(alarms_bp, url_prefix="/alarms")
    app.register_blueprint(devices_bp)
    app.register_blueprint(reports_bp)
    app.register_blueprint(logger_settings_bp)
    app.register_blueprint(subdash_bp)
    app.register_blueprint(datalogger_bp)
    socketio.init_app(app)

    # ----- License guard: redirect to activation page if no valid license -----
    # Paths that are always accessible (license page itself, auth, static assets)
    _LICENSE_EXEMPT = ("/license", "/auth", "/static")

    @app.before_request
    def check_license_guard():
        """Block access to all routes when no valid license is present."""
        # Allow exempt paths unconditionally
        if any(request.path.startswith(p) for p in _LICENSE_EXEMPT):
            return None

        from .license_manager import is_license_valid
        from flask import redirect, url_for
        if not is_license_valid():
            return redirect(url_for("license_bp.activate"))
        return None

    # ----- Static file robustness for long uptime (10+ days) -----
    # Add explicit Cache-Control headers for static assets so browsers
    # cache CSS/JS/images locally and don't re-request each page load.
    # Also add error recovery: if static file serving fails, log and
    # return a useful error instead of crashing the request context.
    @app.after_request
    def add_static_cache_headers(response):
        """Add long-lived cache headers for static assets."""
        if request.path.startswith('/static/'):
            # Cache static files for 1 hour in browser, allow revalidation
            response.headers['Cache-Control'] = 'public, max-age=3600, stale-while-revalidate=86400'
        return response

    @app.errorhandler(500)
    def handle_500(e):
        """Catch static file serving errors gracefully."""
        if request.path.startswith('/static/'):
            # Log the error for diagnostics
            print(f'⚠️ Static file error: {request.path} — {e}')
            # Return a specific message instead of a full crash page
            from flask import make_response
            resp = make_response(f'/* Static file temporarily unavailable: {request.path} */', 503)
            resp.headers['Retry-After'] = '5'
            resp.headers['Content-Type'] = 'text/plain'
            return resp
        # For non-static 500s, use default behavior
        return e

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
