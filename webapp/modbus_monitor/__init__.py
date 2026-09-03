from flask import Flask, request, session, redirect, url_for, jsonify
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
import hashlib
import mimetypes
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

    # Session cookie hardening.
    #  - HTTPONLY: JavaScript cannot read the session cookie (mitigates XSS theft).
    #  - SAMESITE=Lax: cookie is NOT sent on cross-site POST/sub-requests, which
    #    blocks CSRF and stops a malicious page from driving Socket.IO writes with
    #    the victim's session. Same-site fetch()/navigation still works normally.
    #  NOTE: SESSION_COOKIE_SECURE is intentionally NOT enabled because the app is
    #  served over plain HTTP (0.0.0.0:5000). Turning it on would stop the browser
    #  from ever sending the cookie and break login. Enable it once TLS is in front.
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

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
    def format_value_filter(value, arg1=None, arg2=None, arg3=None):
        """Format numeric values.

        Preferred behavior: decimal digits follow tag scale precision.
        - scale 0.1  -> 1 decimal (e.g. 12.3)
        - scale 0.01 -> 2 decimals (e.g. 12.34)

        Backward compatibility:
        - old call style: format_value(value, datatype)
        - new call style: format_value(value, scale, datatype)
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

        # Parse args with backward compatibility.
        datatype = None
        scale = None
        offset = 0
        known_datatypes = {
            'float', 'float32', 'real', 'float_inverse', 'floatinverse', 'float-inverse',
            'double', 'float64', 'double_inverse', 'doubleinverse', 'double-inverse',
            'hex', 'binary', 'bit', 'bool', 'boolean', 'raw',
            'signed', 'unsigned', 'word', 'short', 'dword', 'dint', 'long',
            'long_inverse', 'longinverse', 'long-inverse', 'int16', 'int32',
            'uint16', 'uint32', 'ushort', 'udint', 'int64'
        }

        if isinstance(arg1, str) and arg1.lower() in known_datatypes:
            datatype = arg1
            scale = arg2
            offset = arg3
        else:
            scale = arg1
            if isinstance(arg2, str) and arg2.lower() in known_datatypes:
                datatype = arg2
                offset = arg3
            else:
                offset = arg2
                datatype = arg3

        def _scale_decimals(scale_value):
            try:
                scale_float = abs(float(scale_value))
                if scale_float == 0.0:
                    return 0
                # Keep precision from string representation without trailing zeros.
                scale_text = f"{scale_float:.12f}".rstrip('0').rstrip('.')
                if '.' not in scale_text:
                    return 0
                return min(6, len(scale_text.split('.', 1)[1]))
            except Exception:
                return None

        def _display_decimals(scale_value):
            try:
                scale_float = abs(float(scale_value))
            except Exception:
                return None

            # Explicit display policy requested by user.
            if abs(scale_float - 1.0) < 1e-9:
                return None
            if abs(scale_float - 0.1) < 1e-9:
                return 1
            if scale_float >= 0.2:
                return 2

            # Fallback for other uncommon scales.
            return _scale_decimals(scale_float)

        # Datatypes with explicit non-decimal representations.
        if datatype:
            datatype_lower = datatype.lower()
            if datatype_lower == 'hex':
                int_val = int(abs(num_value))
                return f"0x{int_val:X}"
            if datatype_lower in ('binary', 'bit', 'bool', 'boolean'):
                int_val = int(abs(num_value))
                return f"0b{int_val:b}"
            if datatype_lower == 'raw':
                return str(num_value)

        # Apply engineering transform first: display = raw * scale + offset.
        # NOTE: Giá trị trong DB đã là engineering units (worker đã áp scale+offset tại nguồn).
        # Khối này được giữ lại nhưng KHÔNG thực hiện transform nữa để tránh double-apply.
        # (scale/offset vẫn được dùng bên dưới để xác định số chữ số thập phân.)

        # Primary rule: decimals follow scale precision when scale is provided.
        try:
            scale_float = abs(float(scale)) if scale is not None else None
        except Exception:
            scale_float = None

        if scale_float is not None and abs(scale_float - 1.0) < 1e-9:
            return f"{int(num_value)}" if num_value.is_integer() else f"{round(num_value, 2):g}"

        decimals = _display_decimals(scale)
        if decimals is not None:
            return f"{num_value:.{decimals}f}"

        # Fallback behavior when scale is absent.
        if datatype:
            datatype_lower = datatype.lower()
            if datatype_lower in ('float', 'float32', 'real', 'float_inverse', 'floatinverse', 'float-inverse'):
                return f"{num_value:.2f}"
            if datatype_lower in ('double', 'float64', 'double_inverse', 'doubleinverse', 'double-inverse'):
                return f"{num_value:.4f}"
        if num_value.is_integer():
            return f"{int(num_value)}"
        else:
            return f"{round(num_value, 2):g}"

    @app.template_filter('format_fixed_value')
    def format_fixed_value_filter(value, decimal_places=None):
        """Format fixed SV values with max 2 decimals while preserving typed style.

        decimal_places should be metadata captured from the user input (0, 1, or 2).
        """
        if value is None or value == '':
            return '—'
        try:
            num_value = float(value)
        except (ValueError, TypeError):
            return str(value)

        # Avoid rendering -0 / -0.0 in UI.
        if abs(num_value) < 1e-12:
            num_value = 0.0

        try:
            dp = int(decimal_places) if decimal_places is not None else None
        except (ValueError, TypeError):
            dp = None

        if dp is None:
            # Backward compatibility for old records without metadata:
            # show up to 2 decimals but do not force trailing zeros.
            return str(int(num_value)) if float(num_value).is_integer() else str(float(f"{num_value:.2f}"))

        dp = max(0, min(2, dp))
        if dp == 0:
            return f"{int(round(num_value))}"

        # Render with exact requested precision — preserve trailing zeros as typed by user.
        # e.g. dp=2 -> "1000.00", dp=1 -> "1000.0"
        return f"{num_value:.{dp}f}"

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

    # ----- Authentication guard: require a logged-in session for every route -----
    # Previously the app had NO global auth check — most read routes (and several
    # write/API routes) were reachable without logging in. This guard closes that
    # by requiring session["user_id"] on all paths except the public ones below.
    #
    # IMPORTANT: /socket.io is exempt. Modbus/alarm workers talk to the server ONLY
    # over Socket.IO (socketio.Client → /socket.io) and never carry a browser
    # session, so blocking it here would break worker<->server traffic. Per-event
    # authorization for sensitive socket messages (e.g. modbus_write_command) is
    # enforced inside the individual handlers instead.
    _AUTH_EXEMPT = ("/auth", "/license", "/static", "/socket.io")

    @app.before_request
    def require_login_guard():
        """Redirect anonymous users to login (or return 401 JSON for API calls)."""
        p = request.path
        if any(p.startswith(x) for x in _AUTH_EXEMPT):
            return None
        if session.get("user_id"):
            return None
        # API/XHR callers get a clean 401 instead of an HTML redirect body.
        wants_json = (
            "/api/" in p
            or p.startswith("/api")
            or request.headers.get("X-Requested-With") == "XMLHttpRequest"
        )
        if wants_json:
            return jsonify({"success": False, "message": "Authentication required"}), 401
        return redirect(url_for("auth_bp.login"))

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

    # ===== Hardened static serving — fixes "CSS disappears after long uptime" =====
    #
    # ROOT CAUSE: Flask's default static view (werkzeug send_from_directory) calls
    # os.path.isfile() before serving. os.path.isfile() swallows *any* OSError and
    # returns False — including EMFILE ("too many open files") and WinError 1450
    # ("insufficient system resources") that appear once the process has been running
    # for days, and including the case where a PyInstaller --onefile build's assets
    # under %TEMP%\_MEIxxxx are pruned by Windows cleanup while the app is live.
    # When isfile() -> False, Flask returns 404 even though the file still exists, so
    # the browser drops bootstrap.min.css / style.css / *-theme.css and the page loses
    # all styling. (The previous 500 error handler never caught this — it is a 404.)
    #
    # FIX: the whole static tree is tiny (~4 MB). Preload it into RAM at startup and
    # serve straight from memory. No per-request filesystem syscall means no
    # isfile()->404 and no temp-dir race — regardless of uptime or handle pressure.
    _static_mem_cache = {}  # rel_path (posix, no leading slash) -> (bytes, mimetype, etag)

    # Windows' registry-backed mimetypes are unreliable (e.g. .js can map to
    # text/plain), which can stop browsers from executing JS. Pin the render-
    # critical types so behavior is deterministic across machines.
    _STATIC_MIME_OVERRIDES = {
        '.js': 'text/javascript', '.mjs': 'text/javascript',
        '.css': 'text/css', '.svg': 'image/svg+xml',
        '.json': 'application/json', '.map': 'application/json',
        '.woff': 'font/woff', '.woff2': 'font/woff2', '.ttf': 'font/ttf',
    }

    def _static_mimetype(path):
        ext = os.path.splitext(path)[1].lower()
        if ext in _STATIC_MIME_OVERRIDES:
            return _STATIC_MIME_OVERRIDES[ext]
        return mimetypes.guess_type(path)[0] or 'application/octet-stream'

    def _resolve_static_path(rel_key):
        """Return the on-disk path for rel_key, or None if it escapes static_folder."""
        base = app.static_folder
        if not base:
            return None
        base_norm = os.path.normpath(base)
        full = os.path.normpath(os.path.join(base_norm, rel_key))
        try:
            if os.path.commonpath([base_norm, full]) != base_norm:
                return None  # path traversal attempt
        except ValueError:
            return None
        return full

    def _read_static_from_disk(rel_key):
        """Read + cache a static file, retrying transient OSErrors.

        Returns (bytes, mimetype, etag), or None for a genuine missing file.
        A transient failure (resource exhaustion) never returns None as "missing";
        it falls back to any previously cached copy so we never emit a false 404.
        time.sleep here is eventlet-cooperative (app.py monkey-patches eventlet).
        """
        full = _resolve_static_path(rel_key)
        if full is None:
            return None
        last_err = None
        for attempt in range(5):
            try:
                with open(full, 'rb') as fh:
                    data = fh.read()
                mimetype = _static_mimetype(full)
                etag = hashlib.md5(data).hexdigest()  # content hash, stable across restarts
                entry = (data, mimetype, etag)
                _static_mem_cache[rel_key] = entry
                return entry
            except FileNotFoundError:
                return None  # real 404
            except OSError as e:
                last_err = e
                time.sleep(0.02 * (attempt + 1))
        print(f"[static] transient read failure for {rel_key}: {last_err}")
        return _static_mem_cache.get(rel_key)  # serve stale copy if we have one

    def _serve_static_hardened(filename):
        from flask import Response, abort
        rel_key = os.path.normpath(filename).replace('\\', '/').lstrip('/')
        if rel_key.startswith('../') or rel_key == '..' or os.path.isabs(filename):
            abort(404)
        entry = _static_mem_cache.get(rel_key)
        if entry is None:
            entry = _read_static_from_disk(rel_key)
        if entry is None:
            abort(404)
        data, mimetype, etag = entry
        cache_control = 'public, max-age=3600, stale-while-revalidate=86400'
        if etag in (request.headers.get('If-None-Match') or ''):
            resp = Response(status=304)
            resp.headers['ETag'] = etag
            resp.headers['Cache-Control'] = cache_control
            return resp
        resp = Response(data, mimetype=mimetype)
        resp.headers['ETag'] = etag
        resp.headers['Cache-Control'] = cache_control
        return resp

    # Swap Flask's built-in static view for the hardened one (keeps the same
    # /static/<path:filename> rule and 'static' endpoint, so url_for('static', ...)
    # is unchanged).
    if 'static' in app.view_functions:
        app.view_functions['static'] = _serve_static_hardened

    # Warm the cache at startup so assets are in RAM before the first request and
    # survive any later deletion/pruning of the source files.
    def _warm_load_static():
        base = app.static_folder
        if not base or not os.path.isdir(base):
            return 0
        count = 0
        for root, _dirs, files in os.walk(base):
            for fn in files:
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, base).replace('\\', '/')
                try:
                    with open(full, 'rb') as fh:
                        data = fh.read()
                    mimetype = _static_mimetype(full)
                    _static_mem_cache[rel] = (data, mimetype, hashlib.md5(data).hexdigest())
                    count += 1
                except OSError:
                    pass
        return count

    try:
        _warm_count = _warm_load_static()
        _warm_mb = sum(len(v[0]) for v in _static_mem_cache.values()) / 1048576
        print(f"[static] preloaded {_warm_count} files into memory ({_warm_mb:.1f} MB)")
    except Exception as _e:
        print(f"[static] warm-load skipped: {_e}")

    return app
