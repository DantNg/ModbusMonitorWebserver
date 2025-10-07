from flask import Blueprint
logger_settings_bp = Blueprint("logger_settings_bp", __name__)
from . import routes  # noqa
