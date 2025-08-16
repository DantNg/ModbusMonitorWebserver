from flask import Blueprint
dashboard_bp = Blueprint("dashboard_bp", __name__)
from . import routes  # noqa
