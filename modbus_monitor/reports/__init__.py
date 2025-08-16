from flask import Blueprint
reports_bp = Blueprint("reports_bp", __name__)
from . import routes  # noqa
