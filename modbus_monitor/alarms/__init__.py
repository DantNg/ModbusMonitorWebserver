from flask import Blueprint
alarms_bp = Blueprint("alarms_bp", __name__)
from . import routes  # noqa
