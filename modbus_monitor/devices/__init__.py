from flask import Blueprint
devices_bp = Blueprint("devices_bp", __name__)
from . import routes  # noqa
