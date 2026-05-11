from flask import Blueprint

license_bp = Blueprint("license_bp", __name__)

from . import routes  # noqa
