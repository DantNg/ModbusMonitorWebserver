from flask import Blueprint
subdash_bp = Blueprint("subdash_bp", __name__, url_prefix="/subdash")

from . import routes  # noqa
