"""
Datalogger module initialization
"""
from flask import Blueprint

# Create the datalogger blueprint
datalogger_bp = Blueprint('datalogger', __name__, url_prefix='/api/datalogger')

# Import routes to register them with the blueprint
from . import routes