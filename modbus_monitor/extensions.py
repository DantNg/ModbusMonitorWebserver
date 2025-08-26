from flask_socketio import SocketIO
from engineio.async_drivers import eventlet

# Initialize SocketIO (without app)
socketio = SocketIO(async_mode="eventlet",cors_allowed_origins="*")