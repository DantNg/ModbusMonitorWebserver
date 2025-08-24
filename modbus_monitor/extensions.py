from flask_socketio import SocketIO

# Initialize SocketIO (without app)
socketio = SocketIO(async_mode="threading",cors_allowed_origins="*")