from flask_socketio import SocketIO

# Initialize SocketIO (without app)
socketio = SocketIO(cors_allowed_origins="*")