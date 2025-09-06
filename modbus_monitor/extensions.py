from flask_socketio import SocketIO
from engineio.async_drivers import eventlet
from flask_socketio import SocketIO, join_room, emit

# Initialize SocketIO (without app)
socketio = SocketIO(async_mode="eventlet",cors_allowed_origins="*")
@socketio.on('join')
def on_join(data):
    room = data['room']
    join_room(room)

# Khi modbus service có dữ liệu mới:
def emit_dashboard_update(data):
    socketio.emit('update_tags', data, room='dashboard')

def emit_subdashboard_update(subdash_id, data):
    socketio.emit('update_tags', data, room=f'subdashboard_{subdash_id}')