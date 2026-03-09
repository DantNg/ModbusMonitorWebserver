from flask_socketio import SocketIO
from engineio.async_drivers import eventlet
from flask_socketio import SocketIO, join_room, leave_room, emit

# Initialize SocketIO (without app)
# ping_timeout=60s / ping_interval=25s: cho phép client tạm ngưng (background tab)
# mà không bị server đóng kết nối quá sớm (default chỉ 20s timeout)
socketio = SocketIO(
    async_mode="eventlet",
    cors_allowed_origins="*",
    ping_timeout=60,
    ping_interval=25,
)

@socketio.on('join')
def on_join(data):
    room = data['room']
    join_room(room)
    print(f"Client joined room: {room}")

@socketio.on('leave')
def on_leave(data):
    room = data['room']
    leave_room(room)
    print(f"Client left room: {room}")

# Khi modbus service có dữ liệu mới:
def emit_dashboard_update(data):
    # Emit to "all devices" view
    socketio.emit('update_tags', data, room='dashboard_all')


def emit_subdashboard_update(subdash_id, data):
    socketio.emit('modbus_update', data, room=f'subdashboard_{subdash_id}')