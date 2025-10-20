import socketio
import time

sio = socketio.Client()
sio.connect('http://localhost:5000')

def send_alarm_event():
    # Join room/subdashboard trước
    # sio.emit('join_subdashboard', {'room': 'subdashboard_19'})
    time.sleep(0.2)  # Đợi server xác nhận join (nên có)
    data = {
        'id': 999,
        'title': 'Test Alarm',
        'message': 'Test alarm from Python client',
        'level': 'Critical',
        'tag_id': 123,
        'tag_name': 'TestTag',
        'device_name': 'TestDevice',
        'value': 42.0,
        'status': 'Active',
        'created_at': time.strftime('%Y-%m-%dT%H:%M:%S')
    }
    sio.emit('alarm_event', data)
    print('Sent alarm_event:', data)

if __name__ == '__main__':
    send_alarm_event()
    time.sleep(1)
    sio.disconnect()