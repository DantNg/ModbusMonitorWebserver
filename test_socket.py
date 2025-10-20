import socketio
import time

sio = socketio.Client()
sio.connect('http://localhost:5000')

def send_alarm_event():
    # Join room/subdashboard trước
    # sio.emit('join_subdashboard', {'room': 'subdashboard_19'})
    time.sleep(0.2)  # Đợi server xác nhận join (nên có)
    data = {
        "title": f"Alarm Triggered: hello",
        "message": f"Alarm cleared: ",
        "level": "High",
        "tag_name": 123,
        "value": 123,
        "status": "INCOMING",
        "created_at": time.strftime('%Y-%m-%dT%H:%M:%S')
    }
    sio.emit('alarm_event', data)
    print('Sent alarm_event:', data)

if __name__ == '__main__':
    send_alarm_event()
    time.sleep(1)
    sio.disconnect()