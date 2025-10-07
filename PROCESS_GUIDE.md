# 📋 Flask Modbus Monitor - Independent Process Guide

## 🎯 Kiến Trúc Mới: Multi-Process Architecture

Hệ thống đã được tách thành **5 process độc lập** để dễ dàng debug và quản lý:

1. **🌐 Webapp** - Web interface & configuration only
2. **📻 RTU Workers** - Modbus RTU communication  
3. **🌐 TCP Workers** - Modbus TCP communication
4. **📊 Datalogger** - Data logging to database/files
5. **🚨 Alarm System** - Alarm monitoring & notifications

---

## 🚀 Cách Chạy Từng Process

### 1. **Web Interface (Webapp)**
```bash
# Terminal 1 - Chạy web interface
python run_webapp.py --port 5000

# Truy cập: http://localhost:5000
# ✅ UI để quản lý devices, tags, alarms
# ✅ Không chạy workers
```

### 2. **RTU Workers** 
```bash
# Terminal 2 - Chạy RTU worker cho COM3
python run_rtu_worker.py COM3 --baudrate 9600

# Terminal 3 - Chạy RTU worker cho COM4 (nếu có)
python run_rtu_worker.py COM4 --baudrate 19200

# ✅ Mỗi COM port cần 1 process riêng
# ✅ Tự động load devices từ database
```

### 3. **TCP Workers**
```bash
# Terminal 4 - Chạy TCP worker cho 192.168.1.100:502
python run_tcp_worker.py 192.168.1.100 502

# Terminal 5 - Chạy TCP worker cho 192.168.1.101:502 (nếu có)
python run_tcp_worker.py 192.168.1.101 502 --timeout 10

# ✅ Mỗi IP:Port cần 1 process riêng
# ✅ Tự động load devices từ database
```

### 4. **Data Logger**
```bash
# Terminal 6 - Chạy datalogger
python run_datalogger.py --interval 60 --database --csv

# Options:
# --interval 60        : Log mỗi 60 giây
# --database          : Lưu vào MySQL database  
# --csv               : Lưu vào file CSV
# --json              : Lưu vào file JSON
# --output-dir ./logs : Thư mục output
```

### 5. **Alarm System**
```bash
# Terminal 7 - Chạy alarm system
python run_alarm.py --check-interval 10 --email --sms

# Options:
# --check-interval 10 : Kiểm tra alarm mỗi 10 giây
# --email            : Bật email notifications
# --sms              : Bật SMS notifications  
# --webhook          : Bật webhook notifications
# --slack            : Bật Slack notifications
```

---

## 📊 Database Configuration

Tất cả process đều dùng **MySQL database** được config trong:
```json
config/SMTP_config.json:
{
  "MYSQL_URI": "mysql+pymysql://root:123456@127.0.0.1:3306/modbus_monitor_db",
  "POOL_SIZE": 8
}
```

**✅ Ưu điểm:**
- Nhiều process cùng truy cập database an toàn
- Pool connection tự động quản lý
- Thread-safe operations

---

## 🔧 Workflow Hoàn Chỉnh

### **Bước 1: Chuẩn Bị**
1. Start MySQL server
2. Ensure database `modbus_monitor_db` exists
3. Install dependencies: `pip install pymysql sqlalchemy pymodbus`

### **Bước 2: Start Processes**
```bash
# Terminal 1: Web Interface
python run_webapp.py --port 5000

# Terminal 2: RTU Communication  
python run_rtu_worker.py COM3 --baudrate 9600

# Terminal 3: TCP Communication
python run_tcp_worker.py 192.168.1.100 502

# Terminal 4: Data Logging
python run_datalogger.py --interval 60 --database --csv

# Terminal 5: Alarm Monitoring
python run_alarm.py --check-interval 10 --email
```

### **Bước 3: Configuration**
1. Access webapp: `http://localhost:5000`
2. Add devices trong **Devices** section
3. Add tags cho mỗi device
4. Configure alarms trong **Alarms** section
5. Setup dataloggers trong **Reports** section

### **Bước 4: Monitor**
- **Worker terminals**: Xem real-time Modbus communication
- **Datalogger terminal**: Xem data logging status
- **Alarm terminal**: Xem alarm events
- **Webapp**: Dashboard overview

---

## 🐛 Debug & Troubleshooting

### **Nếu Workers Không Tìm Thấy Devices:**
```bash
⚠️  No TCP devices found for 192.168.1.100:502
💡 Add TCP devices in the webapp first
```
**Giải pháp:** Vào webapp → Devices → Add Device với đúng IP:Port

### **Database Connection Error:**
```bash
❌ Database connection error: No module named 'pymysql'
```
**Giải pháp:** `pip install pymysql`

### **Simulation Mode:**
```bash
🛠️ Running in simulation mode (pymodbus not available)
```
**Giải pháp:** `pip install pymodbus` hoặc chạy simulation để test

---

## 🎯 Lợi Ích Kiến Trúc Mới

### **🔧 Debugging**
- Mỗi process riêng terminal → Dễ debug
- Clear console output với emojis
- Real-time error reporting

### **⚡ Performance** 
- Process độc lập → Không ảnh hưởng lẫn nhau
- Multi-core utilization
- Memory isolation

### **🛠️ Flexibility**
- Start/stop từng component riêng biệt
- Scale workers theo nhu cầu
- Easy troubleshooting

### **📈 Scalability**
- Add nhiều RTU/TCP workers
- Horizontal scaling
- Load balancing

---

## 📝 Examples

### **Scenario 1: Factory với 2 RTU + 1 TCP**
```bash
# Terminal 1: Webapp
python run_webapp.py --port 5000

# Terminal 2: Production line RTU
python run_rtu_worker.py COM3 --baudrate 9600

# Terminal 3: Quality control RTU  
python run_rtu_worker.py COM4 --baudrate 19200

# Terminal 4: SCADA TCP connection
python run_tcp_worker.py 192.168.1.50 502

# Terminal 5: Data logging mỗi 30s
python run_datalogger.py --interval 30 --database --csv

# Terminal 6: Critical alarms
python run_alarm.py --check-interval 5 --email --sms
```

### **Scenario 2: Development Testing**
```bash
# Chỉ webapp để config
python run_webapp.py --port 5000

# Simulation worker để test
python run_tcp_worker.py 127.0.0.1 502
# → Sẽ chạy simulation mode với fake data
```

---

## 🎉 Hoàn Thành!

Bây giờ bạn có thể:
- **🌐** Quản lý UI qua webapp
- **📻** Monitor RTU communication riêng biệt  
- **🌐** Monitor TCP communication riêng biệt
- **📊** Track data logging
- **🚨** Monitor alarms real-time
- **🐛** Debug từng component độc lập