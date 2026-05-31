# Modbus Monitor Webserver

Ứng dụng web giám sát thiết bị công nghiệp qua giao thức Modbus (TCP và RTU).  
Xây dựng bằng **Flask + Socket.IO**, giao diện HTML/CSS/JS, lưu trữ dữ liệu trên **MySQL**.

---

## Mục lục

1. [Tổng quan chức năng](#tổng-quan-chức-năng)
2. [Cấu trúc thư mục](#cấu-trúc-thư-mục)
3. [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
4. [Thành phần UI — Card Types](#thành-phần-ui--card-types)
5. [Hệ thống Alarm](#hệ-thống-alarm)
6. [Cách chạy dự án](#cách-chạy-dự-án)
7. [Tech Stack](#tech-stack)

---

## Tổng quan chức năng

| Chức năng | Mô tả |
|---|---|
| **Giám sát Modbus** | Đọc register từ thiết bị qua TCP/RTU theo chu kỳ |
| **Dashboard thời gian thực** | Hiển thị giá trị tag cập nhật qua Socket.IO (không cần reload trang) |
| **Sub-Dashboard** | Trang con chứa các card giám sát, phân nhóm theo tag group |
| **Alarm** | Đánh giá điều kiện alarm per-column, gửi thông báo Email/SMS |
| **Data Logger** | Ghi giá trị tag vào file CSV/DB theo chu kỳ |
| **Reports** | Xuất báo cáo dạng Excel từ dữ liệu đã log |
| **Device Management** | Quản lý danh sách thiết bị Modbus và tag |
| **License** | Kiểm soát license theo Machine UID |
| **Multi-role Auth** | Đăng nhập với phân quyền `admin` / `viewer` |

---

## Cấu trúc thư mục

```
flask_modbus_monitor/
│
├── modbus_monitor_webserver.py   # Entry point — khởi động Flask + SocketIO
│
├── webapp/                       # Ứng dụng Flask chính
│   ├── app.py                    # App factory, khởi tạo extensions
│   ├── __init__.py
│   └── modbus_monitor/           # Package chứa tất cả blueprint
│       ├── __init__.py           # create_app(), đăng ký blueprints
│       ├── extensions.py         # Khởi tạo SQLAlchemy, SocketIO, ...
│       ├── config.py             # Cấu hình Flask (SECRET_KEY, DB URL, ...)
│       ├── auth/                 # Blueprint: đăng nhập, phân quyền
│       ├── dashboard/            # Blueprint: trang chủ, danh sách tag
│       ├── subdashboards/        # Blueprint: sub-dashboard + API card
│       │   └── routes.py
│       ├── alarms/               # Blueprint: quản lý alarm rules, export
│       ├── devices/              # Blueprint: CRUD thiết bị Modbus + tag
│       ├── reports/              # Blueprint: xuất báo cáo Excel
│       ├── datalogger/           # Blueprint: cấu hình data logger
│       ├── logger_settings/      # Blueprint: cài đặt logger
│       ├── services/             # Business logic nội bộ
│       ├── routes/               # Routes phụ (API tag, units, ...)
│       ├── license/              # Kiểm tra license
│       └── database/
│           └── db.py             # Toàn bộ table definitions + CRUD functions
│
├── webapp/templates/             # Jinja2 HTML templates
│   ├── base.html
│   ├── subdashboards/
│   │   ├── detail.html           # Trang sub-dashboard chính
│   │   ├── _qtag2_cards.html     # Partial: QTAG2 cards
│   │   ├── _qtag3_cards.html     # Partial: QTAG3 cards
│   │   ├── _qtag4_cards.html     # Partial: QTAG4 cards
│   │   ├── _qtag6_cards.html     # Partial: QTAG6 cards
│   │   ├── _qtag_pv_cards.html   # Partial: PV Only cards
│   │   ├── _qtag_pv_dual_cards.html  # Partial: PV Dual cards
│   │   └── _qtag_single3_cards.html  # Partial: Single3 cards
│   └── ...
│
├── webapp/static/
│   ├── css/
│   │   └── subdashboard-detail.css   # CSS cho sub-dashboard và alarm visual
│   └── js/
│       ├── subdashboard-detail.js    # Logic JS chính: realtime, alarm visual
│       ├── card_conditions.js        # Modal cài đặt điều kiện alarm
│       ├── alarm-events.js           # Trang lịch sử alarm events
│       ├── app.js                    # JS chung toàn app
│       └── ...
│
├── workers/                      # Background worker processes
│   ├── tcp_worker.py             # Đọc Modbus TCP theo chu kỳ
│   ├── rtu_worker.py             # Đọc Modbus RTU theo chu kỳ
│   ├── alarm_worker.py           # Đánh giá alarm conditions
│   ├── alarm_strategies.py       # Strategy pattern cho từng loại card
│   └── logger_worker.py          # Ghi log dữ liệu tag
│
├── shared/                       # Module dùng chung giữa webapp và workers
│   ├── config.py                 # Dataclass DeviceConfig, TagConfig
│   ├── modbus_client.py          # Modbus TCP client wrapper
│   ├── database_manager.py       # Quản lý kết nối DB
│   ├── alarm_engine.py           # Core logic đánh giá alarm rule
│   ├── notification_manager.py   # Gửi Email / SMS
│   ├── app_logger.py             # Centralized logging
│   ├── data_collector.py         # Thu thập dữ liệu từ Modbus
│   └── file_logger.py            # Ghi dữ liệu ra file
│
├── config/
│   └── SMTP_config.json          # Cấu hình SMTP email
│
├── logs/                         # Log files runtime
├── output/                       # Scripts cài đặt startup task
├── release/                      # Bản release (scripts + config)
│
├── start_webapp.bat              # Khởi động chỉ webapp
├── start_all_services.bat        # Khởi động toàn bộ services
├── stop_all_services.bat         # Dừng tất cả services
├── setup_venv.bat                # Tạo .venv và cài requirements
└── requirements.txt
```

---

## Kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────────┐
│                     BROWSER (Client)                    │
│  HTML/CSS/JS — Socket.IO client — fetch API             │
└────────────────────┬────────────────────────────────────┘
                     │ HTTP / WebSocket
┌────────────────────▼────────────────────────────────────┐
│              Flask Webserver (webapp/)                  │
│  - Blueprint: auth, dashboard, subdashboards, alarms,  │
│    devices, reports, datalogger                         │
│  - Socket.IO server (eventlet)                         │
│  - Jinja2 template rendering                           │
└────────┬───────────────────────────────┬────────────────┘
         │ SQLAlchemy (MySQL)            │ HTTP API
         │                              │ (localhost)
┌────────▼────────┐          ┌──────────▼──────────────┐
│   MySQL DB      │          │   Worker Processes      │
│  - tags         │◄─────────│  - tcp_worker           │
│  - alarm_events │          │  - rtu_worker           │
│  - card_alarm   │          │  - alarm_worker         │
│    _states      │          │  - logger_worker        │
│  - log tables   │          └─────────────────────────┘
└─────────────────┘                    │
                                       │ pymodbus
                            ┌──────────▼──────────────┐
                            │  Modbus Devices          │
                            │  (TCP port 502 / RTU)    │
                            └─────────────────────────┘
```

**Luồng dữ liệu:**
1. `tcp_worker` / `rtu_worker` đọc register từ thiết bị → ghi vào DB + emit `tag_update` qua Socket.IO
2. `alarm_worker` nhận `tag_update` → đánh giá condition → ghi `alarm_events` + `card_alarm_states` → emit `alarm_event` / `card_alarm_event`
3. `logger_worker` nhận `tag_update` → ghi log theo cấu hình
4. Browser nhận Socket.IO event → cập nhật giá trị và màu alarm trực tiếp trên UI

---

## Thành phần UI — Card Types

Sub-Dashboard hiển thị giá trị tag theo dạng **card**. Mỗi card có thể chứa từ 1 đến 6 tag.  
Hai khái niệm cốt lõi:

- **PV** (Process Variable): Giá trị đo thực tế từ thiết bị (đọc từ Modbus register)
- **SV** (Set Value / Setpoint): Ngưỡng tham chiếu — có thể là tag khác hoặc giá trị cố định

---

### PV ONLY

> 1 tag — chỉ hiển thị giá trị PV

```
┌─────────────────────┐
│ ● Title    PV ONLY  │
│─────────────────────│
│   Tên tag           │
│   123.4  °C         │
└─────────────────────┘
```

- **Tags:** `pv_tag_id` (1 tag)
- **Dùng khi:** Cần theo dõi đơn giản 1 điểm đo, không cần setpoint

---

### SINGLE3

> 3 tags — PV + SV HIGH + SV LOW

```
┌─────────────────────┐
│ ● Title   SINGLE3   │
│─────────────────────│
│   Tên tag           │
│   123.4  °C         │
│   SP↑ 150.0  °C     │
│   SP↓  80.0  °C     │
└─────────────────────┘
```

- **Tags:** `pv_tag_id`, `sv_high_tag_id` (hoặc fixed), `sv_low_tag_id` (hoặc fixed)
- **Dùng khi:** 1 điểm đo cần hiển thị cả ngưỡng trên và dưới

---

### QTAG2

> 2 tags — 1 cột PV + SV (style đơn giản, không có header card riêng)

```
┌─────────────────────┐
│ ● Title    QTAG2    │
│─────────────────────│
│ Group               │
│   PV: 123.4  °C     │
│   SV:  80.0  °C     │
└─────────────────────┘
```

- **Tags:** `tag1_id` (PV), `tag2_id` (SV — optional, hoặc fixed value)
- **Dùng khi:** Hiển thị 1 điểm đo + 1 setpoint duy nhất

---

### QTAG3

> 3 tags — 1 cột PV + SV HIGH/LOW (có card header riêng, hỗ trợ màu sub-header)

```
┌─────────────────────┐
│ ● Title    QTAG3    │
│─────────────────────│
│ ┌───────────────┐   │
│ │ Column Title  │   │
│ │ PV: 123.4 °C  │   │
│ │ SV↑: 150.0 °C │   │
│ │ SV↓:  80.0 °C │   │
│ └───────────────┘   │
└─────────────────────┘
```

- **Tags:** `tag1_id` (PV), `tag2_id` (SV HIGH), `tag3_id` (SV LOW)
- **Dùng khi:** Giống SINGLE3 nhưng cần card header độc lập và tùy chỉnh màu

---

### QTAG4

> 4 tags — 2 cột, mỗi cột PV + SV

```
┌──────────────────────────────────────┐
│ ● Title                    QTAG4    │
│──────────────────────────────────────│
│ ┌─── Left ───┐  ┌─── Right ───┐     │
│ │ PV: 120 °C │  │ PV: 135 °C  │     │
│ │ SV:  80 °C │  │ SV:  90 °C  │     │
│ └────────────┘  └─────────────┘     │
└──────────────────────────────────────┘
```

- **Tags:** `tag1_id` (PV trái), `tag2_id` (PV phải), `tag3_id` (SV trái), `tag4_id` (SV phải)
- SV có thể là tag hoặc fixed value
- **Dùng khi:** So sánh 2 điểm đo có liên quan (ví dụ: đầu vào / đầu ra)
- **Alarm:** Độc lập per-column (cột trái và cột phải alarm riêng biệt)

---

### QTAG6

> 6 tags — 2 cột, mỗi cột PV + SV HIGH + SV LOW

```
┌──────────────────────────────────────────────┐
│ ● Title                          QTAG6       │
│──────────────────────────────────────────────│
│ ┌──────── Left ──────┐  ┌──────── Right ────┐│
│ │ PV:  123.4  °C     │  │ PV:  135.2  °C   ││
│ │ SP↑: 150.0  °C     │  │ SP↑: 160.0  °C   ││
│ │ SP↓:  80.0  °C     │  │ SP↓:  90.0  °C   ││
│ └────────────────────┘  └──────────────────┘│
└──────────────────────────────────────────────┘
```

- **Tags:**
  - `tag1_id` — PV trái
  - `tag2_id` — PV phải
  - `tag3_id` — SV HIGH trái (optional, có thể fixed)
  - `tag4_id` — SV HIGH phải (optional, có thể fixed)
  - `tag5_id` — SV LOW trái (optional, có thể fixed)
  - `tag6_id` — SV LOW phải (optional, có thể fixed)
- **Dùng khi:** Giám sát 2 điểm đo đầy đủ với ngưỡng HIGH/LOW mỗi bên
- **Alarm:** Độc lập per-column — cột trái alarm không ảnh hưởng cột phải

---

### Quad Tag Card

> 4 tags — lưới 2×2: PV trái/phải (hàng trên) + SV trái/phải (hàng dưới)

```
┌──────────────────────────────────────┐
│ ● Title                  QUAD       │
│──────────────────────────────────────│
│ ┌─── Left ────┐  ┌─── Right ────┐   │
│ │ TagA  120°C │  │ TagB  135°C  │   │
│ └─────────────┘  └──────────────┘   │
│ ┌─── SV Left ─┐  ┌── SV Right ──┐  │
│ │ TagC   80°C │  │ TagD   90°C  │  │
│ └─────────────┘  └──────────────┘   │
└──────────────────────────────────────┘
```

- **Tags:** `tag1_id` (PV trái), `tag2_id` (PV phải), `tag3_id` (SV trái), `tag4_id` (SV phải)
- SV trái/phải có thể là tag hoặc **fixed value** (giá trị cố định nhập tay)
- Layout dùng CSS grid `.quad-tag-grid` với 4 ô `.quad-tag-item`
- Hỗ trợ **Compare Conditions**: so sánh PV với SV theo điều kiện do admin cài đặt
- Hỗ trợ **Write value**: admin có thể ghi giá trị vào PV/SV tag qua form (nếu function code cho phép)
- **Loại card cũ** — các card kiểu mới (QTAG2–QTAG6) là bản nâng cấp với UI chi tiết hơn

---

### PV DUAL

> 2 tags — 2 cột, mỗi cột chỉ có 1 PV (không có SV)

```
┌──────────────────────────────────────┐
│ ● Title                  PV DUAL    │
│──────────────────────────────────────│
│ ┌─── Left ────┐  ┌─── Right ────┐   │
│ │  123.4  °C  │  │  135.2  °C   │   │
│ └─────────────┘  └──────────────┘   │
└──────────────────────────────────────┘
```

- **Tags:** `left_tag_id`, `right_tag_id`
- **Dùng khi:** So sánh nhanh 2 giá trị, không cần setpoint

---

### Bảng tóm tắt Card Types

| Card Type | Số Tag | Cột | PV | SV | Alarm per-column |
|---|---|---|---|---|---|
| **PV ONLY** | 1 | 1 | ✓ | — | — |
| **SINGLE3** | 3 | 1 | ✓ | HIGH + LOW | — |
| **QTAG2** | 2 | 1 | ✓ | 1 SV | — |
| **QTAG3** | 3 | 1 | ✓ | HIGH + LOW | — |
| **QTAG4** | 4 | 2 | ✓ | 1 SV/cột | ✓ |
| **QTAG6** | 6 | 2 | ✓ | HIGH + LOW/cột | ✓ |
| **PV DUAL** | 2 | 2 | ✓ | — | ✓ |
| **Quad Tag** | 4 | 2×2 grid | ✓ | 1 SV/cột | Compare Conditions |

---

## Hệ thống Alarm

### Luồng hoạt động

```
alarm_worker
    │
    ├── Nhận tag_update (giá trị PV mới)
    │
    ├── _evaluate_card_column()
    │       So sánh PV với threshold (hoặc SV tag)
    │       Trả về True/False cho từng cột
    │
    ├── _process_card_alarm_state()
    │       State machine:
    │       ┌─ current=True,  prev=False → INCOMING alarm
    │       │    → ghi alarm_events (INCOMING)
    │       │    → ghi card_alarm_states
    │       │    → emit card_alarm_event (Socket.IO)
    │       │    → gửi Email/SMS
    │       └─ current=False, prev=True  → OUTGOING alarm
    │            → ghi alarm_events (OUTGOING)
    │            → xóa card_alarm_states
    │            → emit card_alarm_event cleared
    │
    └── alarm_strategies.py
            AlarmStrategyFactory → chiến lược riêng cho từng card type
```

### Hai hệ thống alarm song song

**1. Card Alarm System (chính xác, per-column)**
- Nguồn: bảng `card_alarm_states` trong DB
- Event: `card_alarm_event` → `applyCardAlarmVisual(cardType, cardId, column, alarmType)`
- Tô màu trực tiếp sub-card theo column (left/right)
- **Đây là nguồn chân thực duy nhất cho trạng thái alarm hiện tại**

**2. Tag Alarm System (bổ sung, per-tag)**
- Nguồn: bảng `alarm_events` (lấy event cuối cùng của mỗi tag)
- Event: `alarm_event` → `applyTagAlarmVisual(tagId, alarmClass)`
- Dùng để tô màu tag item `.qtag6-tag-item` và sync định kỳ 10s
- Cross-reference với `card_alarm_states` để tránh stale data

### Màu alarm trên UI

| Alarm Type | Màu sub-card | Animation |
|---|---|---|
| **High** | Đỏ nhạt (`#fee2e2`) | `pulse-red` 2s loop |
| **Low** | Vàng nhạt (`#fef3c7`) | `pulse-yellow` 2s loop |
| **Normal** | Trắng `#ffffff` | Không |

### Cài đặt điều kiện alarm (Card Conditions)

Admin click **Set Alarm Conditions** → modal `card_conditions_modal.html`:
- Chọn **operator**: `>`, `>=`, `<`, `<=`
- Nhập **threshold value** (số cố định) hoặc chọn **compare tag** (so sánh với tag khác)
- Bật/tắt alarm monitoring per-card qua toggle switch

---

## Cách chạy dự án

### Yêu cầu

- Python 3.9+
- MySQL Server
- File `.env` hoặc `web_config.txt` chứa database URL

### Setup lần đầu

```bat
setup_venv.bat
```

Tạo `.venv` và cài tất cả packages từ `requirements.txt`.

### Khởi động

```bat
REM Chỉ webapp (không chạy workers)
start_webapp.bat

REM Toàn bộ services (webapp + alarm + logger + modbus workers)
start_all_services.bat
```

Webapp mặc định chạy tại: `http://localhost:5000`

### Dừng

```bat
stop_all_services.bat
```

### Chạy thủ công

```bat
.venv\Scripts\python.exe modbus_monitor_webserver.py
```

---

## Tech Stack

| Thành phần | Công nghệ |
|---|---|
| Web framework | Flask 3.0 + Blueprint |
| Real-time | Flask-SocketIO 5.5 + eventlet |
| Template | Jinja2 |
| Database ORM | SQLAlchemy 2.0 |
| Database | MySQL (PyMySQL connector) |
| Modbus | pymodbus 3.6 |
| Frontend | Bootstrap 5 + Bootstrap Icons |
| Build (EXE) | PyInstaller 6 |
| Notifications | SMTP Email + pyserial (SMS via GSM modem) |
