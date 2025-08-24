@echo off
set MYSQL_URI=mysql+pymysql://root:123456@localhost:3306/modbus_monitor_db
set SECRET_KEY=your_secret_key_here

echo Activating virtual environment...
call .venv\Scripts\activate

echo Running the Flask application...
python app.py