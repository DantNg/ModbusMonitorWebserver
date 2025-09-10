import smtplib
import serial  # Required for SMS/COM functionality
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
# Load environment variables
with open("config/SMTP_config.json") as config_file:
    config = json.load(config_file)

# SMTP Service
def send_email(to_email, subject, body):
    try:
        print(f"Sending email to {to_email}")
        smtp_server = config["SMTPSettings"]["Host"]
        smtp_port = int(config["SMTPSettings"]["Port"])
        smtp_username = config["SMTPSettings"]["Username"]
        smtp_password = config["SMTPSettings"]["Password"]
        # print(smtp_server,smtp_port,smtp_username,smtp_password)
        # # Create email
        msg = MIMEMultipart()
        msg["From"] = smtp_username
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))
        # print(msg)
        # Connect to SMTP server
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False

# SMS Service via COM Port
def send_sms(phone_number, message):
    """
    Send SMS via COM port (GSM modem/AT commands)
    Note: Serial port access may trigger antivirus warnings - this is normal for industrial applications
    """
    try:
        print(f"Sending SMS to {phone_number}")
        com_port = config["SMSSettings"]["COMPort"]
        baud_rate = int(config["SMSSettings"]["BaudRate"])
        parity = config["SMSSettings"].get("Parity", "N")
        stopbits = int(config["SMSSettings"].get("StopBits", 1))
        bytesize = int(config["SMSSettings"].get("DataBits", 8))
        timeout = int(config["SMSSettings"].get("Timeout", 1))
        
        # Connect to COM port for GSM modem
        with serial.Serial(
            port=com_port,
            baudrate=baud_rate,
            parity=parity,
            stopbits=stopbits,
            bytesize=bytesize,
            timeout=timeout
        ) as ser:
            # Standard GSM AT commands for SMS
            ser.write(b'AT\r')  # Test modem connection
            ser.write(b'AT+CMGF=1\r')  # Set SMS text mode
            ser.write(f'AT+CMGS="{phone_number}"\r'.encode())
            ser.write(message.encode() + b"\x1A")  # Send message + Ctrl+Z terminator
        return True
    except Exception as e:
        print(f"Failed to send SMS via COM: {e}")
        return False