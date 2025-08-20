import smtplib
import serial
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import dotenv_values

# Load environment variables
config = dotenv_values(".env")

# SMTP Service
def send_email(to_email, subject, body):
    try:
        smtp_server = config["SMTP_SERVER"]
        smtp_port = int(config["SMTP_PORT"])
        smtp_username = config["SMTP_USERNAME"]
        smtp_password = config["SMTP_PASSWORD"]

        # Create email
        msg = MIMEMultipart()
        msg["From"] = smtp_username
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        # Connect to SMTP server
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False

# SMS Service
def send_sms(phone_number, message):
    try:
        com_port = config["SMS_COM_PORT"]
        baud_rate = int(config["SMS_BAUD_RATE"])
        parity = config.get("SMS_PARITY", "N")
        stopbits = int(config.get("SMS_STOPBITS", 1))
        bytesize = int(config.get("SMS_BYTESIZE", 8))
        timeout = int(config.get("SMS_TIMEOUT", 1))
        # Connect to COM port
        with serial.Serial(
            port=com_port,
            baudrate=baud_rate,
            parity=parity,
            stopbits=stopbits,
            bytesize=bytesize,
            timeout=timeout
        ) as ser:
            ser.write(b'AT\r')  # Test AT command
            ser.write(b'AT+CMGF=1\r')  # Set SMS mode to text
            ser.write(f'AT+CMGS="{phone_number}"\r'.encode())
            ser.write(message.encode() + b"\x1A")  # Send message and Ctrl+Z
        return True
    except Exception as e:
        print(f"Failed to send SMS: {e}")
        return False