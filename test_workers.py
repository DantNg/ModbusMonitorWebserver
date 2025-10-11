#!/usr/bin/env python3
import sys
import os

# Add webapp to path
WEBAPP_DIR = os.path.join('.', 'webapp')
sys.path.insert(0, WEBAPP_DIR)

from modbus_monitor.database import db

# Test the new get_auto_start_workers function
workers = db.get_auto_start_workers()
print(f'Found {len(workers)} auto-generated workers:')

for w in workers:
    print(f'  - {w["worker_id"]}: {w["worker_type"]} ({len(w["devices"])} devices, {len(w["tags"])} tags)')
    if w['worker_type'] == 'tcp':
        print(f'    TCP: {w["host"]}:{w["port"]}')
    elif w['worker_type'] == 'rtu':
        print(f'    RTU: {w["serial_port"]}@{w["baudrate"]}')
    
    # Show devices in this worker
    for device in w["devices"]:
        print(f'      Device: {device["name"]} (ID: {device["id"]})')