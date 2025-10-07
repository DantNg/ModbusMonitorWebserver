import time
import socket
import json
import logging
import struct
from multiprocessing import Process, Queue
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusIOException, ConnectionException
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from datetime import datetime

class ModbusTCPWorker:
    """TCP Modbus Worker Process"""
    
    def __init__(self, config, data_queue, command_queue, log_queue, shared_state):
        self.config = config
        self.data_queue = data_queue
        self.command_queue = command_queue
        self.log_queue = log_queue
        self.shared_state = shared_state
        
        # Connection
        self.client = None
        self.connected = False
        
        # Runtime state
        self.running = False
        self.seq = 0
        self.active_rooms: Set[str] = set()
        self.room_tag_mapping: Dict[str, List[int]] = {}  # room_id -> tag_ids
        self.emit_enabled = False
        
        # Error handling
        self.consecutive_errors = 0
        self.max_consecutive_errors = 3
        self.offline_timeout = 15.0  # seconds
        self.last_error_time = 0
        
        # Timing
        self.last_poll_time = 0
        
        # Setup logging
        self.logger = logging.getLogger(f"TCP-{config.worker_id}")
        
    def log(self, level, message):
        """Send log message to main process"""
        try:
            log_data = {
                "worker_id": self.config.worker_id,
                "level": level,
                "message": message,
                "timestamp": time.time()
            }
            self.log_queue.put(log_data)
        except:
            pass
            
    def connect(self) -> bool:
        """Establish TCP connection"""
        try:
            if self.client:
                self.client.close()
                
            self.client = ModbusTcpClient(
                host=self.config.host,
                port=self.config.port,
                timeout=1.0,
                retries=0
            )
            
            if self.client.connect():
                self.connected = True
                self.consecutive_errors = 0
                self.log("INFO", f"Connected to {self.config.host}:{self.config.port}")
                return True
            else:
                self.connected = False
                self.log("ERROR", f"Failed to connect to {self.config.host}:{self.config.port}")
                return False
                
        except Exception as e:
            self.connected = False
            self.log("ERROR", f"Connection error: {e}")
            return False
            
    def disconnect(self):
        """Close connection"""
        if self.client:
            try:
                self.client.close()
            except:
                pass
        self.client = None
        self.connected = False
        
    def handle_commands(self):
        """Process commands from main process"""
        while not self.command_queue.empty():
            try:
                cmd_data = self.command_queue.get(block=False)
                command = cmd_data.get("command")
                data = cmd_data.get("data", {})
                
                if command == "stop":
                    self.running = False
                    self.log("INFO", "Received stop command")
                    
                elif command == "update_config":
                    self.update_config(data)
                    
                elif command == "join_room":
                    room_id = data.get("room_id")
                    tag_ids = data.get("tag_ids", [])
                    self.join_room(room_id, tag_ids)
                    
                elif command == "leave_room":
                    room_id = data.get("room_id")
                    self.leave_room(room_id)
                    
                elif command == "enable_emit":
                    self.emit_enabled = True
                    self.log("INFO", "Emit enabled")
                    
                elif command == "disable_emit":
                    self.emit_enabled = False
                    self.log("INFO", "Emit disabled")
                    
            except Exception as e:
                self.log("ERROR", f"Error handling command: {e}")
                
    def update_config(self, new_config_data):
        """Update worker configuration"""
        try:
            # Update connection settings
            if "host" in new_config_data:
                self.config.host = new_config_data["host"]
            if "port" in new_config_data:
                self.config.port = new_config_data["port"]
            if "polling_interval" in new_config_data:
                self.config.polling_interval = new_config_data["polling_interval"]
            if "byte_order" in new_config_data:
                self.config.byte_order = new_config_data["byte_order"]
            if "word_order" in new_config_data:
                self.config.word_order = new_config_data["word_order"]
            if "devices" in new_config_data:
                self.config.devices = new_config_data["devices"]
            if "tags" in new_config_data:
                self.config.tags = new_config_data["tags"]
                
            self.log("INFO", "Configuration updated")
            
            # Reconnect if connection settings changed
            if any(k in new_config_data for k in ["host", "port"]):
                self.disconnect()
                self.connect()
                
        except Exception as e:
            self.log("ERROR", f"Failed to update config: {e}")
            
    def join_room(self, room_id: str, tag_ids: List[int]):
        """Handle user joining room"""
        self.active_rooms.add(room_id)
        self.room_tag_mapping[room_id] = tag_ids
        self.emit_enabled = len(self.active_rooms) > 0
        self.log("INFO", f"Joined room {room_id} with tags {tag_ids}")
        
    def leave_room(self, room_id: str):
        """Handle user leaving room"""
        self.active_rooms.discard(room_id)
        self.room_tag_mapping.pop(room_id, None)
        self.emit_enabled = len(self.active_rooms) > 0
        self.log("INFO", f"Left room {room_id}")
        
    def parse_value(self, raw_value, datatype: str, byte_order: str, word_order: str):
        """Parse raw Modbus value according to datatype and byte/word order"""
        try:
            if not raw_value:
                return None
                
            if datatype.lower() in ['float', 'float32', 'real']:
                if len(raw_value) >= 2:
                    # IEEE 754 float from 2 registers
                    w1, w2 = (raw_value[1], raw_value[0]) if word_order == "BA" else (raw_value[0], raw_value[1])
                    b1 = w1.to_bytes(2, "big")
                    b2 = w2.to_bytes(2, "big")
                    b = b1 + b2
                    if byte_order == "LittleEndian":
                        b = b[1:2] + b[0:1] + b[3:4] + b[2:3]
                    return struct.unpack(">f", b)[0]
                    
            elif datatype.lower() in ['double', 'float64']:
                if len(raw_value) >= 4:
                    # IEEE 754 double from 4 registers
                    if word_order == "BA":
                        regs = [raw_value[3], raw_value[2], raw_value[1], raw_value[0]]
                    else:
                        regs = raw_value[:4]
                    
                    bytes_data = b''
                    for reg in regs:
                        bytes_data += reg.to_bytes(2, "big")
                    
                    if byte_order == "LittleEndian":
                        # Swap bytes within each word
                        new_bytes = b''
                        for i in range(0, len(bytes_data), 2):
                            new_bytes += bytes_data[i+1:i+2] + bytes_data[i:i+1]
                        bytes_data = new_bytes
                    
                    return struct.unpack(">d", bytes_data)[0]
                    
            elif datatype.lower() in ['int16', 'short']:
                return raw_value[0] if raw_value else 0
                
            elif datatype.lower() in ['uint16', 'word']:
                return raw_value[0] if raw_value else 0
                
            elif datatype.lower() in ['int32', 'long']:
                if len(raw_value) >= 2:
                    if word_order == "BA":
                        return (raw_value[1] << 16) | raw_value[0]
                    else:
                        return (raw_value[0] << 16) | raw_value[1]
                        
            elif datatype.lower() in ['uint32', 'dword']:
                if len(raw_value) >= 2:
                    if word_order == "BA":
                        return (raw_value[1] << 16) | raw_value[0]
                    else:
                        return (raw_value[0] << 16) | raw_value[1]
                        
            return raw_value[0] if raw_value else 0
            
        except Exception as e:
            self.log("ERROR", f"Error parsing value {raw_value} as {datatype}: {e}")
            return None
            
    def read_device_data(self):
        """Read data from all configured devices"""
        if not self.connected or not self.config.devices:
            return
            
        current_time = time.time()
        
        for device in self.config.devices:
            try:
                device_id = device.get('id')
                unit_id = device.get('unit_id', 1)
                
                # Check if device is in offline timeout
                if self.consecutive_errors >= self.max_consecutive_errors:
                    if current_time - self.last_error_time < self.offline_timeout:
                        continue  # Skip this device during timeout
                    else:
                        # Reset error count after timeout
                        self.consecutive_errors = 0
                        self.log("INFO", f"Retrying device {device_id} after offline timeout")
                
                # Get device tags
                device_tags = [tag for tag in self.config.tags if tag.get('device_id') == device_id]
                
                if not device_tags:
                    continue
                    
                # Group tags by function code and address range for efficient reading
                fc_groups = self.group_tags_by_function_code(device_tags)
                
                for fc, groups in fc_groups.items():
                    for group in groups:
                        self.read_tag_group(device, group, fc)
                        
            except Exception as e:
                self.log("ERROR", f"Error reading device {device.get('id', 'unknown')}: {e}")
                self.consecutive_errors += 1
                self.last_error_time = current_time
                
    def group_tags_by_function_code(self, tags) -> Dict[int, List[List[dict]]]:
        """Group tags by function code and create address ranges"""
        fc_groups = {}
        
        # Group by function code first
        by_fc = {}
        for tag in tags:
            fc = tag.get('function_code', 3)
            if fc not in by_fc:
                by_fc[fc] = []
            by_fc[fc].append(tag)
            
        # Create address groups for each FC
        for fc, fc_tags in by_fc.items():
            # Sort by address
            fc_tags.sort(key=lambda t: t.get('address', 0))
            
            groups = []
            current_group = []
            last_addr = -1
            
            for tag in fc_tags:
                addr = tag.get('address', 0)
                
                # Start new group if address gap > 10 or group too large
                if (addr - last_addr > 10 or len(current_group) >= 20) and current_group:
                    groups.append(current_group)
                    current_group = []
                    
                current_group.append(tag)
                last_addr = addr
                
            if current_group:
                groups.append(current_group)
                
            fc_groups[fc] = groups
            
        return fc_groups
        
    def read_tag_group(self, device, tag_group, function_code):
        """Read a group of tags with same function code"""
        try:
            unit_id = device.get('unit_id', 1)
            
            # Calculate address range
            addresses = [tag.get('address', 0) for tag in tag_group]
            start_addr = min(addresses)
            end_addr = max(addresses)
            count = end_addr - start_addr + 1
            
            # Log read operation
            timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"{timestamp} [MODBUS_READ] TCP Device {device.get('id')} (Unit {unit_id}) - FC{function_code:02d} addr={start_addr} count={count}")
            
            # Perform read
            t0 = time.monotonic()
            result = None
            
            if function_code == 1:  # Read Coils
                result = self.client.read_coils(start_addr, count, slave=unit_id)
            elif function_code == 2:  # Read Discrete Inputs
                result = self.client.read_discrete_inputs(start_addr, count, slave=unit_id)
            elif function_code == 3:  # Read Holding Registers
                result = self.client.read_holding_registers(start_addr, count, slave=unit_id)
            elif function_code == 4:  # Read Input Registers
                result = self.client.read_input_registers(start_addr, count, slave=unit_id)
                
            rtt_ms = (time.monotonic() - t0) * 1000.0
            
            if result and not result.isError():
                # Reset error count on success
                self.consecutive_errors = 0
                
                # Parse and emit data for each tag
                for tag in tag_group:
                    tag_addr = tag.get('address', 0)
                    tag_id = tag.get('id')
                    datatype = tag.get('datatype', 'int16')
                    
                    # Extract value for this tag
                    addr_offset = tag_addr - start_addr
                    
                    if function_code in [1, 2]:  # Boolean values
                        raw_value = [bool(result.bits[addr_offset])] if addr_offset < len(result.bits) else [False]
                    else:  # Register values
                        reg_count = self.get_register_count(datatype)
                        raw_value = result.registers[addr_offset:addr_offset + reg_count] if addr_offset + reg_count <= len(result.registers) else []
                    
                    # Parse value
                    parsed_value = self.parse_value(raw_value, datatype, device.get('byte_order', 'BigEndian'), device.get('word_order', 'AB'))
                    
                    # Create payload
                    payload = {
                        "worker_id": self.config.worker_id,
                        "device_id": device.get('id'),
                        "tag_id": tag_id,
                        "value": parsed_value,
                        "raw_value": raw_value,
                        "timestamp": time.time(),
                        "seq": self.seq,
                        "rtt_ms": round(rtt_ms, 3),
                        "function_code": function_code,
                        "address": tag_addr,
                        "datatype": datatype,
                        "ok": True
                    }
                    
                    # Send to main process if emit is enabled
                    if self.emit_enabled:
                        self.data_queue.put(payload)
                        
                    # Always save to database (this would be implemented in main process)
                    
                self.seq += 1
                
            else:
                error_msg = f"Read error: {result}"
                self.log("ERROR", error_msg)
                self.consecutive_errors += 1
                self.last_error_time = time.time()
                
                # Send error payload
                if self.emit_enabled:
                    error_payload = {
                        "worker_id": self.config.worker_id,
                        "device_id": device.get('id'),
                        "error": error_msg,
                        "timestamp": time.time(),
                        "seq": self.seq,
                        "rtt_ms": round(rtt_ms, 3),
                        "ok": False
                    }
                    self.data_queue.put(error_payload)
                    
        except Exception as e:
            self.log("ERROR", f"Error reading tag group: {e}")
            self.consecutive_errors += 1
            self.last_error_time = time.time()
            
    def get_register_count(self, datatype: str) -> int:
        """Get number of registers required for datatype"""
        datatype = datatype.lower()
        if datatype in ['float', 'float32', 'real', 'int32', 'uint32', 'long', 'dword']:
            return 2
        elif datatype in ['double', 'float64']:
            return 4
        else:
            return 1
            
    def run(self):
        """Main worker loop"""
        self.running = True
        self.log("INFO", f"TCP Worker {self.config.worker_id} started")
        
        # Initial connection
        self.connect()
        
        while self.running:
            try:
                # Handle commands from main process
                self.handle_commands()
                
                # Check if it's time to poll
                current_time = time.time()
                if current_time - self.last_poll_time >= self.config.polling_interval:
                    
                    # Ensure connection
                    if not self.connected:
                        self.connect()
                        
                    # Read device data
                    if self.connected:
                        self.read_device_data()
                        
                    self.last_poll_time = current_time
                    
                # Small sleep to prevent CPU spinning
                time.sleep(0.01)
                
            except Exception as e:
                self.log("ERROR", f"Error in main loop: {e}")
                time.sleep(1.0)
                
        # Cleanup
        self.disconnect()
        self.log("INFO", f"TCP Worker {self.config.worker_id} stopped")

def modbus_tcp_worker(config, data_queue, command_queue, log_queue, shared_state):
    """Entry point for TCP worker process"""
    worker = ModbusTCPWorker(config, data_queue, command_queue, log_queue, shared_state)
    worker.run()
