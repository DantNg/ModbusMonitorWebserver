"""
RTU Modbus Worker - Handles RTU devices on a COM port
"""
import time
import logging
import struct
from multiprocessing import Process, Queue
from pymodbus.client import ModbusSerialClient
from pymodbus.exceptions import ModbusIOException, ConnectionException
from typing import Dict, List, Optional, Set
from datetime import datetime

class ModbusRTUWorker:
    """RTU Modbus Worker Process"""
    
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
        
        # Device timing management
        self.device_last_poll = {}  # device_id -> last_poll_time
        self.device_intervals = {}  # device_id -> interval_seconds
        
        # Error handling per device
        self.device_errors = {}  # device_id -> consecutive_errors
        self.device_offline_until = {}  # device_id -> timestamp_when_to_retry
        self.max_consecutive_errors = 3
        self.offline_timeout = 15.0  # seconds
        
        # Setup logging
        self.logger = logging.getLogger(f"RTU-{config.worker_id}")
        
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
        """Establish RTU connection"""
        try:
            if self.client:
                self.client.close()
                
            self.client = ModbusSerialClient(
                port=self.config.serial_port,
                baudrate=self.config.baudrate,
                bytesize=8,
                parity='N',
                stopbits=1,
                timeout=1.0
            )
            
            if self.client.connect():
                self.connected = True
                self.log("INFO", f"Connected to {self.config.serial_port} at {self.config.baudrate} baud")
                return True
            else:
                self.connected = False
                self.log("ERROR", f"Failed to connect to {self.config.serial_port}")
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
            if "serial_port" in new_config_data:
                self.config.serial_port = new_config_data["serial_port"]
            if "baudrate" in new_config_data:
                self.config.baudrate = new_config_data["baudrate"]
            if "polling_interval" in new_config_data:
                self.config.polling_interval = new_config_data["polling_interval"]
            if "byte_order" in new_config_data:
                self.config.byte_order = new_config_data["byte_order"]
            if "word_order" in new_config_data:
                self.config.word_order = new_config_data["word_order"]
            if "devices" in new_config_data:
                self.config.devices = new_config_data["devices"]
                # Update device intervals
                for device in self.config.devices:
                    device_id = device.get('id')
                    interval = device.get('polling_interval', self.config.polling_interval)
                    self.device_intervals[device_id] = interval
            if "tags" in new_config_data:
                self.config.tags = new_config_data["tags"]
                
            self.log("INFO", "Configuration updated")
            
            # Reconnect if connection settings changed
            if any(k in new_config_data for k in ["serial_port", "baudrate"]):
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
            
    def is_device_ready_to_poll(self, device_id: int, current_time: float) -> bool:
        """Check if device is ready to be polled based on individual interval"""
        # Check if device is in offline timeout
        if device_id in self.device_offline_until:
            if current_time < self.device_offline_until[device_id]:
                return False
            else:
                # Remove from offline list
                del self.device_offline_until[device_id]
                self.device_errors[device_id] = 0
                self.log("INFO", f"Device {device_id} timeout expired, retrying")
                
        # Check individual device interval
        interval = self.device_intervals.get(device_id, self.config.polling_interval)
        last_poll = self.device_last_poll.get(device_id, 0)
        
        return (current_time - last_poll) >= interval
        
    def read_device_data(self):
        """Read data from devices that are ready"""
        if not self.connected or not self.config.devices:
            return
            
        current_time = time.time()
        
        for device in self.config.devices:
            try:
                device_id = device.get('id')
                unit_id = device.get('unit_id', 1)
                
                # Check if this device is ready to poll
                if not self.is_device_ready_to_poll(device_id, current_time):
                    continue
                    
                # Update last poll time
                self.device_last_poll[device_id] = current_time
                
                # Get device tags
                device_tags = [tag for tag in self.config.tags if tag.get('device_id') == device_id]
                
                if not device_tags:
                    continue
                    
                # Group tags by function code for efficient reading
                fc_groups = self.group_tags_by_function_code(device_tags)
                
                device_success = False
                for fc, groups in fc_groups.items():
                    for group in groups:
                        if self.read_tag_group(device, group, fc):
                            device_success = True
                            
                # Update device error tracking
                if device_success:
                    self.device_errors[device_id] = 0
                else:
                    # Increment error count
                    self.device_errors[device_id] = self.device_errors.get(device_id, 0) + 1
                    
                    # Check if device should go offline
                    if self.device_errors[device_id] >= self.max_consecutive_errors:
                        self.device_offline_until[device_id] = current_time + self.offline_timeout
                        self.log("WARNING", f"Device {device_id} going offline for {self.offline_timeout}s after {self.max_consecutive_errors} errors")
                        
            except Exception as e:
                self.log("ERROR", f"Error processing device {device.get('id', 'unknown')}: {e}")
                device_id = device.get('id')
                self.device_errors[device_id] = self.device_errors.get(device_id, 0) + 1
                
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
        
    def read_tag_group(self, device, tag_group, function_code) -> bool:
        """Read a group of tags with same function code. Returns True on success."""
        try:
            unit_id = device.get('unit_id', 1)
            device_id = device.get('id')
            
            # Calculate address range
            addresses = [tag.get('address', 0) for tag in tag_group]
            start_addr = min(addresses)
            end_addr = max(addresses)
            count = end_addr - start_addr + 1
            
            # Log read operation
            timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"{timestamp} [RTU_READ] Port {self.config.serial_port} Device Unit {unit_id} - FC{function_code:02d} addr={start_addr} count={count} (tags={len(tag_group)})")
            
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
                        "device_id": device_id,
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
                        
                self.seq += 1
                
                # Small delay between device reads on same serial port
                time.sleep(0.05)  # 50ms delay
                
                return True
                
            else:
                error_msg = f"Read error: {result}"
                self.log("ERROR", f"Device {device_id}: {error_msg}")
                
                # Send error payload
                if self.emit_enabled:
                    error_payload = {
                        "worker_id": self.config.worker_id,
                        "device_id": device_id,
                        "error": error_msg,
                        "timestamp": time.time(),
                        "seq": self.seq,
                        "rtt_ms": round(rtt_ms, 3),
                        "ok": False
                    }
                    self.data_queue.put(error_payload)
                    
                return False
                
        except Exception as e:
            self.log("ERROR", f"Error reading tag group for device {device.get('id')}: {e}")
            return False
            
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
        self.log("INFO", f"RTU Worker {self.config.worker_id} started")
        
        # Initialize device intervals
        for device in self.config.devices or []:
            device_id = device.get('id')
            interval = device.get('polling_interval', self.config.polling_interval)
            self.device_intervals[device_id] = interval
            self.device_errors[device_id] = 0
        
        # Initial connection
        self.connect()
        
        while self.running:
            try:
                # Handle commands from main process
                self.handle_commands()
                
                # Ensure connection
                if not self.connected:
                    self.connect()
                    
                # Read device data (with individual timing)
                if self.connected:
                    self.read_device_data()
                    
                # Small sleep to prevent CPU spinning
                time.sleep(0.01)
                
            except Exception as e:
                self.log("ERROR", f"Error in main loop: {e}")
                time.sleep(1.0)
                
        # Cleanup
        self.disconnect()
        self.log("INFO", f"RTU Worker {self.config.worker_id} stopped")

def modbus_rtu_worker(config, data_queue, command_queue, log_queue, shared_state):
    """Entry point for RTU worker process"""
    worker = ModbusRTUWorker(config, data_queue, command_queue, log_queue, shared_state)
    worker.run()