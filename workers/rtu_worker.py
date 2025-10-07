"""
RTU Worker - Handles Modbus RTU communication over serial port
"""

import time
import threading
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Try to import pymodbus with correct syntax
try:
    from pymodbus.client import ModbusSerialClient
    from pymodbus.exceptions import ModbusException
    import serial
    PYMODBUS_AVAILABLE = True
except ImportError:
    print("⚠️ pymodbus not available - RTU worker will use simulation mode")
    PYMODBUS_AVAILABLE = False
    ModbusSerialClient = None
    ModbusException = Exception
    serial = None

class RTUWorker:
    def __init__(self, worker_id, serial_port, baudrate=9600, timeout=5, devices=None, tags=None):
        self.worker_id = worker_id
        self.serial_port = serial_port
        self.baudrate = baudrate
        self.timeout = timeout
        self.devices = devices or []
        self.tags = tags or []
        self.is_running = False
        self.client = None
        self.worker_thread = None
        self.debug = True
        
        # Group tags by device for efficient reading
        self.device_tags = {}
        for tag in self.tags:
            if tag.device_id not in self.device_tags:
                self.device_tags[tag.device_id] = []
            self.device_tags[tag.device_id].append(tag)
    
    def start(self):
        """Start the RTU worker"""
        if self.is_running:
            print(f"⚠️  RTU Worker {self.worker_id} already running")
            return
        
        try:
            if not PYMODBUS_AVAILABLE:
                print("🛠️ Running in simulation mode (pymodbus not available)")
                self.client = None  # Use simulation mode
            else:
                # Create Modbus RTU client
                self.client = ModbusSerialClient(
                    port=self.serial_port,
                    baudrate=self.baudrate,
                    timeout=self.timeout,
                    parity='N',
                    stopbits=1,
                    bytesize=8
                )
                
                if not self.client.connect():
                    print(f"❌ Failed to connect to {self.serial_port}")
                    return False
            
            print(f"✅ Connected to RTU: {self.serial_port} @ {self.baudrate} baud")
            
            self.is_running = True
            self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self.worker_thread.start()
            
            return True
            
        except Exception as e:
            print(f"❌ RTU Worker start error: {e}")
            return False
    
    def stop(self):
        """Stop the RTU worker"""
        self.is_running = False
        if self.client:
            self.client.close()
        
        if self.worker_thread:
            self.worker_thread.join(timeout=5)
        
        print(f"🛑 RTU Worker {self.worker_id} stopped")
    
    def _worker_loop(self):
        """Main worker loop"""
        print(f"🔄 RTU Worker loop started for {len(self.devices)} devices")
        
        while self.is_running:
            try:
                for device in self.devices:
                    if not self.is_running:
                        break
                    
                    self._read_device(device)
                    
                # Wait before next polling cycle
                time.sleep(1)
                
            except Exception as e:
                print(f"❌ RTU Worker loop error: {e}")
                time.sleep(5)  # Wait before retrying
    
    def _read_device(self, device):
        """Read all tags for a device using optimized block reads"""
        try:
            device_tags = self.device_tags.get(device.id, [])
            if not device_tags:
                return
            
            if self.debug:
                print(f"📖 Reading device {device.name} (Unit {device.unit_id}) - {len(device_tags)} tags")
            
            # Group tags by function code for batch reading
            tags_by_function = {}
            for tag in device_tags:
                function_code = tag.function_code or device.default_function_code or 3
                if function_code not in tags_by_function:
                    tags_by_function[function_code] = []
                tags_by_function[function_code].append(tag)
            
            successful_reads = 0
            
            # Read each function code group as a block
            for function_code, tags in tags_by_function.items():
                try:
                    values = self._read_tags_block(device, function_code, tags)
                    if values:
                        for tag, value in zip(tags, values):
                            if value is not None:
                                successful_reads += 1
                                
                                # Apply scaling
                                if tag.scale_factor != 1.0:
                                    value = value * tag.scale_factor
                                if tag.offset != 0.0:
                                    value = value + tag.offset
                                
                                if self.debug:
                                    print(f"   📊 {tag.name}: {value}")
                                
                                # Store value
                                self._store_tag_value(tag.id, value, datetime.now())
                            else:
                                if self.debug:
                                    print(f"   ❌ {tag.name}: No data")
                                
                except Exception as block_error:
                    print(f"❌ Error reading block FC{function_code}: {block_error}")
                    # Fallback to individual tag reading
                    for tag in tags:
                        try:
                            value = self._read_tag_individual(device, tag)
                            if value is not None:
                                successful_reads += 1
                                
                                # Apply scaling
                                if tag.scale_factor != 1.0:
                                    value = value * tag.scale_factor
                                if tag.offset != 0.0:
                                    value = value + tag.offset
                                
                                if self.debug:
                                    print(f"   📊 {tag.name}: {value} (individual)")
                                
                                self._store_tag_value(tag.id, value, datetime.now())
                        except Exception as tag_error:
                            print(f"❌ Error reading tag {tag.name}: {tag_error}")
            
            if self.debug:
                print(f"✅ Device {device.name}: {successful_reads}/{len(device_tags)} tags read successfully")
                
        except Exception as e:
            print(f"❌ Device read error for {device.name}: {e}")
    
    def _read_tags_block(self, device, function_code, tags):
        """Read multiple tags in a single block read"""
        try:
            if not PYMODBUS_AVAILABLE or self.client is None:
                # Simulation mode - generate fake data for all tags
                import random
                return [random.randint(100, 500) for _ in tags]
            
            # Sort tags by address
            sorted_tags = sorted(tags, key=lambda t: t.address)
            
            # Find address range
            min_addr = min(tag.address for tag in sorted_tags)
            max_addr = max(tag.address for tag in sorted_tags)
            
            # Convert to 0-based addressing
            start_addr = max(0, min_addr - 1) if min_addr > 0 else min_addr
            count = max_addr - min_addr + 1
            
            if self.debug:
                print(f"   🔍 Block read FC{function_code}: Addr {min_addr}-{max_addr} → {start_addr}+{count}, Unit {device.unit_id}")
            
            # Read the block
            if function_code == 1:  # Read Coils
                result = self.client.read_coils(start_addr, count, unit=device.unit_id)
            elif function_code == 2:  # Read Discrete Inputs
                result = self.client.read_discrete_inputs(start_addr, count, unit=device.unit_id)
            elif function_code == 3:  # Read Holding Registers
                result = self.client.read_holding_registers(start_addr, count, unit=device.unit_id)
            elif function_code == 4:  # Read Input Registers
                result = self.client.read_input_registers(start_addr, count, unit=device.unit_id)
            else:
                print(f"⚠️  Unsupported function code: {function_code}")
                return None
            
            if result.isError():
                print(f"❌ Modbus block read error FC{function_code}: {result}")
                return None
            
            # Map values to tags
            values = []
            for tag in tags:
                try:
                    # Calculate offset in the read block
                    offset = tag.address - min_addr
                    
                    if function_code in [1, 2]:  # Coils/Discrete Inputs
                        value = 1 if result.bits[offset] else 0
                    else:  # Registers
                        value = result.registers[offset]
                    
                    values.append(value)
                except (IndexError, AttributeError) as e:
                    print(f"⚠️  Error mapping tag {tag.name} at offset {offset}: {e}")
                    values.append(None)
            
            return values
            
        except Exception as e:
            print(f"❌ Block read error: {e}")
            return None
    
    def _read_tag_individual(self, device, tag):
        """Read a single tag value (fallback method)"""
        try:
            if not PYMODBUS_AVAILABLE or self.client is None:
                # Simulation mode - generate fake data
                import random
                if tag.data_type in ['Bit']:
                    return random.choice([0, 1])
                else:
                    return random.randint(100, 500)  # Simulate sensor reading
            
            function_code = tag.function_code or device.default_function_code or 3
            
            # Convert address - some systems use 1-based addressing
            # Pymodbus uses 0-based addressing, so subtract 1 if address > 0
            modbus_address = max(0, tag.address - 1) if tag.address > 0 else tag.address
            
            if self.debug:
                print(f"   🔍 Reading {tag.name}: FC={function_code}, Addr={tag.address}→{modbus_address}, Unit={device.unit_id}")
            
            if function_code == 1:  # Read Coils
                result = self.client.read_coils(modbus_address, 1, unit=device.unit_id)
            elif function_code == 2:  # Read Discrete Inputs
                result = self.client.read_discrete_inputs(modbus_address, 1, unit=device.unit_id)
            elif function_code == 3:  # Read Holding Registers
                result = self.client.read_holding_registers(modbus_address, 1, unit=device.unit_id)
            elif function_code == 4:  # Read Input Registers
                result = self.client.read_input_registers(modbus_address, 1, unit=device.unit_id)
            else:
                print(f"⚠️  Unsupported function code: {function_code}")
                return None
            
            if result.isError():
                print(f"❌ Modbus error reading {tag.name}: {result}")
                return None
            
            # Extract value based on function code
            if function_code in [1, 2]:  # Coils/Discrete Inputs
                return 1 if result.bits[0] else 0
            else:  # Registers
                return result.registers[0]
                
        except Exception as e:
            print(f"❌ Tag read error {tag.name}: {e}")
            return None
    
    def _store_tag_value(self, tag_id, value, timestamp):
        """Store tag value (placeholder - in real implementation, save to database)"""
        # This would interface with your database or queue system
        pass