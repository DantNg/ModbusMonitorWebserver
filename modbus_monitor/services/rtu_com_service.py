"""
RTU COM Service - handles multiple devices on same COM port efficiently
"""
import threading
import time
import logging
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from queue import Queue
from pymodbus.client import ModbusSerialClient
from pymodbus.exceptions import ModbusIOException, ConnectionException

from modbus_monitor.services.config_cache import (
    ConfigCache, DeviceConfig, TagConfig, FunctionCodeGroup
)
from modbus_monitor.services.socket_emission_manager import SocketEmissionManager
from modbus_monitor.services.value_queue_service import ValueQueueService, RawModbusValue

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Enable debug logging for troubleshooting

@dataclass
class RTUComConfig:
    """Configuration for RTU COM port"""
    serial_port: str
    baudrate: int = 9600
    bytesize: int = 8
    parity: str = "N"
    stopbits: int = 1
    timeout: float = 1.0
    
    def __eq__(self, other):
        if not isinstance(other, RTUComConfig):
            return False
        return (self.serial_port == other.serial_port and
                self.baudrate == other.baudrate and
                self.bytesize == other.bytesize and
                self.parity == other.parity and
                self.stopbits == other.stopbits)
    
    def __hash__(self):
        return hash((self.serial_port, self.baudrate, self.bytesize, self.parity, self.stopbits))

class RTUComReader:
    """Reads multiple devices on same COM port"""
    
    def __init__(self, com_config: RTUComConfig, config_cache: ConfigCache, 
                 emission_manager: SocketEmissionManager, value_queue: ValueQueueService):
        self.com_config = com_config
        self.config_cache = config_cache
        self.emission_manager = emission_manager
        self.value_queue = value_queue
        
        self.client: Optional[ModbusSerialClient] = None
        self.connected = False
        self.devices: Dict[int, DeviceConfig] = {}  # unit_id -> DeviceConfig
        self.device_tags: Dict[int, List[TagConfig]] = {}  # unit_id -> tags
        
        self._last_connection_test = 0
        self._next_retry_ts = 0
        self._backoff = 1.0
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._last_config_reload = 0  # Track last config reload time
        
        logger.info(f"Created RTU COM reader for {com_config.serial_port}")

    def add_device(self, device_config: DeviceConfig):
        """Add device to this COM port"""
        with self._lock:
            unit_id = device_config.unit_id
            self.devices[unit_id] = device_config
            
            # Load tags for this device
            tags = self.config_cache.get_device_tags(device_config.id)
            self.device_tags[unit_id] = tags
            
            logger.info(f"Added device {device_config.name} (Unit ID: {unit_id}) to COM {self.com_config.serial_port} with {len(tags)} tags")

    def reload_device_configs(self):
        """Reload device configs and tags (minimal impact)"""
        with self._lock:
            for unit_id, device in self.devices.items():
                # Only reload tags, don't touch global cache
                current_tag_count = len(self.device_tags.get(unit_id, []))
                fresh_tags = self.config_cache.get_device_tags(device.id)
                
                # Only update if tag count changed (new tags added)
                if len(fresh_tags) != current_tag_count:
                    self.device_tags[unit_id] = fresh_tags
                    logger.info(f"Updated device {device.name} (Unit {unit_id}): {current_tag_count} → {len(fresh_tags)} tags")

    def force_reload_device_tags(self, device_id: int):
        """Force reload tags for specific device"""
        with self._lock:
            for unit_id, device in self.devices.items():
                if device.id == device_id:
                    # Force fresh load
                    tags = self.config_cache.get_device_tags(device.id)
                    self.device_tags[unit_id] = tags
                    logger.info(f"Force reloaded tags for device {device.name} (Unit {unit_id}): {len(tags)} tags")
                    break

    def remove_device(self, unit_id: int):
        """Remove device from this COM port"""
        with self._lock:
            if unit_id in self.devices:
                device_name = self.devices[unit_id].name
                del self.devices[unit_id]
                del self.device_tags[unit_id]
                logger.info(f"Removed device {device_name} (Unit ID: {unit_id}) from COM {self.com_config.serial_port}")

    def has_devices(self) -> bool:
        """Check if this COM reader has any devices"""
        with self._lock:
            return len(self.devices) > 0

    def _connect(self) -> bool:
        """Connect to COM port"""
        try:
            if self.client:
                self.client.close()
            
            self.client = ModbusSerialClient(
                port=self.com_config.serial_port,
                baudrate=self.com_config.baudrate,
                bytesize=self.com_config.bytesize,
                parity=self.com_config.parity,
                stopbits=self.com_config.stopbits,
                timeout=self.com_config.timeout
            )
            
            connected = self.client.connect()
            logger.info(f"RTU COM {self.com_config.serial_port}: Connection {'SUCCESS' if connected else 'FAILED'}")
            return connected
            
        except Exception as e:
            logger.error(f"RTU COM {self.com_config.serial_port}: Connection error: {e}")
            return False

    def _ensure_connected(self) -> bool:
        """Ensure connection is active"""
        now = time.time()
        
        if self.connected:
            # Test connection periodically
            if now - self._last_connection_test > 30:
                if not self._test_connection():
                    logger.warning(f"RTU COM {self.com_config.serial_port}: Connection lost, reconnecting...")
                    self.connected = False
                    self._close()
                else:
                    self._last_connection_test = now
            
            if self.connected:
                return True
        
        if now < self._next_retry_ts:
            return False
        
        logger.info(f"RTU COM {self.com_config.serial_port}: Attempting connection...")
        ok = self._connect()
        
        if ok:
            self.connected = True
            self._backoff = 1.0
            self._last_connection_test = now
            
            # Update all devices status to connected
            for unit_id, device in self.devices.items():
                self.config_cache.update_device_status(device.id, "connected")
        else:
            self.connected = False
            self._backoff = min(self._backoff * 2, 60)
            self._next_retry_ts = now + self._backoff
            
            # Update all devices status to disconnected
            for unit_id, device in self.devices.items():
                self.config_cache.update_device_status(device.id, "disconnected")
        
        return ok

    def _test_connection(self) -> bool:
        """Test connection by reading a small register"""
        if not self.client:
            return False
        
        try:
            # Try to read from first available device
            for unit_id in self.devices:
                result = self.client.read_holding_registers(0, 1, slave=unit_id)
                if not result.isError():
                    return True
            return False
        except:
            return False

    def _close(self):
        """Close connection"""
        if self.client:
            try:
                self.client.close()
            except:
                pass
            self.client = None

    def _read_device_tags(self, unit_id: int, device: DeviceConfig, tags: List[TagConfig]) -> List[RawModbusValue]:
        """Read all tags for a specific device (unit_id)"""
        if not self.client:
            return []
        
        # Get pre-calculated function code groups for this device
        fc_groups = self.config_cache.get_device_fc_groups(device.id)
        raw_values = []
        
        # Safety check: if no groups, try to read individual tags
        if not fc_groups:
            logger.warning(f"Device {device.name} (Unit {unit_id}): No function code groups found, trying individual tag reads")
            for tag in tags:
                try:
                    fc = tag.function_code or device.default_function_code
                    addr = self._normalize_address(tag.address)  # Use normalized address
                    
                    logger.debug(f"Individual tag {tag.name}: raw_address={tag.address}, normalized_address={addr}, FC={fc}")
                    
                    # Read single register/coil
                    if fc == 1:  # Read Coils
                        result = self.client.read_coils(addr, 1, slave=unit_id)
                    elif fc == 2:  # Read Discrete Inputs
                        result = self.client.read_discrete_inputs(addr, 1, slave=unit_id)
                    elif fc == 3:  # Read Holding Registers
                        result = self.client.read_holding_registers(addr, 1, slave=unit_id)
                    elif fc == 4:  # Read Input Registers
                        result = self.client.read_input_registers(addr, 1, slave=unit_id)
                    else:
                        continue
                    
                    if not result.isError():
                        if fc in [1, 2]:  # Boolean
                            raw_value = bool(result.bits[0]) if hasattr(result, 'bits') and len(result.bits) > 0 else False
                        else:  # Register
                            raw_value = result.registers[0] if hasattr(result, 'registers') and len(result.registers) > 0 else 0
                        
                        raw_values.append(RawModbusValue(
                            device_id=device.id,
                            tag_id=tag.id,
                            tag_name=tag.name,
                            function_code=fc,
                            address=tag.address,
                            raw_value=raw_value,
                            timestamp=time.time(),
                            data_type=tag.datatype,
                            scale=tag.scale,
                            offset=tag.offset,
                            unit=tag.unit
                        ))
                        logger.debug(f"Individual tag {tag.name}: Successfully read value {raw_value}")
                    else:
                        logger.warning(f"Individual tag {tag.name}: Read error: {result}")
                        
                except Exception as e:
                    logger.error(f"Individual tag {tag.name}: Read error: {e}")
            
            return raw_values
        
        for group in fc_groups:
            try:
                fc = group.function_code
                
                # Safety checks
                if not group.tags:
                    logger.warning(f"Device {device.name} (Unit {unit_id}): Empty tags in FC group {fc}")
                    continue
                
                if group.count <= 0:
                    logger.warning(f"Device {device.name} (Unit {unit_id}): Invalid count {group.count} for FC group {fc}")
                    continue
                
                logger.debug(f"Device {device.name} (Unit {unit_id}): Reading FC {fc}, start_addr={group.start_addr}, count={group.count}, tags={len(group.tags)}")
                
                # Read batch
                if fc == 1:  # Read Coils
                    result = self.client.read_coils(group.start_addr, group.count, slave=unit_id)
                elif fc == 2:  # Read Discrete Inputs
                    result = self.client.read_discrete_inputs(group.start_addr, group.count, slave=unit_id)
                elif fc == 3:  # Read Holding Registers
                    result = self.client.read_holding_registers(group.start_addr, group.count, slave=unit_id)
                elif fc == 4:  # Read Input Registers
                    result = self.client.read_input_registers(group.start_addr, group.count, slave=unit_id)
                else:
                    logger.warning(f"Device {device.name} (Unit {unit_id}): Unsupported function code {fc}")
                    continue
                
                if result.isError():
                    logger.warning(f"Device {device.name} (Unit {unit_id}): Function {fc} read error: {result}")
                    continue
                
                # Log what we got back
                if fc in [1, 2]:  # Boolean
                    result_length = len(result.bits) if hasattr(result, 'bits') else 0
                    logger.debug(f"Device {device.name} (Unit {unit_id}): FC {fc} returned {result_length} bits")
                else:  # Register
                    result_length = len(result.registers) if hasattr(result, 'registers') else 0
                    logger.debug(f"Device {device.name} (Unit {unit_id}): FC {fc} returned {result_length} registers")
                
                # Extract values for each tag
                for tag in group.tags:
                    try:
                        # Use normalized address for offset calculation
                        normalized_tag_addr = self._normalize_address(tag.address)
                        offset = normalized_tag_addr - group.start_addr
                        
                        # Debug info
                        logger.debug(f"Tag {tag.name}: raw_address={tag.address}, normalized_address={normalized_tag_addr}, start_addr={group.start_addr}, offset={offset}, count={group.count}")
                        
                        # Bounds checking
                        if offset < 0:
                            logger.warning(f"Tag {tag.name}: Invalid offset {offset} (normalized_addr {normalized_tag_addr} < start_addr {group.start_addr})")
                            continue
                        
                        if fc in [1, 2]:  # Boolean
                            if not hasattr(result, 'bits') or offset >= len(result.bits):
                                logger.warning(f"Tag {tag.name}: Offset {offset} out of range for bits (length: {len(result.bits) if hasattr(result, 'bits') else 'N/A'})")
                                continue
                            raw_value = bool(result.bits[offset])
                        else:  # Register
                            if not hasattr(result, 'registers') or offset >= len(result.registers):
                                logger.warning(f"Tag {tag.name}: Offset {offset} out of range for registers (length: {len(result.registers) if hasattr(result, 'registers') else 'N/A'})")
                                continue
                            raw_value = result.registers[offset]
                        
                        raw_values.append(RawModbusValue(
                            device_id=device.id,
                            tag_id=tag.id,
                            tag_name=tag.name,
                            function_code=fc,
                            address=tag.address,
                            raw_value=raw_value,
                            timestamp=time.time(),
                            data_type=tag.datatype,
                            scale=tag.scale,
                            offset=tag.offset,
                            unit=tag.unit
                        ))
                        
                        logger.debug(f"Tag {tag.name}: Successfully read value {raw_value}")
                        
                    except (IndexError, AttributeError) as e:
                        logger.warning(f"Tag {tag.name}: Value extraction error: {e}")
                        logger.debug(f"  Tag details: raw_address={tag.address}, normalized_address={self._normalize_address(tag.address)}, FC={fc}")
                        logger.debug(f"  Group details: start_addr={group.start_addr}, count={group.count}")
                        logger.debug(f"  Result type: {type(result)}, has bits: {hasattr(result, 'bits')}, has registers: {hasattr(result, 'registers')}")
                        if hasattr(result, 'bits'):
                            logger.debug(f"  Bits length: {len(result.bits)}")
                        if hasattr(result, 'registers'):
                            logger.debug(f"  Registers length: {len(result.registers)}")
                        
            except Exception as e:
                logger.error(f"Device {device.name} (Unit {unit_id}): Read error for FC {fc}: {e}")
        
        return raw_values

    def _read_cycle(self):
        """Single read cycle for all devices on this COM port"""
        if not self._ensure_connected():
            return
        
        # Only reload configs every 30 seconds to avoid clearing cache too often
        now = time.time()
        if now - self._last_config_reload > 30:
            self.config_cache.reload_if_needed()
            self.reload_device_configs()
            self._last_config_reload = now
        
        with self._lock:
            devices_copy = dict(self.devices)
            tags_copy = dict(self.device_tags)
        
        total_values = 0
        
        # Read each device sequentially (important for RTU)
        for unit_id, device in devices_copy.items():
            tags = tags_copy.get(unit_id, [])
            if not tags:
                continue
            
            try:
                start_time = time.time()
                raw_values = self._read_device_tags(unit_id, device, tags)
                read_time = time.time() - start_time
                
                if raw_values:
                    # Send to value queue for processing
                    success_count = self.value_queue.enqueue_raw_values_batch(raw_values)
                    total_values += len(raw_values)
                    
                    # Update device status
                    self.config_cache.update_device_status(device.id, "connected")
                    
                    logger.debug(f"Device {device.name} (Unit {unit_id}): Read {len(raw_values)} values ({success_count} queued) in {read_time:.3f}s")
                else:
                    logger.warning(f"Device {device.name} (Unit {unit_id}): No values read")
                
                # Small delay between devices to prevent overwhelming RTU bus
                time.sleep(0.01)
                
            except Exception as e:
                logger.error(f"Device {device.name} (Unit {unit_id}): Read cycle error: {e}")
                self.config_cache.update_device_status(device.id, "disconnected")
        
        if total_values > 0:
            logger.debug(f"COM {self.com_config.serial_port}: Read cycle completed, {total_values} total values")

    def start(self):
        """Start the RTU COM reader thread"""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info(f"Started RTU COM reader for {self.com_config.serial_port}")

    def stop(self):
        """Stop the RTU COM reader thread"""
        if not self._running:
            return
        
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        
        self._close()
        logger.info(f"Stopped RTU COM reader for {self.com_config.serial_port}")

    def _run(self):
        """Main thread loop"""
        logger.info(f"RTU COM reader thread started for {self.com_config.serial_port}")
        
        while self._running:
            try:
                cycle_start = time.time()
                self._read_cycle()
                
                # Calculate cycle time and sleep
                cycle_time = time.time() - cycle_start
                target_interval = 1.0  # 1 second between cycles
                sleep_time = max(0, target_interval - cycle_time)
                
                # Sleep in small chunks to be responsive to stop signal
                end_time = time.time() + sleep_time
                while time.time() < end_time and self._running:
                    time.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"RTU COM {self.com_config.serial_port}: Thread error: {e}")
                time.sleep(1)
        
        logger.info(f"RTU COM reader thread stopped for {self.com_config.serial_port}")

    def _normalize_address(self, addr: int) -> int:
        """Normalize Modbus address to 0-based (same logic as ConfigCache)"""
        a = int(addr)
        if a >= 40001:      # Holding Registers (4xxxx)
            return a - 40001
        if a >= 30001:      # Input Registers   (3xxxx)
            return a - 30001
        if a >= 10001:      # Coils/Discrete (1/2xxxx)
            return a - 10001
        return a            # already 0-based

    def write_tag_value(self, tag_id: int, value: float) -> bool:
        """Write value to RTU tag"""
        with self._lock:
            # Find which device this tag belongs to
            for unit_id, device in self.devices.items():
                tag = None
                device_tags = self.device_tags.get(unit_id, [])
                for t in device_tags:
                    if t.id == tag_id:
                        tag = t
                        break
                
                if tag:
                    return self._write_tag_to_device(unit_id, device, tag, value)
        
        logger.warning(f"Tag {tag_id} not found in RTU COM {self.com_config.serial_port}")
        return False

    def _write_tag_to_device(self, unit_id: int, device: DeviceConfig, tag: TagConfig, value: float) -> bool:
        """Write value to specific device tag"""
        if not self.client:
            logger.warning(f"No client connection for RTU COM {self.com_config.serial_port}")
            return False

        try:
            # Determine function code
            function_code = tag.function_code or device.default_function_code
            normalized_addr = self._normalize_address(tag.address)
            
            logger.debug(f"Writing to tag {tag.name} (Unit {unit_id}): raw_addr={tag.address}, normalized_addr={normalized_addr}, FC={function_code}, value={value}")
            
            # Only allow writing to writable function codes
            if function_code == 1:  # Coils - writable (function code 05/15)
                return self._write_coil(unit_id, tag, normalized_addr, value)
            elif function_code == 2:  # Discrete Inputs - READ ONLY
                logger.warning(f"Cannot write to discrete input tag {tag.name} (function code 02)")
                return False
            elif function_code == 3:  # Holding Registers - writable (function code 06/16)
                return self._write_holding_register(unit_id, tag, normalized_addr, value)
            elif function_code == 4:  # Input Registers - READ ONLY  
                logger.warning(f"Cannot write to input register tag {tag.name} (function code 04)")
                return False
            else:
                logger.warning(f"Unsupported function code {function_code} for tag {tag.name}")
                return False
                
        except Exception as e:
            logger.error(f"Error writing to tag {tag.name} (Unit {unit_id}): {e}")
            return False

    def _write_coil(self, unit_id: int, tag: TagConfig, address: int, value: float) -> bool:
        """Write value to coil (function code 05)"""
        try:
            # Convert value to boolean
            bool_value = bool(value)
            result = self.client.write_coil(address, bool_value, slave=unit_id)
            
            if result.isError():
                logger.warning(f"Coil write error for tag {tag.name} (Unit {unit_id}): {result}")
                return False
            
            logger.debug(f"Successfully wrote coil {tag.name} (Unit {unit_id}): {bool_value}")
            return True
            
        except Exception as e:
            logger.error(f"Exception writing coil {tag.name} (Unit {unit_id}): {e}")
            return False

    def _write_holding_register(self, unit_id: int, tag: TagConfig, address: int, value: float) -> bool:
        """Write value to holding register (function code 06)"""
        try:
            # Convert based on datatype
            datatype = (tag.datatype or "").strip().lower()
            
            if datatype in ("float", "float32", "real"):
                # Write float as 2 registers
                import struct
                # Convert float to 2 registers
                byte_data = struct.pack('>f', value)
                reg1 = struct.unpack('>H', byte_data[0:2])[0]
                reg2 = struct.unpack('>H', byte_data[2:4])[0]
                
                # Write 2 registers
                result = self.client.write_registers(address, [reg1, reg2], slave=unit_id)
            else:
                # Single register (int16, uint16, etc.)
                int_value = int(value)
                if int_value < 0:
                    # Handle signed integers
                    int_value = int_value & 0xFFFF
                result = self.client.write_register(address, int_value, slave=unit_id)
            
            if result.isError():
                logger.warning(f"Register write error for tag {tag.name} (Unit {unit_id}): {result}")
                return False
            
            logger.debug(f"Successfully wrote register {tag.name} (Unit {unit_id}): {value}")
            return True
            
        except Exception as e:
            logger.error(f"Exception writing register {tag.name} (Unit {unit_id}): {e}")
            return False


class RTUComService:
    """Manages multiple RTU COM readers"""
    
    def __init__(self, config_cache: ConfigCache, emission_manager: SocketEmissionManager, 
                 value_queue: ValueQueueService):
        self.config_cache = config_cache
        self.emission_manager = emission_manager
        self.value_queue = value_queue
        
        self.com_readers: Dict[RTUComConfig, RTUComReader] = {}
        self._lock = threading.RLock()
        
        logger.info("RTU COM Service initialized")

    def add_device(self, device_config: DeviceConfig):
        """Add RTU device to appropriate COM reader"""
        if device_config.protocol != "ModbusRTU":
            return
        
        com_config = RTUComConfig(
            serial_port=device_config.serial_port,
            baudrate=device_config.baudrate,
            bytesize=device_config.bytesize,
            parity=device_config.parity,
            stopbits=device_config.stopbits,
            timeout=device_config.timeout_ms / 1000.0
        )
        
        with self._lock:
            if com_config not in self.com_readers:
                reader = RTUComReader(com_config, self.config_cache, 
                                    self.emission_manager, self.value_queue)
                self.com_readers[com_config] = reader
                reader.start()
            
            self.com_readers[com_config].add_device(device_config)

    def remove_device(self, device_config: DeviceConfig):
        """Remove RTU device from COM reader"""
        if device_config.protocol != "ModbusRTU":
            return
        
        com_config = RTUComConfig(
            serial_port=device_config.serial_port,
            baudrate=device_config.baudrate,
            bytesize=device_config.bytesize,
            parity=device_config.parity,
            stopbits=device_config.stopbits
        )
        
        with self._lock:
            if com_config in self.com_readers:
                reader = self.com_readers[com_config]
                reader.remove_device(device_config.unit_id)
                
                # If no more devices, stop and remove reader
                if not reader.has_devices():
                    reader.stop()
                    del self.com_readers[com_config]

    def stop_all(self):
        """Stop all COM readers"""
        with self._lock:
            for reader in self.com_readers.values():
                reader.stop()
            self.com_readers.clear()
        
        logger.info("All RTU COM readers stopped")

    def write_tag_value(self, tag_id: int, value: float) -> bool:
        """Write value to RTU tag across all COM ports"""
        with self._lock:
            for reader in self.com_readers.values():
                if reader.write_tag_value(tag_id, value):
                    return True
            
            logger.warning(f"RTU tag {tag_id} not found in any COM port")
            return False

    def reload_configs(self):
        """Reload configurations for all RTU COM readers"""
        with self._lock:
            for reader in self.com_readers.values():
                reader.reload_device_configs()
        logger.info("RTU COM service configurations reloaded")

# Global instance
_rtu_com_service: Optional[RTUComService] = None

def get_rtu_com_service() -> RTUComService:
    """Get global RTU COM service instance"""
    global _rtu_com_service
    if _rtu_com_service is None:
        from modbus_monitor.services.config_cache import get_config_cache
        from modbus_monitor.services.socket_emission_manager import get_emission_manager
        from modbus_monitor.services.value_queue_service import value_queue_service
        
        _rtu_com_service = RTUComService(
            get_config_cache(),
            get_emission_manager(),
            value_queue_service
        )
    return _rtu_com_service

def shutdown_rtu_com_service():
    """Shutdown global RTU COM service"""
    global _rtu_com_service
    if _rtu_com_service:
        _rtu_com_service.stop_all()
        _rtu_com_service = None