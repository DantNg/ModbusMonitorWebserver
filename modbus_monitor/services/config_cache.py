"""
Config cache để giảm truy xuất DB trong Modbus threads
"""
import threading
import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from modbus_monitor.database import db as dbsync

logger = logging.getLogger(__name__)

@dataclass
class DeviceConfig:
    """Cached device configuration"""
    id: int
    name: str
    protocol: str
    host: Optional[str] = None
    port: Optional[int] = None
    serial_port: Optional[str] = None
    baudrate: int = 9600
    bytesize: int = 8
    parity: str = "N"
    stopbits: int = 1
    unit_id: int = 1
    timeout_ms: int = 200
    default_function_code: int = 3
    byte_order: str = "BigEndian"
    word_order: str = "AB"
    status: str = "unknown"  # Status: "connected", "disconnected", "unknown"
    last_seen: Optional[float] = None  # Timestamp of last successful communication
    
    @classmethod
    def from_db_row(cls, row: Dict) -> 'DeviceConfig':
        """Tạo DeviceConfig từ database row"""
        return cls(
            id=int(row["id"]),
            name=row.get("name", "Unknown"),
            protocol=row.get("protocol", "ModbusTCP"),
            host=row.get("host"),
            port=int(row.get("port", 502)) if row.get("port") is not None else 502,
            serial_port=row.get("serial_port"),
            baudrate=int(row.get("baudrate", 9600)) if row.get("baudrate") is not None else 9600,
            bytesize=int(row.get("bytesize", 8)) if row.get("bytesize") is not None else 8,
            parity=row.get("parity", "N"),
            stopbits=int(row.get("stopbits", 1)) if row.get("stopbits") is not None else 1,
            unit_id=int(row.get("unit_id", 1)) if row.get("unit_id") is not None else 1,
            timeout_ms=int(row.get("timeout_ms", 200)) if row.get("timeout_ms") is not None else 200,
            default_function_code=int(row.get("default_function_code", 3)) if row.get("default_function_code") is not None else 3,
            byte_order=row.get("byte_order", "BigEndian"),
            word_order=row.get("word_order", "AB")
        )

@dataclass 
class TagConfig:
    """Cached tag configuration"""
    id: int
    device_id: int
    name: str
    address: int
    datatype: str
    scale: float = 1.0
    offset: float = 0.0
    function_code: Optional[int] = None
    unit: str = ""  # Add unit field
    
    @classmethod
    def from_db_row(cls, row: Dict) -> 'TagConfig':
        """Tạo TagConfig từ database row"""
        return cls(
            id=row["id"],
            device_id=row["device_id"],
            name=row.get("name", "Unknown"),
            address=int(row["address"]) if row.get("address") is not None else 0,
            datatype=row.get("datatype", "unsigned"),
            scale=float(row.get("scale", 1.0)) if row.get("scale") is not None else 1.0,
            offset=float(row.get("offset", 0.0)) if row.get("offset") is not None else 0.0,
            function_code=row.get("function_code"),
            unit=row.get("unit", "")  # Add unit from database
        )

@dataclass
class FunctionCodeGroup:
    """Pre-calculated function code group"""
    function_code: int
    tags: List[TagConfig]
    start_addr: int
    count: int
    
class ConfigCache:
    """
    Cache cho device và tag configurations với khả năng read/write
    - Giảm thiểu truy xuất DB trong Modbus threads
    - Đồng bộ thay đổi với database
    - Thread-safe operations
    """
    
    def __init__(self, reload_interval: float = 30.0):
        self._devices: Dict[int, DeviceConfig] = {}
        self._tags_by_device: Dict[int, List[TagConfig]] = {}
        self._fc_groups_by_device: Dict[int, List[FunctionCodeGroup]] = {}
        self._subdashboard_cache: Dict[int, List[int]] = {}  # subdash_id -> tag_ids
        
        self._lock = threading.RLock()
        self._reload_interval = reload_interval
        self._last_reload = 0.0
        self._last_reload = 0.0
        self._subdash_cache_time = 0.0
        
        # Track dirty states for efficient updates
        self._dirty_devices: set = set()
        self._dirty_tags: set = set()
        
        # Load initial data
        self._load_all_configs()
    
    # ===== READ OPERATIONS (Cache First) =====
    
    def get_device(self, device_id: int) -> Optional[DeviceConfig]:
        """Get device config from cache"""
        with self._lock:
            self.reload_if_needed()
            return self._devices.get(device_id)
    
    def get_all_devices(self) -> Dict[int, DeviceConfig]:
        """Get all device configs from cache"""
        with self._lock:
            self.reload_if_needed()
            return self._devices.copy()
    
    def get_device_tags(self, device_id: int) -> List[TagConfig]:
        """Get tags for device from cache"""
        with self._lock:
            self.reload_if_needed()
            return self._tags_by_device.get(device_id, []).copy()
    
    def get_tag(self, tag_id: int) -> Optional[TagConfig]:
        """Get specific tag config from cache"""
        with self._lock:
            self.reload_if_needed()
            for tags in self._tags_by_device.values():
                for tag in tags:
                    if tag.id == tag_id:
                        return tag
            return None
    
    # ===== WRITE OPERATIONS (Cache + DB) =====
    
    def update_device(self, device_id: int, updates: Dict[str, Any]) -> bool:
        """Update device config in cache and DB"""
        with self._lock:
            try:
                # Update database first
                result = dbsync.update_device_row(device_id, updates)
                if result <= 0:
                    return False
                
                # Update cache
                device = self._devices.get(device_id)
                if device:
                    # Apply updates to cached device
                    for key, value in updates.items():
                        if hasattr(device, key):
                            setattr(device, key, value)
                    
                    # Recalculate function code groups if relevant fields changed
                    fc_affecting_fields = {'default_function_code', 'byte_order', 'word_order'}
                    if any(field in updates for field in fc_affecting_fields):
                        tags = self._tags_by_device.get(device_id, [])
                        self._fc_groups_by_device[device_id] = self._calculate_fc_groups(tags, device)
                    
                    print(f"✅ Updated device {device_id} in cache and DB")
                    return True
                else:
                    # Device not in cache, force reload
                    self._load_all_configs()
                    return True
                    
            except Exception as e:
                print(f"❌ Error updating device {device_id}: {e}")
                return False
    
    def add_device(self, device_data: Dict[str, Any]) -> Optional[int]:
        """Add new device to cache and DB"""
        with self._lock:
            try:
                # Add to database first
                device_id = dbsync.add_device_row(device_data)
                
                # Add to cache
                device_row = {**device_data, "id": device_id}
                device = DeviceConfig.from_db_row(device_row)
                self._devices[device_id] = device
                self._tags_by_device[device_id] = []
                self._fc_groups_by_device[device_id] = []
                
                print(f"✅ Added device {device_id} to cache and DB")
                return device_id
                
            except Exception as e:
                print(f"❌ Error adding device: {e}")
                return None
    
    def get_tag_logger_map(self, device_id: int = None) -> dict:
        """Get mapping of tag_id -> logger info for a device or all devices"""
        try:
            from modbus_monitor.database import db as dbsync
            return dbsync.get_tag_logger_map(device_id)
        except Exception as e:
            logger.error(f"Error getting tag logger map: {e}")
            return {}

    def delete_device(self, device_id: int) -> bool:
        """Delete device from cache and DB"""
        with self._lock:
            try:
                # Delete from database first
                result = dbsync.delete_device_row(device_id)
                if result <= 0:
                    return False
                
                # Remove from cache
                self._devices.pop(device_id, None)
                self._tags_by_device.pop(device_id, None)
                self._fc_groups_by_device.pop(device_id, None)
                
                print(f"✅ Deleted device {device_id} from cache and DB")
                return True
                
            except Exception as e:
                print(f"❌ Error deleting device {device_id}: {e}")
                return False
    
    def update_tag(self, tag_id: int, updates: Dict[str, Any]) -> bool:
        """Update tag config in cache and DB"""
        with self._lock:
            try:
                # Update database first
                result = dbsync.update_tag_row(tag_id, updates)
                if result <= 0:
                    return False
                
                # Update cache
                tag = self.get_tag(tag_id)
                if tag:
                    # Apply updates to cached tag
                    for key, value in updates.items():
                        if hasattr(tag, key):
                            setattr(tag, key, value)
                    
                    # Recalculate function code groups for device
                    device = self._devices.get(tag.device_id)
                    if device:
                        tags = self._tags_by_device.get(tag.device_id, [])
                        self._fc_groups_by_device[tag.device_id] = self._calculate_fc_groups(tags, device)
                    
                    print(f"✅ Updated tag {tag_id} in cache and DB")
                    return True
                else:
                    # Tag not in cache, force reload
                    self._load_all_configs()
                    return True
                    
            except Exception as e:
                print(f"❌ Error updating tag {tag_id}: {e}")
                return False
    
    def add_tag(self, device_id: int, tag_data: Dict[str, Any]) -> Optional[int]:
        """Add new tag to cache and DB"""
        with self._lock:
            try:
                # Add to database first
                tag_id = dbsync.add_tag_row(device_id, tag_data)
                
                # Add to cache
                tag_row = {**tag_data, "id": tag_id, "device_id": device_id}
                tag = TagConfig.from_db_row(tag_row)
                
                if device_id not in self._tags_by_device:
                    self._tags_by_device[device_id] = []
                self._tags_by_device[device_id].append(tag)
                
                # Recalculate function code groups
                device = self._devices.get(device_id)
                if device:
                    tags = self._tags_by_device[device_id]
                    self._fc_groups_by_device[device_id] = self._calculate_fc_groups(tags, device)
                
                print(f"✅ Added tag {tag_id} to cache and DB")
                return tag_id
                
            except Exception as e:
                print(f"❌ Error adding tag: {e}")
                return None
    
    def delete_tag(self, tag_id: int) -> bool:
        """Delete tag from cache and DB"""
        with self._lock:
            try:
                # Find tag first
                tag = self.get_tag(tag_id)
                if not tag:
                    return False
                
                device_id = tag.device_id
                
                # Delete from database first
                result = dbsync.delete_tag_row(tag_id)
                if result <= 0:
                    return False
                
                # Remove from cache
                if device_id in self._tags_by_device:
                    self._tags_by_device[device_id] = [
                        t for t in self._tags_by_device[device_id] if t.id != tag_id
                    ]
                    
                    # Recalculate function code groups
                    device = self._devices.get(device_id)
                    if device:
                        tags = self._tags_by_device[device_id]
                        self._fc_groups_by_device[device_id] = self._calculate_fc_groups(tags, device)
                
                print(f"✅ Deleted tag {tag_id} from cache and DB")
                return True
                
            except Exception as e:
                print(f"❌ Error deleting tag {tag_id}: {e}")
                return False
    
    # ===== LEGACY COMPATIBILITY METHODS =====
    
    def should_reload(self) -> bool:
        """Kiểm tra có cần reload không"""
        return time.time() - self._last_reload > self._reload_interval
    
    def reload_if_needed(self):
        """Reload configs nếu cần"""
        if self.should_reload():
            self.reload_configs()
    
    def reload_configs(self):
        """Force reload tất cả configs"""
        with self._lock:
            self._load_all_configs()
    
    def get_device_config(self, device_id: int) -> Optional[DeviceConfig]:
        """Legacy method - use get_device() instead"""
        return self.get_device(device_id)
    
    def get_device_fc_groups(self, device_id: int) -> List[FunctionCodeGroup]:
        """Get pre-calculated function code groups for device"""
        with self._lock:
            self.reload_if_needed()
            return self._fc_groups_by_device.get(device_id, [])
    
    def update_device_status(self, device_id: int, status: str, last_seen: Optional[float] = None):
        """Update device connection status in cache"""
        import time
        with self._lock:
            device = self._devices.get(device_id)
            if device:
                device.status = status
                device.last_seen = last_seen or time.time()
                logger.debug(f"Updated device {device_id} status to {status}")
    
    def get_device_status(self, device_id: int) -> Dict[str, Any]:
        """Get device status information"""
        with self._lock:
            device = self._devices.get(device_id)
            if device:
                return {
                    "id": device.id,
                    "name": device.name,
                    "status": device.status,
                    "last_seen": device.last_seen
                }
            return {"id": device_id, "name": "Unknown", "status": "unknown", "last_seen": None}
    
    def get_all_device_statuses(self) -> Dict[int, Dict[str, Any]]:
        """Get status information for all devices"""
        with self._lock:
            statuses = {}
            for device_id, device in self._devices.items():
                statuses[device_id] = {
                    "id": device.id,
                    "name": device.name,
                    "status": device.status,
                    "last_seen": device.last_seen
                }
            return statuses
    
    def invalidate_device(self, device_id: int):
        """Invalidate specific device cache - force reload on next access"""
        with self._lock:
            # Mark for reload by clearing the device
            if device_id in self._devices:
                self._dirty_devices.add(device_id)
        self._subdash_cache_time = 0.0
        
        # Load initial data
        self._load_all_configs()
    
    def should_reload(self) -> bool:
        """Kiểm tra có cần reload không"""
        return time.time() - self._last_reload > self._reload_interval
    
    def reload_if_needed(self):
        """Reload configs nếu cần"""
        if self.should_reload():
            self.reload_configs()
    
    def reload_configs(self):
        """Force reload tất cả configs"""
        with self._lock:
            self._load_all_configs()
    
    def _load_all_configs(self):
        """Load tất cả configs từ DB"""
        try:
            start_time = time.time()
            
            # Load devices
            device_rows = dbsync.list_devices()
            devices = {}
            for row in device_rows:
                device = DeviceConfig.from_db_row(row)
                devices[device.id] = device
            
            # Load tags by device
            tags_by_device = {}
            fc_groups_by_device = {}
            
            for device_id in devices.keys():
                tag_rows = dbsync.list_tags(device_id)
                tags = [TagConfig.from_db_row(row) for row in tag_rows]
                tags_by_device[device_id] = tags
                
                # Pre-calculate function code groups
                fc_groups = self._calculate_fc_groups(tags, devices[device_id])
                fc_groups_by_device[device_id] = fc_groups
            
            # Update cache atomically
            self._devices = devices
            self._tags_by_device = tags_by_device
            self._fc_groups_by_device = fc_groups_by_device
            self._last_reload = time.time()
            
            load_time = time.time() - start_time
            print(f"Config cache loaded: {len(devices)} devices, "
                  f"{sum(len(tags) for tags in tags_by_device.values())} tags "
                  f"in {load_time:.3f}s")
            
        except Exception as e:
            print(f"Error loading configs: {e}")
    
    def _calculate_fc_groups(self, tags: List[TagConfig], device: DeviceConfig) -> List[FunctionCodeGroup]:
        """Pre-calculate function code groups để tránh tính toán lặp lại"""
        groups_dict = {}
        device_default_fc = device.default_function_code
        
        # Group tags by function code
        for tag in tags:
            fc = tag.function_code or device_default_fc
            if fc not in groups_dict:
                groups_dict[fc] = []
            groups_dict[fc].append(tag)
        
        # Calculate read ranges for each group
        groups = []
        for fc, fc_tags in groups_dict.items():
            if not fc_tags:
                continue
                
            # Calculate address range
            addresses = []
            for tag in fc_tags:
                addr = self._normalize_address(tag.address)
                # Estimate register count based on datatype
                count = self._get_register_count(tag.datatype)
                addresses.extend(range(addr, addr + count))
            
            if addresses:
                start_addr = min(addresses)
                end_addr = max(addresses)
                count = end_addr - start_addr + 1
                
                group = FunctionCodeGroup(
                    function_code=fc,
                    tags=fc_tags,
                    start_addr=start_addr,
                    count=count
                )
                groups.append(group)
        
        return groups
    
    def _normalize_address(self, addr: int) -> int:
        """Normalize address như trong _DeviceReader"""
        a = int(addr)
        if a >= 40001:      # Holding Registers (4xxxx)
            return a - 40001
        if a >= 30001:      # Input Registers   (3xxxx)
            return a - 30001
        if a >= 10001:      # Coils/Discrete (1/2xxxx)
            return a - 10001
        return a            # already 0-based
    
    def _get_register_count(self, datatype: str) -> int:
        """Estimate số register cần cho datatype"""
        name = (datatype or "").strip().lower()
        if name in ("float", "float32", "real", "float_inverse", "floatinverse", "float-inverse",
                   "dword", "uint32", "udint", "dint", "int32", "int"):
            return 2
        elif name in ("double", "float64", "double_inverse", "doubleinverse", "double-inverse",
                     "long", "int64", "long_inverse", "longinverse", "long-inverse"):
            return 4
        else:
            return 1  # single register
    
    # Getter methods
    def get_device(self, device_id: int) -> Optional[DeviceConfig]:
        """Lấy device config"""
        with self._lock:
            return self._devices.get(device_id)
    
    def get_devices(self) -> Dict[int, DeviceConfig]:
        """Lấy tất cả devices"""
        with self._lock:
            return self._devices.copy()
    
    def get_device_tags(self, device_id: int) -> List[TagConfig]:
        """Lấy tags của device"""
        with self._lock:
            return self._tags_by_device.get(device_id, []).copy()
    
    def get_device_fc_groups(self, device_id: int) -> List[FunctionCodeGroup]:
        """Lấy pre-calculated function code groups"""
        with self._lock:
            return self._fc_groups_by_device.get(device_id, []).copy()
    
    def get_subdashboard_tags(self, subdash_id: int) -> List[int]:
        """Lấy tag IDs của subdashboard (with caching)"""
        current_time = time.time()
        
        with self._lock:
            # Reload subdashboard cache mỗi 60s
            if current_time - self._subdash_cache_time > 60:
                try:
                    self._subdashboard_cache.clear()
                    subdashboards = dbsync.list_subdashboards() or []
                    for subdash in subdashboards:
                        subdash_id_key = subdash['id']
                        tag_ids = [t['id'] for t in dbsync.get_subdashboard_tags(subdash_id_key) or []]
                        self._subdashboard_cache[subdash_id_key] = tag_ids
                    self._subdash_cache_time = current_time
                except Exception as e:
                    print(f"Error loading subdashboard cache: {e}")
            
            return self._subdashboard_cache.get(subdash_id, [])
    
    # ===== UTILITY METHODS =====
    
    def _load_all_configs(self):
        """Load tất cả configs từ DB"""
        try:
            start_time = time.time()
            
            # Load devices
            device_rows = dbsync.list_devices()
            devices = {}
            for row in device_rows:
                device = DeviceConfig.from_db_row(row)
                devices[device.id] = device
            
            # Load tags by device
            tags_by_device = {}
            fc_groups_by_device = {}
            
            for device_id in devices.keys():
                tag_rows = dbsync.list_tags(device_id)
                tags = [TagConfig.from_db_row(row) for row in tag_rows]
                tags_by_device[device_id] = tags
                
                # Pre-calculate function code groups
                fc_groups = self._calculate_fc_groups(tags, devices[device_id])
                fc_groups_by_device[device_id] = fc_groups
            
            # Update cache atomically
            self._devices = devices
            self._tags_by_device = tags_by_device
            self._fc_groups_by_device = fc_groups_by_device
            self._last_reload = time.time()
            
            # Clear dirty flags
            self._dirty_devices.clear()
            self._dirty_tags.clear()
            
            load_time = time.time() - start_time
            print(f"🔄 Config cache reloaded: {len(devices)} devices, "
                  f"{sum(len(tags) for tags in tags_by_device.values())} tags "
                  f"in {load_time:.3f}s")
            
        except Exception as e:
            print(f"❌ Error loading configs: {e}")
    
    def _calculate_fc_groups(self, tags: List[TagConfig], device: DeviceConfig) -> List[FunctionCodeGroup]:
        """Pre-calculate function code groups để tránh tính toán lặp lại"""
        groups_dict = {}
        device_default_fc = device.default_function_code
        
        # Group tags by function code
        for tag in tags:
            fc = tag.function_code or device_default_fc
            if fc not in groups_dict:
                groups_dict[fc] = []
            groups_dict[fc].append(tag)
        
        # Calculate read ranges for each group
        groups = []
        for fc, fc_tags in groups_dict.items():
            if not fc_tags:
                continue
                
            # Calculate address range
            addresses = []
            for tag in fc_tags:
                addr = self._normalize_address(tag.address)
                # Estimate register count based on datatype
                count = self._get_register_count(tag.datatype)
                addresses.extend(range(addr, addr + count))
            
            if addresses:
                start_addr = min(addresses)
                end_addr = max(addresses)
                count = end_addr - start_addr + 1
                
                group = FunctionCodeGroup(
                    function_code=fc,
                    tags=fc_tags,
                    start_addr=start_addr,
                    count=count
                )
                groups.append(group)
        
        return groups
    
    def _normalize_address(self, addr: int) -> int:
        """Normalize Modbus address to 0-based"""
        a = int(addr)
        if a >= 40001:      # Holding Registers (4xxxx)
            return a - 40001
        if a >= 30001:      # Input Registers   (3xxxx)
            return a - 30001
        if a >= 10001:      # Coils/Discrete (1/2xxxx)
            return a - 10001
        return a            # Already 0-based
    
    def _get_register_count(self, datatype: str) -> int:
        """Get register count for datatype"""
        if datatype in ["DWord", "DInt", "Float", "Double"]:
            return 2
        elif datatype in ["Long", "Long_inverse"]:
            return 4
        else:
            return 1  # Word, Short, Bit, etc.


# ===== GLOBAL CACHE INSTANCE =====

_config_cache: Optional[ConfigCache] = None
_cache_lock = threading.Lock()

def get_config_cache() -> ConfigCache:
    """Get global config cache instance (singleton)"""
    global _config_cache
    if _config_cache is None:
        with _cache_lock:
            if _config_cache is None:
                _config_cache = ConfigCache()
    return _config_cache

def reload_config_cache():
    """Force reload global config cache"""
    cache = get_config_cache()
    cache.reload_configs()

def invalidate_config_cache():
    """Invalidate and recreate global config cache"""
    global _config_cache
    with _cache_lock:
        _config_cache = None
