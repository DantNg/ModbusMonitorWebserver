"""
TCP Worker - Handles Modbus TCP communication over network
"""

import time
import threading
from datetime import datetime
import socket
import sys
import os
import requests
import json

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Import shared database manager
try:
    from shared.database_manager import DatabaseManager
    DB_AVAILABLE = True
except ImportError:
    print("⚠️ DatabaseManager not available")
    DB_AVAILABLE = False
    DatabaseManager = None

# Try to import SocketIO client for direct emission
try:
    import socketio
    SOCKETIO_AVAILABLE = True
    print("✅ SocketIO client available for direct emission")
except ImportError:
    print("⚠️ python-socketio not available - falling back to HTTP API")
    SOCKETIO_AVAILABLE = False
    socketio = None

# Try to import pymodbus with correct syntax
try:
    from pymodbus.client import ModbusTcpClient
    from pymodbus.exceptions import ModbusException
    PYMODBUS_AVAILABLE = True
except ImportError:
    print("⚠️ pymodbus not available - TCP worker will use simulation mode")
    PYMODBUS_AVAILABLE = False
    ModbusTcpClient = None
    ModbusException = Exception

class TCPWorker:
    def __init__(self, worker_id, host, port=502, timeout=5, devices=None, tags=None, webapp_url="http://localhost:5000"):
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.timeout = timeout
        self.devices = devices or []
        self.tags = tags or []
        self.is_running = False
        self.client = None
        self.worker_thread = None
        self.debug = True
        self.webapp_url = webapp_url
        
        # Initialize database manager
        self.db_manager = DatabaseManager() if DB_AVAILABLE else None
        
        # Initialize SocketIO client for direct emission to webapp
        self.sio_client = None
        self.sio_connected = False
        self.active_rooms = set()  # Track active rooms that need updates
        self.room_tag_mapping = {}  # Map rooms to tag sets for filtering
        if SOCKETIO_AVAILABLE:
            self._init_socketio_client()
        
        # Group tags by device for efficient reading
        self.device_tags = {}
        for tag in self.tags:
            if tag.device_id not in self.device_tags:
                self.device_tags[tag.device_id] = []
            self.device_tags[tag.device_id].append(tag)
    
    def _init_socketio_client(self):
        """Initialize SocketIO client connection to webapp"""
        try:
            self.sio_client = socketio.SimpleClient()
            
            # Extract host and port from webapp_url
            webapp_host = self.webapp_url.replace('http://', '').replace('https://', '')
            
            print(f"🔌 Connecting SocketIO client to {self.webapp_url}")
            self.sio_client.connect(self.webapp_url)
            self.sio_connected = True
            print(f"✅ SocketIO client connected to webapp")
            
            # Listen for room management events from webapp
            self._setup_room_listeners()
            
        except Exception as e:
            print(f"❌ Failed to connect SocketIO client: {e}")
            self.sio_client = None
            self.sio_connected = False
    
    def _setup_room_listeners(self):
        """Setup event listeners for room management"""
        if not self.sio_client:
            return
            
        # For SimpleClient, we need to use a polling approach or handle events in the main loop
        # Since SimpleClient doesn't support event decorators, we'll use a different approach
        print("👂 Room event listeners setup complete (using polling approach)")
        
    def _handle_room_joined(self, data):
        """Handle room join notification from webapp"""
        try:
            room = data.get('room')
            client_id = data.get('client_id')
            
            if room:
                self.active_rooms.add(room)
                print(f"📍 Room joined: {room} (client: {client_id})")
                
                # If it's a subdashboard room, get tag list for filtering
                if room.startswith('subdashboard_'):
                    subdash_id = room.replace('subdashboard_', '')
                    self._update_room_tag_mapping(room, subdash_id)
                
        except Exception as e:
            print(f"❌ Error handling room join: {e}")
    
    def _handle_room_left(self, data):
        """Handle room leave notification from webapp"""
        try:
            room = data.get('room')
            client_id = data.get('client_id')
            
            if room and room in self.active_rooms:
                # Check if any clients are still in the room
                # For simplicity, we'll remove room immediately
                # In production, you might want to check room occupancy
                self.active_rooms.discard(room)
                self.room_tag_mapping.pop(room, None)
                print(f"📍 Room left: {room} (client: {client_id})")
                
        except Exception as e:
            print(f"❌ Error handling room leave: {e}")
    
    def _update_room_tag_mapping(self, room, subdash_id):
        """Update tag mapping for a room by querying database"""
        try:
            if not self.db_manager:
                return
                
            # Query database to get tags for this subdashboard
            query = """
                SELECT t.id 
                FROM tags t
                JOIN dashboard_tags dt ON t.id = dt.tag_id
                WHERE dt.dashboard_id = :subdash_id
            """
            
            result = self.db_manager.execute_query(query, {'subdash_id': subdash_id})
            tag_ids = {row[0] for row in result} if result else set()
            
            if tag_ids:
                self.room_tag_mapping[room] = tag_ids
                print(f"📋 Room {room} mapped to {len(tag_ids)} tags")
            
        except Exception as e:
            print(f"❌ Error updating room tag mapping: {e}")
    
    def _reconnect_socketio(self):
        """Reconnect SocketIO client if disconnected"""
        if not SOCKETIO_AVAILABLE:
            return
            
        try:
            if self.sio_client:
                self.sio_client.disconnect()
            self._init_socketio_client()
        except Exception as e:
            print(f"❌ SocketIO reconnection failed: {e}")
            self.sio_connected = False
    
    def start(self):
        """Start the TCP worker"""
        if self.is_running:
            print(f"⚠️  TCP Worker {self.worker_id} already running")
            return
        
        try:
            if not PYMODBUS_AVAILABLE:
                print("🛠️ Running in simulation mode (pymodbus not available)")
                self.client = None  # Use simulation mode
            else:
                # Create Modbus TCP client
                self.client = ModbusTcpClient(
                    host=self.host,
                    port=self.port,
                    timeout=self.timeout
                )
                
                if not self.client.connect():
                    print(f"❌ Failed to connect to {self.host}:{self.port}")
                    return False
            
            print(f"✅ Connected to TCP: {self.host}:{self.port}")
            
            self.is_running = True
            self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self.worker_thread.start()
            
            return True
            
        except Exception as e:
            print(f"❌ TCP Worker start error: {e}")
            return False
    
    def stop(self):
        """Stop the TCP worker"""
        self.is_running = False
        
        # Disconnect SocketIO client
        if self.sio_client and self.sio_connected:
            try:
                self.sio_client.disconnect()
                print("🔌 SocketIO client disconnected")
            except Exception as e:
                print(f"⚠️ Error disconnecting SocketIO: {e}")
        
        if self.client:
            self.client.close()
        
        if self.worker_thread:
            self.worker_thread.join(timeout=5)
        
        print(f"🛑 TCP Worker {self.worker_id} stopped")
    
    def _worker_loop(self):
        """Main worker loop"""
        print(f"🔄 TCP Worker loop started for {len(self.devices)} devices")
        
        while self.is_running:
            try:
                for device in self.devices:
                    if not self.is_running:
                        break
                    
                    self._read_device(device)
                    
                # Wait before next polling cycle
                time.sleep(1)
                
            except Exception as e:
                print(f"❌ TCP Worker loop error: {e}")
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
            all_tag_data = []  # Collect all successful tag reads for batch emission
            
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
                                
                                # Store to database
                                if self.db_manager:
                                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    self.db_manager.update_tag_latest_value(tag.id, value, timestamp)
                                
                                # Collect tag data for batch emission
                                all_tag_data.append({
                                    "id": tag.id,
                                    "name": tag.name,
                                    "value": value,
                                    "ts": datetime.now().isoformat(),
                                    "datatype": tag.data_type if hasattr(tag, 'data_type') else "Word"
                                })
                            else:
                                if self.debug:
                                    print(f"   ❌ {tag.name}: No data")
                                
                except Exception as block_error:
                    print(f"❌ Error reading block FC{function_code}: {block_error}")
                    print(f"⚠️  Skipping tags in failed block")
            
            # Emit all tags for this device in one batch
            if all_tag_data:
                self._emit_device_tags_to_webapp(all_tag_data, device)
            
            if self.debug:
                print(f"✅ Device {device.name}: {successful_reads}/{len(device_tags)} tags read successfully")
                
        except Exception as e:
            print(f"❌ Error reading device {device.name}: {e}")
            
            if self.debug:
                print(f"✅ Device {device.name}: {successful_reads}/{len(device_tags)} tags read successfully")
                
        except Exception as e:
            print(f"❌ Device read error for {device.name}: {e}")
    
    def _read_tags_block(self, device, function_code, tags):
        """Read multiple tags in a single block read"""
        try:
            # Sort tags by address
            sorted_tags = sorted(tags, key=lambda t: t.address)
            
            # Find address range
            min_addr = min(tag.address for tag in sorted_tags)
            max_addr = max(tag.address for tag in sorted_tags)
            
            # Convert to 0-based addressing
            start_addr = max(0, min_addr - 1) if min_addr > 0 else min_addr
            count = max_addr - min_addr + 1
            
            print(f"   🔍 Block read FC{function_code}: Addr {min_addr}-{max_addr} → {start_addr}+{count}, Unit {device.unit_id}")
            # Read the block
            if function_code == 1:  # Read Coils
                result = self.client.read_coils(address=start_addr, count=count, slave=device.unit_id)
            elif function_code == 2:  # Read Discrete Inputs
                result = self.client.read_discrete_inputs(address=start_addr, count=count, slave=device.unit_id)
            elif function_code == 3:  # Read Holding Registers
                result = self.client.read_holding_registers(address=start_addr, count=count, slave=device.unit_id)
            elif function_code == 4:  # Read Input Registers
                result = self.client.read_input_registers(address=start_addr, count=count, slave=device.unit_id)
            else:
                print(f"⚠️  Unsupported function code: {function_code}")
                return None
            
            if result.isError():
                print(f"❌ Modbus block read error FC{function_code}: {result}")
                return None
            
            # Map values to tags
            # values = []
            # for tag in tags:
            #     try:
            #         # Calculate offset in the read block
            #         offset = tag.address - min_addr
                    
            #         if function_code in [1, 2]:  # Coils/Discrete Inputs
            #             value = 1 if result.bits[offset] else 0
            #         else:  # Registers
            #             value = result.registers[offset]
                    
            #         values.append(value)
            #     except (IndexError, AttributeError) as e:
            #         print(f"⚠️  Error mapping tag {tag.name} at offset {offset}: {e}")
            #         values.append(None)
            # print(values)
            return result.registers if function_code in [3,4] else result.bits
            
        except Exception as e:
            print(f"❌ Block read error: {e}")
            return None
    

    
    def _emit_device_tags_to_webapp(self, tag_data_list, device):
        """Emit all tags for a device in one batch to webapp via SocketIO"""
        try:
            # Prepare data in format expected by frontend (matching detail.html)
            modbus_data = {
                "tags": tag_data_list,  # Send all tags in one batch
                "device_id": device.id,
                "device_name": device.name,
                "worker_id": self.worker_id,
                "worker_type": "tcp",
                "source": "tcp_worker",
                "ok": True,
                "status": "connected",
                "seq": int(time.time())  # Add sequence number for debugging
            }
            
            # Try direct SocketIO emission first
            if self.sio_client and self.sio_connected:
                try:
                    # For SimpleClient, emit to all connected clients
                    # The webapp will handle room filtering on the server side
                    self.sio_client.emit('modbus_update', modbus_data)
                    
                    if self.debug:
                        print(f"   📡 SocketIO emitted {len(tag_data_list)} tags from device {device.name}")
                    return
                    
                except Exception as sio_error:
                    print(f"⚠️ SocketIO emission failed: {sio_error}")
                    # Try to reconnect
                    self._reconnect_socketio()
            
            # Fallback to HTTP API if SocketIO fails
            print(f"⚠️ SocketIO not available, skipping emission for device {device.name}")
                
        except Exception as e:
            print(f"❌ Error emitting device tags to webapp: {e}")
    
    def _store_tag_value(self, tag_id, value, timestamp, device):
        """Store tag value to database and emit to webapp (DEPRECATED - use batch method)"""
        try:
            # 1. Store to database
            if self.db_manager:
                self.db_manager.update_tag_latest_value(tag_id, value, timestamp)
                if self.debug:
                    print(f"   💾 Stored tag {tag_id}: {value}")
            
            # 2. Emit to webapp via HTTP API (SocketIO endpoint) - DEPRECATED
            # Now using batch emission in _emit_device_tags_to_webapp
            
        except Exception as e:
            print(f"❌ Error storing tag value {tag_id}: {e}")
    
    def _emit_to_webapp(self, tag_id, value, timestamp, device):
        """Emit tag update directly to webapp via SocketIO (DEPRECATED - use batch method)"""
        print("⚠️ _emit_to_webapp is deprecated, use _emit_device_tags_to_webapp instead")
   