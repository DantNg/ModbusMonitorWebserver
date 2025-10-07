"""
Process Manager - Quản lý các worker processes cho từng device/group và service workers
"""
import logging
import time
from multiprocessing import Process, Queue, Manager
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class WorkerCommand(Enum):
    """Commands sent to worker processes"""
    START = "start"
    STOP = "stop"
    UPDATE_CONFIG = "update_config"
    ENABLE_EMIT = "enable_emit"
    DISABLE_EMIT = "disable_emit"
    JOIN_ROOM = "join_room"
    LEAVE_ROOM = "leave_room"
    RELOAD_CONFIG = "reload_config"

@dataclass
class WorkerConfig:
    """Configuration for a worker process"""
    worker_id: str
    protocol: str  # TCP, RTU, ALARM, DATALOGGER
    host: str = None
    port: int = None
    serial_port: str = None
    baudrate: int = 9600
    devices: List[dict] = None
    tags: List[dict] = None
    polling_interval: float = 1.0
    byte_order: str = "BigEndian"
    word_order: str = "AB"
    
    # Service worker specific configs
    check_interval: float = 1.0  # For service workers
    enable_notifications: bool = True  # For alarm worker

class ProcessManager:
    """Manages all worker processes (Modbus + Service workers)"""
    
    def __init__(self, flask_app=None):
        self.flask_app = flask_app
        
        # All workers (Modbus + Service)
        self.workers: Dict[str, Process] = {}
        self.worker_configs: Dict[str, WorkerConfig] = {}
        self.data_queues: Dict[str, Queue] = {}
        self.command_queues: Dict[str, Queue] = {}
        self.log_queue = Queue()
        
        # Shared state between processes
        self.manager = Manager()
        self.shared_state = self.manager.dict()
        self.room_mappings = self.manager.dict()  # room_id -> {worker_id: [tag_ids]}
        self.active_rooms = self.manager.dict()   # worker_id -> set of room_ids
        
        self.running = False
        
        # Auto-create service workers on startup
        self._create_service_workers()
        
    def _create_service_workers(self):
        """Auto-create alarm and datalogger workers"""
        try:
            # Create alarm worker
            alarm_config = {
                'worker_id': 'alarm_worker',
                'worker_type': 'ALARM',
                'check_interval': 0.5,
                'enable_notifications': True
            }
            self.create_worker(alarm_config)
            logger.info("Auto-created alarm worker")
            
            # Create datalogger worker
            datalogger_config = {
                'worker_id': 'datalogger_worker', 
                'worker_type': 'DATALOGGER',
                'check_interval': 1.0,
                'buffer_size': 1000,
                'batch_size': 100
            }
            self.create_worker(datalogger_config)
            logger.info("Auto-created datalogger worker")
            
        except Exception as e:
            logger.error(f"Failed to create service workers: {e}")
    
    def start(self):
        """Start the process manager"""
        self.running = True
        logger.info("Process Manager started")
        
    def stop(self):
        """Stop all worker processes"""
        self.running = False
        
        # Send stop commands to all workers
        for worker_id in list(self.workers.keys()):
            self.stop_worker(worker_id)
            
        logger.info("Process Manager stopped")
        
    def create_worker(self, config_dict: dict) -> bool:
        """Create and start a new worker process (Modbus or Service)"""
        try:
            worker_id = config_dict.get('worker_id')
            if not worker_id:
                logger.error("Worker ID is required")
                return False
                
            if worker_id in self.workers:
                logger.warning(f"Worker {worker_id} already exists")
                return False
                
            # Convert dict to WorkerConfig
            config = WorkerConfig(
                worker_id=worker_id,
                protocol=config_dict.get('worker_type', 'tcp').upper(),
                host=config_dict.get('host'),
                port=config_dict.get('port'),
                serial_port=config_dict.get('serial_port'),
                baudrate=config_dict.get('baudrate', 9600),
                devices=config_dict.get('devices', []),
                tags=config_dict.get('tags', []),
                polling_interval=config_dict.get('polling_interval', 1.0),
                byte_order=config_dict.get('byte_order', 'BigEndian'),
                word_order=config_dict.get('word_order', 'AB'),
                check_interval=config_dict.get('check_interval', 1.0),
                enable_notifications=config_dict.get('enable_notifications', True)
            )
                
            # Create queues for this worker
            data_queue = Queue()
            command_queue = Queue()
            
            # Store references
            self.worker_configs[worker_id] = config
            self.data_queues[worker_id] = data_queue
            self.command_queues[worker_id] = command_queue
            self.active_rooms[worker_id] = set()
            
            # Import appropriate worker function
            if config.protocol == "TCP":
                from .modbus_tcp_worker import modbus_tcp_worker
                worker_func = modbus_tcp_worker
            elif config.protocol == "RTU":
                from .modbus_rtu_worker import modbus_rtu_worker
                worker_func = modbus_rtu_worker
            elif config.protocol == "ALARM":
                from .alarm_worker import create_alarm_worker_process, AlarmConfig
                alarm_config = AlarmConfig(
                    check_interval=config.check_interval,
                    enable_notifications=config.enable_notifications
                )
                process = create_alarm_worker_process(
                    alarm_config, data_queue, command_queue, self.log_queue, self.shared_state
                )
                process.start()
                self.workers[worker_id] = process
                logger.info(f"Alarm worker {worker_id} created and started")
                return True
            elif config.protocol == "DATALOGGER":
                from .datalogger_worker import create_datalogger_worker_process, DataLoggerConfig
                datalogger_config = DataLoggerConfig(
                    check_interval=config.check_interval,
                    buffer_size=config_dict.get('buffer_size', 1000),
                    batch_size=config_dict.get('batch_size', 100)
                )
                process = create_datalogger_worker_process(
                    datalogger_config, data_queue, command_queue, self.log_queue, self.shared_state
                )
                process.start()
                self.workers[worker_id] = process
                logger.info(f"DataLogger worker {worker_id} created and started")
                return True
            else:
                logger.error(f"Unknown worker type: {config.protocol}")
                return False
                
            # Create and start Modbus process
            process = Process(
                target=worker_func,
                args=(config, data_queue, command_queue, self.log_queue, self.shared_state),
                name=f"worker-{worker_id}"
            )
            process.start()
            
            self.workers[worker_id] = process
            
            logger.info(f"Created and started worker {worker_id} (PID: {process.pid})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create worker: {e}")
            return False
            
        except Exception as e:
            logger.error(f"Failed to create worker {config.worker_id}: {e}")
            return False
            
    def stop_worker(self, worker_id: str) -> bool:
        """Stop a specific worker process"""
        try:
            if worker_id not in self.workers:
                logger.warning(f"Worker {worker_id} not found")
                return False
                
            # Send stop command
            self.send_command(worker_id, WorkerCommand.STOP)
            
            # Wait for process to terminate
            process = self.workers[worker_id]
            process.join(timeout=5.0)
            
            if process.is_alive():
                logger.warning(f"Worker {worker_id} didn't stop gracefully, terminating")
                process.terminate()
                process.join(timeout=2.0)
                
            if process.is_alive():
                logger.error(f"Worker {worker_id} couldn't be stopped, killing")
                process.kill()
                
            # Clean up
            self.cleanup_worker(worker_id)
            
            logger.info(f"Stopped worker {worker_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to stop worker {worker_id}: {e}")
            return False
            
    def cleanup_worker(self, worker_id: str):
        """Clean up worker resources"""
        self.workers.pop(worker_id, None)
        self.worker_configs.pop(worker_id, None)
        self.data_queues.pop(worker_id, None)
        self.command_queues.pop(worker_id, None)
        self.active_rooms.pop(worker_id, None)
        
    def send_command(self, worker_id: str, command: WorkerCommand, data: dict = None):
        """Send command to specific worker"""
        try:
            if worker_id not in self.command_queues:
                logger.warning(f"No command queue for worker {worker_id}")
                return False
                
            cmd_data = {
                "command": command.value,
                "timestamp": time.time(),
                "data": data or {}
            }
            
            self.command_queues[worker_id].put(cmd_data)
            logger.debug(f"Sent command {command.value} to worker {worker_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send command to worker {worker_id}: {e}")
            return False
            
    def update_worker_config(self, worker_id: str, new_config: WorkerConfig):
        """Update configuration for a worker"""
        try:
            if worker_id not in self.workers:
                logger.warning(f"Worker {worker_id} not found for config update")
                return False
                
            # Update stored config
            self.worker_configs[worker_id] = new_config
            
            # Send update command to worker
            config_dict = {
                "protocol": new_config.protocol,
                "host": new_config.host,
                "port": new_config.port,
                "serial_port": new_config.serial_port,
                "baudrate": new_config.baudrate,
                "devices": new_config.devices,
                "tags": new_config.tags,
                "polling_interval": new_config.polling_interval,
                "byte_order": new_config.byte_order,
                "word_order": new_config.word_order
            }
            
            return self.send_command(worker_id, WorkerCommand.UPDATE_CONFIG, config_dict)
            
        except Exception as e:
            logger.error(f"Failed to update config for worker {worker_id}: {e}")
            return False
            
    def join_room(self, room_id: str, worker_tag_mapping: Dict[str, List[int]]):
        """Handle user joining a subdashboard room"""
        try:
            # Store room mapping
            self.room_mappings[room_id] = worker_tag_mapping
            
            # Send join_room commands to relevant workers
            for worker_id, tag_ids in worker_tag_mapping.items():
                if worker_id in self.active_rooms:
                    self.active_rooms[worker_id].add(room_id)
                    
                    self.send_command(worker_id, WorkerCommand.JOIN_ROOM, {
                        "room_id": room_id,
                        "tag_ids": tag_ids
                    })
                    
            logger.info(f"User joined room {room_id}, notified workers: {list(worker_tag_mapping.keys())}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to handle join room {room_id}: {e}")
            return False
            
    def leave_room(self, room_id: str):
        """Handle user leaving a subdashboard room"""
        try:
            if room_id not in self.room_mappings:
                logger.warning(f"Room {room_id} not found in mappings")
                return False
                
            worker_tag_mapping = self.room_mappings[room_id]
            
            # Send leave_room commands to relevant workers
            for worker_id, tag_ids in worker_tag_mapping.items():
                if worker_id in self.active_rooms:
                    if room_id in self.active_rooms[worker_id]:
                        self.active_rooms[worker_id].remove(room_id)
                        
                    self.send_command(worker_id, WorkerCommand.LEAVE_ROOM, {
                        "room_id": room_id,
                        "tag_ids": tag_ids
                    })
                    
            # Remove room mapping
            del self.room_mappings[room_id]
            
            logger.info(f"User left room {room_id}, notified workers: {list(worker_tag_mapping.keys())}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to handle leave room {room_id}: {e}")
            return False
            
    def get_worker_data(self, worker_id: str, timeout: float = 0.1) -> Optional[dict]:
        """Get data from specific worker (non-blocking)"""
        try:
            if worker_id not in self.data_queues:
                return None
                
            queue = self.data_queues[worker_id]
            if queue.empty():
                return None
                
            return queue.get(block=False)
            
        except Exception:
            return None
            
    def get_all_worker_data(self, timeout: float = 0.1) -> Dict[str, dict]:
        """Get data from all workers"""
        data = {}
        for worker_id in self.workers.keys():
            worker_data = self.get_worker_data(worker_id, timeout)
            if worker_data:
                data[worker_id] = worker_data
        return data
        
    def get_log_messages(self) -> List[dict]:
        """Get log messages from workers"""
        messages = []
        while not self.log_queue.empty():
            try:
                messages.append(self.log_queue.get(block=False))
            except:
                break
        return messages
        
    def get_worker_status(self) -> List[dict]:
        """Get status of all workers"""
        status = []
        for worker_id, process in self.workers.items():
            config = self.worker_configs.get(worker_id)
            status.append({
                "worker_id": worker_id,
                "worker_type": config.protocol.lower() if config else "unknown",
                "status": "running" if process.is_alive() else "stopped",
                "pid": process.pid if process.is_alive() else None,
                "config": {
                    "host": config.host if config else None,
                    "port": config.port if config else None,
                    "serial_port": config.serial_port if config else None,
                    "baudrate": config.baudrate if config else None,
                    "devices": config.devices if config else [],
                    "tags": config.tags if config else []
                },
                "active_rooms": list(self.active_rooms.get(worker_id, set()))
            })
        return status
        
    def handle_room_join(self, room_id: str, tag_ids: List[int]):
        """Handle user joining a room - optimized version"""
        try:
            # Find which workers have the requested tags
            worker_tag_mapping = {}
            
            for worker_id, config in self.worker_configs.items():
                if not config.tags:
                    continue
                    
                # Find tags that belong to this worker
                worker_tag_ids = []
                for tag in config.tags:
                    if tag.get('id') in tag_ids:
                        worker_tag_ids.append(tag['id'])
                        
                if worker_tag_ids:
                    worker_tag_mapping[worker_id] = worker_tag_ids
                    
            if worker_tag_mapping:
                return self.join_room(room_id, worker_tag_mapping)
            return True
            
        except Exception as e:
            logger.error(f"Failed to handle room join: {e}")
            return False
            
    def handle_room_leave(self, room_id: str):
        """Handle user leaving a room"""
        return self.leave_room(room_id)
        
    def get_data(self, timeout: float = 1.0) -> Optional[dict]:
        """Get data from any worker - used by background thread"""
        for worker_id in self.workers.keys():
            data = self.get_worker_data(worker_id, timeout=0.01)
            if data:
                return data
        return None
        
    def stop_all_workers(self):
        """Stop all workers"""
        for worker_id in list(self.workers.keys()):
            self.stop_worker(worker_id)
            
    def get_worker_logs(self, worker_id: str, limit: int = 50) -> List[dict]:
        """Get recent logs for a specific worker"""
        # Get all log messages and filter by worker_id
        all_logs = self.get_log_messages()
        worker_logs = [log for log in all_logs if log.get('worker_id') == worker_id]
        return worker_logs[-limit:] if worker_logs else []
        
    def restart_worker(self, worker_id: str) -> bool:
        """Restart a worker with its existing configuration"""
        try:
            if worker_id not in self.worker_configs:
                return False
                
            # Stop the worker
            self.stop_worker(worker_id)
            time.sleep(1)  # Give time for cleanup
            
            # Recreate with existing config
            config = self.worker_configs[worker_id]
            return self.create_worker(config.__dict__)
            
        except Exception as e:
            logger.error(f"Failed to restart worker {worker_id}: {e}")
            return False
    
    def shutdown(self):
        """Shutdown ProcessManager and cleanup all resources"""
        try:
            logger.info("Shutting down ProcessManager...")
            
            # Stop all workers
            self.stop()
            
            # Wait for processes to finish
            for worker_id, process in self.workers.items():
                if process.is_alive():
                    process.join(timeout=5)
                    if process.is_alive():
                        logger.warning(f"Force terminating worker {worker_id}")
                        process.terminate()
            
            # Cleanup manager
            if hasattr(self, 'manager'):
                self.manager.shutdown()
            
            logger.info("ProcessManager shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during ProcessManager shutdown: {e}")
    
    def shutdown(self):
        """Shutdown all workers and cleanup resources"""
        logger.info("Shutting down ProcessManager...")
        
        # Stop all workers
        for worker_id in list(self.workers.keys()):
            self.stop_worker(worker_id)
        
        # Wait for workers to terminate
        for worker_id, process in self.workers.items():
            if process.is_alive():
                process.join(timeout=5)
                if process.is_alive():
                    logger.warning(f"Force terminating worker {worker_id}")
                    process.terminate()
        
        # Cleanup manager
        try:
            self.manager.shutdown()
        except:
            pass
        
        logger.info("ProcessManager shutdown complete")