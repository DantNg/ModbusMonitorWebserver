import multiprocessing
import queue
import time
from modbus_monitor.services.datalogger_service import DataLoggerService
from modbus_monitor.services.common import LatestCache

class DataLoggerProcess(multiprocessing.Process):
    def __init__(self, command_queue, status_queue):
        super().__init__()
        self.command_queue = command_queue
        self.status_queue = status_queue
        self.datalogger = None
        self.cache = None
        self.running = False

    def run(self):
        while True:
            try:
                cmd, data = self.command_queue.get(timeout=1)
            except queue.Empty:
                continue
            if cmd == 'start':
                if not self.running:
                    self.cache = LatestCache()
                    self.datalogger = DataLoggerService(self.cache)
                    self.datalogger.start()
                    self.running = True
                    self.status_queue.put(('started', None))
            elif cmd == 'stop':
                if self.datalogger and self.running:
                    self.datalogger.stop()
                    self.running = False
                    self.status_queue.put(('stopped', None))
            elif cmd == 'update':
                if self.datalogger and self.running:
                    # Giả sử DataLoggerService có hàm reload_configs hoặc tương tự
                    try:
                        self.datalogger.reload_configs()
                        self.status_queue.put(('updated', None))
                    except Exception as e:
                        self.status_queue.put(('update_error', str(e)))
            elif cmd == 'exit':
                if self.datalogger and self.running:
                    self.datalogger.stop()
                self.status_queue.put(('exited', None))
                break
            else:
                self.status_queue.put(('unknown_cmd', cmd))
