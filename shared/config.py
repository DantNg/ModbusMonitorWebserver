"""
Configuration classes for devices and tags
"""

class DeviceConfig:
    def __init__(self, id, name, protocol, host=None, port=None, serial_port=None, 
                 baudrate=9600, bytesize=8, parity='N', stopbits=1, unit_id=1, 
                 timeout_ms=1000, read_interval_ms=1000, default_function_code=3,
                 byte_order='big', word_order='big'):
        self.id = id
        self.name = name
        self.protocol = protocol
        self.host = host
        self.port = port
        self.serial_port = serial_port
        self.baudrate = baudrate
        self.bytesize = bytesize
        self.parity = parity
        self.stopbits = stopbits
        self.unit_id = unit_id
        self.timeout_ms = timeout_ms
        self.read_interval_ms = read_interval_ms
        self.default_function_code = default_function_code
        self.byte_order = byte_order
        self.word_order = word_order
    
    def __repr__(self):
        return f"DeviceConfig(id={self.id}, name='{self.name}', protocol='{self.protocol}')"

class TagConfig:
    def __init__(self, id, device_id, name, address, function_code=3, data_type='int16',
                 scale=1.0, offset=0.0, description='', is_readonly=True):
        self.id = id
        self.device_id = device_id
        self.name = name
        self.address = address
        self.function_code = function_code
        self.data_type = data_type
        self.scale = scale
        self.offset = offset
        self.description = description
        self.is_readonly = is_readonly
    
    def __repr__(self):
        return f"TagConfig(id={self.id}, name='{self.name}', address={self.address})"