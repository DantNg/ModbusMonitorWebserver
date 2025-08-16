from pymodbus.client import ModbusTcpClient, ModbusSerialClient
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder

def _end(val:str):
    return Endian.Big if (val or '').lower().startswith('big') else Endian.Little

def _client(dev):
    if dev.protocol == 'ModbusTCP':
        return ModbusTcpClient(host=dev.host, port=dev.port, timeout=dev.timeout_ms/1000.0)
    return ModbusSerialClient(method='rtu', port=dev.serial_port, baudrate=dev.baudrate,
        parity=dev.parity, stopbits=dev.stopbits, bytesize=dev.bytesize, timeout=dev.timeout_ms/1000.0)

def read_modbus_value(tag):
    dev = tag.device
    if not dev: return None
    addr = max(0, tag.address-1)
    unit = dev.unit_id or 1
    client = _client(dev)
    if not client.connect():
        try: client.close()
        except: pass
        return None
    try:
        if tag.datatype == 'Bit':
            rr = client.read_coils(addr,1,unit=unit)
            return int(rr.bits[0]) if rr and not rr.isError() else None
        count = 1 if tag.datatype=='Word' else 2
        rr = client.read_holding_registers(addr,count=count,unit=unit)
        if not rr or rr.isError(): return None
        dec = BinaryPayloadDecoder.fromRegisters(rr.registers, byteorder=_end(dev.byte_order), wordorder=_end(dev.byte_order))
        if tag.datatype=='Word': return dec.decode_16bit_uint()
        if tag.datatype=='DWord': return dec.decode_32bit_uint()
        if tag.datatype=='Float': return dec.decode_32bit_float()
    finally:
        try: client.close()
        except: pass
    return None
