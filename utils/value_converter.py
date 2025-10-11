"""
Value Converter Utilities for Modbus Data
Handles conversion between raw Modbus values and web-displayable values
Supports: signed, unsigned, hex, binary, float, double, long with big/little endian
"""
import struct
from typing import List, Union, Optional


def convert_raw_value_to_web(raw_value: List[int], datatype: str, byte_order: str = "BigEndian", word_order: str = "BA") -> Union[int, float, str, bool, None]:
    """
    Convert raw Modbus register values to web-displayable format
    
    Args:
        raw_value: List of raw register values from Modbus
        datatype: Data type (signed, unsigned, hex, binary, float, double, long, int16, uint16, int32, uint32, float32, float64)
        byte_order: "BigEndian" or "LittleEndian" 
        word_order: "AB" or "BA" for multi-register values
        
    Returns:
        Converted value suitable for web display
    """
    try:
        if not raw_value:
            return None
            
        datatype = datatype.lower()
        
        # Boolean/binary values
        if datatype in ['binary', 'bool', 'boolean']:
            return bool(raw_value[0]) if raw_value else False
            
        # Single register types
        elif datatype in ['int16', 'short', 'signed']:
            val = raw_value[0] if raw_value else 0
            # Convert to signed 16-bit
            return val if val < 32768 else val - 65536
            
        elif datatype in ['uint16', 'word', 'unsigned']:
            return raw_value[0] if raw_value else 0
            
        elif datatype == 'hex':
            return f"0x{raw_value[0]:04X}" if raw_value else "0x0000"
            
        # Two register types (32-bit)
        elif datatype in ['float', 'float32', 'real']:
            if len(raw_value) >= 2:
                # IEEE 754 float from 2 registers
                w1, w2 = (raw_value[1], raw_value[0]) if word_order == "BA" else (raw_value[0], raw_value[1])
                b1 = w1.to_bytes(2, "big")
                b2 = w2.to_bytes(2, "big")
                b = b1 + b2
                if byte_order == "LittleEndian":
                    b = b[1:2] + b[0:1] + b[3:4] + b[2:3]
                return struct.unpack(">f", b)[0]
            return 0.0
            
        elif datatype in ['int32', 'long', 'signed32']:
            if len(raw_value) >= 2:
                if word_order == "BA":
                    val = (raw_value[1] << 16) | raw_value[0]
                else:
                    val = (raw_value[0] << 16) | raw_value[1]
                # Convert to signed 32-bit
                return val if val < 2147483648 else val - 4294967296
            return 0
            
        elif datatype in ['uint32', 'dword', 'unsigned32']:
            if len(raw_value) >= 2:
                if word_order == "BA":
                    return (raw_value[1] << 16) | raw_value[0]
                else:
                    return (raw_value[0] << 16) | raw_value[1]
            return 0
            
        # Four register types (64-bit)
        elif datatype in ['double', 'float64']:
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
            return 0.0
            
        # Default fallback
        return raw_value[0] if raw_value else 0
        
    except Exception as e:
        print(f"Error converting raw value {raw_value} as {datatype}: {e}")
        return None


def convert_web_value_to_raw(value: Union[int, float, str, bool], datatype: str, byte_order: str = "BigEndian", word_order: str = "AB") -> List[int]:
    """
    Convert web input value to raw Modbus register format
    
    Args:
        value: Input value from web interface
        datatype: Data type (signed, unsigned, hex, binary, float, double, long, int16, uint16, int32, uint32, float32, float64)
        byte_order: "BigEndian" or "LittleEndian"
        word_order: "AB" or "BA" for multi-register values
        
    Returns:
        List of register values for Modbus writing
    """
    try:
        if value is None:
            return [0]
            
        datatype = datatype.lower()
        
        # Boolean/binary values
        if datatype in ['binary', 'bool', 'boolean']:
            return [1 if bool(value) else 0]
            
        # Parse numeric value from string if needed
        if isinstance(value, str):
            if datatype == 'hex':
                # Parse hex string
                value = value.replace('0x', '').replace('0X', '')
                value = int(value, 16)
            elif datatype in ['float', 'float32', 'real', 'double', 'float64']:
                value = float(value)
            else:
                value = int(value)
        
        # Single register types (16-bit)
        if datatype in ['int16', 'short', 'signed']:
            # Convert to 16-bit signed, then to unsigned for Modbus
            val = int(value)
            val = max(-32768, min(32767, val))  # Clamp to 16-bit signed range
            return [val if val >= 0 else val + 65536]
            
        elif datatype in ['uint16', 'word', 'unsigned', 'hex']:
            # Convert to 16-bit unsigned
            val = int(value)
            val = max(0, min(65535, val))  # Clamp to 16-bit unsigned range
            return [val]
            
        # Two register types (32-bit)
        elif datatype in ['float', 'float32', 'real']:
            # Convert float to IEEE 754 format
            packed = struct.pack(">f", float(value))
            if byte_order == "LittleEndian":
                # Swap bytes within each word
                packed = packed[1:2] + packed[0:1] + packed[3:4] + packed[2:3]
            
            # Extract 2 registers
            w1 = struct.unpack(">H", packed[:2])[0]
            w2 = struct.unpack(">H", packed[2:])[0]
            
            if word_order == "BA":
                return [w2, w1]
            else:
                return [w1, w2]
                
        elif datatype in ['int32', 'long', 'signed32']:
            # Convert to 32-bit signed
            val = int(value)
            val = max(-2147483648, min(2147483647, val))  # Clamp to 32-bit signed range
            
            # Convert to unsigned for Modbus
            if val < 0:
                val += 4294967296
                
            # Split into 2 registers
            high = (val >> 16) & 0xFFFF
            low = val & 0xFFFF
            
            if word_order == "BA":
                return [low, high]
            else:
                return [high, low]
                
        elif datatype in ['uint32', 'dword', 'unsigned32']:
            # Convert to 32-bit unsigned
            val = int(value)
            val = max(0, min(4294967295, val))  # Clamp to 32-bit unsigned range
            
            # Split into 2 registers
            high = (val >> 16) & 0xFFFF
            low = val & 0xFFFF
            
            if word_order == "BA":
                return [low, high]
            else:
                return [high, low]
                
        # Four register types (64-bit)
        elif datatype in ['double', 'float64']:
            # Convert double to IEEE 754 format
            packed = struct.pack(">d", float(value))
            if byte_order == "LittleEndian":
                # Swap bytes within each word
                new_packed = b''
                for i in range(0, len(packed), 2):
                    new_packed += packed[i+1:i+2] + packed[i:i+1]
                packed = new_packed
            
            # Extract 4 registers
            regs = []
            for i in range(0, 8, 2):
                reg = struct.unpack(">H", packed[i:i+2])[0]
                regs.append(reg)
            
            if word_order == "BA":
                return [regs[3], regs[2], regs[1], regs[0]]
            else:
                return regs
                
        # Default fallback - treat as unsigned 16-bit
        val = int(value)
        val = max(0, min(65535, val))
        return [val]
        
    except Exception as e:
        print(f"Error converting web value {value} as {datatype}: {e}")
        return [0]


def get_register_count(datatype: str) -> int:
    """
    Get number of registers required for a given datatype
    
    Args:
        datatype: Data type string
        
    Returns:
        Number of 16-bit registers needed
    """
    datatype = datatype.lower()
    
    if datatype in ['float', 'float32', 'real', 'int32', 'uint32', 'long', 'dword', 'signed32', 'unsigned32']:
        return 2
    elif datatype in ['double', 'float64']:
        return 4
    else:
        return 1


def format_value_for_display(value: Union[int, float, str, bool], datatype: str) -> str:
    """
    Format a value for display in the web interface
    
    Args:
        value: The value to format
        datatype: Data type for formatting
        
    Returns:
        Formatted string for display
    """
    try:
        if value is None:
            return "N/A"
            
        datatype = datatype.lower()
        
        if datatype in ['binary', 'bool', 'boolean']:
            return "TRUE" if bool(value) else "FALSE"
        elif datatype == 'hex':
            if isinstance(value, str):
                return value
            return f"0x{int(value):04X}"
        elif datatype in ['float', 'float32', 'real']:
            return f"{float(value):.3f}"
        elif datatype in ['double', 'float64']:
            return f"{float(value):.6f}"
        else:
            return str(value)
            
    except Exception:
        return str(value)