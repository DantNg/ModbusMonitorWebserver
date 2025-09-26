#!/usr/bin/env python3
"""
Test script to debug Float32 parsing for different endianness
"""
import struct

def test_parse_float32():
    # Test value: 1.55 (0x3FC66666 in IEEE754)
    expected_value = 1.55
    expected_hex = struct.pack('>f', expected_value)  # Big endian bytes
    print(f"Expected float: {expected_value}")
    print(f"Expected hex bytes: {[hex(b) for b in expected_hex]}")
    
    # Convert to 2 registers (16-bit each)
    # Big endian: high word first
    reg1_be = int.from_bytes(expected_hex[0:2], 'big')  # High word
    reg2_be = int.from_bytes(expected_hex[2:4], 'big')  # Low word
    print(f"BigEndian registers: reg1=0x{reg1_be:04X}, reg2=0x{reg2_be:04X}")
    
    # Little endian: low word first  
    reg1_le = int.from_bytes(expected_hex[2:4], 'big')  # Low word becomes reg1
    reg2_le = int.from_bytes(expected_hex[0:2], 'big')  # High word becomes reg2
    print(f"LittleEndian registers: reg1=0x{reg1_le:04X}, reg2=0x{reg2_le:04X}")
    
    print("\n--- Testing different combinations ---")
    
    # Test BigEndian device with BA word order (should work)
    test_case_1(reg1_be, reg2_be, "BA", "BigEndian", "BigEndian + BA")
    
    # Test LittleEndian device with AB word order (should work)
    test_case_1(reg1_le, reg2_le, "AB", "LittleEndian", "LittleEndian + AB")

def test_case_1(reg1, reg2, word_order, byte_order, desc):
    print(f"\n{desc}: reg1=0x{reg1:04X}, reg2=0x{reg2:04X}, word_order={word_order}, byte_order={byte_order}")
    
    if word_order == "AB":
        w1, w2 = reg1, reg2
    else:
        w1, w2 = reg2, reg1
    
    if byte_order == "BigEndian":
        b1 = w1.to_bytes(2, "big")
        b2 = w2.to_bytes(2, "big") 
        b = b1 + b2
        result = struct.unpack(">f", b)[0]
    else:
        b1 = w1.to_bytes(2, "little")
        b2 = w2.to_bytes(2, "little")
        b = b1 + b2
        result = struct.unpack("<f", b)[0]
    
    print(f"  Result: {result}")
    print(f"  Bytes: {[hex(x) for x in b]}")

if __name__ == "__main__":
    test_parse_float32()