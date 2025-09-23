#!/usr/bin/env python3
"""
Complete Modbus Datatype Test Tool
Đọc RTU và parse tất cả các kiểu dữ liệu, in ra kết quả để so sánh
"""
import struct
import time
from pymodbus.client import ModbusSerialClient
from pymodbus.exceptions import ModbusException, ConnectionException, ModbusIOException

def parse_all_datatypes(registers):
    """Parse registers với tất cả các kiểu dữ liệu và trả về kết quả"""
    
    results = {}
    
    # Kiểm tra số lượng registers
    reg_count = len(registers)
    print(f"📊 Available registers: {reg_count}")
    print(f"📊 Raw registers: {registers}")
    print(f"📊 Hex format: {[f'0x{r:04X}' for r in registers]}")
    print(f"📊 Binary format: {[f'{r:016b}' for r in registers]}")
    print()
    
    # ===== 16-BIT DATATYPES (1 register) =====
    if reg_count >= 1:
        reg = registers[0]
        print("=" * 60)
        print("16-BIT DATATYPES (1 register)")
        print("=" * 60)
        
        # Unsigned 16-bit
        results['unsigned'] = reg
        print(f"UNSIGNED     : {results['unsigned']}")
        
        # Signed 16-bit
        results['signed'] = reg if reg <= 32767 else reg - 65536
        print(f"SIGNED       : {results['signed']}")
        
        # Hex representation
        results['hex'] = f"0x{reg:04X}"
        print(f"HEX          : {results['hex']}")
        
        # Binary representation  
        results['binary'] = f"0b{reg:016b}"
        print(f"BINARY       : {results['binary']}")
        
        # Boolean (0 = False, non-zero = True)
        results['boolean'] = bool(reg)
        print(f"BOOLEAN      : {results['boolean']}")
        print()
    
    # ===== 32-BIT DATATYPES (2 registers) =====
    if reg_count >= 2:
        reg1, reg2 = registers[0], registers[1]
        print("=" * 60)
        print("32-BIT DATATYPES (2 registers)")
        print("=" * 60)
        print(f"Using registers: [0x{reg1:04X}, 0x{reg2:04X}]")
        print()
        
        # === FLOAT 32-BIT ===
        print("--- FLOAT 32-BIT ---")
        
        # Float AB order (reg1=high, reg2=low)
        try:
            b1 = reg1.to_bytes(2, "big")
            b2 = reg2.to_bytes(2, "big")
            b = b1 + b2
            results['float_AB'] = struct.unpack(">f", b)[0]
            print(f"FLOAT (AB)   : {results['float_AB']}")
        except:
            results['float_AB'] = "ERROR"
            print(f"FLOAT (AB)   : ERROR")
        
        # Float BA order (reg2=high, reg1=low)
        try:
            b1 = reg2.to_bytes(2, "big")
            b2 = reg1.to_bytes(2, "big")
            b = b1 + b2
            results['float_BA'] = struct.unpack(">f", b)[0]
            print(f"FLOAT (BA)   : {results['float_BA']}")
        except:
            results['float_BA'] = "ERROR"
            print(f"FLOAT (BA)   : ERROR")
        
        print()
        
        # === INTEGER 32-BIT ===
        print("--- INTEGER 32-BIT ---")
        
        # Unsigned Long AB order
        val_ab = (reg1 << 16) | reg2
        results['ulong_AB'] = val_ab
        print(f"ULONG (AB)   : {results['ulong_AB']}")
        
        # Unsigned Long BA order
        val_ba = (reg2 << 16) | reg1
        results['ulong_BA'] = val_ba
        print(f"ULONG (BA)   : {results['ulong_BA']}")
        
        # Signed Long AB order
        results['long_AB'] = val_ab if val_ab <= 2147483647 else val_ab - 4294967296
        print(f"LONG (AB)    : {results['long_AB']}")
        
        # Signed Long BA order
        results['long_BA'] = val_ba if val_ba <= 2147483647 else val_ba - 4294967296
        print(f"LONG (BA)    : {results['long_BA']}")
        
        print()
    
    # ===== 64-BIT DATATYPES (4 registers) =====
    if reg_count >= 4:
        reg1, reg2, reg3, reg4 = registers[0], registers[1], registers[2], registers[3]
        print("=" * 60)
        print("64-BIT DATATYPES (4 registers)")
        print("=" * 60)
        print(f"Using registers: [0x{reg1:04X}, 0x{reg2:04X}, 0x{reg3:04X}, 0x{reg4:04X}]")
        print()
        
        # === DOUBLE 64-BIT ===
        print("--- DOUBLE 64-BIT ---")
        
        # Double ABCD order
        try:
            bytes_data = b''
            for reg in [reg1, reg2, reg3, reg4]:
                bytes_data += reg.to_bytes(2, "big")
            results['double_ABCD'] = struct.unpack(">d", bytes_data)[0]
            print(f"DOUBLE (ABCD): {results['double_ABCD']}")
        except:
            results['double_ABCD'] = "ERROR"
            print(f"DOUBLE (ABCD): ERROR")
        
        # Double DCBA order
        try:
            bytes_data = b''
            for reg in [reg4, reg3, reg2, reg1]:
                bytes_data += reg.to_bytes(2, "big")
            results['double_DCBA'] = struct.unpack(">d", bytes_data)[0]
            print(f"DOUBLE (DCBA): {results['double_DCBA']}")
        except:
            results['double_DCBA'] = "ERROR"
            print(f"DOUBLE (DCBA): ERROR")
        
        # Double BADC order
        try:
            bytes_data = b''
            for reg in [reg2, reg1, reg4, reg3]:
                bytes_data += reg.to_bytes(2, "big")
            results['double_BADC'] = struct.unpack(">d", bytes_data)[0]
            print(f"DOUBLE (BADC): {results['double_BADC']}")
        except:
            results['double_BADC'] = "ERROR"
            print(f"DOUBLE (BADC): ERROR")
        
        # Double CDAB order
        try:
            bytes_data = b''
            for reg in [reg3, reg4, reg1, reg2]:
                bytes_data += reg.to_bytes(2, "big")
            results['double_CDAB'] = struct.unpack(">d", bytes_data)[0]
            print(f"DOUBLE (CDAB): {results['double_CDAB']}")
        except:
            results['double_CDAB'] = "ERROR"
            print(f"DOUBLE (CDAB): ERROR")
        
        print()
        
        # === LONG LONG 64-BIT ===
        print("--- LONG LONG 64-BIT ---")
        
        # Unsigned Long Long ABCD
        val_abcd = (reg1 << 48) | (reg2 << 32) | (reg3 << 16) | reg4
        results['ulonglong_ABCD'] = val_abcd
        print(f"ULONGLONG (ABCD): {results['ulonglong_ABCD']}")
        
        # Unsigned Long Long DCBA
        val_dcba = (reg4 << 48) | (reg3 << 32) | (reg2 << 16) | reg1
        results['ulonglong_DCBA'] = val_dcba
        print(f"ULONGLONG (DCBA): {results['ulonglong_DCBA']}")
        
        # Signed Long Long ABCD
        results['longlong_ABCD'] = val_abcd if val_abcd <= 9223372036854775807 else val_abcd - 18446744073709551616
        print(f"LONGLONG (ABCD) : {results['longlong_ABCD']}")
        
        # Signed Long Long DCBA
        results['longlong_DCBA'] = val_dcba if val_dcba <= 9223372036854775807 else val_dcba - 18446744073709551616
        print(f"LONGLONG (DCBA) : {results['longlong_DCBA']}")
        
        print()
    
    return results

def test_modbus_reading():
    """Test đọc Modbus và parse tất cả datatypes"""
    
    print("🔌 Complete Modbus Datatype Test")
    print("=" * 80)
    
    # Cấu hình RTU
    config = {
        'port': 'COM2',
        'baudrate': 9600,
        'bytesize': 8,
        'parity': 'N',
        'stopbits': 1,
        'timeout': 2.0
    }
    
    print("📋 RTU Configuration:")
    for key, value in config.items():
        print(f"   {key}: {value}")
    print()
    
    # Nhập thông số từ user
    try:
        slave_id = int(input("Enter Slave ID (default 1): ") or "1")
        start_address = int(input("Enter Start Address (default 0): ") or "0")
        register_count = int(input("Enter Register Count (1-10, default 4): ") or "4")
        
        if register_count < 1 or register_count > 10:
            register_count = 4
            
    except ValueError:
        print("⚠️  Using default values: Slave=1, Address=0, Count=4")
        slave_id = 1
        start_address = 0
        register_count = 4
    
    print()
    print(f"📖 Reading {register_count} registers from address {start_address}, slave {slave_id}")
    print()
    
    try:
        # Kết nối RTU
        print("🔌 Connecting to RTU...")
        client = ModbusSerialClient(**config)
        
        if not client.connect():
            print("❌ Could not connect to RTU device")
            print("   Check:")
            print("   - COM port is correct and available")
            print("   - Device is powered on and connected")
            print("   - Baudrate and parity settings match device")
            return
            
        print("✅ RTU connection successful")
        print()
        
        # Đọc registers
        print(f"📖 Reading registers...")
        result = client.read_holding_registers(
            address=start_address,
            count=register_count,
            slave=slave_id
        )
        
        if result.isError():
            print(f"❌ Modbus read error: {result}")
            print("   Check:")
            print("   - Slave ID is correct")
            print("   - Register address exists")
            print("   - Register count is valid")
            return
            
        # Parse tất cả datatypes
        registers = result.registers
        print("✅ Read successful!")
        print()
        
        print("📊 PARSED RESULTS:")
        print("=" * 80)
        results = parse_all_datatypes(registers)
        
        print("=" * 80)
        print("📝 SUMMARY - Values to compare with your expected values:")
        print("=" * 80)
        
        # Tóm tắt các giá trị quan trọng
        summary_items = [
            ('16-bit Unsigned', results.get('unsigned', 'N/A')),
            ('16-bit Signed', results.get('signed', 'N/A')),
            ('Hex', results.get('hex', 'N/A')),
            ('Binary', results.get('binary', 'N/A')),
        ]
        
        if 'float_AB' in results:
            summary_items.extend([
                ('Float (AB order)', results.get('float_AB', 'N/A')),
                ('Float (BA order)', results.get('float_BA', 'N/A')),
                ('Long (AB order)', results.get('long_AB', 'N/A')),
                ('Long (BA order)', results.get('long_BA', 'N/A')),
                ('ULong (AB order)', results.get('ulong_AB', 'N/A')),
                ('ULong (BA order)', results.get('ulong_BA', 'N/A')),
            ])
        
        if 'double_ABCD' in results:
            summary_items.extend([
                ('Double (ABCD)', results.get('double_ABCD', 'N/A')),
                ('Double (DCBA)', results.get('double_DCBA', 'N/A')),
                ('Double (BADC)', results.get('double_BADC', 'N/A')),
                ('Double (CDAB)', results.get('double_CDAB', 'N/A')),
            ])
        
        for name, value in summary_items:
            print(f"{name:20}: {value}")
        
        print()
        print("💡 Compare these values with your expected input!")
        print("💡 The datatype that matches your expected value is the correct one to use.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        try:
            client.close()
            print("\n🔌 RTU connection closed")
        except:
            pass

def test_manual_values():
    """Test với registers do user nhập thủ công"""
    
    print("\n🧪 Manual Register Test")
    print("=" * 80)
    
    try:
        # Nhập registers từ user
        print("Enter register values (hex format like 0x1234 or decimal like 4660):")
        registers = []
        
        for i in range(4):  # Tối đa 4 registers
            value_str = input(f"Register {i+1} (Enter to stop): ").strip()
            if not value_str:
                break
                
            # Parse hex hoặc decimal
            if value_str.startswith('0x') or value_str.startswith('0X'):
                value = int(value_str, 16)
            else:
                value = int(value_str)
                
            if 0 <= value <= 65535:
                registers.append(value)
            else:
                print(f"⚠️  Invalid register value: {value} (must be 0-65535)")
                break
        
        if registers:
            print(f"\n📊 Testing with {len(registers)} registers: {registers}")
            print()
            
            results = parse_all_datatypes(registers)
            
            print("=" * 80)
            print("📝 Use these results to compare with your expected values!")
            print("=" * 80)
        else:
            print("⚠️  No valid registers entered")
            
    except ValueError as e:
        print(f"❌ Invalid input: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("Complete Modbus Datatype Test Tool")
    print("=" * 80)
    print("This tool reads Modbus registers and parses them with ALL datatypes")
    print("Compare the results with your expected values to find the correct datatype")
    print()
    
    while True:
        print("Choose test mode:")
        print("1. Read from RTU device")
        print("2. Test with manual register values")
        print("3. Exit")
        
        choice = input("Enter choice (1/2/3): ").strip()
        
        if choice == "1":
            test_modbus_reading()
        elif choice == "2":
            test_manual_values()
        elif choice == "3":
            print("👋 Goodbye!")
            break
        else:
            print("⚠️  Invalid choice, please try again")
        
        print("\n" + "=" * 80 + "\n")