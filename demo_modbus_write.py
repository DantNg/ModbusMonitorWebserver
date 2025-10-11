"""
Demonstration of how to use convert_web_value_to_raw for writing values to Modbus devices
"""
import sys
import os
sys.path.append(os.path.dirname(__file__))
from utils.value_converter import convert_web_value_to_raw, convert_raw_value_to_web, format_value_for_display

def demonstrate_modbus_write_conversion():
    """Demonstrate converting web values to raw Modbus register format for writing"""
    print("=== Modbus Write Value Conversion Demo ===\n")
    
    # Example scenarios where user wants to write values to Modbus devices
    write_scenarios = [
        # (web_value, datatype, byte_order, word_order, description)
        (12345, 'int16', 'BigEndian', 'AB', 'Write 16-bit signed integer'),
        (-1000, 'int16', 'BigEndian', 'AB', 'Write negative 16-bit signed integer'),
        (50000, 'uint16', 'BigEndian', 'AB', 'Write 16-bit unsigned integer'),
        ('0xABCD', 'hex', 'BigEndian', 'AB', 'Write hex value from web form'),
        (True, 'binary', 'BigEndian', 'AB', 'Write boolean TRUE'),
        (False, 'binary', 'BigEndian', 'AB', 'Write boolean FALSE'),
        (3.14159, 'float32', 'BigEndian', 'AB', 'Write 32-bit float'),
        (1000000, 'int32', 'BigEndian', 'AB', 'Write 32-bit signed integer'),
        (4294967295, 'uint32', 'BigEndian', 'AB', 'Write 32-bit unsigned max'),
        (2.718281828, 'double', 'BigEndian', 'AB', 'Write 64-bit double'),
        
        # Test different byte/word orders
        (3.14159, 'float32', 'LittleEndian', 'AB', 'Float with little endian'),
        (3.14159, 'float32', 'BigEndian', 'BA', 'Float with BA word order'),
        (1000000, 'int32', 'BigEndian', 'BA', '32-bit int with BA word order'),
    ]
    
    print("Web Value -> Raw Modbus Registers (for writing):")
    print("-" * 70)
    print(f"{'Description':<35} {'Input':<15} {'Raw Registers':<20} {'Verification'}")
    print("-" * 70)
    
    for web_value, datatype, byte_order, word_order, description in write_scenarios:
        try:
            # Convert web value to raw registers for Modbus writing
            raw_registers = convert_web_value_to_raw(web_value, datatype, byte_order, word_order)
            
            # Verify by converting back
            verification = convert_raw_value_to_web(raw_registers, datatype, byte_order, word_order)
            
            # Format for display
            formatted_input = format_value_for_display(web_value, datatype)
            formatted_verification = format_value_for_display(verification, datatype)
            
            # Check if round-trip is successful
            if isinstance(web_value, str) and web_value.startswith('0x'):
                # For hex, compare the actual values
                original_val = int(web_value.replace('0x', ''), 16)
                verified_val = int(formatted_verification.replace('0x', ''), 16)
                status = "✓" if original_val == verified_val else "✗"
            elif isinstance(web_value, bool):
                status = "✓" if bool(verification) == web_value else "✗"
            elif isinstance(web_value, float):
                # Allow small floating point differences
                status = "✓" if abs(verification - web_value) < 0.001 else "✗"
            else:
                status = "✓" if verification == web_value else "✗"
            
            print(f"{description:<35} {formatted_input:<15} {str(raw_registers):<20} {formatted_verification} {status}")
            
        except Exception as e:
            print(f"{description:<35} {str(web_value):<15} ERROR: {e}")
    
    print("\n" + "="*70)
    print("Example Modbus Write Operations:")
    print("="*70)
    
    # Show example of how this would be used in actual Modbus write operations
    examples = [
        ("Set temperature setpoint", 75.5, 'float32'),
        ("Enable pump", True, 'binary'),
        ("Set motor speed", 1500, 'uint16'),
        ("Set pressure limit", 250000, 'uint32'),
    ]
    
    for description, value, datatype in examples:
        raw_regs = convert_web_value_to_raw(value, datatype)
        reg_count = len(raw_regs)
        
        print(f"\n{description}:")
        print(f"  Input value: {value} ({datatype})")
        print(f"  Raw registers: {raw_regs}")
        print(f"  Modbus write: Function 16 (Write Multiple Registers)")
        print(f"  Register count: {reg_count}")
        print(f"  Example: client.write_registers(address=100, values={raw_regs}, slave=1)")

if __name__ == "__main__":
    demonstrate_modbus_write_conversion()