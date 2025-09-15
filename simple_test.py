"""
Simple RTU Connection Tester
Kiểm tra kết nối Modbus RTU cơ bản không qua connection pool
"""

import time
import serial
from pymodbus.client import ModbusSerialClient
from pymodbus.exceptions import ModbusIOException, ConnectionException

def list_available_ports():
    """Liệt kê tất cả COM ports có sẵn"""
    import serial.tools.list_ports
    ports = list(serial.tools.list_ports.comports())
    print("=== Available COM Ports ===")
    if not ports:
        print("❌ No COM ports found!")
        return []
    
    for port in ports:
        print(f"✅ {port.device} - {port.description}")
    return [port.device for port in ports]

def test_serial_port_basic(port, baudrate=9600):
    """Test cơ bản COM port có mở được không"""
    print(f"\n=== Testing Serial Port: {port} ===")
    try:
        ser = serial.Serial(
            port=port,
            baudrate=baudrate,
            bytesize=8,
            parity='N',
            stopbits=1,
            timeout=1.0
        )
        print(f"✅ Port {port} opened successfully")
        ser.close()
        print(f"✅ Port {port} closed successfully")
        return True
    except Exception as e:
        print(f"❌ Cannot open port {port}: {e}")
        return False

def test_modbus_rtu_connection(port, baudrate=9600, unit_id=1, timeout=1.0):
    """Test kết nối Modbus RTU cơ bản"""
    print(f"\n=== Testing Modbus RTU Connection ===")
    print(f"Port: {port}")
    print(f"Baudrate: {baudrate}")
    print(f"Unit ID: {unit_id}")
    print(f"Timeout: {timeout}s")
    
    client = None
    try:
        # Tạo client
        client = ModbusSerialClient(
            port=port,
            baudrate=baudrate,
            bytesize=8,
            parity='N',
            stopbits=1,
            timeout=timeout
        )
        
        # Kết nối
        print("🔌 Connecting...")
        connected = client.connect()
        
        if not connected:
            print("❌ Failed to connect to Modbus RTU")
            return False
            
        print("✅ Connected to Modbus RTU")
        
        # Test đọc một số function codes phổ biến
        test_results = {}
        
        # Test FC03 - Read Holding Registers
        print("\n--- Testing FC03 (Read Holding Registers) ---")
        for addr in [0, 1, 40001, 40002]:  # Test một số địa chỉ phổ biến
            try:
                start_time = time.time()
                
                # Normalize address
                read_addr = addr
                if addr >= 40001:
                    read_addr = addr - 40001
                
                result = client.read_holding_registers(read_addr, 1, slave=unit_id)
                latency = (time.time() - start_time) * 1000
                
                if result.isError():
                    print(f"❌ Address {addr}: {result}")
                    test_results[f"FC03_addr_{addr}"] = f"Error: {result}"
                else:
                    value = result.registers[0]
                    print(f"✅ Address {addr}: {value} (latency: {latency:.1f}ms)")
                    test_results[f"FC03_addr_{addr}"] = {"value": value, "latency_ms": latency}
                    
            except Exception as e:
                print(f"❌ Address {addr}: Exception: {e}")
                test_results[f"FC03_addr_{addr}"] = f"Exception: {e}"
        
        # Test FC01 - Read Coils
        print("\n--- Testing FC01 (Read Coils) ---")
        for addr in [0, 1, 10001, 10002]:
            try:
                start_time = time.time()
                
                read_addr = addr
                if addr >= 10001:
                    read_addr = addr - 10001
                    
                result = client.read_coils(read_addr, 1, slave=unit_id)
                latency = (time.time() - start_time) * 1000
                
                if result.isError():
                    print(f"❌ Coil {addr}: {result}")
                    test_results[f"FC01_addr_{addr}"] = f"Error: {result}"
                else:
                    value = result.bits[0]
                    print(f"✅ Coil {addr}: {value} (latency: {latency:.1f}ms)")
                    test_results[f"FC01_addr_{addr}"] = {"value": value, "latency_ms": latency}
                    
            except Exception as e:
                print(f"❌ Coil {addr}: Exception: {e}")
                test_results[f"FC01_addr_{addr}"] = f"Exception: {e}"
        
        # Test FC04 - Read Input Registers
        print("\n--- Testing FC04 (Read Input Registers) ---")
        for addr in [0, 1]:
            try:
                start_time = time.time()
                result = client.read_input_registers(addr, 1, slave=unit_id)
                latency = (time.time() - start_time) * 1000
                
                if result.isError():
                    print(f"❌ Input Register {addr}: {result}")
                    test_results[f"FC04_addr_{addr}"] = f"Error: {result}"
                else:
                    value = result.registers[0]
                    print(f"✅ Input Register {addr}: {value} (latency: {latency:.1f}ms)")
                    test_results[f"FC04_addr_{addr}"] = {"value": value, "latency_ms": latency}
                    
            except Exception as e:
                print(f"❌ Input Register {addr}: Exception: {e}")
                test_results[f"FC04_addr_{addr}"] = f"Exception: {e}"
        
        return test_results
        
    except Exception as e:
        print(f"❌ Modbus RTU test failed: {e}")
        return False
    finally:
        if client:
            try:
                client.close()
                print("🔌 Connection closed")
            except:
                pass

def test_multiple_configurations(port):
    """Test với nhiều cấu hình baudrate và unit ID khác nhau"""
    print(f"\n=== Testing Multiple Configurations for {port} ===")
    
    # Các cấu hình phổ biến
    configs = [
        {"baudrate": 9600, "unit_id": 1},
        {"baudrate": 19200, "unit_id": 1},
        {"baudrate": 38400, "unit_id": 1},
        {"baudrate": 9600, "unit_id": 2},
        {"baudrate": 9600, "unit_id": 3},
    ]
    
    successful_configs = []
    
    for config in configs:
        print(f"\n--- Testing: Baudrate={config['baudrate']}, Unit ID={config['unit_id']} ---")
        
        # Test với timeout ngắn để nhanh
        result = test_modbus_rtu_connection(
            port=port,
            baudrate=config['baudrate'],
            unit_id=config['unit_id'],
            timeout=0.5
        )
        
        if result and result != False:
            successful_configs.append(config)
            print(f"✅ Configuration successful: {config}")
        else:
            print(f"❌ Configuration failed: {config}")
    
    return successful_configs

def save_test_results(results, filename="rtu_test_results.txt"):
    """Lưu kết quả test ra file"""
    try:
        with open(filename, 'w') as f:
            f.write("=== RTU Test Results ===\n")
            f.write(f"Test time: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for key, value in results.items():
                f.write(f"{key}: {value}\n")
        
        print(f"✅ Test results saved to {filename}")
    except Exception as e:
        print(f"❌ Failed to save results: {e}")

def interactive_test():
    """Test tương tác với người dùng"""
    print("=== Interactive RTU Tester ===")
    
    # Liệt kê ports
    available_ports = list_available_ports()
    if not available_ports:
        print("❌ No COM ports available for testing")
        return
    
    # Chọn port
    print(f"\nAvailable ports: {', '.join(available_ports)}")
    while True:
        port = input(f"Enter COM port to test (e.g. COM3): ").strip().upper()
        if port in available_ports or port.startswith('COM'):
            break
        print(f"❌ Invalid port. Please choose from: {', '.join(available_ports)}")
    
    # Test cơ bản port
    if not test_serial_port_basic(port):
        print("❌ Basic serial port test failed. Check if port is not in use.")
        return
    
    # Test Modbus với cấu hình mặc định
    print(f"\n=== Quick Test with Default Settings ===")
    result = test_modbus_rtu_connection(port, baudrate=9600, unit_id=1, timeout=1.0)
    
    if result and result != False:
        print("\n✅ Quick test successful!")
        
        # Hỏi có muốn test thêm không
        test_more = input("\nDo you want to test multiple configurations? (y/n): ").strip().lower()
        if test_more == 'y':
            successful_configs = test_multiple_configurations(port)
            if successful_configs:
                print(f"\n✅ Found {len(successful_configs)} working configurations:")
                for config in successful_configs:
                    print(f"  - Baudrate: {config['baudrate']}, Unit ID: {config['unit_id']}")
            else:
                print("\n❌ No working configurations found")
    else:
        print("\n❌ Quick test failed. Trying multiple configurations...")
        successful_configs = test_multiple_configurations(port)
        
        if successful_configs:
            print(f"\n✅ Found {len(successful_configs)} working configurations:")
            for config in successful_configs:
                print(f"  - Baudrate: {config['baudrate']}, Unit ID: {config['unit_id']}")
        else:
            print("\n❌ No working configurations found")
            print("\n💡 Troubleshooting tips:")
            print("   1. Check if the device is powered on")
            print("   2. Check cable connections")
            print("   3. Verify COM port settings match device settings")
            print("   4. Try different baudrates: 9600, 19200, 38400")
            print("   5. Try different unit IDs: 1, 2, 3")
            print("   6. Check if another program is using the COM port")

if __name__ == "__main__":
    try:
        interactive_test()
    except KeyboardInterrupt:
        print("\n\n❌ Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
    
    input("\nPress Enter to exit...")