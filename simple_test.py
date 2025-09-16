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

def test_modbus_rtu_connection(port, baudrate=9600, unit_id=1, timeout=None, parity='N', bytesize=8, stopbits=1):
    """Test kết nối Modbus RTU với cấu hình serial chi tiết"""
    
    # Tính timeout tự động dựa trên baudrate nếu không được chỉ định
    if timeout is None:
        # Công thức: timeout = max(2.0, 15000 / baudrate) để đảm bảo đủ thời gian cho response
        timeout = max(2.0, 15000 / baudrate)
    
    print(f"\n=== Testing Modbus RTU Connection ===")
    print(f"Port: {port}")
    print(f"Baudrate: {baudrate}")
    print(f"Unit ID: {unit_id}")
    print(f"Timeout: {timeout:.1f}s")
    print(f"Serial config: {bytesize}{parity}{stopbits}")
    
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

def test_raw_serial_communication(port, baudrate=9600, timeout=2.0):
    """Test raw serial communication để debug cấp thấp"""
    print(f"\n=== Testing Raw Serial Communication ===")
    print(f"Port: {port}, Baudrate: {baudrate}, Timeout: {timeout}s")
    
    try:
        import serial
        
        # Tạo kết nối serial
        ser = serial.Serial(
            port=port,
            baudrate=baudrate,
            bytesize=8,
            parity='N',
            stopbits=1,
            timeout=timeout,
            xonxoff=False,
            rtscts=False,
            dsrdtr=False
        )
        
        print("✅ Raw serial connection established")
        
        # Test gửi Modbus RTU frame thô
        # FC03 đọc 1 holding register tại địa chỉ 0, unit_id=1
        # Frame: [unit_id][function_code][start_addr_hi][start_addr_lo][qty_hi][qty_lo][crc_lo][crc_hi]
        test_frame = bytes([0x01, 0x03, 0x00, 0x00, 0x00, 0x01, 0x84, 0x0A])
        
        print(f"📤 Sending test frame: {test_frame.hex().upper()}")
        
        # Clear buffer
        ser.reset_input_buffer()
        ser.reset_output_buffer()
        
        # Gửi frame
        start_time = time.time()
        bytes_sent = ser.write(test_frame)
        ser.flush()  # Đảm bảo data được gửi
        
        print(f"📤 Sent {bytes_sent} bytes")
        
        # Đợi response
        response_data = b''
        response_timeout = 0.1  # Timeout cho mỗi byte
        
        while True:
            if ser.in_waiting > 0:
                chunk = ser.read(ser.in_waiting)
                response_data += chunk
                print(f"📥 Received chunk: {chunk.hex().upper()}")
            else:
                # Kiểm tra timeout
                elapsed = time.time() - start_time
                if elapsed > timeout:
                    break
                time.sleep(response_timeout)
        
        total_time = (time.time() - start_time) * 1000
        
        if response_data:
            print(f"📥 Total response: {response_data.hex().upper()} ({len(response_data)} bytes)")
            print(f"⏱️ Response time: {total_time:.1f}ms")
            
            # Phân tích response
            if len(response_data) >= 5:
                unit_id = response_data[0]
                func_code = response_data[1]
                byte_count = response_data[2]
                
                print(f"📋 Response analysis:")
                print(f"   Unit ID: {unit_id}")
                print(f"   Function Code: {func_code}")
                print(f"   Byte Count: {byte_count}")
                
                if len(response_data) >= byte_count + 5:
                    data_bytes = response_data[3:3+byte_count]
                    crc_bytes = response_data[3+byte_count:3+byte_count+2]
                    print(f"   Data: {data_bytes.hex().upper()}")
                    print(f"   CRC: {crc_bytes.hex().upper()}")
                    
                    if len(data_bytes) >= 2:
                        value = (data_bytes[0] << 8) | data_bytes[1]
                        print(f"   Register Value: {value}")
            
            return {"success": True, "response": response_data.hex(), "latency_ms": total_time}
        else:
            print("❌ No response received")
            return {"success": False, "error": "No response", "latency_ms": total_time}
            
    except Exception as e:
        print(f"❌ Raw serial test failed: {e}")
        return {"success": False, "error": str(e)}
    finally:
        try:
            ser.close()
            print("🔌 Raw serial connection closed")
        except:
            pass

def test_adaptive_baudrate_detection(port, unit_id=1):
    """Tự động phát hiện baudrate phù hợp"""
    print(f"\n=== Adaptive Baudrate Detection ===")
    print(f"Port: {port}, Unit ID: {unit_id}")
    
    # Danh sách baudrate theo thứ tự phổ biến
    baudrates = [9600, 19200, 38400, 57600, 115200, 4800, 2400, 1200]
    successful_baudrates = []
    
    for baudrate in baudrates:
        print(f"\n--- Testing baudrate: {baudrate} ---")
        
        # Test với timeout ngắn
        timeout = max(1.0, 10000 / baudrate)  # Timeout tự động
        
        # Test raw serial trước
        raw_result = test_raw_serial_communication(port, baudrate, timeout)
        
        if raw_result and raw_result.get('success'):
            print(f"✅ Raw serial successful at {baudrate}")
            
            # Test Modbus RTU
            modbus_result = test_modbus_rtu_connection(
                port=port,
                baudrate=baudrate,
                unit_id=unit_id,
                timeout=timeout
            )
            
            if modbus_result and modbus_result != False:
                print(f"✅ Modbus RTU successful at {baudrate}")
                successful_baudrates.append({
                    "baudrate": baudrate,
                    "raw_latency": raw_result.get('latency_ms', 0),
                    "modbus_result": modbus_result
                })
            else:
                print(f"⚠️ Raw serial OK but Modbus failed at {baudrate}")
        else:
            print(f"❌ Raw serial failed at {baudrate}")
    
    return successful_baudrates

def test_comprehensive_rtu_debug(port):
    """Test debug RTU toàn diện"""
    print(f"\n=== Comprehensive RTU Debug for {port} ===")
    
    results = {
        "port": port,
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
        "basic_serial_test": None,
        "adaptive_baudrate": None,
        "raw_communication": {},
        "modbus_tests": {}
    }
    
    # 1. Test cơ bản serial port
    print("\n1️⃣ Basic Serial Port Test")
    results["basic_serial_test"] = test_serial_port_basic(port)
    
    if not results["basic_serial_test"]:
        print("❌ Basic serial test failed. Aborting comprehensive test.")
        return results
    
    # 2. Test adaptive baudrate detection  
    print("\n2️⃣ Adaptive Baudrate Detection")
    results["adaptive_baudrate"] = test_adaptive_baudrate_detection(port, unit_id=1)
    
    # 3. Test raw communication với các baudrate phổ biến
    print("\n3️⃣ Raw Communication Tests")
    common_baudrates = [9600, 19200, 38400]
    
    for baudrate in common_baudrates:
        print(f"\n--- Raw test at {baudrate} ---")
        results["raw_communication"][baudrate] = test_raw_serial_communication(port, baudrate)
    
    # 4. Test Modbus với multiple unit IDs nếu có baudrate thành công
    print("\n4️⃣ Modbus Tests with Multiple Unit IDs")
    
    if results["adaptive_baudrate"]:
        # Lấy baudrate tốt nhất
        best_baudrate = results["adaptive_baudrate"][0]["baudrate"] if results["adaptive_baudrate"] else 9600
        
        print(f"Using best baudrate: {best_baudrate}")
        
        for unit_id in [1, 2, 3, 247]:  # 247 là broadcast address
            if unit_id == 247:
                continue  # Skip broadcast cho read operations
                
            print(f"\n--- Testing Unit ID: {unit_id} ---")
            modbus_result = test_modbus_rtu_connection(
                port=port,
                baudrate=best_baudrate,
                unit_id=unit_id,
                timeout=max(2.0, 15000 / best_baudrate)
            )
            results["modbus_tests"][f"unit_{unit_id}"] = modbus_result
    
    # Summary
    print(f"\n=== Debug Summary for {port} ===")
    print(f"Basic Serial: {'✅' if results['basic_serial_test'] else '❌'}")
    print(f"Working Baudrates: {len(results['adaptive_baudrate']) if results['adaptive_baudrate'] else 0}")
    
    working_raw = sum(1 for r in results['raw_communication'].values() if r and r.get('success'))
    print(f"Raw Communication: {working_raw}/{len(results['raw_communication'])}")
    
    working_modbus = sum(1 for r in results['modbus_tests'].values() if r and r != False)
    print(f"Modbus Tests: {working_modbus}/{len(results['modbus_tests'])}")
    
    return results

def save_test_results(results, filename="rtu_test_results.txt"):
    """Lưu kết quả test ra file với format đẹp"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("=== RTU Test Results ===\n")
            f.write(f"Test time: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 50 + "\n\n")
            
            def write_dict(data, indent=0):
                """Recursive function để ghi dict với indentation"""
                prefix = "  " * indent
                
                if isinstance(data, dict):
                    for key, value in data.items():
                        if isinstance(value, dict):
                            f.write(f"{prefix}{key}:\n")
                            write_dict(value, indent + 1)
                        elif isinstance(value, list):
                            f.write(f"{prefix}{key}: [\n")
                            for i, item in enumerate(value):
                                f.write(f"{prefix}  [{i}] ")
                                if isinstance(item, dict):
                                    f.write("\n")
                                    write_dict(item, indent + 2)
                                else:
                                    f.write(f"{item}\n")
                            f.write(f"{prefix}]\n")
                        else:
                            f.write(f"{prefix}{key}: {value}\n")
                elif isinstance(data, list):
                    for i, item in enumerate(data):
                        f.write(f"{prefix}[{i}] {item}\n")
                else:
                    f.write(f"{prefix}{data}\n")
            
            write_dict(results)
            
            # Thêm troubleshooting tips nếu có lỗi
            if isinstance(results, dict):
                has_errors = False
                
                # Check for failures
                for key, value in results.items():
                    if isinstance(value, dict):
                        for subkey, subvalue in value.items():
                            if (isinstance(subvalue, str) and "error" in subvalue.lower()) or \
                               (isinstance(subvalue, dict) and subvalue.get('success') == False):
                                has_errors = True
                                break
                    elif value == False or (isinstance(value, str) and "error" in value.lower()):
                        has_errors = True
                        break
                
                if has_errors:
                    f.write("\n" + "=" * 50 + "\n")
                    f.write("TROUBLESHOOTING TIPS:\n")
                    f.write("=" * 50 + "\n")
                    f.write("1. Device Power: Ensure device is powered on and ready\n")
                    f.write("2. Connections: Check A/B wire polarity and termination\n")
                    f.write("3. Port Access: Close other programs using the COM port\n")
                    f.write("4. Settings: Verify baudrate and unit ID match device config\n")
                    f.write("5. Cable: Use proper RS485 cable with correct impedance\n")
                    f.write("6. Distance: Long cables may need lower baudrates\n")
                    f.write("7. Timeout: 9600 baud devices need timeout >= 3.0s\n")
                    f.write("8. Unit ID: Try unit IDs 1, 2, 3 or check device manual\n")
                    f.write("9. Function Codes: Some devices only support specific FCs\n")
                    f.write("10. Grounding: Ensure proper electrical grounding\n")
        
        print(f"✅ Test results saved to {filename}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to save results: {e}")
        return False

def interactive_test():
    """Test tương tác với người dùng được cải thiện"""
    print("=== Enhanced Interactive RTU Tester ===")
    print("🔧 This tool helps debug RTU communication issues, especially for 9600 baud devices")
    
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
    
    # Chọn test mode
    print(f"\n🔧 Test Options:")
    print("1. Quick Test (9600 baud, unit ID 1)")
    print("2. Comprehensive Debug (all baudrates, raw + Modbus)")
    print("3. Adaptive Baudrate Detection")
    print("4. Raw Serial Communication Test")
    print("5. Multiple Configuration Test")
    
    while True:
        try:
            choice = int(input("\nSelect test mode (1-5): ").strip())
            if 1 <= choice <= 5:
                break
        except ValueError:
            pass
        print("❌ Please enter a number between 1-5")
    
    # Execute based on choice
    if choice == 1:
        # Quick Test
        print(f"\n🚀 Running Quick Test on {port}...")
        
        if not test_serial_port_basic(port):
            print("❌ Basic serial port test failed. Check if port is not in use.")
            return
        
        result = test_modbus_rtu_connection(port, baudrate=9600, unit_id=1)
        
        if result and result != False:
            print("\n✅ Quick test successful! Device responds at 9600 baud, unit ID 1")
            save_choice = input("Save results to file? (y/n): ").strip().lower()
            if save_choice == 'y':
                save_test_results({"quick_test": result}, f"quick_test_{port.lower()}.txt")
        else:
            print("\n❌ Quick test failed. Consider running comprehensive debug.")
    
    elif choice == 2:
        # Comprehensive Debug
        print(f"\n🔍 Running Comprehensive Debug on {port}...")
        print("⚠️ This may take several minutes...")
        
        results = test_comprehensive_rtu_debug(port)
        
        # Save results
        filename = f"comprehensive_debug_{port.lower()}_{int(time.time())}.txt"
        save_test_results(results, filename)
        
        # Recommendations
        print(f"\n💡 Recommendations based on test results:")
        
        if results.get("adaptive_baudrate"):
            best_config = results["adaptive_baudrate"][0]
            print(f"✅ Best configuration found:")
            print(f"   - Baudrate: {best_config['baudrate']}")
            print(f"   - Latency: {best_config['raw_latency']:.1f}ms")
            
            if best_config['baudrate'] == 9600:
                print(f"⚠️ Device uses 9600 baud - ensure timeout >= 3.0s in production")
            
        else:
            print(f"❌ No working configuration found. Check:")
            print(f"   - Device power and connections")
            print(f"   - Cable wiring (A/B polarity)")
            print(f"   - Device unit ID settings")
            print(f"   - Port availability (close other programs)")
    
    elif choice == 3:
        # Adaptive Baudrate Detection
        print(f"\n🔍 Running Adaptive Baudrate Detection on {port}...")
        
        if not test_serial_port_basic(port):
            print("❌ Basic serial port test failed.")
            return
            
        results = test_adaptive_baudrate_detection(port, unit_id=1)
        
        if results:
            print(f"\n✅ Found {len(results)} working baudrate(s):")
            for result in results:
                print(f"   - {result['baudrate']} baud (latency: {result['raw_latency']:.1f}ms)")
        else:
            print(f"\n❌ No working baudrates found")
    
    elif choice == 4:
        # Raw Serial Test
        print(f"\n� Running Raw Serial Communication Test on {port}...")
        
        baudrate = 9600
        try:
            custom_baud = input(f"Enter baudrate (default 9600): ").strip()
            if custom_baud:
                baudrate = int(custom_baud)
        except ValueError:
            print("Using default baudrate 9600")
        
        result = test_raw_serial_communication(port, baudrate)
        
        if result and result.get('success'):
            print(f"\n✅ Raw communication successful")
        else:
            print(f"\n❌ Raw communication failed")
    
    elif choice == 5:
        # Multiple Configuration Test
        print(f"\n🔄 Running Multiple Configuration Test on {port}...")
        
        if not test_serial_port_basic(port):
            print("❌ Basic serial port test failed.")
            return
            
        successful_configs = test_multiple_configurations(port)
        
        if successful_configs:
            print(f"\n✅ Found {len(successful_configs)} working configurations:")
            for config in successful_configs:
                print(f"   - Baudrate: {config['baudrate']}, Unit ID: {config['unit_id']}")
        else:
            print(f"\n❌ No working configurations found")
    
    print(f"\n🏁 Test completed for {port}")

if __name__ == "__main__":
    try:
        interactive_test()
    except KeyboardInterrupt:
        print("\n\n❌ Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
    
    input("\nPress Enter to exit...")