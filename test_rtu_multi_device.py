#!/usr/bin/env python3
"""
Test RTU Multi-Device Reader
Đọc lần lượt 2 devices RTU trên cùng 1 COM port với interval khác nhau
- Device 1 (Unit 1): interval 1 giây  
- Device 2 (Unit 2): interval 5 giây
"""

import time
import threading
from datetime import datetime
from pymodbus.client import ModbusSerialClient
from pymodbus.exceptions import ModbusIOException, ConnectionException

class RTUMultiDeviceTest:
    def __init__(self, serial_port='COM2', baudrate=9600):
        self.serial_port = serial_port
        self.baudrate = baudrate
        self.client = None
        self.running = False
        
        # Device configurations
        self.devices = {
            1: {
                'unit_id': 1,
                'name': 'Device_1',
                'interval': 1,  # 1 second
                'last_read': 0,
                'read_count': 0,
                'addresses': [0, 1, 2],  # Addresses to read
                'function_code': 3  # FC03 - Read Holding Registers
            },
            2: {
                'unit_id': 2, 
                'name': 'Device_2',
                'interval': 5,  # 5 seconds
                'last_read': 0,
                'read_count': 0,
                'addresses': [0, 1, 2, 3],  # Addresses to read
                'function_code': 3  # FC03 - Read Holding Registers
            }
        }
        
        # Shared connection lock để tránh conflict
        self.connection_lock = threading.Lock()
        
    def connect(self):
        """Establish connection to RTU"""
        try:
            self.client = ModbusSerialClient(
                port=self.serial_port,
                baudrate=self.baudrate,
                bytesize=8,
                parity='N',
                stopbits=1,
                timeout=1.0
            )
            
            if self.client.connect():
                print(f"✅ Connected to RTU on {self.serial_port} at {self.baudrate} baud")
                return True
            else:
                print(f"❌ Failed to connect to {self.serial_port}")
                return False
                
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False
    
    def disconnect(self):
        """Close RTU connection"""
        if self.client:
            try:
                self.client.close()
                print("🔌 RTU connection closed")
            except:
                pass
            self.client = None
    
    def read_device(self, device_config):
        """Read data from a specific device"""
        unit_id = device_config['unit_id']
        name = device_config['name']
        addresses = device_config['addresses']
        fc = device_config['function_code']
        
        with self.connection_lock:  # Ensure exclusive access to serial port
            try:
                if not self.client or not self.client.is_socket_open():
                    if not self.connect():
                        return False
                
                # Read holding registers
                start_addr = min(addresses)
                count = len(addresses)
                
                timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                print(f"{timestamp} [RTU_READ] Port {self.serial_port} {name} (Unit {unit_id}) - FC{fc:02d} Read Holding Registers addr={start_addr} count={count}")
                
                result = self.client.read_holding_registers(start_addr, count, slave=unit_id)
                
                if not result.isError():
                    device_config['read_count'] += 1
                    print(f"  ✅ {name}: {result.registers} (read #{device_config['read_count']})")
                    return True
                else:
                    print(f"  ❌ {name}: Error - {result}")
                    return False
                    
            except Exception as e:
                print(f"  ❌ {name}: Exception - {e}")
                return False
            
            finally:
                # Small delay between device reads on same serial port
                time.sleep(0.1)  # 100ms delay
    
    def device_reader_thread(self, device_id):
        """Thread function to read a specific device at its interval"""
        device_config = self.devices[device_id]
        name = device_config['name']
        interval = device_config['interval']
        
        print(f"🔄 Started reader thread for {name} (interval: {interval}s)")
        
        while self.running:
            try:
                current_time = time.time()
                
                # Check if it's time to read this device
                if current_time - device_config['last_read'] >= interval:
                    success = self.read_device(device_config)
                    device_config['last_read'] = current_time
                    
                    if not success:
                        print(f"⚠️ {name}: Read failed, will retry in {interval}s")
                
                # Sleep for small amount to prevent CPU spinning
                time.sleep(0.1)
                
            except Exception as e:
                print(f"❌ {name} thread error: {e}")
                time.sleep(1)
    
    def start_test(self, duration=30):
        """Start the multi-device test"""
        print("=" * 60)
        print("🚀 RTU Multi-Device Test Starting")
        print("=" * 60)
        print(f"📡 Serial Port: {self.serial_port}")
        print(f"⚡ Baudrate: {self.baudrate}")
        print(f"⏱️  Test Duration: {duration} seconds")
        print()
        
        for dev_id, config in self.devices.items():
            print(f"📱 Device {dev_id}: {config['name']}")
            print(f"   Unit ID: {config['unit_id']}")
            print(f"   Interval: {config['interval']}s")
            print(f"   Addresses: {config['addresses']}")
            print()
        
        # Connect to RTU
        if not self.connect():
            print("❌ Cannot establish RTU connection. Test aborted.")
            return
        
        # Start reader threads
        self.running = True
        threads = []
        
        for device_id in self.devices.keys():
            thread = threading.Thread(
                target=self.device_reader_thread, 
                args=(device_id,),
                name=f"Reader_Device_{device_id}"
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # Run for specified duration
        try:
            print(f"⏳ Test running for {duration} seconds... (Ctrl+C to stop early)")
            print("-" * 60)
            
            start_time = time.time()
            while time.time() - start_time < duration and self.running:
                time.sleep(1)
                
                # Print progress every 10 seconds
                elapsed = int(time.time() - start_time)
                if elapsed % 10 == 0 and elapsed > 0:
                    print(f"⏱️  {elapsed}s elapsed...")
        
        except KeyboardInterrupt:
            print("\n🛑 Test stopped by user")
        
        finally:
            # Stop all threads
            print("\n🔄 Stopping test...")
            self.running = False
            
            # Wait for threads to finish
            for thread in threads:
                thread.join(timeout=2)
            
            # Disconnect
            self.disconnect()
            
            # Print summary
            self.print_summary()
    
    def print_summary(self):
        """Print test summary"""
        print("\n" + "=" * 60)
        print("📊 TEST SUMMARY")
        print("=" * 60)
        
        for dev_id, config in self.devices.items():
            name = config['name']
            interval = config['interval']
            read_count = config['read_count']
            expected_reads = int(30 / interval)  # Estimate for 30s test
            
            print(f"📱 {name} (Unit {config['unit_id']}):")
            print(f"   Interval: {interval}s")
            print(f"   Actual Reads: {read_count}")
            print(f"   Expected Reads: ~{expected_reads}")
            
            if read_count > 0:
                avg_interval = 30 / read_count if read_count > 0 else 0
                print(f"   Average Interval: {avg_interval:.1f}s")
                
                if abs(avg_interval - interval) <= 0.5:
                    print(f"   ✅ Timing: GOOD (within 0.5s tolerance)")
                else:
                    print(f"   ⚠️ Timing: OFF by {abs(avg_interval - interval):.1f}s")
            else:
                print(f"   ❌ No successful reads!")
            print()
        
        print("🎯 RTU Multi-Device Test Completed!")

def main():
    """Main function"""
    print("RTU Multi-Device Interval Test")
    print("=" * 40)
    
    # Configuration
    SERIAL_PORT = 'COM2'  # Change this to your COM port
    BAUDRATE = 9600       # Change this to your baudrate
    TEST_DURATION = 30    # seconds
    
    # Ask user for configuration
    try:
        port = input(f"Serial Port (default: {SERIAL_PORT}): ").strip()
        if port:
            SERIAL_PORT = port
            
        baudrate_input = input(f"Baudrate (default: {BAUDRATE}): ").strip()
        if baudrate_input:
            BAUDRATE = int(baudrate_input)
            
        duration_input = input(f"Test duration in seconds (default: {TEST_DURATION}): ").strip()
        if duration_input:
            TEST_DURATION = int(duration_input)
            
    except (ValueError, KeyboardInterrupt):
        print("Using default values...")
    
    # Create and run test
    test = RTUMultiDeviceTest(serial_port=SERIAL_PORT, baudrate=BAUDRATE)
    test.start_test(duration=TEST_DURATION)

if __name__ == "__main__":
    main()