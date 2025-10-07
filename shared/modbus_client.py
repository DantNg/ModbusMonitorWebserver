import time
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException

def read_modbus_tcp(ip_address, port, unit_id, register_address, count):
    """
    Reads Modbus TCP registers from a server.

    :param ip_address: IP address of the Modbus TCP server
    :param port: Port of the Modbus TCP server (default is 502)
    :param unit_id: Unit ID of the Modbus device
    :param register_address: Starting register address to read
    :param count: Number of registers to read
    :return: List of register values or None if an error occurs
    """
    try:
        # Connect to Modbus TCP server
        client = ModbusTcpClient(host=ip_address, port=port)
        if not client.connect():
            print(f"Failed to connect to Modbus server at {ip_address}:{port}")
            return None

        # Read holding registers
        response = client.read_holding_registers(register_address, count, slave=unit_id)
        if response.isError():
            print(f"Modbus error: {response}")
            return None

        # Disconnect client
        client.close()

        # Return register values
        return response.registers

    except ModbusException as e:
        print(f"Modbus exception occurred: {e}")
        return None
    except Exception as e:
        print(f"Error reading Modbus TCP: {e}")
        return None


# Example usage
if __name__ == "__main__":
    ip_address = "127.0.0.1"  # Replace with your Modbus server's IP
    port = 502  # Default Modbus TCP port
    unit_id = 1  # Unit ID of the Modbus device
    register_address = 0  # Starting register address
    count = 10  # Number of registers to read
    while(True):
        values = read_modbus_tcp(ip_address, port, unit_id, register_address, count)
        if values:
            print(f"Read values: {values}")
        else:
            print("Failed to read Modbus registers")
        time.sleep(1)