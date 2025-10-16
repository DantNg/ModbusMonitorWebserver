import asyncio
from datetime import datetime
from typing import Optional, Tuple, List

from pymodbus.client import AsyncModbusTcpClient

# ====== CẤU HÌNH ======
HOST = "127.0.0.1"  # IP thiết bị Modbus/TCP (PLC / gateway)
PORT = 502

UNIT_IDS: Tuple[int, int] = (1, 2)   # Hai thiết bị cần đọc
POLL_INTERVAL = 1.0                  # 1 giây
REQ_TIMEOUT = 5.0                    # 5 giây / request

# FC3: Danh sách các dải cần đọc cho mỗi vòng (address, count)
# Bạn sửa tùy nhu cầu (có thể đọc nhiều dải trong một vòng)
READ_RANGES: List[Tuple[int, int]] = [
    (0, 4),       # ví dụ: từ địa chỉ 0, đọc 4 thanh ghi
    # (100, 2),   # thêm dải khác nếu cần
]


async def read_fc3(client: AsyncModbusTcpClient, unit_id: int) -> Optional[dict]:
    """
    Đọc các dải holding registers (FC3) theo READ_RANGES.
    Trả về dict: { (addr,count): [regs...] }, hoặc None nếu có lỗi.
    """
    out = {}
    for addr, count in READ_RANGES:
        try:
            rr = await asyncio.wait_for(
                client.read_holding_registers(address=addr, count=count, slave=unit_id),
                timeout=REQ_TIMEOUT
            )
            if rr.isError():
                raise RuntimeError(f"FC3 error at {addr}:{count} -> {rr}")
            out[(addr, count)] = rr.registers
        except Exception as exc:
            print(f"[{datetime.now()}] U{unit_id} FC3 FAILED at {addr}:{count}: {exc}")
            return None
    return out


async def poll_one_unit(host: str, port: int, unit_id: int):
    """
    Mỗi Unit ID dùng 1 TCP client RIÊNG để đảm bảo độc lập.
    Nếu U1 lỗi/timeout, U2 vẫn tiếp tục bình thường.
    """
    client = AsyncModbusTcpClient(host=host, port=port)
    while True:
        try:
            connected = await client.connect()
            if not connected:
                raise ConnectionError("Cannot connect TCP")

            result = await read_fc3(client, unit_id)
            if result is not None:
                # Log gọn: ghép các dải và giá trị
                flat = {f"{addr}:{cnt}": regs for (addr, cnt), regs in result.items()}
                print(f"[{datetime.now()}] U{unit_id} FC3 OK -> {flat}")
            else:
                print(f"[{datetime.now()}] U{unit_id} FC3 returned None")

        except Exception as exc:
            print(f"[{datetime.now()}] U{unit_id} ERROR: {exc}")
            # đóng để vòng sau connect lại
            try:
                await client.close()
            except Exception:
                pass

        await asyncio.sleep(POLL_INTERVAL)


async def main():
    tasks = [asyncio.create_task(poll_one_unit(HOST, PORT, uid)) for uid in UNIT_IDS]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user")
