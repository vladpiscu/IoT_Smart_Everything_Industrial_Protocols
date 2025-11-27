from __future__ import annotations

import asyncio
import logging
import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, List, Sequence

from pymodbus.datastore import ModbusDeviceContext, ModbusServerContext, ModbusSequentialDataBlock
from pymodbus.server import StartAsyncTcpServer

logger = logging.getLogger(__name__)

# Track event counters per device_id for Modbus
_modbus_event_counters: Dict[str, int] = defaultdict(int)
_modbus_counter_lock = threading.Lock()


@dataclass
class ModbusCollectorConfig:
    unit_id: int
    device_id: str
    sensor: str
    start_address: int = 0
    register_count: int = 10


class _CallbackDataBlock(ModbusSequentialDataBlock):
    def __init__(
        self,
        address: int,
        values: Sequence[int],
        callback: Callable[[dict], None],
        metadata: dict,
    ) -> None:
        super().__init__(address, list(values))
        self.callback = callback
        self.metadata = metadata

    def setValues(self, address: int, values: Sequence[int]) -> None:  # pylint: disable=invalid-name
        super().setValues(address, values)
        timestamp = datetime.now(timezone.utc).isoformat()
        device_id = self.metadata["device_id"]
        sensor = self.metadata["sensor"]
        
        for offset, register_value in enumerate(values):
            # Generate event ID: device_id_counter
            # Note: For Modbus, we generate event_id in the collector since the protocol
            # only carries the value. This event_id may not match the device's sent event_id
            # exactly if messages are lost, but it allows tracking received messages.
            with _modbus_counter_lock:
                _modbus_event_counters[device_id] += 1
                event_id = f"{device_id}_{_modbus_event_counters[device_id]}"
            
            payload = {
                "device_id": device_id,
                "protocol": "modbus",
                "timestamp": timestamp,
                "sensor": sensor,
                "value": int(register_value),
                "event_id": event_id,
            }
            self.callback(payload)
            logger.info(
                "[Modbus Collector] Recorded %s register=%d value=%d",
                self.metadata["device_id"],
                address + offset,
                register_value,
            )


def _build_context(callback: Callable[[dict], None], devices: List[ModbusCollectorConfig]) -> ModbusServerContext:
    slaves: Dict[int, ModbusDeviceContext] = {}
    for cfg in devices:
        block = _CallbackDataBlock(
            cfg.start_address,
            [0] * cfg.register_count,
            callback=callback,
            metadata={
                "device_id": cfg.device_id,
                "sensor": cfg.sensor,
            },
        )
        slaves[cfg.unit_id] = ModbusDeviceContext(hr=block)
    return ModbusServerContext(devices=slaves, single=False)


async def start_modbus_collector(
    callback: Callable[[dict], None],
    devices: List[ModbusCollectorConfig],
    address: tuple[str, int] = ("localhost", 5020),
) -> None:
    # Currently the network simulator isn't applied at the server level because pymodbus
    # handles socket operations internally. A shim could wrap the socket externally if needed.
    if not devices:
        raise ValueError("At least one Modbus device configuration is required")

    context = _build_context(callback, devices)
    logger.info("[Modbus Collector] Starting server on %s:%d", address[0], address[1])
    await StartAsyncTcpServer(context=context, address=address)


