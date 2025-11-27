from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Optional

from pymodbus.client import AsyncModbusTcpClient

from data_providers import create_csv_value_provider

from sent_events_logger import log_sent_event

logger = logging.getLogger(__name__)


class ModbusDevice:
    """Asynchronous Modbus device writing holding registers."""

    def __init__(
        self,
        device_id: str,
        sensor: str,
        unit_id: int,
        start_address: int,
        value_provider: Callable[[], int],
        interval_s: float = 3.0,
        host: str = "localhost",
        port: int = 5020,
        reconnect_delay_s: float = 1.0,
    ) -> None:
        self.device_id = device_id
        self.sensor = sensor
        self.unit_id = unit_id
        self.start_address = start_address
        self.value_provider = value_provider
        self.interval_s = interval_s
        self.host = host
        self.port = port
        self.client = AsyncModbusTcpClient(host, port=port)
        self.reconnect_delay_s = reconnect_delay_s
        self._event_counter = 0

    async def run(self) -> None:
        await self._ensure_connected(initial=True)

        try:
            while True:
                if not await self._ensure_connected(initial=False):
                    await asyncio.sleep(self.reconnect_delay_s)
                    continue

                timestamp = datetime.now(timezone.utc).isoformat()
                value = int(self.value_provider())
                
                # Generate event ID: device_id_counter
                self._event_counter += 1
                event_id = f"{self.device_id}_{self._event_counter}"
                
                payload = {
                    "device_id": self.device_id,
                    "protocol": "modbus",
                    "timestamp": timestamp,
                    "sensor": self.sensor,
                    "value": value,
                    "event_id": event_id,
                }

                # Log sent event BEFORE actually sending (to capture accurate send timestamp)
                log_sent_event({
                    "event_id": event_id,
                    "device_id": self.device_id,
                    "protocol": "modbus",
                    "sensor": self.sensor,
                    "value": value,
                    "timestamp": timestamp
                })
                
                # Send the payload
                success = await self.send(payload)

                await asyncio.sleep(self.interval_s)
        except asyncio.CancelledError:
            logger.info("[Modbus Device] %s cancelled", self.device_id)
        finally:
            self.client.close()

    async def send(self, payload: dict) -> bool:
        if not await self._ensure_connected(initial=False):
            logger.warning("[Modbus Device] %s not connected to server", self.device_id)
            return False

        value = int(payload.get("value", 0))

        try:
            response = await self.client.write_registers(
                self.start_address,
                [value],
                device_id=self.unit_id,
            )
            if hasattr(response, "isError") and response.isError():  # type: ignore[attr-defined]
                logger.error("[Modbus Device] %s write error: %s", self.device_id, response)
                return False

            logger.info(
                "[Modbus Device] %s wrote register=%d value=%d",
                self.device_id,
                self.start_address,
                value,
            )
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("[Modbus Device] %s write failed: %s", self.device_id, exc)
            return False

    async def _ensure_connected(self, initial: bool = False) -> bool:
        if self.client.connected:
            return True

        while True:
            try:
                connected = await self.client.connect()
                if connected or self.client.connected:
                    logger.info(
                        "[Modbus Device] %s connected to %s:%d",
                        self.device_id,
                        self.host,
                        self.port,
                    )
                    return True
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning("[Modbus Device] %s connection attempt failed: %s", self.device_id, exc)

            if initial:
                logger.warning(
                    "[Modbus Device] %s waiting for server %s:%d",
                    self.device_id,
                    self.host,
                    self.port,
                )
                initial = False

            await asyncio.sleep(self.reconnect_delay_s)


def random_temperature_provider(base: float = 80.0, variation: float = 2.0) -> Callable[[], int]:
    import random

    def _inner() -> int:
        return int(base + random.uniform(-variation, variation))

    return _inner


def air_quality_dataset_provider(
    csv_path: str = "AirQualityUCI.csv",
    column: str = "PT08.S1(CO)",
) -> Callable[[], int]:
    """Provide air quality readings from the AirQualityUCI dataset."""
    return create_csv_value_provider(
        csv_path,
        column,
        delimiter=";",
        decimal=",",
        invalid_values={"-200"},
        postprocess=lambda value: int(round(value)),
    )

