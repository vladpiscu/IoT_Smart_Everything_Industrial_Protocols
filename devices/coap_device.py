from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Callable, Optional

from aiocoap import Context, Message
from aiocoap.numbers.codes import POST

from data_providers import create_csv_value_provider
from sent_events_logger import log_sent_event

logger = logging.getLogger(__name__)


class CoapDevice:
    """Asynchronous CoAP device simulator sending periodic sensor values."""

    def __init__(
        self,
        device_id: str,
        sensor: str,
        value_provider: Callable[[], float],
        endpoint: str = "coap://localhost:5683/sensor",
        interval_s: float = 4.0,
    ) -> None:
        self.device_id = device_id
        self.sensor = sensor
        self.value_provider = value_provider
        self.endpoint = endpoint
        self.interval_s = interval_s
        self._protocol: Optional[Context] = None
        self._event_counter = 0

    async def run(self) -> None:
        self._protocol = await Context.create_client_context()
        await asyncio.sleep(1)  # ensure server readiness

        try:
            while True:
                timestamp = datetime.now(timezone.utc).isoformat()
                value = self.value_provider()
                
                # Generate event ID: device_id_counter
                self._event_counter += 1
                event_id = f"{self.device_id}_{self._event_counter}"
                
                payload = {
                    "device_id": self.device_id,
                    "protocol": "coap",
                    "timestamp": timestamp,
                    "sensor": self.sensor,
                    "value": value,
                    "event_id": event_id,
                }

                # Log sent event BEFORE actually sending (to capture accurate send timestamp)
                log_sent_event({
                    "event_id": event_id,
                    "device_id": self.device_id,
                    "protocol": "coap",
                    "sensor": self.sensor,
                    "value": value,
                    "timestamp": timestamp
                })
                
                # Send the payload
                success = await self.send(payload)

                await asyncio.sleep(self.interval_s)
        except asyncio.CancelledError:
            logger.info("[CoAP Device] %s cancelled", self.device_id)

    async def send(self, payload: dict) -> bool:
        if not self._protocol:
            logger.error("[CoAP Device] %s has no active protocol context", self.device_id)
            return False

        request = Message(code=POST, uri=self.endpoint, payload=json.dumps(payload).encode("utf-8"))

        try:
            response = await self._protocol.request(request).response
            logger.info(
                "[CoAP Device] %s sent %s value=%.2f response=%s",
                self.device_id,
                payload.get("sensor", "unknown"),
                payload.get("value", 0),
                response.payload.decode("utf-8"),
            )
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("[CoAP Device] %s send failed: %s", self.device_id, exc)
            return False


def random_heart_rate_provider(low: int = 65, high: int = 90) -> Callable[[], int]:
    import random

    def _inner() -> int:
        return random.randint(low, high)

    return _inner


def heart_rate_dataset_provider(
    csv_path: str = "health.csv",
    column: str = "heartrate-mean",
) -> Callable[[], float]:
    """Create a provider that replays real heart rate values from the dataset."""
    return create_csv_value_provider(
        csv_path,
        column,
        delimiter=",",
        decimal=".",
        postprocess=lambda value: float(round(value)),
    )


