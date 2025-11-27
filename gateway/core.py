from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from storage import save_to_csv


@dataclass
class NormalizedRecord:
    device_id: str
    protocol: str
    timestamp: str
    sensor: str
    value: Any
    status: str = "ok"
    event_id: Optional[str] = None

    @classmethod
    def from_payload(
        cls,
        payload: Dict[str, Any],
        protocol: str,
        status: str = "ok",
        default_sensor: str = "unknown",
    ) -> "NormalizedRecord":
        device_id = payload.get("device_id", "unknown_device")
        timestamp = payload.get("timestamp")
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).isoformat()
        sensor = payload.get("sensor", default_sensor)
        value = payload.get("value")
        event_id = payload.get("event_id")
        return cls(
            device_id=device_id,
            protocol=protocol,
            timestamp=timestamp,
            sensor=sensor,
            value=value,
            status=status,
            event_id=event_id,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "device_id": self.device_id,
            "protocol": self.protocol,
            "timestamp": self.timestamp,
            "sensor": self.sensor,
            "value": self.value,
            "status": self.status,
            "event_id": self.event_id or "",
        }


class Gateway:
    """Normalize incoming payloads and persist them via storage helpers."""

    def __init__(self, csv_filename: str = "gateway_data.csv"):
        self.csv_filename = csv_filename

    def ingest(self, record: NormalizedRecord) -> None:
        save_to_csv(record.to_dict(), self.csv_filename)

    def ingest_payload(
        self,
        payload: Dict[str, Any],
        protocol: str,
        status: str = "ok",
        default_sensor: str = "unknown",
    ) -> NormalizedRecord:
        record = NormalizedRecord.from_payload(
            payload=payload,
            protocol=protocol,
            status=status,
            default_sensor=default_sensor,
        )
        self.ingest(record)
        return record
