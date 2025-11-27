from __future__ import annotations

import json
import threading
import time
from datetime import datetime, UTC
from typing import Callable, Optional

import paho.mqtt.client as mqtt

from sent_events_logger import log_sent_event


class BaseMqttDevice:
    
    def __init__(
        self,
        device_number,
        sensor_type,
        interval=2,
        broker="localhost",
        port=1883,
        topic="iot",
        keepalive=60,
        value_provider: Optional[Callable[[], float]] = None,
    ):
        self.device_id = f"{sensor_type}_mqtt_{device_number}"
        self.sensor_type = sensor_type
        self.interval = interval
        self.broker = broker
        self.port = port
        self.topic = topic
        self.keepalive = keepalive
        self.stop_event = threading.Event()
        self.thread = None
        self.client = None
        self._event_counter = 0
        self._counter_lock = threading.Lock()
        self.value_provider = value_provider
        
    def _extract_sensor_value(self, parts):
        raise NotImplementedError("Subclasses must implement _extract_sensor_value method")
        
    def _run(self):
        # Create MQTT client
        self.client = mqtt.Client()
        
        try:
            # Connect to MQTT broker
            self.client.connect(self.broker, self.port, self.keepalive)
            self.client.loop_start()
            print(f"[MQTT DEVICE] {self.device_id} - Connected to {self.broker}:{self.port}")

            if self.value_provider is not None:
                self._run_with_value_provider()
                return

            # Open and read the data file
            with open("data.txt", "r") as f:
                lines = f.readlines()

            line_index = 0

            while not self.stop_event.is_set():
                # Read the next line from data.txt
                if line_index >= len(lines):
                    line_index = 0  # Loop back to the beginning

                line = lines[line_index].strip()

                # Parse the line to extract sensor value
                parts = line.split()
                if len(parts) >= 4:
                    try:
                        sensor_value = self._extract_sensor_value(parts)

                        if sensor_value is not None:
                            self._publish_value(sensor_value)
                    except NotImplementedError:
                        print(f"[MQTT DEVICE] {self.device_id} - Error: _extract_sensor_value not implemented")
                        break

                line_index += 1

                # Sleep for the configured interval, but check stop_event periodically
                self.stop_event.wait(timeout=self.interval)

        except Exception as e:
            print(f"[MQTT DEVICE] {self.device_id} - Connection error: {e}")
        finally:
            if self.client:
                self.client.loop_stop()
                self.client.disconnect()
                print(f"[MQTT DEVICE] {self.device_id} - Disconnected")
    
    def start(self):
        if self.thread and self.thread.is_alive():
            print(f"[MQTT DEVICE] {self.device_id} - Already running")
            return
        
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        print(f"[MQTT DEVICE] {self.device_id} - Started (interval: {self.interval}s)")
    
    def stop(self):
        """Gracefully stop the device."""
        if self.thread and self.thread.is_alive():
            print(f"[MQTT DEVICE] {self.device_id} - Stopping...")
            self.stop_event.set()
            self.thread.join(timeout=5)
            print(f"[MQTT DEVICE] {self.device_id} - Stopped")
    
    def is_running(self):
        return self.thread and self.thread.is_alive()

    def _publish_value(self, sensor_value):
        try:
            numeric_value = float(sensor_value)
        except (TypeError, ValueError):
            print(f"[MQTT DEVICE] {self.device_id} - Invalid sensor value: {sensor_value}")
            return

        with self._counter_lock:
            self._event_counter += 1
            event_id = f"{self.device_id}_{self._event_counter}"

        timestamp = datetime.now(UTC).isoformat()
        reading = {
            "device_id": self.device_id,
            "protocol": "mqtt",
            "timestamp": timestamp,
            "sensor": self.sensor_type,
            "value": float(numeric_value),
            "event_id": event_id
        }

        # Log sent event BEFORE actually sending (to capture accurate send timestamp)
        log_sent_event({
            "event_id": event_id,
            "device_id": self.device_id,
            "protocol": "mqtt",
            "sensor": self.sensor_type,
            "value": reading["value"],
            "timestamp": timestamp
        })

        # Send the payload
        if self.send(reading):
            print(f"[MQTT DEVICE] {self.device_id} - Published reading: {reading}")

    def _run_with_value_provider(self):
        while not self.stop_event.is_set():
            try:
                sensor_value = self.value_provider()
            except Exception as exc:  # pylint: disable=broad-except
                print(f"[MQTT DEVICE] {self.device_id} - Value provider error: {exc}")
                break

            if sensor_value is not None:
                self._publish_value(sensor_value)

            if self.stop_event.wait(timeout=self.interval):
                break

    def send(self, payload: dict) -> bool:
        if not self.client:
            print(f"[MQTT DEVICE] {self.device_id} - MQTT client not connected")
            return False
        try:
            self.client.publish(self.topic, json.dumps(payload))
            return True
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[MQTT DEVICE] {self.device_id} - Error publishing: {exc}")
            return False
