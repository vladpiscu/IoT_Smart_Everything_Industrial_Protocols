from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from pathlib import Path
from typing import List, Optional, Tuple

from devices.coap_device import CoapDevice, heart_rate_dataset_provider, random_heart_rate_provider
from devices.humidity_mqtt_device import HumidityMqttDevice
from devices.light_mqtt_device import LightMqttDevice
from devices.modbus_device import (
    ModbusDevice,
    air_quality_dataset_provider,
    random_temperature_provider,
)
from devices.temperature_mqtt_device import TemperatureMqttDevice
from gateway.config import load_gateway_config
from gateway.simulation import GatewaySimulation
from sent_events_logger import start_logger, stop_logger, flush_logger, set_csv_filename

logger = logging.getLogger(__name__)

MQTT_DEVICE_MAP = {
    "temperature": TemperatureMqttDevice,
    "humidity": HumidityMqttDevice,
    "light": LightMqttDevice,
}


async def run_simulation(config_path: str | None = None) -> None:
    config = load_gateway_config(config_path or "gateway_config.json")
    logging.basicConfig(level=getattr(logging, config.get("logging", {}).get("level", "INFO")))

    # Extract loss_rate from network config
    network_config = config.get("network", {})
    loss_rate = network_config.get("loss_rate", 0.0)
    
    # Ensure loss_rate is a valid number
    try:
        loss_rate = float(loss_rate)
    except (ValueError, TypeError):
        loss_rate = 0.0
        logger.warning(f"Invalid loss_rate in config, defaulting to 0.0")
    
    # Create experiments directory
    experiments_dir = Path("experiments")
    experiments_dir.mkdir(exist_ok=True)
    
    # Generate filenames with loss_rate suffix
    # Format: loss_0.1 -> loss_0_1 (replace . with _ for filename compatibility)
    # Handle edge cases like 0.01 -> 0_01, 0.001 -> 0_001
    loss_rate_str = str(loss_rate).replace(".", "_")
    sent_events_filename = experiments_dir / f"sent_events_loss_{loss_rate_str}.csv"
    gateway_data_filename = experiments_dir / f"gateway_data_loss_{loss_rate_str}.csv"
    
    # Update config with new gateway data filename
    config["csv_filename"] = str(gateway_data_filename)
    
    # Set sent events filename
    set_csv_filename(str(sent_events_filename))
    
    logger.info(f"Using filenames: sent_events={sent_events_filename}, gateway_data={gateway_data_filename}")

    # Start the sent events logger thread
    start_logger()

    simulation = GatewaySimulation(config=config)
    
    # Start collectors and get proxy endpoints
    mqtt_proxy_endpoint = simulation.start_mqtt_collector()
    coap_endpoint_info = await simulation.start_coap_collector()
    modbus_endpoint = await simulation.start_modbus_collector()
    
    # Device management
    mqtt_devices: List = []
    device_tasks: List[asyncio.Task] = []
    
    # Start MQTT devices
    mqtt_config = config.get("mqtt", {})
    if mqtt_config.get("enabled", True) and mqtt_proxy_endpoint:
        target_host = mqtt_proxy_endpoint[0]
        target_port = mqtt_proxy_endpoint[1]
        
        for device_cfg in mqtt_config.get("devices", []):
            sensor = device_cfg.get("sensor", "temperature")
            device_cls = MQTT_DEVICE_MAP.get(sensor)
            if device_cls is None:
                logger.warning("Unknown MQTT sensor type '%s'; skipping", sensor)
                continue

            count = int(device_cfg.get("count", 1))
            interval = device_cfg.get("interval", mqtt_config.get("device_interval", 2.0))
            for idx in range(count):
                device = device_cls(
                    device_number=idx + 1,
                    interval=interval,
                    broker=target_host,
                    port=target_port,
                    topic=mqtt_config.get("topic", "iot"),
                    keepalive=mqtt_config.get("keepalive", 60),
                )
                device.start()
                mqtt_devices.append(device)
                logger.info("[Simulation] Started MQTT %s device %s", sensor, device.device_id)
    
    # Start CoAP device
    coap_config = config.get("coap", {})
    if coap_config.get("enabled", True) and coap_endpoint_info:
        device_cfg = coap_config.get("device", {})
        proxy_host, proxy_port, endpoint = coap_endpoint_info
        
        interval = device_cfg.get("interval_s", 4.0)
        provider = None
        data_source = device_cfg.get("data_source", "dataset")
        if data_source == "dataset":
            dataset_path = device_cfg.get("dataset_path", "health.csv")
            try:
                provider = heart_rate_dataset_provider(csv_path=dataset_path)
                logger.info("[Simulation] Using heart rate dataset from %s", dataset_path)
            except Exception as exc:  # pylint: disable=broad-except
                logger.warning(
                    "[Simulation] Failed to load heart rate dataset (%s); falling back to random values",
                    exc,
                )

        if provider is None:
            value_range = device_cfg.get("value_range", [65, 90])
            provider = random_heart_rate_provider(value_range[0], value_range[1])

        coap_device = CoapDevice(
            device_id=device_cfg.get("device_id", "wearable_coap_1"),
            sensor=device_cfg.get("sensor", "heart_rate"),
            value_provider=provider,
            interval_s=interval,
            endpoint=endpoint,
        )
        device_tasks.append(asyncio.create_task(coap_device.run()))
        logger.info("[Simulation] Started CoAP device %s (endpoint=%s)", coap_device.device_id, endpoint)
    
    # Start Modbus devices
    modbus_config = config.get("modbus", {})
    if modbus_config.get("enabled", True) and modbus_endpoint:
        target_host = modbus_endpoint[0]
        target_port = modbus_endpoint[1]
        
        for device_cfg in modbus_config.get("devices", []):
            sensor_type = device_cfg.get("sensor", "temperature")
            value_provider = None

            if sensor_type == "air_quality":
                dataset_path = device_cfg.get("dataset_path", "AirQualityUCI.csv")
                column = device_cfg.get("column", "PT08.S1(CO)")
                try:
                    value_provider = air_quality_dataset_provider(
                        csv_path=dataset_path,
                        column=column,
                    )
                    logger.info(
                        "[Simulation] Modbus air quality device using %s (%s)",
                        dataset_path,
                        column,
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning(
                        "[Simulation] Failed to load air quality dataset (%s); falling back to random temperature values",
                        exc,
                    )

            if value_provider is None:
                base_value = device_cfg.get("base_value", 80.0)
                variation = device_cfg.get("variation", 2.0)
                value_provider = random_temperature_provider(base=base_value, variation=variation)

            modbus_device = ModbusDevice(
                device_id=device_cfg.get("device_id", f"modbus_device_{device_cfg.get('unit_id', 1)}"),
                sensor=sensor_type,
                unit_id=device_cfg.get("unit_id", 1),
                start_address=device_cfg.get("start_address", 0),
                value_provider=value_provider,
                interval_s=device_cfg.get("interval_s", 3.0),
                host=target_host,
                port=target_port,
            )
            device_tasks.append(asyncio.create_task(modbus_device.run()))
            logger.info("[Simulation] Started Modbus device %s", modbus_device.device_id)
    
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        logging.info("Shutdown signal received.")
        stop_event.set()
        simulation.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Signal handlers aren't available on some platforms (e.g., Windows event loop).
            pass

    try:
        # Wait for stop signal while devices are running
        await stop_event.wait()
    finally:
        # Cleanup devices
        for device in mqtt_devices:
            try:
                device.stop()
            except Exception as exc:  # pylint: disable=broad-except
                logger.error("Error stopping MQTT device %s: %s", getattr(device, "device_id", "unknown"), exc)
        
        for task in device_tasks:
            task.cancel()
        if device_tasks:
            await asyncio.gather(*device_tasks, return_exceptions=True)
        
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.remove_signal_handler(sig)
            except NotImplementedError:
                pass
        
        # Give devices a moment to finish sending any in-flight events
        logger.info("Waiting for devices to finish sending events...")
        await asyncio.sleep(1.0)
        
        # Flush any remaining events in the logger queue
        flush_logger(timeout=5.0)
        
        # Stop the sent events logger thread (will flush remaining events)
        stop_logger()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the multi-protocol gateway simulation.")
    parser.add_argument("--config", type=str, default="gateway_config.json", help="Path to gateway configuration file.")
    args = parser.parse_args()
    try:
        asyncio.run(run_simulation(args.config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

