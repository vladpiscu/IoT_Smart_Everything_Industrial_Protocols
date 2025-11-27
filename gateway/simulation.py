from __future__ import annotations

import asyncio
import logging
import socket
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from collector.coap_collector import start_coap_collector as start_coap_collector_func
from collector.modbus_collector import ModbusCollectorConfig, start_modbus_collector
from collector.mqtt_collector import MqttCollector
from gateway.core import Gateway
from network.proxy import TcpProxy, TcpProxyConfig, UdpProxy, UdpProxyConfig
from network.simulation import NetworkConditions

logger = logging.getLogger(__name__)


class GatewaySimulation:
    """Manages message collection from MQTT, CoAP, and Modbus protocols."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.gateway = Gateway(csv_filename=config.get("csv_filename", "gateway_data.csv"))
        # Store shared network config if available
        self.shared_network_config = config.get("network")

        self._tasks: List[asyncio.Task] = []
        self._mqtt_collector: Optional[MqttCollector] = None
        self._mqtt_thread: Optional[threading.Thread] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._proxy_tasks: List[asyncio.Task] = []

    async def run(self) -> None:
        """Start all collectors and wait until stopped."""
        if self._stop_event is not None:
            raise RuntimeError("Collectors already running")

        self._stop_event = asyncio.Event()
        
        # Start collectors based on config
        self.start_mqtt_collector()
        await self.start_coap_collector()
        await self.start_modbus_collector()

        try:
            await self._stop_event.wait()
        finally:
            await self.stop()

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        if self._mqtt_collector:
            try:
                self._mqtt_collector.stop()
            except Exception as exc:  # pylint: disable=broad-except
                logger.error("Error stopping MQTT collector: %s", exc)
        self._mqtt_collector = None

        if self._mqtt_thread and self._mqtt_thread.is_alive():
            self._mqtt_thread.join(timeout=2.0)
        self._mqtt_thread = None

        if self._stop_event:
            self._stop_event = None

        for task in self._proxy_tasks:
            task.cancel()
        if self._proxy_tasks:
            await asyncio.gather(*self._proxy_tasks, return_exceptions=True)
        self._proxy_tasks.clear()

    def request_stop(self) -> None:
        if self._stop_event:
            self._stop_event.set()

    # ------------------------------------------------------------------ #
    # MQTT                                                              #
    # ------------------------------------------------------------------ #
    def start_mqtt_collector(self) -> Optional[Tuple[str, int]]:
        """Start MQTT collector. Returns (host, port) endpoint for devices to connect to."""
        config = self.config.get("mqtt", {})
        if not config.get("enabled", True):
            return None

        proxy_cfg = config.get("proxy", {})
        proxy_endpoint: Optional[Tuple[str, int]] = None

        if proxy_cfg.get("enabled"):
            proxy_endpoint = self._start_tcp_proxy(
                proxy_cfg,
                default_listen_port=proxy_cfg.get("listen_port", config.get("port", 1883) + 1000),
                upstream_host=config.get("broker", "localhost"),
                upstream_port=config.get("port", 1883),
            )

        def handle_message(data: Dict[str, Any]) -> None:
            status = data.get("status", "ok")
            self.gateway.ingest_payload(data, protocol="mqtt", status=status)

        self._mqtt_collector = MqttCollector(
            csv_filename=self.gateway.csv_filename,
            broker=config.get("broker", "localhost"),
            port=config.get("port", 1883),
            topic=config.get("topic", "iot"),
            keepalive=config.get("keepalive", 60),
            on_message=handle_message,
        )

        self._mqtt_thread = threading.Thread(target=self._mqtt_collector.start, daemon=True)
        self._mqtt_thread.start()
        time.sleep(1.0)
        logger.info("[Simulation] MQTT collector started")
        # Return proxy endpoint if enabled, otherwise return broker endpoint
        return proxy_endpoint if proxy_endpoint else (config.get("broker", "localhost"), config.get("port", 1883))

    # ------------------------------------------------------------------ #
    # CoAP                                                              #
    # ------------------------------------------------------------------ #
    async def start_coap_collector(self) -> Optional[Tuple[str, int, str]]:
        """Start CoAP collector. Returns (proxy_host, proxy_port, endpoint) if proxy enabled, (bind_host, bind_port, endpoint) otherwise."""
        config = self.config.get("coap", {})
        if not config.get("enabled", True):
            return None

        device_cfg = config.get("device", {})
        proxy_cfg = config.get("proxy", {})

        endpoint = device_cfg.get("endpoint", "coap://localhost:5683/sensor")
        parsed = urlparse(endpoint)
        bind_host = parsed.hostname or "localhost"
        bind_host = self._resolve_ipv4_host(bind_host)
        bind_port = parsed.port or 5683
        path = parsed.path or "/sensor"
        endpoint = f"coap://{bind_host}:{bind_port}{path}"

        if proxy_cfg.get("enabled"):
            proxy_host, proxy_port = self._start_udp_proxy(
                proxy_cfg,
                default_listen_port=proxy_cfg.get("listen_port", bind_port + 1000),
                upstream_host=bind_host,
                upstream_port=bind_port,
            )
            endpoint = f"coap://{proxy_host}:{proxy_port}{path}"

        async def collector_handler(data: Dict[str, Any]) -> None:
            status = data.get("status", "ok")
            self.gateway.ingest_payload(data, protocol="coap", status=status)

        self._tasks.append(
            asyncio.create_task(start_coap_collector_func(handler=collector_handler, bind=(bind_host, bind_port)))
        )
        logger.info("[Simulation] CoAP collector started on %s:%d", bind_host, bind_port)
        return (proxy_host if proxy_cfg.get("enabled") else bind_host, 
                proxy_port if proxy_cfg.get("enabled") else bind_port, 
                endpoint)

    # ------------------------------------------------------------------ #
    # Modbus                                                            #
    # ------------------------------------------------------------------ #
    async def start_modbus_collector(self) -> Optional[Tuple[str, int]]:
        """Start Modbus collector. Returns proxy endpoint if proxy is enabled, (host, port) otherwise."""
        config = self.config.get("modbus", {})
        if not config.get("enabled", True):
            return None

        host = config.get("host", "localhost")
        port = config.get("port", 5020)
        proxy_cfg = config.get("proxy", {})
        collector_devices: List[ModbusCollectorConfig] = []
        proxy_endpoint: Optional[Tuple[str, int]] = None

        if proxy_cfg.get("enabled"):
            proxy_endpoint = self._start_tcp_proxy(
                proxy_cfg,
                default_listen_port=proxy_cfg.get("listen_port", port + 1000),
                upstream_host=host,
                upstream_port=port,
            )

        def collector_handler(data: Dict[str, Any]) -> None:
            status = data.get("status", "ok")
            self.gateway.ingest_payload(data, protocol="modbus", status=status)

        for device_cfg in config.get("devices", []):
            collector_devices.append(
                ModbusCollectorConfig(
                    unit_id=device_cfg.get("unit_id", 1),
                    device_id=device_cfg.get("device_id", f"modbus_device_{device_cfg.get('unit_id', 1)}"),
                    sensor=device_cfg.get("sensor", "temperature"),
                    start_address=device_cfg.get("start_address", 0),
                    register_count=device_cfg.get("register_count", 10),
                )
            )

        if collector_devices:
            self._tasks.append(
                asyncio.create_task(
                    start_modbus_collector(
                        callback=collector_handler,
                        devices=collector_devices,
                        address=(host, port),
                    )
                )
            )
            logger.info("[Simulation] Modbus collector started on %s:%d", host, port)
        
        return proxy_endpoint if proxy_endpoint else (host, port)

    # ------------------------------------------------------------------ #
    # Helpers                                                           #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _build_network_conditions(config: Optional[Dict[str, Any]]) -> Optional[NetworkConditions]:
        if not config:
            return None

        latency = tuple(config.get("latency_range_ms", (0, 0)))
        failure_duration = tuple(config.get("failure_duration_range_s", (5.0, 15.0)))
        return NetworkConditions(
            loss_rate=config.get("loss_rate", 0.0),
            latency_range_ms=(float(latency[0]), float(latency[1])),
            failure_probability=config.get("failure_probability", 0.0),
            failure_duration_range_s=(float(failure_duration[0]), float(failure_duration[1])),
            recovery_check_interval_s=float(config.get("recovery_check_interval_s", 1.0)),
            random_seed=config.get("random_seed"),
        )

    @staticmethod
    def _find_free_port(host: str, socktype: int) -> int:
        family = socket.AF_INET
        with socket.socket(family, socktype) as temp:
            temp.bind((host, 0))
            return temp.getsockname()[1]

    @staticmethod
    def _resolve_ipv4_host(host: str) -> str:
        infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
        if not infos:
            return host
        return infos[0][4][0]

    def _start_tcp_proxy(
        self,
        proxy_cfg: Dict[str, Any],
        default_listen_port: int,
        upstream_host: str,
        upstream_port: int,
    ) -> Tuple[str, int]:
        listen_host = proxy_cfg.get("listen_host", "localhost")
        listen_port = proxy_cfg.get("listen_port", default_listen_port)
        if not listen_port or listen_port == 0:
            listen_port = self._find_free_port(listen_host, socket.SOCK_DGRAM)
        if not listen_port or listen_port == 0:
            listen_port = self._find_free_port(listen_host, socket.SOCK_STREAM)
        upstream_host = proxy_cfg.get("upstream_host", upstream_host)
        upstream_port = proxy_cfg.get("upstream_port", upstream_port)
        # Use shared network config if available, otherwise use protocol-specific config
        network_cfg = self.shared_network_config or proxy_cfg.get("network")
        conditions = self._build_network_conditions(network_cfg)
        config_obj = TcpProxyConfig(
            listen_host=listen_host,
            listen_port=listen_port,
            upstream_host=upstream_host,
            upstream_port=upstream_port,
            conditions=conditions,
        )
        try:
            proxy = TcpProxy(config_obj)
        except OSError as exc:
            if listen_port != 0:
                logger.warning(
                    "[Simulation] Failed to bind TCP proxy on %s:%d (%s); retrying with ephemeral port",
                    listen_host,
                    listen_port,
                    exc,
                )
                listen_port = self._find_free_port(listen_host, socket.SOCK_STREAM)
                config_obj = TcpProxyConfig(
                    listen_host=listen_host,
                    listen_port=listen_port,
                    upstream_host=upstream_host,
                    upstream_port=upstream_port,
                    conditions=conditions,
                )
                proxy = TcpProxy(config_obj)
            else:
                raise
        task = asyncio.create_task(proxy.run())
        self._proxy_tasks.append(task)
        logger.info(
            "[Simulation] TCP proxy listening on %s:%d forwarding to %s:%d",
            listen_host,
            listen_port,
            upstream_host,
            upstream_port,
        )
        return listen_host, listen_port

    def _start_udp_proxy(
        self,
        proxy_cfg: Dict[str, Any],
        default_listen_port: int,
        upstream_host: str,
        upstream_port: int,
    ) -> Tuple[str, int]:
        listen_host = proxy_cfg.get("listen_host", "localhost")
        listen_port = proxy_cfg.get("listen_port", default_listen_port)
        upstream_host = proxy_cfg.get("upstream_host", upstream_host)
        upstream_port = proxy_cfg.get("upstream_port", upstream_port)
        # Use shared network config if available, otherwise use protocol-specific config
        network_cfg = self.shared_network_config or proxy_cfg.get("network")
        conditions = self._build_network_conditions(network_cfg)
        config_obj = UdpProxyConfig(
            listen_host=listen_host,
            listen_port=listen_port,
            upstream_host=upstream_host,
            upstream_port=upstream_port,
            conditions=conditions,
        )
        try:
            proxy = UdpProxy(config_obj)
        except OSError as exc:
            if listen_port != 0:
                logger.warning(
                    "[Simulation] Failed to bind UDP proxy on %s:%d (%s); retrying with ephemeral port",
                    listen_host,
                    listen_port,
                    exc,
                )
                listen_port = self._find_free_port(listen_host, socket.SOCK_DGRAM)
                config_obj = UdpProxyConfig(
                    listen_host=listen_host,
                    listen_port=listen_port,
                    upstream_host=upstream_host,
                    upstream_port=upstream_port,
                    conditions=conditions,
                )
                proxy = UdpProxy(config_obj)
            else:
                raise
        task = asyncio.create_task(proxy.run())
        self._proxy_tasks.append(task)
        actual_host, actual_port = proxy.listen_address
        logger.info(
            "[Simulation] UDP proxy listening on %s:%d forwarding to %s:%d",
            actual_host,
            actual_port,
            upstream_host,
            upstream_port,
        )
        return actual_host, actual_port

