from __future__ import annotations

import asyncio
import logging
import socket
from dataclasses import dataclass
from typing import Optional, Tuple

from network.simulation import NetworkConditions, NetworkEvent, NetworkSimulator

logger = logging.getLogger(__name__)


def _build_simulator(conditions: Optional[NetworkConditions]) -> Optional[NetworkSimulator]:
    if not conditions:
        return None
    return NetworkSimulator(conditions)


@dataclass
class TcpProxyConfig:
    listen_host: str
    listen_port: int
    upstream_host: str
    upstream_port: int
    conditions: Optional[NetworkConditions] = None


class TcpProxy:
    """Simple TCP proxy with optional fault injection."""

    def __init__(self, config: TcpProxyConfig) -> None:
        self.config = config
        self._server: Optional[asyncio.AbstractServer] = None

    async def run(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client,
            host=self.config.listen_host,
            port=self.config.listen_port,
        )
        sockets = ", ".join(
            f"{sock.getsockname()[0]}:{sock.getsockname()[1]}"
            for sock in self._server.sockets or []
        )
        logger.info("[Proxy][TCP] Listening on %s -> %s:%d", sockets, self.config.upstream_host, self.config.upstream_port)
        async with self._server:
            await self._server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        logger.debug("[Proxy][TCP] Accepted connection from %s", peer)
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(
                self.config.upstream_host,
                self.config.upstream_port,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("[Proxy][TCP] Failed to connect upstream: %s", exc)
            writer.close()
            await writer.wait_closed()
            return

        upstream_peer = upstream_writer.get_extra_info("peername")
        logger.debug("[Proxy][TCP] Bridging %s <-> %s", peer, upstream_peer)

        client_sim = _build_simulator(self.config.conditions)
        upstream_sim = _build_simulator(self.config.conditions)

        async def _pipe(src: asyncio.StreamReader, dest: asyncio.StreamWriter, simulator: Optional[NetworkSimulator], direction: str) -> None:
            try:
                while True:
                    data = await src.read(4096)
                    if not data:
                        break
                    if simulator:
                        event = await simulator.apply_async()
                        if event is NetworkEvent.FAILURE:
                            logger.warning("[Proxy][TCP] Simulated failure (%s); closing connection", direction)
                            dest.close()
                            await dest.wait_closed()
                            return
                        if simulator.should_drop():
                            logger.debug("[Proxy][TCP] Dropped %d bytes (%s)", len(data), direction)
                            continue
                        delay = simulator.latency_delay()
                        if delay > 0:
                            logger.debug("[Proxy][TCP] Delaying %s by %.3fs", direction, delay)
                            await asyncio.sleep(delay)
                    dest.write(data)
                    await dest.drain()
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # pylint: disable=broad-except
                logger.debug("[Proxy][TCP] Pipe error (%s): %s", direction, exc)
            finally:
                dest.close()
                with contextlib.suppress(Exception):
                    await dest.wait_closed()

        import contextlib  # local import to avoid top-level dependency

        tasks = [
            asyncio.create_task(_pipe(reader, upstream_writer, client_sim, "upstream")),
            asyncio.create_task(_pipe(upstream_reader, writer, upstream_sim, "downstream")),
        ]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        logger.debug("[Proxy][TCP] Connection %s closed", peer)


@dataclass
class UdpProxyConfig:
    listen_host: str
    listen_port: int
    upstream_host: str
    upstream_port: int
    conditions: Optional[NetworkConditions] = None


class UdpProxy:
    """Simple UDP proxy with optional fault injection."""

    def __init__(self, config: UdpProxyConfig) -> None:
        self.config = config
        self._loop = asyncio.get_running_loop()
        self._listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._upstream_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._upstream_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listen_socket.setblocking(False)
        self._upstream_socket.setblocking(False)
        try:
            self._listen_socket.bind((self.config.listen_host, self.config.listen_port))
        except Exception:
            self._listen_socket.close()
            self._upstream_socket.close()
            raise
        try:
            self._upstream_socket.bind((self.config.listen_host, 0))
        except OSError:
            # Fallback to default binding if specific host binding fails.
            self._upstream_socket.bind(("0.0.0.0", 0))
        self._listen_address = self._listen_socket.getsockname()
        # Suppress WSAECONNRESET on Windows when sending to unreachable endpoints.
        if hasattr(socket, "SIO_UDP_CONNRESET"):
            try:
                self._listen_socket.ioctl(socket.SIO_UDP_CONNRESET, False)  # type: ignore[attr-defined]
                self._upstream_socket.ioctl(socket.SIO_UDP_CONNRESET, False)  # type: ignore[attr-defined]
            except OSError:
                logger.debug("[Proxy][UDP] Could not disable UDP connreset; continuing.")
        self._client_addr: Optional[Tuple[str, int]] = None
        self._upstream_addr = self._resolve_address(self.config.upstream_host, self.config.upstream_port)
        self._client_simulator = _build_simulator(self.config.conditions)
        self._upstream_simulator = _build_simulator(self.config.conditions)

    async def run(self) -> None:
        logger.info(
            "[Proxy][UDP] Listening on %s:%d -> %s:%d",
            self.config.listen_host,
            self.config.listen_port,
            self.config.upstream_host,
            self.config.upstream_port,
        )
        tasks = [
            asyncio.create_task(self._forward_client_to_server()),
            asyncio.create_task(self._forward_server_to_client()),
        ]
        await asyncio.gather(*tasks)

    @property
    def listen_address(self) -> Tuple[str, int]:
        return self._listen_address  # type: ignore[return-value]

    @staticmethod
    def _resolve_address(host: str, port: int) -> Tuple[str, int]:
        infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_DGRAM)
        if not infos:
            raise OSError(f"Unable to resolve {host}:{port}")
        return infos[0][4]

    async def _forward_client_to_server(self) -> None:
        while True:
            data, addr = await self._loop.sock_recvfrom(self._listen_socket, 4096)
            logger.debug("[Proxy][UDP] Received %d bytes from client %s", len(data), addr)
            self._client_addr = addr
            if self._client_simulator:
                event = await self._client_simulator.apply_async()
                if event is NetworkEvent.FAILURE:
                    logger.warning("[Proxy][UDP] Simulated client failure; dropping packet")
                    continue
                if self._client_simulator.should_drop():
                    logger.debug("[Proxy][UDP] Dropped datagram from %s", addr)
                    continue
                delay = self._client_simulator.latency_delay()
                if delay > 0:
                    await asyncio.sleep(delay)
            try:
                await self._loop.sock_sendto(self._upstream_socket, data, self._upstream_addr)
                logger.debug("[Proxy][UDP] Forwarded %d bytes to upstream %s:%d", len(data), self.config.upstream_host, self.config.upstream_port)
            except OSError as exc:
                logger.debug("[Proxy][UDP] Error sending upstream: %s", exc)

    async def _forward_server_to_client(self) -> None:
        while True:
            data, addr = await self._loop.sock_recvfrom(self._upstream_socket, 4096)
            logger.debug("[Proxy][UDP] Received %d bytes from upstream %s", len(data), addr)
            if not self._client_addr:
                continue
            if self._upstream_simulator:
                event = await self._upstream_simulator.apply_async()
                if event is NetworkEvent.FAILURE:
                    logger.warning("[Proxy][UDP] Simulated upstream failure; dropping packet")
                    continue
                if self._upstream_simulator.should_drop():
                    logger.debug("[Proxy][UDP] Dropped upstream datagram from %s", addr)
                    continue
                delay = self._upstream_simulator.latency_delay()
                if delay > 0:
                    await asyncio.sleep(delay)
            try:
                await self._loop.sock_sendto(self._listen_socket, data, self._client_addr)
                logger.debug("[Proxy][UDP] Forwarded %d bytes to client %s", len(data), self._client_addr)
            except OSError as exc:
                logger.debug("[Proxy][UDP] Error sending to client %s: %s", self._client_addr, exc)

