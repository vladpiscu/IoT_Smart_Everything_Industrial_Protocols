from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable, Optional, Tuple, Union

from aiocoap import CHANGED, Context, Message, resource

logger = logging.getLogger(__name__)


class _CoapSensorResource(resource.Resource):
    def __init__(self, handler: Callable[[dict], Union[None, Awaitable[None]]]):
        super().__init__()
        self.handler = handler

    async def render_post(self, request: Message) -> Message:
        try:
            payload = request.payload.decode("utf-8")
            data = json.loads(payload)
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("[CoAP Collector] Failed to decode payload: %s", exc)
            return Message(code=CHANGED, payload=b"ERROR")

        result = self.handler(data)
        if asyncio.iscoroutine(result):
            await result
        logger.info("[CoAP Collector] Received %s", data)
        return Message(code=CHANGED, payload=b"ACK")


async def start_coap_collector(
    handler: Callable[[dict], Union[None, Awaitable[None]]],
    bind: Tuple[str, int] = ("localhost", 5683),
) -> None:
    root = resource.Site()
    root.add_resource(["sensor"], _CoapSensorResource(handler=handler))
    logger.info("[CoAP Collector] Listening on %s:%d", bind[0], bind[1])
    await Context.create_server_context(root, bind=bind)
    await asyncio.get_running_loop().create_future()


