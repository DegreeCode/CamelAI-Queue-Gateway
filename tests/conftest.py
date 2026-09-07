from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

import httpx

from app.config import Settings
from app.db import Database
from app.main import create_app

AsyncHandler = Callable[[httpx.Request], Awaitable[httpx.Response]]


class ChunkedStream(httpx.AsyncByteStream):
    def __init__(self, chunks: list[bytes], delay: float = 0.0) -> None:
        self.chunks = chunks
        self.delay = delay
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        import asyncio

        for chunk in self.chunks:
            if self.delay:
                await asyncio.sleep(self.delay)
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


@dataclass(slots=True)
class GatewayHarness:
    client: httpx.AsyncClient
    database: Database
    keys: dict[str, str]
    settings: Settings


def make_settings(
    tmp_path: Path,
    *,
    retry_post_generation: bool = False,
    max_retries: int = 1,
) -> Settings:
    return Settings(
        upstream_base_url="https://stream.camelai.test",
        camel_api_key="qaml_live_test_secret",
        db_path=tmp_path / "data" / "gateway.db",
        log_dir=tmp_path / "logs",
        spool_dir=tmp_path / "data" / "spool",
        lock_path=tmp_path / "data" / "gateway.lock",
        queue_poll_seconds=0.01,
        io_chunk_size=7,
        max_retries=max_retries,
        retry_post_generation=retry_post_generation,
        retry_max_delay_seconds=1.0,
    )


@asynccontextmanager
async def gateway_harness(
    tmp_path: Path,
    handler: AsyncHandler,
    *,
    key_names: tuple[str, ...] = ("hermes",),
    retry_post_generation: bool = False,
    max_retries: int = 1,
) -> AsyncIterator[GatewayHarness]:
    settings = make_settings(
        tmp_path,
        retry_post_generation=retry_post_generation,
        max_retries=max_retries,
    )
    database = Database(settings.db_path)
    database.initialize()
    keys: dict[str, str] = {}
    for name in key_names:
        plaintext, _ = database.create_key(name)
        keys[name] = plaintext

    app = create_app(
        settings,
        upstream_transport=httpx.MockTransport(handler),
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gateway.test",
        ) as client:
            yield GatewayHarness(
                client=client,
                database=database,
                keys=keys,
                settings=settings,
            )
