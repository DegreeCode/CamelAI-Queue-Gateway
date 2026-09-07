from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from .config import Settings
from .db import Database
from .process_lock import SingleProcessLock
from .proxy import ALLOWED_ENDPOINTS, GatewayProxy
from .queue import GlobalFIFOQueue


def create_app(
    settings: Settings | None = None,
    *,
    upstream_transport: httpx.AsyncBaseTransport | None = None,
) -> Starlette:
    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        active_settings = settings or Settings.from_env(require_camel_key=True)
        active_settings.prepare_directories()

        process_lock = SingleProcessLock(active_settings.lock_path)
        process_lock.acquire()
        client: httpx.AsyncClient | None = None
        try:
            database = Database(active_settings.db_path)
            database.initialize()
            database.mark_inflight_interrupted()
            queue = GlobalFIFOQueue(active_settings.queue_poll_seconds)

            timeout = httpx.Timeout(
                connect=active_settings.connect_timeout_seconds,
                read=None,
                write=active_settings.write_timeout_seconds,
                pool=active_settings.pool_timeout_seconds,
            )
            client = httpx.AsyncClient(
                transport=upstream_transport,
                timeout=timeout,
                follow_redirects=False,
                trust_env=False,
                limits=httpx.Limits(
                    max_connections=20, max_keepalive_connections=10
                ),
            )
            proxy = GatewayProxy(
                settings=active_settings,
                database=database,
                queue=queue,
                client=client,
            )
            app.state.settings = active_settings
            app.state.database = database
            app.state.queue = queue
            app.state.proxy = proxy
            app.state.upstream_client = client
            app.state.process_lock = process_lock

            yield
        finally:
            if client is not None:
                await client.aclose()
            process_lock.release()

    async def proxy_endpoint(request: Request) -> Response:
        return await request.app.state.proxy.handle(request)

    async def healthz(request: Request) -> Response:
        depth, busy, _ = await request.app.state.queue.snapshot()
        return JSONResponse(
            {"status": "ok", "queue_depth": depth, "upstream_busy": busy}
        )

    routes = [Route("/healthz", healthz, methods=["GET"])]
    routes.extend(
        Route(path, proxy_endpoint, methods=[policy.method])
        for path, policy in ALLOWED_ENDPOINTS.items()
    )
    return Starlette(debug=False, routes=routes, lifespan=lifespan)


app = create_app()
