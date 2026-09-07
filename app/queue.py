from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field


class ClientDisconnectedBeforeUpstream(Exception):
    """The client went away before its FIFO ticket could run."""


DisconnectCheck = Callable[[], Awaitable[bool]]


@dataclass(slots=True)
class _Ticket:
    request_id: str
    ready_event: asyncio.Event = field(default_factory=asyncio.Event)
    ready_to_run: bool = False
    granted: bool = False
    abandoned: bool = False


class QueueLease:
    def __init__(self, queue: "GlobalFIFOQueue", request_id: str) -> None:
        self._queue = queue
        self.request_id = request_id
        self._released = False

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        await self._queue.release(self.request_id)

    async def __aenter__(self) -> "QueueLease":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        await self.release()


class QueueReservation:
    """FIFO position reserved before authentication/body spooling completes."""

    def __init__(self, queue: "GlobalFIFOQueue", ticket: _Ticket) -> None:
        self._queue = queue
        self._ticket = ticket
        self._finished = False

    @property
    def request_id(self) -> str:
        return self._ticket.request_id

    async def wait(self, disconnected: DisconnectCheck) -> QueueLease:
        if self._finished:
            raise RuntimeError("Queue reservation is no longer usable")
        try:
            lease = await self._queue._wait_for_ticket(self._ticket, disconnected)
        except BaseException:
            self._finished = True
            raise
        self._finished = True
        return lease

    async def cancel(self) -> None:
        if self._finished:
            return
        self._finished = True
        await self._queue._abandon(self._ticket)


class GlobalFIFOQueue:
    """A strict in-process FIFO with exactly one granted inference lease."""

    def __init__(self, poll_seconds: float = 0.20) -> None:
        self._poll_seconds = poll_seconds
        self._lock = asyncio.Lock()
        self._waiters: deque[_Ticket] = deque()
        self._active_request_id: str | None = None

    async def reserve(self, request_id: str) -> QueueReservation:
        ticket = _Ticket(request_id=request_id)
        async with self._lock:
            self._waiters.append(ticket)
        return QueueReservation(self, ticket)

    async def acquire(
        self, request_id: str, disconnected: DisconnectCheck
    ) -> QueueLease:
        reservation = await self.reserve(request_id)
        return await reservation.wait(disconnected)

    async def _wait_for_ticket(
        self, ticket: _Ticket, disconnected: DisconnectCheck
    ) -> QueueLease:
        async with self._lock:
            if ticket.abandoned:
                raise ClientDisconnectedBeforeUpstream(ticket.request_id)
            ticket.ready_to_run = True
            self._grant_next_locked()

        try:
            while not ticket.ready_event.is_set():
                try:
                    await asyncio.wait_for(
                        ticket.ready_event.wait(), timeout=self._poll_seconds
                    )
                except TimeoutError:
                    if await self._safe_disconnected(disconnected):
                        await self._abandon(ticket)
                        raise ClientDisconnectedBeforeUpstream(ticket.request_id)

            if await self._safe_disconnected(disconnected):
                await self._abandon(ticket)
                raise ClientDisconnectedBeforeUpstream(ticket.request_id)
            return QueueLease(self, ticket.request_id)
        except ClientDisconnectedBeforeUpstream:
            raise
        except BaseException:
            await self._abandon(ticket)
            raise

    async def release(self, request_id: str) -> None:
        async with self._lock:
            if self._active_request_id != request_id:
                return
            self._active_request_id = None
            self._grant_next_locked()

    async def snapshot(self) -> tuple[int, bool, str | None]:
        async with self._lock:
            depth = sum(1 for item in self._waiters if not item.abandoned)
            return depth, self._active_request_id is not None, self._active_request_id

    async def _abandon(self, ticket: _Ticket) -> None:
        async with self._lock:
            if ticket.abandoned:
                return
            ticket.abandoned = True
            if ticket.granted and self._active_request_id == ticket.request_id:
                self._active_request_id = None
                self._grant_next_locked()
                return
            try:
                self._waiters.remove(ticket)
            except ValueError:
                pass
            self._grant_next_locked()

    def _grant_next_locked(self) -> None:
        if self._active_request_id is not None:
            return
        while self._waiters:
            ticket = self._waiters[0]
            if ticket.abandoned:
                self._waiters.popleft()
                continue
            if not ticket.ready_to_run:
                return
            self._waiters.popleft()
            ticket.granted = True
            self._active_request_id = ticket.request_id
            ticket.ready_event.set()
            return

    @staticmethod
    async def _safe_disconnected(disconnected: DisconnectCheck) -> bool:
        try:
            return bool(await disconnected())
        except Exception:
            return False
