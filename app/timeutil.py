from __future__ import annotations

from datetime import UTC, datetime


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_iso(value: datetime | None = None) -> str:
    current = value or utc_now()
    return current.astimezone(UTC).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def elapsed_ms(start: float, end: float) -> float:
    return round(max(0.0, (end - start) * 1000.0), 3)
