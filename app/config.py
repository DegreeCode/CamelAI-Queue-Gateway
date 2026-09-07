from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be one of true/false, yes/no, on/off, or 1/0")


def _env_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    value = default if raw is None else int(raw)
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


def _env_float(name: str, default: float, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    value = default if raw is None else float(raw)
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


def _load_secret() -> str:
    secret_file = os.getenv("CAMEL_API_KEY_FILE", "").strip()
    if secret_file:
        value = Path(secret_file).read_text(encoding="utf-8").strip()
    else:
        value = os.getenv("CAMEL_API_KEY", "").strip()
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    upstream_base_url: str
    camel_api_key: str
    db_path: Path
    log_dir: Path
    spool_dir: Path
    lock_path: Path
    connect_timeout_seconds: float = 10.0
    write_timeout_seconds: float = 60.0
    pool_timeout_seconds: float = 30.0
    queue_poll_seconds: float = 0.20
    io_chunk_size: int = 64 * 1024
    max_request_bytes: int = 0
    max_retries: int = 1
    retry_post_generation: bool = False
    retry_max_delay_seconds: float = 60.0

    @classmethod
    def from_env(cls, *, require_camel_key: bool = True) -> "Settings":
        data_dir = Path(os.getenv("GATEWAY_DATA_DIR", "/data"))
        camel_api_key = _load_secret()
        if require_camel_key and not camel_api_key:
            raise RuntimeError(
                "Set CAMEL_API_KEY or CAMEL_API_KEY_FILE before starting the gateway"
            )

        return cls(
            upstream_base_url=os.getenv(
                "CAMEL_UPSTREAM_BASE_URL", "https://stream.camelai.com"
            ).rstrip("/"),
            camel_api_key=camel_api_key,
            db_path=Path(os.getenv("GATEWAY_DB_PATH", str(data_dir / "gateway.db"))),
            log_dir=Path(os.getenv("GATEWAY_LOG_DIR", "/logs")),
            spool_dir=Path(
                os.getenv("GATEWAY_SPOOL_DIR", str(data_dir / "spool"))
            ),
            lock_path=Path(
                os.getenv("GATEWAY_LOCK_PATH", str(data_dir / "gateway.lock"))
            ),
            connect_timeout_seconds=_env_float(
                "GATEWAY_CONNECT_TIMEOUT_SECONDS", 10.0, 0.1
            ),
            write_timeout_seconds=_env_float(
                "GATEWAY_WRITE_TIMEOUT_SECONDS", 60.0, 0.1
            ),
            pool_timeout_seconds=_env_float(
                "GATEWAY_POOL_TIMEOUT_SECONDS", 30.0, 0.1
            ),
            queue_poll_seconds=_env_float("GATEWAY_QUEUE_POLL_SECONDS", 0.20, 0.01),
            io_chunk_size=_env_int("GATEWAY_IO_CHUNK_SIZE", 64 * 1024, 4096),
            max_request_bytes=_env_int("GATEWAY_MAX_REQUEST_BYTES", 0, 0),
            max_retries=_env_int("GATEWAY_MAX_RETRIES", 1, 0),
            retry_post_generation=_env_bool(
                "GATEWAY_RETRY_POST_GENERATION", False
            ),
            retry_max_delay_seconds=_env_float(
                "GATEWAY_RETRY_MAX_DELAY_SECONDS", 60.0, 0.0
            ),
        )

    def prepare_directories(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
