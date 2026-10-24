"""Environment-driven configuration.

All knobs have safe defaults; nothing here is required to run the demo.
Copy ``.env.example`` to ``.env`` and override what you need.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    source: str = os.environ.get("CDC_SOURCE", "synthetic-shop")
    table: str = os.environ.get("CDC_TABLE", "orders")
    seed: int = _int("CDC_SEED", 42)
    snapshot_rows: int = _int("CDC_SNAPSHOT_ROWS", 50)
    incremental_events: int = _int("CDC_INCREMENTAL_EVENTS", 120)
    max_late_lsns: int = _int("CDC_MAX_LATE_LSNS", 10_000)
    batch_size: int = _int("CDC_BATCH_SIZE", 50)
    checkpoint_path: str = os.environ.get("CDC_CHECKPOINT_PATH", "checkpoint.json")
    host: str = os.environ.get("CDC_HOST", "127.0.0.1")
    port: int = _int("CDC_PORT", 8000)
