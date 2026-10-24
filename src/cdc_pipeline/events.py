"""Change events and the append-only source log.

A change event is the atomic unit of CDC: one row mutation, in one source
table, at one log sequence number (LSN). LSNs are monotonically increasing
*per source* and are the pipeline's single source of ordering truth — event
timestamps are informational only and must never be used to order applies.

Design notes:
- ``lsn`` is assigned by the source log, never by callers, so a replayed
  log and the original log agree on ordering.
- ``before``/``after`` carry full row images. Deletes carry ``before`` and
  a null ``after``; the sink turns them into tombstones.
- ``schema_version`` pins the schema the event was written against, so a
  later schema evolution can be applied (or quarantined) deterministically.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterator, Optional

from cdc_pipeline.errors import SourceLogError


class Operation(str, Enum):
    """Row-level mutation kinds a CDC feed can carry."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    SNAPSHOT = "snapshot"  # initial-load row, applied like an insert


@dataclass(frozen=True)
class ChangeEvent:
    """One row mutation from one source table."""

    source: str
    table: str
    lsn: int
    operation: Operation
    key: str
    before: Optional[dict[str, Any]] = None
    after: Optional[dict[str, Any]] = None
    event_time_ms: int = 0
    schema_version: int = 1
    transaction_id: Optional[str] = None

    def row_image(self) -> Optional[dict[str, Any]]:
        """The row image the sink should end up holding (None for deletes)."""
        if self.operation == Operation.DELETE:
            return None
        return self.after

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "table": self.table,
            "lsn": self.lsn,
            "operation": self.operation.value,
            "key": self.key,
            "before": self.before,
            "after": self.after,
            "event_time_ms": self.event_time_ms,
            "schema_version": self.schema_version,
            "transaction_id": self.transaction_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChangeEvent":
        return cls(
            source=data["source"],
            table=data["table"],
            lsn=int(data["lsn"]),
            operation=Operation(data["operation"]),
            key=str(data["key"]),
            before=data.get("before"),
            after=data.get("after"),
            event_time_ms=int(data.get("event_time_ms", 0)),
            schema_version=int(data.get("schema_version", 1)),
            transaction_id=data.get("transaction_id"),
        )


class SourceLog:
    """Append-only, LSN-ordered log for one source system.

    The log is the durable record the pipeline replays from. Appends are
    normally in LSN order; a late event (LSN below the current high-water
    mark) is accepted but flagged, because real CDC feeds do deliver
    out-of-order events and the pipeline — not the log — owns that policy.
    """

    def __init__(self, source: str) -> None:
        self.source = source
        self._events: list[ChangeEvent] = []
        self._by_lsn: dict[int, ChangeEvent] = {}
        self.high_water_mark: int = 0
        self.late_appends: int = 0

    def append(self, event: ChangeEvent) -> ChangeEvent:
        if event.source != self.source:
            raise SourceLogError(
                f"event source {event.source!r} does not match log {self.source!r}"
            )
        if event.lsn in self._by_lsn:
            raise SourceLogError(f"duplicate LSN {event.lsn} in source {self.source!r}")
        if event.lsn <= 0:
            raise SourceLogError(f"LSN must be positive, got {event.lsn}")
        self._events.append(event)
        self._by_lsn[event.lsn] = event
        if event.lsn > self.high_water_mark:
            self.high_water_mark = event.lsn
        else:
            self.late_appends += 1
        return event

    def next_lsn(self) -> int:
        """The LSN a new event appended in order would receive."""
        return self.high_water_mark + 1

    def events_after(self, lsn: int) -> list[ChangeEvent]:
        """All events with LSN strictly greater than ``lsn``, in LSN order."""
        return sorted(
            (e for e in self._events if e.lsn > lsn), key=lambda e: e.lsn
        )

    def events_in_range(self, start: int, end: int) -> list[ChangeEvent]:
        """Events with ``start <= lsn <= end``, in LSN order."""
        return sorted(
            (e for e in self._events if start <= e.lsn <= end),
            key=lambda e: e.lsn,
        )

    def __len__(self) -> int:
        return len(self._events)

    def __iter__(self) -> Iterator[ChangeEvent]:
        return iter(sorted(self._events, key=lambda e: e.lsn))

    def to_ndjson(self) -> str:
        """Serialize the log, one JSON object per line (LSN order)."""
        return "\n".join(json.dumps(e.to_dict()) for e in self)

    @classmethod
    def from_ndjson(cls, source: str, text: str) -> "SourceLog":
        log = cls(source)
        for line in text.splitlines():
            line = line.strip()
            if line:
                log.append(ChangeEvent.from_dict(json.loads(line)))
        return log
