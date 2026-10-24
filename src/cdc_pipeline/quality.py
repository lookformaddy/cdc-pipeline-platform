"""Data-quality gates and the quarantine store.

Two failure modes, one store:

- **Data-quality failures** — an event that violates a gate (null key,
  missing required column, out-of-range value, unknown column that is not
  a rename) is quarantined with a reason. The batch continues.
- **Schema-incompatible events** — an event that needs a schema evolution
  the target rejected is quarantined the same way, so a bad DDL never
  blocks replication.

Quarantine is a *store*, not a trash can: every entry keeps the original
event, the reason, and the batch it came from, so an operator can fix the
schema or the source and replay exactly the quarantined events.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from cdc_pipeline.events import ChangeEvent

# A gate is a callable returning None (pass) or a human-readable reason.
Gate = Callable[[ChangeEvent, dict[str, Any]], Optional[str]]


@dataclass
class QuarantinedEvent:
    event: ChangeEvent
    reason: str
    batch_id: str
    attempts: int = 0


class QuarantineStore:
    """Append-only store of quarantined events, keyed by (source, LSN)."""

    def __init__(self) -> None:
        self._entries: dict[tuple[str, int], QuarantinedEvent] = {}

    def add(self, event: ChangeEvent, reason: str, batch_id: str) -> None:
        key = (event.source, event.lsn)
        existing = self._entries.get(key)
        if existing is not None:
            existing.attempts += 1
            existing.reason = reason
            return
        self._entries[key] = QuarantinedEvent(
            event=event, reason=reason, batch_id=batch_id
        )

    def remove(self, source: str, lsn: int) -> Optional[QuarantinedEvent]:
        return self._entries.pop((source, lsn), None)

    def all(self) -> list[QuarantinedEvent]:
        return sorted(
            self._entries.values(), key=lambda e: (e.event.source, e.event.lsn)
        )

    def __len__(self) -> int:
        return len(self._entries)


def gate_non_null_key(event: ChangeEvent, row: dict[str, Any]) -> Optional[str]:
    if not event.key:
        return "null or empty row key"
    return None


def gate_required_columns(required: list[str]) -> Gate:
    """Build a gate requiring the given columns on non-delete rows."""

    def _gate(event: ChangeEvent, row: dict[str, Any]) -> Optional[str]:
        if event.operation.value == "delete":
            return None
        missing = [c for c in required if c not in row or row[c] is None]
        if missing:
            return f"missing required columns: {', '.join(missing)}"
        return None

    return _gate


def gate_value_ranges(ranges: dict[str, tuple[float, float]]) -> Gate:
    """Build a gate bounding numeric columns to [low, high]."""

    def _gate(event: ChangeEvent, row: dict[str, Any]) -> Optional[str]:
        for col, (low, high) in ranges.items():
            value = row.get(col)
            if value is None:
                continue
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return f"column {col!r} is not numeric: {value!r}"
            if not low <= numeric <= high:
                return f"column {col!r} out of range [{low}, {high}]: {value!r}"
        return None

    return _gate


def gate_no_unknown_columns(allowed: list[str]) -> Gate:
    """Build a gate rejecting columns outside the known schema."""

    def _gate(event: ChangeEvent, row: dict[str, Any]) -> Optional[str]:
        unknown = [c for c in row if c not in allowed]
        if unknown:
            return f"unknown columns: {', '.join(unknown)}"
        return None

    return _gate


@dataclass
class QualityReport:
    """Per-batch outcome of the quality gates."""

    passed: int = 0
    failed: int = 0
    failures: list[dict[str, Any]] = field(default_factory=list)

    def record_failure(self, event: ChangeEvent, reason: str) -> None:
        self.failed += 1
        self.failures.append(
            {"source": event.source, "lsn": event.lsn, "reason": reason}
        )
