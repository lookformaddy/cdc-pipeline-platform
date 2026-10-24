"""The versioned target table (the sink).

The sink is deliberately boring: an in-memory, versioned key-value table
whose only job is to apply events *idempotently* and to answer
"time-travel" reads. Every applied batch creates a new immutable version
tagged with the batch's max LSN, so a reader can ask for the table "as of
LSN N" and get exactly the state replication had reached at that point.

Semantics that matter:

- **Idempotency.** ``apply`` records every (source, LSN) it has applied.
  Re-applying the same event is a no-op that returns ``False`` — this is
  what makes at-least-once delivery safe and gives the pipeline its
  "exactly-once-ish" guarantee.
- **Ordering.** Events are applied in LSN order within a batch. A delete
  leaves a *tombstone* (the key stays, marked deleted) so a later replay
  cannot resurrect the row and so downstream consumers can distinguish
  "never existed" from "deleted".
- **Deletes of unknown keys** are accepted (idempotent deletes) and
  recorded as tombstones.
- **Compaction** drops versions older than a retention horizon and purges
  tombstones older than the horizon, keeping time-travel bounded.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from cdc_pipeline.events import ChangeEvent, Operation


@dataclass
class TableVersion:
    """One immutable snapshot of the table, tagged by max applied LSN."""

    lsn: int
    rows: dict[str, dict[str, Any]]  # key -> row (rows carry _deleted flag)
    schema_version: int


@dataclass
class TargetTable:
    table: str
    applied: set[tuple[str, int]] = field(default_factory=set)
    versions: list[TableVersion] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.versions:
            self.versions.append(
                TableVersion(lsn=0, rows={}, schema_version=1)
            )

    @property
    def current(self) -> TableVersion:
        return self.versions[-1]

    @property
    def current_lsn(self) -> int:
        return self.current.lsn

    def has_applied(self, source: str, lsn: int) -> bool:
        return (source, lsn) in self.applied

    def apply(self, event: ChangeEvent, schema_version: int) -> bool:
        """Apply one event idempotently. Returns True if it changed state.

        The caller must have already ordered events and run quality gates;
        this method only enforces idempotency and tombstone semantics.
        """
        marker = (event.source, event.lsn)
        if marker in self.applied:
            return False
        rows = {k: dict(v) for k, v in self.current.rows.items()}
        if event.operation == Operation.DELETE:
            rows[event.key] = {"_deleted": True}
        else:
            row = dict(event.after or {})
            row["_deleted"] = False
            rows[event.key] = row
        self.applied.add(marker)
        self.versions.append(
            TableVersion(
                lsn=max(event.lsn, self.current_lsn),
                rows=rows,
                schema_version=schema_version,
            )
        )
        return True

    def read(self, key: str) -> Optional[dict[str, Any]]:
        """Current live row for ``key`` (None if absent or tombstoned)."""
        row = self.current.rows.get(key)
        if row is None or row.get("_deleted"):
            return None
        return {k: v for k, v in row.items() if k != "_deleted"}

    def read_all(self) -> dict[str, dict[str, Any]]:
        """All current live rows."""
        return {
            k: {c: v for c, v in row.items() if c != "_deleted"}
            for k, row in self.current.rows.items()
            if not row.get("_deleted")
        }

    def read_as_of(self, lsn: int) -> dict[str, dict[str, Any]]:
        """Live rows as of LSN ``lsn`` (latest version with version.lsn <= lsn)."""
        chosen = self.versions[0]
        for version in self.versions:
            if version.lsn <= lsn:
                chosen = version
            else:
                break
        return {
            k: {c: v for c, v in row.items() if c != "_deleted"}
            for k, row in chosen.rows.items()
            if not row.get("_deleted")
        }

    def tombstones(self) -> list[str]:
        return [
            k for k, row in self.current.rows.items() if row.get("_deleted")
        ]

    def compact(self, retain_versions: int = 10, lsn_horizon: Optional[int] = None) -> int:
        """Drop old versions; purge tombstones at/below ``lsn_horizon``.

        Returns the number of versions dropped. The newest version is
        always kept, so reads never lose the current state.
        """
        dropped = 0
        if len(self.versions) > retain_versions:
            dropped = len(self.versions) - retain_versions
            self.versions = self.versions[-retain_versions:]
        if lsn_horizon is not None:
            current = self.current
            kept = {
                k: v
                for k, v in current.rows.items()
                if not (v.get("_deleted") and current.lsn >= lsn_horizon)
            }
            # Only rewrite if something was actually purged.
            if len(kept) != len(current.rows):
                self.versions[-1] = TableVersion(
                    lsn=current.lsn,
                    rows=kept,
                    schema_version=current.schema_version,
                )
        return dropped
