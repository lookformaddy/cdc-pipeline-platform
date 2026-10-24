"""The CDC pipeline orchestrator.

One pipeline instance owns one source table end to end:

1. **Snapshot** — the initial load: snapshot events are applied first,
   in LSN order, establishing the baseline.
2. **Incremental merge** — each batch is sorted by (source, LSN) before
   applying, so an out-of-order batch still lands in the right order.
3. **Exactly-once-ish apply** — (source, LSN) dedup at the sink makes
   re-delivery safe; a replayed batch changes nothing.
4. **Schema evolution** — a batch may carry schema changes; safe ones
   apply before the events that need them, incompatible ones quarantine
   the dependent events.
5. **Quality gates** — fail-closed per event; a failed event is
   quarantined, never silently dropped, and never blocks its batch.
6. **Late events** — an event whose LSN is below the applied watermark
   but not yet applied is still applied (out-of-order merge). An event
   whose LSN is below ``max_late_lsn`` behind the watermark — i.e. it
   arrived after its retention window closed — is quarantined as too late.
7. **Checkpoints** — after each batch the pipeline snapshots its state
   (applied LSNs, schema version, target version) so a crash resumes
   from the last checkpoint instead of the beginning of time.
8. **Backfill / replay** — re-read the source log from any checkpoint
   LSN and re-apply; dedup guarantees the replay is a no-op for events
   already applied and applies exactly the ones that were missed.

The pipeline never mutates the source log. All state lives in the sink,
the schema, the quarantine store, and the checkpoint — which is what
makes crash recovery a pure function of those four.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from typing import Any, Optional

from cdc_pipeline.errors import CheckpointError, ReplayError
from cdc_pipeline.events import ChangeEvent, Operation, SourceLog
from cdc_pipeline.metrics import BatchMetrics, PipelineMetrics, Timer
from cdc_pipeline.quality import (
    Gate,
    QualityReport,
    QuarantineStore,
    gate_non_null_key,
)
from cdc_pipeline.schema import SchemaChange, TableSchema
from cdc_pipeline.sink import TargetTable


@dataclass
class Checkpoint:
    """Durable pipeline state after a batch."""

    source: str
    table: str
    applied_lsn: int
    schema_version: int
    applied_markers: list[list[Any]]  # [source, lsn] pairs, JSON-safe
    quarantine_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "table": self.table,
            "applied_lsn": self.applied_lsn,
            "schema_version": self.schema_version,
            "applied_markers": self.applied_markers,
            "quarantine_count": self.quarantine_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Checkpoint":
        try:
            return cls(
                source=data["source"],
                table=data["table"],
                applied_lsn=int(data["applied_lsn"]),
                schema_version=int(data["schema_version"]),
                applied_markers=data["applied_markers"],
                quarantine_count=int(data["quarantine_count"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CheckpointError(f"invalid checkpoint payload: {exc}") from exc


@dataclass
class PipelineResult:
    batch: BatchMetrics
    quality: QualityReport
    checkpoint: Checkpoint


class CdcPipeline:
    """Snapshot + incremental CDC merge for one source table."""

    def __init__(
        self,
        source: str,
        table: str,
        schema: TableSchema,
        gates: Optional[list[Gate]] = None,
        max_late_lsns: int = 10_000,
        sink: Optional[TargetTable] = None,
        quarantine: Optional[QuarantineStore] = None,
    ) -> None:
        self.source = source
        self.table = table
        self.schema = schema
        self.gates: list[Gate] = list(gates or [gate_non_null_key])
        self.max_late_lsns = max_late_lsns
        self.sink = sink or TargetTable(table=table)
        self.quarantine = quarantine or QuarantineStore()
        self.metrics = PipelineMetrics()
        self.watermark = 0  # highest LSN applied (in order) so far
        self._batch_counter = 0

    # -- batch processing -------------------------------------------------

    def _next_batch_id(self) -> str:
        self._batch_counter += 1
        return f"batch-{self._batch_counter:04d}"

    def _row_for_gate(self, event: ChangeEvent) -> dict[str, Any]:
        """Row image the gates see: schema-translated, or {} for deletes."""
        if event.operation == Operation.DELETE:
            return {}
        row = dict(event.after or {})
        return self.schema.translate_row(row)

    def process_batch(
        self,
        events: list[ChangeEvent],
        schema_changes: Optional[list[SchemaChange]] = None,
    ) -> PipelineResult:
        """Apply one batch: schema changes first, then events in LSN order."""
        batch_id = self._next_batch_id()
        quality = QualityReport()
        batch = BatchMetrics(batch_id=batch_id)
        with Timer() as timer:
            # 1. Schema evolution. A failed change quarantines nothing by
            #    itself; events that depend on it fail their own gates.
            failed_changes: list[SchemaChange] = []
            for change in schema_changes or []:
                try:
                    self.schema = self.schema.apply(change)
                except Exception:
                    failed_changes.append(change)
            # 2. Order events deterministically, then apply.
            for event in sorted(events, key=lambda e: (e.source, e.lsn)):
                batch.events_seen += 1
                if self.sink.has_applied(event.source, event.lsn):
                    batch.duplicates_skipped += 1
                    continue
                if (
                    self.watermark > 0
                    and event.lsn < self.watermark
                    and (self.watermark - event.lsn) > self.max_late_lsns
                ):
                    batch.late_events += 1
                    quality.record_failure(event, "too late: outside retention window")
                    self.quarantine.add(event, "too late", batch_id)
                    batch.quarantined += 1
                    continue
                if event.lsn < self.watermark:
                    batch.late_events += 1
                row = self._row_for_gate(event)
                failed_reason: Optional[str] = None
                for gate in self.gates:
                    failed_reason = gate(event, row)
                    if failed_reason:
                        break
                if failed_reason:
                    quality.record_failure(event, failed_reason)
                    self.quarantine.add(event, failed_reason, batch_id)
                    batch.quarantined += 1
                    continue
                # Apply the schema-translated row, not the raw event image,
                # so renames/additions are reflected in the target.
                apply_event = event
                if event.operation != Operation.DELETE:
                    apply_event = replace(event, after=row)
                applied = self.sink.apply(apply_event, self.schema.version)
                if applied:
                    batch.applied += 1
                    if event.operation == Operation.DELETE:
                        batch.deletes += 1
                    self.watermark = max(self.watermark, event.lsn)
                else:  # pragma: no cover - sink dedup already checked above
                    batch.duplicates_skipped += 1
        batch.duration_seconds = timer.seconds
        self.metrics.high_water_mark = max(self.metrics.high_water_mark, self.watermark)
        self.metrics.last_applied_lsn = self.watermark
        self.metrics.record_batch(batch)
        quality.passed = batch.applied
        return PipelineResult(
            batch=batch, quality=quality, checkpoint=self.checkpoint()
        )

    # -- snapshot ----------------------------------------------------------

    def load_snapshot(self, events: list[ChangeEvent]) -> PipelineResult:
        """Initial load. Snapshot events must be INSERT/SNAPSHOT operations."""
        snapshot_events = [
            e for e in events if e.operation in (Operation.SNAPSHOT, Operation.INSERT)
        ]
        return self.process_batch(snapshot_events)

    # -- checkpoints and replay -------------------------------------------

    def checkpoint(self) -> Checkpoint:
        return Checkpoint(
            source=self.source,
            table=self.table,
            applied_lsn=self.watermark,
            schema_version=self.schema.version,
            applied_markers=[[s, l] for (s, l) in sorted(self.sink.applied)],
            quarantine_count=len(self.quarantine),
        )

    def save_checkpoint(self, path: str) -> Checkpoint:
        checkpoint = self.checkpoint()
        try:
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(checkpoint.to_dict(), fh, indent=2, sort_keys=True)
        except OSError as exc:
            raise CheckpointError(f"cannot write checkpoint to {path}: {exc}") from exc
        return checkpoint

    @classmethod
    def restore_checkpoint(cls, path: str) -> Checkpoint:
        try:
            with open(path, encoding="utf-8") as fh:
                return Checkpoint.from_dict(json.load(fh))
        except (OSError, ValueError) as exc:
            raise CheckpointError(f"cannot read checkpoint from {path}: {exc}") from exc

    def replay_from(
        self, log: SourceLog, from_lsn: int, batch_size: int = 500
    ) -> list[PipelineResult]:
        """Backfill/replay: re-apply log events after ``from_lsn``.

        Dedup makes replaying already-applied events a no-op, so replaying
        from an old checkpoint converges to the same target state — the
        property that makes crash recovery and backfills safe.
        """
        if from_lsn < 0:
            raise ReplayError(f"from_lsn must be >= 0, got {from_lsn}")
        events = log.events_after(from_lsn)
        results: list[PipelineResult] = []
        for i in range(0, len(events), batch_size):
            results.append(self.process_batch(events[i : i + batch_size]))
        return results

    def replay_quarantined(self) -> PipelineResult:
        """Retry every quarantined event once, in LSN order.

        Events that now pass (e.g. after a schema fix) are applied and
        removed from quarantine; the rest stay quarantined with an updated
        attempt count.
        """
        entries = self.quarantine.all()
        events = [e.event for e in entries]
        result = self.process_batch(events)
        for entry in entries:
            event = entry.event
            if self.sink.has_applied(event.source, event.lsn):
                self.quarantine.remove(event.source, event.lsn)
        return result
