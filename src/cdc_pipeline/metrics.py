"""Pipeline metrics: lag, throughput, and per-batch accounting.

Lag is measured in *LSNs*, not wall-clock time, because LSN distance is
what an operator can act on ("replication is 4,200 events behind") while
clock skew between source and sink makes time-based lag unreliable. The
wall-clock throughput (events/second) is tracked separately, over batch
windows, so a slow batch shows up as a throughput dip rather than a lag
spike.
"""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass, field


@dataclass
class BatchMetrics:
    batch_id: str
    events_seen: int = 0
    applied: int = 0
    duplicates_skipped: int = 0
    quarantined: int = 0
    late_events: int = 0
    deletes: int = 0
    duration_seconds: float = 0.0

    @property
    def throughput(self) -> float:
        if self.duration_seconds <= 0:
            return 0.0
        return self.events_seen / self.duration_seconds


@dataclass
class PipelineMetrics:
    """Rolling metrics for one pipeline instance."""

    batches: list[BatchMetrics] = field(default_factory=list)
    high_water_mark: int = 0
    last_applied_lsn: int = 0

    def record_batch(self, batch: BatchMetrics) -> None:
        self.batches.append(batch)
        self.last_applied_lsn = max(self.last_applied_lsn, 0)

    @property
    def total_seen(self) -> int:
        return sum(b.events_seen for b in self.batches)

    @property
    def total_applied(self) -> int:
        return sum(b.applied for b in self.batches)

    @property
    def total_duplicates(self) -> int:
        return sum(b.duplicates_skipped for b in self.batches)

    @property
    def total_quarantined(self) -> int:
        return sum(b.quarantined for b in self.batches)

    @property
    def total_late(self) -> int:
        return sum(b.late_events for b in self.batches)

    @property
    def lag_lsns(self) -> int:
        """How far behind the source high-water mark replication is."""
        return max(0, self.high_water_mark - self.last_applied_lsn)

    @property
    def mean_throughput(self) -> float:
        rates = [b.throughput for b in self.batches if b.events_seen]
        return statistics.mean(rates) if rates else 0.0

    @property
    def p95_batch_seconds(self) -> float:
        durations = sorted(b.duration_seconds for b in self.batches)
        if not durations:
            return 0.0
        index = min(len(durations) - 1, int(0.95 * len(durations)))
        return durations[index]

    def summary(self) -> dict[str, float | int]:
        return {
            "batches": len(self.batches),
            "events_seen": self.total_seen,
            "applied": self.total_applied,
            "duplicates_skipped": self.total_duplicates,
            "quarantined": self.total_quarantined,
            "late_events": self.total_late,
            "lag_lsns": self.lag_lsns,
            "mean_throughput_eps": round(self.mean_throughput, 2),
            "p95_batch_seconds": round(self.p95_batch_seconds, 4),
        }


class Timer:
    """Context manager that measures a batch window in seconds."""

    def __init__(self) -> None:
        self.seconds = 0.0

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *exc: object) -> None:
        self.seconds = time.perf_counter() - self._start
