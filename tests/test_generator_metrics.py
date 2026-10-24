"""Tests: generator determinism and metrics math."""

from cdc_pipeline.generator import generate_log
from cdc_pipeline.metrics import BatchMetrics, PipelineMetrics
from tests.conftest import make_event


def test_generator_is_deterministic():
    first = generate_log(seed=7, snapshot_rows=10, incremental_events=20)
    second = generate_log(seed=7, snapshot_rows=10, incremental_events=20)
    assert first[0].to_ndjson() == second[0].to_ndjson()
    third = generate_log(seed=8, snapshot_rows=10, incremental_events=20)
    assert first[0].to_ndjson() != third[0].to_ndjson()


def test_generator_counts_and_lsn_sequence():
    log, snapshot, incremental = generate_log(
        seed=42, snapshot_rows=10, incremental_events=20
    )
    assert len(log) == 30
    assert [e.lsn for e in log] == list(range(1, 31))
    assert all(e.operation.value == "snapshot" for e in snapshot)
    assert len(incremental) == 20


def test_metrics_summary_and_lag():
    metrics = PipelineMetrics()
    metrics.high_water_mark = 100
    metrics.last_applied_lsn = 90
    metrics.record_batch(BatchMetrics(
        batch_id="batch-0001", events_seen=10, applied=9,
        duplicates_skipped=1, duration_seconds=0.5,
    ))
    summary = metrics.summary()
    assert summary["events_seen"] == 10
    assert summary["applied"] == 9
    assert summary["lag_lsns"] == 10
    assert summary["mean_throughput_eps"] == 20.0


def test_batch_throughput_zero_duration():
    batch = BatchMetrics(batch_id="b", events_seen=5, duration_seconds=0.0)
    assert batch.throughput == 0.0
