"""Tests: checkpoints, backfill/replay, quarantine replay."""

import json

import pytest

from cdc_pipeline.errors import CheckpointError, ReplayError
from cdc_pipeline.pipeline import CdcPipeline
from tests.conftest import make_event


def test_checkpoint_round_trip(pipeline, tmp_path):
    pipeline.process_batch([make_event(1), make_event(2)])
    path = str(tmp_path / "ckpt.json")
    checkpoint = pipeline.save_checkpoint(path)
    assert checkpoint.applied_lsn == 2
    restored = CdcPipeline.restore_checkpoint(path)
    assert restored.applied_lsn == 2
    assert restored.table == "orders"
    assert len(restored.applied_markers) == 2


def test_corrupt_checkpoint_rejected(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    with pytest.raises(CheckpointError):
        CdcPipeline.restore_checkpoint(str(bad))


def test_replay_from_checkpoint_is_idempotent(pipeline, log_and_events):
    log, snapshot, incremental = log_and_events
    pipeline.load_snapshot(snapshot)
    pipeline.process_batch(incremental)
    applied_before = pipeline.metrics.total_applied
    results = pipeline.replay_from(log, from_lsn=0)
    assert pipeline.metrics.total_applied == applied_before
    assert all(r.batch.applied == 0 for r in results)


def test_replay_recovers_missed_tail(pipeline, log_and_events):
    log, snapshot, incremental = log_and_events
    pipeline.load_snapshot(snapshot)
    first_half = incremental[: len(incremental) // 2]
    pipeline.process_batch(first_half)
    tail_lsn = first_half[-1].lsn
    results = pipeline.replay_from(log, from_lsn=tail_lsn)
    applied = sum(r.batch.applied for r in results)
    assert applied == len(incremental) - len(first_half)
    assert pipeline.watermark == log.high_water_mark


def test_replay_rejects_negative_lsn(pipeline, log_and_events):
    log, _, _ = log_and_events
    with pytest.raises(ReplayError):
        pipeline.replay_from(log, from_lsn=-1)


def test_quarantine_replay_after_fix(pipeline):
    bad = make_event(1, after={"order_id": "ord-1", "customer_id": "c",
                              "amount_cents": -5, "status": "placed"})
    pipeline.process_batch([bad])
    assert len(pipeline.quarantine) == 1
    # Fix the gate: widen the allowed range, then replay quarantine.
    from cdc_pipeline.quality import gate_value_ranges
    pipeline.gates = [gate_value_ranges({"amount_cents": (-100, 10_000_000)})]
    result = pipeline.replay_quarantined()
    assert result.batch.applied == 1
    assert len(pipeline.quarantine) == 0
