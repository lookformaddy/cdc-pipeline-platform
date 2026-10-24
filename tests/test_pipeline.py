"""Tests: pipeline orchestration — snapshot, merge, dedup, late, gates."""

from cdc_pipeline.events import Operation
from cdc_pipeline.schema import ChangeKind, ColumnType, SchemaChange
from tests.conftest import make_event


def test_snapshot_then_incremental(pipeline, log_and_events):
    log, snapshot, incremental = log_and_events
    result = pipeline.load_snapshot(snapshot)
    assert result.batch.applied == len(snapshot)
    assert len(pipeline.sink.read_all()) == len(snapshot)
    result = pipeline.process_batch(incremental)
    assert result.batch.applied + result.batch.quarantined == len(incremental)
    # Deletes in the stream produced tombstones or live rows, never crashes.
    assert pipeline.watermark == log.high_water_mark


def test_batch_is_sorted_before_apply(pipeline):
    events = [make_event(3, key="ord-3"), make_event(1, key="ord-1"),
              make_event(2, key="ord-2")]
    pipeline.process_batch(events)
    assert pipeline.watermark == 3
    assert len(pipeline.sink.read_all()) == 3


def test_duplicate_delivery_is_skipped(pipeline):
    event = make_event(1)
    pipeline.process_batch([event])
    result = pipeline.process_batch([event])
    assert result.batch.duplicates_skipped == 1
    assert result.batch.applied == 0


def test_out_of_order_late_event_still_applies(pipeline):
    pipeline.process_batch([make_event(1), make_event(3)])
    result = pipeline.process_batch([make_event(2, key="ord-2")])
    assert result.batch.applied == 1
    assert result.batch.late_events == 1
    assert pipeline.sink.read("ord-2") is not None


def test_too_late_event_is_quarantined(pipeline):
    pipeline.max_late_lsns = 2
    pipeline.process_batch([make_event(10)])
    result = pipeline.process_batch([make_event(5, key="ord-5")])
    assert result.batch.quarantined == 1
    assert len(pipeline.quarantine) == 1
    assert pipeline.sink.read("ord-5") is None


def test_quality_failure_quarantines_but_batch_continues(pipeline):
    bad = make_event(1, after={"order_id": "ord-1", "customer_id": "c",
                              "amount_cents": -5, "status": "placed"})
    good = make_event(2, key="ord-2")
    result = pipeline.process_batch([bad, good])
    assert result.batch.quarantined == 1
    assert result.batch.applied == 1
    assert pipeline.sink.read("ord-2") is not None


def test_schema_evolution_applies_then_events_use_it(pipeline):
    add = SchemaChange(kind=ChangeKind.ADD_COLUMN, column="channel",
                       new_type=ColumnType.STRING)
    renamed = SchemaChange(kind=ChangeKind.RENAME_COLUMN, column="region",
                           new_name="sales_region")
    pipeline.process_batch([make_event(1)], schema_changes=[add, renamed])
    assert pipeline.schema.version == 3
    # Old-name rows translate to the new column name.
    event = make_event(2, key="ord-2")
    pipeline.process_batch([event])
    row = pipeline.sink.read("ord-2")
    assert row is not None and row.get("sales_region") == "north"
    assert "region" not in row


def test_incompatible_change_quarantines_nothing_but_keeps_schema(pipeline):
    widen = SchemaChange(kind=ChangeKind.WIDEN_TYPE, column="amount_cents",
                         new_type=ColumnType.BIGINT)
    narrow = SchemaChange(kind=ChangeKind.WIDEN_TYPE, column="amount_cents",
                          new_type=ColumnType.INT)
    pipeline.process_batch([make_event(1)], schema_changes=[widen])
    assert pipeline.schema.version == 2
    pipeline.process_batch([make_event(2, key="ord-2")], schema_changes=[narrow])
    # Narrowing rejected: schema stays BIGINT, event still applies fine.
    assert pipeline.schema.version == 2
    assert pipeline.sink.read("ord-2") is not None


def test_delete_creates_tombstone_and_counts(pipeline):
    pipeline.process_batch([make_event(1)])
    result = pipeline.process_batch(
        [make_event(2, op=Operation.DELETE, key="ord-1", after=None,
                    before={"order_id": "ord-1"})]
    )
    assert result.batch.deletes == 1
    assert pipeline.sink.tombstones() == ["ord-1"]
