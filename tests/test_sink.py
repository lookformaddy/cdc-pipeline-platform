"""Tests: the versioned sink — idempotency, tombstones, time travel."""

from cdc_pipeline.events import Operation
from cdc_pipeline.sink import TargetTable
from tests.conftest import make_event


def test_apply_and_read():
    sink = TargetTable(table="orders")
    assert sink.apply(make_event(1), schema_version=1) is True
    row = sink.read("ord-1")
    assert row is not None and row["status"] == "placed"
    assert "_deleted" not in row


def test_reapply_is_idempotent_noop():
    sink = TargetTable(table="orders")
    event = make_event(1)
    assert sink.apply(event, 1) is True
    assert sink.apply(event, 1) is False
    assert len(sink.versions) == 2  # initial + one apply


def test_delete_leaves_tombstone():
    sink = TargetTable(table="orders")
    sink.apply(make_event(1), 1)
    sink.apply(make_event(2, op=Operation.DELETE, after=None,
                          before={"order_id": "ord-1"}), 1)
    assert sink.read("ord-1") is None
    assert sink.tombstones() == ["ord-1"]
    # A replayed delete does not resurrect, and a re-insert after delete works.
    sink.apply(make_event(3, op=Operation.INSERT), 1)
    assert sink.read("ord-1") is not None


def test_time_travel_reads():
    sink = TargetTable(table="orders")
    sink.apply(make_event(1), 1)
    sink.apply(make_event(2, key="ord-2"), 1)
    sink.apply(make_event(3, op=Operation.DELETE, key="ord-1", after=None,
                          before={"order_id": "ord-1"}), 1)
    assert set(sink.read_as_of(1)) == {"ord-1"}
    assert set(sink.read_as_of(2)) == {"ord-1", "ord-2"}
    assert set(sink.read_as_of(3)) == {"ord-2"}
    assert set(sink.read_as_of(0)) == set()


def test_compact_keeps_current_state():
    sink = TargetTable(table="orders")
    for lsn in range(1, 15):
        sink.apply(make_event(lsn, key=f"ord-{lsn}"), 1)
    dropped = sink.compact(retain_versions=5)
    assert dropped == 10
    assert len(sink.versions) == 5
    assert len(sink.read_all()) == 14
