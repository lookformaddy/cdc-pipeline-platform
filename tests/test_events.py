"""Tests: change events and the source log."""

import pytest

from cdc_pipeline.errors import SourceLogError
from cdc_pipeline.events import ChangeEvent, Operation, SourceLog
from tests.conftest import make_event


def test_log_assigns_and_orders_by_lsn():
    log = SourceLog("synthetic-shop")
    log.append(make_event(3))
    log.append(make_event(1))
    log.append(make_event(2))
    assert [e.lsn for e in log] == [1, 2, 3]
    assert log.high_water_mark == 3


def test_duplicate_lsn_rejected():
    log = SourceLog("synthetic-shop")
    log.append(make_event(1))
    with pytest.raises(SourceLogError):
        log.append(make_event(1))


def test_wrong_source_rejected():
    log = SourceLog("synthetic-shop")
    event = make_event(1)
    event = ChangeEvent(
        source="other", table=event.table, lsn=event.lsn,
        operation=event.operation, key=event.key, after=event.after,
    )
    with pytest.raises(SourceLogError):
        log.append(event)


def test_late_append_flagged_but_accepted():
    log = SourceLog("synthetic-shop")
    log.append(make_event(5))
    log.append(make_event(4))  # late arrival
    assert log.late_appends == 1
    assert log.high_water_mark == 5


def test_events_after_and_range():
    log = SourceLog("synthetic-shop")
    for lsn in range(1, 6):
        log.append(make_event(lsn, key=f"ord-{lsn}"))
    assert [e.lsn for e in log.events_after(2)] == [3, 4, 5]
    assert [e.lsn for e in log.events_in_range(2, 4)] == [2, 3, 4]


def test_ndjson_round_trip():
    log = SourceLog("synthetic-shop")
    log.append(make_event(1))
    log.append(make_event(2, op=Operation.DELETE, after=None,
                          before={"order_id": "ord-1"}))
    restored = SourceLog.from_ndjson("synthetic-shop", log.to_ndjson())
    assert [e.lsn for e in restored] == [1, 2]
    assert restored.events_after(1)[0].operation == Operation.DELETE


def test_event_row_image_none_for_delete():
    event = make_event(1, op=Operation.DELETE, after=None)
    assert event.row_image() is None
    assert make_event(1).row_image() is not None
