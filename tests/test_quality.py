"""Tests: quality gates and the quarantine store."""

from cdc_pipeline.events import Operation
from cdc_pipeline.quality import (
    QuarantineStore,
    gate_no_unknown_columns,
    gate_required_columns,
    gate_value_ranges,
)
from tests.conftest import make_event


def test_required_columns_gate():
    gate = gate_required_columns(["order_id", "status"])
    event = make_event(1, after={"order_id": "ord-1"})
    assert gate(event, event.after) is not None
    assert gate(event, {"order_id": "ord-1", "status": "paid"}) is None


def test_value_range_gate_and_non_numeric():
    gate = gate_value_ranges({"amount_cents": (0, 1000)})
    assert gate(make_event(1), {"amount_cents": 500}) is None
    assert gate(make_event(1), {"amount_cents": 5000}) is not None
    assert gate(make_event(1), {"amount_cents": "lots"}) is not None
    assert gate(make_event(1), {}) is None  # absent column is not a failure


def test_unknown_columns_gate():
    gate = gate_no_unknown_columns(["order_id", "status"])
    assert gate(make_event(1), {"order_id": "x", "nope": 1}) is not None


def test_delete_skips_row_gates():
    gate = gate_required_columns(["order_id", "status"])
    event = make_event(1, op=Operation.DELETE)
    assert gate(event, {}) is None


def test_quarantine_store_dedups_and_retries():
    store = QuarantineStore()
    event = make_event(7)
    store.add(event, "bad range", "batch-0001")
    store.add(event, "bad range again", "batch-0002")
    assert len(store) == 1
    assert store.all()[0].attempts == 1
    removed = store.remove("synthetic-shop", 7)
    assert removed is not None and len(store) == 0
