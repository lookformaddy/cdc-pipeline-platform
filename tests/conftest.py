"""Shared fixtures for the CDC pipeline tests."""

import pytest

from cdc_pipeline.events import ChangeEvent, Operation, SourceLog
from cdc_pipeline.generator import BASE_SCHEMA, generate_log
from cdc_pipeline.pipeline import CdcPipeline
from cdc_pipeline.quality import gate_required_columns, gate_value_ranges


@pytest.fixture
def log_and_events():
    log, snapshot, incremental = generate_log(
        seed=42, snapshot_rows=20, incremental_events=40
    )
    return log, snapshot, incremental


@pytest.fixture
def pipeline():
    return CdcPipeline(
        source="synthetic-shop",
        table="orders",
        schema=BASE_SCHEMA,
        gates=[
            gate_required_columns(
                ["order_id", "customer_id", "amount_cents", "status"]
            ),
            gate_value_ranges({"amount_cents": (0, 10_000_000)}),
        ],
    )


def make_event(lsn, op=Operation.INSERT, key="ord-1", after=None, before=None):
    return ChangeEvent(
        source="synthetic-shop",
        table="orders",
        lsn=lsn,
        operation=op,
        key=key,
        before=before,
        after=after or {
            "order_id": key,
            "customer_id": "cust-1",
            "amount_cents": 1000,
            "status": "placed",
            "region": "north",
        },
    )
