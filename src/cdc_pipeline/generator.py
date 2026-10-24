"""Deterministic synthetic source generator.

Generates a fake "orders" table and a CDC event stream for it: an initial
snapshot, then inserts, updates, deletes, and schema evolution events —
including one deliberately incompatible change (a narrowing) so the demo
and tests exercise the quarantine path.

Everything is seeded: the same seed always produces the same log, which
is what makes the demo and the tests reproducible. The domain is generic
e-commerce orders; no real customer, product, or company data is involved.
"""

from __future__ import annotations

import random
from typing import Optional

from cdc_pipeline.events import ChangeEvent, Operation, SourceLog
from cdc_pipeline.schema import (
    ChangeKind,
    Column,
    ColumnType,
    SchemaChange,
    TableSchema,
)

SOURCE = "synthetic-shop"
TABLE = "orders"

BASE_SCHEMA = TableSchema(
    table=TABLE,
    columns=[
        Column("order_id", ColumnType.STRING, nullable=False),
        Column("customer_id", ColumnType.STRING, nullable=False),
        Column("amount_cents", ColumnType.INT, nullable=False),
        Column("status", ColumnType.STRING, nullable=False),
        Column("region", ColumnType.STRING, nullable=True),
    ],
)

STATUSES = ["placed", "paid", "shipped", "delivered", "cancelled"]
REGIONS = ["north", "south", "east", "west", "central"]


def _row(rng: random.Random, order_id: str) -> dict[str, object]:
    return {
        "order_id": order_id,
        "customer_id": f"cust-{rng.randint(1000, 9999)}",
        "amount_cents": rng.randint(500, 250_000),
        "status": rng.choice(STATUSES),
        "region": rng.choice(REGIONS),
    }


def generate_log(
    seed: int = 42,
    snapshot_rows: int = 50,
    incremental_events: int = 120,
) -> tuple[SourceLog, list[ChangeEvent], list[ChangeEvent]]:
    """Build a source log. Returns (log, snapshot_events, incremental_events).

    The log contains, in order: snapshot rows, then a mix of inserts,
    updates, and deletes. Schema evolution is *not* embedded in the log —
    the demo applies it as batch metadata (see examples/run_demo.py).
    """
    rng = random.Random(seed)
    log = SourceLog(SOURCE)
    lsn = 0
    snapshot: list[ChangeEvent] = []
    live: dict[str, dict[str, object]] = {}

    for i in range(snapshot_rows):
        order_id = f"ord-{100000 + i}"
        row = _row(rng, order_id)
        live[order_id] = row
        lsn += 1
        event = ChangeEvent(
            source=SOURCE,
            table=TABLE,
            lsn=lsn,
            operation=Operation.SNAPSHOT,
            key=order_id,
            after=dict(row),
            event_time_ms=1_700_000_000_000 + lsn,
        )
        log.append(event)
        snapshot.append(event)

    incremental: list[ChangeEvent] = []
    next_order = 100000 + snapshot_rows
    for _ in range(incremental_events):
        roll = rng.random()
        lsn += 1
        if roll < 0.35 or not live:
            order_id = f"ord-{next_order}"
            next_order += 1
            row = _row(rng, order_id)
            live[order_id] = row
            event = ChangeEvent(
                source=SOURCE, table=TABLE, lsn=lsn,
                operation=Operation.INSERT, key=order_id, after=dict(row),
                event_time_ms=1_700_000_000_000 + lsn,
            )
        elif roll < 0.80:
            order_id = rng.choice(sorted(live))
            before = dict(live[order_id])
            row = dict(before)
            row["status"] = rng.choice(STATUSES)
            if rng.random() < 0.5:
                row["amount_cents"] = rng.randint(500, 250_000)
            live[order_id] = row
            event = ChangeEvent(
                source=SOURCE, table=TABLE, lsn=lsn,
                operation=Operation.UPDATE, key=order_id,
                before=before, after=dict(row),
                event_time_ms=1_700_000_000_000 + lsn,
            )
        else:
            order_id = rng.choice(sorted(live))
            before = dict(live.pop(order_id))
            event = ChangeEvent(
                source=SOURCE, table=TABLE, lsn=lsn,
                operation=Operation.DELETE, key=order_id, before=before,
                event_time_ms=1_700_000_000_000 + lsn,
            )
        log.append(event)
        incremental.append(event)

    return log, snapshot, incremental


# Schema evolution script used by the demo: add a column, widen a type,
# rename a column (with mapping), then attempt an incompatible narrowing.
DEMO_SCHEMA_CHANGES: list[tuple[int, SchemaChange]] = [
    # (index into the incremental stream, change)
    (20, SchemaChange(kind=ChangeKind.ADD_COLUMN, column="channel",
                      new_type=ColumnType.STRING, nullable=True)),
    (50, SchemaChange(kind=ChangeKind.WIDEN_TYPE, column="amount_cents",
                      new_type=ColumnType.BIGINT)),
    (80, SchemaChange(kind=ChangeKind.RENAME_COLUMN, column="region",
                      new_name="sales_region")),
    # Incompatible: narrowing BIGINT back to INT is rejected and the
    # dependent events are quarantined, not applied.
    (100, SchemaChange(kind=ChangeKind.WIDEN_TYPE, column="amount_cents",
                       new_type=ColumnType.INT)),
]


def apply_demo_schema_script(
    pipeline: object, incremental: list[ChangeEvent]
) -> list[tuple[list[ChangeEvent], list[SchemaChange]]]:
    """Split the incremental stream into batches at schema-change points."""
    from cdc_pipeline.pipeline import CdcPipeline

    assert isinstance(pipeline, CdcPipeline)
    batches: list[tuple[list[ChangeEvent], list[SchemaChange]]] = []
    cursor = 0
    for index, change in DEMO_SCHEMA_CHANGES:
        batches.append((incremental[cursor:index], []))
        batches.append((incremental[index : index + 1], [change]))
        cursor = index + 1
    batches.append((incremental[cursor:], []))
    return batches
