"""Tests: schema evolution rules."""

import pytest

from cdc_pipeline.errors import IncompatibleSchemaChange, SchemaError
from cdc_pipeline.generator import BASE_SCHEMA
from cdc_pipeline.schema import (
    ChangeKind,
    ColumnType,
    SchemaChange,
)


def test_add_column_bumps_version():
    schema = BASE_SCHEMA.apply(
        SchemaChange(kind=ChangeKind.ADD_COLUMN, column="channel",
                     new_type=ColumnType.STRING)
    )
    assert schema.version == BASE_SCHEMA.version + 1
    assert "channel" in schema.column_names()


def test_widen_int_to_bigint():
    schema = BASE_SCHEMA.apply(
        SchemaChange(kind=ChangeKind.WIDEN_TYPE, column="amount_cents",
                     new_type=ColumnType.BIGINT)
    )
    assert schema.get("amount_cents").type == ColumnType.BIGINT


def test_narrowing_is_incompatible():
    widened = BASE_SCHEMA.apply(
        SchemaChange(kind=ChangeKind.WIDEN_TYPE, column="amount_cents",
                     new_type=ColumnType.BIGINT)
    )
    with pytest.raises(IncompatibleSchemaChange):
        widened.apply(
            SchemaChange(kind=ChangeKind.WIDEN_TYPE, column="amount_cents",
                         new_type=ColumnType.INT)
        )


def test_drop_column_is_incompatible():
    with pytest.raises(IncompatibleSchemaChange):
        BASE_SCHEMA.apply(
            SchemaChange(kind=ChangeKind.DROP_COLUMN, column="region")
        )


def test_rename_maps_old_rows_forward():
    renamed = BASE_SCHEMA.apply(
        SchemaChange(kind=ChangeKind.RENAME_COLUMN, column="region",
                     new_name="sales_region")
    )
    row = {"order_id": "ord-1", "region": "north"}
    translated = renamed.translate_row(row)
    assert translated == {"order_id": "ord-1", "sales_region": "north"}


def test_rename_chain_and_unknown_column_drop():
    renamed = BASE_SCHEMA.apply(
        SchemaChange(kind=ChangeKind.RENAME_COLUMN, column="region",
                     new_name="sales_region")
    )
    renamed2 = renamed.apply(
        SchemaChange(kind=ChangeKind.RENAME_COLUMN, column="sales_region",
                     new_name="geo")
    )
    translated = renamed2.translate_row({"region": "west", "bogus": 1})
    assert translated == {"geo": "west"}


def test_add_existing_column_rejected():
    with pytest.raises(SchemaError):
        BASE_SCHEMA.apply(
            SchemaChange(kind=ChangeKind.ADD_COLUMN, column="region",
                         new_type=ColumnType.STRING)
        )


def test_decimal_widening_rules():
    from cdc_pipeline.schema import Column, TableSchema

    schema = TableSchema(
        table="t",
        columns=[Column("price", ColumnType.DECIMAL, precision=10, scale=2)],
    )
    wider = schema.apply(
        SchemaChange(kind=ChangeKind.WIDEN_TYPE, column="price",
                     new_type=ColumnType.DECIMAL, precision=18, scale=4)
    )
    assert wider.get("price").precision == 18
    with pytest.raises(IncompatibleSchemaChange):
        wider.apply(
            SchemaChange(kind=ChangeKind.WIDEN_TYPE, column="price",
                         new_type=ColumnType.DECIMAL, precision=12, scale=2)
        )
