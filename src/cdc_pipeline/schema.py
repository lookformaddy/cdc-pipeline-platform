"""Table schemas and schema evolution.

A CDC pipeline outlives its source schema. This module owns the schema
version history and the rules for evolving it:

- ``add_column`` — always safe; backfilled with a default or NULL.
- ``widen_type`` — INT -> BIGINT, DECIMAL(p,s) -> wider (p,s); safe.
- ``rename_column`` — safe when a mapping is supplied; events written
  against the old name are translated, never dropped.
- Anything else (narrowing, type swap, dropping a column that still has
  data) is *incompatible*: the evolution itself is rejected and any event
  that requires it is quarantined, so a bad DDL can never silently corrupt
  the target.

Every evolution bumps ``version`` and is recorded in ``history`` so a
time-travel read can reconstruct the schema that was in force at any LSN.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from cdc_pipeline.errors import IncompatibleSchemaChange, SchemaError


class ColumnType(str, Enum):
    INT = "int"
    BIGINT = "bigint"
    DECIMAL = "decimal"
    STRING = "string"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"


# Safe widening transitions. DECIMAL widening is (precision, scale) checked
# numerically rather than by this table.
_WIDENING: dict[ColumnType, set[ColumnType]] = {
    ColumnType.INT: {ColumnType.BIGINT, ColumnType.DECIMAL},
    ColumnType.BIGINT: {ColumnType.DECIMAL},
}


@dataclass(frozen=True)
class Column:
    name: str
    type: ColumnType
    nullable: bool = True
    precision: Optional[int] = None  # DECIMAL only
    scale: Optional[int] = None  # DECIMAL only

    def can_widen_to(self, target: "Column") -> bool:
        if self.type == target.type:
            if self.type == ColumnType.DECIMAL:
                return (target.precision or 0) >= (self.precision or 0) and (
                    target.scale or 0
                ) >= (self.scale or 0)
            return True
        if target.type in _WIDENING.get(self.type, set()):
            return True
        return False


class ChangeKind(str, Enum):
    ADD_COLUMN = "add_column"
    WIDEN_TYPE = "widen_type"
    RENAME_COLUMN = "rename_column"
    DROP_COLUMN = "drop_column"


@dataclass(frozen=True)
class SchemaChange:
    kind: ChangeKind
    column: str
    new_name: Optional[str] = None
    new_type: Optional[ColumnType] = None
    nullable: bool = True
    precision: Optional[int] = None
    scale: Optional[int] = None
    default: Any = None


@dataclass
class TableSchema:
    """Versioned schema for one target table."""

    table: str
    columns: list[Column] = field(default_factory=list)
    version: int = 1
    # old_name -> new_name for every rename ever applied, in order.
    renames: dict[str, str] = field(default_factory=dict)
    history: list[dict[str, Any]] = field(default_factory=list)

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def get(self, name: str) -> Optional[Column]:
        for c in self.columns:
            if c.name == name:
                return c
        return None

    def translate_row(self, row: dict[str, Any]) -> dict[str, Any]:
        """Rewrite a row written against an older schema to this schema.

        Renamed columns are mapped forward; columns added since the row was
        written are absent (the sink backfills them); unknown columns are
        dropped rather than failing the whole event.
        """
        out: dict[str, Any] = {}
        for key, value in row.items():
            new_key = self.renames.get(key, key)
            if self.get(new_key) is not None:
                out[new_key] = value
        return out

    def apply(self, change: SchemaChange) -> "TableSchema":
        """Apply one evolution, returning the new schema version.

        Raises IncompatibleSchemaChange for unsafe evolutions; the caller
        (the pipeline) quarantines the offending event instead of applying.
        """
        cols = list(self.columns)
        if change.kind == ChangeKind.ADD_COLUMN:
            if self.get(change.column) is not None:
                raise SchemaError(f"column {change.column!r} already exists")
            cols.append(
                Column(
                    name=change.column,
                    type=change.new_type or ColumnType.STRING,
                    nullable=change.nullable,
                    precision=change.precision,
                    scale=change.scale,
                )
            )
        elif change.kind == ChangeKind.WIDEN_TYPE:
            existing = self.get(change.column)
            if existing is None:
                raise SchemaError(f"cannot widen unknown column {change.column!r}")
            target = Column(
                name=existing.name,
                type=change.new_type or existing.type,
                nullable=existing.nullable,
                precision=change.precision or existing.precision,
                scale=change.scale or existing.scale,
            )
            if not existing.can_widen_to(target):
                raise IncompatibleSchemaChange(
                    f"cannot widen {existing.type.value} to {target.type.value} "
                    f"for column {change.column!r}"
                )
            cols = [target if c.name == change.column else c for c in cols]
        elif change.kind == ChangeKind.RENAME_COLUMN:
            if change.new_name is None:
                raise SchemaError("rename requires new_name")
            existing = self.get(change.column)
            if existing is None:
                raise SchemaError(f"cannot rename unknown column {change.column!r}")
            if self.get(change.new_name) is not None:
                raise SchemaError(f"column {change.new_name!r} already exists")
            renamed = Column(
                name=change.new_name,
                type=existing.type,
                nullable=existing.nullable,
                precision=existing.precision,
                scale=existing.scale,
            )
            cols = [renamed if c.name == change.column else c for c in cols]
            renames = dict(self.renames)
            # Chain renames: anything previously renamed to the old name
            # now maps to the new name as well.
            for old, new in list(renames.items()):
                if new == change.column:
                    renames[old] = change.new_name
            renames[change.column] = change.new_name
        elif change.kind == ChangeKind.DROP_COLUMN:
            raise IncompatibleSchemaChange(
                f"dropping column {change.column!r} is incompatible: "
                "target history must keep the column (use a nullable "
                "deprecation instead)"
            )
        else:  # pragma: no cover - defensive
            raise SchemaError(f"unknown schema change kind {change.kind!r}")

        new_schema = TableSchema(
            table=self.table,
            columns=cols,
            version=self.version + 1,
            renames=renames if change.kind == ChangeKind.RENAME_COLUMN else dict(self.renames),
            history=list(self.history),
        )
        new_schema.history.append(
            {"version": new_schema.version, "change": change.kind.value,
             "column": change.column, "new_name": change.new_name}
        )
        return new_schema
