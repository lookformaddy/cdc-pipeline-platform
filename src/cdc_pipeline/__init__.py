"""cdc_pipeline: an enterprise change-data-capture pipeline platform.

Clean-room implementation. All data is synthetic; no employer code, data,
or internal designs are included.
"""

from cdc_pipeline.events import ChangeEvent, Operation, SourceLog
from cdc_pipeline.schema import Column, ColumnType, SchemaChange, TableSchema
from cdc_pipeline.sink import TargetTable
from cdc_pipeline.pipeline import CdcPipeline, PipelineMetrics, PipelineResult

__all__ = [
    "ChangeEvent",
    "Operation",
    "SourceLog",
    "Column",
    "ColumnType",
    "SchemaChange",
    "TableSchema",
    "TargetTable",
    "CdcPipeline",
    "PipelineMetrics",
    "PipelineResult",
]

__version__ = "1.0.0"
