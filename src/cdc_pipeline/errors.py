"""Error taxonomy for the CDC pipeline.

Every failure is classified so the pipeline can decide, per failure class,
whether to quarantine the event, fail the batch, or abort the run. Nothing
here retries blindly: a quarantined event never blocks the rest of the batch.
"""


class CdcError(Exception):
    """Base class for all CDC pipeline errors."""


class SchemaError(CdcError):
    """A schema-level problem (unknown column, incompatible type, ...)."""


class IncompatibleSchemaChange(SchemaError):
    """A schema change that cannot be applied safely (narrowing, type swap)."""


class DataQualityError(CdcError):
    """A data-quality gate rejected an event or batch."""


class CheckpointError(CdcError):
    """A checkpoint could not be written, read, or validated."""


class ReplayError(CdcError):
    """A backfill/replay request is invalid (unknown checkpoint, bad range)."""


class SourceLogError(CdcError):
    """The source log itself is inconsistent (duplicate LSN, gap, ...)."""
