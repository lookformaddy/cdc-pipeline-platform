"""Optional FastAPI service surface.

Import-guarded: importing this module without FastAPI installed raises a
clear error, and the service tests auto-skip in that case (they run in CI,
where FastAPI is installed). The service is a thin adapter over
:class:`CdcPipeline` — it owns no CDC logic of its own.
"""

from __future__ import annotations

from typing import Any

try:
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel

    _FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised by the skip path
    FastAPI = None  # type: ignore
    HTTPException = Exception  # type: ignore
    BaseModel = object  # type: ignore
    _FASTAPI_AVAILABLE = False

from cdc_pipeline.events import ChangeEvent
from cdc_pipeline.pipeline import CdcPipeline

FASTAPI_AVAILABLE = _FASTAPI_AVAILABLE


if _FASTAPI_AVAILABLE:

    class IngestRequest(BaseModel):
        events: list[dict[str, Any]]


def create_app(pipeline: CdcPipeline) -> "FastAPI":
    if not _FASTAPI_AVAILABLE:
        raise RuntimeError("FastAPI is not installed: pip install fastapi")
    app = FastAPI(title="cdc-pipeline-platform")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "table": pipeline.table}

    @app.post("/ingest")
    def ingest(payload: IngestRequest) -> dict[str, Any]:
        events = [ChangeEvent.from_dict(e) for e in payload.events]
        result = pipeline.process_batch(events)
        return {
            "batch_id": result.batch.batch_id,
            "applied": result.batch.applied,
            "duplicates_skipped": result.batch.duplicates_skipped,
            "quarantined": result.batch.quarantined,
            "watermark": pipeline.watermark,
        }

    @app.get("/tables/{table}/rows/{key}")
    def get_row(table: str, key: str) -> dict[str, Any]:
        if table != pipeline.table:
            raise HTTPException(status_code=404, detail="unknown table")
        row = pipeline.sink.read(key)
        if row is None:
            raise HTTPException(status_code=404, detail="row not found")
        return row

    @app.get("/tables/{table}/as-of/{lsn}")
    def read_as_of(table: str, lsn: int) -> dict[str, Any]:
        if table != pipeline.table:
            raise HTTPException(status_code=404, detail="unknown table")
        return {"lsn": lsn, "rows": pipeline.sink.read_as_of(lsn)}

    @app.get("/metrics")
    def metrics() -> dict[str, Any]:
        return pipeline.metrics.summary()

    @app.get("/quarantine")
    def quarantine() -> dict[str, Any]:
        return {
            "count": len(pipeline.quarantine),
            "events": [
                {"source": e.event.source, "lsn": e.event.lsn, "reason": e.reason}
                for e in pipeline.quarantine.all()
            ],
        }

    return app
