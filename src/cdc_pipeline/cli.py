"""Command-line interface: demo, serve, snapshot export."""

from __future__ import annotations

import argparse
import json
import sys

from cdc_pipeline.config import Settings
from cdc_pipeline.generator import (
    BASE_SCHEMA,
    apply_demo_schema_script,
    generate_log,
)
from cdc_pipeline.pipeline import CdcPipeline
from cdc_pipeline.quality import (
    gate_required_columns,
    gate_value_ranges,
)


def build_pipeline(settings: Settings) -> CdcPipeline:
    gates = [
        gate_required_columns(["order_id", "customer_id", "amount_cents", "status"]),
        gate_value_ranges({"amount_cents": (0, 10_000_000)}),
    ]
    return CdcPipeline(
        source=settings.source,
        table=settings.table,
        schema=BASE_SCHEMA,
        gates=gates,
        max_late_lsns=settings.max_late_lsns,
    )


def run_demo(settings: Settings) -> int:
    """End-to-end demo: snapshot + incremental merge + replay + time travel."""
    log, snapshot, incremental = generate_log(
        seed=settings.seed,
        snapshot_rows=settings.snapshot_rows,
        incremental_events=settings.incremental_events,
    )
    pipeline = build_pipeline(settings)

    snap_result = pipeline.load_snapshot(snapshot)
    print(f"snapshot: {snap_result.batch.applied} rows loaded "
          f"(lsn {pipeline.watermark})")

    applied_total = 0
    for events, changes in apply_demo_schema_script(pipeline, incremental):
        result = pipeline.process_batch(events, schema_changes=changes)
        applied_total += result.batch.applied

    print(f"incremental: {applied_total} applied, "
          f"{pipeline.metrics.total_duplicates} duplicates, "
          f"{pipeline.metrics.total_quarantined} quarantined, "
          f"{pipeline.metrics.total_late} late")
    print(f"schema version: {pipeline.schema.version} "
          f"(columns: {', '.join(pipeline.schema.column_names())})")
    print(f"live rows: {len(pipeline.sink.read_all())}, "
          f"tombstones: {len(pipeline.sink.tombstones())}")

    # Exactly-once-ish proof: replay the whole log; nothing new applies.
    before = pipeline.metrics.total_applied
    pipeline.replay_from(log, from_lsn=0)
    replayed_new = pipeline.metrics.total_applied - before
    print(f"replay from lsn 0 applied {replayed_new} new events "
          f"(expected 0: dedup holds)")

    # Time travel: state as of the snapshot boundary vs now.
    snap_lsn = snapshot[-1].lsn
    print(f"time travel: {len(pipeline.sink.read_as_of(snap_lsn))} rows as of "
          f"snapshot lsn {snap_lsn}; {len(pipeline.sink.read_all())} rows now")

    checkpoint = pipeline.save_checkpoint(settings.checkpoint_path)
    print(f"checkpoint saved to {settings.checkpoint_path} "
          f"(applied_lsn={checkpoint.applied_lsn}, "
          f"quarantine={checkpoint.quarantine_count})")
    print("metrics:", json.dumps(pipeline.metrics.summary(), indent=2))
    return 0


def serve(settings: Settings) -> int:
    from cdc_pipeline.service import create_app

    try:
        import uvicorn  # type: ignore
    except ImportError:
        print("uvicorn is required to serve: pip install uvicorn", file=sys.stderr)
        return 1
    app = create_app(build_pipeline(settings))
    uvicorn.run(app, host=settings.host, port=settings.port)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cdc-pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("demo", help="run the end-to-end synthetic demo")
    sub.add_parser("serve", help="serve the FastAPI HTTP API")
    args = parser.parse_args(argv)
    settings = Settings()
    if args.command == "demo":
        return run_demo(settings)
    if args.command == "serve":
        return serve(settings)
    parser.error(f"unknown command {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
