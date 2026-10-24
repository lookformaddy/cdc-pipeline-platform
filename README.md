# cdc-pipeline-platform

An enterprise change-data-capture (CDC) pipeline platform: snapshot +
incremental merge, schema evolution, exactly-once-ish dedup, late and
out-of-order event handling, tombstones, time-travel reads, data-quality
gates, lag/throughput metrics, and checkpointed backfill/replay — built as
production-grade Python with synthetic data only.

**Track:** Data Engineering · **Day 7** of the enterprise portfolio series.

## The enterprise problem

Replicating a source database into an analytics target sounds solved
until the edge cases arrive, and they always arrive together:

- **The initial load must not miss rows** that change while it runs —
  snapshot and incremental streams have to merge on one ordering key.
- **The source schema changes** mid-flight: a column is added, a type is
  widened, a column is renamed. A narrowing or a type swap must never
  silently corrupt the target.
- **Delivery is at-least-once**: the same event arrives twice, and
  replaying a batch after a crash must be a no-op, not a double-apply.
- **Events arrive late and out of order**, and deletes must not resurrect
  rows on replay.
- **Someone always asks** "what did this table look like last Tuesday?"
  and "how far behind are we, in events?" — time travel and LSN lag are
  first-class answers, not afterthoughts.

This repo implements all of it, clean-room, with synthetic data only.

## Architecture overview

```
synthetic source ──▶ SourceLog (append-only, LSN-ordered)
                           │
                     ┌─────▼──────┐
                     │ CdcPipeline │
                     │ 1. sort batch by (source, LSN)
                     │ 2. apply schema changes (safe) / note failures
                     │ 3. dedup (source, LSN) at the sink
                     │ 4. late-window check → quarantine if too late
                     │ 5. quality gates → quarantine on failure
                     │ 6. idempotent apply → versioned TargetTable
                     └─────┬──────┘
                           ├──▶ QuarantineStore (reason + batch, replayable)
                           ├──▶ Checkpoint (applied LSNs, schema version)
                           └──▶ PipelineMetrics (lag in LSNs, throughput)
```

See [docs/architecture.md](docs/architecture.md) for the full component
map, data flow, and design trade-offs.

## How to run it

```bash
python -m venv /tmp/venv && source /tmp/venv/bin/activate
pip install -r requirements.txt
pip install -e .

# run the tests (API tests auto-skip without FastAPI)
pytest -q

# end-to-end demo: snapshot + incremental + schema evolution + replay
python -m cdc_pipeline demo
# or: make demo / python examples/run_demo.py

# serve the HTTP API
python -m cdc_pipeline serve            # -> http://127.0.0.1:8000
# POST /ingest                {"events": [...]}
# GET  /tables/{table}/rows/{key}
# GET  /tables/{table}/as-of/{lsn}
# GET  /metrics  ·  GET /quarantine
```

Docker:

```bash
docker build -t cdc-pipeline-platform .
docker run -p 8000:8000 cdc-pipeline-platform
```

Configuration is environment-driven; copy `.env.example` to `.env` to
override the seed, snapshot size, late window, batch size, and checkpoint
path.

## Key design decisions and trade-offs

- **LSN is the only ordering truth.** Event timestamps are informational;
  clock skew between source and sink makes time-based ordering wrong by
  construction. Batches are sorted by (source, LSN) before applying.
- **Exactly-once-ish, honestly named.** True exactly-once needs
  transactional sinks; this platform gets the observable guarantee —
  re-delivery and replay never change the target twice — via (source,
  LSN) dedup at the sink, and says so in the name.
- **Deletes are tombstones, not erasures.** A tombstone survives replay
  (so a replayed insert-then-delete sequence cannot resurrect a row) and
  distinguishes "never existed" from "deleted" until compaction purges it.
- **Schema evolution is versioned and fail-closed.** Add, widen, and
  rename (with a mapping) are safe; narrowing, type swaps, and drops are
  rejected, and events that need them are quarantined with a reason —
  never silently dropped, never blocking the batch.
- **Quarantine is a store, not a trash can.** Every quarantined event
  keeps its original payload, reason, and batch id, and
  `replay_quarantined()` retries them after a fix (e.g. a schema change
  or a widened gate range).
- **Late events have a retention window.** An event below the watermark
  still applies (out-of-order merge); one older than `max_late_lsns` is
  quarantined as too late, bounding how far back a merge can reach.
- **Lag is measured in LSNs, not seconds.** LSN distance is actionable
  ("4,200 events behind"); wall-clock lag conflates source idle time
  with replication delay.
- **Time travel is per-batch versions.** Every applied batch creates an
  immutable version tagged with its max LSN; reads "as of LSN N" return
  the newest version at or below N. Compaction bounds version growth.
- **The sink is in-memory by design.** The `TargetTable` boundary is
  where Postgres/Delta/Iceberg slots in; the pipeline logic above it
  does not change.

## What it demonstrates

For a Director-level interview, this repo is evidence of: designing a
CDC merge pipeline with snapshot/incremental consistency; schema
evolution as a versioned, fail-closed discipline; idempotency and
exactly-once semantics reasoning; late/out-of-order event handling with
explicit retention policy; quarantine-and-replay operability; and
observability (LSN lag, throughput, p95 batch latency) as a first-class
feature rather than an afterthought.

## CI

The CI definition ships at [docs/github-actions-ci.yml](docs/github-actions-ci.yml).
It activates by moving it to `.github/workflows/ci.yml` (kept out of
`.github/workflows/` here because the GitHub App used for publishing lacks
the Workflows permission).

## Clean-room note

All code is original and all data is synthetic (fake e-commerce orders).
No employer code, data, or internal designs are included.
