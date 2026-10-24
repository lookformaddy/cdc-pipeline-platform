# Architecture

## Components

| Module | Responsibility |
|---|---|
| `events.py` | `ChangeEvent` (one row mutation, pinned to an LSN) and `SourceLog` (append-only, LSN-ordered, NDJSON-serializable). The log is the durable record replays read from. |
| `schema.py` | `TableSchema`: versioned columns, rename mapping, evolution history. Rules: add / widen / rename are safe; narrowing, type swaps, and drops are incompatible. |
| `sink.py` | `TargetTable`: versioned key-value sink. Idempotent apply by (source, LSN), tombstones for deletes, `read_as_of(lsn)` time travel, compaction with a retention horizon. |
| `quality.py` | Composable gates (non-null key, required columns, value ranges, unknown columns) plus `QuarantineStore` — the replayable home for failed and schema-incompatible events. |
| `pipeline.py` | `CdcPipeline`: batch orchestration (sort → schema changes → dedup → late check → gates → apply), snapshot load, checkpoints, backfill/replay, quarantine replay. |
| `metrics.py` | `PipelineMetrics`: per-batch accounting, LSN lag, throughput, p95 batch latency. |
| `generator.py` | Deterministic seeded synthetic source: snapshot rows, then inserts/updates/deletes, plus the demo's schema-evolution script. |
| `service.py` | Optional FastAPI adapter (import-guarded; tests auto-skip without it). Owns no CDC logic. |
| `cli.py` / `config.py` | `demo` / `serve` commands and environment-driven settings. |

## Data flow

```
generate_log() ──▶ SourceLog ──▶ load_snapshot() ──▶ TargetTable v0..vN
                                        │
incremental batches ──▶ process_batch() ──▶ schema changes
                                              │ (safe: apply; unsafe: note)
                                              ▼
                                        sort by (source, LSN)
                                              │
                              ┌───────────────┼───────────────┐
                              ▼               ▼               ▼
                         already applied   too late        gate failure
                         → skip (dedup)    → quarantine    → quarantine
                                              │
                                              ▼ (pass)
                                        sink.apply() → new version
                                              │
                                        checkpoint + metrics
```

## Failure handling

| Failure | Handling |
|---|---|
| Duplicate delivery / replay | Skipped by (source, LSN) dedup; batch continues. |
| Out-of-order event within window | Applied; counted as late; watermark unchanged. |
| Event older than `max_late_lsns` | Quarantined as "too late"; batch continues. |
| Quality gate failure | Quarantined with reason; batch continues. |
| Incompatible schema change | Change rejected; schema version unchanged; dependent events quarantined, not dropped. |
| Corrupt checkpoint | `CheckpointError` on restore; pipeline refuses to start from it rather than guessing. |
| Crash mid-batch | Resume from last checkpoint; replay is a no-op for applied events and applies exactly the missed tail. |

## Trade-offs and limits

- **In-memory sink.** Fine for the demo and for reasoning about the
  pipeline; a production deployment swaps `TargetTable` for a
  transactional store and persists versions, not just the checkpoint.
- **One version per applied batch, one row-copy per apply.** Simple and
  correct; a production sink would use copy-on-write / MVCC structures.
- **LSN per source, not global.** Multi-source joins need a global
  ordering layer (e.g. a transaction log or a watermark per source with
  a merge policy) — deliberately out of scope here.
- **No transaction grouping.** Events carry an optional
  `transaction_id`, but atomic multi-row transactions are not enforced;
  a production CDC feed would buffer a transaction's events until commit.
- **Schema changes arrive as batch metadata**, not as log events. A
  production feed would carry DDL as first-class log entries with their
  own LSNs; the ordering rules here are identical either way.
