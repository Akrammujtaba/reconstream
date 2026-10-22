# Design decisions

Why ReconStream is built the way it is, written as the questions a reviewer or interviewer would ask.

---

### Why `foreachBatch` + MERGE instead of a Spark stream-stream join?

A stream-stream join emits each match once, in append mode. Reconciliation needs results that **change later**: a `MISSING_PAYMENT` must resolve when a late payment arrives, and a resolved exception must re-open if something breaks again. That is state that gets updated, not appended.

So each micro-batch first lands data in Iceberg, then re-evaluates the affected bookings against the full silver tables and MERGEs the outcome into `gold.recon_exceptions`. The stored table becomes the state, which also makes it queryable and survives restarts, unlike in-memory streaming state.

### Why judge deadlines on event time and not wall-clock time?

If Spark falls behind by 10 minutes, wall-clock "now" would make every booking confirmed in that window look unpaid, which is a flood of false alarms caused by our own lag. The watermark is the **newest event time seen minus 30 seconds of allowed lateness**. A booking is flagged only when the data itself shows the SLA has passed. `test_wall_clock_alone_does_not_trigger_timeouts` covers this.

The trade-off: with no traffic the watermark stops, so timeouts wait for the next event. The nightly batch backstops that, and a per-partition heartbeat event would fix it in production.

### How is a duplicate *delivery* different from a duplicate *charge*?

- **Duplicate delivery:** the provider sends the same event twice. Same `event_id`, so it is the same money. It is collapsed by `event_id` when parsed and again by the insert-only MERGE into silver. It is counted in metrics, never flagged.
- **Duplicate charge:** two different `payment_id`s captured for one booking. That is two charges and a real `DUPLICATE_CHARGE`.

Captures are also de-duplicated on `payment_id`, in case a provider ever emits two events for one capture.

### What makes every write idempotent?

Spark can replay a micro-batch after a crash, so every write must be safe to repeat:

| Table | Key | Rule |
|---|---|---|
| silver.bookings | `booking_id` | update only if the incoming Postgres **LSN** is newer |
| silver.payments | `event_id` | insert-only |
| gold.recon_exceptions | `sha256(booking_id, type)` | deterministic id, upsert |
| bronze | — | append-only, at-least-once by design |

Bookings are ordered by **LSN** rather than `updated_at` because LSNs strictly increase, while application timestamps can tie or go backwards with clock drift.

Bronze is deliberately at-least-once. It is a replay log, and making it exactly-once would mean a MERGE over a growing table every few seconds. Nothing reads bronze without de-duplicating.

### How does per-batch work stay flat as the tables grow?

The first version re-checked every booking from the last 6 hours on every micro-batch. On a real run, batch time climbed from 7s to 50s within minutes, because the work grew with the table rather than with the batch.

An exception can only change for two reasons: new data arrived for that booking, or its deadline passed. So each batch evaluates exactly those bookings:

- **touched:** booking ids in this batch's CDC changes or payment events
- **newly due:** bookings whose `confirmed_at`, `cancelled_at`, or first capture plus the SLA falls between the previous watermark and the new one

Those windows are contiguous, so no deadline is skipped. After a restart, the first batch catches up over the full horizon.

### Why merge-on-read Iceberg tables, and what does it cost?

Copy-on-write would rewrite whole data files on every MERGE. Merge-on-read writes small delete files instead, so streaming writes are cheap. The cost is read amplification: every batch adds files and delete files, and every read has to open them. So the stream itself runs `rewrite_data_files`, `rewrite_position_delete_files`, and `expire_snapshots` every 40 batches, and `reconstream_maintenance` does a fuller pass (including `rewrite_manifests`) nightly.

`write.merge.isolation-level = snapshot` reduces commit conflicts between the streaming job and the nightly batch, which both write to gold. Conflicts that still happen are retried by Airflow.

### Why does a streaming system need a nightly batch job?

The stream only re-checks bookings inside its 6-hour horizon, and only when events arrive. The nightly job runs the **same `reconcile()` function** with a wall-clock "now" over the full day. That catches quiet periods, very late data, and anything missed during an outage. Because the code is shared, the two paths cannot disagree about what counts as an exception.

### Why reject bad webhooks to a DLQ, and why return 503 on Kafka errors?

- **Contract failures** (bad currency, negative amount, missing field, unknown type) are not retryable, so they get a `400` and go to `payments.dlq` with the reason for someone to inspect, instead of disappearing.
- **Kafka failures** are temporary, so the webhook returns `503`. The provider retries, and because the event keeps its `event_id`, the retry is harmless.
- **The `202` is only sent after the broker acknowledges** (`acks=all`, idempotent producer), so an accepted event is never lost.

### Why a JDBC catalog in Postgres instead of Hive Metastore or a REST catalog?

Spark and Trino both support Iceberg's JDBC catalog, and it reuses the Postgres that is already running. That means no extra service to operate locally. In AWS the same tables would use the Glue catalog, which `infra/terraform` provisions. One limitation: the JDBC catalog has no views, so dbt staging models are ephemeral (inlined as CTEs).

### How is "it works" measured rather than claimed?

The generator writes every injected fault to `audit.injected_faults`, with the time it became detectable. The evaluator compares that to `gold.recon_exceptions` and reports recall, precision, and detection latency per type. Noise that must **not** alert (redeliveries, out-of-order events, failed attempts before success, late-but-valid payments) is mixed in, so precision is measured too.

### What would change at real scale?

- Partition `silver.payments` by `bucket(N, booking_id)` so the semi-joins on scope ids prune files.
- Run compaction as its own scheduled job instead of inside the stream, so it never pauses detection.
- Run on EMR Serverless or Kubernetes with checkpoints in S3 instead of local mode.
- Move SLAs and refund policy into a config table so they can differ per market or merchant.
- Add a heartbeat event per Kafka partition so the watermark advances during quiet periods.
