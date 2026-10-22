# Roadmap

Planned improvements, roughly smallest first. Each one is a self-contained change with a test.

## Small (under an hour)

- [ ] **Configurable SLA per currency.** Read SLA minutes from a small dict (e.g. INR bookings get 5 minutes) instead of one global value. Add a test in `tests/test_reconcile.py`.
- [ ] **Partial-refund tolerance.** Let `REFUND_MISMATCH` ignore differences under a configurable amount (e.g. 1 cent from rounding). Test both sides of the threshold.
- [ ] **Alert severity in Kafka messages.** Add `severity` (from the dbt `exception_types` seed) to the alert JSON in `store.publish_alerts`.
- [ ] **Generator CLI flag for a single fault type.** `--only DUPLICATE_CHARGE` to demo one scenario at a time.
- [ ] **Grafana panel: exceptions resolved vs. opened, by type.** Edit `infra/grafana/dashboards/reconstream.json`.
- [ ] **dbt test: no booking has both DUPLICATE_CHARGE and MISSING_PAYMENT open.** Singular test in `dbt/tests/`.
- [ ] **Webhook: reject events dated in the future** (more than 5 minutes ahead of server time). Add a test case.
- [ ] **Evaluator: CSV output option** alongside the markdown table.

## Medium (a few sessions)

- [ ] **Heartbeat events so the watermark advances when traffic is quiet.** Today timeouts wait for the next real event. Emit a heartbeat per Kafka partition and ignore it in parsing.
- [ ] **Bucket-partition `silver.payments` by `booking_id`** and measure the effect on batch duration with the evaluator.
- [ ] **Move in-stream compaction to its own Airflow DAG** running every 15 minutes, and compare p95 latency before and after.
- [ ] **Dead-letter replay tool.** Read `payments.dlq`, fix a known class of error, and re-post to the webhook.
- [ ] **dbt incremental model** for `fct_recon_exceptions` instead of a full rebuild.
- [ ] **Trino queries for finance:** a `docs/queries.md` with five useful questions (open exposure by currency, slowest-to-resolve types, and so on).

## Larger

- [ ] **Deploy the Spark jobs to EMR Serverless** using `infra/terraform`, with the Glue catalog instead of the JDBC catalog.
- [ ] **Load test at 20 bookings/sec** and document where the bottleneck moves.
- [ ] **Replace polling in the evaluator with a scheduled Airflow DAG** that writes results to an Iceberg table and a Grafana panel.
