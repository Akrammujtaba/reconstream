"""Nightly backstop for the streaming pipeline.

1. batch_reconcile  re-runs the reconciliation rules over yesterday with a
                    wall-clock "now", catching what the stream could not judge
                    (quiet periods, data later than its horizon, outages)
2. quality_checks   Great Expectations on silver; fails the run on bad data
3. dbt_build        snapshots, finance marts, and dbt tests on Trino

Backfill a range:   airflow dags backfill reconstream_nightly -s 2026-01-01 -e 2026-01-07
Reconcile all data: trigger with config {"full": true}
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

SPARK_SUBMIT = "/opt/airflow/jobs-venv/bin/spark-submit --master 'local[2]' --driver-memory 2g"
JOBS = "/opt/reconstream/reconstream/jobs"
DBT = (
    "/opt/airflow/dbt-venv/bin/dbt {cmd} --project-dir /opt/dbt --profiles-dir /opt/dbt "
    "--target trino --target-path /tmp/dbt/target --log-path /tmp/dbt/logs"
)
RANGE = (
    "{% if params.full %}--full{% else %}"
    "--start {{ data_interval_start | ds }} --end {{ data_interval_end | ds }}{% endif %}"
)

with DAG(
    dag_id="reconstream_nightly",
    description="Batch reconciliation, data quality, and finance marts",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    params={"full": Param(False, type="boolean", description="Reconcile all data")},
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        # Iceberg commits from this job can race the streaming job's commits;
        # a retry re-reads the latest snapshot and succeeds.
        "retry_exponential_backoff": True,
    },
    doc_md=__doc__,
    tags=["reconstream", "batch"],
) as dag:
    batch_reconcile = BashOperator(
        task_id="batch_reconcile",
        bash_command=f"{SPARK_SUBMIT} {JOBS}/batch_reconcile.py {RANGE}",
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=(
            f"{SPARK_SUBMIT} {JOBS}/quality_checks.py "
            "--start {{ data_interval_start | ds }} --end {{ data_interval_end | ds }}"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=DBT.format(cmd="build"),
    )

    batch_reconcile >> quality_checks >> dbt_build
