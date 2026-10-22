"""Iceberg maintenance: compaction, delete-file cleanup, snapshot expiry.

Runs after the nightly reconciliation so it does not compete for the same tables.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

with DAG(
    dag_id="reconstream_maintenance",
    description="Compact and clean up Iceberg tables",
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=5)},
    doc_md=__doc__,
    tags=["reconstream", "maintenance"],
) as dag:
    BashOperator(
        task_id="compact_and_expire",
        bash_command=(
            "/opt/airflow/jobs-venv/bin/spark-submit --master 'local[2]' --driver-memory 2g "
            "/opt/reconstream/reconstream/jobs/maintenance.py"
        ),
    )
