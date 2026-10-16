from datetime import datetime
import json
import logging
import os
from pathlib import Path
import subprocess
import sys

from deltalake import DeltaTable
from elasticsearch import Elasticsearch
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


EXPECTED_TABLES = {"customers", "orders", "order_items"}
EXPECTED_DELTA_COUNTS = {
    "customers": 12,
    "orders": 12,
    "order_items": 34,
    "daily_sales": 1,
    "category_sales": 4,
}
EXPECTED_ES_COUNTS = {
    "ecommerce-orders": 12,
    "ecommerce-category-sales": 4,
}

TRANSFORM_SCRIPT = Path("/opt/airflow/src/transform/phase6_transform.py")
ELASTICSEARCH_SCRIPT = Path("/opt/airflow/src/transform/phase8_elasticsearch.py")
DELTA_DIR = Path("/opt/airflow/data/phase6/delta")
PHASE6_REPORT = Path("/opt/airflow/reports/phase6_validation.json")
PHASE8_REPORT = Path("/opt/airflow/reports/phase8_elasticsearch_validation.json")
ELASTICSEARCH_URL = os.getenv(
    "ELASTICSEARCH_URL",
    "http://ecommerce_elasticsearch:9200",
)


def get_connection():
    required = [
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Missing PostgreSQL environment variables: " + ", ".join(missing)
        )

    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )


def check_postgres_connection():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT current_database(), current_user")
            database, user = cursor.fetchone()
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name IN ('customers', 'orders', 'order_items')
                """
            )
            found = {row[0] for row in cursor.fetchall()}

    missing = EXPECTED_TABLES - found
    if missing:
        raise RuntimeError(
            "Expected PostgreSQL tables are missing: "
            + ", ".join(sorted(missing))
        )

    logging.info(
        "PostgreSQL connection successful. Database=%s, user=%s, tables=%s",
        database, user, ", ".join(sorted(found)),
    )


def validate_table_counts():
    counts = {}
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for table in sorted(EXPECTED_TABLES):
                cursor.execute(f'SELECT COUNT(*) FROM public."{table}"')
                counts[table] = cursor.fetchone()[0]

    for table, count in counts.items():
        logging.info("Table %s: %s rows", table, count)

    empty_tables = [table for table, count in counts.items() if count == 0]
    if empty_tables:
        raise RuntimeError(
            "Expected non-empty tables, but these are empty: "
            + ", ".join(empty_tables)
        )

    logging.info("PostgreSQL validation passed.")


def run_script(script_path, label):
    if not script_path.is_file():
        raise FileNotFoundError(f"{label} script not found: {script_path}")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        check=False,
        capture_output=True,
        text=True,
        timeout=600,
    )

    if result.stdout:
        logging.info("%s stdout:\n%s", label, result.stdout)
    if result.stderr:
        logging.info("%s stderr:\n%s", label, result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"{label} failed with exit code {result.returncode}"
        )

    logging.info("%s completed successfully.", label)


def run_phase6_transform():
    run_script(TRANSFORM_SCRIPT, "Phase 6 transformation")


def verify_phase6_outputs():
    if not PHASE6_REPORT.is_file():
        raise FileNotFoundError(f"Phase 6 report not found: {PHASE6_REPORT}")

    report = json.loads(PHASE6_REPORT.read_text(encoding="utf-8"))
    if report.get("status") != "success":
        raise RuntimeError("Phase 6 report does not show success.")
    if report.get("order_total_mismatches") != 0:
        raise RuntimeError("Phase 6 report contains order-total mismatches.")

    reported_tables = report.get("delta_tables", {})
    if set(reported_tables) != set(EXPECTED_DELTA_COUNTS):
        raise RuntimeError("Phase 6 report has an unexpected set of Delta tables.")

    for table_name, expected in EXPECTED_DELTA_COUNTS.items():
        path = DELTA_DIR / table_name
        if not path.is_dir():
            raise FileNotFoundError(f"Delta table not found: {path}")

        actual = DeltaTable(str(path)).to_pyarrow_table().num_rows
        reported = reported_tables[table_name].get("rows")
        logging.info(
            "Delta table %s: expected=%s, actual=%s, report=%s",
            table_name, expected, actual, reported,
        )
        if actual != expected or reported != expected:
            raise RuntimeError(f"Delta row-count validation failed: {table_name}")

    logging.info("Phase 6 output verification passed.")


def run_phase8_indexing():
    run_script(ELASTICSEARCH_SCRIPT, "Phase 8 Elasticsearch indexing")


def verify_phase8_indices():
    if not PHASE8_REPORT.is_file():
        raise FileNotFoundError(f"Phase 8 report not found: {PHASE8_REPORT}")

    report = json.loads(PHASE8_REPORT.read_text(encoding="utf-8"))
    if report.get("status") != "success":
        raise RuntimeError("Phase 8 report does not show success.")

    reported_indices = report.get("indices", {})
    if set(reported_indices) != set(EXPECTED_ES_COUNTS):
        raise RuntimeError("Phase 8 report has an unexpected set of indices.")

    client = Elasticsearch(
        ELASTICSEARCH_URL,
        request_timeout=30,
        max_retries=3,
        retry_on_timeout=True,
    )

    try:
        if not client.ping():
            raise RuntimeError(
                f"Elasticsearch did not respond at {ELASTICSEARCH_URL}"
            )

        for index_name, expected in EXPECTED_ES_COUNTS.items():
            if not client.indices.exists(index=index_name):
                raise RuntimeError(f"Elasticsearch index is missing: {index_name}")

            client.indices.refresh(index=index_name)
            actual = client.count(index=index_name)["count"]
            source_count = reported_indices[index_name].get("source_rows")
            report_count = reported_indices[index_name].get("documents_after_refresh")

            logging.info(
                "Elasticsearch index %s: expected=%s, actual=%s, "
                "source=%s, report=%s",
                index_name, expected, actual, source_count, report_count,
            )

            if (
                actual != expected
                or source_count != expected
                or report_count != expected
            ):
                raise RuntimeError(
                    f"Elasticsearch document-count validation failed: {index_name}"
                )

    finally:
        client.close()

    logging.info("Phase 8 Elasticsearch verification passed.")


with DAG(
    dag_id="ecommerce_validation_dag",
    description="Validate PostgreSQL, transform Delta data, and index Elasticsearch",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ecommerce", "validation", "transformation", "elasticsearch"],
) as dag:
    connection_check = PythonOperator(
        task_id="check_postgres_connection",
        python_callable=check_postgres_connection,
    )

    counts_check = PythonOperator(
        task_id="validate_table_counts",
        python_callable=validate_table_counts,
    )

    transform = PythonOperator(
        task_id="run_phase6_transform",
        python_callable=run_phase6_transform,
    )

    delta_check = PythonOperator(
        task_id="verify_phase6_outputs",
        python_callable=verify_phase6_outputs,
    )

    index_elasticsearch = PythonOperator(
        task_id="run_phase8_indexing",
        python_callable=run_phase8_indexing,
    )

    elasticsearch_check = PythonOperator(
        task_id="verify_phase8_indices",
        python_callable=verify_phase8_indices,
    )

    (
        connection_check
        >> counts_check
        >> transform
        >> delta_check
        >> index_elasticsearch
        >> elasticsearch_check
    )
