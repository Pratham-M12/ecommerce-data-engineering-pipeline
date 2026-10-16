import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import psycopg2
from deltalake import DeltaTable, write_deltalake

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("phase6")

DATA_DIR = Path("/opt/airflow/data/phase6")
DELTA_DIR = DATA_DIR / "delta"
REPORT_DIR = Path("/opt/airflow/reports")

EXPECTED_COUNTS = {
    "customers": 12,
    "orders": 12,
    "order_items": 34,
}


def get_postgres_connection():
    required = [
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(
            "Missing PostgreSQL environment variables: "
            + ", ".join(missing)
        )

    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )


def read_table(connection, table_name, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()

    frame = pl.DataFrame(rows, schema=columns, orient="row")
    logger.info("Read %s rows from %s", frame.height, table_name)
    return frame


def validate_sources(customers, orders, items):
    actual = {
        "customers": customers.height,
        "orders": orders.height,
        "order_items": items.height,
    }

    if any(count == 0 for count in actual.values()):
        raise ValueError(f"Source table is empty: {actual}")

    if actual != EXPECTED_COUNTS:
        logger.warning(
            "Source counts differ from initial baseline. "
            "Baseline=%s, current=%s",
            EXPECTED_COUNTS,
            actual,
        )

    if customers["customer_id"].null_count():
        raise ValueError("customers contains null customer_id values")

    if orders["order_id"].null_count():
        raise ValueError("orders contains null order_id values")

    if items["item_id"].null_count():
        raise ValueError("order_items contains null item_id values")

    if items.filter(
        (pl.col("quantity") <= 0)
        | (pl.col("unit_price") < 0)
        | (pl.col("total_price") < 0)
    ).height:
        raise ValueError("order_items contains invalid quantities or prices")

    if orders["total_amount"].null_count():
        raise ValueError("orders contains null total_amount values")

    if items["order_id"].is_in(orders["order_id"]).not_().any():
        raise ValueError("order_items contains an unknown order_id")

    if orders["customer_id"].is_in(customers["customer_id"]).not_().any():
        raise ValueError("orders contains an unknown customer_id")

    return actual


def write_delta(frame, table_name):
    destination = DELTA_DIR / table_name
    destination.mkdir(parents=True, exist_ok=True)

    # Replace only this pipeline's own output table on each run.
    write_deltalake(
        str(destination),
        frame.to_arrow(),
        mode="overwrite",
    )

    # Reopen the committed Delta table to verify persistence.
    table = DeltaTable(str(destination))
    persisted_rows = table.to_pyarrow_table().num_rows

    if persisted_rows != frame.height:
        raise RuntimeError(
            f"Delta verification failed for {table_name}: "
            f"expected {frame.height}, got {persisted_rows}"
        )

    logger.info(
        "Delta table %s verified: %s rows at %s",
        table_name,
        persisted_rows,
        destination,
    )

    return {
        "path": str(destination),
        "rows": persisted_rows,
        "version": table.version(),
    }


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DELTA_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    connection = get_postgres_connection()
    try:
        customers = read_table(
            connection,
            "customers",
            """
            SELECT customer_id, first_name, last_name, email,
                   phone, city, state, country, postal_code, created_at
            FROM public.customers
            """,
        )

        orders = read_table(
            connection,
            "orders",
            """
            SELECT order_id, customer_id, order_status, order_date,
                   total_amount::DOUBLE PRECISION AS total_amount,
                   currency, payment_method, shipping_address,
                   created_at, updated_at
            FROM public.orders
            """,
        )

        items = read_table(
            connection,
            "order_items",
            """
            SELECT item_id, order_id, product_id, product_name,
                   category, quantity,
                   unit_price::DOUBLE PRECISION AS unit_price,
                   total_price::DOUBLE PRECISION AS total_price,
                   created_at
            FROM public.order_items
            """,
        )
    finally:
        connection.close()

    source_counts = validate_sources(customers, orders, items)

    # DuckDB provides the analytical SQL layer over the extracted data.
    database = duckdb.connect(":memory:")
    try:
        database.register("customers", customers.to_arrow())
        database.register("orders", orders.to_arrow())
        database.register("order_items", items.to_arrow())

        daily_sales_arrow = database.execute(
            """
            SELECT
                CAST(o.order_date AS DATE) AS sales_date,
                o.currency,
                COUNT(DISTINCT o.order_id) AS order_count,
                COUNT(DISTINCT o.customer_id) AS customer_count,
                ROUND(SUM(o.total_amount), 2) AS revenue
            FROM orders o
            GROUP BY CAST(o.order_date AS DATE), o.currency
            ORDER BY sales_date, o.currency
            """
        ).fetch_arrow_table()

        category_sales_arrow = database.execute(
            """
            SELECT
                i.category,
                COUNT(DISTINCT i.order_id) AS order_count,
                SUM(i.quantity) AS units_sold,
                ROUND(SUM(i.total_price), 2) AS item_revenue
            FROM order_items i
            GROUP BY i.category
            ORDER BY item_revenue DESC, i.category
            """
        ).fetch_arrow_table()

        # Cross-check order totals against item totals.
        mismatches = database.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    o.order_id,
                    o.total_amount,
                    SUM(i.total_price) AS item_total
                FROM orders o
                JOIN order_items i ON i.order_id = o.order_id
                GROUP BY o.order_id, o.total_amount
                HAVING ABS(o.total_amount - SUM(i.total_price)) > 0.02
            ) differences
            """
        ).fetchone()[0]

        if mismatches:
            raise ValueError(
                f"{mismatches} orders have totals that differ "
                "from their item totals by more than 0.02"
            )

    finally:
        database.close()

    daily_sales = pl.from_arrow(daily_sales_arrow)
    category_sales = pl.from_arrow(category_sales_arrow)

    output_frames = {
        "customers": customers,
        "orders": orders,
        "order_items": items,
        "daily_sales": daily_sales,
        "category_sales": category_sales,
    }

    delta_results = {}
    for name, frame in output_frames.items():
        delta_results[name] = write_delta(frame, name)

    report = {
        "pipeline": "phase6",
        "status": "success",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_row_counts": source_counts,
        "order_total_mismatches": mismatches,
        "transformations": {
            "daily_sales": {
                "description": "Revenue, orders, and customers by date and currency",
                "rows": daily_sales.height,
            },
            "category_sales": {
                "description": "Units sold and item revenue by category",
                "rows": category_sales.height,
            },
        },
        "delta_tables": delta_results,
    }

    report_path = REPORT_DIR / "phase6_validation.json"
    report_path.write_text(
        json.dumps(report, indent=2, default=str),
        encoding="utf-8",
    )

    logger.info("Phase 6 transformation completed successfully.")
    logger.info("Validation report: %s", report_path)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
