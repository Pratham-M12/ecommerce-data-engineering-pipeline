import json
import logging
import os
from pathlib import Path

from deltalake import DeltaTable
from elasticsearch import Elasticsearch, helpers


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("phase8_elasticsearch")

DELTA_DIR = Path("/opt/airflow/data/phase6/delta")
REPORT_DIR = Path("/opt/airflow/reports")

ELASTICSEARCH_URL = os.getenv(
    "ELASTICSEARCH_URL",
    "http://ecommerce_elasticsearch:9200",
)

ORDERS_INDEX = "ecommerce-orders"
CATEGORY_INDEX = "ecommerce-category-sales"


def read_delta_table(table_name):
    path = DELTA_DIR / table_name
    if not path.is_dir():
        raise FileNotFoundError(f"Delta table not found: {path}")

    arrow_table = DeltaTable(str(path)).to_pyarrow_table()
    records = arrow_table.to_pylist()
    logger.info("Read %s rows from Delta table %s", len(records), table_name)
    return records


def json_safe(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


def index_records(client, index_name, records, id_field=None):
    actions = []

    for position, record in enumerate(records):
        if id_field:
            if record.get(id_field) is None:
                raise ValueError(
                    f"Missing {id_field} for a document in {index_name}"
                )
            document_id = str(record[id_field])
        else:
            document_id = str(position)

        actions.append(
            {
                "_op_type": "index",
                "_index": index_name,
                "_id": document_id,
                "_source": json_safe(record),
            }
        )

    if not actions:
        raise ValueError(f"Refusing to index zero documents into {index_name}")

    success, errors = helpers.bulk(
        client,
        actions,
        raise_on_error=False,
        raise_on_exception=False,
    )

    if errors:
        raise RuntimeError(
            f"Indexing errors in {index_name}: "
            + json.dumps(errors[:3], default=str)
        )

    logger.info("Indexed %s documents into %s", success, index_name)
    return success


def main():
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    orders = read_delta_table("orders")
    category_sales = read_delta_table("category_sales")

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

        orders_indexed = index_records(
            client,
            ORDERS_INDEX,
            orders,
            id_field="order_id",
        )
        categories_indexed = index_records(
            client,
            CATEGORY_INDEX,
            category_sales,
        )

        client.indices.refresh(index=[ORDERS_INDEX, CATEGORY_INDEX])

        actual_orders = client.count(index=ORDERS_INDEX)["count"]
        actual_categories = client.count(index=CATEGORY_INDEX)["count"]

        if actual_orders != len(orders):
            raise RuntimeError(
                f"Order count mismatch: source={len(orders)}, "
                f"indexed={actual_orders}"
            )

        if actual_categories != len(category_sales):
            raise RuntimeError(
                f"Category count mismatch: source={len(category_sales)}, "
                f"indexed={actual_categories}"
            )

        report = {
            "pipeline": "phase8_elasticsearch",
            "status": "success",
            "elasticsearch_url": ELASTICSEARCH_URL,
            "indices": {
                ORDERS_INDEX: {
                    "source_rows": len(orders),
                    "indexed_this_run": orders_indexed,
                    "documents_after_refresh": actual_orders,
                },
                CATEGORY_INDEX: {
                    "source_rows": len(category_sales),
                    "indexed_this_run": categories_indexed,
                    "documents_after_refresh": actual_categories,
                },
            },
        }

        report_path = REPORT_DIR / "phase8_elasticsearch_validation.json"
        report_path.write_text(
            json.dumps(report, indent=2, default=str),
            encoding="utf-8",
        )

        logger.info("Phase 8 indexing completed successfully.")
        logger.info("Validation report: %s", report_path)
        print(json.dumps(report, indent=2, default=str))

    finally:
        client.close()


if __name__ == "__main__":
    main()
