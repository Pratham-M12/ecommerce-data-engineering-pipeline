# Mini Project 01 — E-Commerce Data Engineering Pipeline

An end-to-end, locally runnable e-commerce data pipeline built with Docker Compose. It generates synthetic order events, routes them through RabbitMQ and Apache NiFi, persists raw and normalized data, transforms relational data into Delta Lake analytics tables, and indexes analytics documents in Elasticsearch for exploration through Kibana.

> **Local-development note:** This is an educational local stack, not a production deployment. Elasticsearch security is disabled in the current Compose configuration. Do not expose these services to untrusted networks.

## Architecture

See [`docs/architecture.mmd`](docs/architecture.mmd) for the Mermaid architecture diagram.

### Data flow

1. **Synthetic source:** `src/generator/order_generator.py` creates sample e-commerce order data and publishes events to RabbitMQ.
2. **Message broker:** RabbitMQ provides the queue/broker layer.
3. **Routing and ingestion:** Apache NiFi consumes messages and routes data to MongoDB and PostgreSQL.
4. **Raw store:** MongoDB retains raw order documents.
5. **Relational store:** PostgreSQL stores normalized customer, order, and order-item records.
6. **Orchestration:** `dags/ecommerce_validation_dag.py` runs validation, transformations, and indexing tasks in order.
7. **Analytics transformation:** `src/transform/phase6_transform.py` uses Polars and DuckDB and writes Delta Lake outputs under the project data directory.
8. **Search and visualization:** `src/transform/phase8_elasticsearch.py` indexes order and category-sales documents into Elasticsearch. Kibana data views expose those indices in Discover.

## Repository layout

```text
.
├── airflow/                         # Custom Airflow image and dependencies
├── dags/
│   └── ecommerce_validation_dag.py  # Validation/transform/index orchestration
├── data/                            # Local transformation and Delta Lake outputs
├── nifi/                            # NiFi configuration and exports
├── postgres/init/                   # Database initialization SQL
├── reports/
│   ├── phase6_validation.json
│   └── phase8_elasticsearch_validation.json
├── src/
│   ├── generator/
│   │   ├── order_generator.py
│   │   └── requirements.txt
│   └── transform/
│       ├── phase6_transform.py
│       └── phase8_elasticsearch.py
├── docker-compose.yml
├── .env.example                     # Environment-variable template; no real secrets
└── README.md
```

## Services and local endpoints

| Service | Endpoint | Purpose |
|---|---|---|
| RabbitMQ management | http://localhost:15672 | Queue and broker inspection |
| PostgreSQL | `localhost:5432` | Normalized relational data |
| MongoDB | `localhost:27017` | Raw JSON documents |
| Apache NiFi | https://localhost:8443/nifi | Ingestion and routing UI |
| Airflow | http://localhost:8080 | DAG monitoring and execution |
| Elasticsearch | http://localhost:9200 | Search and aggregation API |
| Kibana | http://localhost:5601 | Discover, data views, visual exploration |

Credentials are configured through the local `.env` file and/or service configuration. Consult `.env.example` for variable names. **Do not commit `.env` or paste its contents into reports.** Verify the current credentials locally rather than relying on example values.

## Prerequisites

- Docker Desktop with Docker Compose support
- Sufficient memory for the database, NiFi, Elasticsearch, and Airflow containers
- PowerShell on Windows, or a compatible shell on macOS/Linux

## Start or resume the stack

Run commands from the repository root.

```powershell
# Validate Compose syntax
docker compose config --quiet

# Start services in the background
docker compose up -d

# Inspect service status
docker compose ps
```

The project includes Kibana (`kibana:8.13.4`) connected to the existing `ecommerce-network` and configured to use the Elasticsearch Compose service. Kibana is published on port 5601.

If you have just added Kibana and the other services are already running, start only Kibana:

```powershell
docker compose up -d kibana
docker compose logs --tail 60 kibana
Invoke-WebRequest -Uri http://localhost:5601/api/status -UseBasicParsing |
    Select-Object StatusCode, Status
```

A successful status request returns HTTP `200`. Allow time for Kibana's first startup.

## Run and verify the pipeline

### 1. Check containers

```powershell
docker compose ps
```

Wait for services with health checks to report healthy. Kibana may show `Up` without a Compose health status if no health check is configured for it.

### 2. Generate sample events

Inspect the generator's arguments before running it, since options may depend on the implementation currently in the repository:

```powershell
python .\src\generator\order_generator.py --help
```

Use the documented options shown by that command. The generator publishes synthetic events to the configured RabbitMQ broker.

### 3. Verify ingestion

Use the NiFi UI and the PostgreSQL/MongoDB clients or the project's existing verification commands to confirm that records reached both storage destinations. Check the actual database/schema names in `.env` and `postgres/init/02-schema.sql`; do not assume credentials from this README.

### 4. Run the Airflow DAG

Open http://localhost:8080 and locate `ecommerce_validation_dag`. Trigger the DAG from the UI, or use the Airflow CLI inside the webserver container:

```powershell
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags list-runs -d ecommerce_validation_dag
```

Review task logs if any task fails. The DAG's expected sequence is:

1. `check_postgres_connection`
2. `validate_table_counts`
3. `run_phase6_transform`
4. `verify_phase6_outputs`
5. `run_phase8_indexing`
6. `verify_phase8_indices`

### 5. Inspect transformation reports and outputs

The repository contains:

- `reports/phase6_validation.json`
- `reports/phase8_elasticsearch_validation.json`

The Phase 6 transformation produces Delta Lake analytics tables, including customer, order, order-item, daily-sales, category-sales, and order-total-mismatch outputs. The exact physical paths are determined by `src/transform/phase6_transform.py`.

Run the transformation through the Airflow DAG for the orchestrated workflow. Before running scripts manually, inspect their configuration and avoid launching concurrent duplicate runs.

### 6. Verify Elasticsearch indices

Expected indices:

- `ecommerce-orders`
- `ecommerce-category-sales`

PowerShell checks:

```powershell
Invoke-RestMethod http://localhost:9200/_cluster/health
Invoke-RestMethod http://localhost:9200/_cat/indices/ecommerce-orders,ecommerce-category-sales?v
Invoke-RestMethod http://localhost:9200/ecommerce-orders/_count
Invoke-RestMethod http://localhost:9200/ecommerce-category-sales/_count
```

The verified run documented for this project contained **12 order documents** and **4 category-sales documents**. These are sample-run counts, not fixed counts for every future generator run.

The category-sales totals previously verified were:

| Category | Item revenue (INR) | Units sold |
|---|---:|---:|
| Electronics | 42,184 | 16 |
| Fashion | 20,186 | 14 |
| Home & Kitchen | 19,184 | 16 |
| Stationery | 8,933 | 17 |
| **Total** | **90,487** | **63** |

The category revenue total matches the verified order revenue total of **INR 90,487** for that sample run.

A single-node Elasticsearch cluster may report yellow health because replica shards cannot be allocated to another node. If primary shards are active and the indices are queryable, yellow can be expected in this local single-node setup.

### 7. Use Kibana Discover

Open http://localhost:5601.

Create or use these data views:

| Display name | Index pattern | Time setting |
|---|---|---|
| Ecommerce Orders | `ecommerce-orders` | `order_date` |
| Ecommerce Category Sales | `ecommerce-category-sales` | No time filter |

In Discover, the verified run showed 12 order documents and 4 category aggregate documents.

## Troubleshooting

### Kibana is not reachable

```powershell
docker compose ps kibana
docker compose logs --tail 100 kibana
Test-NetConnection localhost -Port 5601
```

If Kibana has just started, wait briefly and check `http://localhost:5601/api/status` again. Confirm that the service is attached to `ecommerce-network` and points to `http://elasticsearch:9200`.

### Elasticsearch is not ready

```powershell
docker compose ps elasticsearch
docker compose logs --tail 100 elasticsearch
Invoke-RestMethod http://localhost:9200/_cluster/health
```

### Airflow task fails

Inspect the failed task's log in the Airflow UI, then check:

```powershell
docker compose logs --tail 100 airflow-scheduler
docker compose logs --tail 100 airflow-webserver
```

The scheduler and webserver must see the mounted `dags`, `src`, `data`, and `reports` directories. Verify environment variables without printing secret values.

### No documents appear in Kibana

1. Confirm the indices exist using the Elasticsearch `_cat/indices` endpoint.
2. Check the document counts using `/_count`.
3. Verify the selected data view and time filter. The orders view uses `order_date`; category aggregates should not use a time filter.
4. Check `reports/phase8_elasticsearch_validation.json` and the Airflow task logs.

### NiFi UI certificate warning

The local NiFi endpoint uses HTTPS. A browser certificate warning may occur with local/self-signed certificates. Confirm that the URL and local environment are correct before proceeding; do not disable TLS verification for unrelated systems.

## Stop the stack safely

To stop containers while preserving persistent volumes:

```powershell
docker compose stop
```

To stop and remove containers/networks while preserving named volumes:

```powershell
docker compose down
```

Avoid `docker compose down -v` unless you intentionally want to delete persistent database and Elasticsearch data. Do not run reset scripts unless you explicitly intend a clean reset.

## Security and reproducibility notes

- `.env` contains local configuration and must remain untracked.
- Elasticsearch security is disabled in the current Compose configuration; keep the stack restricted to a trusted local environment.
- Sample records and aggregate values change when new synthetic events are generated.
- For a reproducible report, record the date/time, generator settings, DAG run ID, report files, and index counts for the specific run being demonstrated.
