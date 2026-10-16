#!/usr/bin/env python3
"""
scripts/setup_phase3_nifi.py
Automates Apache NiFi Phase 3 configuration for E-Commerce Data Engineering Pipeline:
- Configures DBCPConnectionPool for PostgreSQL
- Configures ConsumeAMQP for RabbitMQ
- Configures PutMongo for MongoDB raw JSON storage
- Configures ExecuteGroovyScript for transactional PostgreSQL normalization
- Configures LogAttribute processors for visible failure routing
- Connects components with backpressure and failure isolation
- Validates all processor and controller service states
- Exports flow definition to nifi/exports/phase3_ecommerce_ingestion.json
"""

import os
import ssl
import json
import time
import urllib.request
import urllib.error
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# NiFi TLS context (self-signed cert)
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

NIFI_BASE_URL = "https://localhost:8443/nifi-api"
EXPORT_PATH = PROJECT_ROOT / "nifi" / "exports" / "phase3_ecommerce_ingestion.json"


def get_auth_token():
    user = os.getenv("NIFI_ADMIN_USER", "admin")
    password = os.getenv("NIFI_ADMIN_PASSWORD", "AdminPassword123!")
    req = urllib.request.Request(
        f"{NIFI_BASE_URL}/access/token",
        data=f"username={user}&password={password}".encode("utf-8"),
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    with urllib.request.urlopen(req, context=CTX) as resp:
        return resp.read().decode("utf-8")


def api_request(endpoint, method="GET", data=None, headers=None, token=None):
    hdrs = {}
    if token:
        hdrs["Authorization"] = f"Bearer {token}"
    if headers:
        hdrs.update(headers)
    body = None
    if data is not None:
        hdrs["Content-Type"] = "application/json"
        body = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(f"{NIFI_BASE_URL}{endpoint}", data=body, headers=hdrs, method=method)
    with urllib.request.urlopen(req, context=CTX) as resp:
        content = resp.read()
        if content:
            return json.loads(content.decode("utf-8"))
        return {}


def get_root_process_group_id(token):
    res = api_request("/process-groups/root", token=token)
    return res["id"]


def stop_and_delete_process_group(pg_id, token):
    """Recursively stops components and deletes a process group."""
    try:
        api_request(f"/flow/process-groups/{pg_id}", method="PUT", data={"id": pg_id, "state": "STOPPED"}, token=token)
    except Exception:
        pass
    time.sleep(1)

    flow = api_request(f"/flow/process-groups/{pg_id}", token=token)["processGroupFlow"]["flow"]

    # Delete child process groups
    for child in flow.get("processGroups", []):
        stop_and_delete_process_group(child["id"], token)

    # Empty and delete connections
    for c in flow.get("connections", []):
        try:
            drop_res = api_request(f"/flowfile-queues/{c['id']}/drop-requests", method="POST", token=token)
            drop_id = drop_res["dropRequest"]["id"]
            time.sleep(0.5)
            api_request(f"/flowfile-queues/{c['id']}/drop-requests/{drop_id}", method="DELETE", token=token)
        except Exception:
            pass
        api_request(f"/connections/{c['id']}?version={c['revision']['version']}", method="DELETE", token=token)

    # Delete processors
    for p in flow.get("processors", []):
        p_info = api_request(f"/processors/{p['id']}", token=token)
        api_request(f"/processors/{p['id']}?version={p_info['revision']['version']}", method="DELETE", token=token)

    # Disable and delete controller services
    services = api_request(f"/flow/process-groups/{pg_id}/controller-services", token=token)["controllerServices"]
    for s in services:
        try:
            api_request(
                f"/controller-services/{s['id']}/run-status",
                method="PUT",
                data={"revision": s["revision"], "state": "DISABLED"},
                token=token
            )
        except Exception:
            pass
        time.sleep(0.5)
        s_info = api_request(f"/controller-services/{s['id']}", token=token)
        api_request(f"/controller-services/{s['id']}?version={s_info['revision']['version']}", method="DELETE", token=token)

    # Finally delete the process group itself
    pg_info = api_request(f"/process-groups/{pg_id}", token=token)
    api_request(f"/process-groups/{pg_id}?version={pg_info['revision']['version']}", method="DELETE", token=token)


def create_phase3_flow():
    print("[1/8] Authenticating with Apache NiFi REST API...")
    token = get_auth_token()
    root_id = get_root_process_group_id(token)
    print(f"      Root Process Group ID: {root_id}")

    # Check for existing Phase 3 process group and remove it for idempotent configuration
    root_flow = api_request(f"/flow/process-groups/{root_id}", token=token)["processGroupFlow"]["flow"]
    for pg in root_flow.get("processGroups", []):
        if pg["component"]["name"] == "Phase 3 - E-Commerce Ingestion":
            print(f"      Removing existing process group: {pg['id']}")
            stop_and_delete_process_group(pg["id"], token)
            time.sleep(1)

    print("[2/8] Creating Process Group 'Phase 3 - E-Commerce Ingestion'...")
    pg_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "Phase 3 - E-Commerce Ingestion",
            "position": {"x": 300.0, "y": 80.0},
            "comments": "Consumes synthetic e-commerce orders from RabbitMQ, stores raw events in MongoDB, and ingests normalized records into PostgreSQL."
        }
    }
    pg_res = api_request(f"/process-groups/{root_id}/process-groups", method="POST", data=pg_payload, token=token)
    pg_id = pg_res["id"]
    print(f"      Created Process Group ID: {pg_id}")

    print("[3/8] Creating and enabling PostgreSQL DBCPConnectionPool Controller Service...")
    pg_user = os.getenv("POSTGRES_USER", "postgres")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "postgres")

    cs_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "PostgreSQL_Connection_Pool",
            "type": "org.apache.nifi.dbcp.DBCPConnectionPool",
            "bundle": {"group": "org.apache.nifi", "artifact": "nifi-dbcp-service-nar", "version": "1.26.0"},
            "properties": {
                "Database Connection URL": "jdbc:postgresql://postgres:5432/ecommerce",
                "Database Driver Class Name": "org.postgresql.Driver",
                "database-driver-locations": "/opt/nifi/nifi-current/drivers/postgresql-42.7.3.jar",
                "Database User": pg_user,
                "Password": pg_pass,
                "Max Wait Time": "500 millis",
                "Max Total Connections": "10"
            }
        }
    }
    cs_res = api_request(f"/process-groups/{pg_id}/controller-services", method="POST", data=cs_payload, token=token)
    cs_id = cs_res["id"]

    # Enable Controller Service
    api_request(
        f"/controller-services/{cs_id}/run-status",
        method="PUT",
        data={"revision": cs_res["revision"], "state": "ENABLED"},
        token=token
    )

    # Wait for Controller Service to become ENABLED
    for attempt in range(15):
        time.sleep(1)
        s_info = api_request(f"/controller-services/{cs_id}", token=token)
        cs_state = s_info["component"]["state"]
        if cs_state == "ENABLED":
            print(f"      DBCP Connection Pool enabled successfully (ID: {cs_id})")
            break
    else:
        raise RuntimeError(f"Controller service {cs_id} failed to enable. State: {cs_state}")

    print("[4/8] Creating Processors inside Process Group...")

    # RabbitMQ Credentials & Queue
    amqp_user = os.getenv("RABBITMQ_USER", "ecommerce_app")
    amqp_pass = os.getenv("RABBITMQ_PASSWORD", "")
    amqp_queue = os.getenv("RABBITMQ_QUEUE", "ecommerce_orders")

    # MongoDB Credentials & Database
    mongo_user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "mongoadmin")
    mongo_pass = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "mongopassword")
    mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@mongodb:27017/admin"

    # Processor 1: ConsumeAMQP
    p_amqp_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "Consume_RabbitMQ_Orders",
            "type": "org.apache.nifi.amqp.processors.ConsumeAMQP",
            "bundle": {"group": "org.apache.nifi", "artifact": "nifi-amqp-nar", "version": "1.26.0"},
            "position": {"x": 400.0, "y": 60.0},
            "config": {
                "properties": {
                    "Queue": amqp_queue,
                    "Host Name": "rabbitmq",
                    "Port": "5672",
                    "Virtual Host": "/",
                    "User Name": amqp_user,
                    "Password": amqp_pass,
                    "auto.acknowledge": "false",
                    "batch.size": "10"
                }
            }
        }
    }
    p_amqp = api_request(f"/process-groups/{pg_id}/processors", method="POST", data=p_amqp_payload, token=token)
    amqp_id = p_amqp["id"]
    print(f"      Created Consume_RabbitMQ_Orders (ID: {amqp_id})")

    # Processor 2: PutMongo
    p_mongo_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "Store_Raw_Order_MongoDB",
            "type": "org.apache.nifi.processors.mongodb.PutMongo",
            "bundle": {"group": "org.apache.nifi", "artifact": "nifi-mongodb-nar", "version": "1.26.0"},
            "position": {"x": 100.0, "y": 320.0},
            "config": {
                "properties": {
                    "Mongo URI": mongo_uri,
                    "Mongo Database Name": "ecommerce",
                    "Mongo Collection Name": "raw_orders",
                    "Mode": "update",
                    "Upsert": "true",
                    "Update Query Key": "event_id",
                    "Character Set": "UTF-8"
                },
                "autoTerminatedRelationships": ["success"]
            }
        }
    }
    p_mongo = api_request(f"/process-groups/{pg_id}/processors", method="POST", data=p_mongo_payload, token=token)
    mongo_id = p_mongo["id"]
    print(f"      Created Store_Raw_Order_MongoDB (ID: {mongo_id})")

    # Processor 3: Log_MongoDB_Failure
    p_mongo_fail_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "Log_MongoDB_Failure",
            "type": "org.apache.nifi.processors.standard.LogAttribute",
            "bundle": {"group": "org.apache.nifi", "artifact": "nifi-standard-nar", "version": "1.26.0"},
            "position": {"x": 100.0, "y": 580.0},
            "config": {
                "properties": {
                    "Log Level": "error",
                    "Log Payload": "true",
                    "attributes-to-log-regex": ".*"
                },
                "autoTerminatedRelationships": []
            }
        }
    }
    p_mongo_fail = api_request(f"/process-groups/{pg_id}/processors", method="POST", data=p_mongo_fail_payload, token=token)
    mongo_fail_id = p_mongo_fail["id"]
    print(f"      Created Log_MongoDB_Failure (ID: {mongo_fail_id})")

    # Groovy script for ExecuteGroovyScript
    groovy_code = """
import groovy.json.JsonSlurper
import java.sql.Timestamp
import java.time.Instant

def flowFile = session.get()
if (!flowFile) return

try {
    def text = ''
    session.read(flowFile, { inputStream ->
        text = inputStream.text
    } as InputStreamCallback)

    def event = new JsonSlurper().parseText(text)
    def cust = event.customer
    def ord = event.order
    def items = event.items

    def parseTs = { str ->
        if (!str) return new Timestamp(System.currentTimeMillis())
        try {
            return Timestamp.from(Instant.parse(str.toString()))
        } catch (Exception e) {
            return new Timestamp(System.currentTimeMillis())
        }
    }

    def custTs = parseTs(cust.created_at)
    def ordDateTs = parseTs(ord.order_date)
    def ordCreatedTs = parseTs(ord.created_at ?: ord.order_date)
    def totalAmount = new BigDecimal(ord.total_amount.toString())

    SQL.mydb.withTransaction {
        // 1. Insert or update customer
        SQL.mydb.execute('''
            INSERT INTO customers (customer_id, first_name, last_name, email, phone, city, state, country, postal_code, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (customer_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                phone = EXCLUDED.phone,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                country = EXCLUDED.country,
                postal_code = EXCLUDED.postal_code
        ''', [cust.customer_id, cust.first_name, cust.last_name, cust.email, cust.phone, cust.city, cust.state, cust.country, cust.postal_code, custTs])

        // 2. Insert or update order
        SQL.mydb.execute('''
            INSERT INTO orders (order_id, customer_id, order_status, order_date, total_amount, currency, payment_method, shipping_address, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (order_id) DO UPDATE SET
                order_status = EXCLUDED.order_status,
                total_amount = EXCLUDED.total_amount,
                payment_method = EXCLUDED.payment_method,
                shipping_address = EXCLUDED.shipping_address,
                updated_at = CURRENT_TIMESTAMP
        ''', [ord.order_id, ord.customer_id, ord.order_status, ordDateTs, totalAmount, ord.currency ?: 'USD', ord.payment_method, ord.shipping_address, ordCreatedTs])

        // 3. Insert or update line items
        for (item in items) {
            def itemTs = parseTs(item.created_at ?: ord.order_date)
            def unitPrice = new BigDecimal(item.unit_price.toString())
            def totalPrice = new BigDecimal(item.total_price.toString())
            def quantity = Integer.valueOf(item.quantity.toString())

            SQL.mydb.execute('''
                INSERT INTO order_items (item_id, order_id, product_id, product_name, category, quantity, unit_price, total_price, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (item_id) DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    unit_price = EXCLUDED.unit_price,
                    total_price = EXCLUDED.total_price
            ''', [item.item_id, item.order_id, item.product_id, item.product_name, item.category, quantity, unitPrice, totalPrice, itemTs])
        }
    }

    flowFile = session.putAttribute(flowFile, 'postgres.ingest.status', 'SUCCESS')
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error('Failed to ingest order to PostgreSQL: ' + e.getMessage(), e)
    flowFile = session.putAttribute(flowFile, 'postgres.ingest.error', e.getMessage() ?: 'Unknown error')
    session.transfer(flowFile, REL_FAILURE)
}
"""

    # Processor 4: ExecuteGroovyScript
    p_groovy_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "Ingest_Normalized_PostgreSQL",
            "type": "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
            "bundle": {"group": "org.apache.nifi", "artifact": "nifi-groovyx-nar", "version": "1.26.0"},
            "position": {"x": 700.0, "y": 320.0},
            "config": {
                "properties": {
                    "groovyx-script-body": groovy_code,
                    "SQL.mydb": cs_id
                },
                "autoTerminatedRelationships": ["success"]
            }
        }
    }
    p_groovy = api_request(f"/process-groups/{pg_id}/processors", method="POST", data=p_groovy_payload, token=token)
    groovy_id = p_groovy["id"]
    print(f"      Created Ingest_Normalized_PostgreSQL (ID: {groovy_id})")

    # Processor 5: Log_PostgreSQL_Failure
    p_pg_fail_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "Log_PostgreSQL_Failure",
            "type": "org.apache.nifi.processors.standard.LogAttribute",
            "bundle": {"group": "org.apache.nifi", "artifact": "nifi-standard-nar", "version": "1.26.0"},
            "position": {"x": 700.0, "y": 580.0},
            "config": {
                "properties": {
                    "Log Level": "error",
                    "Log Payload": "true",
                    "attributes-to-log-regex": ".*"
                },
                "autoTerminatedRelationships": []
            }
        }
    }
    p_pg_fail = api_request(f"/process-groups/{pg_id}/processors", method="POST", data=p_pg_fail_payload, token=token)
    pg_fail_id = p_pg_fail["id"]
    print(f"      Created Log_PostgreSQL_Failure (ID: {pg_fail_id})")

    # Child Process Group: Dead Letter & Recovery
    dlq_pg_payload = {
        "revision": {"version": 0},
        "component": {
            "name": "Dead Letter & Recovery",
            "position": {"x": 400.0, "y": 750.0},
            "comments": "Quarantines failed order events durably on disk for operator inspection, audit, and controlled idempotent replay."
        }
    }
    sub_pg_res = api_request(f"/process-groups/{pg_id}/process-groups", method="POST", data=dlq_pg_payload, token=token)
    sub_pg_id = sub_pg_res["id"]
    print(f"      Created child Process Group 'Dead Letter & Recovery' (ID: {sub_pg_id})")

    # Inside Dead Letter & Recovery: Input Ports
    p_in_m = api_request(
        f"/process-groups/{sub_pg_id}/input-ports",
        method="POST",
        data={"revision": {"version": 0}, "component": {"name": "MongoDB_DLQ_In", "position": {"x": 100.0, "y": 50.0}}},
        token=token
    )
    m_in_id = p_in_m["id"]

    p_in_p = api_request(
        f"/process-groups/{sub_pg_id}/input-ports",
        method="POST",
        data={"revision": {"version": 0}, "component": {"name": "PostgreSQL_DLQ_In", "position": {"x": 600.0, "y": 50.0}}},
        token=token
    )
    p_in_id = p_in_p["id"]

    # Inside Dead Letter & Recovery: Output Ports
    p_out_m = api_request(
        f"/process-groups/{sub_pg_id}/output-ports",
        method="POST",
        data={"revision": {"version": 0}, "component": {"name": "Retry_MongoDB_Out", "position": {"x": 100.0, "y": 500.0}}},
        token=token
    )
    m_out_id = p_out_m["id"]

    p_out_p = api_request(
        f"/process-groups/{sub_pg_id}/output-ports",
        method="POST",
        data={"revision": {"version": 0}, "component": {"name": "Retry_PostgreSQL_Out", "position": {"x": 600.0, "y": 500.0}}},
        token=token
    )
    p_out_id = p_out_p["id"]

    # Inside Dead Letter & Recovery: Quarantine Processors (Kept STOPPED so FlowFiles park in inbound queue)
    q_m = api_request(
        f"/process-groups/{sub_pg_id}/processors",
        method="POST",
        data={
            "revision": {"version": 0},
            "component": {
                "name": "Quarantine_MongoDB_DLQ",
                "type": "org.apache.nifi.processors.attributes.UpdateAttribute",
                "bundle": {"group": "org.apache.nifi", "artifact": "nifi-update-attribute-nar", "version": "1.26.0"},
                "position": {"x": 100.0, "y": 260.0},
                "config": {
                    "properties": {
                        "dlq_destination": "MongoDB",
                        "dlq_quarantined_at": "${now():format('yyyy-MM-dd HH:mm:ss')}",
                        "dlq_resolution_status": "PENDING_OPERATOR_REVIEW"
                    }
                }
            }
        },
        token=token
    )
    q_m_id = q_m["id"]

    q_p = api_request(
        f"/process-groups/{sub_pg_id}/processors",
        method="POST",
        data={
            "revision": {"version": 0},
            "component": {
                "name": "Quarantine_PostgreSQL_DLQ",
                "type": "org.apache.nifi.processors.attributes.UpdateAttribute",
                "bundle": {"group": "org.apache.nifi", "artifact": "nifi-update-attribute-nar", "version": "1.26.0"},
                "position": {"x": 600.0, "y": 260.0},
                "config": {
                    "properties": {
                        "dlq_destination": "PostgreSQL",
                        "dlq_quarantined_at": "${now():format('yyyy-MM-dd HH:mm:ss')}",
                        "dlq_resolution_status": "PENDING_OPERATOR_REVIEW"
                    }
                }
            }
        },
        token=token
    )
    q_p_id = q_p["id"]

    # Inside Dead Letter & Recovery: Internal connections
    api_request(
        f"/process-groups/{sub_pg_id}/connections",
        method="POST",
        data={
            "revision": {"version": 0},
            "component": {
                "name": "MongoDB Dead-Letter Queue",
                "source": {"id": m_in_id, "type": "INPUT_PORT", "groupId": sub_pg_id},
                "destination": {"id": q_m_id, "type": "PROCESSOR", "groupId": sub_pg_id},
                "backPressureObjectThreshold": 10000,
                "backPressureDataSizeThreshold": "1 GB"
            }
        },
        token=token
    )

    api_request(
        f"/process-groups/{sub_pg_id}/connections",
        method="POST",
        data={
            "revision": {"version": 0},
            "component": {
                "name": "PostgreSQL Dead-Letter Queue",
                "source": {"id": p_in_id, "type": "INPUT_PORT", "groupId": sub_pg_id},
                "destination": {"id": q_p_id, "type": "PROCESSOR", "groupId": sub_pg_id},
                "backPressureObjectThreshold": 10000,
                "backPressureDataSizeThreshold": "1 GB"
            }
        },
        token=token
    )

    api_request(
        f"/process-groups/{sub_pg_id}/connections",
        method="POST",
        data={
            "revision": {"version": 0},
            "component": {
                "name": "MongoDB Quarantine to Retry Port",
                "source": {"id": q_m_id, "type": "PROCESSOR", "groupId": sub_pg_id},
                "destination": {"id": m_out_id, "type": "OUTPUT_PORT", "groupId": sub_pg_id},
                "selectedRelationships": ["success"]
            }
        },
        token=token
    )

    api_request(
        f"/process-groups/{sub_pg_id}/connections",
        method="POST",
        data={
            "revision": {"version": 0},
            "component": {
                "name": "PostgreSQL Quarantine to Retry Port",
                "source": {"id": q_p_id, "type": "PROCESSOR", "groupId": sub_pg_id},
                "destination": {"id": p_out_id, "type": "OUTPUT_PORT", "groupId": sub_pg_id},
                "selectedRelationships": ["success"]
            }
        },
        token=token
    )

    print("[5/8] Creating connections and backpressure thresholds...")

    # Connection 1: RabbitMQ -> MongoDB
    conn_amqp_mongo = {
        "revision": {"version": 0},
        "component": {
            "name": "RabbitMQ to MongoDB Raw Storage",
            "source": {"id": amqp_id, "type": "PROCESSOR", "groupId": pg_id},
            "destination": {"id": mongo_id, "type": "PROCESSOR", "groupId": pg_id},
            "selectedRelationships": ["success"],
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB"
        }
    }
    api_request(f"/process-groups/{pg_id}/connections", method="POST", data=conn_amqp_mongo, token=token)
    print("      Connected: Consume_RabbitMQ_Orders [success] -> Store_Raw_Order_MongoDB")

    # Connection 2: RabbitMQ -> PostgreSQL
    conn_amqp_pg = {
        "revision": {"version": 0},
        "component": {
            "name": "RabbitMQ to PostgreSQL Normalized Ingestion",
            "source": {"id": amqp_id, "type": "PROCESSOR", "groupId": pg_id},
            "destination": {"id": groovy_id, "type": "PROCESSOR", "groupId": pg_id},
            "selectedRelationships": ["success"],
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB"
        }
    }
    api_request(f"/process-groups/{pg_id}/connections", method="POST", data=conn_amqp_pg, token=token)
    print("      Connected: Consume_RabbitMQ_Orders [success] -> Ingest_Normalized_PostgreSQL")

    # Connection 3: MongoDB Failure -> Log_MongoDB_Failure
    conn_mongo_fail = {
        "revision": {"version": 0},
        "component": {
            "name": "MongoDB Ingestion Failures",
            "source": {"id": mongo_id, "type": "PROCESSOR", "groupId": pg_id},
            "destination": {"id": mongo_fail_id, "type": "PROCESSOR", "groupId": pg_id},
            "selectedRelationships": ["failure"]
        }
    }
    api_request(f"/process-groups/{pg_id}/connections", method="POST", data=conn_mongo_fail, token=token)
    print("      Connected: Store_Raw_Order_MongoDB [failure] -> Log_MongoDB_Failure")

    # Connection 4: PostgreSQL Failure -> Log_PostgreSQL_Failure
    conn_pg_fail = {
        "revision": {"version": 0},
        "component": {
            "name": "PostgreSQL Ingestion Failures",
            "source": {"id": groovy_id, "type": "PROCESSOR", "groupId": pg_id},
            "destination": {"id": pg_fail_id, "type": "PROCESSOR", "groupId": pg_id},
            "selectedRelationships": ["failure"]
        }
    }
    api_request(f"/process-groups/{pg_id}/connections", method="POST", data=conn_pg_fail, token=token)
    print("      Connected: Ingest_Normalized_PostgreSQL [failure] -> Log_PostgreSQL_Failure")

    # Connection 5: Log_MongoDB_Failure -> MongoDB_DLQ_In
    api_request(
        f"/process-groups/{pg_id}/connections",
        method="POST",
        data={
            "revision": {"version": 0},
            "component": {
                "name": "MongoDB Failures to DLQ",
                "source": {"id": mongo_fail_id, "type": "PROCESSOR", "groupId": pg_id},
                "destination": {"id": m_in_id, "type": "INPUT_PORT", "groupId": sub_pg_id},
                "selectedRelationships": ["success"]
            }
        },
        token=token
    )
    print("      Connected: Log_MongoDB_Failure [success] -> MongoDB_DLQ_In")

    # Connection 6: Log_PostgreSQL_Failure -> PostgreSQL_DLQ_In
    api_request(
        f"/process-groups/{pg_id}/connections",
        method="POST",
        data={
            "revision": {"version": 0},
            "component": {
                "name": "PostgreSQL Failures to DLQ",
                "source": {"id": pg_fail_id, "type": "PROCESSOR", "groupId": pg_id},
                "destination": {"id": p_in_id, "type": "INPUT_PORT", "groupId": sub_pg_id},
                "selectedRelationships": ["success"]
            }
        },
        token=token
    )
    print("      Connected: Log_PostgreSQL_Failure [success] -> PostgreSQL_DLQ_In")

    # Connection 7: Retry_MongoDB_Out -> Store_Raw_Order_MongoDB
    api_request(
        f"/process-groups/{pg_id}/connections",
        method="POST",
        data={
            "revision": {"version": 0},
            "component": {
                "name": "Operator Retry to MongoDB",
                "source": {"id": m_out_id, "type": "OUTPUT_PORT", "groupId": sub_pg_id},
                "destination": {"id": mongo_id, "type": "PROCESSOR", "groupId": pg_id}
            }
        },
        token=token
    )
    print("      Connected: Retry_MongoDB_Out -> Store_Raw_Order_MongoDB")

    # Connection 8: Retry_PostgreSQL_Out -> Ingest_Normalized_PostgreSQL
    api_request(
        f"/process-groups/{pg_id}/connections",
        method="POST",
        data={
            "revision": {"version": 0},
            "component": {
                "name": "Operator Retry to PostgreSQL",
                "source": {"id": p_out_id, "type": "OUTPUT_PORT", "groupId": sub_pg_id},
                "destination": {"id": groovy_id, "type": "PROCESSOR", "groupId": pg_id}
            }
        },
        token=token
    )
    print("      Connected: Retry_PostgreSQL_Out -> Ingest_Normalized_PostgreSQL")

    # Start ports inside sub-group
    for port_id in [m_in_id, p_in_id]:
        p_info = api_request(f"/input-ports/{port_id}", token=token)
        api_request(f"/input-ports/{port_id}/run-status", method="PUT", data={"revision": p_info["revision"], "state": "RUNNING"}, token=token)
    for port_id in [m_out_id, p_out_id]:
        p_info = api_request(f"/output-ports/{port_id}", token=token)
        api_request(f"/output-ports/{port_id}/run-status", method="PUT", data={"revision": p_info["revision"], "state": "RUNNING"}, token=token)

    print("[6/8] Validating processor configurations...")
    time.sleep(2)
    proc_ids = [amqp_id, mongo_id, mongo_fail_id, groovy_id, pg_fail_id]
    all_valid = True
    for pid in proc_ids:
        p_data = api_request(f"/processors/{pid}", token=token)
        p_name = p_data["component"]["name"]
        val_status = p_data["component"]["validationStatus"]
        val_errors = p_data["component"].get("validationErrors", [])
        print(f"      Processor '{p_name}': status={val_status}, errors={len(val_errors)}")
        if val_status != "VALID":
            print(f"        ERROR DETAILS: {val_errors}")
            all_valid = False

    if not all_valid:
        raise RuntimeError("One or more processors have validation errors!")

    print("[7/8] Exporting flow definition to nifi/exports/phase3_ecommerce_ingestion.json...")
    EXPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    req_dl = urllib.request.Request(
        f"{NIFI_BASE_URL}/process-groups/{pg_id}/download",
        headers={"Authorization": f"Bearer {token}"}
    )
    with urllib.request.urlopen(req_dl, context=CTX) as resp:
        flow_json_bytes = resp.read()
        flow_json_obj = json.loads(flow_json_bytes.decode("utf-8"))

    with open(EXPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(flow_json_obj, f, indent=2)
    print(f"      Export saved ({len(flow_json_bytes)} bytes) -> {EXPORT_PATH}")

    print("[8/8] Phase 3 flow successfully created and ready in STOPPED state.")
    return {
        "process_group_id": pg_id,
        "controller_service_id": cs_id,
        "processors": {
            "amqp": amqp_id,
            "mongo": mongo_id,
            "mongo_fail": mongo_fail_id,
            "groovy": groovy_id,
            "pg_fail": pg_fail_id
        }
    }


def set_components_state(state, processor_keys=None):
    """
    Enables/starts or stops processors in the Phase 3 process group.
    """
    token = get_auth_token()
    root_id = get_root_process_group_id(token)
    root_flow = api_request(f"/flow/process-groups/{root_id}", token=token)["processGroupFlow"]["flow"]

    pg_id = None
    for pg in root_flow.get("processGroups", []):
        if pg["component"]["name"] == "Phase 3 - E-Commerce Ingestion":
            pg_id = pg["id"]
            break

    if not pg_id:
        print("Phase 3 Process Group not found.")
        return

    flow = api_request(f"/flow/process-groups/{pg_id}", token=token)["processGroupFlow"]["flow"]
    processors = flow.get("processors", [])

    for p in processors:
        pid = p["id"]
        name = p["component"]["name"]
        rev = p["revision"]

        if processor_keys:
            match = False
            for k in processor_keys:
                if k.lower() in name.lower():
                    match = True
                    break
            if not match:
                continue

        p_info = api_request(f"/processors/{pid}", token=token)
        cur_status = p_info["status"]["runStatus"]
        if cur_status == state.capitalize():
            print(f"Processor '{name}' is already {cur_status}")
            continue

        api_request(
            f"/processors/{pid}/run-status",
            method="PUT",
            data={"revision": p_info["revision"], "state": state.upper()},
            token=token
        )
        print(f"Updated '{name}' -> {state.upper()}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd == "start":
            set_components_state("RUNNING")
        elif cmd == "stop":
            set_components_state("STOPPED")
        elif cmd == "start-destinations":
            set_components_state("RUNNING", ["mongo", "postgres", "log"])
        elif cmd == "start-consumer":
            set_components_state("RUNNING", ["amqp"])
        elif cmd == "export":
            token = get_auth_token()
            root_id = get_root_process_group_id(token)
            root_flow = api_request(f"/flow/process-groups/{root_id}", token=token)["processGroupFlow"]["flow"]
            for pg in root_flow.get("processGroups", []):
                if pg["component"]["name"] == "Phase 3 - E-Commerce Ingestion":
                    req_dl = urllib.request.Request(
                        f"{NIFI_BASE_URL}/process-groups/{pg['id']}/download",
                        headers={"Authorization": f"Bearer {token}"}
                    )
                    with urllib.request.urlopen(req_dl, context=CTX) as resp:
                        data = json.loads(resp.read().decode("utf-8"))
                    with open(EXPORT_PATH, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2)
                    print(f"Exported to {EXPORT_PATH}")
                    break
        else:
            print("Unknown command. Options: create (default), start, stop, start-destinations, start-consumer, export")
    else:
        create_phase3_flow()
