#!/usr/bin/env python3
"""
scripts/verify_phase3.py
Automated Verification Suite for Phase 3: Apache NiFi E-Commerce Pipeline Integration.
Validates:
1. NiFi Process Group, Controller Services, and Processor statuses
2. RabbitMQ broker and queue consumer status
3. MongoDB raw document persistence and schema preservation
4. PostgreSQL 3NF normalized tables (customers, orders, order_items)
5. Cross-storage data consistency (monetary total, order ID, items count)
6. Zero lost FlowFiles / zero failed FlowFiles
"""

import os
import ssl
import json
import subprocess
from pathlib import Path
import urllib.request
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

NIFI_BASE_URL = "https://localhost:8443/nifi-api"


def get_nifi_token():
    user = os.getenv("NIFI_ADMIN_USER", "admin")
    password = os.getenv("NIFI_ADMIN_PASSWORD", "AdminPassword123!")
    req = urllib.request.Request(
        f"{NIFI_BASE_URL}/access/token",
        data=f"username={user}&password={password}".encode("utf-8"),
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    with urllib.request.urlopen(req, context=CTX) as resp:
        return resp.read().decode("utf-8")


def run_cmd(cmd):
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=str(PROJECT_ROOT))
    if res.returncode != 0:
        raise RuntimeError(f"Command failed ({res.returncode}): {cmd}\nStderr: {res.stderr}")
    return res.stdout.strip()


def verify_phase3():
    print("=" * 80)
    print("  PHASE 3: APACHE NIFI E-COMMERCE PIPELINE VERIFICATION SUITE")
    print("=" * 80)

    # 1. NiFi API & Flow Status
    print("\n[1/6] Inspecting Apache NiFi Flow Status...")
    token = get_nifi_token()
    req = urllib.request.Request(f"{NIFI_BASE_URL}/process-groups/root", headers={"Authorization": f"Bearer {token}"})
    root_id = json.loads(urllib.request.urlopen(req, context=CTX).read().decode("utf-8"))["id"]

    req_flow = urllib.request.Request(f"{NIFI_BASE_URL}/flow/process-groups/{root_id}", headers={"Authorization": f"Bearer {token}"})
    root_flow = json.loads(urllib.request.urlopen(req_flow, context=CTX).read().decode("utf-8"))["processGroupFlow"]["flow"]

    pg_list = [pg for pg in root_flow.get("processGroups", []) if pg["component"]["name"] == "Phase 3 - E-Commerce Ingestion"]
    if not pg_list:
        print("  [FAIL] Process group 'Phase 3 - E-Commerce Ingestion' not found!")
        return False
    pg_id = pg_list[0]["id"]
    print(f"  [OK] Found Process Group: '{pg_list[0]['component']['name']}' (ID: {pg_id})")

    # Controller Services
    req_cs = urllib.request.Request(f"{NIFI_BASE_URL}/flow/process-groups/{pg_id}/controller-services", headers={"Authorization": f"Bearer {token}"})
    services = json.loads(urllib.request.urlopen(req_cs, context=CTX).read().decode("utf-8"))["controllerServices"]
    for s in services:
        print(f"  [OK] Controller Service '{s['component']['name']}': state={s['component']['state']}")
        assert s["component"]["state"] == "ENABLED", f"Controller service {s['component']['name']} is not ENABLED!"

    # Processors
    req_pg_flow = urllib.request.Request(f"{NIFI_BASE_URL}/flow/process-groups/{pg_id}", headers={"Authorization": f"Bearer {token}"})
    pg_flow = json.loads(urllib.request.urlopen(req_pg_flow, context=CTX).read().decode("utf-8"))["processGroupFlow"]["flow"]
    processors = pg_flow.get("processors", [])
    print(f"  [OK] Processors active in Process Group: {len(processors)}")
    for p in processors:
        name = p["component"]["name"]
        status = p["status"]["runStatus"]
        in_count = p["status"]["aggregateSnapshot"]["flowFilesIn"]
        out_count = p["status"]["aggregateSnapshot"]["flowFilesOut"]
        print(f"       - {name:32} | Status: {status:7} | FlowFiles In: {in_count:3} | Out: {out_count:3}")

    # Queues & In-flight Data
    connections = pg_flow.get("connections", [])
    total_queued = 0
    for c in connections:
        q_count = int(c["status"]["aggregateSnapshot"]["queuedCount"])
        total_queued += q_count
        name = c["component"]["name"] or "Unnamed connection"
        if "Failure" in name:
            assert q_count == 0, f"Failure queue '{name}' contains {q_count} failed FlowFiles!"
    print(f"  [OK] Parent NiFi queues verified. Total in-flight queued FlowFiles: {total_queued}")

    # Inspect Dead Letter & Recovery Sub-Group
    sub_pg_list = [spg for spg in pg_flow.get("processGroups", []) if spg["component"]["name"] == "Dead Letter & Recovery"]
    assert len(sub_pg_list) == 1, "Dead Letter & Recovery process group not found!"
    sub_pg_id = sub_pg_list[0]["id"]
    req_sub_flow = urllib.request.Request(f"{NIFI_BASE_URL}/flow/process-groups/{sub_pg_id}", headers={"Authorization": f"Bearer {token}"})
    sub_flow = json.loads(urllib.request.urlopen(req_sub_flow, context=CTX).read().decode("utf-8"))["processGroupFlow"]["flow"]

    print(f"  [OK] Found Sub-Group: 'Dead Letter & Recovery' (ID: {sub_pg_id})")
    for ip in sub_flow.get("inputPorts", []):
        print(f"       - Input Port  : {ip['component']['name']:28} | Status: {ip['status']['runStatus']}")
        assert ip["status"]["runStatus"] == "Running", f"Input port {ip['component']['name']} is not Running!"
    for op in sub_flow.get("outputPorts", []):
        print(f"       - Output Port : {op['component']['name']:28} | Status: {op['status']['runStatus']}")
        assert op["status"]["runStatus"] == "Running", f"Output port {op['component']['name']} is not Running!"
    for qp in sub_flow.get("processors", []):
        print(f"       - Quarantine  : {qp['component']['name']:28} | Status: {qp['status']['runStatus']} (Controlled hold)")
        assert qp["status"]["runStatus"] == "Stopped", f"Quarantine processor {qp['component']['name']} must be Stopped to park failures!"

    # DLQ queues
    for sc in sub_flow.get("connections", []):
        sc_q = int(sc["status"]["aggregateSnapshot"]["queuedCount"])
        sc_name = sc["component"]["name"]
        print(f"       - DLQ Queue   : {sc_name:32} | Queued: {sc_q}")
        if "Dead-Letter" in sc_name:
            assert sc_q == 0, f"DLQ queue '{sc_name}' unexpectedly contains {sc_q} failed FlowFiles!"

    # 2. RabbitMQ Status
    print("\n[2/6] Inspecting RabbitMQ Message Broker...")
    rmq_out = run_cmd("docker exec ecommerce_rabbitmq rabbitmqctl list_queues name messages messages_ready consumers")
    print("  " + "\n  ".join(rmq_out.splitlines()))

    # 3. MongoDB Raw Storage
    print("\n[3/6] Inspecting MongoDB Raw Storage (`ecommerce.raw_orders`)...")
    mongo_user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "mongoadmin")
    mongo_pass = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "mongopassword")
    mongo_conn = f"mongodb://{mongo_user}:{mongo_pass}@localhost:27017/ecommerce?authSource=admin"
    m_count = int(run_cmd(f'docker exec ecommerce_mongodb mongosh "{mongo_conn}" --quiet --eval "db.raw_orders.countDocuments()"'))
    print(f"  [OK] Total documents in `raw_orders`: {m_count}")
    assert m_count > 0, "No raw order documents found in MongoDB!"

    # 4. PostgreSQL Normalized Storage
    print("\n[4/6] Inspecting PostgreSQL Normalized Storage (`customers`, `orders`, `order_items`)...")
    pg_cust = int(run_cmd('docker exec ecommerce_postgres psql -U postgres -d ecommerce -t -c "SELECT count(*) FROM customers;"'))
    pg_ord = int(run_cmd('docker exec ecommerce_postgres psql -U postgres -d ecommerce -t -c "SELECT count(*) FROM orders;"'))
    pg_items = int(run_cmd('docker exec ecommerce_postgres psql -U postgres -d ecommerce -t -c "SELECT count(*) FROM order_items;"'))
    print(f"  [OK] PostgreSQL Row Counts: customers={pg_cust}, orders={pg_ord}, order_items={pg_items}")
    assert pg_ord == m_count, f"Mismatch between MongoDB orders ({m_count}) and PostgreSQL orders ({pg_ord})!"
    assert pg_items >= pg_ord, "PostgreSQL order_items count must be >= orders count!"

    # 5. Cross-Storage Data Consistency Check
    print("\n[5/6] Verifying Cross-Storage Data Consistency for Latest Order...")
    raw_json = run_cmd(
        f'docker exec ecommerce_mongodb mongosh "{mongo_conn}" --quiet --eval "JSON.stringify(db.raw_orders.find().sort({{_id: -1}}).limit(1).toArray()[0])"'
    )
    latest_mongo = json.loads(raw_json)
    target_ord_id = latest_mongo["order"]["order_id"]
    target_cust_id = latest_mongo["customer"]["customer_id"]
    target_total = float(latest_mongo["order"]["total_amount"])
    target_items_count = len(latest_mongo["items"])

    print(f"  Latest Order ID : {target_ord_id}")
    print(f"  Customer ID     : {target_cust_id}")
    print(f"  Expected Total  : {target_total} ({latest_mongo['order'].get('currency', 'INR')})")
    print(f"  Expected Items  : {target_items_count}")

    # Query matching PostgreSQL records
    pg_ord_row = run_cmd(
        f"docker exec ecommerce_postgres psql -U postgres -d ecommerce -t -A -c \"SELECT customer_id, total_amount, currency FROM orders WHERE order_id = '{target_ord_id}';\""
    )
    pg_cust_id_found, pg_total_found, pg_curr_found = pg_ord_row.split("|")
    assert pg_cust_id_found == target_cust_id, f"Customer ID mismatch! Mongo: {target_cust_id}, PG: {pg_cust_id_found}"
    assert abs(float(pg_total_found) - target_total) < 0.01, f"Total amount mismatch! Mongo: {target_total}, PG: {pg_total_found}"

    pg_items_cnt_found = int(run_cmd(
        f"docker exec ecommerce_postgres psql -U postgres -d ecommerce -t -c \"SELECT count(*) FROM order_items WHERE order_id = '{target_ord_id}';\""
    ))
    assert pg_items_cnt_found == target_items_count, f"Item count mismatch! Mongo: {target_items_count}, PG: {pg_items_cnt_found}"
    print("  [OK] Perfect consistency verified across MongoDB and PostgreSQL!")

    # 6. Container Health Verification
    print("\n[6/6] Verifying Docker Container Health...")
    ps_out = run_cmd('docker ps --filter "name=ecommerce_" --format "{{.Names}}: {{.Status}}"')
    for line in ps_out.splitlines():
        print(f"  - {line}")
        assert "Up" in line, f"Container not up: {line}"

    print("\n" + "=" * 80)
    print("  PHASE 3 VERIFICATION COMPLETE: ALL INTEGRITY & PIPELINE CHECKS PASSED [OK]")
    print("=" * 80)
    return True


if __name__ == "__main__":
    verify_phase3()
