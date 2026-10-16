#!/usr/bin/env python3
"""
scripts/update_phase3_dlq.py
Updates the Phase 3 NiFi pipeline to add a durable Dead-Letter & Recovery mechanism:
1. Updates Log_MongoDB_Failure and Log_PostgreSQL_Failure to remove autoTerminatedRelationships on 'success'.
2. Creates child Process Group 'Dead Letter & Recovery'.
3. Adds Input Ports (MongoDB_DLQ_In, PostgreSQL_DLQ_In) and Output Ports (Retry_MongoDB_Out, Retry_PostgreSQL_Out).
4. Adds stopped Quarantine processors holding failed FlowFiles in persistent disk queues.
5. Connects Failure loggers -> Input Ports -> Quarantine Queues -> Output Ports -> Ingestion processors for operator retry.
6. Exports updated flow to nifi/exports/phase3_ecommerce_ingestion.json.
"""

import os
import ssl
import json
import time
import urllib.request
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

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


def api_request(endpoint, method="GET", data=None, token=None):
    hdrs = {}
    if token:
        hdrs["Authorization"] = f"Bearer {token}"
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


def update_dlq():
    print("[1/7] Authenticating with NiFi API...")
    token = get_auth_token()
    root_id = api_request("/process-groups/root", token=token)["id"]

    # Locate Phase 3 process group
    root_flow = api_request(f"/flow/process-groups/{root_id}", token=token)["processGroupFlow"]["flow"]
    pg_list = [pg for pg in root_flow.get("processGroups", []) if pg["component"]["name"] == "Phase 3 - E-Commerce Ingestion"]
    if not pg_list:
        raise RuntimeError("Phase 3 - E-Commerce Ingestion process group not found!")
    pg_id = pg_list[0]["id"]
    print(f"      Phase 3 Process Group ID: {pg_id}")

    pg_flow = api_request(f"/flow/process-groups/{pg_id}", token=token)["processGroupFlow"]["flow"]
    processors = {p["component"]["name"]: p for p in pg_flow.get("processors", [])}

    log_mongo = processors["Log_MongoDB_Failure"]
    log_pg = processors["Log_PostgreSQL_Failure"]
    store_mongo = processors["Store_Raw_Order_MongoDB"]
    ingest_pg = processors["Ingest_Normalized_PostgreSQL"]

    print("[2/7] Stopping failure logging processors to update relationship routing...")
    for p_obj in [log_mongo, log_pg]:
        pid = p_obj["id"]
        p_info = api_request(f"/processors/{pid}", token=token)
        if p_info["status"]["runStatus"] != "Stopped":
            api_request(
                f"/processors/{pid}/run-status",
                method="PUT",
                data={"revision": p_info["revision"], "state": "STOPPED"},
                token=token
            )
            print(f"      Stopped {p_obj['component']['name']}")

    time.sleep(1)

    print("[3/7] Reconfiguring failure loggers to route 'success' to Dead-Letter Queue...")
    for p_obj in [log_mongo, log_pg]:
        pid = p_obj["id"]
        p_info = api_request(f"/processors/{pid}", token=token)
        update_payload = {
            "revision": p_info["revision"],
            "component": {
                "id": pid,
                "config": {
                    "autoTerminatedRelationships": []
                }
            }
        }
        api_request(f"/processors/{pid}", method="PUT", data=update_payload, token=token)
        print(f"      Cleared auto-terminated relationships on {p_obj['component']['name']}")

    # Check if 'Dead Letter & Recovery' sub-process group already exists
    sub_pg_id = None
    for child in pg_flow.get("processGroups", []):
        if child["component"]["name"] == "Dead Letter & Recovery":
            sub_pg_id = child["id"]
            break

    if not sub_pg_id:
        print("[4/7] Creating 'Dead Letter & Recovery' Process Group...")
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
        print(f"      Created sub-group ID: {sub_pg_id}")
    else:
        print(f"      Found existing 'Dead Letter & Recovery' sub-group ID: {sub_pg_id}")

    # Inspect sub-process group contents
    sub_flow = api_request(f"/flow/process-groups/{sub_pg_id}", token=token)["processGroupFlow"]["flow"]
    sub_ports_in = {p["component"]["name"]: p for p in sub_flow.get("inputPorts", [])}
    sub_ports_out = {p["component"]["name"]: p for p in sub_flow.get("outputPorts", [])}
    sub_procs = {p["component"]["name"]: p for p in sub_flow.get("processors", [])}

    print("[5/7] Configuring DLQ Input Ports, Output Ports, and Quarantine Processors...")

    # Input Port 1: MongoDB_DLQ_In
    if "MongoDB_DLQ_In" not in sub_ports_in:
        p_in_m = api_request(
            f"/process-groups/{sub_pg_id}/input-ports",
            method="POST",
            data={"revision": {"version": 0}, "component": {"name": "MongoDB_DLQ_In", "position": {"x": 100.0, "y": 50.0}}},
            token=token
        )
        m_in_id = p_in_m["id"]
    else:
        m_in_id = sub_ports_in["MongoDB_DLQ_In"]["id"]

    # Input Port 2: PostgreSQL_DLQ_In
    if "PostgreSQL_DLQ_In" not in sub_ports_in:
        p_in_p = api_request(
            f"/process-groups/{sub_pg_id}/input-ports",
            method="POST",
            data={"revision": {"version": 0}, "component": {"name": "PostgreSQL_DLQ_In", "position": {"x": 600.0, "y": 50.0}}},
            token=token
        )
        p_in_id = p_in_p["id"]
    else:
        p_in_id = sub_ports_in["PostgreSQL_DLQ_In"]["id"]

    # Output Port 1: Retry_MongoDB_Out
    if "Retry_MongoDB_Out" not in sub_ports_out:
        p_out_m = api_request(
            f"/process-groups/{sub_pg_id}/output-ports",
            method="POST",
            data={"revision": {"version": 0}, "component": {"name": "Retry_MongoDB_Out", "position": {"x": 100.0, "y": 500.0}}},
            token=token
        )
        m_out_id = p_out_m["id"]
    else:
        m_out_id = sub_ports_out["Retry_MongoDB_Out"]["id"]

    # Output Port 2: Retry_PostgreSQL_Out
    if "Retry_PostgreSQL_Out" not in sub_ports_out:
        p_out_p = api_request(
            f"/process-groups/{sub_pg_id}/output-ports",
            method="POST",
            data={"revision": {"version": 0}, "component": {"name": "Retry_PostgreSQL_Out", "position": {"x": 600.0, "y": 500.0}}},
            token=token
        )
        p_out_id = p_out_p["id"]
    else:
        p_out_id = sub_ports_out["Retry_PostgreSQL_Out"]["id"]

    # Quarantine Processor 1: Quarantine_MongoDB_DLQ (Kept STOPPED so FlowFiles park in the inbound queue)
    if "Quarantine_MongoDB_DLQ" not in sub_procs:
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
    else:
        q_m_id = sub_procs["Quarantine_MongoDB_DLQ"]["id"]

    # Quarantine Processor 2: Quarantine_PostgreSQL_DLQ (Kept STOPPED so FlowFiles park in the inbound queue)
    if "Quarantine_PostgreSQL_DLQ" not in sub_procs:
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
    else:
        q_p_id = sub_procs["Quarantine_PostgreSQL_DLQ"]["id"]

    # Internal sub-group connections
    sub_conns = {c["component"]["name"]: c for c in sub_flow.get("connections", [])}

    # 1. MongoDB_DLQ_In -> Quarantine_MongoDB_DLQ (MongoDB Dead-Letter Queue)
    if "MongoDB Dead-Letter Queue" not in sub_conns:
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
        print("      Created internal connection: 'MongoDB Dead-Letter Queue'")

    # 2. PostgreSQL_DLQ_In -> Quarantine_PostgreSQL_DLQ (PostgreSQL Dead-Letter Queue)
    if "PostgreSQL Dead-Letter Queue" not in sub_conns:
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
        print("      Created internal connection: 'PostgreSQL Dead-Letter Queue'")

    # 3. Quarantine_MongoDB_DLQ -> Retry_MongoDB_Out
    if "MongoDB Quarantine to Retry Port" not in sub_conns:
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
        print("      Created internal connection: 'MongoDB Quarantine to Retry Port'")

    # 4. Quarantine_PostgreSQL_DLQ -> Retry_PostgreSQL_Out
    if "PostgreSQL Quarantine to Retry Port" not in sub_conns:
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
        print("      Created internal connection: 'PostgreSQL Quarantine to Retry Port'")

    print("[6/7] Connecting Phase 3 Loggers to DLQ Input Ports and Retry Ports to Ingestion Processors...")
    # Fetch latest parent connections
    pg_flow_latest = api_request(f"/flow/process-groups/{pg_id}", token=token)["processGroupFlow"]["flow"]
    parent_conns = {c["component"].get("name"): c for c in pg_flow_latest.get("connections", [])}

    # Connection: Log_MongoDB_Failure -> MongoDB_DLQ_In
    if "MongoDB Failures to DLQ" not in parent_conns:
        api_request(
            f"/process-groups/{pg_id}/connections",
            method="POST",
            data={
                "revision": {"version": 0},
                "component": {
                    "name": "MongoDB Failures to DLQ",
                    "source": {"id": log_mongo["id"], "type": "PROCESSOR", "groupId": pg_id},
                    "destination": {"id": m_in_id, "type": "INPUT_PORT", "groupId": sub_pg_id},
                    "selectedRelationships": ["success"]
                }
            },
            token=token
        )
        print("      Connected: Log_MongoDB_Failure [success] -> MongoDB_DLQ_In")

    # Connection: Log_PostgreSQL_Failure -> PostgreSQL_DLQ_In
    if "PostgreSQL Failures to DLQ" not in parent_conns:
        api_request(
            f"/process-groups/{pg_id}/connections",
            method="POST",
            data={
                "revision": {"version": 0},
                "component": {
                    "name": "PostgreSQL Failures to DLQ",
                    "source": {"id": log_pg["id"], "type": "PROCESSOR", "groupId": pg_id},
                    "destination": {"id": p_in_id, "type": "INPUT_PORT", "groupId": sub_pg_id},
                    "selectedRelationships": ["success"]
                }
            },
            token=token
        )
        print("      Connected: Log_PostgreSQL_Failure [success] -> PostgreSQL_DLQ_In")

    # Connection: Retry_MongoDB_Out -> Store_Raw_Order_MongoDB
    if "Operator Retry to MongoDB" not in parent_conns:
        api_request(
            f"/process-groups/{pg_id}/connections",
            method="POST",
            data={
                "revision": {"version": 0},
                "component": {
                    "name": "Operator Retry to MongoDB",
                    "source": {"id": m_out_id, "type": "OUTPUT_PORT", "groupId": sub_pg_id},
                    "destination": {"id": store_mongo["id"], "type": "PROCESSOR", "groupId": pg_id}
                }
            },
            token=token
        )
        print("      Connected: Retry_MongoDB_Out -> Store_Raw_Order_MongoDB")

    # Connection: Retry_PostgreSQL_Out -> Ingest_Normalized_PostgreSQL
    if "Operator Retry to PostgreSQL" not in parent_conns:
        api_request(
            f"/process-groups/{pg_id}/connections",
            method="POST",
            data={
                "revision": {"version": 0},
                "component": {
                    "name": "Operator Retry to PostgreSQL",
                    "source": {"id": p_out_id, "type": "OUTPUT_PORT", "groupId": sub_pg_id},
                    "destination": {"id": ingest_pg["id"], "type": "PROCESSOR", "groupId": pg_id}
                }
            },
            token=token
        )
        print("      Connected: Retry_PostgreSQL_Out -> Ingest_Normalized_PostgreSQL")

    time.sleep(1)

    print("      Starting input ports and output ports in sub-group...")
    for port_id in [m_in_id, p_in_id]:
        p_info = api_request(f"/input-ports/{port_id}", token=token)
        if p_info["status"]["runStatus"] != "Running":
            api_request(
                f"/input-ports/{port_id}/run-status",
                method="PUT",
                data={"revision": p_info["revision"], "state": "RUNNING"},
                token=token
            )
    for port_id in [m_out_id, p_out_id]:
        p_info = api_request(f"/output-ports/{port_id}", token=token)
        if p_info["status"]["runStatus"] != "Running":
            api_request(
                f"/output-ports/{port_id}/run-status",
                method="PUT",
                data={"revision": p_info["revision"], "state": "RUNNING"},
                token=token
            )

    # Restart loggers
    for p_obj in [log_mongo, log_pg]:
        pid = p_obj["id"]
        p_info = api_request(f"/processors/{pid}", token=token)
        api_request(
            f"/processors/{pid}/run-status",
            method="PUT",
            data={"revision": p_info["revision"], "state": "RUNNING"},
            token=token
        )
        print(f"      Restarted {p_obj['component']['name']}")

    print("[7/7] Exporting updated flow definition to nifi/exports/phase3_ecommerce_ingestion.json...")
    req_dl = urllib.request.Request(
        f"{NIFI_BASE_URL}/process-groups/{pg_id}/download",
        headers={"Authorization": f"Bearer {token}"}
    )
    with urllib.request.urlopen(req_dl, context=CTX) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    with open(EXPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"      Updated export saved -> {EXPORT_PATH}")
    print("\n[SUCCESS] Dead-Letter & Recovery architecture successfully integrated!")


if __name__ == "__main__":
    update_dlq()
