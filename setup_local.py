#!/usr/bin/env python3
"""
Zerobus IoT Real-Time — Local Setup
=====================================
Configures the simulators to connect to your Databricks workspace.

Prerequisites: Run setup_workspace.py in Databricks first.
It will give you the credentials to paste here.

Usage:
    python setup_local.py
"""

import json
import os
import re
import sys
import time


def clean_url(url):
    """Extract clean workspace URL from any Databricks URL."""
    url = url.strip().rstrip("/")
    if not url.startswith("http"):
        url = f"https://{url}"
    match = re.match(r'(https?://[^/?#]+)', url)
    return match.group(1) if match else url


def prompt(label, default="", required=True, secret=False):
    while True:
        suffix = f" [{default}]" if default else ""
        if secret:
            import getpass
            val = getpass.getpass(f"  {label}{suffix}: ") or default
        else:
            val = input(f"  {label}{suffix}: ") or default
        if val or not required:
            return val
        print("    Required.")


def extract_workspace_id(url):
    match = re.search(r'adb-(\d+)', url)
    return match.group(1) if match else ""


def main():
    print("=" * 60)
    print("  Zerobus IoT Real-Time — Local Setup")
    print("=" * 60)
    print()
    print("  Prerequisites: run setup_workspace.py in Databricks first.")
    print("  It will show the credentials you need to paste below.\n")

    # ── Install dependencies ──
    print("[1/4] Installing dependencies...")
    import subprocess
    for pkg in ["requests", "databricks-zerobus-ingest-sdk"]:
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", "-q", "--break-system-packages", pkg],
                capture_output=True, text=True)
            if result.returncode == 0:
                print(f"  {pkg}: OK")
            else:
                print(f"  {pkg}: FAILED — try manually: pip3 install {pkg}")
        except Exception:
            print(f"  {pkg}: FAILED — try manually: pip3 install {pkg}")
    print()

    # ── Workspace config ──
    print("[2/4] Workspace configuration\n")
    raw_url = prompt("Databricks workspace URL")
    workspace_url = clean_url(raw_url)
    if workspace_url != raw_url.strip():
        print(f"    Cleaned to: {workspace_url}")

    workspace_id = extract_workspace_id(workspace_url)
    if not workspace_id:
        workspace_id = prompt("Workspace ID (number from URL)", required=False)

    catalog = prompt("Catalog name")
    schema = prompt("Schema name", default="iot_demo")

    if "." in schema:
        parts = schema.split(".")
        if len(parts) == 2 and parts[0] == catalog:
            schema = parts[1]
            print(f"    Cleaned schema to: {schema}")

    table_name = prompt("Table name", default="iot_events_raw")
    warehouse_id = prompt("SQL Warehouse ID")

    # ── Credentials (from setup_workspace.py output) ──
    print("\n[3/4] Service Principal credentials")
    print("      (copy from setup_workspace.py output in Databricks)\n")
    client_id = prompt("Client ID", secret=True)
    client_secret = prompt("Client Secret", secret=True)

    # ── Save config ──
    zerobus_endpoint = f"{workspace_id}.zerobus.us-west-2.cloud.databricks.com" if workspace_id else ""

    config = {
        "workspace_url": workspace_url,
        "workspace_id": workspace_id,
        "catalog": catalog,
        "schema": schema,
        "warehouse_id": warehouse_id,
        "client_id": client_id,
        "client_secret": client_secret,
        "zerobus_endpoint": zerobus_endpoint,
        "table": f"{catalog}.{schema}.{table_name}",
    }

    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)

    print(f"\n  Config saved to: config.json")
    print(f"  (credentials are stored locally, not displayed)")

    # ── Test connection ──
    print("\n[4/4] Testing connection...")
    try:
        import requests
        token_resp = requests.post(f"{workspace_url}/oidc/v1/token", data={
            "grant_type": "client_credentials", "client_id": client_id,
            "client_secret": client_secret, "scope": "all-apis"})
        if token_resp.status_code == 200:
            print("  Authentication: OK")
            token = token_resp.json()["access_token"]
            table = f"{catalog}.{schema}.{table_name}"
            sql_resp = requests.post(f"{workspace_url}/api/2.0/sql/statements",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"warehouse_id": warehouse_id,
                      "statement": f"SELECT COUNT(*) FROM {table}",
                      "wait_timeout": "20s", "disposition": "INLINE"})
            sql_data = sql_resp.json()
            state = sql_data.get("status", {}).get("state", "")
            if state == "SUCCEEDED":
                rows = sql_data.get("result", {}).get("data_array", [])
                count = rows[0][0] if rows else "0"
                print(f"  Table access: OK ({count} events)")
            else:
                error = sql_data.get("status", {}).get("error", {}).get("message", state)
                print(f"  Table access: {error}")
        else:
            print(f"  Authentication FAILED: {token_resp.status_code}")
            print(f"  Check your Client ID and Secret.")
    except ImportError:
        print("  Skipping test (install 'requests')")
    except Exception as e:
        print(f"  Test error: {e}")

    # ── Summary ──
    print("\n" + "=" * 60)
    print("  SETUP COMPLETE")
    print("=" * 60)
    print(f"""
  Workspace:  {workspace_url}
  Table:      {catalog}.{schema}.{table_name}
  Warehouse:  {warehouse_id}

  Run a simulator:
    python iot_simulator_rest.py
    python iot_device_simulator.py

  Open the dashboard in your browser to see real-time data.
""")
    print("=" * 60)


if __name__ == "__main__":
    main()
