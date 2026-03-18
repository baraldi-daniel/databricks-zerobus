# Databricks notebook source
# MAGIC %md
# MAGIC # Zerobus IoT Real-Time — Automated Setup
# MAGIC
# MAGIC This notebook is the **self-contained setup** for Zerobus IoT Real-Time.
# MAGIC Run each cell in order. No manual editing is required.
# MAGIC
# MAGIC ### What this notebook does:
# MAGIC 1. Detects your environment (user, workspace, Zerobus endpoint)
# MAGIC 2. Creates catalog, schema, and table (6 columns, no ingestion_latency_ms)
# MAGIC 3. Detects a SQL warehouse
# MAGIC 4. Generates all application files (app.yaml uses `value:` for warehouse ID)
# MAGIC 5. Deploys a Databricks App and waits for SP provisioning
# MAGIC 6. Creates OAuth secret for the app SP, grants permissions
# MAGIC 7. Shows credentials for local setup, then clears them

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Detect Environment

# COMMAND ----------

# Step 1 — Detect Environment (automatic)
import re, requests, json, time, base64

current_user = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = current_user.split("@")[0].replace(".", "_").replace("-", "_")
print(f"Current user: {current_user}")
print(f"User prefix:  {user_prefix}")

# Auth token
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
print("Auth token: obtained")

# Workspace URL — try spark.conf first, fallback to API
workspace_url = ""
try:
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "")
except Exception:
    pass

if not workspace_url:
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        workspace_url = ctx.browserHostName().getOrElse(lambda: "")
    except Exception:
        pass

if workspace_url and not workspace_url.startswith("http"):
    workspace_url = f"https://{workspace_url}"

base_url = workspace_url  # always https://
print(f"Workspace URL: {workspace_url}")

# Workspace ID — try multiple methods (spark.conf doesn't work on serverless)
workspace_id = ""
try:
    workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.orgId")
except Exception:
    pass

if not workspace_id:
    match = re.search(r'adb-(\d+)', workspace_url)
    if match:
        workspace_id = match.group(1)

if not workspace_id:
    try:
        resp = requests.get(f"{base_url}/api/2.1/unity-catalog/current-metastore-assignment", headers=headers)
        if resp.status_code == 200:
            workspace_id = str(resp.json().get("workspace_id", ""))
    except Exception:
        pass

zerobus_endpoint = f"{workspace_id}.zerobus.us-west-2.cloud.databricks.com" if workspace_id else "UNKNOWN"
print(f"Workspace ID:  {workspace_id}")
print(f"Zerobus:       {zerobus_endpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Configure (Widgets)

# COMMAND ----------

# Step 2 — Configure (widgets)
dbutils.widgets.text("catalog", f"{user_prefix}_catalog", "Catalog Name")
dbutils.widgets.text("schema", "iot_demo", "Schema Name")
dbutils.widgets.text("app_name", "iot-realtime", "App Name")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
APP_NAME = dbutils.widgets.get("app_name")

print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print(f"App:     {APP_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Create Catalog, Schema & Table

# COMMAND ----------

# Step 3 — Create Catalog, Schema & Table

def _offer_existing_catalogs():
    """List catalogs the user can access and auto-select the best one."""
    skip = {"hive_metastore", "system", "samples"}
    cats = [c[0] for c in spark.sql("SHOW CATALOGS").collect() if c[0] not in skip]
    print(f"\n  Cannot create new catalog. Available catalogs:")
    for i, c in enumerate(cats, 1):
        print(f"    {i}. {c}")
    if cats:
        global CATALOG
        CATALOG = cats[0]
        dbutils.widgets.remove("catalog")
        dbutils.widgets.text("catalog", CATALOG, "Catalog Name")
        print(f"\n  Auto-selected: {CATALOG}")

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
    print(f"Catalog '{CATALOG}' ready.")
except Exception as e:
    err = str(e)
    if "storage root" in err.lower() or "Default Storage" in err:
        storage_root = ""
        try:
            ms_resp = requests.get(f"{base_url}/api/2.1/unity-catalog/metastores/summary", headers=headers)
            if ms_resp.status_code == 200:
                storage_root = ms_resp.json().get("storage_root", "")
        except Exception:
            pass
        if not storage_root:
            try:
                ext_locs = spark.sql("SHOW EXTERNAL LOCATIONS").collect()
                for loc in ext_locs:
                    loc_url = loc["url"] if "url" in loc.asDict() else str(loc[1]) if len(loc) > 1 else ""
                    loc_name = loc["name"] if "name" in loc.asDict() else str(loc[0])
                    if loc_url and ("managed" in loc_name.lower() or "default" in loc_name.lower()):
                        storage_root = loc_url
                        break
                if not storage_root and ext_locs:
                    loc_url = ext_locs[0]["url"] if "url" in ext_locs[0].asDict() else str(ext_locs[0][1]) if len(ext_locs[0]) > 1 else ""
                    if loc_url:
                        storage_root = loc_url
            except Exception:
                pass
        if storage_root:
            loc = f"{storage_root.rstrip('/')}/{CATALOG}"
            try:
                spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}` MANAGED LOCATION '{loc}'")
                print(f"Catalog '{CATALOG}' ready.")
            except Exception:
                _offer_existing_catalogs()
        else:
            _offer_existing_catalogs()
    else:
        _offer_existing_catalogs()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
print(f"Schema '{CATALOG}.{SCHEMA}' ready.")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.iot_events_raw (
        device_id STRING,
        temperature DOUBLE,
        humidity DOUBLE,
        pressure DOUBLE,
        source STRING,
        event_time TIMESTAMP
    )
    USING DELTA
    COMMENT 'Raw IoT events ingested via Zerobus'
""")
print(f"Table '{CATALOG}.{SCHEMA}.iot_events_raw' ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Detect SQL Warehouse

# COMMAND ----------

# Step 4 — Detect SQL Warehouse

resp = requests.get(f"{base_url}/api/2.0/sql/warehouses", headers=headers)
resp.raise_for_status()
warehouses = resp.json().get("warehouses", [])

WAREHOUSE_ID = None
WAREHOUSE_NAME = None

candidates = []
for wh in warehouses:
    is_serverless = wh.get("warehouse_type") == "PRO" or wh.get("enable_serverless_compute", False)
    is_running = wh.get("state") == "RUNNING"
    score = (2 if is_serverless else 0) + (1 if is_running else 0)
    candidates.append((score, wh))

candidates.sort(key=lambda x: x[0], reverse=True)

if candidates:
    best = candidates[0][1]
    WAREHOUSE_ID = best["id"]
    WAREHOUSE_NAME = best["name"]
    print(f"Selected warehouse: {WAREHOUSE_NAME} (ID: {WAREHOUSE_ID})")
    print(f"  Type: {best.get('warehouse_type', 'N/A')}, State: {best.get('state', 'N/A')}")
else:
    raise Exception("No SQL warehouses found! Please create one first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Generate Application Files & Deploy

# COMMAND ----------

# Step 5 — Generate Application Files
# CRITICAL: app.yaml uses value: for warehouse ID (NOT valueFrom/resources — that doesn't work)

app_base = f"/Users/{current_user}/apps/{APP_NAME}"

# --- app.yaml (value: for warehouse, NOT valueFrom) ---
app_yaml_content = f"""command:
  - uvicorn
  - backend.main:app
  - --host
  - 0.0.0.0
  - --port
  - 8000

env:
  - name: DATABRICKS_CATALOG
    value: "{CATALOG}"
  - name: DATABRICKS_SCHEMA
    value: "{SCHEMA}"
  - name: DATABRICKS_SQL_WAREHOUSE_ID
    value: "{WAREHOUSE_ID}"
"""

# --- requirements.txt ---
requirements_content = """fastapi>=0.110
uvicorn[standard]>=0.27
httpx>=0.27
databricks-sdk>=0.20
"""

# --- backend/__init__.py ---
init_content = ""

# --- backend/main.py ---
# This is the working deployed version with pause/resume support
main_py_content = '''"""
IoT Real-Time Monitor - FastAPI Backend
WebSocket endpoint polls Delta table and pushes updates to React frontend.
"""
import os
import asyncio
import json
import logging
from datetime import datetime
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("iot-backend")

# Config from environment
WAREHOUSE_ID = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID", "")
CATALOG = os.environ.get("DATABRICKS_CATALOG", "")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "")
TABLE = f"{CATALOG}.{SCHEMA}.iot_events_raw"
POLL_INTERVAL = 3  # seconds


def get_auth_headers():
    """Get auth headers from Databricks SDK (works with app service principal)."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    host = w.config.host
    if not host.startswith("http"):
        host = f"https://{host}"
    return host, dict(w.config.authenticate())


async def execute_sql(query: str) -> dict:
    """Execute SQL via Databricks Statement API."""
    try:
        host, headers = get_auth_headers()
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{host}/api/2.0/sql/statements",
                headers=headers,
                json={
                    "warehouse_id": WAREHOUSE_ID,
                    "statement": query,
                    "wait_timeout": "20s",
                    "disposition": "INLINE",
                },
            )
            data = resp.json()
            if data.get("status", {}).get("state") != "SUCCEEDED":
                log.warning(f"SQL failed: {data.get('status', {})}")
                return {"columns": [], "rows": []}
            cols = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
            rows = data.get("result", {}).get("data_array", [])
            return {"columns": cols, "rows": rows}
    except Exception as e:
        log.error(f"SQL error: {e}")
        return {"columns": [], "rows": []}


async def fetch_dashboard_data() -> dict:
    """Fetch all dashboard data in parallel queries."""
    metrics_q = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT CASE WHEN event_time >= current_timestamp() - INTERVAL 10 MINUTES THEN device_id END) AS devices,
            SUM(CASE WHEN event_time >= current_timestamp() - INTERVAL 5 MINUTES THEN 1 ELSE 0 END) AS recent,
            SUM(CASE WHEN temperature > 40 AND event_time >= current_timestamp() - INTERVAL 60 MINUTES THEN 1 ELSE 0 END) AS alerts
        FROM {TABLE}
    """
    timeline_q = f"""
        SELECT date_trunc('minute', event_time) AS minute, COUNT(*) AS events,
               ROUND(AVG(temperature), 1) AS avg_temp, ROUND(AVG(humidity), 1) AS avg_hum
        FROM {TABLE}
        WHERE event_time >= current_timestamp() - INTERVAL 15 MINUTES
        GROUP BY 1 ORDER BY 1
    """
    devices_q = f"""
        SELECT device_id, COUNT(*) AS events,
               ROUND(AVG(temperature), 1) AS avg_temp, ROUND(MAX(temperature), 1) AS max_temp,
               ROUND(AVG(humidity), 1) AS avg_hum, ROUND(MAX(humidity), 1) AS max_hum,
               MAX(event_time) AS last_seen
        FROM {TABLE}
        WHERE event_time >= current_timestamp() - INTERVAL 10 MINUTES
        GROUP BY device_id ORDER BY device_id
    """
    recent_q = f"""
        SELECT device_id, ROUND(temperature, 1) AS temperature, ROUND(humidity, 1) AS humidity,
               ROUND(pressure, 1) AS pressure, source, event_time
        FROM {TABLE} ORDER BY event_time DESC LIMIT 25
    """
    temp_series_q = f"""
        SELECT device_id, event_time, ROUND(temperature, 1) AS temperature
        FROM {TABLE}
        WHERE event_time >= current_timestamp() - INTERVAL 15 MINUTES
        ORDER BY event_time
    """
    alerts_q = f"""
        SELECT device_id, ROUND(temperature, 1) AS temperature, ROUND(humidity, 1) AS humidity,
               event_time,
               CASE
                   WHEN temperature > 50 THEN 'CRITICAL'
                   WHEN temperature > 40 THEN 'WARNING'
                   WHEN humidity > 90 OR humidity < 20 THEN 'WARNING'
                   ELSE 'INFO'
               END AS severity,
               CASE
                   WHEN temperature > 40 THEN 'High Temperature'
                   WHEN humidity > 90 THEN 'High Humidity'
                   WHEN humidity < 20 THEN 'Low Humidity'
               END AS alert_type
        FROM {TABLE}
        WHERE event_time >= current_timestamp() - INTERVAL 60 MINUTES
          AND (temperature > 40 OR humidity > 90 OR humidity < 20)
        ORDER BY event_time DESC LIMIT 30
    """

    metrics, timeline, devices, recent, temp_series, alerts_data = await asyncio.gather(
        execute_sql(metrics_q),
        execute_sql(timeline_q),
        execute_sql(devices_q),
        execute_sql(recent_q),
        execute_sql(temp_series_q),
        execute_sql(alerts_q),
    )

    # Parse metrics
    m = metrics["rows"][0] if metrics["rows"] else ["0", "0", "0", "0"]
    total, active_devices, recent_count, alert_count = int(m[0]), int(m[1]), int(m[2]), int(m[3])
    rate = round(recent_count / 5.0, 1) if recent_count else 0

    def rows_to_dicts(result):
        return [dict(zip(result["columns"], row)) for row in result["rows"]]

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "metrics": {
            "total_events": total,
            "active_devices": active_devices,
            "events_per_min": rate,
            "active_alerts": alert_count,
        },
        "timeline": rows_to_dicts(timeline),
        "devices": rows_to_dicts(devices),
        "recent_events": rows_to_dicts(recent),
        "temp_series": rows_to_dicts(temp_series),
        "alerts": rows_to_dicts(alerts_data),
    }


# Connection manager for WebSocket clients
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []
        self.paused: set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
        log.info(f"Client connected. Total: {len(self.active)}")

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)
        self.paused.discard(ws)
        log.info(f"Client disconnected. Total: {len(self.active)}")

    def pause(self, ws: WebSocket):
        self.paused.add(ws)
        log.info(f"Client paused. Active polling: {len(self.active) - len(self.paused)}")

    def resume(self, ws: WebSocket):
        self.paused.discard(ws)
        log.info(f"Client resumed. Active polling: {len(self.active) - len(self.paused)}")

    @property
    def has_active_listeners(self):
        return len(self.active) > len(self.paused)

    async def broadcast(self, data: dict):
        payload = json.dumps(data)
        for ws in self.active[:]:
            if ws in self.paused:
                continue
            try:
                await ws.send_text(payload)
            except Exception:
                self.active.remove(ws)
                self.paused.discard(ws)


manager = ConnectionManager()


async def polling_loop():
    """Background task: poll Delta table and broadcast to all WebSocket clients."""
    while True:
        if manager.has_active_listeners:
            try:
                data = await fetch_dashboard_data()
                await manager.broadcast(data)
            except Exception as e:
                log.error(f"Poll error: {e}")
        await asyncio.sleep(POLL_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(polling_loop())
    log.info(f"Polling started (every {POLL_INTERVAL}s) | Table: {TABLE} | Warehouse: {WAREHOUSE_ID}")
    yield
    task.cancel()


app = FastAPI(title="IoT Real-Time Monitor", lifespan=lifespan)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    # Send initial data immediately
    try:
        data = await fetch_dashboard_data()
        await ws.send_text(json.dumps(data))
    except Exception as e:
        log.error(f"Initial send error: {e}")
    # Listen for pause/resume commands
    try:
        while True:
            msg = await ws.receive_text()
            if msg == "pause":
                manager.pause(ws)
            elif msg == "resume":
                manager.resume(ws)
    except WebSocketDisconnect:
        manager.disconnect(ws)


@app.get("/api/health")
async def health():
    return {"status": "ok", "table": TABLE, "warehouse": WAREHOUSE_ID, "poll_interval": POLL_INTERVAL}


@app.get("/api/snapshot")
async def snapshot():
    """REST fallback for initial page load."""
    return await fetch_dashboard_data()


from backend.frontend_html import FRONTEND_HTML  # noqa: E402


@app.get("/")
async def serve_index():
    return HTMLResponse(FRONTEND_HTML)
'''

# --- backend/frontend_html.py (with pause button) ---
frontend_html_content = r'''FRONTEND_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>IoT Real-Time Monitor</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,system-ui,sans-serif;background:#0f1117;color:#e1e4e8;min-height:100vh}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
a{color:#58a6ff;text-decoration:none}
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:#161b22}
::-webkit-scrollbar-thumb{background:#30363d;border-radius:3px}
.container{max-width:1400px;margin:0 auto;padding:20px 24px}
/* Header */
.header{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;flex-wrap:wrap;gap:12px}
.header-title{font-size:20px;font-weight:700;display:flex;align-items:center;gap:10px}
.status-badge{display:flex;align-items:center;gap:8px;font-size:13px}
.status-dot{width:8px;height:8px;border-radius:50%}
.status-dot.live{background:#3fb950;animation:pulse 1.5s infinite}
.status-dot.off{background:#f85149}
.status-text.live{color:#3fb950;font-weight:600}
.status-text.off{color:#f85149;font-weight:600}
.last-update{color:#484f58;margin-left:8px;font-size:12px}
/* Pipeline */
.pipeline{display:flex;gap:0;margin-bottom:20px;align-items:stretch}
.pipe-node{flex:1;background:#161b22;border:1px solid #30363d;padding:14px 10px;text-align:center;position:relative}
.pipe-node:first-child{border-radius:10px 0 0 10px}
.pipe-node:last-child{border-radius:0 10px 10px 0}
.pipe-node:not(:last-child){border-right:none}
.pipe-node:not(:last-child)::after{content:'';position:absolute;right:-8px;top:50%;transform:translateY(-50%);width:0;height:0;border-top:8px solid transparent;border-bottom:8px solid transparent;border-left:8px solid #30363d;z-index:1}
.pipe-label{font-size:11px;color:#8b949e;font-weight:600;text-transform:uppercase;letter-spacing:.5px}
.pipe-value{font-size:22px;font-weight:700;margin:4px 0}
.pipe-sub{font-size:10px;color:#484f58}
/* Tabs */
.tabs{display:flex;gap:0;border-bottom:1px solid #30363d;margin-bottom:20px}
.tab-btn{padding:10px 24px;cursor:pointer;font-size:13px;font-weight:500;background:none;border:none;color:#8b949e;border-bottom:2px solid transparent;transition:all .15s}
.tab-btn:hover{color:#c9d1d9}
.tab-btn.active{color:#58a6ff;border-bottom-color:#58a6ff}
.tab-content{display:none}
.tab-content.active{display:block}
/* Cards */
.grid4{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:16px}
.grid3{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin-bottom:16px}
.metric-card{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:16px;border-left:3px solid var(--accent)}
.metric-val{font-size:28px;font-weight:700}
.metric-label{font-size:12px;color:#8b949e;margin-top:4px}
.card{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:20px;margin-bottom:16px}
.card-title{font-size:14px;font-weight:600;margin-bottom:12px}
/* Tables */
table{width:100%;border-collapse:collapse;font-size:12px}
th{padding:8px 12px;text-align:left;color:#8b949e;border-bottom:1px solid #30363d;font-weight:600;white-space:nowrap}
td{padding:8px 12px;border-bottom:1px solid #21262d;color:#c9d1d9}
tr.alert-row{background:rgba(248,81,73,.06)}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:600}
.badge-blue{background:rgba(88,166,255,.12);color:#58a6ff}
.badge-red{background:rgba(248,81,73,.12);color:#f85149}
.badge-yellow{background:rgba(210,153,34,.12);color:#d29922}
.badge-green{background:rgba(63,185,80,.12);color:#3fb950}
/* SVG Chart */
.chart-wrap{width:100%;overflow-x:auto}
svg.chart{display:block}
svg.chart text{fill:#484f58;font-size:10px;font-family:-apple-system,system-ui,sans-serif}
svg.chart .axis-line{stroke:#30363d;stroke-width:1}
svg.chart .grid-line{stroke:#21262d;stroke-width:1}
svg.chart .bar{transition:opacity .15s}
svg.chart .bar:hover{opacity:.8}
/* Device cards */
.dev-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:16px;margin-bottom:16px}
.dev-card{background:#161b22;border:1px solid #30363d;border-radius:12px;padding:16px;text-align:center}
.dev-card.alert{border-color:rgba(248,81,73,.3)}
.dev-name{font-weight:700;font-size:13px;margin-bottom:6px}
.dev-status{font-size:18px;font-weight:700;margin-bottom:6px}
.dev-info{font-size:11px;color:#8b949e;line-height:1.8}
.empty{text-align:center;padding:40px;color:#484f58;font-size:13px}
.footer{text-align:center;padding:20px;font-size:11px;color:#30363d}
@media(max-width:900px){.grid4,.grid3{grid-template-columns:repeat(2,1fr)}.pipeline{flex-wrap:wrap}.pipe-node{min-width:120px}}
@media(max-width:600px){.grid4,.grid3{grid-template-columns:1fr}.header{flex-direction:column;align-items:flex-start}}
</style>
</head>
<body>
<div class="container" id="app">
  <!-- Header -->
  <div class="header">
    <div class="header-title"><span>&#x1F4E1;</span> IoT Real-Time Monitor</div>
    <div class="status-badge">
      <button id="pauseBtn" onclick="togglePause()" style="padding:4px 14px;border-radius:6px;border:1px solid #30363d;background:#161b22;color:#e1e4e8;cursor:pointer;font-size:12px;font-weight:600;margin-right:8px">&#x23F8; Pause</button>
      <div class="status-dot off" id="statusDot"></div>
      <span class="status-text off" id="statusText">DISCONNECTED</span>
      <span class="last-update" id="lastUpdate"></span>
    </div>
  </div>

  <!-- Pipeline -->
  <div class="pipeline">
    <div class="pipe-node">
      <div class="pipe-label">Devices</div>
      <div class="pipe-value" id="pipeDevices" style="color:#58a6ff">--</div>
      <div class="pipe-sub">Active (10m)</div>
    </div>
    <div class="pipe-node">
      <div class="pipe-label">Raw Events</div>
      <div class="pipe-value" id="pipeEvents" style="color:#3fb950">--</div>
      <div class="pipe-sub">Delta Table</div>
    </div>
    <div class="pipe-node">
      <div class="pipe-label">Throughput</div>
      <div class="pipe-value" id="pipeRate" style="color:#d29922">--</div>
      <div class="pipe-sub">Last 5 min</div>
    </div>
    <div class="pipe-node">
      <div class="pipe-label">Alerts</div>
      <div class="pipe-value" id="pipeAlerts" style="color:#3fb950">--</div>
      <div class="pipe-sub">Last hour</div>
    </div>
  </div>

  <!-- Tabs -->
  <div class="tabs">
    <button class="tab-btn active" data-tab="dashboard">Dashboard</button>
    <button class="tab-btn" data-tab="alerts">Alerts</button>
    <button class="tab-btn" data-tab="devices">Devices</button>
  </div>

  <!-- Dashboard Tab -->
  <div class="tab-content active" id="tab-dashboard">
    <div class="grid4" id="metricCards"></div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px" id="chartsRow">
      <div class="card">
        <div class="card-title">&#x1F321; Temperature Timeline</div>
        <div class="chart-wrap" id="chartTemp"></div>
      </div>
      <div class="card">
        <div class="card-title">&#x1F4C8; Events per Minute</div>
        <div class="chart-wrap" id="chartEvents"></div>
      </div>
    </div>
    <div class="card">
      <div class="card-title">&#x1F4CB; Live Event Stream</div>
      <div style="max-height:280px;overflow:auto" id="eventsTable"></div>
    </div>
  </div>

  <!-- Alerts Tab -->
  <div class="tab-content" id="tab-alerts">
    <div class="grid3" id="alertCounts"></div>
    <div class="card" id="alertsTableCard"></div>
  </div>

  <!-- Devices Tab -->
  <div class="tab-content" id="tab-devices">
    <div class="dev-grid" id="deviceCards"></div>
    <div class="card" id="deviceChartCard">
      <div class="card-title">Temperature by Device</div>
      <div class="chart-wrap" id="chartDevices"></div>
    </div>
  </div>

  <div class="footer">Zerobus + Delta Tables | WebSocket every 3s</div>
</div>

<script>
(function(){
"use strict";

/* -- Helpers -- */
function esc(s){var d=document.createElement('div');d.textContent=s;return d.innerHTML}
function fmt(ts){if(!ts)return'--';try{return new Date(ts).toLocaleTimeString()}catch(e){return ts}}
function fmtShort(ts){if(!ts)return'';try{var d=new Date(ts);return ('0'+d.getHours()).slice(-2)+':'+('0'+d.getMinutes()).slice(-2)}catch(e){return ts}}
function num(v){return v==null?0:typeof v==='number'?v:parseFloat(v)||0}

/* -- SVG Bar Chart -- */
function barChart(container,data,opts){
  if(!data||data.length===0){container.innerHTML='<div class="empty">No data yet</div>';return}
  var W=opts.width||560,H=opts.height||220;
  var ml=48,mr=12,mt=12,mb=32;
  var cw=W-ml-mr,ch=H-mt-mb;
  var n=data.length;
  var vals=data.map(function(d){return num(d[opts.key])});
  var maxV=Math.max.apply(null,vals);
  if(maxV===0)maxV=1;
  var niceMax=Math.ceil(maxV/5)*5||5;
  var barW=Math.max(4,Math.min(36,(cw/n)-4));
  var gap=(cw-barW*n)/(n+1);

  var lines='';
  for(var i=0;i<=5;i++){
    var yVal=niceMax*(i/5);
    var y=mt+ch-ch*(i/5);
    lines+='<line class="grid-line" x1="'+ml+'" y1="'+y+'" x2="'+(W-mr)+'" y2="'+y+'"/>';
    lines+='<text x="'+(ml-6)+'" y="'+(y+3)+'" text-anchor="end">'+Math.round(yVal)+'</text>';
  }
  lines+='<line class="axis-line" x1="'+ml+'" y1="'+mt+'" x2="'+ml+'" y2="'+(H-mb)+'"/>';
  lines+='<line class="axis-line" x1="'+ml+'" y1="'+(H-mb)+'" x2="'+(W-mr)+'" y2="'+(H-mb)+'"/>';

  var bars='';
  for(var j=0;j<n;j++){
    var x=ml+gap+(barW+gap)*j;
    var bh=ch*(vals[j]/niceMax);
    var y2=mt+ch-bh;
    bars+='<rect class="bar" x="'+x+'" y="'+y2+'" width="'+barW+'" height="'+bh+'" rx="2" fill="'+opts.color+'"/>';
    var step=Math.max(1,Math.floor(n/10));
    if(j%step===0||j===n-1){
      var lbl=data[j].label||'';
      bars+='<text x="'+(x+barW/2)+'" y="'+(H-mb+14)+'" text-anchor="middle" style="font-size:9px">'+esc(lbl)+'</text>';
    }
  }
  var titles='';
  for(var k=0;k<n;k++){
    var xk=ml+gap+(barW+gap)*k;
    var bhk=ch*(vals[k]/niceMax);
    var yk=mt+ch-bhk;
    titles+='<rect x="'+xk+'" y="'+yk+'" width="'+barW+'" height="'+bhk+'" fill="transparent"><title>'+(data[k].label||'')+': '+vals[k]+'</title></rect>';
  }

  container.innerHTML='<svg class="chart" viewBox="0 0 '+W+' '+H+'" width="100%" height="'+H+'">'+lines+bars+titles+'</svg>';
}

/* -- Tab switching -- */
var tabBtns=document.querySelectorAll('.tab-btn');
var tabPanels=document.querySelectorAll('.tab-content');
tabBtns.forEach(function(btn){
  btn.addEventListener('click',function(){
    tabBtns.forEach(function(b){b.classList.remove('active')});
    tabPanels.forEach(function(p){p.classList.remove('active')});
    btn.classList.add('active');
    document.getElementById('tab-'+btn.dataset.tab).classList.add('active');
  });
});

/* -- Render functions -- */
function renderPipeline(m){
  document.getElementById('pipeDevices').textContent=m.active_devices;
  document.getElementById('pipeEvents').textContent=num(m.total_events).toLocaleString();
  document.getElementById('pipeRate').textContent=m.events_per_min+'/min';
  var pa=document.getElementById('pipeAlerts');
  pa.textContent=m.active_alerts;
  pa.style.color=m.active_alerts>0?'#f85149':'#3fb950';
}

function renderMetrics(m){
  var items=[
    {l:'Total Events',v:num(m.total_events).toLocaleString(),c:'#58a6ff'},
    {l:'Events/min',v:m.events_per_min,c:'#3fb950'},
    {l:'Active Devices',v:m.active_devices,c:'#d29922'},
    {l:'Alerts (1h)',v:m.active_alerts,c:'#f85149'}
  ];
  var html='';
  items.forEach(function(it){
    html+='<div class="metric-card" style="--accent:'+it.c+'">'
      +'<div class="metric-val">'+esc(String(it.v))+'</div>'
      +'<div class="metric-label">'+esc(it.l)+'</div></div>';
  });
  document.getElementById('metricCards').innerHTML=html;
}

function renderCharts(timeline){
  var tData=timeline.map(function(r){return{label:fmtShort(r.minute),temp:num(r.avg_temp),events:num(r.events)}});
  barChart(document.getElementById('chartTemp'),tData,{key:'temp',color:'#f0883e'});
  barChart(document.getElementById('chartEvents'),tData,{key:'events',color:'#3fb950'});
}

function renderEventStream(events){
  if(!events||events.length===0){
    document.getElementById('eventsTable').innerHTML='<div class="empty">No events</div>';return;
  }
  var html='<table><thead><tr><th>Device</th><th>Temp</th><th>Humidity</th><th>Pressure</th><th>Source</th><th>Time</th></tr></thead><tbody>';
  events.forEach(function(e){
    var anom=num(e.temperature)>40||num(e.humidity)>90||num(e.humidity)<20;
    var cls=anom?' class="alert-row"':'';
    var tc=num(e.temperature)>40?' style="color:#f85149"':'';
    var hc=num(e.humidity)>90||num(e.humidity)<20?' style="color:#d29922"':'';
    html+='<tr'+cls+'>'
      +'<td><b>'+esc(e.device_id)+'</b></td>'
      +'<td'+tc+'>'+esc(e.temperature)+'&#176;C</td>'
      +'<td'+hc+'>'+esc(e.humidity)+'%</td>'
      +'<td>'+esc(e.pressure||'--')+'</td>'
      +'<td><span class="badge badge-blue">'+esc(e.source||'--')+'</span></td>'
      +'<td>'+esc(fmt(e.event_time))+'</td></tr>';
  });
  html+='</tbody></table>';
  document.getElementById('eventsTable').innerHTML=html;
}

function renderAlerts(alerts){
  var counts={CRITICAL:0,WARNING:0,INFO:0};
  (alerts||[]).forEach(function(a){counts[a.severity]=(counts[a.severity]||0)+1});
  var colors={CRITICAL:'#f85149',WARNING:'#d29922',INFO:'#58a6ff'};
  var badgeCls={CRITICAL:'badge-red',WARNING:'badge-yellow',INFO:'badge-blue'};
  var ch='';
  ['CRITICAL','WARNING','INFO'].forEach(function(s){
    ch+='<div class="metric-card" style="--accent:'+colors[s]+'">'
      +'<div class="metric-val">'+counts[s]+'</div>'
      +'<div class="metric-label">'+s+'</div></div>';
  });
  document.getElementById('alertCounts').innerHTML=ch;

  if(!alerts||alerts.length===0){
    document.getElementById('alertsTableCard').innerHTML='<div class="empty">No alerts in the last hour.</div>';return;
  }
  var html='<table><thead><tr><th>Device</th><th>Type</th><th>Severity</th><th>Temp</th><th>Humidity</th><th>Time</th></tr></thead><tbody>';
  alerts.forEach(function(a){
    html+='<tr>'
      +'<td>'+esc(a.device_id)+'</td>'
      +'<td>'+esc(a.alert_type||'--')+'</td>'
      +'<td><span class="badge '+(badgeCls[a.severity]||'badge-blue')+'">'+esc(a.severity)+'</span></td>'
      +'<td>'+esc(a.temperature)+'&#176;C</td>'
      +'<td>'+esc(a.humidity)+'%</td>'
      +'<td>'+esc(fmt(a.event_time))+'</td></tr>';
  });
  html+='</tbody></table>';
  document.getElementById('alertsTableCard').innerHTML=html;
}

function renderDevices(devices){
  if(!devices||devices.length===0){
    document.getElementById('deviceCards').innerHTML='<div class="empty" style="grid-column:1/-1">No active devices.</div>';
    document.getElementById('deviceChartCard').style.display='none';
    return;
  }
  var html='';
  devices.forEach(function(dv){
    var alert=num(dv.max_temp)>40;
    html+='<div class="dev-card'+(alert?' alert':'')+'">'
      +'<div class="dev-name">'+esc(dv.device_id)+'</div>'
      +'<div class="dev-status" style="color:'+(alert?'#f85149':'#3fb950')+'">'+(alert?'&#x26A0; ALERT':'&#x2713; OK')+'</div>'
      +'<div class="dev-info">'
      +'Temp: '+esc(dv.avg_temp)+'&#176;C (max '+esc(dv.max_temp)+'&#176;C)<br>'
      +'Humidity: '+esc(dv.avg_hum)+'%<br>'
      +'Events: '+esc(dv.events)
      +'</div></div>';
  });
  document.getElementById('deviceCards').innerHTML=html;

  document.getElementById('deviceChartCard').style.display='';
  var dData=devices.map(function(d){return{label:d.device_id.replace('sensor-','s'),avg:num(d.avg_temp),max:num(d.max_temp)}});
  renderDeviceChart(document.getElementById('chartDevices'),dData);
}

function renderDeviceChart(container,data){
  if(!data||data.length===0){container.innerHTML='<div class="empty">No data</div>';return}
  var W=560,H=240;
  var ml=48,mr=12,mt=12,mb=36;
  var cw=W-ml-mr,ch=H-mt-mb;
  var n=data.length;
  var allVals=[];
  data.forEach(function(d){allVals.push(d.avg,d.max)});
  var maxV=Math.max.apply(null,allVals)||1;
  var niceMax=Math.ceil(maxV/10)*10||10;
  var groupW=(cw/n);
  var barW=Math.max(4,Math.min(20,(groupW-12)/2));

  var svg='';
  for(var i=0;i<=5;i++){
    var yVal=niceMax*(i/5);
    var y=mt+ch-ch*(i/5);
    svg+='<line class="grid-line" x1="'+ml+'" y1="'+y+'" x2="'+(W-mr)+'" y2="'+y+'"/>';
    svg+='<text x="'+(ml-6)+'" y="'+(y+3)+'" text-anchor="end">'+Math.round(yVal)+'</text>';
  }
  svg+='<line class="axis-line" x1="'+ml+'" y1="'+mt+'" x2="'+ml+'" y2="'+(H-mb)+'"/>';
  svg+='<line class="axis-line" x1="'+ml+'" y1="'+(H-mb)+'" x2="'+(W-mr)+'" y2="'+(H-mb)+'"/>';
  if(40<=niceMax){
    var yAlert=mt+ch-ch*(40/niceMax);
    svg+='<line x1="'+ml+'" y1="'+yAlert+'" x2="'+(W-mr)+'" y2="'+yAlert+'" stroke="#f85149" stroke-dasharray="5 5" stroke-width="1"/>';
    svg+='<text x="'+(W-mr+2)+'" y="'+(yAlert+3)+'" fill="#f85149" style="font-size:9px">40&#176;C</text>';
  }
  for(var j=0;j<n;j++){
    var gx=ml+groupW*j+groupW/2;
    var x1=gx-barW-1;
    var x2=gx+1;
    var h1=ch*(data[j].avg/niceMax);
    var h2=ch*(data[j].max/niceMax);
    svg+='<rect class="bar" x="'+x1+'" y="'+(mt+ch-h1)+'" width="'+barW+'" height="'+h1+'" rx="2" fill="#58a6ff"><title>Avg: '+data[j].avg+'&#176;C</title></rect>';
    svg+='<rect class="bar" x="'+x2+'" y="'+(mt+ch-h2)+'" width="'+barW+'" height="'+h2+'" rx="2" fill="#f0883e"><title>Max: '+data[j].max+'&#176;C</title></rect>';
    svg+='<text x="'+gx+'" y="'+(H-mb+14)+'" text-anchor="middle" style="font-size:9px">'+esc(data[j].label)+'</text>';
  }
  svg+='<rect x="'+ml+'" y="'+(H-10)+'" width="10" height="10" rx="2" fill="#58a6ff"/>';
  svg+='<text x="'+(ml+14)+'" y="'+(H-2)+'" style="font-size:10px">Avg</text>';
  svg+='<rect x="'+(ml+44)+'" y="'+(H-10)+'" width="10" height="10" rx="2" fill="#f0883e"/>';
  svg+='<text x="'+(ml+58)+'" y="'+(H-2)+'" style="font-size:10px">Max</text>';

  container.innerHTML='<svg class="chart" viewBox="0 0 '+W+' '+H+'" width="100%" height="'+H+'">'+svg+'</svg>';
}

/* -- Main render -- */
function render(data){
  if(!data)return;
  var m=data.metrics;
  renderPipeline(m);
  renderMetrics(m);
  renderCharts(data.timeline||[]);
  renderEventStream(data.recent_events||[]);
  renderAlerts(data.alerts||[]);
  renderDevices(data.devices||[]);
}

/* -- Pause/Resume -- */
var paused=false;
window.togglePause=function(){
  paused=!paused;
  var btn=document.getElementById('pauseBtn');
  if(paused){
    btn.innerHTML='&#x25B6; Resume';
    btn.style.borderColor='#d29922';
    btn.style.color='#d29922';
    document.getElementById('statusText').textContent='PAUSED';
    document.getElementById('statusText').style.color='#d29922';
    document.getElementById('statusDot').style.background='#d29922';
    document.getElementById('statusDot').style.animation='none';
    if(ws&&ws.readyState===1)ws.send('pause');
  }else{
    btn.innerHTML='&#x23F8; Pause';
    btn.style.borderColor='#30363d';
    btn.style.color='#e1e4e8';
    document.getElementById('statusText').textContent='LIVE';
    document.getElementById('statusText').style.color='#3fb950';
    document.getElementById('statusDot').style.background='#3fb950';
    document.getElementById('statusDot').style.animation='pulse 1.5s infinite';
    if(ws&&ws.readyState===1)ws.send('resume');
  }
};

/* -- WebSocket -- */
var ws=null;
function connect(){
  var proto=location.protocol==='https:'?'wss:':'ws:';
  ws=new WebSocket(proto+'//'+location.host+'/ws');
  ws.onopen=function(){
    document.getElementById('statusDot').className='status-dot live';
    document.getElementById('statusText').className='status-text live';
    document.getElementById('statusText').textContent='LIVE';
  };
  ws.onmessage=function(e){
    if(paused)return;
    try{
      var data=JSON.parse(e.data);
      render(data);
      document.getElementById('lastUpdate').textContent='Updated: '+new Date().toLocaleTimeString();
    }catch(err){console.error('Parse error',err)}
  };
  ws.onclose=function(){
    document.getElementById('statusDot').className='status-dot off';
    document.getElementById('statusText').className='status-text off';
    document.getElementById('statusText').textContent='DISCONNECTED';
    setTimeout(connect,3000);
  };
  ws.onerror=function(){ws.close()};
}
connect();

})();
</script>
</body>
</html>"""
'''

print("All file contents prepared:")
print(f"  - app.yaml ({len(app_yaml_content)} bytes)")
print(f"  - requirements.txt ({len(requirements_content)} bytes)")
print(f"  - backend/__init__.py ({len(init_content)} bytes)")
print(f"  - backend/main.py ({len(main_py_content)} bytes)")
print(f"  - backend/frontend_html.py ({len(frontend_html_content)} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Upload Files & Deploy App

# COMMAND ----------

# Step 6a — Upload all application files to workspace

def upload_workspace_file(path, content):
    """Upload a file using the workspace import API."""
    parent = "/".join(path.split("/")[:-1])
    requests.post(
        f"{base_url}/api/2.0/workspace/mkdirs",
        headers=headers,
        json={"path": parent},
    )
    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    resp = requests.post(
        f"{base_url}/api/2.0/workspace/import",
        headers=headers,
        json={
            "path": path,
            "format": "AUTO",
            "content": encoded,
            "overwrite": True,
        },
    )
    return resp

files_to_upload = {
    f"{app_base}/app.yaml": app_yaml_content,
    f"{app_base}/requirements.txt": requirements_content,
    f"{app_base}/backend/__init__.py": init_content,
    f"{app_base}/backend/main.py": main_py_content,
    f"{app_base}/backend/frontend_html.py": frontend_html_content,
}

print("Uploading application files...")
for fpath, fcontent in files_to_upload.items():
    resp = upload_workspace_file(fpath, fcontent)
    status = "OK" if resp.status_code in (200, 201) else f"FAILED ({resp.status_code}: {resp.text})"
    print(f"  {fpath} -> {status}")

# COMMAND ----------

# Step 6b — Create the Databricks App and wait for SP
print(f"Creating app '{APP_NAME}'...")

check_resp = requests.get(f"{base_url}/api/2.0/apps/{APP_NAME}", headers=headers)

if check_resp.status_code == 200:
    print(f"App '{APP_NAME}' already exists. Proceeding to deploy...")
    app_info = check_resp.json()
else:
    create_resp = requests.post(
        f"{base_url}/api/2.0/apps",
        headers=headers,
        json={
            "name": APP_NAME,
            "description": "IoT Real-Time Monitor",
        },
    )
    if create_resp.status_code not in (200, 201):
        print(f"Warning: Create app response: {create_resp.status_code} - {create_resp.text}")
    app_info = create_resp.json()
    print(f"App created: {json.dumps(app_info, indent=2)}")

# Wait for Service Principal to be provisioned
sp_client_id = ""
sp_id = ""
print("Waiting for Service Principal to be provisioned...")
for _attempt in range(20):
    fresh = requests.get(f"{base_url}/api/2.0/apps/{APP_NAME}", headers=headers)
    if fresh.status_code == 200:
        app_info = fresh.json()
        sp_client_id = app_info.get("service_principal_client_id", "")
        sp_id = str(app_info.get("service_principal_id", ""))
        if sp_client_id:
            break
    time.sleep(5)

print(f"Service Principal Client ID: {sp_client_id or 'NOT YET (re-run this cell)'}")
print(f"Service Principal ID: {sp_id or 'NOT YET'}")

# COMMAND ----------

# Step 6c — Grant permissions to the app service principal
print("Granting permissions to service principal...")

if sp_client_id:
    # Grant catalog/schema/table permissions
    for grant_sql, desc in [
        (f"GRANT USE CATALOG ON CATALOG `{CATALOG}` TO `{sp_client_id}`", f"USE CATALOG on {CATALOG}"),
        (f"GRANT USE SCHEMA ON SCHEMA `{CATALOG}`.`{SCHEMA}` TO `{sp_client_id}`", f"USE SCHEMA on {CATALOG}.{SCHEMA}"),
        (f"GRANT SELECT ON TABLE `{CATALOG}`.`{SCHEMA}`.iot_events_raw TO `{sp_client_id}`", f"SELECT on table"),
        (f"GRANT MODIFY ON TABLE `{CATALOG}`.`{SCHEMA}`.iot_events_raw TO `{sp_client_id}`", f"MODIFY on table"),
    ]:
        try:
            spark.sql(grant_sql)
            print(f"  Granted {desc}")
        except Exception as e:
            print(f"  {desc}: {e}")

    # Grant warehouse permission — use PUT (not PATCH)
    try:
        perm_resp = requests.put(
            f"{base_url}/api/2.0/permissions/sql/warehouses/{WAREHOUSE_ID}",
            headers=headers,
            json={
                "access_control_list": [
                    {
                        "service_principal_name": sp_client_id,
                        "permission_level": "CAN_USE",
                    }
                ]
            },
        )
        if perm_resp.status_code == 200:
            print(f"  Granted CAN_USE on warehouse {WAREHOUSE_ID}")
        else:
            print(f"  Warehouse permission response: {perm_resp.status_code} - {perm_resp.text}")
    except Exception as e:
        print(f"  Warehouse permission: {e}")
else:
    print("WARNING: No service principal found. Re-run this cell after Step 6b completes.")

# COMMAND ----------

# Step 6d — Deploy the app (with retry)
# Wait for app to be ready before deploying
print("Waiting for app to be ready for deployment...")
time.sleep(15)
print(f"Deploying app '{APP_NAME}'...")

deploy_payload = {
    "source_code_path": f"/Workspace{app_base}",
}

# Retry deploy up to 3 times (app may not be ready immediately after creation)
deployment_id = "unknown"
for attempt in range(3):
    deploy_resp = requests.post(
        f"{base_url}/api/2.0/apps/{APP_NAME}/deployments",
        headers=headers,
        json=deploy_payload,
    )
    if deploy_resp.status_code in (200, 201):
        deploy_info = deploy_resp.json()
        deployment_id = deploy_info.get("deployment_id", "unknown")
        print(f"Deployment started: {deployment_id}")
        break
    else:
        print(f"  Attempt {attempt+1}: {deploy_resp.status_code} - {deploy_resp.text[:100]}")
        if attempt < 2:
            print(f"  Retrying in 15s...")
            time.sleep(15)
else:
    print(f"Deployment failed after 3 attempts. Try deploying manually from the Apps page.")

# Wait for deployment
print("Waiting for deployment to complete...")
app_url = ""

for i in range(30):
    time.sleep(10)
    status_resp = requests.get(f"{base_url}/api/2.0/apps/{APP_NAME}", headers=headers)
    if status_resp.status_code == 200:
        app_data = status_resp.json()
        app_status = app_data.get("status", {}).get("state", app_data.get("compute_status", {}).get("state", "UNKNOWN"))
        print(f"  [{i+1}/30] Status: {app_status}")
        if app_status in ("RUNNING", "ACTIVE", "DEPLOYED"):
            app_url = app_data.get("url", "")
            print(f"\nApp is RUNNING!")
            print(f"URL: {app_url}")
            break
    else:
        print(f"  [{i+1}/30] Status check failed: {status_resp.status_code}")
else:
    print(f"\nDeployment may still be in progress. Check the Apps page in Databricks.")

if not app_url:
    app_url = f"(check Databricks Apps page for {APP_NAME})"

print(f"\nApp URL: {app_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Generate Simulator Credentials

# COMMAND ----------

# Step 7 — Create OAuth secret for the app's SP (account-level, supports OAuth)
#
# The app's own SP is account-level and supports OAuth authentication.
# We create a new secret and grant MODIFY so the simulator can INSERT.

SP_CLIENT_ID = sp_client_id
SP_CLIENT_SECRET = ""

print(f"Using app Service Principal: {SP_CLIENT_ID}")
print(f"SP ID: {sp_id}")

if sp_id:
    print(f"\nCreating OAuth secret...")
    try:
        secret_resp = requests.post(
            f"{base_url}/api/2.0/accounts/servicePrincipals/{sp_id}/credentials/secrets",
            headers=headers,
        )
        if secret_resp.status_code in (200, 201):
            secret_data = secret_resp.json()
            SP_CLIENT_SECRET = secret_data.get("secret", "")
            if SP_CLIENT_SECRET:
                print(f"  OAuth secret created!")
            else:
                print(f"  ERROR: No secret in response: {secret_data}")
        else:
            print(f"  ERROR: {secret_resp.status_code} - {secret_resp.text}")
    except Exception as e:
        print(f"  ERROR: {e}")
else:
    print("ERROR: No SP ID. Make sure Step 6b completed successfully.")

# Test OAuth
if SP_CLIENT_ID and SP_CLIENT_SECRET:
    print(f"\nTesting OAuth authentication...")
    try:
        test_resp = requests.post(f"{base_url}/oidc/v1/token", data={
            "grant_type": "client_credentials",
            "client_id": SP_CLIENT_ID,
            "client_secret": SP_CLIENT_SECRET,
            "scope": "all-apis"})
        if test_resp.status_code == 200:
            print(f"  OAuth: OK")
        else:
            print(f"  OAuth: FAILED - {test_resp.text}")
    except Exception as e:
        print(f"  OAuth test error: {e}")

# Display credentials in a box
print(f"\n  +{'='*58}+")
print(f"  |  COPY THESE FOR LOCAL SETUP (paste into setup_local.py)  |")
print(f"  +{'='*58}+")
print(f"  |  Client ID:     {SP_CLIENT_ID:<40}|")
print(f"  |  Client Secret: {SP_CLIENT_SECRET:<40}|")
print(f"  +{'='*58}+")
print(f"\n  (credentials will be cleared when you run the next cell)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Clear Credentials & Summary

# COMMAND ----------

# Step 8 — Clear credentials and print summary

# Clear sensitive widget values
try:
    dbutils.widgets.remove("catalog")
    dbutils.widgets.remove("schema")
    dbutils.widgets.remove("app_name")
except Exception:
    pass

print("=" * 70)
print("  ZEROBUS IoT REAL-TIME — SETUP COMPLETE")
print("=" * 70)
print(f"""
CONFIGURATION
  Catalog:          {CATALOG}
  Schema:           {SCHEMA}
  Table:            {CATALOG}.{SCHEMA}.iot_events_raw
  SQL Warehouse:    {WAREHOUSE_NAME} ({WAREHOUSE_ID})
  App Name:         {APP_NAME}
  App URL:          {app_url}
  Zerobus Endpoint: {zerobus_endpoint}

SERVICE PRINCIPAL (App + Simulator)
  Client ID:        {sp_client_id or 'Check app settings'}
  SP ID:            {sp_id or 'Check app settings'}
  Secret:           {'Created successfully (see Step 7 output)' if SP_CLIENT_SECRET else 'FAILED - check errors above'}

NEXT STEPS
  1. On your laptop, run:
     python setup_local.py

  2. Paste the Client ID and Secret from Step 7 when prompted.

  3. Run a simulator:
     python iot_simulator_rest.py    (SQL REST, simple)
     python iot_device_simulator.py  (Zerobus gRPC, fast)

  4. Open the dashboard:
     {app_url}

ARCHITECTURE
  [Simulator] --INSERT--> [Delta Table] <--SQL Poll-- [FastAPI App] --WebSocket--> [Browser]
""")
print("=" * 70)
