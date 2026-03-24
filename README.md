# silentpulse-databricks

[![CI](https://github.com/silentpulse-io/silentpulse-databricks/actions/workflows/ci.yml/badge.svg)](https://github.com/silentpulse-io/silentpulse-databricks/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Databricks integrations for [SilentPulse](https://silentpulse.io) - see when security goes silent.

Monitor your Databricks security data pipelines for silence, degradation, and visibility gaps. When telemetry stops flowing through your SDP pipelines, SilentPulse detects it and alerts your team before attackers exploit the blind spot.

## Architecture

```
Databricks Workspace                              SilentPulse
+------------------------------------------+      +---------------------+
|                                          |      |                     |
|  Spark Declarative Pipeline (SDP)        |      |  REST API           |
|  +------------------------------------+  |      |  +--------------+   |
|  |                                    |  |      |  |              |   |
|  |  @dlt.table(...)                   |  |      |  |  Ingest      |   |
|  |  @heartbeat(ip="siem-raw", 300s)   |  | ---> |  |  (webhook)   |   |
|  |  @volume(ip="siem-raw", min=100)   |  |      |  +--------------+   |
|  |  def bronze_events():              |  |      |        |            |
|  |      ...                           |  |      |  +--------------+   |
|  |                                    |  |      |  |              |   |
|  |  @dlt.table(...)                   |  |      |  |  Scheduler   |   |
|  |  @completeness(ip="siem-norm")     |  | ---> |  |  (evaluate)  |   |
|  |  @freshness(ip="siem-norm", 600s)  |  |      |  +--------------+   |
|  |  def silver_events():              |  |      |        |            |
|  |      ...                           |  |      |  +--------------+   |
|  +------------------------------------+  |      |  |              |   |
|                                          |      |  |  Alerts      |   |
|  Unity Catalog Discovery                 |      |  |  (notify)    |   |
|  +------------------------------------+  |      |  +--------------+   |
|  |  UCDiscovery.discover_pipelines()  |  |      |        |            |
|  |  UCDiscovery.discover_tables()     |  | ---> |  +--------------+   |
|  |  UCDiscovery.sync_to_silentpulse() |  |      |  |  CMDB        |   |
|  +------------------------------------+  |      |  |  (assets)    |   |
|                                          |      |  +--------------+   |
|  PySpark DataSource                      |      |                     |
|  +------------------------------------+  |      |                     |
|  |  spark.read                        |  |      |                     |
|  |    .format("silentpulse")          |  | <--- |  (read alerts,      |
|  |    .option("resource", "alerts")   |  |      |   flows, coverage)  |
|  |    .load()                         |  |      |                     |
|  +------------------------------------+  |      |                     |
+------------------------------------------+      +---------------------+
```

## Packages

This monorepo contains three independent Python packages:

| Package | PyPI name | Description |
|---------|-----------|-------------|
| [silentpulse-sdp](silentpulse-sdp/) | `silentpulse-sdp` | Observability decorators for Spark Declarative Pipelines. Wrap your `@dlt.table` functions with `@heartbeat`, `@volume`, `@completeness`, and `@freshness` to report telemetry to SilentPulse. |
| [silentpulse-datasource](silentpulse-datasource/) | `silentpulse-datasource` | PySpark DataSource that reads SilentPulse data (alerts, flow status, asset coverage) as Spark DataFrames. Use for dashboards and cross-referencing visibility gaps with your security data. |
| [silentpulse-uc-discovery](silentpulse-uc-discovery/) | `silentpulse-uc-discovery` | Auto-discovers Unity Catalog assets (pipelines, tables, lineage) and registers them in SilentPulse CMDB. Keeps your asset inventory in sync without manual configuration. |

### Package dependencies

```
silentpulse-sdp          -> requests (runtime: report telemetry to SilentPulse API)
silentpulse-datasource   -> requests (runtime: fetch data from SilentPulse API)
                            pyspark  (optional: type hints only, provided by Databricks Runtime)
silentpulse-uc-discovery -> requests, databricks-sdk (runtime: UC metadata + CMDB sync)
```

### Compatibility

| Package | Python | Databricks Runtime | Spark |
|---------|--------|--------------------|-------|
| silentpulse-sdp | 3.9+ | 14.2+ (SDP support) | 3.5+ |
| silentpulse-datasource | 3.9+ | 14.2+ (DataSource API v2) | 3.5+ |
| silentpulse-uc-discovery | 3.9+ | Any (uses SDK, not Spark) | N/A |

## Quickstart

### 1. Install

```bash
# On a Databricks cluster or in a notebook:
%pip install silentpulse-sdp
```

### 2. Add observability to an SDP pipeline

```python
import dlt
from silentpulse_sdp import heartbeat, volume, completeness, freshness

# SilentPulse decorators go BELOW @dlt.table (see "Decorator order" below)
@dlt.table(comment="Raw security events from SIEM")
@heartbeat(integration_point="siem-raw-events", interval_seconds=300)
@volume(integration_point="siem-raw-events", min_rows=100)
def bronze_security_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/mnt/security/raw-events/")
    )
```

SilentPulse will alert when:
- Heartbeat signals stop arriving (pipeline stalled or erroring)
- Data volume drops below 100 rows per batch (source went silent)

### 3. Discover Unity Catalog assets

```python
from silentpulse_uc_discovery import UCDiscovery

discovery = UCDiscovery(
    silentpulse_url="https://silentpulse.example.com/api",
    silentpulse_token=dbutils.secrets.get("silentpulse", "api_token"),
)

# Discover all SDP pipelines in the 'security' catalog
pipelines = discovery.discover_pipelines(catalog="security")

# Push discovered assets to SilentPulse CMDB
result = discovery.sync_to_silentpulse(pipelines)
print(f"Created: {result['created']}, Updated: {result['updated']}")
```

### 4. Read SilentPulse data in Spark

```python
alerts_df = (
    spark.read
    .format("silentpulse")
    .option("url", "https://silentpulse.example.com/api")
    .option("token", dbutils.secrets.get("silentpulse", "api_token"))
    .option("resource", "alerts")
    .load()
)

alerts_df.filter("severity = 'critical'").display()
```

## Decorator reference

| Decorator | What it monitors | Key parameters | Default |
|-----------|-----------------|----------------|---------|
| `@heartbeat` | Pipeline liveness - is this table still being updated? | `integration_point`, `interval_seconds` | 300s |
| `@volume` | Row count per batch - did the data source go silent? | `integration_point`, `min_rows`, `max_rows` | 0, None |
| `@completeness` | NULL rates in columns - is data quality degrading? | `integration_point`, `expected_columns` | None |
| `@freshness` | Data lag - is upstream data stale? | `integration_point`, `max_delay_seconds` | 3600s |

All decorators share the `integration_point` parameter, which maps to the corresponding integration point in your SilentPulse flow configuration.

### Decorator order

SilentPulse decorators must go **below** `@dlt.table`. Decorators are applied bottom-up, so SilentPulse wraps the raw function first, then DLT registers the wrapped version:

```python
@dlt.table(...)        # Outermost: registers with DLT engine
@heartbeat(...)        # Wraps function for observability
@volume(...)           # Wraps function for observability
def my_table():
    ...
```

## Examples

- [examples/notebooks/quickstart.py](examples/notebooks/quickstart.py) - Complete SDP pipeline with all four decorators (bronze/silver/gold)
- [examples/metric-views/security-visibility.yaml](examples/metric-views/security-visibility.yaml) - Unity Catalog Metric View template for security visibility KPIs

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for full development setup and guidelines.

```bash
git clone https://github.com/silentpulse-io/silentpulse-databricks.git
cd silentpulse-databricks
uv venv .venv && source .venv/bin/activate
uv pip install -e ./silentpulse-sdp[dev] -e ./silentpulse-datasource[dev] -e ./silentpulse-uc-discovery[dev]
```

Run linters:

```bash
uvx ruff@0.11.0 check silentpulse-sdp/ silentpulse-datasource/ silentpulse-uc-discovery/ examples/
uvx ruff@0.11.0 format --check silentpulse-sdp/ silentpulse-datasource/ silentpulse-uc-discovery/ examples/
```

## Roadmap

This repository is part of [SilentPulse Milestone #42: Databricks Integration](https://github.com/silentpulse-io/silentpulse-databricks/milestone/1).

| Issue | Package | Status |
|-------|---------|--------|
| #376 | silentpulse-sdp | Implement SDP decorators with telemetry reporting |
| #377 | silentpulse-datasource | Implement PySpark DataSource reader |
| #378 | Backend | Add webhook endpoint for Databricks telemetry |
| #379 | Examples | Unity Catalog Metric View definitions |
| #380 | silentpulse-uc-discovery | Implement UC discovery and CMDB sync |
| #381 | All | Monorepo scaffolding (done) |

## Links

- [SilentPulse](https://silentpulse.io) - Main project
- [SilentPulse Documentation](https://docs.silentpulse.io) - Full docs
- [Deploy Repository](https://github.com/silentpulse-io/deploy) - Helm charts, Docker Compose, Kubernetes

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
