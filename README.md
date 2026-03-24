# silentpulse-databricks

[![CI](https://github.com/silentpulse-io/silentpulse-databricks/actions/workflows/ci.yml/badge.svg)](https://github.com/silentpulse-io/silentpulse-databricks/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Databricks integrations for [SilentPulse](https://silentpulse.io) - see when security goes silent.

## Architecture

```
Databricks Workspace                          SilentPulse
+-------------------------------+             +------------------+
|                               |             |                  |
|  SDP Pipeline                 |  heartbeat  |  API Gateway     |
|  +-------------------------+  |  -------->  |  +-----------+   |
|  | @heartbeat              |  |  volume     |  | Ingest    |   |
|  | @volume                 |  |  freshness  |  +-----------+   |
|  | @completeness           |  |  -------->  |        |         |
|  | @freshness              |  |             |  +-----------+   |
|  +-------------------------+  |             |  | Scheduler |   |
|                               |             |  +-----------+   |
|  Unity Catalog                |  discovery  |        |         |
|  +-------------------------+  |  -------->  |  +-----------+   |
|  | Catalogs / Schemas      |  |             |  | Alerts    |   |
|  | Tables / Lineage        |  |             |  +-----------+   |
|  +-------------------------+  |             |                  |
|                               |  read       |                  |
|  DataSource                   |  <--------  |                  |
|  +-------------------------+  |             |                  |
|  | spark.read               |  |             |                  |
|  |   .format("silentpulse") |  |             |                  |
|  +-------------------------+  |             |                  |
+-------------------------------+             +------------------+
```

## Packages

| Package | Description |
|---------|-------------|
| [silentpulse-sdp](silentpulse-sdp/) | Observability decorators for Spark Declarative Pipelines (SDP). Add `@heartbeat`, `@volume`, `@completeness`, and `@freshness` to pipeline tables. |
| [silentpulse-datasource](silentpulse-datasource/) | PySpark DataSource to read SilentPulse visibility data (alerts, flow status, asset coverage) as Spark DataFrames. |
| [silentpulse-uc-discovery](silentpulse-uc-discovery/) | Auto-discover Unity Catalog assets (pipelines, tables, lineage) and register them in SilentPulse CMDB. |

## Quickstart

### Install

```bash
pip install silentpulse-sdp
```

### Add observability to an SDP pipeline

```python
import dlt
from silentpulse_sdp import heartbeat, volume

@heartbeat(integration_point="siem-raw-events", interval_seconds=300)
@volume(integration_point="siem-raw-events", min_rows=100)
@dlt.table(comment="Raw security events from SIEM")
def bronze_security_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/mnt/security/raw-events/")
    )
```

SilentPulse will alert when heartbeat signals stop arriving or data volume drops below the expected threshold.

See [examples/notebooks/quickstart.py](examples/notebooks/quickstart.py) for a complete pipeline example.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

```bash
uv venv .venv && source .venv/bin/activate
uv pip install -e ./silentpulse-sdp[dev] -e ./silentpulse-datasource[dev] -e ./silentpulse-uc-discovery[dev]
```

## Links

- [SilentPulse](https://silentpulse.io) - Main project
- [SilentPulse Documentation](https://docs.silentpulse.io) - Full docs
- [Deploy Repository](https://github.com/silentpulse-io/deploy) - Helm charts, Docker Compose

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
