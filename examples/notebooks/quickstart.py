# Databricks notebook source
# MAGIC %md
# MAGIC # SilentPulse SDP Quickstart
# MAGIC
# MAGIC This notebook demonstrates how to add SilentPulse observability
# MAGIC decorators to a Spark Declarative Pipeline (SDP).
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - SilentPulse instance with API access configured via Databricks Secrets
# MAGIC - `silentpulse-sdp` package installed on the cluster
# MAGIC - Databricks Runtime 14.2+ (for SDP / Lakeflow Declarative Pipelines)
# MAGIC
# MAGIC ## How it works
# MAGIC SilentPulse decorators wrap your SDP table functions to observe
# MAGIC pipeline behavior (heartbeat, volume, completeness, freshness).
# MAGIC They report telemetry to SilentPulse without modifying your data.
# MAGIC
# MAGIC **Decorator order matters:** SilentPulse decorators go BELOW `@dlt.table`
# MAGIC so they wrap the raw function first, then DLT registers the wrapped version.

# COMMAND ----------

# MAGIC %pip install silentpulse-sdp

# COMMAND ----------

import dlt
from silentpulse_sdp import completeness, freshness, heartbeat, volume

# Store your SilentPulse API token in Databricks Secrets:
#   databricks secrets put-secret silentpulse api_token
#
# Configure the SilentPulse endpoint (once per pipeline):
#   silentpulse_sdp.configure(
#       url=dbutils.secrets.get("silentpulse", "api_url"),
#       token=dbutils.secrets.get("silentpulse", "api_token"),
#   )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Ingest raw security events
# MAGIC
# MAGIC The `@heartbeat` decorator sends periodic signals to SilentPulse
# MAGIC confirming the pipeline is running. If signals stop, SilentPulse alerts.
# MAGIC
# MAGIC The `@volume` decorator tracks row counts per batch and alerts
# MAGIC when volume drops below `min_rows`.

# COMMAND ----------


@dlt.table(comment="Raw security events from SIEM")
@heartbeat(integration_point="siem-raw-events", interval_seconds=300)
@volume(integration_point="siem-raw-events", min_rows=100)
def bronze_security_events():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .load("/mnt/security/raw-events/")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Normalized events with completeness checks
# MAGIC
# MAGIC The `@completeness` decorator checks for unexpected NULL rates
# MAGIC in the specified columns. If NULLs exceed the threshold,
# MAGIC SilentPulse creates a degradation alert.
# MAGIC
# MAGIC The `@freshness` decorator monitors data lag. If the newest
# MAGIC event timestamp is older than `max_delay_seconds`, it alerts.

# COMMAND ----------


@dlt.table(comment="Normalized security events")
@completeness(
    integration_point="siem-normalized",
    expected_columns=["event_id", "timestamp", "source_ip", "event_type"],
)
@freshness(integration_point="siem-normalized", max_delay_seconds=600)
def silver_security_events():
    return dlt.read_stream("bronze_security_events").select(
        "event_id",
        "timestamp",
        "source_ip",
        "dest_ip",
        "event_type",
        "severity",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Aggregated visibility metrics
# MAGIC
# MAGIC Even materialized views benefit from freshness monitoring.
# MAGIC If upstream data stops flowing, the gold table becomes stale.

# COMMAND ----------


@dlt.table(comment="Hourly security event counts by type")
@freshness(integration_point="siem-gold-hourly", max_delay_seconds=7200)
def gold_events_hourly():
    return (
        dlt.read("silver_security_events")  # noqa: F821
        .groupBy(
            dlt.col("event_type"),  # noqa: F821
            dlt.expr("date_trunc('hour', timestamp)").alias("hour"),  # noqa: F821
        )
        .count()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC With these decorators in place, SilentPulse will:
# MAGIC - Alert when heartbeat signals stop (pipeline stalled or erroring)
# MAGIC - Alert when data volume drops below expected thresholds
# MAGIC - Alert when columns have unexpected NULL rates (data quality degradation)
# MAGIC - Alert when data freshness exceeds acceptable delay (upstream issues)
# MAGIC
# MAGIC ## Decorator reference
# MAGIC
# MAGIC | Decorator | What it monitors | Key parameters |
# MAGIC |-----------|-----------------|----------------|
# MAGIC | `@heartbeat` | Pipeline liveness | `interval_seconds` (default: 300) |
# MAGIC | `@volume` | Row count per batch | `min_rows`, `max_rows` |
# MAGIC | `@completeness` | NULL rates in columns | `expected_columns` |
# MAGIC | `@freshness` | Data lag / staleness | `max_delay_seconds` (default: 3600) |
# MAGIC
# MAGIC ## Important: Decorator order
# MAGIC
# MAGIC Always place `@dlt.table` on top (outermost). SilentPulse decorators
# MAGIC go below it so they wrap the raw function before DLT registration:
# MAGIC
# MAGIC ```
# MAGIC @dlt.table(...)        # <-- outermost: registers with DLT
# MAGIC @heartbeat(...)        # <-- wraps function for observability
# MAGIC @volume(...)           # <-- wraps function for observability
# MAGIC def my_table():
# MAGIC     ...
# MAGIC ```
