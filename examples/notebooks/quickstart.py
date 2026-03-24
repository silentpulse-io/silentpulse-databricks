# Databricks notebook source
# MAGIC %md
# MAGIC # SilentPulse SDP Quickstart
# MAGIC
# MAGIC This notebook demonstrates how to add SilentPulse observability
# MAGIC decorators to a Spark Declarative Pipeline (SDP).
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - SilentPulse instance with API access
# MAGIC - `silentpulse-sdp` package installed on the cluster

# COMMAND ----------

# MAGIC %pip install silentpulse-sdp

# COMMAND ----------

import dlt
from silentpulse_sdp import completeness, freshness, heartbeat, volume

# Store your SilentPulse API token in Databricks Secrets:
#   databricks secrets put-secret silentpulse api_token

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Ingest raw security events

# COMMAND ----------


@heartbeat(integration_point="siem-raw-events", interval_seconds=300)
@volume(integration_point="siem-raw-events", min_rows=100)
@dlt.table(comment="Raw security events from SIEM")
def bronze_security_events():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .load("/mnt/security/raw-events/")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Normalized events with completeness checks

# COMMAND ----------


@completeness(
    integration_point="siem-normalized",
    expected_columns=["event_id", "timestamp", "source_ip", "event_type"],
)
@freshness(integration_point="siem-normalized", max_delay_seconds=600)
@dlt.table(comment="Normalized security events")
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
# MAGIC ## Summary
# MAGIC
# MAGIC With these decorators in place, SilentPulse will:
# MAGIC - Alert when heartbeat signals stop (pipeline stalled)
# MAGIC - Alert when data volume drops below expected thresholds
# MAGIC - Alert when columns have unexpected null rates
# MAGIC - Alert when data freshness exceeds acceptable delay
