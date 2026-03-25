# Databricks notebook source
# MAGIC %md
# MAGIC # SilentPulse AI Enrichment Pipeline
# MAGIC
# MAGIC A Spark Declarative Pipeline (SDP) that reads SilentPulse alerts,
# MAGIC enriches them with AI-powered root cause classification, and
# MAGIC materializes the results for dashboards and Genie queries.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - `silentpulse-sdp` and `silentpulse-datasource` installed
# MAGIC - SilentPulse API credentials in Databricks Secrets
# MAGIC - DBR 15.4 ML LTS or newer
# MAGIC - Foundation model endpoint available

# COMMAND ----------

# MAGIC %pip install silentpulse-sdp silentpulse-datasource

# COMMAND ----------

import dlt  # noqa: F811
from pyspark.sql.functions import col, concat, concat_ws, expr, lit  # noqa: F811

import silentpulse_sdp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

SILENTPULSE_URL = spark.conf.get("spark.silentpulse.url")  # noqa: F821
SILENTPULSE_TOKEN = spark.conf.get("spark.silentpulse.token")  # noqa: F821

silentpulse_sdp.configure(api_url=SILENTPULSE_URL, api_token=SILENTPULSE_TOKEN)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Raw Alerts from SilentPulse API

# COMMAND ----------

from silentpulse_datasource import SilentPulseDataSource  # noqa: E402
from silentpulse_sdp import heartbeat, volume  # noqa: E402

spark.dataSource.register(SilentPulseDataSource)  # noqa: F821


@dlt.table(comment="Raw SilentPulse alerts ingested from the API")
@heartbeat(integration_point="silentpulse-alerts-ingest", interval_seconds=600)
@volume(integration_point="silentpulse-alerts-ingest", min_rows=0)
def bronze_alerts():
    return (
        spark.read.format("silentpulse")  # noqa: F821
        .option("url", SILENTPULSE_URL)
        .option("token", SILENTPULSE_TOKEN)
        .option("resource", "alerts")
        .load()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: AI-Enriched Alerts
# MAGIC
# MAGIC Enrich each alert with:
# MAGIC - **probable_cause** - AI-classified root cause category
# MAGIC - **affected_component** - extracted component name
# MAGIC - **recommended_action** - suggested remediation step

# COMMAND ----------

from silentpulse_sdp import freshness  # noqa: E402


@dlt.table(comment="Alerts enriched with AI-classified root causes")
@freshness(integration_point="silentpulse-alerts-enriched", max_delay_seconds=3600)
def silver_alerts_enriched():
    df = dlt.read("bronze_alerts")

    alert_text = concat(
        lit('Security feed "'),
        col("flow_name"),
        lit('" for asset group "'),
        col("asset_group_name"),
        lit('" alert since '),
        col("started_at").cast("string"),
        lit(". Type: "),
        col("alert_type"),
        lit(". Severity: "),
        col("severity"),
        lit("."),
    )

    return (
        df.withColumn(
            "probable_cause",
            expr(
                "ai_classify("
                "alert_text, "
                "array('network_outage', 'credential_rotation', 'source_decommission', "
                "'pipeline_failure', 'rate_limiting', 'maintenance_window'))"
            ).alias("probable_cause"),
        )
        .withColumn(
            "ai_insights",
            expr(
                "ai_extract("
                "alert_text, "
                "array('affected_component', 'recommended_action'))"
            ),
        )
        .withColumn("alert_text", alert_text)
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Aggregated Root Cause Summary
# MAGIC
# MAGIC Materialized view of root cause distribution for dashboards.

# COMMAND ----------


@dlt.table(comment="Root cause distribution for active alerts")
def gold_root_cause_summary():
    return (
        dlt.read("silver_alerts_enriched")
        .filter(col("status") == "active")
        .groupBy("probable_cause", "severity", "asset_group_name")
        .agg(
            expr("COUNT(*) AS alert_count"),
            expr("MIN(started_at) AS earliest_alert"),
            expr("MAX(started_at) AS latest_alert"),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Executive Summary (refreshed per pipeline run)

# COMMAND ----------


@dlt.table(comment="AI-generated executive summary of current alert landscape")
def gold_executive_summary():
    df = dlt.read("silver_alerts_enriched").filter(col("status") == "active")

    summary_input = df.select(
        concat_ws(
            "\n",
            concat(lit("Total active alerts: "), expr("COUNT(*)")),
            concat(
                lit("By cause: "), expr("CONCAT_WS(', ', COLLECT_SET(probable_cause))")
            ),
            concat(
                lit("By severity: high="),
                expr("COUNT(CASE WHEN severity='high' THEN 1 END)"),
            ),
            concat(
                lit("Affected groups: "),
                expr("CONCAT_WS(', ', COLLECT_SET(asset_group_name))"),
            ),
        ).alias("context")
    )

    return summary_input.withColumn(
        "executive_summary",
        expr("ai_summarize(context, 50)"),
    )
