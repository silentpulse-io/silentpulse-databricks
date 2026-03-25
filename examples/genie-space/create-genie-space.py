# Databricks notebook source
# MAGIC %md
# MAGIC # SilentPulse Genie Space Setup
# MAGIC
# MAGIC Creates a Genie Space that enables natural language querying of
# MAGIC SilentPulse security visibility data.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - SilentPulse metric views deployed (run `deploy-metric-views.sql` first)
# MAGIC - Databricks workspace with AI/BI Genie enabled
# MAGIC - `databricks-sdk` installed on the cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Adjust the warehouse ID and table names to match your environment.

# COMMAND ----------

WAREHOUSE_ID = spark.conf.get("spark.silentpulse.warehouse_id", "<your-warehouse-id>")  # noqa: F821
CATALOG = "silentpulse"
SCHEMA = "monitoring"

# COMMAND ----------

from databricks.sdk import WorkspaceClient  # noqa: E402

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Genie Space

# COMMAND ----------

space = w.genie.create_space(
    title="SilentPulse Security Visibility",
    description=(
        "Ask questions about security telemetry visibility, alert trends, "
        "feed health, and detection coverage monitored by SilentPulse."
    ),
    warehouse_id=WAREHOUSE_ID,
    table_identifiers=[
        f"{CATALOG}.{SCHEMA}.alerts",
        f"{CATALOG}.{SCHEMA}.flows",
        f"{CATALOG}.{SCHEMA}.coverage",
        f"{CATALOG}.{SCHEMA}.alert_metrics",
        f"{CATALOG}.{SCHEMA}.feed_health_metrics",
    ],
    sample_questions=[
        "Which security feeds went silent in the last 24 hours?",
        "What is our current detection coverage percentage?",
        "Show me the longest visibility gap this month",
        "How many active alerts are there by severity?",
        "What is the mean time to detect silence this week?",
        "Which asset groups have the most blind spot hours?",
        "Show me the trend of silent feeds over the last 30 days",
        "Which flows are currently disabled?",
        "What percentage of our flows are healthy right now?",
        "List all high-severity alerts that are still unresolved",
    ],
)

print(f"Genie Space created: {space.space_id}")
print(f"URL: {w.config.host}/genie/spaces/{space.space_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Queries
# MAGIC
# MAGIC After creating the Genie Space, users can ask questions like:
# MAGIC
# MAGIC | Question | What it answers |
# MAGIC |----------|----------------|
# MAGIC | "Which security feeds went silent in the last 24 hours?" | Recent silence alerts with asset/flow details |
# MAGIC | "What is our current detection coverage percentage?" | Feed health coverage ratio |
# MAGIC | "Show me the longest visibility gap this month" | Max gap duration from alert metrics |
# MAGIC | "How many active alerts are there by severity?" | Alert breakdown by severity level |
# MAGIC | "What is the mean time to detect silence this week?" | MTTD metric filtered to current week |
# MAGIC | "Which asset groups have the most blind spot hours?" | Cumulative visibility loss per group |
# MAGIC | "Show me the trend of silent feeds over the last 30 days" | Time series of silent feed count |
# MAGIC | "Which flows are currently disabled?" | Disabled flows from coverage data |
# MAGIC | "What percentage of our flows are healthy right now?" | Coverage percentage metric |
# MAGIC | "List all high-severity alerts that are still unresolved" | Active high-severity alerts |
