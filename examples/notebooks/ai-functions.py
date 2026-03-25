# Databricks notebook source
# MAGIC %md
# MAGIC # SilentPulse AI Functions for Root Cause Analysis
# MAGIC
# MAGIC Uses Databricks AI Functions to classify, summarize, and extract
# MAGIC insights from SilentPulse alert data - no model endpoint setup needed.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - SilentPulse data materialized in Unity Catalog (via `silentpulse-datasource`)
# MAGIC - DBR 15.4 ML LTS or newer
# MAGIC - Foundation model endpoint available in your workspace
# MAGIC
# MAGIC ## AI Functions Used
# MAGIC | Function | Purpose |
# MAGIC |----------|---------|
# MAGIC | `ai_classify` | Classify probable root cause of silence |
# MAGIC | `ai_summarize` | Executive summary of alert timeline |
# MAGIC | `ai_extract` | Extract affected components and impact |
# MAGIC | `ai_query` | Complex analysis with custom prompts |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Classify Root Cause with `ai_classify`
# MAGIC
# MAGIC Automatically classify the probable cause of each silence alert
# MAGIC into one of six operational categories.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     id,
# MAGIC     flow_name,
# MAGIC     asset_group_name,
# MAGIC     severity,
# MAGIC     started_at,
# MAGIC     ai_classify(
# MAGIC         CONCAT(
# MAGIC             'Security feed "', flow_name, '" for asset group "', asset_group_name,
# MAGIC             '" went silent at ', CAST(started_at AS STRING),
# MAGIC             '. Alert type: ', alert_type, '. Severity: ', severity, '.'
# MAGIC         ),
# MAGIC         ARRAY(
# MAGIC             'network_outage',
# MAGIC             'credential_rotation',
# MAGIC             'source_decommission',
# MAGIC             'pipeline_failure',
# MAGIC             'rate_limiting',
# MAGIC             'maintenance_window'
# MAGIC         )
# MAGIC     ) AS probable_cause
# MAGIC FROM silentpulse.monitoring.alerts
# MAGIC WHERE status = 'active'
# MAGIC ORDER BY started_at DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Executive Summary with `ai_summarize`
# MAGIC
# MAGIC Generate a concise executive summary of the current alert landscape
# MAGIC for security leadership briefings.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH alert_context AS (
# MAGIC     SELECT CONCAT_WS('\n',
# MAGIC         CONCAT('Total active alerts: ', COUNT(*)),
# MAGIC         CONCAT('High severity: ', COUNT(CASE WHEN severity = 'high' THEN 1 END)),
# MAGIC         CONCAT('Medium severity: ', COUNT(CASE WHEN severity = 'medium' THEN 1 END)),
# MAGIC         CONCAT('Low severity: ', COUNT(CASE WHEN severity = 'low' THEN 1 END)),
# MAGIC         CONCAT('Affected asset groups: ',
# MAGIC             ARRAY_JOIN(COLLECT_SET(asset_group_name), ', ')),
# MAGIC         CONCAT('Affected flows: ',
# MAGIC             ARRAY_JOIN(COLLECT_SET(flow_name), ', ')),
# MAGIC         CONCAT('Oldest unresolved alert: ',
# MAGIC             CAST(MIN(started_at) AS STRING)),
# MAGIC         CONCAT('Alert types: ',
# MAGIC             ARRAY_JOIN(COLLECT_SET(alert_type), ', '))
# MAGIC     ) AS context
# MAGIC     FROM silentpulse.monitoring.alerts
# MAGIC     WHERE status = 'active'
# MAGIC )
# MAGIC SELECT ai_summarize(context, 50) AS executive_summary
# MAGIC FROM alert_context

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extract Structured Data with `ai_extract`
# MAGIC
# MAGIC Extract affected components, estimated impact duration, and
# MAGIC recommended actions from alert descriptions.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     id,
# MAGIC     flow_name,
# MAGIC     ai_extract(
# MAGIC         CONCAT(
# MAGIC             'Alert ID ', id, ': Security feed "', flow_name,
# MAGIC             '" for asset group "', asset_group_name,
# MAGIC             '" has been in "', status, '" state since ',
# MAGIC             CAST(started_at AS STRING),
# MAGIC             CASE WHEN resolved_at IS NOT NULL
# MAGIC                 THEN CONCAT(', resolved at ', CAST(resolved_at AS STRING))
# MAGIC                 ELSE ', still unresolved'
# MAGIC             END,
# MAGIC             '. Severity: ', severity,
# MAGIC             '. Alert type: ', alert_type, '.'
# MAGIC         ),
# MAGIC         ARRAY(
# MAGIC             'affected_component',
# MAGIC             'impact_duration_hours',
# MAGIC             'detection_gap',
# MAGIC             'recommended_action'
# MAGIC         )
# MAGIC     ) AS extracted
# MAGIC FROM silentpulse.monitoring.alerts
# MAGIC WHERE status = 'active'
# MAGIC ORDER BY severity DESC, started_at

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Custom Analysis with `ai_query`
# MAGIC
# MAGIC For complex analysis that goes beyond the task-specific functions,
# MAGIC use `ai_query` with custom prompts. Here we ask the model to
# MAGIC perform risk assessment and suggest remediation priorities.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH alert_summary AS (
# MAGIC     SELECT CONCAT_WS('\n',
# MAGIC         'Current SilentPulse visibility status:',
# MAGIC         CONCAT('- Active silence alerts: ', COUNT(CASE WHEN alert_type = 'silence' AND status = 'active' THEN 1 END)),
# MAGIC         CONCAT('- Active degradation alerts: ', COUNT(CASE WHEN alert_type = 'degradation' AND status = 'active' THEN 1 END)),
# MAGIC         CONCAT('- High severity count: ', COUNT(CASE WHEN severity = 'high' AND status = 'active' THEN 1 END)),
# MAGIC         CONCAT('- Longest gap: ', MAX(TIMESTAMPDIFF(HOUR, started_at, COALESCE(resolved_at, current_timestamp()))), ' hours'),
# MAGIC         CONCAT('- Affected groups: ', ARRAY_JOIN(COLLECT_SET(
# MAGIC             CASE WHEN status = 'active' THEN asset_group_name END), ', ')),
# MAGIC         CONCAT('- Affected flows: ', ARRAY_JOIN(COLLECT_SET(
# MAGIC             CASE WHEN status = 'active' THEN flow_name END), ', '))
# MAGIC     ) AS context
# MAGIC     FROM silentpulse.monitoring.alerts
# MAGIC )
# MAGIC SELECT ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     CONCAT(
# MAGIC         'You are a security operations analyst. Based on the following ',
# MAGIC         'SilentPulse visibility data, provide:\n',
# MAGIC         '1. Risk assessment (critical/high/medium/low)\n',
# MAGIC         '2. Top 3 remediation priorities\n',
# MAGIC         '3. Potential attack techniques that could exploit these blind spots\n\n',
# MAGIC         context
# MAGIC     )
# MAGIC ).response AS risk_assessment
# MAGIC FROM alert_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Batch Enrichment - Classify All Active Alerts
# MAGIC
# MAGIC Create an enriched table with AI-classified root causes for
# MAGIC all active alerts. This can feed into dashboards and reports.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silentpulse.monitoring.alerts_enriched AS
# MAGIC SELECT
# MAGIC     a.*,
# MAGIC     ai_classify(
# MAGIC         CONCAT(
# MAGIC             'Security feed "', flow_name, '" for asset group "', asset_group_name,
# MAGIC             '" went silent at ', CAST(started_at AS STRING),
# MAGIC             '. Alert type: ', alert_type, '. Severity: ', severity, '.'
# MAGIC         ),
# MAGIC         ARRAY(
# MAGIC             'network_outage',
# MAGIC             'credential_rotation',
# MAGIC             'source_decommission',
# MAGIC             'pipeline_failure',
# MAGIC             'rate_limiting',
# MAGIC             'maintenance_window'
# MAGIC         )
# MAGIC     ) AS probable_cause,
# MAGIC     ai_extract(
# MAGIC         CONCAT(
# MAGIC             'Alert: "', flow_name, '" for "', asset_group_name,
# MAGIC             '" since ', CAST(started_at AS STRING),
# MAGIC             '. Type: ', alert_type, '. Severity: ', severity, '.'
# MAGIC         ),
# MAGIC         ARRAY('affected_component', 'recommended_action')
# MAGIC     ) AS ai_insights
# MAGIC FROM silentpulse.monitoring.alerts a
# MAGIC WHERE status = 'active'
