-- SilentPulse Metric Views - Deployment Script
-- Run this in a Databricks SQL warehouse (Runtime 17.2+).
--
-- Before running:
--   1. Materialize SilentPulse data into Unity Catalog tables:
--        spark.read.format("silentpulse").option("resource","alerts").load()
--          .write.mode("overwrite").saveAsTable("silentpulse.monitoring.alerts")
--   2. Adjust catalog/schema names below if using different locations.

-- ============================================================================
-- Alert Metrics
-- ============================================================================

CREATE OR REPLACE VIEW silentpulse.monitoring.alert_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Security telemetry visibility metrics from SilentPulse alerts"
source: silentpulse.monitoring.alerts

dimensions:
  - name: Severity
    expr: severity
    comment: "Alert severity level"
  - name: Alert Type
    expr: alert_type
    comment: "Type of visibility issue (silence, degradation)"
  - name: Asset Group
    expr: asset_group_name
    comment: "Logical group of monitored assets"
  - name: Flow Name
    expr: flow_name
    comment: "Data flow pipeline name"
  - name: Alert Day
    expr: DATE_TRUNC('DAY', started_at)
    comment: "Day the alert was created"
  - name: Alert Week
    expr: DATE_TRUNC('WEEK', started_at)
    comment: "Week the alert was created"

measures:
  - name: Active Alerts
    expr: COUNT(CASE WHEN status = 'active' THEN 1 END)
    comment: "Number of currently active alerts"
  - name: Silent Feeds
    expr: COUNT(CASE WHEN status = 'active' AND alert_type = 'silence' THEN 1 END)
    comment: "Number of security feeds that have gone silent"
  - name: Degraded Feeds
    expr: COUNT(CASE WHEN status = 'active' AND alert_type = 'degradation' THEN 1 END)
    comment: "Number of feeds with degraded telemetry"
  - name: Total Alerts
    expr: COUNT(1)
    comment: "Total alert count (all statuses)"
  - name: Resolved Alerts
    expr: COUNT(CASE WHEN status = 'resolved' THEN 1 END)
    comment: "Number of resolved alerts"
  - name: Mean Time to Detect (min)
    expr: AVG(TIMESTAMPDIFF(MINUTE, started_at, resolved_at))
    comment: "Average minutes from silence start to resolution"
  - name: Blind Spot Hours
    expr: SUM(TIMESTAMPDIFF(HOUR, started_at, COALESCE(resolved_at, current_timestamp())))
    comment: "Cumulative hours of visibility loss across all alerts"
  - name: Max Gap Duration (hours)
    expr: MAX(TIMESTAMPDIFF(HOUR, started_at, COALESCE(resolved_at, current_timestamp())))
    comment: "Longest single visibility gap in hours"
$$;

-- ============================================================================
-- Feed Health Metrics
-- ============================================================================

CREATE OR REPLACE VIEW silentpulse.monitoring.feed_health_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Feed health and pipeline coverage metrics from SilentPulse"
source: silentpulse.monitoring.coverage

dimensions:
  - name: Flow Name
    expr: flow_name
    comment: "Data flow pipeline name"
  - name: Asset Group
    expr: asset_group_name
    comment: "Logical group of monitored assets"
  - name: Flow Enabled
    expr: CASE WHEN flow_enabled THEN 'Enabled' ELSE 'Disabled' END
    comment: "Whether the flow is currently enabled"

measures:
  - name: Total Assets
    expr: SUM(total_assets)
    comment: "Total number of assets across flows"
  - name: Healthy Flows
    expr: COUNT(CASE WHEN flow_enabled THEN 1 END)
    comment: "Number of enabled/healthy flows"
  - name: Disabled Flows
    expr: COUNT(CASE WHEN NOT flow_enabled THEN 1 END)
    comment: "Number of disabled flows"
  - name: Total Flows
    expr: COUNT(1)
    comment: "Total number of configured flows"
  - name: Coverage Pct
    expr: COUNT(CASE WHEN flow_enabled THEN 1 END) * 100.0 / NULLIF(COUNT(1), 0)
    comment: "Percentage of flows that are enabled and reporting"
$$;

-- ============================================================================
-- Example Queries
-- ============================================================================

-- Active alerts by severity
-- SELECT `Severity`, MEASURE(`Active Alerts`), MEASURE(`Silent Feeds`)
-- FROM silentpulse.monitoring.alert_metrics
-- GROUP BY ALL
-- ORDER BY MEASURE(`Active Alerts`) DESC;

-- Weekly blind spot trend
-- SELECT `Alert Week`, MEASURE(`Blind Spot Hours`), MEASURE(`Active Alerts`)
-- FROM silentpulse.monitoring.alert_metrics
-- GROUP BY ALL
-- ORDER BY `Alert Week`;

-- Feed health overview
-- SELECT `Flow Name`, `Asset Group`,
--        MEASURE(`Total Assets`), MEASURE(`Coverage Pct`)
-- FROM silentpulse.monitoring.feed_health_metrics
-- GROUP BY ALL;
