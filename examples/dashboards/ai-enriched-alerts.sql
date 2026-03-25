-- SilentPulse AI-Enriched Alerts Dashboard
-- Create these as datasets in an AI/BI Dashboard (Lakeview).
--
-- Prerequisites:
--   - Run ai-enrichment-pipeline.py SDP pipeline first
--   - Or run the batch enrichment in ai-functions.py notebook
--
-- Recommended dashboard layout:
--   Row 1: [Active Alerts KPI] [Blind Spot Hours KPI] [Coverage % KPI]
--   Row 2: [Root Cause Distribution pie] [Severity Breakdown bar]
--   Row 3: [Alert Timeline by Cause line] [Top Affected Asset Groups table]
--   Row 4: [AI Insights Detail table]

-- ============================================================================
-- Dataset 1: Active Alert KPIs
-- Widget: Counter / KPI cards
-- ============================================================================
SELECT
    COUNT(*) AS active_alerts,
    COUNT(CASE WHEN severity = 'high' THEN 1 END) AS high_severity,
    COUNT(CASE WHEN severity = 'medium' THEN 1 END) AS medium_severity,
    COUNT(DISTINCT asset_group_name) AS affected_groups,
    COUNT(DISTINCT flow_name) AS affected_flows,
    SUM(TIMESTAMPDIFF(HOUR, started_at, current_timestamp())) AS total_blind_spot_hours,
    MAX(TIMESTAMPDIFF(HOUR, started_at, current_timestamp())) AS max_gap_hours
FROM silentpulse.monitoring.alerts_enriched
WHERE status = 'active';

-- ============================================================================
-- Dataset 2: Root Cause Distribution
-- Widget: Pie chart (probable_cause -> alert_count)
-- ============================================================================
SELECT
    probable_cause,
    COUNT(*) AS alert_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM silentpulse.monitoring.alerts_enriched
WHERE status = 'active'
GROUP BY probable_cause
ORDER BY alert_count DESC;

-- ============================================================================
-- Dataset 3: Severity Breakdown by Root Cause
-- Widget: Stacked bar chart (probable_cause x severity)
-- ============================================================================
SELECT
    probable_cause,
    severity,
    COUNT(*) AS alert_count
FROM silentpulse.monitoring.alerts_enriched
WHERE status = 'active'
GROUP BY probable_cause, severity
ORDER BY probable_cause, severity;

-- ============================================================================
-- Dataset 4: Alert Timeline by Root Cause
-- Widget: Line chart (day x probable_cause -> count)
-- ============================================================================
SELECT
    DATE_TRUNC('DAY', started_at) AS alert_day,
    probable_cause,
    COUNT(*) AS alert_count
FROM silentpulse.monitoring.alerts_enriched
GROUP BY alert_day, probable_cause
ORDER BY alert_day;

-- ============================================================================
-- Dataset 5: Top Affected Asset Groups
-- Widget: Table with conditional formatting on alert_count
-- ============================================================================
SELECT
    asset_group_name,
    COUNT(*) AS active_alerts,
    COUNT(CASE WHEN severity = 'high' THEN 1 END) AS high_severity,
    SUM(TIMESTAMPDIFF(HOUR, started_at, current_timestamp())) AS blind_spot_hours,
    ARRAY_JOIN(COLLECT_SET(probable_cause), ', ') AS root_causes
FROM silentpulse.monitoring.alerts_enriched
WHERE status = 'active'
GROUP BY asset_group_name
ORDER BY active_alerts DESC;

-- ============================================================================
-- Dataset 6: AI Insights Detail Table
-- Widget: Table with all enriched fields
-- ============================================================================
SELECT
    id,
    flow_name,
    asset_group_name,
    severity,
    alert_type,
    started_at,
    probable_cause,
    ai_insights.affected_component,
    ai_insights.recommended_action,
    TIMESTAMPDIFF(HOUR, started_at, current_timestamp()) AS gap_hours
FROM silentpulse.monitoring.alerts_enriched
WHERE status = 'active'
ORDER BY
    CASE severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
    started_at;
