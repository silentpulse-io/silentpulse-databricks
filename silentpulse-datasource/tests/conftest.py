"""Shared fixtures for SilentPulse DataSource tests."""

from __future__ import annotations

import pytest


@pytest.fixture
def base_options():
    """Minimal valid options dict."""
    return {
        "url": "https://sp.example.com",
        "token": "sp_pat_test123",
        "resource": "alerts",
    }


@pytest.fixture
def alerts_page():
    """Single page of alerts API response."""
    return {
        "data": [
            {
                "id": "a1",
                "flow_pulse_id": "fp1",
                "asset_id": "as1",
                "status": "active",
                "alert_type": "silence",
                "severity": "high",
                "started_at": "2026-03-01T10:00:00Z",
                "resolved_at": None,
                "tenant_id": "t1",
                "asset_identifier": "host-01",
                "pulse_label": "EDR",
                "flow_name": "EDR Flow",
                "asset_group_name": "Servers",
            }
        ],
        "total_pages": 1,
        "page": 1,
    }


@pytest.fixture
def flows_page():
    """Single page of flows API response."""
    return {
        "data": [
            {
                "id": "f1",
                "name": "EDR Flow",
                "description": "Monitor EDR telemetry",
                "enabled": True,
                "asset_group_id": "ag1",
                "tenant_id": "t1",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-03-01T00:00:00Z",
            }
        ],
        "total_pages": 1,
        "page": 1,
    }


@pytest.fixture
def coverage_response():
    """Pipeline coverage API response (non-paginated)."""
    return {
        "pipelines": [
            {
                "flow_id": "f1",
                "flow_name": "EDR Flow",
                "flow_enabled": True,
                "asset_group_name": "Servers",
                "total_assets": 42,
                "stages": '[{"name": "ingest"}]',
            }
        ]
    }
