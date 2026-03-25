"""Unit tests for SilentPulse PySpark DataSource.

All HTTP calls are mocked — no SparkSession or real network needed.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from silentpulse_datasource.datasource import (
    SCHEMAS,
    SilentPulseBatchReader,
    SilentPulseDataSource,
    SilentPulseStreamReader,
    SilentPulseStreamWriter,
)

# -- DataSource -----------------------------------------------------------------


class TestDataSourceName:
    def test_name(self):
        assert SilentPulseDataSource.name() == "silentpulse"


class TestDataSourceSchema:
    def test_schema_alerts(self):
        ds = SilentPulseDataSource(options={"resource": "alerts"})
        assert "id STRING" in ds.schema()
        assert "severity STRING" in ds.schema()

    def test_schema_flows(self):
        ds = SilentPulseDataSource(options={"resource": "flows"})
        assert "name STRING" in ds.schema()
        assert "enabled BOOLEAN" in ds.schema()

    def test_schema_entities(self):
        ds = SilentPulseDataSource(options={"resource": "entities"})
        assert "external_id STRING" in ds.schema()

    def test_schema_observations(self):
        ds = SilentPulseDataSource(options={"resource": "observations"})
        assert "entity_count INT" in ds.schema()

    def test_schema_coverage(self):
        ds = SilentPulseDataSource(options={"resource": "coverage"})
        assert "total_assets INT" in ds.schema()

    def test_schema_default_is_alerts(self):
        ds = SilentPulseDataSource(options={})
        assert ds.schema() == SCHEMAS["alerts"]

    def test_schema_unknown_resource(self):
        ds = SilentPulseDataSource(options={"resource": "nope"})
        with pytest.raises(ValueError, match="Unknown resource 'nope'"):
            ds.schema()


# -- Batch Reader ---------------------------------------------------------------


def _mock_response(json_data, status_code=200, headers=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.headers = headers or {}
    resp.text = json.dumps(json_data)
    return resp


class TestBatchReader:
    def test_single_page(self, base_options, alerts_page):
        reader = SilentPulseBatchReader(base_options, SCHEMAS["alerts"])
        partitions = reader.partitions()
        assert len(partitions) == 1

        mock_session = MagicMock()
        mock_session.get.return_value = _mock_response(alerts_page)
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("requests.Session", return_value=mock_session):
            rows = list(reader.read(partitions[0]))

        assert len(rows) == 1
        assert rows[0][0] == "a1"  # id
        assert rows[0][3] == "active"  # status

    def test_pagination(self, base_options):
        page1 = {
            "data": [{"id": "a1", "status": "active"}],
            "total_pages": 2,
            "page": 1,
        }
        page2 = {
            "data": [{"id": "a2", "status": "resolved"}],
            "total_pages": 2,
            "page": 2,
        }

        reader = SilentPulseBatchReader(base_options, SCHEMAS["alerts"])

        mock_session = MagicMock()
        mock_session.get.side_effect = [_mock_response(page1), _mock_response(page2)]
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("requests.Session", return_value=mock_session):
            rows = list(reader.read(reader.partitions()[0]))

        assert len(rows) == 2
        assert rows[0][0] == "a1"
        assert rows[1][0] == "a2"

    def test_auth_header(self, base_options, alerts_page):
        reader = SilentPulseBatchReader(base_options, SCHEMAS["alerts"])

        mock_session = MagicMock()
        mock_session.get.return_value = _mock_response(alerts_page)
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("requests.Session", return_value=mock_session):
            list(reader.read(reader.partitions()[0]))

        call_kwargs = mock_session.get.call_args
        assert call_kwargs.kwargs["headers"]["Authorization"] == "Bearer sp_pat_test123"

    def test_retry_on_500(self, base_options, alerts_page):
        reader = SilentPulseBatchReader(
            {**base_options, "max_retries": "2"},
            SCHEMAS["alerts"],
        )

        error_resp = _mock_response({}, status_code=500)
        ok_resp = _mock_response(alerts_page)

        mock_session = MagicMock()
        mock_session.get.side_effect = [error_resp, ok_resp]
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("requests.Session", return_value=mock_session), patch("time.sleep"):
            rows = list(reader.read(reader.partitions()[0]))

        assert len(rows) == 1
        assert mock_session.get.call_count == 2

    def test_flows_resource(self, base_options, flows_page):
        opts = {**base_options, "resource": "flows"}
        reader = SilentPulseBatchReader(opts, SCHEMAS["flows"])

        mock_session = MagicMock()
        mock_session.get.return_value = _mock_response(flows_page)
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("requests.Session", return_value=mock_session):
            rows = list(reader.read(reader.partitions()[0]))

        assert len(rows) == 1
        assert rows[0][1] == "EDR Flow"  # name

    def test_coverage_non_paginated(self, base_options, coverage_response):
        opts = {**base_options, "resource": "coverage"}
        reader = SilentPulseBatchReader(opts, SCHEMAS["coverage"])

        mock_session = MagicMock()
        mock_session.get.return_value = _mock_response(coverage_response)
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("requests.Session", return_value=mock_session):
            rows = list(reader.read(reader.partitions()[0]))

        assert len(rows) == 1
        assert rows[0][0] == "f1"  # flow_id
        assert rows[0][4] == 42  # total_assets

    def test_nested_json_serialized(self, base_options):
        """Nested dicts/lists in response are serialized to JSON strings."""
        page = {
            "data": [{"id": "a1", "traits": {"os": "linux"}}],
            "total_pages": 1,
        }
        opts = {**base_options, "resource": "entities"}
        reader = SilentPulseBatchReader(opts, SCHEMAS["entities"])

        mock_session = MagicMock()
        mock_session.get.return_value = _mock_response(page)
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("requests.Session", return_value=mock_session):
            rows = list(reader.read(reader.partitions()[0]))

        traits_val = rows[0][3]  # traits column
        assert json.loads(traits_val) == {"os": "linux"}


# -- Validation -----------------------------------------------------------------


class TestValidation:
    def test_missing_url_raises(self):
        with pytest.raises(AssertionError):
            SilentPulseBatchReader({"token": "t"}, SCHEMAS["alerts"])

    def test_missing_token_raises(self):
        with pytest.raises(AssertionError):
            SilentPulseBatchReader({"url": "http://x"}, SCHEMAS["alerts"])


# -- Stream Reader --------------------------------------------------------------


class TestStreamReader:
    def test_initial_offset(self, base_options):
        reader = SilentPulseStreamReader(base_options, SCHEMAS["alerts"])
        offset = json.loads(reader.initialOffset())
        assert offset["timestamp"] == "1970-01-01T00:00:00Z"

    def test_latest_offset(self, base_options):
        reader = SilentPulseStreamReader(base_options, SCHEMAS["alerts"])
        offset = json.loads(reader.latestOffset())
        assert "timestamp" in offset
        assert offset["timestamp"].endswith("Z")

    def test_partitions_pass_time_range(self, base_options):
        reader = SilentPulseStreamReader(base_options, SCHEMAS["alerts"])
        start = json.dumps({"timestamp": "2026-03-01T00:00:00Z"})
        end = json.dumps({"timestamp": "2026-03-01T01:00:00Z"})
        parts = reader.partitions(start, end)
        assert len(parts) == 1
        assert parts[0].start_time == "2026-03-01T00:00:00Z"
        assert parts[0].end_time == "2026-03-01T01:00:00Z"

    def test_read_passes_time_filters(self, base_options, alerts_page):
        reader = SilentPulseStreamReader(base_options, SCHEMAS["alerts"])
        partition = reader.partitions(
            json.dumps({"timestamp": "2026-03-01T00:00:00Z"}),
            json.dumps({"timestamp": "2026-03-01T01:00:00Z"}),
        )[0]

        mock_session = MagicMock()
        mock_session.get.return_value = _mock_response(alerts_page)
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        with patch("requests.Session", return_value=mock_session):
            list(reader.read(partition))

        call_kwargs = mock_session.get.call_args
        assert call_kwargs.kwargs["params"]["started_after"] == "2026-03-01T00:00:00Z"
        assert call_kwargs.kwargs["params"]["started_before"] == "2026-03-01T01:00:00Z"

    def test_stream_only_alerts(self, base_options):
        ds = SilentPulseDataSource(options={**base_options, "resource": "flows"})
        with pytest.raises(ValueError, match="only supported for 'alerts'"):
            ds.streamReader(SCHEMAS["flows"])


# -- Stream Writer --------------------------------------------------------------


class TestStreamWriter:
    def test_sends_telemetry(self, base_options):
        writer = SilentPulseStreamWriter(base_options, "id STRING, value STRING")

        row = MagicMock()
        row.asDict.return_value = {"id": "1", "value": "test"}

        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            writer.write(iter([row]))

            assert mock_post.called
            call_kwargs = mock_post.call_args
            payload = call_kwargs.kwargs["json"]
            assert "events" in payload
            assert payload["events"][0]["id"] == "1"

    def test_commit_noop(self, base_options):
        writer = SilentPulseStreamWriter(base_options, "id STRING")
        writer.commit([], 0)  # should not raise

    def test_abort_noop(self, base_options):
        writer = SilentPulseStreamWriter(base_options, "id STRING")
        writer.abort([], 0)  # should not raise

    def test_auth_header(self, base_options):
        writer = SilentPulseStreamWriter(base_options, "id STRING")

        row = MagicMock()
        row.asDict.return_value = {"id": "1"}

        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            writer.write(iter([row]))

            call_kwargs = mock_post.call_args
            assert call_kwargs.kwargs["headers"]["Authorization"] == "Bearer sp_pat_test123"
