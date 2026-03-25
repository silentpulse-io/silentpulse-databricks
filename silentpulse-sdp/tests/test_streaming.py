"""Tests for silentpulse_sdp.streaming module (foreachBatch integration)."""

from __future__ import annotations

from unittest.mock import patch

from silentpulse_sdp.streaming import report_batch, streaming_sink

# ---------------------------------------------------------------------------
# Mock DataFrame helpers (same pattern as test_observe.py)
# ---------------------------------------------------------------------------


class MockRow:
    def __init__(self, *values):
        self._values = values

    def __getitem__(self, idx):
        return self._values[idx]


class MockColumn:
    def isNull(self):
        return self


class MockGrouped:
    def __init__(self, rows: list | None = None):
        self._rows = rows

    def count(self):
        return MockDataFrame(rows=self._rows)


class MockDataFrame:
    def __init__(
        self,
        count: int = 0,
        columns: list[str] | None = None,
        rows: list | None = None,
        grouped_rows: list | None = None,
    ):
        self._count = count
        self.columns = columns or []
        self._rows = rows
        self._grouped_rows = grouped_rows

    def count(self) -> int:
        return self._count

    def filter(self, cond):
        return MockDataFrame(count=0, columns=self.columns, rows=self._rows)

    def select(self, *cols):
        return self

    def distinct(self):
        return self

    def groupBy(self, *cols):
        return MockGrouped(self._grouped_rows)

    def collect(self):
        return self._rows or []

    def __getitem__(self, key):
        return MockColumn()


class BrokenDataFrame:
    """DataFrame that raises on every operation."""

    columns = ["a"]

    def count(self):
        raise RuntimeError("broken")

    def filter(self, cond):
        raise RuntimeError("broken")

    def groupBy(self, *cols):
        raise RuntimeError("broken")

    def __getitem__(self, key):
        raise RuntimeError("broken")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _types(events):
    return [e["type"] for e in events]


def _events_by_type(events, t):
    return [e for e in events if e["type"] == t]


# ---------------------------------------------------------------------------
# report_batch — table-level
# ---------------------------------------------------------------------------


@patch("silentpulse_sdp.streaming.client.send_telemetry")
def test_report_batch_heartbeat_and_volume(mock_send):
    df = MockDataFrame(count=50)
    report_batch(df, 1, integration_point="zerobus")

    mock_send.assert_called_once()
    ip, events = mock_send.call_args[0]
    assert ip == "zerobus"
    assert len(events) == 2
    assert _types(events) == ["heartbeat", "volume"]
    assert events[0]["metadata"]["table_name"] == "zerobus"
    assert events[0]["metadata"]["batch_id"] == 1
    assert events[1]["metadata"]["row_count"] == 50
    assert events[1]["metadata"]["min_rows"] == 0


@patch("silentpulse_sdp.streaming.client.send_telemetry")
def test_report_batch_with_max_rows(mock_send):
    df = MockDataFrame(count=100)
    report_batch(df, 2, integration_point="ip", max_rows=500)

    events = mock_send.call_args[0][1]
    vol = _events_by_type(events, "volume")[0]
    assert vol["metadata"]["max_rows"] == 500


@patch("silentpulse_sdp.streaming.client.send_telemetry")
def test_report_batch_with_completeness(mock_send):
    df = MockDataFrame(count=200, columns=["host", "severity"])
    report_batch(df, 3, integration_point="ip", expected_columns=["host", "severity"])

    events = mock_send.call_args[0][1]
    types = _types(events)
    assert "completeness" in types
    comp = _events_by_type(events, "completeness")[0]
    assert "null_counts" in comp["metadata"]
    assert comp["metadata"]["total_rows"] == 200


@patch("silentpulse_sdp.streaming.client.send_telemetry")
def test_report_batch_with_freshness(mock_send):
    df = MockDataFrame(count=10)
    report_batch(df, 4, integration_point="ip", max_delay_seconds=600)

    events = mock_send.call_args[0][1]
    types = _types(events)
    assert "freshness" in types
    fresh = _events_by_type(events, "freshness")[0]
    assert fresh["metadata"]["max_delay_seconds"] == 600


@patch("silentpulse_sdp.streaming.client.send_telemetry")
def test_report_batch_all_options(mock_send):
    df = MockDataFrame(count=300, columns=["host", "ts"])
    report_batch(
        df,
        5,
        integration_point="ip",
        min_rows=10,
        max_rows=1000,
        expected_columns=["host", "ts"],
        max_delay_seconds=900,
    )

    events = mock_send.call_args[0][1]
    assert _types(events) == ["heartbeat", "volume", "completeness", "freshness"]


# ---------------------------------------------------------------------------
# report_batch — per-asset
# ---------------------------------------------------------------------------


def _asset_df():
    return MockDataFrame(
        count=100,
        columns=["host", "ts"],
        rows=[MockRow("h1"), MockRow("h2"), MockRow("h3")],
        grouped_rows=[MockRow("h1", 40), MockRow("h2", 35), MockRow("h3", 25)],
    )


@patch("silentpulse_sdp.streaming.client.send_telemetry")
def test_report_batch_per_asset(mock_send):
    report_batch(_asset_df(), 10, integration_point="zb", asset_column="host")

    events = mock_send.call_args[0][1]
    heartbeats = _events_by_type(events, "heartbeat")
    volumes = _events_by_type(events, "volume")
    assert len(heartbeats) == 3
    assert len(volumes) == 3
    names = {e["metadata"]["table_name"] for e in heartbeats}
    assert names == {"h1", "h2", "h3"}
    counts = {e["metadata"]["table_name"]: e["metadata"]["row_count"] for e in volumes}
    assert counts == {"h1": 40, "h2": 35, "h3": 25}


@patch("silentpulse_sdp.streaming.client.send_telemetry")
def test_report_batch_per_asset_with_freshness(mock_send):
    report_batch(_asset_df(), 11, integration_point="zb", asset_column="host", max_delay_seconds=300)

    events = mock_send.call_args[0][1]
    fresh = _events_by_type(events, "freshness")
    assert len(fresh) == 3
    assert all(e["metadata"]["max_delay_seconds"] == 300 for e in fresh)


@patch("silentpulse_sdp.streaming.client.send_telemetry")
def test_report_batch_per_asset_with_completeness(mock_send):
    report_batch(_asset_df(), 12, integration_point="zb", asset_column="host", expected_columns=["host", "ts"])

    events = mock_send.call_args[0][1]
    comp = _events_by_type(events, "completeness")
    assert len(comp) == 3


# ---------------------------------------------------------------------------
# Exception safety
# ---------------------------------------------------------------------------


@patch("silentpulse_sdp.streaming.client.send_telemetry", side_effect=RuntimeError("boom"))
def test_report_batch_swallows_exceptions(mock_send):
    df = MockDataFrame(count=10)
    # Must not raise
    report_batch(df, 99, integration_point="ip")


def test_report_batch_swallows_df_errors():
    df = BrokenDataFrame()
    # Must not raise even when DataFrame operations fail
    report_batch(df, 99, integration_point="ip")


# ---------------------------------------------------------------------------
# Empty batch
# ---------------------------------------------------------------------------


@patch("silentpulse_sdp.streaming.client.send_telemetry")
def test_report_batch_empty_batch(mock_send):
    df = MockDataFrame(count=0)
    report_batch(df, 0, integration_point="ip")

    events = mock_send.call_args[0][1]
    assert _types(events) == ["heartbeat", "volume"]
    assert events[1]["metadata"]["row_count"] == 0


# ---------------------------------------------------------------------------
# streaming_sink factory
# ---------------------------------------------------------------------------


def test_streaming_sink_returns_callable():
    sink = streaming_sink(integration_point="zb")
    assert callable(sink)


@patch("silentpulse_sdp.streaming.client.send_telemetry")
def test_streaming_sink_all_options(mock_send):
    sink = streaming_sink(
        integration_point="zb",
        asset_column="host",
        min_rows=5,
        max_rows=500,
        expected_columns=["host"],
        max_delay_seconds=120,
    )
    df = MockDataFrame(count=20, columns=["host"])
    sink(df, 7)

    mock_send.assert_called_once()
    events = mock_send.call_args[0][1]
    assert _types(events) == ["heartbeat", "volume", "completeness", "freshness"]


@patch("silentpulse_sdp.streaming.client.send_telemetry", side_effect=RuntimeError("boom"))
def test_streaming_sink_swallows_exceptions(mock_send):
    sink = streaming_sink(integration_point="zb")
    # Must not raise
    sink(MockDataFrame(count=1), 0)


# ---------------------------------------------------------------------------
# Fallback when asset_column extraction fails
# ---------------------------------------------------------------------------


@patch("silentpulse_sdp.streaming.client.send_telemetry")
def test_report_batch_fallback_on_empty_assets(mock_send):
    """When asset_column is set but groupBy returns empty, fall back to table-level."""
    df = MockDataFrame(count=50, columns=["host"], grouped_rows=[])
    report_batch(df, 20, integration_point="zb", asset_column="host")

    events = mock_send.call_args[0][1]
    assert len(events) == 2
    assert events[0]["metadata"]["table_name"] == "zb"
