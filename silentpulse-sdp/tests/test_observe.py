"""Tests for silentpulse_sdp.observe module (decorators)."""

from __future__ import annotations

from unittest.mock import patch

from silentpulse_sdp.observe import completeness, freshness, heartbeat, volume

# ---------------------------------------------------------------------------
# Mock DataFrame helpers
# ---------------------------------------------------------------------------


class MockRow:
    """Minimal mock for a PySpark Row."""

    def __init__(self, *values):
        self._values = values

    def __getitem__(self, idx):
        return self._values[idx]


class MockColumn:
    """Minimal mock for a PySpark Column object."""

    def isNull(self):
        return self


class MockGrouped:
    """Mock for GroupedData — returned by DataFrame.groupBy()."""

    def __init__(self, rows: list | None = None):
        self._rows = rows

    def count(self):
        return MockDataFrame(rows=self._rows)


class MockDataFrame:
    """Minimal mock for a PySpark DataFrame."""

    def __init__(
        self,
        count: int = 0,
        columns: list[str] | None = None,
        streaming: bool = False,
        rows: list | None = None,
        grouped_rows: list | None = None,
    ):
        self.isStreaming = streaming
        self._count = count
        self.columns = columns or []
        self._rows = rows  # list of MockRow for collect()
        self._grouped_rows = grouped_rows  # rows returned by groupBy().count().collect()

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


# ---------------------------------------------------------------------------
# heartbeat
# ---------------------------------------------------------------------------


@patch("silentpulse_sdp.observe.client.send_telemetry")
def test_heartbeat_sends_event(mock_send):
    @heartbeat(integration_point="edr-raw")
    def bronze_events():
        return MockDataFrame(count=100)

    result = bronze_events()
    assert result.count() == 100

    mock_send.assert_called_once()
    args = mock_send.call_args
    assert args[0][0] == "edr-raw"
    events = args[0][1]
    assert len(events) == 1
    assert events[0]["type"] == "heartbeat"
    assert events[0]["metadata"]["table_name"] == "bronze_events"


@patch("silentpulse_sdp.observe.client.send_telemetry")
def test_heartbeat_streaming(mock_send):
    @heartbeat(integration_point="edr-raw")
    def streaming_table():
        return MockDataFrame(streaming=True)

    result = streaming_table()
    assert result.isStreaming is True

    mock_send.assert_called_once()
    events = mock_send.call_args[0][1]
    assert events[0]["type"] == "heartbeat"


# ---------------------------------------------------------------------------
# volume
# ---------------------------------------------------------------------------


@patch("silentpulse_sdp.observe.client.send_telemetry")
def test_volume_batch(mock_send):
    @volume(integration_point="edr-raw", min_rows=10)
    def bronze_events():
        return MockDataFrame(count=1250)

    result = bronze_events()
    assert result._count == 1250

    mock_send.assert_called_once()
    events = mock_send.call_args[0][1]
    assert events[0]["type"] == "volume"
    assert events[0]["metadata"]["row_count"] == 1250
    assert events[0]["metadata"]["min_rows"] == 10


@patch("silentpulse_sdp.observe.client.send_telemetry")
def test_volume_streaming_sends_heartbeat(mock_send):
    @volume(integration_point="edr-raw")
    def streaming_table():
        return MockDataFrame(streaming=True)

    streaming_table()

    mock_send.assert_called_once()
    events = mock_send.call_args[0][1]
    assert events[0]["type"] == "heartbeat"


# ---------------------------------------------------------------------------
# completeness
# ---------------------------------------------------------------------------


@patch("silentpulse_sdp.observe.client.send_telemetry")
def test_completeness_batch(mock_send):
    @completeness(integration_point="edr-raw", expected_columns=["host", "severity"])
    def bronze_events():
        return MockDataFrame(count=500, columns=["host", "severity", "message"])

    result = bronze_events()
    assert result._count == 500

    mock_send.assert_called_once()
    events = mock_send.call_args[0][1]
    assert events[0]["type"] == "completeness"
    metadata = events[0]["metadata"]
    assert metadata["table_name"] == "bronze_events"
    assert "null_counts" in metadata
    assert metadata["total_rows"] == 500


@patch("silentpulse_sdp.observe.client.send_telemetry")
def test_completeness_streaming_no_null_counts(mock_send):
    @completeness(integration_point="edr-raw", expected_columns=["host"])
    def streaming_table():
        return MockDataFrame(streaming=True, columns=["host"])

    streaming_table()

    mock_send.assert_called_once()
    events = mock_send.call_args[0][1]
    assert events[0]["type"] == "completeness"
    assert "null_counts" not in events[0]["metadata"]


# ---------------------------------------------------------------------------
# freshness
# ---------------------------------------------------------------------------


@patch("silentpulse_sdp.observe.client.send_telemetry")
def test_freshness_batch(mock_send):
    @freshness(integration_point="edr-raw", max_delay_seconds=1800)
    def bronze_events():
        return MockDataFrame(count=100)

    bronze_events()

    mock_send.assert_called_once()
    events = mock_send.call_args[0][1]
    assert events[0]["type"] == "freshness"
    assert events[0]["metadata"]["max_delay_seconds"] == 1800
    assert events[0]["metadata"]["table_name"] == "bronze_events"


@patch("silentpulse_sdp.observe.client.send_telemetry")
def test_freshness_streaming_sends_heartbeat(mock_send):
    @freshness(integration_point="edr-raw")
    def streaming_table():
        return MockDataFrame(streaming=True)

    streaming_table()

    mock_send.assert_called_once()
    events = mock_send.call_args[0][1]
    assert events[0]["type"] == "heartbeat"


# ---------------------------------------------------------------------------
# Decorator does not crash pipeline on error
# ---------------------------------------------------------------------------


@patch("silentpulse_sdp.observe.client.send_telemetry", side_effect=RuntimeError("boom"))
def test_heartbeat_swallows_exceptions(mock_send):
    @heartbeat(integration_point="edr-raw")
    def bronze_events():
        return "ok"

    result = bronze_events()
    assert result == "ok"


@patch("silentpulse_sdp.observe.client.send_telemetry", side_effect=RuntimeError("boom"))
def test_volume_swallows_exceptions(mock_send):
    @volume(integration_point="edr-raw")
    def bronze_events():
        return MockDataFrame(count=10)

    result = bronze_events()
    assert result._count == 10


# ---------------------------------------------------------------------------
# Function name preserved (functools.wraps)
# ---------------------------------------------------------------------------


def test_decorator_preserves_function_name():
    @heartbeat(integration_point="ip")
    @volume(integration_point="ip")
    def my_special_table():
        return None

    assert my_special_table.__name__ == "my_special_table"


# ---------------------------------------------------------------------------
# asset_column — per-asset telemetry
# ---------------------------------------------------------------------------


def _mock_df_with_hosts():
    """DataFrame with 3 distinct hosts."""
    return MockDataFrame(
        count=100,
        columns=["host", "timestamp"],
        rows=[MockRow("host1"), MockRow("host2"), MockRow("host3")],
    )


def _mock_df_with_host_counts():
    """DataFrame that returns per-host row counts from groupBy."""
    return MockDataFrame(
        count=100,
        columns=["host", "timestamp"],
        rows=[MockRow("host1"), MockRow("host2"), MockRow("host3")],
        grouped_rows=[MockRow("host1", 40), MockRow("host2", 35), MockRow("host3", 25)],
    )


@patch("silentpulse_sdp.observe.client.send_telemetry")
def test_heartbeat_asset_column(mock_send):
    @heartbeat(integration_point="zerobus", asset_column="host")
    def my_pipeline():
        return _mock_df_with_hosts()

    my_pipeline()

    mock_send.assert_called_once()
    ip, events = mock_send.call_args[0]
    assert ip == "zerobus"
    assert len(events) == 3
    names = {e["metadata"]["table_name"] for e in events}
    assert names == {"host1", "host2", "host3"}
    assert all(e["type"] == "heartbeat" for e in events)


@patch("silentpulse_sdp.observe.client.send_telemetry")
def test_volume_asset_column(mock_send):
    @volume(integration_point="zerobus", min_rows=10, asset_column="host")
    def my_pipeline():
        return _mock_df_with_host_counts()

    my_pipeline()

    mock_send.assert_called_once()
    ip, events = mock_send.call_args[0]
    assert len(events) == 3
    assert all(e["type"] == "volume" for e in events)
    counts = {e["metadata"]["table_name"]: e["metadata"]["row_count"] for e in events}
    assert counts == {"host1": 40, "host2": 35, "host3": 25}


@patch("silentpulse_sdp.observe.client.send_telemetry")
def test_completeness_asset_column(mock_send):
    @completeness(integration_point="zerobus", expected_columns=["host", "timestamp"], asset_column="host")
    def my_pipeline():
        return _mock_df_with_hosts()

    my_pipeline()

    mock_send.assert_called_once()
    ip, events = mock_send.call_args[0]
    assert len(events) == 3
    assert all(e["type"] == "completeness" for e in events)
    names = {e["metadata"]["table_name"] for e in events}
    assert names == {"host1", "host2", "host3"}


@patch("silentpulse_sdp.observe.client.send_telemetry")
def test_freshness_asset_column(mock_send):
    @freshness(integration_point="zerobus", max_delay_seconds=600, asset_column="host")
    def my_pipeline():
        return _mock_df_with_hosts()

    my_pipeline()

    mock_send.assert_called_once()
    ip, events = mock_send.call_args[0]
    assert len(events) == 3
    assert all(e["type"] == "freshness" for e in events)
    names = {e["metadata"]["table_name"] for e in events}
    assert names == {"host1", "host2", "host3"}
    assert all(e["metadata"]["max_delay_seconds"] == 600 for e in events)


@patch("silentpulse_sdp.observe.client.send_telemetry")
def test_heartbeat_asset_column_streaming_falls_back(mock_send):
    """Streaming DataFrames ignore asset_column (can't collect)."""

    @heartbeat(integration_point="zerobus", asset_column="host")
    def my_pipeline():
        return MockDataFrame(streaming=True)

    my_pipeline()

    mock_send.assert_called_once()
    events = mock_send.call_args[0][1]
    assert len(events) == 1
    assert events[0]["metadata"]["table_name"] == "my_pipeline"
