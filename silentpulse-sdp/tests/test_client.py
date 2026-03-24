"""Tests for silentpulse_sdp.client module."""

from unittest.mock import MagicMock, patch

from silentpulse_sdp.client import _send_payload, send_telemetry
from silentpulse_sdp.config import configure, reset


def test_send_payload_success():
    with patch("silentpulse_sdp.client.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=202)
        _send_payload(
            {"integration_point": "test", "events": []},
            "https://sp.example.com",
            "sp_pat_123",
            5.0,
        )
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "https://sp.example.com/api/v1/telemetry/ingest"
        assert kwargs["headers"]["Authorization"] == "Bearer sp_pat_123"


def test_send_payload_http_error_logged(caplog):
    with patch("silentpulse_sdp.client.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=400, text="bad request")
        import logging

        with caplog.at_level(logging.WARNING, logger="silentpulse_sdp"):
            _send_payload({"events": []}, "https://sp.example.com", "tok", 5.0)
        assert "400" in caplog.text


def test_send_payload_connection_error_logged(caplog):
    import logging

    import requests

    with patch("silentpulse_sdp.client.requests.post", side_effect=requests.ConnectionError("refused")):
        with caplog.at_level(logging.WARNING, logger="silentpulse_sdp"):
            _send_payload({"events": []}, "https://sp.example.com", "tok", 5.0)
        assert "refused" in caplog.text


def test_send_telemetry_disabled():
    reset()
    configure(api_url="https://sp.example.com", api_token="tok", enabled=False)
    with patch("silentpulse_sdp.client._executor") as mock_exec:
        send_telemetry("test-ip", [{"type": "heartbeat"}])
        mock_exec.submit.assert_not_called()
    reset()


def test_send_telemetry_unconfigured():
    reset()
    with patch("silentpulse_sdp.client._executor") as mock_exec:
        send_telemetry("test-ip", [{"type": "heartbeat"}])
        mock_exec.submit.assert_not_called()
    reset()


def test_send_telemetry_submits_to_executor():
    reset()
    configure(api_url="https://sp.example.com", api_token="sp_pat_test")
    with patch("silentpulse_sdp.client._executor") as mock_exec:
        send_telemetry("test-ip", [{"type": "heartbeat"}])
        mock_exec.submit.assert_called_once()
    reset()
