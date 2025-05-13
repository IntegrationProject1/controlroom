import pytest
from consumer.consumer import process_heartbeat, downtime_tracking
from unittest.mock import patch
from datetime import datetime, timedelta, timezone
import requests

@pytest.fixture(autouse=True)
def reset_tracking():
    downtime_tracking.clear()
    yield
    downtime_tracking.clear()

@pytest.fixture
def valid_heartbeat():
    return b"<Heartbeat><ServiceName>service1</ServiceName></Heartbeat>"

@patch("consumer.consumer.requests.post")
def test_process_valid_heartbeat(mock_post, valid_heartbeat):
    mock_post.return_value.status_code = 200

    process_heartbeat(valid_heartbeat)

    assert "service1" in downtime_tracking
    assert downtime_tracking["service1"]["downtime_start"] is None
    assert mock_post.call_count == 1

@patch("consumer.consumer.requests.post")
def test_process_heartbeat_resolves_downtime(mock_post, valid_heartbeat):
    mock_post.return_value.status_code = 200

    now = datetime.now(timezone.utc)
    downtime_tracking["service1"] = {
        "last_seen": (now - timedelta(seconds=10)).isoformat() + "Z",
        "downtime_start": (now - timedelta(seconds=10)).isoformat() + "Z"
    }

    process_heartbeat(valid_heartbeat)

    assert "service1" in downtime_tracking
    assert downtime_tracking["service1"]["downtime_start"] is None

    assert mock_post.call_count == 2
    payloads = [call.kwargs['json'] for call in mock_post.call_args_list]
    assert any(p["Type"] == "downtime" for p in payloads)
    assert any(p["Type"] == "heartbeat" for p in payloads)

@patch("consumer.consumer.requests.post")
def test_invalid_xml_handled_gracefully(mock_post):
    bad_data = b"<Heartbeat><ServiceName>Servicename"
    process_heartbeat(bad_data)
    assert mock_post.call_count == 0

@patch("consumer.consumer.requests.post")
def test_missing_service_name_handled(mock_post):
    bad_data = b"<Heartbeat></Heartbeat>"
    process_heartbeat(bad_data)
    assert mock_post.call_count == 0

@patch("consumer.consumer.requests.post")
def test_new_service_tracking(mock_post):
    mock_post.return_value.status_code = 200
    new_heartbeat = b"<Heartbeat><ServiceName>new_service</ServiceName></Heartbeat>"

    process_heartbeat(new_heartbeat)

    assert "new_service" in downtime_tracking
    assert downtime_tracking["new_service"]["downtime_start"] is None
    assert mock_post.call_count == 1

@patch("consumer.consumer.requests.post")
def test_last_seen_updated(mock_post, valid_heartbeat):
    mock_post.return_value.status_code = 200

    process_heartbeat(valid_heartbeat)
    first_seen = downtime_tracking["service1"]["last_seen"]

    # Send a second heartbeat with a short delay
    process_heartbeat(valid_heartbeat)
    second_seen = downtime_tracking["service1"]["last_seen"]

    assert second_seen >= first_seen

@patch("consumer.consumer.requests.post")
def test_post_failure_handled(mock_post, valid_heartbeat):
    mock_post.return_value.status_code = 500

    process_heartbeat(valid_heartbeat)

    assert "service1" in downtime_tracking
    assert mock_post.call_count == 1  # Still tries to send

@patch("consumer.consumer.requests.post")
def test_unexpected_xml_structure(mock_post):
    unexpected_xml = b"<RandomTag><Other>data</Other></RandomTag>"
    process_heartbeat(unexpected_xml)
    assert mock_post.call_count == 0
