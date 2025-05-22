import pytest
import pika
from consumer.consumer import (
    process_heartbeat, downtime_tracking, process_log, 
    purge_queues, connect, heartbeat_callback, log_callback
)
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timedelta, timezone
import requests

# Zorgt dat de tracking dictionary leeg is voor/na elke test
@pytest.fixture(autouse=True)
def reset_tracking():
    downtime_tracking.clear()
    yield
    downtime_tracking.clear()

# Geldig heartbeat XML-bericht
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
    process_heartbeat(valid_heartbeat)
    second_seen = downtime_tracking["service1"]["last_seen"]
    assert second_seen >= first_seen

@patch("consumer.consumer.requests.post")
def test_post_failure_handled(mock_post, valid_heartbeat):
    mock_post.return_value.status_code = 500
    process_heartbeat(valid_heartbeat)
    assert "service1" in downtime_tracking
    assert mock_post.call_count == 1

@patch("consumer.consumer.requests.post")
def test_unexpected_xml_structure(mock_post):
    unexpected_xml = b"<RandomTag><Other>data</Other></RandomTag>"
    process_heartbeat(unexpected_xml)
    assert mock_post.call_count == 0

@patch("consumer.consumer.requests.post")
def test_process_log_valid(mock_post):
    mock_post.return_value.status_code = 200
    xml = b"<Log><ServiceName>service1</ServiceName><Status>OK</Status><Message>All good</Message></Log>"
    process_log(xml)
    assert mock_post.call_count == 1

@patch("consumer.consumer.requests.post")
def test_process_log_invalid_xml(mock_post):
    process_log(b"<Log><ServiceName>bad")
    assert mock_post.call_count == 0

@patch("consumer.consumer.requests.post")
def test_process_log_missing_fields(mock_post):
    xml = b"<Log><ServiceName>service1</ServiceName></Log>"
    process_log(xml)
    assert mock_post.call_count == 0

@patch("consumer.consumer.requests.post")
def test_process_heartbeat_missing_service_name(mock_post):
    xml = b"<Heartbeat></Heartbeat>"
    process_heartbeat(xml)
    assert mock_post.call_count == 0

@patch("consumer.consumer.requests.post")
def test_process_heartbeat_downtime_resolution_parsing_error(mock_post):
    downtime_tracking["bad_service"] = {
        "last_seen": "broken",
        "downtime_start": "not-a-date"
    }
    xml = b"<Heartbeat><ServiceName>bad_service</ServiceName></Heartbeat>"
    mock_post.return_value.status_code = 200
    process_heartbeat(xml)
    assert mock_post.call_count == 2  

def test_purge_queues_success():
    mock_channel = MagicMock()
    purge_queues(mock_channel)
    mock_channel.queue_purge.assert_any_call(queue="controlroom.heartbeat.ping")
    mock_channel.queue_purge.assert_any_call(queue="controlroom.log.event")

@patch("consumer.consumer.requests.post")
def test_downtime_duration_parsing_error(mock_post):
    downtime_tracking["service1"] = {
        "last_seen": "2024-05-01T12:00:00Z",
        "downtime_start": "INVALID_TIMESTAMP"
    }
    xml = b"<Heartbeat><ServiceName>service1</ServiceName></Heartbeat>"
    mock_post.return_value.status_code = 200
    process_heartbeat(xml)

def test_log_parse_error():
    xml = b"<Log><ServiceName>oops</Log>"
    process_log(xml)

def test_log_missing_status():
    xml = b"<Log><ServiceName>test</ServiceName><Message>Missing status</Message></Log>"
    process_log(xml)
