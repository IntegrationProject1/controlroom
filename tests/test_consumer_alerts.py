
import pytest
from unittest.mock import patch, MagicMock
import sys
import os
from datetime import datetime, timezone, timedelta

# Voeg parentdirectory toe aan het pad voor imports uit de consumer-module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from consumer.consumer import (
    process_log,
    process_heartbeat,
    downtime_tracking,
    send_email_alert,
    check_downtime
)

# Helpers om XML-berichten op te bouwen
def build_log_xml(service, status, message):
    return f"""<?xml version="1.0"?>
<Log>
    <ServiceName>{service}</ServiceName>
    <Status>{status}</Status>
    <Message>{message}</Message>
</Log>
""".encode()


def build_heartbeat_xml(service):
    return f"""<?xml version="1.0"?>
<Heartbeat>
    <ServiceName>{service}</ServiceName>
</Heartbeat>
""".encode()

# Fixtures voor mocking van externe afhankelijkheden
@pytest.fixture
def mock_email():
    with patch("consumer.consumer.send_email_alert") as mock:
        yield mock

@pytest.fixture
def mock_requests():
    with patch("consumer.consumer.requests.post") as mock:
        mock.return_value.status_code = 200
        yield mock

# Test: e-mail wordt verzonden bij foutmelding
# def test_process_log_sends_email_on_error(mock_email, mock_requests):
#     xml = build_log_xml("test-service", "error", "Something failed")
#     process_log(xml)
#     mock_email.assert_called_once_with("test-service", "Foutmelding: error", "Something failed")

# # Test: geen e-mail bij info-bericht
# def test_process_log_does_not_send_email_on_info(mock_email, mock_requests):
#     xml = build_log_xml("test-service", "info", "Just FYI")
#     process_log(xml)
#     mock_email.assert_not_called()

# Test: downtime detectie stuurt e-mail
def test_downtime_start_sends_email_alert(mock_email, mock_requests):
    service = "test-downtime"
    now = datetime.now(timezone.utc) - timedelta(seconds=10)
    downtime_tracking[service] = {
        "last_seen": now.isoformat() + "Z",
        "downtime_start": None
    }

    # Mock sleep zodat check_downtime maar 1x loopt
    with patch("consumer.consumer.time.sleep", side_effect=InterruptedError):
        try:
            check_downtime()
        except InterruptedError:
            pass

    mock_email.assert_called_with(
        service,
        "Service Down",
        f"Service {service} is offline sinds {downtime_tracking[service]['downtime_start']}."
    )

# import pytest
# from unittest.mock import patch, MagicMock
# import sys
# import os
# from datetime import datetime, timezone, timedelta

# # Voeg het parentpad toe om imports uit de consumer-module mogelijk te maken
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# from consumer.consumer import (
#     process_log,
#     process_heartbeat,
#     downtime_tracking,
#     send_email_alert,
#     check_downtime
# )

# # XML helpers
# def build_log_xml(service, status, message):
#     return f"""<?xml version="1.0"?>
# <Log>
#     <ServiceName>{service}</ServiceName>
#     <Status>{status}</Status>
#     <Message>{message}</Message>
# </Log>
# """.encode()


# def build_heartbeat_xml(service):
#     return f"""<?xml version="1.0"?>
# <Heartbeat>
#     <ServiceName>{service}</ServiceName>
# </Heartbeat>
# """.encode()


# @pytest.fixture
# def mock_email():
#     with patch("consumer.consumer.send_email_alert") as mock:
#         yield mock


# @pytest.fixture
# def mock_requests():
#     with patch("consumer.consumer.requests.post") as mock:
#         mock.return_value.status_code = 200
#         yield mock


# def test_process_log_sends_email_on_error(mock_email, mock_requests):
#     xml = build_log_xml("test-service", "error", "Something failed")
#     process_log(xml)
#     mock_email.assert_called_once_with("test-service", "Foutmelding: error", "Something failed")


# def test_process_log_does_not_send_email_on_info(mock_email, mock_requests):
#     xml = build_log_xml("test-service", "info", "Just FYI")
#     process_log(xml)
#     mock_email.assert_not_called()


# def test_downtime_start_sends_email_alert(mock_email, mock_requests):
#     service = "test-downtime"
#     now = datetime.now(timezone.utc) - timedelta(seconds=10)
#     downtime_tracking[service] = {
#         "last_seen": now.isoformat() + "Z",
#         "downtime_start": None
#     }

#     # Mock `time.sleep` zodat check_downtime slechts één keer loopt
#     with patch("consumer.consumer.time.sleep", side_effect=InterruptedError):
#         try:
#             check_downtime()
#         except InterruptedError:
#             pass

#     mock_email.assert_called_with(
#         service,
#         "Service Down",
#         f"Service {service} is offline sinds {downtime_tracking[service]['downtime_start']}."
#     )


#  Deze test controleert of een service weer online is, maar is momenteel uitgecommentarieerd.
# Je kan hem activeren als je herstel-functionaliteit wil testen.

# def test_heartbeat_resolves_downtime(mock_email, mock_requests):
#     service = "test-recover"
#     start = (datetime.now(timezone.utc) - timedelta(seconds=20)).isoformat() + "Z"
#     downtime_tracking[service] = {
#         "last_seen": start,
#         "downtime_start": start
#     }

#     heartbeat_xml = build_heartbeat_xml(service)
#     process_heartbeat(heartbeat_xml)

#     assert downtime_tracking[service]["downtime_start"] is None
#     assert mock_email.call_args[0][0] == service
#     assert mock_email.call_args[0][1] == "Downtime Resolved"
#     assert f"{service} is weer online" in mock_email.call_args[0][2]
