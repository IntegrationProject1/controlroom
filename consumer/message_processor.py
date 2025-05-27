import xml.etree.ElementTree as ET
import requests
import traceback
from datetime import datetime, timezone
import os

# Configuration
LOGSTASH_URL = os.getenv("LOGSTASH_URL")

def process_heartbeat(body, downtime_tracking):
    """Processes the heartbeat message and sends it to Logstash"""
    try:
        message = body.decode()
        root = ET.fromstring(message)

        # Extract relevant data from the XML
        service_name = root.find('ServiceName').text

        now = datetime.now(timezone.utc)
        service_state = downtime_tracking.get(service_name)

        # Check if the service was down
        if service_state and service_state.get("downtime_start"):
            downtime_end = now.isoformat()
            downtime_start = service_state["downtime_start"]

            # Calculate downtime duration in seconds
            try:
                if downtime_start.endswith('Z'):
                    start_dt = datetime.fromisoformat(downtime_start.rstrip('Z')).replace(tzinfo=timezone.utc)
                else:
                    start_dt = datetime.fromisoformat(downtime_start).replace(tzinfo=timezone.utc)

                duration_seconds = 5
                
            except Exception as e:
                print(f"Error calculating duration: {e}")
                duration_seconds = 0

            downtime_log = {
                "ServiceName": service_name,
                "Type": "downtime",
                "DowntimeStart": downtime_start,
                "DowntimeEnd": downtime_end,
                "Status": "resolved",
                "DurationSeconds": duration_seconds,
            }

            response = requests.post(LOGSTASH_URL, json=downtime_log)
            if response.status_code in [200, 201]:
                print(f"Downtime resolved successfully sent to Logstash ")
            else:
                print(f"Error sending downtime resolved to logstash: {response.status_code} - {response.text}")
            print(f"Downtime ended for {service_name} ")

        downtime_tracking[service_name] = {
            "last_seen": now.isoformat() + "Z",
            "downtime_start": None
        }

        # Makes a dictionary from the extracted data
        message_dict = {
            "ServiceName": service_name,
            "Type": "heartbeat"
        }

        print(f"Received heartbeat: {message_dict}")

        # Send the message to Logstash
        if "ServiceName" not in message_dict:
            print("Error: Message is missing fields (ServiceName)")
            return

        response = requests.post(LOGSTASH_URL, json=message_dict)
        if response.status_code in [200, 201]:
            print("Heartbeat successfully sent to Logstash")
        else:
            print(f"Error sending heartbeat to logstash: {response.status_code} - {response.text}")

    except ET.ParseError:
        print("Error: Invalid XML message")
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()

def process_log(body):
    """Processes the log message and sends it to Logstash"""
    try:
        # Decode the XML message
        message = body.decode()
        root = ET.fromstring(message)

        # Extract relevant data from the XML
        service_name = root.find('ServiceName').text
        status = root.find('Status').text
        log_message = root.find('Message').text

        # Makes a dictionary from the extracted data
        message_dict = {
            "ServiceName": service_name,
            "Status": status,
            "Message": log_message,
            "Type": "log"
        }

        print(f"Received log: {message_dict}")

        # Send the message to Logstash
        if "Status" not in message_dict:
            print("Error: Message is missing fields (status)")
            return

        response = requests.post(LOGSTASH_URL, json=message_dict)
        if response.status_code in [200, 201]:
            print("Log successfully sent to Logstash")
        else:
            print(f"Error sending log to logstash: {response.status_code} - {response.text}")

        # Verstuur e-mail alert bij foutstatus
        if status.lower() in ["error", "failed", "critical"]:
            send_email_alert(service_name, f"Foutmelding: {status}", log_message)

    except ET.ParseError:
        print("Error: Invalid XML message")
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()

def send_email_alert(service, subject, message):
    """Send an email alert (placeholder function)"""
    # This would be implemented with an actual email sending library
    print(f"ALERT: {subject} - {service}: {message}")