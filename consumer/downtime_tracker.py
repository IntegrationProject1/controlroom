import requests
import time
from datetime import datetime, timezone
import os
import threading

# Configuration
LOGSTASH_URL = os.getenv("LOGSTASH_URL")

# Global state
downtime_tracking = {}

def check_downtime():
    """Continuously check for service downtime"""
    while True:
        now = datetime.now(timezone.utc)
        print(f"Checking downtime at {now.isoformat()}")

        for service, state in list(downtime_tracking.items()):
            last_seen = state.get("last_seen")
            downtime_start = state.get("downtime_start")

            # For debugging
            print(f"Checking service {service}: last_seen={last_seen}, downtime_start={downtime_start}")

            # Parse the ISO timestamp string back to a datetime object for comparison
            if last_seen:
                if isinstance(last_seen, str):
                    try:
                        last_seen_dt = datetime.fromisoformat(last_seen.rstrip('Z'))
                        # Add UTC timezone if missing
                        if last_seen_dt.tzinfo is None:
                            last_seen_dt = last_seen_dt.replace(tzinfo=timezone.utc)
                    except ValueError:
                        print(f"Error parsing last_seen timestamp for {service}: {last_seen}")
                        continue
                else:
                    last_seen_dt = last_seen

                time_diff = now - last_seen_dt
                print(f"Service {service} time difference: {time_diff.total_seconds()} seconds")

                if not downtime_start and time_diff.total_seconds() > 5:
                    downtime_tracking[service]["downtime_start"] = now.isoformat() + "Z"
                    print(f"Downtime started for {service}")

                    downtime_log = {
                        "ServiceName": service,
                        "Type": "downtime",
                        "DowntimeStart": downtime_tracking[service]["downtime_start"],
                        "Status": "Down",
                        "DurationSeconds": 0,
                        "Duration": "00:00:00"
                    }

                    try:
                        print(f"Sending downtime log to {LOGSTASH_URL}: {downtime_log}")
                        response = requests.post(LOGSTASH_URL, json=downtime_log)
                        if response.status_code in [200, 201]:
                            print(f"Downtime log successfully sent to Logstash")
                        else:
                            print(f"Error sending downtime log to logstash: {response.status_code} - {response.text}")
                    except Exception as e:
                        print(f"Error sending downtime log: {e}")

                    # Send email alert for downtime
                    send_email_alert(
                        service,
                        "Service Down",
                        f"Service {service} is offline sinds {downtime_tracking[service]['downtime_start']}."
                    )

        time.sleep(5)  # Check every 5 seconds

def send_email_alert(service, subject, message):
    """Send an email alert (placeholder function)"""
    # This would be implemented with an actual email sending library
    print(f"📧 ALERT: {subject} - {service}: {message}")

def start_downtime_checker():
    """Start the downtime checker in a separate thread"""
    thread = threading.Thread(target=check_downtime, daemon=True)
    thread.start()
    return thread