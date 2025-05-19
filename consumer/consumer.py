import pika
import xml.etree.ElementTree as ET
import requests
import traceback
import time
import os
from datetime import datetime, timedelta, timezone
import threading

# Configuratie
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
LOGSTASH_URL = os.getenv("LOGSTASH_URL")
HEARTBEAT_QUEUE = "controlroom.heartbeat.ping"
LOG_QUEUE = "controlroom.log.event"


downtime_tracking = {}

def process_heartbeat(body):
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

                # Calculate seconds as an integer
                duration_seconds = int((now - start_dt).total_seconds())
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

            # Verstuur resolved alert
            send_email_alert(service_name, "Downtime Resolved", f"{service_name} is weer online na {duration_seconds} seconden downtime.")

        downtime_tracking[service_name] = {
            "last_seen": now.isoformat() + "Z",
            "downtime_start": None
        }

        # Makes a dictionary from the extracted data
        message_dict = {
            "ServiceName": service_name,
            "Type": "heartbeat"
        }

        print(f"✅ Received heartbeat: {message_dict}")

        # Send the message to Logstash
        if "ServiceName" not in message_dict :
            print("⚠️ Error: Message is missing fields (ServiceName)")
            return

        response = requests.post(LOGSTASH_URL, json=message_dict)
        if response.status_code in [200, 201]:
            print("📨 Heartbeat successfully sent to Logstash")
        else:
            print(f"⚠️ Error sending heartbeat to logstash: {response.status_code} - {response.text}")

    except ET.ParseError:
        print("⚠️ Error: Invalid XML message")
    except Exception as e:
        print(f"❌ Error: {e}")
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

        print(f"✅ Received log: {message_dict}")

        # Send the message to Logstash
        if "Status" not in message_dict:
            print("⚠️ Error: Message is missing fields (status)")
            return

        response = requests.post(LOGSTASH_URL, json=message_dict)
        if response.status_code in [200, 201]:
            print("📨 Log successfully sent to Logstash")
        else:
            print(f"⚠️ Error sending log to logstash: {response.status_code} - {response.text}")

        # Verstuur e-mail alert bij foutstatus
        if status.lower() in ["error", "failed", "critical"]:
            send_email_alert(service_name, f"Foutmelding: {status}", log_message)

    except ET.ParseError:
        print("Error: Invalid XML message")
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()

def heartbeat_callback(ch, method, properties, body):
    """Gets called when a heartbeat message is received"""
    process_heartbeat(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def log_callback(ch, method, properties, body):
    """Gets called when a log message is received"""
    process_log(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def check_downtime():
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
                        print(f"Response status: {response.status_code}")

                        if response.status_code in [200, 201]:
                            print("Downtime started successfully sent to Logstash")
                        else:
                            print(f"Error sending downtime started to logstash: {response.status_code} - {response.text}")
                    except Exception as e:
                        print(f"Exception sending downtime log: {e}")

                    # Verstuur e-mail alert bij downtime start
                    send_email_alert(service, "Service Down", f"Service {service} is offline sinds {downtime_tracking[service]['downtime_start']}.")

                elif downtime_start and time_diff.total_seconds() > 5:
                    try:
                        if downtime_start.endswith('Z'):
                            start_dt = datetime.fromisoformat(downtime_start.rstrip('Z')).replace(tzinfo=timezone.utc)
                        else:
                            start_dt = datetime.fromisoformat(downtime_start).replace(tzinfo=timezone.utc)
                        duration_seconds = (now - start_dt).total_seconds()

                        # Send an update every 60 seconds
                        
                        downtime_log = {
                            "ServiceName": service,
                            "Type": "downtime",
                            "DowntimeStart": downtime_start,
                            "Status": "Still Down",
                            "DurationSeconds": duration_seconds,
                            
                        }
                            
                        
                        print(f"Sending downtime update log to {LOGSTASH_URL}: {downtime_log}")
                        response = requests.post(LOGSTASH_URL, json=downtime_log)
                            
                        if response.status_code in [200, 201]:
                            print(f"Downtime update successfully sent to Logstash ")
                        else:
                            print(f"Error sending downtime update to logstash: {response.status_code} - {response.text}")
                            
                    except Exception as e:
                        print(f"Error calculating ongoing duration for {service}: {e}")

        time.sleep(5)

def purge_queues(channel):
    """Purges the queues to remove any old messages before starting"""
    try:
        channel.queue_purge(queue=HEARTBEAT_QUEUE)
        channel.queue_purge(queue=LOG_QUEUE)
        print("Queues purged successfully")
        
    except Exception as e:
        print(f"Error purging queues: {e}")
        traceback.print_exc()

def send_email_alert(service_name, subject, message):
    """Sends an email alert message as XML to RabbitMQ exchange"""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)))
        channel = connection.channel()

        # Gebruik de bestaande 'email' exchange van type 'topic'
        channel.exchange_declare(exchange="email", exchange_type="topic", durable=True)

        xml_message = f"""<?xml version="1.0" encoding="UTF-8"?>
<Alert>
    <Timestamp>{datetime.utcnow().isoformat()}</Timestamp>
    <ServiceName>{service_name}</ServiceName>
    <ErrorType>{subject}</ErrorType>
    <Description>{message}</Description>
</Alert>
"""

        # Publiceer naar de topic exchange met routing key 'mail'
        channel.basic_publish(exchange="email", routing_key="mail", body=xml_message.encode())
        print(f"📧 Email alert sent for {service_name}: {subject}")
        connection.close()
    except Exception as e:
        print(f"Error sending email alert: {e}")
        traceback.print_exc()
        
def send_startup_notification():
    "Sends a test email when the control room starts up"
    send_email_alert("Controlroom", "Startup Notification", "Controlroom has started monitoring microservices.")
    print("🚀 Startup notification email sent.")

def connect():
    """Connects to RabbitMQ and starts consuming messages"""
    time.sleep(60)  # Wait for RabbitMQ to start
    for attempt in range(15):  # Retry up to 10 times
        try:
            print(f"Connecting to RabbitMQ (attempt {attempt + 1})...")
            credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
            connection_params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()

            # Declare both queues
            channel.queue_declare(queue=HEARTBEAT_QUEUE, durable=True)
            channel.queue_declare(queue=LOG_QUEUE, durable=True)

            purge_queues(channel)  # Purge the queues before starting

            # Set up consumers for both queues
            channel.basic_consume(queue=HEARTBEAT_QUEUE, on_message_callback=heartbeat_callback)
            channel.basic_consume(queue=LOG_QUEUE, on_message_callback=log_callback)

            print("Waiting for heartbeats and logs...")
            channel.start_consuming()
            break
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error connecting to RabbitMQ: {e}")
            traceback.print_exc()
            time.sleep(5)
        except Exception as e:
            print(f"Unexpected error: {e}")
            traceback.print_exc()
            time.sleep(5)
    else:
        print("Unable to connect to RabbitMQ after several attempts.")

if __name__ == "__main__":
    send_startup_notification()
    downtime_thread = threading.Thread(target=check_downtime, daemon=True)
    downtime_thread.start()
    connect()
