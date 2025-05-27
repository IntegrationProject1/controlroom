import pika
import time
import datetime
import random
import traceback
import os
from lxml import etree

# Genereert XML voor heartbeat-bericht
def dict_to_heartbeat_xml(log):
    xml = f"""
    <Heartbeat>
        <ServiceName>{log['ServiceName']}</ServiceName>
    </Heartbeat>
    """
    return xml.strip()

# Genereert XML voor logbericht
def dict_to_log_xml(log):
    xml = f"""
    <Log>
        <ServiceName>{log['ServiceName']}</ServiceName>
        <Status>{log['Status']}</Status>
        <Message>{log['Message']}</Message>
    </Log>
    """
    return xml.strip()

# RabbitMQ-configuratie via omgeving
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
QUEUE_NAME = "controlroom.heartbeat.ping"

# Genereert dummylogs met willekeurige status en boodschap
def generate_dummy_logs():
    statuses = ["OK", "ERROR", "WARNING", "INFO"]
    messages = {
        "ERROR": [
            "Failed to connect to database.",
            "Null pointer exception in ServiceHandler.",
            "Unhandled exception occurred during processing.",
        ],
        "WARNING": [
            "High memory usage detected.",
            "Slow response time from external API.",
            "Service response delayed, retrying...",
        ],
        "OK": [
            "Service is running smoothly.",
            "No issues detected.",
            "All systems operational.",
        ],
        "INFO": [
            "Service started successfully.",
            "Configuration loaded.",
            "Service is shutting down gracefully.",
        ]
    }
    status = random.choice(statuses)
    log = {
        "ServiceName": "Test_Service",
        "Status": status,
        "Message": random.choice(messages[status]) if status in messages else ""
    }
    return [log]

# Valideert heartbeat XML tegen XSD-schema
def validate_heartbeat_with_xsd(xml_str, xsd_path="heartbeat.xsd"):
    try:
        xml_doc = etree.fromstring(xml_str.encode("utf-8"))
        with open(xsd_path, 'rb') as f:
            xmlschema_doc = etree.parse(f)
            xmlschema = etree.XMLSchema(xmlschema_doc)
        xmlschema.assertValid(xml_doc)
        return True, None
    except etree.DocumentInvalid as e:
        return False, str(e)
    except Exception as e:
        return False, f"Unexpected error during validation: {str(e)}"

# Valideert log XML tegen XSD-schema
def validate_log_with_xsd(xml_str, xsd_path="log.xsd"):
    try:
        xml_doc = etree.fromstring(xml_str.encode("utf-8"))
        with open(xsd_path, 'rb') as f:
            xmlschema_doc = etree.parse(f)
            xmlschema = etree.XMLSchema(xmlschema_doc)
        xmlschema.assertValid(xml_doc)
        return True, None
    except etree.DocumentInvalid as e:
        return False, str(e)
    except Exception as e:
        return False, f"Unexpected error during validation: {str(e)}"

# Stuurt logs en heartbeats naar RabbitMQ
def publish_logs(channel):
    last_sent_data = None
    last_log_time = 0

    while True:
        logs = generate_dummy_logs()
        current_time = time.time()

        if logs != last_sent_data:
            for log in logs:
                heartbeat = dict_to_heartbeat_xml(log)
                is_valid, error = validate_heartbeat_with_xsd(heartbeat)
                if not is_valid:
                    print(f" Invalid XML: {error}")
                    continue

                channel.basic_publish(
                    exchange='heartbeat_monitoring',
                    routing_key='controlroom.heartbeat.ping',
                    body=heartbeat,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                print(f" Sent heartbeat: {heartbeat}")

                # Stuur ook log mee als het minstens 30s geleden is
                if current_time - last_log_time >= 30 and log["Status"] in ["ERROR", "WARNING", "INFO", "OK"]:
                    log_xml = dict_to_log_xml(log)
                    is_valid, error = validate_log_with_xsd(log_xml)
                    if not is_valid:
                        print(f" Invalid XML: {error}")
                        continue

                    channel.basic_publish(
                        exchange='log_monitoring',
                        routing_key='controlroom.log.event',
                        body=log_xml,
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    print(f" Sent log message: {log_xml}")
                    last_log_time = current_time

                time.sleep(1)
            last_sent_data = logs
        else:
            print(" No new data. Waiting...")
            time.sleep(5)

# Maakt verbinding met RabbitMQ en start publishing
def main():
    time.sleep(60)  # Wacht tot RabbitMQ is opgestart

    for attempt in range(10):
        try:
            print(f"Connecting to RabbitMQ (attempt {attempt + 1})...")
            credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
            connection_params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials
            )
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()

            # Exchanges en queues declareren
            channel.exchange_declare(exchange='heartbeat_monitoring', exchange_type='direct', durable=True)
            channel.exchange_declare(exchange='log_monitoring', exchange_type='direct', durable=True)
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.queue_declare(queue='controlroom.log.event', durable=True)
            channel.queue_bind(exchange='heartbeat_monitoring', queue=QUEUE_NAME, routing_key=QUEUE_NAME)
            channel.queue_bind(exchange='log_monitoring', queue='controlroom.log.event', routing_key='controlroom.log.test')

            print("Connected to RabbitMQ")
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
        return

    publish_logs(channel)
    connection.close()

if __name__ == "__main__":
    main()
