import pika
import time
import datetime
import random
import traceback
import os
from lxml import etree
from dotenv import load_dotenv

load_dotenv()

# Function to convert a dictionary to XML
def dict_to_xml(log):
    """Convert dictionary to the specified XML format."""
    xml = """
    <Heartbeat>
        <ServiceName>{ServiceName}</ServiceName>
        <Status>{Status}</Status>
        <Timestamp>{Timestamp}</Timestamp>
        <HeartBeatInterval>{HeartBeatInterval}</HeartBeatInterval>
        <Metadata>
            <Version>{Version}</Version>
            <Host>{Host}</Host>
            <Environment>{Environment}</Environment>
        </Metadata>
    </Heartbeat>
    """.format(**log)
    return xml.strip()

# Function to validate XML against XSD
def validate_xml_with_xsd(xml_str, xsd_path="publisher\heartbeat.xsd"):
    """Validate an XML string against an XSD schema file."""
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

# Generate dummy logs
def generate_dummy_logs():
    statuses = ["OK"]
    environments = ["production", "staging", "development"]
    logs = []
    for i in range(5):
        logs.append({
            "ServiceName": f"TestService_{i}",
            "Status": random.choice(statuses),
            "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "HeartBeatInterval": str(1),
            "Version": f"1.{random.randint(0, 9)}.{random.randint(0, 9)}",
            "Host": f"host_{random.randint(100, 999)}",
            "Environment": random.choice(environments)
        })
    return logs

# Publish logs to RabbitMQ
def publish_logs(channel):
    last_sent_data = None
    while True:
        logs = generate_dummy_logs()
        if logs != last_sent_data:
            for log in logs:
                message = dict_to_xml(log)
                is_valid, error = validate_xml_with_xsd(message)
                if not is_valid:
                    print(f"❌ Invalid XML: {error}")
                    continue
                channel.basic_publish(
                    exchange='',
                    routing_key=os.getenv("QUEUE_NAME"),
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                print(f"✅ Sent: {message}")
                time.sleep(1)
            last_sent_data = logs
        else:
            print("No new data. Waiting...")
            time.sleep(1)

# Connect to RabbitMQ
def main():
    time.sleep(60)  # Wait for RabbitMQ to start (for Docker/local dev)
    for attempt in range(10):
        try:
            print(f"🔄 Connecting to RabbitMQ (attempt {attempt + 1})...")
            credentials = pika.PlainCredentials(
                os.getenv("RABBITMQ_USER"), os.getenv("RABBITMQ_PASSWORD"))
            connection_params = pika.ConnectionParameters(
                host=os.getenv("RABBITMQ_HOST"),
                port=int(os.getenv("RABBITMQ_PORT")),
                credentials=credentials
            )
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()
            channel.queue_declare(queue=os.getenv("QUEUE_NAME"), durable=True)
            print("✅ Connected to RabbitMQ")
            break
        except pika.exceptions.AMQPConnectionError as e:
            print(f"❌ Error connecting to RabbitMQ: {e}")
            traceback.print_exc()
            time.sleep(5)
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            traceback.print_exc()
            time.sleep(5)
    else:
        print("❌ Unable to connect to RabbitMQ after several attempts.")
        return

    publish_logs(channel)
    connection.close()

if __name__ == "__main__":
    main()
