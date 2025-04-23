import pika
import time
import datetime
import random
import traceback
import os
from lxml import etree

# Function to convert a dictionary to XML
def dict_to_heartbeat_xml(log):
    """Convert dictionary to the specified XML format."""

    xml = """
    <Heartbeat>
        <ServiceName>{ServiceName}</ServiceName>
    </Heartbeat>
    """.format(**log)
    return xml.strip()

def dict_to_log_xml(log):
    xml = """
    <Log>
        <ServiceName>{ServiceName}</ServiceName>
        <Status>{Status}</Status>
        <Message>{Message}</Message>
    </Log>
    """.format(**log)
    return xml.strip()

# RabbitMQ Configuration
# RABBITMQ_PORT = 30020

#RABBITMQ_HOST = "integrationproject-2425s2-001.westeurope.cloudapp.azure.com"
#RABBITMQ_PORT = 30020
#RABBITMQ_USERNAME = os.getenv("RABBITMQ_USER")
#RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASS")


# Rabbitmq host and port for local development:

RABBITMQ_USERNAME = "guest"
RABBITMQ_PASSWORD = "guest"
QUEUE_NAME = "controlroom.heartbeat.test"
RABBITMQ_HOST = "rabbitmq"
RABBITMQ_PORT = 5672


# Generate dummy logs
def generate_dummy_logs():
    """Generate a list of dummy logs with random values."""
    statuses = ["OK", "ERROR", "WARNING"]
    environments = ["production", "staging", "development"]
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
        ]
    }
    
    logs = []
    for i in range(5):  # Generate 5 dummy logs
        status = random.choice(statuses)
        log = {
            "ServiceName": f"TestService_{i}",
            "Status": status,
        }
        
        # Voeg message toe
        if status in ["ERROR", "WARNING"]:
            log["Message"] = random.choice(messages[status])
            
        logs.append(log)
    return logs

# Function to validate XML against XSD
def validate_heartbeat_with_xsd(xml_str, xsd_path="heartbeat.xsd"):
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
    
def validate_log_with_xsd(xml_str, xsd_path="log.xsd"):
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

# Publish logs to RabbitMQ
def publish_logs(channel):
    """Continuously check for new logs and send them every second."""
    last_sent_data = None
    
    while True:
        logs = generate_dummy_logs()
        
        if logs != last_sent_data:  # Check if new data is available
            for log in logs:
                message = dict_to_heartbeat_xml(log)
                is_valid, error = validate_heartbeat_with_xsd(message)
                if not is_valid:
                    print(f"❌ Invalid XML: {error}")
                    continue
                channel.basic_publish(
                    exchange='',
                    routing_key='controlroom.heartbeat.test',
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2)  # Make messages persistent
                )
                print(f"Sent: {message}")
                
                # Send log if it's an error or warning
                if log["Status"] in ["ERROR", "WARNING"]:
                    log_xml = dict_to_log_xml(log)
                    is_valid, error = validate_log_with_xsd(log_xml)
                    if not is_valid:
                        print(f"❌ Invalid XML: {error}")
                        continue
                    channel.basic_publish(
                        exchange='',
                        routing_key='controlroom.log.test',  # You can change this queue name if needed
                        body=log_xml,
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    print(f"Sent log message: {log_xml}")
                
                    time.sleep(1)  # Wait 1 seconds before sending the next log
            last_sent_data = logs  # Store last sent data
        else:
            print("No new data. Waiting...")
            time.sleep(1)  # Keep waiting if no new data




# Connect to RabbitMQ

def main():
    """Connecting to RabbitMQ and starting the publisher."""
    time.sleep(60) # Waits for RabbitMQ to start (Local development)
    for attempt in range(10):  # Retry up to 10 times
        try:
            print(f"🔄 Connecting to RabbitMQ (attempt {attempt + 1})...")
            #credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
            credentials = pika.PlainCredentials('guest', 'guest') #credentials for local development
            connection_params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            print("Connected to RabbitMQ")
            break
            # Exit the loop if the connection is successful
        except pika.exceptions.AMQPConnectionError as e:
            print(f"❌ Error connecting to RabbitMQ: {e}")
            traceback.print_exc()  # Log the full traceback for debugging
            time.sleep(5)  # Wait 5 seconds before retrying
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            traceback.print_exc()  # Log unexpected errors
            time.sleep(5)  # Wait 5 seconds before retrying
    else:
        print("❌ Unable to connect to RabbitMQ after several attempts.")
    
    publish_logs(channel)
    connection.close()
    

if __name__ == "__main__":
    main()