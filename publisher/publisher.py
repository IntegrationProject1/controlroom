import pika
import time
import datetime
import random
import traceback
import os

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
            "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "HeartBeatInterval": str(1),
            "Version": f"1.{random.randint(0, 9)}.{random.randint(0, 9)}",
            "Host": f"host_{random.randint(100, 999)}",
            "Environment": random.choice(environments)
        }
        
        # Voeg message toe
        if status in ["ERROR", "WARNING"]:
            log["Message"] = random.choice(messages[status])
        else:
            log["Message"] = "No issues detected."
            
        logs.append(log)
    return logs

# Publish logs to RabbitMQ
def publish_logs(channel):
    """Continuously check for new logs and send them every second."""
    last_sent_data = None
    
    while True:
        logs = generate_dummy_logs()
        
        if logs != last_sent_data:  # Check if new data is available
            for log in logs:
                message = dict_to_xml(log)
                channel.basic_publish(
                    exchange='',
                    routing_key='controlroom.heartbeat.test',
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2)  # Make messages persistent
                )
                print(f"Sent: {message}")
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
