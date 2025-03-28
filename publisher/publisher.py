import pika
import time
import datetime
import random

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
# RABBITMQ_HOST = "integrationproject-2425s2-001.westeurope.cloudapp.azure.com"
# RABBITMQ_PORT = 30020
# Rabbitmq host and port for local development:
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672

RABBITMQ_QUEUE = "controlroom.heartbeat.test"
RABBITMQ_USERNAME = ""
RABBITMQ_PASSWORD = ""

# Create credentials
credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)

# Connection parameters
connection_params = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    credentials=credentials
)

# Connect to RabbitMQ
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

# Declare Queue
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

def generate_dummy_logs():
    """Generate a list of dummy logs with random values."""
    statuses = ["OK", "WARNING", "ERROR"]
    environments = ["production", "staging", "development"]
    
    logs = []
    for i in range(5):  # Generate 5 dummy logs
        logs.append({
            "ServiceName": f"TestService_{i}",
            "Status": random.choice(statuses),
            "Timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "HeartBeatInterval": str(random.randint(30, 120)),
            "Version": f"1.{random.randint(0, 9)}.{random.randint(0, 9)}",
            "Host": f"host_{random.randint(100, 999)}",
            "Environment": random.choice(environments)
        })
    return logs

def publish_logs():
    """Continuously check for new logs and send them every 10 seconds."""
    last_sent_data = None
    
    while True:
        logs = generate_dummy_logs()
        
        if logs != last_sent_data:  # Check if new data is available
            for log in logs:
                message = dict_to_xml(log)
                channel.basic_publish(
                    exchange='',
                    routing_key=RABBITMQ_QUEUE,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2)  # Make messages persistent
                )
                print(f"Sent: {message}")
                time.sleep(10)  # Wait 10 seconds before sending the next log
            last_sent_data = logs  # Store last sent data
        else:
            print("No new data. Waiting...")
            time.sleep(10)  # Keep waiting if no new data

if __name__ == "__main__":
    publish_logs()
    connection.close()
