import pika
import time

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
RABBITMQ_HOST = "localhost"  # Change if RabbitMQ is hosted remotely
RABBITMQ_QUEUE = "controlroom.heartbeat.test"

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
channel = connection.channel()

# Declare Queue
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

def generate_dummy_logs():
    """Generate dummy logs."""
    logs = [
        {
            "ServiceName": "Kassa",
            "Status": "OK",
            "Timestamp": "2023-10-10T12:34:56.789Z",
            "HeartBeatInterval": "60",
            "Version": "1.0.0",
            "Host": "hostname_123",
            "Environment": "production"
        }
    ]
    return logs

def publish_logs(logs):
    """Publish dummy logs to RabbitMQ in XML format."""
    for log in logs:
        message = dict_to_xml(log)
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)  # Make messages persistent
        )
        print(f"Sent: {message}")
        time.sleep(1)  # Simulate log generation delay

if __name__ == "__main__":
    logs = generate_dummy_logs()
    publish_logs(logs)
    connection.close()