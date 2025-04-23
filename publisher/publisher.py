import pika
import time
import datetime
import random
import traceback
import os
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

RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")
# Rabbitmq host and port for local development:
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
# RABBITMQ_PORT = 5672

QUEUE_NAME = "controlroom.heartbeat.ping"
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
# Rabbitmq credentials for local development:
# RABBITMQ_USERNAME = "guest"
# RABBITMQ_PASSWORD = "guest"





# Generate dummy logs
def generate_dummy_logs():
    """Generate a list of dummy logs with random values."""
    statuses = ["OK"]
    environments = ["production", "staging", "development"]
    
    logs = []
    for i in range(5):  # Generate 5 dummy logs
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
    """Continuously check for new logs and send them every second."""
    last_sent_data = None
    
    while True:
        logs = generate_dummy_logs()
        
        if logs != last_sent_data:  # Check if new data is available
            for log in logs:
                message = dict_to_xml(log)
                channel.basic_publish(
                    exchange='heartbeat',
                    routing_key='controlroom.heartbeat.ping',
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
            print(f"Connecting to RabbitMQ (attempt {attempt + 1})...")
            print("Rabbitmq info: ",RABBITMQ_USERNAME, RABBITMQ_PASSWORD, RABBITMQ_HOST, RABBITMQ_PORT)
            #credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
            credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD) #credentials for local development
            connection_params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()
            channel.exchange_declare(exchange='heartbeat', exchange_type='direct', durable=True)
            
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            print("Connected to RabbitMQ")
            break
            # Exit the loop if the connection is successful
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error connecting to RabbitMQ: {e}")
            traceback.print_exc()  # Log the full traceback for debugging
            time.sleep(5)  # Wait 5 seconds before retrying
        except Exception as e:
            print(f"Unexpected error: {e}")
            traceback.print_exc()  # Log unexpected errors
            time.sleep(5)  # Wait 5 seconds before retrying
    else:
        print("Unable to connect to RabbitMQ after several attempts.")
    
    publish_logs(channel)
    connection.close()
    

if __name__ == "__main__":
    main()
