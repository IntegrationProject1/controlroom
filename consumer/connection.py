import pika
import time
import os
import traceback

# Configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
HEARTBEAT_QUEUE = "controlroom.heartbeat.ping"
LOG_QUEUE = "controlroom.log.event"

#Maakt alle queues leeg wanneer de consumer opstart
def purge_queues(channel):
    """Purge all queues before starting to consume"""
    try:
        channel.queue_purge(queue=HEARTBEAT_QUEUE)
        channel.queue_purge(queue=LOG_QUEUE)
        print("Queues purged successfully")
    except Exception as e:
        print(f"Error purging queues: {e}")

# Maakt verbinding met RabbitMQ en start het consumeren van berichten
def connect(heartbeat_callback, log_callback):
    """Connects to RabbitMQ and starts consuming messages"""
    time.sleep(60)  # Wait for RabbitMQ to start
    for attempt in range(15):  # Retry up to 15 times
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