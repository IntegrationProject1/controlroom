import pika
import xml.etree.ElementTree as ET
import requests
import traceback
import time

# Configuratie
RABBITMQ_HOST = "rabbitmq"
RABBITMQ_PORT = 5672
QUEUE_NAME = "controlroom.heartbeat.test"
LOGSTASH_URL = "http://logstash:5044"

def process_message(body):
    """Processes the message and sends it to Logstash"""
    try:
        message = body.decode()
        root = ET.fromstring(message)

        # Extract only the needed field
        service_name = root.find('ServiceName').text

        message_dict = {
            "ServiceName": service_name
        }

        print(f"✅ Received message: {message_dict}")

        # Send the message to Logstash
        response = requests.post(LOGSTASH_URL, json=message_dict)
        if response.status_code in [200, 201]:
            print("📨 Message successfully sent to Logstash")
        else:
            print(f"⚠️ Error sending message to Logstash: {response.status_code} - {response.text}")

    except ET.ParseError:
        print("⚠️ Error: Invalid XML message")
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()

def callback(ch, method, properties, body):
    """Gets called when a message is received"""
    process_message(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def connect():
    """Connects to RabbitMQ and starts consuming messages"""
    time.sleep(60)  # Wait for RabbitMQ to start (Local development)
    for attempt in range(10):
        try:
            print(f"🔄 Connecting to RabbitMQ (attempt {attempt + 1})...")
            credentials = pika.PlainCredentials('guest', 'guest')
            connection_params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials
            )
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
            print("🎧 Waiting for messages...")
            channel.start_consuming()
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

if __name__ == "__main__":
    connect()

