import pika
import xml.etree.ElementTree as ET
import requests
import traceback
import time
import os 
from dotenv import load_dotenv

load_dotenv()

# Configuratie
# RABBITMQ_HOST = "integrationproject-2425s2-001.westeurope.cloudapp.azure.com"  
# RABBITMQ_PORT = 30020
# QUEUE_NAME = "controlroom.heartbeat.test"

#logstash url 
LOGSTASH_URL = "http://logstash:5044"

def process_message(body):
    """Processes the message and sends it to Logstash"""
    try:
        # Decode the XML message
        message = body.decode()
        root = ET.fromstring(message)

        # Extract relevant data from the XML
        service_name = root.find('ServiceName').text
        
        

        # Makes a dictionary from the extracted data
        message_dict = {
            "ServiceName": service_name,
            
        }

        print(f"✅ Received message: {message_dict}")
        
        # Send the message to Logstash
        if "ServiceName" not in message_dict:
            print("⚠️ Error: Message is missing fields")
            return

        response = requests.post(LOGSTASH_URL, json=message_dict)
        if response.status_code in [200, 201]:
            print("📨  Message succesfully sent to Logstash")
        else:
            print(f"⚠️ Error sending message to logstash: {response.status_code} - {response.text}")
    
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
    for attempt in range(10):  # Retry up to 10 times
        try:
            print(f"🔄 Connecting to RabbitMQ (attempt {attempt + 1})...")
            credentials = pika.PlainCredentials(os.getenv("RABBITMQ_USER"), os.getenv("RABBITMQ_PASSWORD")) #credentials for local development
            connection_params = pika.ConnectionParameters(host=os.getenv("RABBITMQ_HOST"), port=os.getenv("RABBITMQ_PORT"), credentials=credentials)
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()
            channel.queue_declare(queue=os.getenv("QUEUE_NAME"), durable=True)
            channel.basic_consume(queue=os.getenv("QUEUE_NAME"), on_message_callback=callback)
            print("🎧 Waiting for messages...")
            channel.start_consuming()
            break  # Exit the loop if the connection is successful
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

if __name__ == "__main__":
    connect()