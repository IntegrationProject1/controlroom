import pika
import xml.etree.ElementTree as ET
import requests
import traceback
import time
import os
 
# Configuratie
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT")
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
LOGSTASH_URL = os.getenv("LOGSTASH_URL")
HEARTBEAT_QUEUE = "controlroom.heartbeat.test"
LOG_QUEUE = "controlroom.log.test"
#logstash url for local development


def process_heartbeat(body):
    """Processes the heartbeat message and sends it to Logstash"""
    try:
        message = body.decode()
        root = ET.fromstring(message)
 
        # Extract relevant data from the XML
        service_name = root.find('ServiceName').text
        
 
        # Makes a dictionary from the extracted data
        message_dict = {
            "ServiceName": service_name,
            "Type": "heartbeat"
        }
 
        print(f"✅ Received heartbeat: {message_dict}")
        
        # Send the message to Logstash
        if "ServiceName" not in message_dict :
            print("⚠️ Error: Message is missing fields (ServiceName)")
            return
 
        response = requests.post(LOGSTASH_URL, json=message_dict)
        if response.status_code in [200, 201]:
            print("📨 Heartbeat successfully sent to Logstash")
        else:
            print(f"⚠️ Error sending heartbeat to logstash: {response.status_code} - {response.text}")
    
    except ET.ParseError:
        print("⚠️ Error: Invalid XML message")
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
 
def process_log(body):
    """Processes the log message and sends it to Logstash"""
    try:
        # Decode the XML message
        message = body.decode()
        root = ET.fromstring(message)
 
        # Extract relevant data from the XML
        service_name = root.find('ServiceName').text
        status = root.find('Status').text
        log_message = root.find('Message').text
        
 
        # Makes a dictionary from the extracted data
        message_dict = {
            "ServiceName": service_name,
            "Status": status,
            "Message": log_message,
            "Type": "log"
        }
 
        print(f"✅ Received log: {message_dict}")
        
        # Send the message to Logstash
        if "Status" not in message_dict:
            print("⚠️ Error: Message is missing fields (status)")
            return
 
        response = requests.post(LOGSTASH_URL, json=message_dict)
        if response.status_code in [200, 201]:
            print("📨 Log successfully sent to Logstash")
        else:
            print(f"⚠️ Error sending log to logstash: {response.status_code} - {response.text}")
    
    except ET.ParseError:
        print("Error: Invalid XML message")
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
 
def heartbeat_callback(ch, method, properties, body):
    """Gets called when a heartbeat message is received"""
    process_heartbeat(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)
 
def log_callback(ch, method, properties, body):
    """Gets called when a log message is received"""
    process_log(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)
 
def connect():
    """Connects to RabbitMQ and starts consuming messages"""
    time.sleep(60)  # Wait for RabbitMQ to start 
    for attempt in range(10):  # Retry up to 10 times
        try:
            print(f"Connecting to RabbitMQ (attempt {attempt + 1})...")
            credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD) #credentials for local development
            connection_params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()
            
            # Declare both queues
            channel.queue_declare(queue=HEARTBEAT_QUEUE, durable=True)
            channel.queue_declare(queue=LOG_QUEUE, durable=True)
            
            # Set up consumers for both queues
            channel.basic_consume(queue=HEARTBEAT_QUEUE, on_message_callback=heartbeat_callback)
            channel.basic_consume(queue=LOG_QUEUE, on_message_callback=log_callback)
            
            print("Waiting for heartbeats and logs...")
            channel.start_consuming()
            break
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

       
 
if __name__ == "__main__":
    connect()

