import pika
import time
import datetime
import random
import traceback

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
RABBITMQ_HOST = "rabbitmq"
RABBITMQ_PORT = 5672

QUEUE_NAME = "controlroom.heartbeat.test"
RABBITMQ_USERNAME = "guest"
RABBITMQ_PASSWORD = "guest"


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

def publish_logs(channel):
    """Continuously check for new logs and send them every 10 seconds."""
    last_sent_data = None
    
    while True:
        logs = generate_dummy_logs()
        
        if logs != last_sent_data:  # Check if new data is available
            for log in logs:
                message = dict_to_xml(log)
                channel.basic_publish(
                    exchange='',
                    routing_key=QUEUE_NAME,
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
    """Maakt verbinding met RabbitMQ en start de publisher"""
    time.sleep(60) # Wacht 60 seconden om te verzekeren dat RabbitMQ en Logstash klaar zijn
    for attempt in range(10):  # Retry up to 5 times
        try:
            print(f"🔄 Verbinden met RabbitMQ (poging {attempt + 1})...")
            credentials = pika.PlainCredentials('guest', 'guest')
            connection_params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            print("Connected to RabbitMQ")
            break
            # Exit the loop if the connection is successful
        except pika.exceptions.AMQPConnectionError as e:
            print(f"❌ Fout bij verbinden met RabbitMQ: {e}")
            traceback.print_exc()  # Log the full traceback for debugging
            time.sleep(5)  # Wait 5 seconds before retrying
        except Exception as e:
            print(f"❌ Onverwachte fout: {e}")
            traceback.print_exc()  # Log unexpected errors
            time.sleep(5)  # Wait 5 seconds before retrying
    else:
        print("❌ Kon geen verbinding maken met RabbitMQ na meerdere pogingen.")
    
    publish_logs(channel)
    connection.close()
    

# time.sleep(60)  # Wait 60 seconds to ensure that RabbitMQ is ready, only for local development
# for attempt in range(10):  
#     try:
#         print(f"Connecting to RabbitMQ (attempt {attempt + 1})...")
#         connection = pika.BlockingConnection(connection_params)
        
#         print("Connected to RabbitMQ")
#         break
#     except pika.exceptions.AMQPConnectionError as e:
#         print(f"Error connecting to RabbitMQ: {e}")
#         time.sleep(5)
#     except Exception as e:
#         print(f"Unexpected error: {e}")
#         time.sleep(5)

# channel = connection.channel()
# channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)








if __name__ == "__main__":
    main()
