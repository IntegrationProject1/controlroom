import pika
import xml.etree.ElementTree as ET
import requests
import traceback
import time

# Configuratie
RABBITMQ_HOST = "rabbitmq"  # IP-adres van de VM
RABBITMQ_PORT = 5672
QUEUE_NAME = "controlroom.heartbeat.test"
#LOGSTASH_URL = "http://localhost:5044"
LOGSTASH_URL = "http://logstash:5044"

def process_message(body):
    """Verwerkt het bericht en stuurt het door naar Logstash"""
    try:
        # Decodeer het XML bericht
        message = body.decode()
        root = ET.fromstring(message)

        # Extract relevant data from the XML
        service_name = root.find('ServiceName').text
        status = root.find('Status').text
        timestamp = root.find('Timestamp').text
        heartbeat_interval = root.find('HeartBeatInterval').text
        version = root.find('.//Version').text
        host = root.find('.//Host').text
        environment = root.find('.//Environment').text

        # Maak een gestructureerd bericht met de geëxtraheerde gegevens
        message_dict = {
            "ServiceName": service_name,
            "Status": status,
            "Timestamp": timestamp,
            "HeartBeatInterval": heartbeat_interval,
            "Version": version,
            "Host": host,
            "Environment": environment
        }

        print(f"✅ Ontvangen bericht: {message_dict}")
        
        # Verzend het bericht naar Logstash
        if "Status" not in message_dict or "Timestamp" not in message_dict:
            print("⚠️ Fout: Bericht mist verplichte velden (status/timestamp)")
            return

        response = requests.post(LOGSTASH_URL, json=message_dict)
        if response.status_code in [200, 201]:
            print("📨 Bericht succesvol doorgestuurd naar Logstash")
        else:
            print(f"⚠️ Fout bij verzenden naar Logstash: {response.status_code} - {response.text}")
    
    except ET.ParseError:
        print("⚠️ Fout: Ongeldig XML-formaat ontvangen")
    except Exception as e:
        print(f"❌ Fout bij verwerken van bericht: {e}")
        traceback.print_exc()

def callback(ch, method, properties, body):
    """Wordt aangeroepen bij een nieuw bericht in de queue"""
    process_message(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def connect():
    """Maakt verbinding met RabbitMQ en start de consumer"""
    time.sleep(60)  # Wacht 60 seconden om te verzekeren dat RabbitMQ en Logstash klaar zijn
    for attempt in range(10):  # Retry up to 10 times
        try:
            print(f"🔄 Verbinden met RabbitMQ (poging {attempt + 1})...")
            credentials = pika.PlainCredentials('guest', 'guest')
            connection_params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
            print("🎧 Wachten op berichten...")
            channel.start_consuming()
            break  # Exit the loop if the connection is successful
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

if __name__ == "__main__":
    connect()
