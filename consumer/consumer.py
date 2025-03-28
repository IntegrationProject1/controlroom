import pika
import json
import requests
import traceback
import time
 
# Configuratie
RABBITMQ_HOST = "rabbitmq"  # IP-adres van de VM
# RABBITMQ_PORT = 30020
# Rabbitmq host and port for local development:
RABBITMQ_PORT = 5672
QUEUE_NAME = "controlroom.heartbeat.test"
# LOGSTASH_URL = "http://108.143.20.102:30053"  # Logstash draait ook op de VM
# Logstash host and port for local development:
LOGSTASH_URL = "http://localhost:5044"
 
def process_message(body):
    """Verwerkt het bericht en stuurt het door naar Logstash"""
    try:
        message = json.loads(body)
        print(f"✅ Ontvangen bericht: {message}")
        if "status" not in message or "timestamp" not in message:
            print("⚠️ Fout: Bericht mist verplichte velden (status/timestamp)")
            return
        response = requests.post(LOGSTASH_URL, json=message)
        if response.status_code in [200, 201]:
            print("📨 Bericht succesvol doorgestuurd naar Logstash")
        else:
            print(f"⚠️ Fout bij verzenden naar Logstash: {response.status_code} - {response.text}")
    except json.JSONDecodeError:
        print("⚠️ Fout: Ongeldig JSON-formaat ontvangen")
 
def callback(ch, method, properties, body):
    """Wordt aangeroepen bij een nieuw bericht in de queue"""
    process_message(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)
 
def connect():
    """Maakt verbinding met RabbitMQ en start de consumer"""
    time.sleep(60) # Wacht 60 seconden om te verzekeren dat RabbitMQ en Logstash klaar zijn
    for attempt in range(10):  # Retry up to 5 times
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

 
 