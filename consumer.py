import pika
import json
import requests
 
# Configuratie
RABBITMQ_HOST = "108.143.20.102"  # IP-adres van de VM
RABBITMQ_PORT = 30020
QUEUE_NAME = "consumer.controlroom.heartbeat"
LOGSTASH_URL = "http://108.143.20.102:30053"  # Logstash draait ook op de VM
 
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
    try:
        connection_params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
        print("🎧 Wachten op berichten...")
        channel.start_consuming()
    except Exception as e:
        print(f"❌ Fout bij verbinden met RabbitMQ: {e}")
 
if __name__ == "__main__":
    connect()

 
 