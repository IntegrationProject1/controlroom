# test_integration_heartbeat.py

import json
import time
import uuid
import pika
import pytest
import requests
from requests.auth import HTTPBasicAuth
from xml.etree.ElementTree import Element, tostring
import os

# RabbitMQ settings
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
HEARTBEAT_QUEUE = os.getenv("HEARTBEAT_QUEUE", "HEARTBEAT_QUEUE")

# Elasticsearch settings
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "heartbeat-index")
ELASTICSEARCH_USER = os.getenv("ELASTICSEARCH_USER", "elastic")
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD", "changeme")

def create_test_xml(service_name: str) -> str:
    """Creates a minimal XML heartbeat message."""
    root = Element("Heartbeat")
    service_elem = Element("ServiceName")
    service_elem.text = service_name
    root.append(service_elem)
    return tostring(root, encoding="utf-8")

def publish_heartbeat(xml_message: str):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    # Declare the exchange (same as in your publisher)
    channel.exchange_declare(exchange='heartbeat_monitoring', exchange_type='direct', durable=True)

    # Publish to exchange, not to a queue
    channel.basic_publish(
        exchange='heartbeat_monitoring',
        routing_key='controlroom.heartbeat.ping',  # <- Use correct routing key
        body=xml_message,
        properties=pika.BasicProperties(delivery_mode=2)  # persistent
    )

    print(f"✅ Test message sent to exchange 'heartbeat_monitoring' with routing key 'controlroom.heartbeat.ping'")
    connection.close()

def query_elasticsearch(service_name: str) -> bool:
    time.sleep(5)
    response = requests.get(
        f"{ELASTICSEARCH_HOST}/heartbeat-index-*/_search",
        json={
            "query": {
                "match": {
                    "ServiceName": service_name
                }
            }
        },
        auth=HTTPBasicAuth(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD)
    )
    print(json.dumps(response.json(), indent=2))
    hits = response.json().get("hits", {}).get("hits", [])
    return any(hit["_source"].get("ServiceName") == service_name for hit in hits)



@pytest.mark.integration
def test_heartbeat_message_is_indexed():
    """Integration test: XML → RabbitMQ → Logstash → Elasticsearch"""
    unique_service_name = f"test_service_{uuid.uuid4()}"
    xml_message = create_test_xml(unique_service_name)
    
    publish_heartbeat(xml_message)
    
    success = False
    for attempt in range(5):  # retry for up to ~25s
        print(f"Attempt {attempt + 1}: Searching for {unique_service_name} in Elasticsearch...")
        if query_elasticsearch(unique_service_name):
            success = True
            break
        time.sleep(5)

    assert success, f"Service '{unique_service_name}' not found in Elasticsearch."
