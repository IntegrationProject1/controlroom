import docker
import json
import time
import pika
import os
import sys
from datetime import datetime, timezone
import logging
 
# Configure logging - reduce verbosity
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('log_collector')
 
# Reduce Pika's logging verbosity
logging.getLogger('pika').setLevel(logging.WARNING)
 
# Configuration - use environment variables or defaults
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")  # Use localhost since we're on the host
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASSWORD", "guest")
LOG_QUEUE = os.getenv("LOG_QUEUE", "controlroom.container.logs")
COLLECTION_INTERVAL = int(os.getenv("COLLECTION_INTERVAL", "30"))  # seconds
 
# Stats tracking
stats = {
    "total_logs_collected": 0,
    "total_logs_sent": 0,
    "failed_sends": 0,
    "collection_runs": 0,
    "containers_processed": 0
}
 
def setup_rabbitmq_connection():
    """Create and return a RabbitMQ connection and channel"""
    try:
        logger.info(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        connection_params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
        connection = pika.BlockingConnection(connection_params)
        channel = connection.channel()
        
        # Declare the queue
        channel.queue_declare(queue=LOG_QUEUE, durable=True)
        logger.info(f"Successfully connected to RabbitMQ and declared queue {LOG_QUEUE}")
        
        # Remove delivery confirmation
        # channel.confirm_delivery()
        
        return connection, channel
    except Exception as e:
        logger.error(f"Error connecting to RabbitMQ: {e}")
        return None, None
 
def collect_container_logs(client, since=None):
    """Collect logs from all running containers"""
    logs = []
    container_count = 0
    
    try:
        containers = client.containers.list()
        logger.info(f"Found {len(containers)} running containers")
        
        for container in containers:
            try:
                container_count += 1
                container_logs = container.logs(
                    since=since,
                    timestamps=True,
                    stream=False
                ).decode('utf-8', errors='replace')
                
                if container_logs.strip():
                    # Split logs by line and process each line
                    for line in container_logs.strip().split('\n'):
                        # Extract timestamp if present (format: 2023-04-23T09:44:18.424152Z)
                        timestamp = datetime.now(timezone.utc).isoformat()
                        log_content = line
                        
                        if ' ' in line and line[0:4].isdigit() and line[4] == '-':
                            parts = line.split(' ', 1)
                            if len(parts) == 2:
                                timestamp = parts[0]
                                log_content = parts[1]
                        
                        log_entry = {
                            "ContainerID": container.id,
                            "ContainerName": container.name,
                            "Image": container.image.tags[0] if container.image.tags else container.image.id,
                            "Timestamp": timestamp,
                            "Message": log_content,
                            "Type": "container_log"
                        }
                        logs.append(log_entry)
            except Exception as e:
                logger.warning(f"Error collecting logs from container {container.name}: {e}")
                continue
    
    except Exception as e:
        logger.error(f"Error listing containers: {e}")
    
    stats["containers_processed"] += container_count
    stats["total_logs_collected"] += len(logs)
    return logs