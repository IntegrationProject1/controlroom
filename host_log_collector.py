import docker
import json
import time
import pika
import os
import sys
from datetime import datetime, timezone
import logging

# Loggingconfiguratie
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('log_collector')

# Beperk logging van Pika
logging.getLogger('pika').setLevel(logging.WARNING)

# Configuratie via environment variables
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASSWORD", "guest")
LOG_QUEUE = os.getenv("LOG_QUEUE", "controlroom.log.test")
COLLECTION_INTERVAL = int(os.getenv("COLLECTION_INTERVAL", "30"))

# Statistieken
stats = {
    "total_logs_collected": 0,
    "total_logs_sent": 0,
    "failed_sends": 0,
    "collection_runs": 0,
    "containers_processed": 0
}

def setup_rabbitmq_connection():
    """Maak verbinding met RabbitMQ en declareer de queue"""
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
        channel.queue_declare(queue=LOG_QUEUE, durable=True)
        logger.info(f"Connected and queue '{LOG_QUEUE}' declared")
        return connection, channel
    except Exception as e:
        logger.error(f"Error connecting to RabbitMQ: {e}")
        return None, None

def collect_container_logs(client, since=None):
    """Verzamel logs van alle actieve containers"""
    logs = []
    container_count = 0

    try:
        containers = client.containers.list()
        logger.info(f"Found {len(containers)} running containers")

        for container in containers:
            try:
                container_count += 1
                raw_logs = container.logs(
                    since=since,
                    timestamps=True,
                    stream=False
                ).decode('utf-8', errors='replace')

                if raw_logs.strip():
                    for line in raw_logs.strip().split('\n'):
                        timestamp = datetime.now(timezone.utc).isoformat()
                        log_content = line

                        if ' ' in line and line[0:4].isdigit() and line[4] == '-':
                            parts = line.split(' ', 1)
                            if len(parts) == 2:
                                timestamp, log_content = parts

                        logs.append({
                            "ContainerID": container.id,
                            "ContainerName": container.name,
                            "Image": container.image.tags[0] if container.image.tags else container.image.id,
                            "Timestamp": timestamp,
                            "Message": log_content,
                            "Type": "container_log"
                        })
            except Exception as e:
                logger.warning(f"Failed to collect logs from {container.name}: {e}")
                continue

    except Exception as e:
        logger.error(f"Error accessing Docker containers: {e}")

    stats["containers_processed"] += container_count
    stats["total_logs_collected"] += len(logs)
    return logs

def send_logs_to_rabbitmq(channel, logs):
    """Stuur verzamelde logs naar RabbitMQ"""
    if not logs:
        logger.info("No new logs to send")
        return

    logger.info(f"Sending {len(logs)} logs to RabbitMQ")
    sent_count = 0

    try:
        messages = [json.dumps(log) for log in logs]

        for message in messages:
            channel.basic_publish(
                exchange='',
                routing_key=LOG_QUEUE,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )
            sent_count += 1

        stats["total_logs_sent"] += sent_count
        logger.info(f"Sent {sent_count} logs")

    except Exception as e:
        failed_count = len(logs) - sent_count
        stats["failed_sends"] += failed_count
        logger.error(f"Error sending logs: {e}")
        if failed_count > 0:
            logger.warning(f"{failed_count} logs failed to send")

def print_stats():
    """Toon verzamelstatistieken"""
    logger.info("=== Log Collector Stats ===")
    logger.info(f"Collection runs: {stats['collection_runs']}")
    logger.info(f"Containers processed: {stats['containers_processed']}")
    logger.info(f"Total logs collected: {stats['total_logs_collected']}")
    logger.info(f"Total logs sent: {stats['total_logs_sent']}")
    logger.info(f"Failed sends: {stats['failed_sends']}")
    rate = (stats['total_logs_sent'] / stats['total_logs_collected'] * 100) if stats['total_logs_collected'] else 100.0
    logger.info(f"Success rate: {rate:.2f}%")
    logger.info("===========================")

def main():
    """Start logverzamelaar"""
    logger.info("Starting container log collector")

    try:
        docker_client = docker.from_env()
        logger.info("Connected to Docker")
    except Exception as e:
        logger.error(f"Docker connection failed: {e}")
        sys.exit(1)

    last_collection = datetime.now(timezone.utc)

    try:
        while True:
            stats["collection_runs"] += 1

            connection, channel = setup_rabbitmq_connection()
            if not connection or not channel:
                logger.error("Retrying RabbitMQ connection in 10 seconds...")
                time.sleep(10)
                continue

            logs = collect_container_logs(docker_client, since=last_collection)
            send_logs_to_rabbitmq(channel, logs)
            last_collection = datetime.now(timezone.utc)

            if stats["collection_runs"] % 5 == 0:
                print_stats()

            try:
                connection.close()
            except:
                pass

            logger.info(f"Sleeping for {COLLECTION_INTERVAL} seconds...")
            time.sleep(COLLECTION_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Stopped by user")
        print_stats()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        time.sleep(COLLECTION_INTERVAL)

if __name__ == "__main__":
    main()
