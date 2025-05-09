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


def send_logs_to_rabbitmq(channel, logs):
    """Send collected logs to RabbitMQ"""
    if not logs:
        logger.info("No new logs to send")
        return
    
    logger.info(f"Sending {len(logs)} log entries to RabbitMQ")
    sent_count = 0
    
    try:
        # First convert all logs to JSON
        messages = [json.dumps(log) for log in logs]
        
        # Publish messages without waiting for confirmations
        for message in messages:
            channel.basic_publish(
                exchange='',
                routing_key=LOG_QUEUE,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    content_type='application/json'
                )
            )
            sent_count += 1
        
        # Update stats
        stats["total_logs_sent"] += sent_count
        logger.info(f"✅ Successfully sent {sent_count} logs to RabbitMQ")
        
    except Exception as e:
        failed_count = len(logs) - sent_count
        stats["failed_sends"] += failed_count
        logger.error(f"Error sending logs to RabbitMQ: {e}")
        if failed_count > 0:
            logger.warning(f"⚠️ Failed to send {failed_count} logs")

def print_stats():
    """Print statistics about log collection and sending"""
    logger.info("=== Log Collector Statistics ===")
    logger.info(f"Collection runs: {stats['collection_runs']}")
    logger.info(f"Containers processed: {stats['containers_processed']}")
    logger.info(f"Total logs collected: {stats['total_logs_collected']}")
    logger.info(f"Total logs sent: {stats['total_logs_sent']}")
    logger.info(f"Failed sends: {stats['failed_sends']}")
    success_rate = 100.0
    if stats['total_logs_collected'] > 0:
        success_rate = (stats['total_logs_sent'] / stats['total_logs_collected'] * 100)
    logger.info(f"Success rate: {success_rate:.2f}%")
    logger.info("===============================")

def main():
    """Main function to collect and send logs periodically"""
    logger.info("Starting container log collector (host mode)")
    
    # Setup Docker client
    try:
        docker_client = docker.from_env()
        logger.info("Connected to Docker daemon")
    except Exception as e:
        logger.error(f"Failed to connect to Docker daemon: {e}")
        sys.exit(1)
    
    # Track the last collection time
    last_collection = datetime.now(timezone.utc)
    
    try:
        while True:
            try:
                stats["collection_runs"] += 1
                
                # Setup RabbitMQ connection (reconnect each time to handle potential disconnects)
                rabbitmq_connection, rabbitmq_channel = setup_rabbitmq_connection()
                if not rabbitmq_connection or not rabbitmq_channel:
                    logger.error("Failed to connect to RabbitMQ. Retrying in 10 seconds...")
                    time.sleep(10)
                    continue
                
                # Collect logs since last collection
                logs = collect_container_logs(docker_client, since=last_collection)
                
                # Send logs to RabbitMQ
                send_logs_to_rabbitmq(rabbitmq_channel, logs)
                
                # Update last collection time
                last_collection = datetime.now(timezone.utc)
                
                # Print stats every 5 runs
                if stats["collection_runs"] % 5 == 0:
                    print_stats()
                
                # Close RabbitMQ connection
                try:
                    rabbitmq_connection.close()
                except:
                    pass
                
                # Wait for next collection interval
                logger.info(f"Waiting {COLLECTION_INTERVAL} seconds until next collection...")
                time.sleep(COLLECTION_INTERVAL)
                
            except pika.exceptions.AMQPConnectionError:
                logger.warning("Lost connection to RabbitMQ. Will reconnect on next cycle.")
                time.sleep(5)
            except docker.errors.APIError as e:
                logger.warning(f"Docker API error: {e}. Reconnecting...")
                try:
                    docker_client = docker.from_env()
                except Exception as docker_error:
                    logger.error(f"Failed to reconnect to Docker daemon: {docker_error}")
                    time.sleep(10)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(COLLECTION_INTERVAL)
    
    except KeyboardInterrupt:
        logger.info("Stopping container log collector")
        print_stats()  # Print final stats

if __name__ == "__main__":
    main()