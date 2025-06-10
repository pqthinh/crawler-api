from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
from .settings import settings

logger = logging.getLogger(__name__)

producer = None

def get_kafka_producer():
    global producer
    if producer is None:
        try:
            # Allow comma-separated list of bootstrap servers
            bootstrap_servers_list = [s.strip() for s in settings.KAFKA_BOOTSTRAP_SERVERS.split(',')]
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers_list, 
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3, # Retry sending if produce fails
                # linger_ms=10, # Optional: Wait up to 10ms to batch messages
                # acks='all' # Optional: Ensure messages are received by all In-Sync Replicas (ISRs)
            )
            logger.info(f"KafkaProducer initialized for servers: {bootstrap_servers_list}")
        except KafkaError as e:
            logger.error(f"Failed to initialize KafkaProducer: {e}")
            raise # Re-raise to signal failure to the caller
    return producer

def send_job_to_kafka(topic: str, job_message_data: dict):
    """Sends a job message (as a dictionary) to the specified Kafka topic."""
    try:
        kafka_producer = get_kafka_producer()
        # The job_message_data should be a dict, Pydantic model's .model_dump() can provide this
        future = kafka_producer.send(topic, value=job_message_data)
        # Optional: Block for synchronous send & get metadata or handle errors asynchronously
        # future.add_callback(on_send_success).add_errback(on_send_error)
        logger.debug(f"Sent job to Kafka topic {topic} for api_id: {job_message_data.get('api_id')}")
        # producer.flush() # Call flush if you want to ensure messages are sent immediately (e.g., before exiting a short script)
    except KafkaError as e:
        logger.error(f"Kafka sending error to topic {topic} for api_id {job_message_data.get('api_id')}: {e}")
        # Decide on error handling: re-raise, DLQ, or log and continue
    except Exception as e:
        logger.error(f"Unexpected error sending job to Kafka topic {topic}: {e}")

# It's generally better to manage producer lifecycle with the application context
# For Celery, producer can be initialized when the app starts or per task if needed.
# atexit might not work reliably in all deployment scenarios (e.g. with some process managers for Celery). 