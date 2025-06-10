# worker/src/kafka_clients.py
import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime # Added for timestamp
from typing import Optional, Dict # For type hinting

from .settings import settings
from .models import JobMessage # For type hinting if needed for next_job

logger = logging.getLogger(__name__)

_kafka_producer = None

def get_kafka_producer_client():
    global _kafka_producer
    if _kafka_producer is None:
        try:
            bootstrap_servers_list = [s.strip() for s in settings.KAFKA_BOOTSTRAP_SERVERS.split(',')]
            _kafka_producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers_list,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3,
            )
            logger.info(f"Worker KafkaProducer connected to {bootstrap_servers_list}")
        except KafkaError as e:
            logger.error(f"Worker failed to initialize KafkaProducer: {e}", exc_info=True)
            _kafka_producer = None 
            raise 
    return _kafka_producer

def _send_message(topic: str, message_data: dict, api_id_log: str):
    try:
        producer = get_kafka_producer_client()
        if not producer:
             logger.error(f"Kafka producer not available. Cannot send message to topic {topic} for api {api_id_log}.")
             return

        future = producer.send(topic, value=message_data)
        logger.debug(f"Sent message to Kafka topic '{topic}' for api_id '{api_id_log}'.")
    except KafkaError as e:
        logger.error(f"Kafka sending error to topic '{topic}' for api_id '{api_id_log}': {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error sending message to topic '{topic}' for api_id '{api_id_log}': {e}", exc_info=True)

def send_next_page_job(next_job_message: JobMessage):
    if not next_job_message:
        logger.warning("send_next_page_job called with no message.")
        return
    logger.info(f"Sending next page job for api_id: {next_job_message.api_id}, params: {next_job_message.params}")
    _send_message(settings.CRAWL_JOBS_TOPIC, next_job_message.model_dump(), next_job_message.api_id)

def send_to_dlq(failed_job_message: JobMessage, error_info: str, api_config_dlq_topic: Optional[str]):
    if not failed_job_message:
        logger.warning("send_to_dlq called with no message.")
        return
        
    dlq_topic = api_config_dlq_topic or settings.DEFAULT_DLQ_TOPIC
    
    dlq_message = {
        "failed_job": failed_job_message.model_dump(),
        "error_reason": error_info,
        "timestamp": datetime.utcnow().isoformat() 
    }
    logger.warning(f"Sending job for api_id: {failed_job_message.api_id} to DLQ topic: {dlq_topic} due to: {error_info}")
    _send_message(dlq_topic, dlq_message, failed_job_message.api_id)


def send_crawl_result(api_id: str, status: str, records_count: int = 0, error_message: Optional[str] = None, details: Optional[Dict] = None):
    result_message = {
        "api_id": api_id,
        "status": status, 
        "records_count": records_count,
        "error_message": error_message,
        "details": details or {}, 
        "timestamp": datetime.utcnow().isoformat()
    }
    logger.info(f"Sending crawl result for api_id: {api_id}, status: {status}, records: {records_count}")
    _send_message(settings.CRAWL_RESULTS_TOPIC, result_message, api_id)

# Optional: Graceful shutdown for the producer can be handled via Celery signals like worker_shutdown
# For example:
# from celery.signals import worker_shutdown
# @worker_shutdown.connect
# def close_kafka_on_shutdown(**kwargs):
#     global _kafka_producer
#     if _kafka_producer:
#         logger.info("Flushing and closing KafkaProducer before worker shutdown...")
#         _kafka_producer.flush()
#         _kafka_producer.close()
#         _kafka_producer = None
#         logger.info("KafkaProducer closed.") 