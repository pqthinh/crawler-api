# worker/src/tasks.py
import logging
from celery import Celery, Task
from celery.exceptions import MaxRetriesExceededError, Reject, Ignore
from typing import Optional # Added Optional

from .settings import settings
from .models import JobMessage, ApiConfig 
from .db import load_full_api_config
from .base_crawler import BaseCrawler, AuthenticationError, HttpClientError 
from .kafka_clients import send_to_dlq 

logger = logging.getLogger(__name__)

celery_app = Celery('crawler_worker_app', broker=settings.CELERY_BROKER_URL)
celery_app.conf.task_default_queue = settings.CRAWL_JOBS_TOPIC 
celery_app.conf.task_create_missing_queues = True
celery_app.conf.update(
    result_backend=settings.CELERY_RESULT_BACKEND,
    timezone=settings.CELERY_TIMEZONE,
    task_acks_late=True, # Recommended for tasks that can be long or critical
    worker_prefetch_multiplier=1, # Often set to 1 for I/O bound tasks to prevent task hoarding
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    # Celery default retry policy for task decorator (can be overridden per task)
    task_autoretry_for=(HttpClientError, AuthenticationError,),
    task_retry_kwargs={'max_retries': 3, 'default_retry_delay': 60}, # default_retry_delay in seconds
    task_retry_backoff=True,
    task_retry_backoff_max=7200, # Max retry delay 2 hours
)

@celery_app.task(bind=True) # Removed autoretry from here to handle it more granularly below
def process_crawl_job_task(self: Task, job_message_dict: dict):
    logger.info(f"Received job message for task_id: {self.request.id}. Data: {job_message_dict}")

    try:
        job_message = JobMessage(**job_message_dict)
    except Exception as e:
        logger.error(f"Failed to parse JobMessage for task_id {self.request.id}: {job_message_dict}. Error: {e}", exc_info=True)
        # This is a malformed message, do not requeue. Send to a generic "malformed_messages_dlq" if possible, or just log.
        # Celery will ack it due to Reject or Ignore.
        raise Ignore() # Or Reject(requeue=False) - Ignore means task is marked as successful but did nothing.
                       # Reject might be better if a DLQ is configured at broker level for rejected messages.

    api_id = job_message.api_id
    logger.info(f"Processing api_id: {api_id} with params: {job_message.params} (Task ID: {self.request.id}, Attempt: {self.request.retries + 1})")

    api_config: Optional[ApiConfig] = None
    try:
        api_config = load_full_api_config(api_id)
        if not api_config:
            logger.error(f"No active API config found for api_id: {api_id}. Task ID: {self.request.id}. Sending to DLQ.")
            send_to_dlq(job_message, f"No active API config for api_id: {api_id}", settings.DEFAULT_DLQ_TOPIC)
            raise Ignore() # Successfully DLQ'd, ignore task.
            
    except Exception as e: 
        logger.error(f"DB or config loading error for {api_id} (Task ID: {self.request.id}): {e}", exc_info=True)
        try:
            # Retry for DB connection issues or transient load errors
            countdown = settings.task_retry_kwargs.get('default_retry_delay', 60) * (2 ** self.request.retries)
            countdown = min(countdown, settings.task_retry_backoff_max or 7200)
            raise self.retry(exc=e, countdown=countdown, max_retries=(settings.task_retry_kwargs.get('max_retries', 3)))
        except MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for DB/config load for {api_id}. Sending to DLQ. Task ID: {self.request.id}")
            send_to_dlq(job_message, f"Failed to load API config after max retries: {e}", settings.DEFAULT_DLQ_TOPIC)
            # Task has failed all retries, Celery will mark as FAILED.
        return # Stop processing this instance

    # At this point, api_config is guaranteed to be loaded.
    # Use api_config.max_retries for the job processing part, distinct from DB load retries.
    job_processing_max_retries = api_config.max_retries

    try:
        crawler = BaseCrawler(api_config, job_message)
        crawler.process_job() 
        logger.info(f"Successfully completed processing job for api_id: {api_id}, params: {job_message.params}. Task ID: {self.request.id}")

    except (HttpClientError, AuthenticationError) as e:
        logger.warning(f"Known recoverable error during job processing for {api_id} (Task ID: {self.request.id}): {type(e).__name__} - {e}. Attempt {self.request.retries + 1}/{job_processing_max_retries +1}.", exc_info=True)
        try:
            countdown = settings.task_retry_kwargs.get('default_retry_delay', 60) * (2 ** self.request.retries)
            countdown = min(countdown, settings.task_retry_backoff_max or 7200)
            # Use job_processing_max_retries from api_config for this type of error
            raise self.retry(exc=e, countdown=countdown, max_retries=job_processing_max_retries)
        except MaxRetriesExceededError:
            logger.error(f"Max retries ({job_processing_max_retries}) exceeded for job processing {api_id} due to {type(e).__name__}. Sending to DLQ. Task ID: {self.request.id}")
            send_to_dlq(job_message, f"Max retries for {type(e).__name__}: {e}", api_config.dlq_topic)
    
    except Ignore: # If BaseCrawler decided to ignore (e.g., after DLQing itself)
        logger.info(f"Task for {api_id} was ignored during processing. Task ID: {self.request.id}")
        # Celery handles Ignore by marking as success, no further action.

    except Exception as e:
        logger.critical(f"CRITICAL unhandled error in task for {api_id} (Task ID: {self.request.id}): {e}", exc_info=True)
        # For truly unexpected errors, attempt a limited number of retries with backoff
        # before sending to DLQ to avoid overwhelming DLQ for transient system issues.
        try:
            # Use a generic retry policy for these critical, unexpected errors
            countdown = settings.task_retry_kwargs.get('default_retry_delay', 120) * (2 ** self.request.retries)
            countdown = min(countdown, settings.task_retry_backoff_max or 7200) # Cap at 2 hours
            critical_error_max_retries = 2 # Fewer retries for truly unknown issues
            raise self.retry(exc=e, countdown=countdown, max_retries=critical_error_max_retries)
        except MaxRetriesExceededError:
            logger.error(f"Max retries for critical unhandled error for {api_id}. Sending to DLQ. Task ID: {self.request.id}")
            send_to_dlq(job_message, f"Critical unhandled error after retries: {e}", api_config.dlq_topic)

# Command to run worker:
# celery -A worker.src.tasks:celery_app worker -l INFO -c [concurrency_value]
# Example: celery -A worker.src.tasks:celery_app worker -l INFO -c 4 