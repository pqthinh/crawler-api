import logging
from celery import Celery
from celery.schedules import crontab
import json

from .settings import settings
from .db import load_active_api_configs
from .kafka_producer import send_job_to_kafka, get_kafka_producer
from .models import JobMessage, ApiConfigForScheduler, PaginationConfig

# Configure logging based on settings
logging.basicConfig(level=settings.LOG_LEVEL.upper())
logger = logging.getLogger(__name__)

# Initialize Celery
# The Celery app name can be anything, 'scheduler_app' is descriptive.
celery_app = Celery('scheduler_app', broker=settings.CELERY_BROKER_URL)
celery_app.conf.update(
    result_backend=settings.CELERY_RESULT_BACKEND,
    timezone=settings.CELERY_TIMEZONE,
    # beat_schedule_filename='celerybeat-schedule', # If using a file-based persistent scheduler, ensure volume mount
)

def get_initial_params(api_config: ApiConfigForScheduler) -> dict:
    """
    Constructs the initial parameters for a job based on pagination config.
    """
    params = {}
    if not api_config.pagination_json:
        logger.debug(f"No pagination config for {api_config.api_id}, initial params are empty.")
        return params

    p_config = api_config.pagination_json
    logger.debug(f"Generating initial params for {api_config.api_id} with pagination type: {p_config.type}")
    
    if p_config.type == "page":
        if p_config.page_param_name_in_request and p_config.initial_page_number is not None:
            params[p_config.page_param_name_in_request] = p_config.initial_page_number
        if p_config.page_size_param_name_in_request and p_config.page_size_value is not None:
            params[p_config.page_size_param_name_in_request] = p_config.page_size_value
    elif p_config.type == "offset_limit":
        if p_config.offset_param_name_in_request and p_config.initial_offset is not None:
            params[p_config.offset_param_name_in_request] = p_config.initial_offset
        # Use limit_value from pagination_json for the limit parameter in the request
        if p_config.limit_param_name_in_request and p_config.limit_value is not None:
            params[p_config.limit_param_name_in_request] = p_config.limit_value
    elif p_config.type == "cursor":
        if p_config.cursor_param_name_in_request and p_config.initial_cursor_value is not None:
            params[p_config.cursor_param_name_in_request] = p_config.initial_cursor_value
            
    logger.debug(f"Initial params for {api_config.api_id}: {params}")
    return params


@celery_app.task(name="scheduler.dispatch_crawl_job")
def dispatch_crawl_job(api_id: str, initial_params: dict, default_context: dict):
    """
    Celery task that constructs and sends the initial job message to Kafka.
    This task is triggered by Celery Beat.
    """
    logger.info(f"Dispatching crawl job for api_id: {api_id} with initial_params: {initial_params}")
    
    job_msg = JobMessage(
        api_id=api_id,
        params=initial_params, 
        context=default_context if default_context else {},
        current_retry=0
    )
    
    try:
        send_job_to_kafka(settings.CRAWL_JOBS_TOPIC, job_msg.model_dump())
        logger.info(f"Successfully sent initial job for api_id: {api_id} to Kafka topic {settings.CRAWL_JOBS_TOPIC}")
    except Exception as e:
        logger.error(f"Failed to send job for api_id {api_id} to Kafka: {e}", exc_info=True)


@celery_app.task(name="scheduler.internal_dispatch_to_kafka") # Đổi tên để rõ ràng hơn
def internal_dispatch_to_kafka(api_id: str, initial_params: dict, default_context: dict):
    logger.info(f"Scheduler task: Preparing JobMessage for api_id: {api_id}")
    
    job_msg_obj = JobMessage( # Tạo Pydantic model
        api_id=api_id,
        params=initial_params,
        context=default_context if default_context else {},
        current_retry=0
    )
    
    job_msg_dict = job_msg_obj.model_dump() # Chuyển thành dict để gửi đi

    try:
        # Gửi trực tiếp message (dict) lên Kafka topic mà worker sẽ lắng nghe
        send_job_to_kafka(settings.CRAWL_JOBS_TOPIC, job_msg_dict) # Hàm này của kafka_producer
        logger.info(f"Scheduler task: Successfully sent JobMessage for api_id: {api_id} to Kafka topic {settings.CRAWL_JOBS_TOPIC}")
    except Exception as e:
        logger.error(f"Scheduler task: Failed to send JobMessage for api_id {api_id} to Kafka: {e}", exc_info=True)
        # Có thể retry task này của scheduler nếu gửi Kafka thất bại
        raise # Để Celery Beat biết task này lỗi và có thể retry theo cấu hình của Beat



def setup_periodic_tasks():
    logger.info("Attempting to set up periodic tasks for Celery Beat...")
    try:
        get_kafka_producer() # Initialize Kafka producer early if it has global state or connection logic
        
        active_configs = load_active_api_configs()
        if not active_configs:
            logger.warning("No active API configs found in DB to schedule.")
            celery_app.conf.beat_schedule = {}
            return

        logger.info(f"Loaded {len(active_configs)} active API configs from DB.")
        schedule = {}
        for config in active_configs:
            if not config.schedule_cron or not config.schedule_cron.strip():
                logger.warning(f"API config {config.api_id} is active but has no valid schedule_cron. Skipping.")
                continue

            try:
                cron_parts = config.schedule_cron.strip().split()
                if len(cron_parts) != 5:
                     logger.error(f"Invalid cron string for {config.api_id}: '{config.schedule_cron}'. Must have 5 parts (m h dom mon dow). Skipping.")
                     continue
                
                initial_job_params = get_initial_params(config)
                effective_default_context = config.default_context_json if config.default_context_json else {}
                
                # task_name = f"crawl-{config.api_id}" # Unique name for the scheduled task
                # schedule[task_name] = {
                #     'task': 'scheduler.dispatch_crawl_job',
                #     'schedule': crontab(*cron_parts),
                #     'args': (config.api_id, initial_job_params, effective_default_context),
                #     'options': {
                #         # 'expires': 60, # Optional: task expires in 60 seconds if not picked up
                #     }
                # }
                task_name = f"schedule_kafka_job_for_{config.api_id}" # Tên schedule entry
                schedule[task_name] = {
                    'task': 'scheduler.internal_dispatch_to_kafka', # Tên task của scheduler
                    'schedule': crontab(*cron_parts),
                    'args': (config.api_id, initial_job_params, effective_default_context),
                }
                logger.info(f"Scheduled Kafka dispatch for api_id '{config.api_id}' with cron: '{config.schedule_cron}'")
                logger.info(f"Scheduled task '{task_name}' for api_id '{config.api_id}' with cron: '{config.schedule_cron}'")
            except ValueError as ve:
                logger.error(f"Invalid crontab format for api_id {config.api_id} ('{config.schedule_cron}'): {ve}. Skipping.")
            except Exception as e:
                logger.error(f"Error setting up schedule for api_id {config.api_id}: {e}. Skipping.", exc_info=True)
        
        celery_app.conf.beat_schedule = schedule
        if not schedule:
            logger.warning("Celery Beat schedule is empty after processing all configs.")
        else:
            logger.info(f"Celery Beat schedule configured with {len(schedule)} tasks.")
            # Using json.dumps with default=str for complex objects like crontab if direct print fails
            logger.debug(f"Final Beat schedule: {json.dumps(celery_app.conf.beat_schedule, default=str)}")

    except Exception as e:
        logger.critical(f"CRITICAL: Failed to load API configs or setup Kafka producer for periodic tasks: {e}", exc_info=True)
        # If this fails, Beat might start with an empty or stale schedule.
        # Ensure this path is robust.
        celery_app.conf.beat_schedule = {} # Ensure it's at least an empty dict

# This function is called when the module is loaded by Celery Beat.
# This dynamically configures the beat_schedule.
setup_periodic_tasks()

logger.info(f"Scheduler Celery app '{celery_app.main}' initialized. Broker: {celery_app.conf.broker_url}")
if celery_app.conf.beat_schedule:
    logger.info(f"Celery Beat schedule contains {len(celery_app.conf.beat_schedule)} tasks.")
else:
    logger.warning("Celery Beat schedule is currently empty or setup failed.")

# To run Celery Beat (example, command will be in docker-compose.yml):
# celery -A scheduler.src.app beat -l info --pidfile=/tmp/celerybeat.pid
# Ensure the -A points to this app instance, e.g., module.app_object
# If scheduler/src is in PYTHONPATH, then `scheduler.src.app` should work.
# In Docker, WORKDIR is /app, scheduler code is in /app/src. So module path is src.app 