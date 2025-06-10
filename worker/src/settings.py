import os
from typing import Optional, Dict, Any
from pydantic_settings import BaseSettings
from pydantic import model_validator # For Pydantic v2
from dotenv import load_dotenv

load_dotenv() # Load .env file if it exists

class Settings(BaseSettings):
    # MySQL Settings - Default values match your provided config
    MYSQL_HOST: str = "10.14.119.8"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "appuser"
    MYSQL_PASSWORD: str = "Admin123$"
    MYSQL_DATABASE: str = "crawler"
    MYSQL_CONFIG_TABLE: str = "api_configs"
    MYSQL_CONNECTION_LIMIT: int = 2048 # Usage for connection pool needs specific app-level code

    # Kafka Settings
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    CRAWL_JOBS_TOPIC: str = "crawl_jobs"       # Topic to consume jobs from
    CRAWL_RESULTS_TOPIC: str = "crawl_results" # Topic to publish results/logs to
    DEFAULT_DLQ_TOPIC: str = "crawl_jobs_dlq"  # Default DLQ for jobs that fail max_retries

    # Redis Settings
    REDIS_HOST: str = "10.14.119.8"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: Optional[str] = None # Will be populated from env if set (e.g., REDIS_PASSWORD="redis@6379")
    REDIS_DB: int = 0

    # Celery settings - URLs will be constructed if not explicitly set in env
    CELERY_BROKER_URL: Optional[str] = None
    CELERY_RESULT_BACKEND: Optional[str] = None
    CELERY_TIMEZONE: str = "UTC"
    CELERY_WORKER_CONCURRENCY: int = 4 # Default concurrency for worker, can be overridden by env or CLI

    LOG_LEVEL: str = "INFO"

    # This dictionary will store all environment variables. 
    # Secrets can be accessed via settings.API_SECRETS.get("YOUR_ENV_VAR_NAME")
    API_SECRETS: Dict[str, str] = {}

    @model_validator(mode='after')
    def _construct_urls_and_load_secrets(self) -> 'Settings':
        # Populate API_SECRETS with all current environment variables
        self.API_SECRETS = dict(os.environ)

        # Construct Redis URL for Celery if not explicitly provided
        redis_password_part = f":{self.REDIS_PASSWORD}@" if self.REDIS_PASSWORD else ""
        base_redis_url = f"redis://{redis_password_part}{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

        if self.CELERY_BROKER_URL is None:
            self.CELERY_BROKER_URL = base_redis_url
        
        if self.CELERY_RESULT_BACKEND is None:
            self.CELERY_RESULT_BACKEND = base_redis_url
        
        return self

settings = Settings() 