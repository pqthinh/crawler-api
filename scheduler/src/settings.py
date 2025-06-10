import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import model_validator # For Pydantic v2
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Load .env file if it exists (for local development)
load_dotenv()

class Settings(BaseSettings):
    MYSQL_HOST: str = "10.14.119.8"
    MYSQL_PORT: int = 3306
    MYSQL_USER: str = "appuser"
    MYSQL_PASSWORD: str = "Admin123$"
    MYSQL_DATABASE: str = "crawler"
    MYSQL_CONFIG_TABLE: str = "api_configs"
    MYSQL_CONNECTION_LIMIT: int = 2048 # Added from docker-compose, usage pending

    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"
    CRAWL_JOBS_TOPIC: str = "crawl_jobs"

    REDIS_HOST: str = "10.14.119.8"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: Optional[str] = None # Will be populated from env if set
    REDIS_DB: int = 0

    # Celery settings - will be constructed if not explicitly set in env
    CELERY_BROKER_URL: Optional[str] = None
    CELERY_RESULT_BACKEND: Optional[str] = None
    CELERY_TIMEZONE: str = "UTC"
    
    LOG_LEVEL: str = "INFO"
    
    @model_validator(mode='after')
    def set_celery_urls_if_not_set(self) -> 'Settings':
        # Construct Redis URL part with optional password
        # redis_auth = f":{self.REDIS_PASSWORD}@" if self.REDIS_PASSWORD else ""
        # base_redis_url = f"redis://{redis_auth}{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        redis_password = self.REDIS_PASSWORD
        redis_auth = f":{quote_plus(redis_password)}@" if redis_password else ""
        # base_redis_url = f"redis://{redis_auth}{self.get('REDIS_HOST')}:{self.get('REDIS_PORT')}/{self.get('REDIS_DB')}"
        base_redis_url = f"redis://{redis_auth}{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"

        if self.CELERY_BROKER_URL is None:
            self.CELERY_BROKER_URL = base_redis_url
        
        if self.CELERY_RESULT_BACKEND is None:
            self.CELERY_RESULT_BACKEND = base_redis_url
        
        return self

settings = Settings() 