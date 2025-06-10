import mysql.connector
import json
import logging
from typing import List, Optional, Dict, Any
from .settings import settings
from .models import ApiConfigForScheduler, PaginationConfig

logging.basicConfig(level=settings.LOG_LEVEL.upper())
logger = logging.getLogger(__name__)

def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=settings.MYSQL_HOST,
            port=settings.MYSQL_PORT,
            user=settings.MYSQL_USER,
            password=settings.MYSQL_PASSWORD,
            database=settings.MYSQL_DATABASE
        )
        return conn
    except mysql.connector.Error as err:
        logger.error(f"Error connecting to MySQL: {err}")
        raise

def load_active_api_configs() -> List[ApiConfigForScheduler]:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True) # Fetches rows as dictionaries

        query = f"""
            SELECT 
                api_id, 
                schedule_cron,
                default_context_json,
                pagination_json
            FROM {settings.MYSQL_CONFIG_TABLE}
            WHERE is_active = TRUE AND schedule_cron IS NOT NULL AND schedule_cron != ''
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        api_configs = []
        for row in rows:
            try:
                # Parse JSON fields
                default_context = json.loads(row['default_context_json']) if row['default_context_json'] else {}
                pagination_data = json.loads(row['pagination_json']) if row['pagination_json'] else None
                
                pagination_conf_obj = None
                if pagination_data:
                    try:
                        pagination_conf_obj = PaginationConfig(**pagination_data)
                    except Exception as e:
                        logger.error(f"Failed to parse pagination_json for {row['api_id']}: {pagination_data}. Error: {e}")
                        # Decide if this API should be skipped or use default pagination
                        continue # Skip this API config if pagination is critical and malformed

                api_configs.append(
                    ApiConfigForScheduler(
                        api_id=row['api_id'],
                        schedule_cron=row['schedule_cron'],
                        default_context_json=default_context,
                        pagination_json=pagination_conf_obj
                    )
                )
            except json.JSONDecodeError as e:
                logger.error(f"JSONDecodeError for api_id {row['api_id']}: {e}. Skipping this config.")
            except Exception as e:
                logger.error(f"Error processing row for api_id {row['api_id']}: {e}. Skipping this config.")
        
        return api_configs
    except mysql.connector.Error as err:
        logger.error(f"MySQL Error in load_active_api_configs: {err}")
        return [] # Return empty list on DB error to prevent scheduler crash
    except Exception as e:
        logger.error(f"Unexpected error in load_active_api_configs: {e}")
        return []
    finally:
        if conn and conn.is_connected():
            # cursor might not be defined if get_db_connection failed before cursor assignment
            if 'cursor' in locals() and cursor is not None:
                 cursor.close()
            conn.close() 