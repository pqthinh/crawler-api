import mysql.connector
import mysql.connector.pooling # For connection pooling
import json
import logging
from typing import List, Optional, Dict, Any

from .settings import settings
from .models import ApiConfig, PaginationConfig, RateLimitConfig, PaginationStopCondition

logging.basicConfig(level=settings.LOG_LEVEL.upper())
logger = logging.getLogger(__name__)

# --- Database Connection Pool ---
_db_pool = None

def get_db_connection_pool():
    """Initializes and returns the MySQL connection pool."""
    global _db_pool
    if _db_pool is None:
        try:
            logger.info(f"Attempting to create MySQL connection pool for host: {settings.MYSQL_HOST} with pool size: {settings.MYSQL_CONNECTION_LIMIT}")
            _db_pool = mysql.connector.pooling.MySQLConnectionPool(
                pool_name="worker_db_pool",
                pool_size=settings.MYSQL_CONNECTION_LIMIT, 
                host=settings.MYSQL_HOST,
                port=settings.MYSQL_PORT,
                user=settings.MYSQL_USER,
                password=settings.MYSQL_PASSWORD,
                database=settings.MYSQL_DATABASE,
                # auth_plugin='mysql_native_password' # May be needed for some MySQL versions
            )
            logger.info("MySQL connection pool created successfully.")
        except mysql.connector.Error as err:
            logger.critical(f"CRITICAL: Error creating MySQL connection pool: {err}", exc_info=True)
            _db_pool = None # Ensure pool is None if creation fails
            raise # Re-raise to make the application aware of this critical failure
    return _db_pool

def get_db_connection():
    """Gets a connection from the pool."""
    pool = get_db_connection_pool() # This will initialize if needed, or raise if it fails
    if pool is None: # Should not happen if get_db_connection_pool raises on failure
        logger.critical("MySQL connection pool is None even after attempting initialization.")
        raise mysql.connector.Error("MySQL connection pool not available.")
    try:
        conn = pool.get_connection()
        logger.debug("Acquired DB connection from pool.")
        return conn
    except mysql.connector.Error as err:
        logger.error(f"Error getting DB connection from pool: {err}", exc_info=True)
        raise

# --- API Config Loading ---
def load_full_api_config(api_id: str) -> Optional[ApiConfig]:
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True) # Fetches rows as dictionaries

        # Select all relevant columns for the ApiConfig model
        query = f"""SELECT api_id, description, url, http_method, 
                         headers_json, params_json, body_template_json, default_context_json,
                         auth_type, auth_config_json, 
                         pagination_json, 
                         data_extraction_path_in_response,
                         rate_limit_json, timeout_sec, 
                         dest_table, dest_table_data_column,
                         dlq_topic, max_retries
                  FROM {settings.MYSQL_CONFIG_TABLE} 
                  WHERE api_id = %s AND is_active = TRUE"""
        cursor.execute(query, (api_id,))
        row = cursor.fetchone()

        if not row:
            logger.warning(f"No active API config found in DB for api_id: {api_id}")
            return None

        try:
            # Helper to parse JSON string fields from DB row, defaulting to empty dict or None
            def parse_json_field(json_string: Optional[str], default_val: Any = {}) -> Any:
                if json_string:
                    return json.loads(json_string)
                return default_val

            pagination_data = parse_json_field(row.get('pagination_json'), default_val={'type': 'none'})
            # The ApiConfig model's validator will handle creating a default PaginationConfig if type is 'none' and stop_condition is missing
            
            rate_limit_data = parse_json_field(row.get('rate_limit_json'), default_val=None)

            api_config_data = {
                'api_id': row['api_id'],
                'description': row.get('description'),
                'url': row['url'],
                'http_method': row['http_method'],
                'headers_json': parse_json_field(row.get('headers_json')),
                'params_json': parse_json_field(row.get('params_json')),
                'body_template_json': parse_json_field(row.get('body_template_json')),
                'default_context_json': parse_json_field(row.get('default_context_json')),
                'auth_type': row['auth_type'],
                'auth_config_json': parse_json_field(row.get('auth_config_json')),
                'pagination_json': pagination_data, # Pass the dict, Pydantic will parse
                'data_extraction_path_in_response': row.get('data_extraction_path_in_response'),
                'rate_limit_json': rate_limit_data, # Pass the dict or None
                'timeout_sec': row.get('timeout_sec', 30),
                'dest_table': row['dest_table'],
                'dest_table_data_column': row.get('dest_table_data_column', 'raw_data'),
                'dlq_topic': row.get('dlq_topic'),
                'max_retries': row.get('max_retries', 3)
            }
            api_config_obj = ApiConfig(**api_config_data)
            logger.info(f"Successfully loaded and parsed API config for: {api_id}")
            return api_config_obj
        
        except (json.JSONDecodeError, TypeError) as e: 
            logger.error(f"Error parsing JSON for api_id {api_id}: {e}. Row data: { {k: v for k, v in row.items() if isinstance(v, (str, bytes))} }", exc_info=True)
            return None
        except Exception as e: # Catch Pydantic validation errors or other unexpected issues
             logger.error(f"Validation or other error creating ApiConfig for {api_id}: {e}", exc_info=True)
             return None

    except mysql.connector.Error as err:
        logger.error(f"MySQL Error loading config for api_id {api_id}: {err}", exc_info=True)
        return None # Or re-raise depending on desired error handling for the Celery task
    except Exception as e:
        logger.error(f"Unexpected error in load_full_api_config for {api_id}: {e}", exc_info=True)
        return None
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            logger.debug(f"DB connection for api_id {api_id} loading closed/returned to pool.")

# --- Data Saving ---
def save_crawled_data(dest_table: str, data_column_name: str, data_list: List[Dict[str, Any]], api_id_for_logging: str) -> int:
    if not data_list:
        logger.info(f"No data to save for api_id: {api_id_for_logging}.")
        return 0

    conn = None
    cursor = None
    inserted_row_count = 0
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Basic validation for table and column names to prevent SQL injection risks.
        # For production, use a more robust method like whitelisting or an ORM.
        if not (dest_table and dest_table.replace('_','').isalnum() and 
                data_column_name and data_column_name.replace('_','').isalnum()):
            logger.error(f"Invalid destination table ('{dest_table}') or column ('{data_column_name}') name. Aborting save for {api_id_for_logging}.")
            return 0
        
        # Assuming the destination table has at least these columns:
        # - An auto-increment ID (e.g., 'id')
        # - The specified `data_column_name` (expected to be JSON type or TEXT)
        # - `api_id_ref` (VARCHAR, to link back to api_configs.api_id)
        # - `crawled_at` (TIMESTAMP, defaults to CURRENT_TIMESTAMP)
        # Example DDL for such a table (should be created manually or by a migration tool):
        # CREATE TABLE IF NOT EXISTS example_dest_table (
        #     id INT AUTO_INCREMENT PRIMARY KEY,
        #     raw_data JSON,
        #     api_id_ref VARCHAR(128),
        #     crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #     INDEX (api_id_ref) -- Optional: For faster lookups by api_id_ref
        # );

        sql_insert_query = f"INSERT INTO {mysql.connector.utils.quote_identifier(dest_table)} ({mysql.connector.utils.quote_identifier(data_column_name)}, api_id_ref) VALUES (%s, %s)"
        
        values_to_insert = []
        for item_data in data_list:
            try:
                json_string_data = json.dumps(item_data) 
                values_to_insert.append((json_string_data, api_id_for_logging))
            except TypeError:
                logger.warning(f"Data item is not JSON serializable for {api_id_for_logging}. Skipping item: {item_data}")
                continue
        
        if not values_to_insert:
            logger.warning(f"No valid JSON-serializable data items to insert for {api_id_for_logging}.")
            return 0

        cursor.executemany(sql_insert_query, values_to_insert)
        conn.commit()
        inserted_row_count = cursor.rowcount
        logger.info(f"Successfully saved {inserted_row_count} items to table '{dest_table}' for api_id: {api_id_for_logging}")

    except mysql.connector.Error as err:
        logger.error(f"MySQL Error saving data for {api_id_for_logging} to table '{dest_table}': {err}", exc_info=True)
        if conn: 
            try: conn.rollback(); logger.info(f"Transaction rolled back for {api_id_for_logging}.")
            except mysql.connector.Error as roll_err: logger.error(f"Error during rollback for {api_id_for_logging}: {roll_err}")
        inserted_row_count = -1 # Indicate error
    except Exception as e:
        logger.error(f"Unexpected error in save_crawled_data for {api_id_for_logging}: {e}", exc_info=True)
        inserted_row_count = -1 # Indicate error
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            logger.debug(f"DB connection for saving data (api_id: {api_id_for_logging}) closed/returned to pool.")
            
    return inserted_row_count 