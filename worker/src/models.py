from typing import Optional, Dict, Any, List, Union
from pydantic import BaseModel, field_validator, HttpUrl

# Re-using JobMessage from scheduler or defining it again if there are worker-specific fields
class JobMessage(BaseModel):
    api_id: str
    params: Dict[str, Any] # Current parameters for this specific API call (e.g., page number, offset)
    context: Optional[Dict[str, Any]] = {} # Dynamic context from scheduler or previous steps for placeholders
    current_retry: int = 0
    # Worker might internally add/use more fields if a job is re-queued for pagination (e.g. next_page_token)

# --- Specific Auth Config Models (to be used based on ApiConfig.auth_type) ---
class AuthAPIKeyConfig(BaseModel):
    key_location: str # "header" or "query"
    key_name_in_request: str # Name of the header or query parameter
    key_value_env: str     # Environment variable name that holds the API key, e.g., "API_SECRET_XYZ"

class AuthBasicConfig(BaseModel):
    username_env: str # Environment variable name for username
    password_env: str # Environment variable name for password

class AuthOAuth2ClientCredentialsConfig(BaseModel):
    token_url: HttpUrl
    client_id_env: str
    client_secret_env: str
    scopes: Optional[List[str]] = []
    token_header_name: str = "Authorization" # Header name for sending the Bearer token
    grant_type: str = "client_credentials"
    additional_token_params: Optional[Dict[str, Any]] = {} # e.g. {"resource": "some_resource_url"}

# --- Pagination Models ---
class PaginationStopCondition(BaseModel):
    type: str # "empty_on_path", "count_less_than_page_size", "specific_value_on_path", "no_next_cursor_on_path"
    items_path_in_response: Optional[str] = None # JSONPath to the list of items or main data array
    value_path_in_response: Optional[str] = None # JSONPath to a specific field for value check
    expected_value: Optional[Any] = None         # Value to check against for "specific_value_on_path"
    # For "no_next_cursor_on_path", items_path_in_response would point to where the next cursor should be.
    # Worker will check if this path exists and is non-null/non-empty.

class PaginationConfig(BaseModel):
    type: str # "page", "offset_limit", "cursor", or "none"
    
    page_param_name_in_request: Optional[str] = None
    initial_page_number: Optional[int] = 1
    page_size_param_name_in_request: Optional[str] = None
    page_size_value: Optional[int] = None # Fixed page size for this API config
    increment_step: Optional[int] = 1
    
    offset_param_name_in_request: Optional[str] = None
    limit_param_name_in_request: Optional[str] = None
    # limit_value is often the same as page_size_value for offset/limit pagination type
    # This would be set in page_size_value if that's the case and type is offset_limit
    initial_offset: Optional[int] = 0
    
    cursor_param_name_in_request: Optional[str] = None
    cursor_path_in_response: Optional[str] = None # JSONPath to get the next cursor from the API response
    initial_cursor_value: Optional[str] = None    # Optional, if the first request needs a predefined cursor

    stop_condition: Optional[PaginationStopCondition] = None # Mandatory for paginated types

# --- Rate Limit Model ---
class RateLimitConfig(BaseModel):
    limit: int  # Number of requests allowed
    period: int # Time period in seconds

# --- Main API Config Model for Worker ---
class ApiConfig(BaseModel):
    api_id: str
    description: Optional[str] = None
    url: str # URL template, can contain {context.field} or {config.default_context.field}
    http_method: str = 'GET'
    
    # These are templates. Placeholders like {context.some_val} or {pagination.current_page} will be resolved by worker.
    headers_json: Optional[Dict[str, str]] = {}
    params_json: Optional[Dict[str, Any]] = {} 
    body_template_json: Optional[Dict[str, Any]] = {} 
    default_context_json: Optional[Dict[str, Any]] = {}

    auth_type: str = 'none' # 'none', 'api_key', 'basic', 'oauth2_cc'
    # auth_config_json will store the raw JSON dict from the DB column `auth_config`
    auth_config_json: Optional[Dict[str, Any]] = {} 

    # pagination_json from DB will be parsed into this model
    pagination_json: PaginationConfig 
    
    data_extraction_path_in_response: Optional[str] = None # JSONPath, e.g., "data.items[*]" or "results"

    # rate_limit_json from DB will be parsed into this model
    rate_limit_json: Optional[RateLimitConfig] = None 
    timeout_sec: int = 30 # Default timeout for HTTP requests
    
    dest_table: str # MySQL table name to store crawled data
    dest_table_data_column: str = 'raw_data' # Column name in dest_table for the JSON data

    # is_active: bool = True # Worker assumes job is for active config; scheduler handles filtering
    
    dlq_topic: Optional[str] = None # Specific DLQ topic for this API, else worker uses settings.DEFAULT_DLQ_TOPIC
    max_retries: int = 3 # Max retries for a single job instance (e.g. one page fetch)

    @field_validator('http_method')
    def method_must_be_valid(cls, v):
        normalized_method = v.upper()
        if normalized_method not in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS']:
            raise ValueError(f'Invalid HTTP method: {v}')
        return normalized_method

    @field_validator('pagination_json', mode='before') # Handle if pagination_json is None from DB for type 'none'
    def ensure_pagination_config_for_none_type(cls, v, values):
        # If pagination_json is None in DB (meaning type 'none'), create a default PaginationConfig for it.
        # This is because ApiConfig expects a PaginationConfig object.
        # Or, the DB schema should ensure pagination_json always has at least `{"type": "none"}`.
        if v is None:
            return PaginationConfig(type='none', stop_condition=None) # stop_condition is Optional
        if isinstance(v, dict) and v.get('type') == 'none' and 'stop_condition' not in v:
             v['stop_condition'] = None # Ensure stop_condition is None if not provided for type 'none'
        return v

    # Further validation, e.g. ensuring stop_condition exists if type is not 'none'
    @field_validator('pagination_json')
    def check_stop_condition_for_pagination(cls, v: PaginationConfig):
        if v.type != 'none' and v.stop_condition is None:
            raise ValueError(f"stop_condition is required for pagination type '{v.type}'")
        return v 