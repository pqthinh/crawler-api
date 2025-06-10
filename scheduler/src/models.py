from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Json # Json might not be needed directly here

class PaginationConfig(BaseModel):
    type: str # "page", "offset_limit", "cursor"
    # Fields to help construct the initial job message
    page_param_name_in_request: Optional[str] = None
    initial_page_number: Optional[int] = None
    page_size_param_name_in_request: Optional[str] = None # if scheduler needs to set initial page size
    page_size_value: Optional[int] = None
    
    offset_param_name_in_request: Optional[str] = None
    limit_param_name_in_request: Optional[str] = None
    limit_value: Optional[int] = None # Often the same as page_size_value for offset/limit
    initial_offset: Optional[int] = None
    
    cursor_param_name_in_request: Optional[str] = None
    initial_cursor_value: Optional[str] = None # If the first request needs a pre-defined cursor

class ApiConfigForScheduler(BaseModel):
    api_id: str
    schedule_cron: str
    # Storing as dict directly after DB read and JSON parsing
    default_context_json: Optional[Dict[str, Any]] = {}
    pagination_json: Optional[PaginationConfig] = None 
    # No need for other fields like url, auth_config for the scheduler's initial job creation

class JobMessage(BaseModel):
    api_id: str
    # Parameters for the *current* API call, specific to this job instance
    # e.g., {"page": 1}, {"offset": 0, "limit": 100}, {"cursor": "xyz"}
    params: Dict[str, Any] 
    # Contextual data that might be used for placeholders in URL, headers, body by the worker
    # This can be merged from api_config.default_context_json and job-specific context.
    context: Optional[Dict[str, Any]] = {}
    current_retry: int = 0
    # No need for other fields like next_url here, that's for worker-to-worker communication if any 