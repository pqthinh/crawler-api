# worker/src/http_client.py
import logging
import time
import requests
from requests.auth import HTTPBasicAuth
from typing import Optional, Dict, Any, Tuple
from ratelimit import limits, RateLimitException # Using the basic decorator for now

from .models import ApiConfig # For RateLimitConfig and timeout_sec

logger = logging.getLogger(__name__)

class HttpClientError(Exception):
    """Custom exception for HTTP client errors after retries."""
    def __init__(self, message, status_code=None, response_text=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text

class HttpClient:
    def __init__(self, api_config: ApiConfig):
        self.api_config = api_config
        self.timeout = api_config.timeout_sec
        self._configure_rate_limiter()

    def _configure_rate_limiter(self):
        self.rate_limited_session_request = None
        if self.api_config.rate_limit_json:
            try:
                calls_per_period = self.api_config.rate_limit_json.limit
                period_seconds = self.api_config.rate_limit_json.period
                
                # Define the actual request function that will be rate-limited
                def actual_request_func(session_obj, method, url, **kwargs):
                    return session_obj.request(method=method, url=url, **kwargs)
                
                # Apply the decorator
                self.rate_limited_session_request = limits(calls=calls_per_period, period=period_seconds)(actual_request_func)
                logger.info(f"Rate limiter configured for {self.api_config.api_id}: {calls_per_period} calls / {period_seconds}s")
            except Exception as e:
                logger.error(f"Failed to configure dynamic rate limiter for {self.api_config.api_id} from {self.api_config.rate_limit_json}: {e}. Proceeding without explicit rate limiting.")
        else:
            logger.debug(f"No rate limit configured for {self.api_config.api_id}")

    def _execute_request_with_session(self,
        session: requests.Session,
        method: str, 
        url: str, 
        request_kwargs: Dict[str, Any] 
    ) -> requests.Response:
        """Executes the request using the session, applying rate limiting if configured."""
        if self.rate_limited_session_request:
            logger.debug(f"Executing rate-limited request for {self.api_config.api_id} to {url} via instance method.")
            try:
                # Pass the session object as the first argument to the decorated function
                response = self.rate_limited_session_request(session, method, url, **request_kwargs)
            except RateLimitException as rle:
                logger.warning(f"Rate limit hit for {self.api_config.api_id}. Sleeping for {rle.period_remaining:.2f}s...")
                time.sleep(rle.period_remaining)
                logger.info(f"Retrying request for {self.api_config.api_id} after rate limit sleep (non-rate-limited attempt for this retry).")
                # Retry once without rate limiting for this specific attempt after sleep
                response = session.request(method=method, url=url, **request_kwargs)
        else:
            logger.debug(f"Executing non-rate-limited request for {self.api_config.api_id} to {url}")
            response = session.request(method=method, url=url, **request_kwargs)
        return response

    def request(
        self, 
        method: str, 
        url: str, 
        # Placeholders in headers/params/body should be resolved before this call
        resolved_headers: Optional[Dict[str, str]] = None, 
        resolved_params: Optional[Dict[str, Any]] = None,
        resolved_json_body: Optional[Dict[str, Any]] = None,
        # Auth details obtained from AuthHandler
        auth_headers: Optional[Dict[str, str]] = None, 
        auth_query_params: Optional[Dict[str, Any]] = None, 
        basic_auth_tuple: Optional[Tuple[str,str]] = None 
    ) -> requests.Response:
        
        final_headers = (self.api_config.headers_json or {}).copy()
        if resolved_headers: final_headers.update(resolved_headers)
        if auth_headers: final_headers.update(auth_headers)

        final_params = (self.api_config.params_json or {}).copy() 
        if resolved_params: final_params.update(resolved_params)
        if auth_query_params: final_params.update(auth_query_params)

        # Ensure no None values are passed for params/headers/json to requests library
        final_headers = {k: v for k, v in final_headers.items() if v is not None}
        final_params = {k: v for k, v in final_params.items() if v is not None}
        # resolved_json_body can be None or an empty dict if no body
        
        request_kwargs = {
            "headers": final_headers if final_headers else None,
            "params": final_params if final_params else None,
            "json": resolved_json_body,
            "timeout": self.timeout,
            "auth": HTTPBasicAuth(basic_auth_tuple[0], basic_auth_tuple[1]) if basic_auth_tuple else None
        }
        # Filter out top-level None kwargs to avoid issues with requests library
        request_kwargs = {k: v for k, v in request_kwargs.items() if v is not None}

        logger.debug(f"Requesting {method} {url} for {self.api_config.api_id} with effective kwargs: {request_kwargs}")

        http_level_max_retries = 2 # Max HTTP-level retries (total 3 attempts)
        base_backoff_factor = 1  # seconds

        with requests.Session() as session:
            for attempt in range(http_level_max_retries + 1):
                try:
                    response = self._execute_request_with_session(
                        session,
                        method,
                        url,
                        request_kwargs
                    )
                    
                    # Check for 4xx/5xx errors to decide on retrying or failing
                    if response.status_code >= 400:
                        logger.warning(f"HTTP Error for {self.api_config.api_id} to {url}: {response.status_code} {response.reason}. Attempt {attempt + 1}/{http_level_max_retries + 1}")
                        if response.status_code in [401, 403]: # Definite auth errors, no retry
                             logger.error(f"Authorization error ({response.status_code}) for {self.api_config.api_id}. No retry. Resp: {response.text[:500]}")
                             raise HttpClientError(f"Authorization error: {response.status_code}", response.status_code, response.text)
                        
                        # Retry on 429 (Too Many Requests) or 5xx server errors
                        if response.status_code == 429 or response.status_code >= 500:
                            if attempt < http_level_max_retries:
                                sleep_duration = base_backoff_factor * (2 ** attempt) 
                                logger.info(f"Retrying HTTP request for {self.api_config.api_id} in {sleep_duration}s due to {response.status_code}...")
                                time.sleep(sleep_duration)
                                continue # Next attempt
                            else:
                                logger.error(f"Max HTTP retries ({http_level_max_retries + 1}) reached for {url}. Last status: {response.status_code}")
                                raise HttpClientError(f"Max HTTP retries. Last status: {response.status_code}", response.status_code, response.text)
                        else: # Other 4xx errors (e.g., 400, 404), do not retry at this level
                            logger.error(f"Client error {response.status_code} for {url}. No HTTP retry. Resp: {response.text[:500]}")
                            raise HttpClientError(f"Client error: {response.status_code}", response.status_code, response.text)
                    
                    logger.info(f"HTTP {method} to {url} for {self.api_config.api_id} successful with status {response.status_code}.")
                    return response # Successful response

                except requests.exceptions.Timeout as e:
                    logger.warning(f"Request timeout for {url}. Attempt {attempt + 1}/{http_level_max_retries + 1}", exc_info=True)
                    if attempt < http_level_max_retries:
                        time.sleep(base_backoff_factor * (2**attempt))
                        continue
                    raise HttpClientError(f"Request timeout after {http_level_max_retries + 1} attempts for {url}. Error: {e}", status_code=408)
                except requests.exceptions.RequestException as e: # Other network errors like ConnectionError
                    logger.warning(f"Request failed for {url}: {e}. Attempt {attempt + 1}/{http_level_max_retries + 1}", exc_info=True)
                    if attempt < http_level_max_retries:
                        time.sleep(base_backoff_factor * (2**attempt))
                        continue
                    raise HttpClientError(f"Request failed after {http_level_max_retries + 1} attempts for {url}. Error: {e}")
                # Catch HttpClientError explicitly to prevent it from being caught by the generic Exception below if re-raised from _execute_request_with_session
                except HttpClientError:
                    raise # Re-raise it as it's already processed/logged appropriately
                except Exception as e: # Catch-all for unexpected errors within the loop
                    logger.error(f"Unexpected error during HTTP request attempt {attempt + 1} for {url}: {e}", exc_info=True)
                    if attempt < http_level_max_retries:
                        time.sleep(base_backoff_factor * (2**attempt))
                        continue
                    raise HttpClientError(f"Unexpected error after {http_level_max_retries + 1} attempts for {url}. Last error: {e}")
            
            # Fallback, though ideally should not be reached if all conditions are handled
            raise HttpClientError(f"Request to {url} failed after all retries due to an unhandled reason.") 