# worker/src/base_crawler.py
import logging
import json # For parsing response content
# from string import Template # Basic templating, consider more robust if needed
from typing import Dict, Any, Optional, List, Tuple, Union # Added Union

from .models import ApiConfig, JobMessage
from .auth_handler import AuthHandler, AuthenticationError
from .http_client import HttpClient, HttpClientError
from .pagination_handler import PaginationHandler
from .data_processor import extract_data_from_response
from .db import save_crawled_data 
from .kafka_clients import send_next_page_job, send_crawl_result, send_to_dlq

logger = logging.getLogger(__name__)

class BaseCrawler:
    def __init__(self, api_config: ApiConfig, job_message: JobMessage):
        self.api_config = api_config
        self.job_message = job_message
        self.auth_handler = AuthHandler(api_config)
        self.http_client = HttpClient(api_config)

    def _resolve_placeholders(self, target: Union[str, Dict, List], context: Dict[str, Any]) -> Union[str, Dict, List]:
        """
        Recursively resolves placeholders in strings, dict values, or list elements.
        Placeholders are like {key} or {namespace.key}.
        Context keys should be flat, e.g., context["namespace.key"].
        """
        if isinstance(target, str):
            resolved_str = target
            # Iterate multiple times to handle nested placeholders to some extent, though true recursion is better.
            # A more robust solution would be a proper template engine for complex cases.
            for _ in range(3): # Allow a few passes for simple nested-like substitutions
                try:
                    # string.format_map is safer as it only substitutes existing keys if the map is a defaultdict
                    # or if all keys are present. For simple {} placeholders, this can work if context has all keys.
                    # Using a loop for simple replacement to avoid KeyError for missing keys easily.
                    temp_str = resolved_str
                    for key, value in context.items():
                        placeholder = f"{{{key}}}" # Assuming placeholders are {key} or {namespace.key}
                        temp_str = temp_str.replace(placeholder, str(value))
                    if temp_str == resolved_str and any(f"{{{k}}}" in temp_str for k in context.keys()):
                        # No change and still placeholders that *could* be substituted, break if no progress
                        pass # Or log if placeholders remain that are in context but not subbed (e.g. due to order)
                    resolved_str = temp_str
                    if not any(f"{{{k}}}" in resolved_str for k in context.keys()): # if no known placeholders left
                        break
                except Exception as e:
                    logger.error(f"Error during string placeholder resolution for '{target[:100]}': {e}")
                    # Keep partially resolved string or original on error
                    break 
            return resolved_str
        elif isinstance(target, dict):
            return {k: self._resolve_placeholders(v, context) for k, v in target.items()}
        elif isinstance(target, list):
            return [self._resolve_placeholders(elem, context) for elem in target]
        return target # Return as is if not str, dict, or list

    def _prepare_request_components(self) -> Tuple[str, Dict, Dict, Optional[Dict]]:
        """Prepares URL, headers, params, body by resolving placeholders."""
        
        active_context = (self.api_config.default_context_json or {}).copy()
        if self.job_message.params: # These are current page params like {"pageNo": 1}
            for k, v in self.job_message.params.items():
                active_context[f"pagination.{k}"] = v # Access as {pagination.pageNo}
        if self.job_message.context: # Job-specific context from scheduler
            active_context.update(self.job_message.context)

        logger.debug(f"Effective context for placeholder resolution ({self.api_config.api_id}): {active_context}")

        resolved_url = self._resolve_placeholders(self.api_config.url, active_context)
        resolved_headers = self._resolve_placeholders(self.api_config.headers_json or {}, active_context)
        resolved_params = self._resolve_placeholders(self.api_config.params_json or {}, active_context)
        resolved_body = self._resolve_placeholders(self.api_config.body_template_json or {}, active_context)
        
        # Ensure resolved_body is None if it results in an empty dictionary and method is GET/DELETE etc.
        # Or let HttpClient handle it if json_body is {} or None.
        if not resolved_body and self.api_config.http_method in ['GET', 'DELETE', 'HEAD']:
            final_resolved_body = None
        else:
            final_resolved_body = resolved_body if resolved_body else None

        return resolved_url, resolved_headers, resolved_params, final_resolved_body

    def process_job(self) -> None:
        logger.info(f"Processing job for api_id: {self.api_config.api_id}, job_params: {self.job_message.params}, context: {self.job_message.context}")
        response_content_dict: Optional[Dict[str, Any]] = None
        job_status = "INITIATED"
        error_message_for_result = None
        records_processed_count = 0

        try:
            resolved_url, resolved_config_headers, resolved_config_params, resolved_config_body = \
                self._prepare_request_components()

            auth_headers, auth_query_params, basic_auth_tuple = \
                self.auth_handler.get_auth_details_for_request()
            
            # HttpClient expects all resolved parts. It will merge auth details internally.
            response = self.http_client.request(
                method=self.api_config.http_method,
                url=resolved_url,
                resolved_headers=resolved_config_headers, # Base headers from config (resolved)
                resolved_params=resolved_config_params,   # Base params from config (resolved)
                resolved_json_body=resolved_config_body,  # Body from config (resolved)
                auth_headers=auth_headers,                # Auth-specific headers
                auth_query_params=auth_query_params,      # Auth-specific query params
                basic_auth_tuple=basic_auth_tuple         # Basic auth credentials
            )
            job_status = "API_CALLED_SUCCESS"
            
            try:
                response_content_dict = response.json()
            except json.JSONDecodeError as je:
                logger.error(f"JSONDecodeError for {self.api_config.api_id}. Status: {response.status_code}. Resp text: {response.text[:500]}. Err: {je}")
                job_status = "FAILURE_JSON_DECODE"
                error_message_for_result = f"JSONDecodeError: {je}"
                raise # Re-raise to be caught by the main try-except for Celery retry/DLQ

            extracted_data = extract_data_from_response(
                response_content_dict, 
                self.api_config.data_extraction_path_in_response,
                self.api_config.api_id
            )
            fetched_count = len(extracted_data)
            records_processed_count = fetched_count # For now, processed = fetched
            logger.info(f"Extracted {fetched_count} records for {self.api_config.api_id}, job_params: {self.job_message.params}")

            if extracted_data:
                saved_count = save_crawled_data(
                    dest_table=self.api_config.dest_table,
                    data_column_name=self.api_config.dest_table_data_column,
                    data_list=extracted_data,
                    api_id_for_logging=self.api_config.api_id
                )
                if saved_count < fetched_count and saved_count != -1: # saved_count can be -1 on error
                    logger.warning(f"Data extracted ({fetched_count}) but only {saved_count} saved for {self.api_config.api_id}.")
                    job_status = "PARTIAL_SUCCESS_SAVE"
                    error_message_for_result = error_message_for_result or "Partial data save."
                elif saved_count == -1:
                    job_status = "FAILURE_SAVE"
                    error_message_for_result = error_message_for_result or "Failed to save any data."
                else:
                    job_status = "SUCCESS_SAVE" # All extracted data saved
            elif self.api_config.data_extraction_path_in_response: # Path specified but no data
                job_status = "SUCCESS_NO_DATA_EXTRACTED"
                logger.info(f"No data extracted for {self.api_config.api_id} with path '{self.api_config.data_extraction_path_in_response}'.")
            else: # No extraction path, or no data which is fine
                job_status = "SUCCESS_NO_EXTRACTION_PATH"

            # Pagination must happen *after* current page processing (auth, call, extract, save)
            if self.api_config.pagination_json.type != 'none' and response_content_dict is not None:
                paginator = PaginationHandler(self.api_config, self.job_message.params)
                next_page_params = paginator.get_next_page_params(response_content_dict, fetched_items_count=fetched_count)

                if next_page_params:
                    next_job_msg = JobMessage(
                        api_id=self.api_config.api_id,
                        params=next_page_params,
                        context=self.job_message.context, 
                        current_retry=0 
                    )
                    send_next_page_job(next_job_msg)
                    logger.info(f"Sent next page job for {self.api_config.api_id}. Next params: {next_page_params}")
                else:
                    logger.info(f"All pages processed for {self.api_config.api_id} (job_params: {self.job_message.params}).")
                    job_status = f"{job_status}_ALL_PAGES_COMPLETE"
            else:
                logger.info(f"No pagination or no response content for pagination for {self.api_config.api_id}. Crawl for this job instance complete.")
                if self.api_config.pagination_json.type != 'none' and response_content_dict is None:
                     job_status = "FAILURE_NO_RESPONSE_FOR_PAGINATION"
                     error_message_for_result = error_message_for_result or "No response content to determine pagination."
                else:
                     job_status = f"{job_status}_NO_PAGINATION"
            
            # Final overall status for this job instance
            if "FAILURE" not in job_status and "PARTIAL" not in job_status:
                final_job_instance_status = "SUCCESS"
            elif "PARTIAL" in job_status:
                final_job_instance_status = "PARTIAL_SUCCESS"
            else:
                final_job_instance_status = "FAILURE"

            send_crawl_result(self.api_config.api_id, final_job_instance_status, 
                              records_count=records_processed_count if final_job_instance_status == "SUCCESS" else 0, 
                              error_message=error_message_for_result, 
                              details={"params": self.job_message.params, "final_state": job_status})

        except AuthenticationError as auth_err:
            logger.error(f"AuthError for {self.api_config.api_id}: {auth_err}", exc_info=True)
            send_crawl_result(self.api_config.api_id, "FAILURE_AUTH", error_message=str(auth_err), details={"params": self.job_message.params})
            raise # Re-raise for Celery task to handle (retry or DLQ)
        
        except HttpClientError as http_err:
            logger.error(f"HttpClientError for {self.api_config.api_id}: {http_err}", exc_info=True)
            send_crawl_result(self.api_config.api_id, "FAILURE_HTTP", error_message=str(http_err), 
                              details={"params": self.job_message.params, "status_code": http_err.status_code})
            raise 
            
        except Exception as e:
            logger.critical(f"Unhandled critical error in BaseCrawler for {self.api_config.api_id}: {e}", exc_info=True)
            send_crawl_result(self.api_config.api_id, "CRITICAL_ERROR_BASE_CRAWLER", error_message=str(e), details={"params": self.job_message.params})
            raise 