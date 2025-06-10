# worker/src/pagination_handler.py
import logging
from typing import Optional, Dict, Any, List
from jsonpath_ng import parse as jsonpath_parse # Using jsonpath_ng for JSONPath evaluation

from .models import ApiConfig, PaginationConfig, PaginationStopCondition

logger = logging.getLogger(__name__)

class PaginationHandler:
    def __init__(self, api_config: ApiConfig, current_job_params: Dict[str, Any]):
        self.api_config = api_config
        # api_config.pagination_json is already a PaginationConfig object due to Pydantic model validation
        self.pagination_config: PaginationConfig = api_config.pagination_json
        self.current_job_params = current_job_params # Params used for the *current* request that yielded the response

    def _evaluate_jsonpath(self, response_data: Dict[str, Any], path_expression: Optional[str]) -> List[Any]:
        if not path_expression:
            logger.debug(f"JSONPath expression is empty for {self.api_config.api_id}, returning empty list.")
            return []
        try:
            jsonpath_expr = jsonpath_parse(path_expression)
            matches = [match.value for match in jsonpath_expr.find(response_data)]
            logger.debug(f"JSONPath '{path_expression}' on {self.api_config.api_id} found {len(matches)} matches.")
            return matches
        except Exception as e:
            # Log specific error, e.g., if jsonpath_parse fails or find causes an issue
            logger.error(f"Error evaluating JSONPath '{path_expression}' for api_id {self.api_config.api_id}: {e}", exc_info=True)
            return [] # Return empty on error to prevent crash, stopping pagination might be a side effect

    def _check_stop_condition(self, response_data: Dict[str, Any], fetched_items_count: Optional[int] = None) -> bool:
        # If type is 'none', pagination_json.stop_condition will be None due to model validator.
        if self.pagination_config.type == 'none' or self.pagination_config.stop_condition is None:
            logger.debug(f"Pagination type is 'none' or no stop condition for {self.api_config.api_id}. Stopping pagination.")
            return True 

        stop_cond: PaginationStopCondition = self.pagination_config.stop_condition
        
        if stop_cond.type == "empty_on_path":
            if not stop_cond.items_path_in_response:
                logger.warning(f"Stop condition 'empty_on_path' for {self.api_config.api_id} but items_path_in_response is not defined. Stopping.")
                return True
            items = self._evaluate_jsonpath(response_data, stop_cond.items_path_in_response)
            if not items: 
                logger.info(f"Stop condition 'empty_on_path' met for {self.api_config.api_id} at path '{stop_cond.items_path_in_response}'.")
                return True
            return False

        elif stop_cond.type == "count_less_than_page_size":
            actual_count = -1 # Default to a value that would not satisfy less_than_page_size unless updated
            if fetched_items_count is not None:
                actual_count = fetched_items_count
            elif stop_cond.items_path_in_response:
                 items_on_path = self._evaluate_jsonpath(response_data, stop_cond.items_path_in_response)
                 # Assuming items_path_in_response correctly targets the list of items.
                 actual_count = len(items_on_path[0]) if items_on_path and isinstance(items_on_path[0], list) else len(items_on_path)
            else:
                logger.warning(f"Stop 'count_less_than_page_size' for {self.api_config.api_id} has no items_path and no fetched_items_count. Stopping.")
                return True

            page_size = self.pagination_config.page_size_value
            if page_size is None:
                logger.warning(f"Stop 'count_less_than_page_size' for {self.api_config.api_id} but page_size_value is not set. Stopping.")
                return True 
            
            if actual_count < page_size:
                logger.info(f"Stop 'count_less_than_page_size' met for {self.api_config.api_id}. Fetched: {actual_count}, PageSize: {page_size}.")
                return True
            return False

        elif stop_cond.type == "specific_value_on_path":
            if not stop_cond.value_path_in_response or stop_cond.expected_value is None:
                logger.warning(f"Stop 'specific_value_on_path' for {self.api_config.api_id} missing value_path or expected_value. Stopping.")
                return True
            
            values = self._evaluate_jsonpath(response_data, stop_cond.value_path_in_response)
            # Check if any of the found values match the expected value
            if values and any(val == stop_cond.expected_value for val in values):
                logger.info(f"Stop 'specific_value_on_path' met for {self.api_config.api_id}. Path '{stop_cond.value_path_in_response}' matched '{stop_cond.expected_value}'.")
                return True
            return False
            
        elif stop_cond.type == "no_next_cursor_on_path":
            if not self.pagination_config.cursor_path_in_response:
                 logger.warning(f"Stop 'no_next_cursor_on_path' for {self.api_config.api_id} but cursor_path_in_response not defined. Stopping.")
                 return True
            
            next_cursor_values = self._evaluate_jsonpath(response_data, self.pagination_config.cursor_path_in_response)
            if not next_cursor_values or next_cursor_values[0] is None or str(next_cursor_values[0]).strip() == "":
                logger.info(f"Stop 'no_next_cursor_on_path' met for {self.api_config.api_id}. No next cursor at '{self.pagination_config.cursor_path_in_response}'.")
                return True
            return False

        logger.warning(f"Unknown stop_condition.type '{stop_cond.type}' for {self.api_config.api_id}. Defaulting to stop.")
        return True


    def get_next_page_params(self, response_data: Dict[str, Any], fetched_items_count: Optional[int] = None) -> Optional[Dict[str, Any]]:
        if self._check_stop_condition(response_data, fetched_items_count):
            return None # Stop pagination

        next_params = self.current_job_params.copy()
        p_config = self.pagination_config

        if p_config.type == "page":
            if not p_config.page_param_name_in_request:
                logger.error(f"Page pagination for {self.api_config.api_id} misconfigured: missing page_param_name_in_request.")
                return None
            current_page_val = self.current_job_params.get(p_config.page_param_name_in_request, p_config.initial_page_number or 1)
            try:
                current_page = int(current_page_val)
            except ValueError:
                logger.error(f"Current page value '{current_page_val}' for {self.api_config.api_id} is not an integer. Cannot paginate.")
                return None
            increment = p_config.increment_step or 1
            next_params[p_config.page_param_name_in_request] = current_page + increment
            if p_config.page_size_param_name_in_request and p_config.page_size_value is not None: # Keep page size consistent
                next_params[p_config.page_size_param_name_in_request] = p_config.page_size_value


        elif p_config.type == "offset_limit":
            if not p_config.offset_param_name_in_request or p_config.page_size_value is None: # page_size_value acts as the limit increment
                logger.error(f"Offset/Limit pagination for {self.api_config.api_id} misconfigured: missing offset_param or page_size_value (for limit amount).")
                return None
            
            current_offset_val = self.current_job_params.get(p_config.offset_param_name_in_request, p_config.initial_offset or 0)
            try:
                current_offset = int(current_offset_val)
            except ValueError:
                logger.error(f"Current offset value '{current_offset_val}' for {self.api_config.api_id} is not an integer. Cannot paginate.")
                return None
            
            next_params[p_config.offset_param_name_in_request] = current_offset + p_config.page_size_value
            if p_config.limit_param_name_in_request: # Ensure limit param is also in next_params
                next_params[p_config.limit_param_name_in_request] = p_config.page_size_value


        elif p_config.type == "cursor":
            if not p_config.cursor_param_name_in_request or not p_config.cursor_path_in_response:
                logger.error(f"Cursor pagination for {self.api_config.api_id} misconfigured: missing cursor_param_req or cursor_path_resp.")
                return None

            next_cursor_values = self._evaluate_jsonpath(response_data, p_config.cursor_path_in_response)
            # Stop condition should have already caught this, but as a safeguard:
            if not next_cursor_values or next_cursor_values[0] is None or str(next_cursor_values[0]).strip() == "":
                logger.info(f"No valid next cursor found for {self.api_config.api_id} at path '{p_config.cursor_path_in_response}' during param generation. Stopping.")
                return None 
            next_params[p_config.cursor_param_name_in_request] = next_cursor_values[0]
        
        else: # 'none' or unknown type
            logger.debug(f"Pagination type is '{p_config.type}'. No next page parameters generated by handler for {self.api_config.api_id}.")
            return None 

        logger.info(f"Generated next page parameters for {self.api_config.api_id}: {next_params}")
        return next_params 