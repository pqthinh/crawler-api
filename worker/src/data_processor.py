# worker/src/data_processor.py
import logging
from typing import List, Dict, Any, Optional
from jsonpath_ng import parse as jsonpath_parse

logger = logging.getLogger(__name__)

def extract_data_from_response(
    response_data: Dict[str, Any], 
    extraction_path: Optional[str],
    api_id_for_log: str
) -> List[Dict[str, Any]]:
    """
    Extracts data from the API response using a JSONPath expression.
    The result is expected to be a list of dictionaries (records).
    If extraction_path is None or empty, the entire response_data is returned as a single item list if it's a dict.
    """
    if not extraction_path:
        logger.debug(f"No data_extraction_path for {api_id_for_log}. Returning entire response if it's a list, or list-wrapped dict.")
        if isinstance(response_data, list):
            # Ensure all items in the list are dictionaries for consistency
            processed_list = []
            for i, item in enumerate(response_data):
                if isinstance(item, dict):
                    processed_list.append(item)
                else:
                    logger.warning(f"Item at index {i} in response list for {api_id_for_log} is not a dictionary (type: {type(item)}). Wrapping it as {{ 'value': item }}.")
                    processed_list.append({"value": item}) # Or skip, or raise error
            return processed_list
        elif isinstance(response_data, dict):
            return [response_data] # Wrap the single dictionary in a list
        else:
            logger.warning(f"Response for {api_id_for_log} is not a list or dict and no extraction path. Cannot process type: {type(response_data)}. Returning as single item list {{ 'value': response_data }}.")
            return [{ "value": response_data }]

    try:
        jsonpath_expr = jsonpath_parse(extraction_path)
        matches = jsonpath_expr.find(response_data)
        
        extracted_items: List[Dict[str, Any]] = []
        if not matches:
            logger.warning(f"JSONPath '{extraction_path}' found no matches in response for {api_id_for_log}.")
            return []

        for match_idx, match in enumerate(matches):
            # match.value can be a list itself if the jsonpath ends with [*] or similar
            # or it can be a single dict if the jsonpath points to a specific object
            if isinstance(match.value, list):
                logger.debug(f"Match {match_idx} for '{extraction_path}' is a list with {len(match.value)} elements.")
                for item_idx, item in enumerate(match.value):
                    if isinstance(item, dict):
                        extracted_items.append(item)
                    else:
                        logger.warning(f"Item {item_idx} within match {match_idx} (path '{extraction_path}') for {api_id_for_log} is not a dictionary (type: {type(item)}). Skipping this sub-item.")
            elif isinstance(match.value, dict):
                logger.debug(f"Match {match_idx} for '{extraction_path}' is a dictionary.")
                extracted_items.append(match.value)
            else:
                logger.warning(f"Value of match {match_idx} (path '{extraction_path}') for {api_id_for_log} is not a list or dict (type: {type(match.value)}). Wrapping it as {{ 'value': match.value }}.")
                extracted_items.append({"value": match.value})
        
        logger.info(f"Extracted {len(extracted_items)} items using path '{extraction_path}' for {api_id_for_log}.")
        return extracted_items

    except Exception as e:
        logger.error(f"Error evaluating JSONPath '{extraction_path}' or processing matches for {api_id_for_log}: {e}", exc_info=True)
        return [] 