CREATE TABLE IF NOT EXISTS api_configs (
    api_id VARCHAR(128) PRIMARY KEY,
    description TEXT, -- API description
    url TEXT NOT NULL, -- URL, can contain placeholders like {context.field} or {config.default_context.field}
    http_method VARCHAR(10) NOT NULL DEFAULT 'GET',
    
    headers_json JSON, -- Headers, values can contain placeholders
    params_json JSON, -- Query params, values can contain placeholders
    body_template_json JSON, -- Template for body (for POST/PUT), can contain placeholders
    default_context_json JSON, -- Default context values for this API

    auth_type VARCHAR(20) NOT NULL DEFAULT 'none', -- e.g., none, api_key, basic, oauth2_cc
    auth_config JSON, -- Auth details, including secret references like 'env:VAR_NAME'

    pagination_json JSON, -- {type: "page/offset_limit/cursor", page_param_name_in_request:"page", ...}
    
    data_extraction_path_in_response VARCHAR(255), -- JSONPath e.g., "data.items", "results[*]"

    rate_limit_json JSON, -- {limit:100, period:60} (requests/period_in_seconds)
    timeout_sec INT DEFAULT 10,
    
    schedule_cron VARCHAR(64), -- Cron expression for Celery Beat
    
    dest_table VARCHAR(128) NOT NULL, -- MySQL table name to store data
    dest_table_data_column VARCHAR(128) DEFAULT 'raw_data', -- Column to store the extracted JSON data

    is_active BOOLEAN DEFAULT TRUE, -- Flag to enable/disable crawling for this API
    
    dlq_topic VARCHAR(128), -- Kafka topic for Dead Letter Queue (if null, uses default from env)
    max_retries INT DEFAULT 3,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Example (commented out, for reference only)
/*
INSERT INTO api_configs (
    api_id, description, url, http_method, headers_json, params_json, body_template_json, default_context_json,
    auth_type, auth_config,
    pagination_json,
    data_extraction_path_in_response,
    rate_limit_json, timeout_sec, schedule_cron,
    dest_table, dest_table_data_column, is_active, dlq_topic, max_retries
) VALUES (
    'example_api_key_auth',
    'Example API using API Key and page number pagination',
    'https://api.example.com/data',
    'GET',
    '{"Accept": "application/json"}',
    '{"api_key_param": "{config.default_context.api_key_value}", "category": "{context.category_filter}", "page_number_param": "{pagination.current_page}"}',
    NULL, -- No body for GET
    '{"api_key_value": "env:EXAMPLE_API_KEY_SECRET", "default_category": "all"}',
    'api_key', 
    '{ "key_name_in_request": "api_key_param", "key_location": "query" }', // Indicates the key is in query params and its name
    '{
        "type": "page",
        "page_param_name_in_request": "page_number_param",
        "page_size_param_name_in_request": "page_size_param", 
        "page_size_value": 50,
        "initial_page_number": 1,
        "increment_step": 1,
        "stop_condition": {
            "type": "empty_on_path", 
            "path_in_response": "items"
        }
    }',
    '$.items',
    '{"limit": 5, "period": 1}', -- 5 requests per second
    15,
    '* * * * *', -- Every minute (for testing)
    'example_api_data_table',
    'crawled_content',
    TRUE,
    'example_api_dlq',
    5
);

INSERT INTO api_configs (
    api_id, description, url, http_method, headers_json, params_json, body_template_json, default_context_json,
    auth_type, auth_config,
    pagination_json,
    data_extraction_path_in_response,
    rate_limit_json, timeout_sec, schedule_cron,
    dest_table, is_active, dlq_topic, max_retries
) VALUES (
    'oauth_api_offset_limit',
    'Example API using OAuth2 CC and offset/limit pagination',
    'https://api.oauth_example.com/v2/records',
    'GET',
    '{"Accept": "application/json"}', -- Auth header will be added by worker
    '{"limit": "{pagination.limit_value}", "offset": "{pagination.offset_value}"}',
    NULL,
    NULL,
    'oauth2_cc',
    '{
        "token_url": "https://auth.oauth_example.com/token",
        "client_id_env": "OAUTH_CLIENT_ID",
        "client_secret_env": "OAUTH_CLIENT_SECRET",
        "scopes": ["read_records", "user_info"],
        "token_header_name": "Authorization",
        "grant_type": "client_credentials"
    }',
    '{
        "type": "offset_limit",
        "offset_param_name_in_request": "offset",
        "limit_param_name_in_request": "limit",
        "limit_value": 100,
        "initial_offset": 0,
        "stop_condition": {
            "type": "count_less_than_limit", 
            "items_path_in_response": "data.entries"
        }
    }',
    'data.entries',
    '{"limit": 10, "period": 60}', -- 10 requests per minute
    20,
    "0 * * * *", -- Every hour
    'oauth_api_records_table',
    TRUE,
    NULL, -- Use default DLQ topic
    3
);
*/ 

INSERT INTO api_configs (
    api_id, 
    description, 
    url, 
    http_method, 
    headers_json, 
    params_json, 
    body_template_json, 
    default_context_json,
    auth_type, 
    auth_config,
    pagination_json,
    data_extraction_path_in_response,
    rate_limit_json, 
    timeout_sec, 
    schedule_cron,
    dest_table, 
    dest_table_data_column,
    is_active, 
    dlq_topic, 
    max_retries
) VALUES (
    'myinvois_documents_search_v1',
    'Search documents from MyInvois API (Preprod)',
    'https://preprod-api.myinvois.hasil.gov.my/api/v1.0/documents/search',
    'GET',
    '{"Accept": "application/json"}', 
    '{
        "pageSize": "{pagination.page_size_value}", 
        "issueDateFrom": "{context.issueDateFrom}", 
        "issueDateTo": "{context.issueDateTo}", 
        "pageNo": "{pagination.current_page}"
    }',
    NULL, 
    '{
        "issueDateFrom": "2025-01-01", 
        "issueDateTo": "2025-01-15" 
    }',
    'oauth2_cc',
    '{
        "token_url": "https://preprod-api.myinvois.hasil.gov.my/connect/token",
        "client_id_env": "",
        "client_secret_env": "",
        "scopes": ["InvoicingAPI"],
        "token_header_name": "Authorization",
        "grant_type": "client_credentials",
        "additional_token_params": {}
    }',
    '{
        "type": "page",
        "page_param_name_in_request": "pageNo",
        "initial_page_number": 1,
        "page_size_param_name_in_request": "pageSize",
        "page_size_value": 100,
        "increment_step": 1,
        "stop_condition": {
            "type": "items_count_on_path",
            "items_path_in_response": "result",
            "count_condition": "less_than_page_size"
        }
    }',
    "result",
    '{"limit": 10, "period": 60}',
    30,
    "0 1 * * *",
    'myinvois_documents_preprod',
    'raw_data',
    TRUE,
    'myinvois_documents_search_dlq',
    3
)
ON DUPLICATE KEY UPDATE
    description = VALUES(description),
    url = VALUES(url),
    http_method = VALUES(http_method),
    headers_json = VALUES(headers_json),
    params_json = VALUES(params_json),
    body_template_json = VALUES(body_template_json),
    default_context_json = VALUES(default_context_json),
    auth_type = VALUES(auth_type),
    auth_config = VALUES(auth_config),
    pagination_json = VALUES(pagination_json),
    data_extraction_path_in_response = VALUES(data_extraction_path_in_response),
    rate_limit_json = VALUES(rate_limit_json),
    timeout_sec = VALUES(timeout_sec),
    schedule_cron = VALUES(schedule_cron),
    dest_table = VALUES(dest_table),
    dest_table_data_column = VALUES(dest_table_data_column),
    is_active = VALUES(is_active),
    dlq_topic = VALUES(dlq_topic),
    max_retries = VALUES(max_retries),
    updated_at = CURRENT_TIMESTAMP;

/*
-- Commenting out the generic examples if they exist to avoid conflicts
-- DELETE FROM api_configs WHERE api_id = 'example_api_key_auth';
-- DELETE FROM api_configs WHERE api_id = 'oauth_api_offset_limit';


/* CREATE TABLE IF NOT EXISTS myinvois_documents_preprod (
     id INT AUTO_INCREMENT PRIMARY KEY,
     raw_data JSON,
     api_id_ref VARCHAR(128),
     crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
     INDEX (api_id_ref) -- Optional: For faster lookups by api_id_ref
 );
*/ 