# worker/src/auth_handler.py
import logging
import time
import requests # For OAuth2 token fetching
from typing import Dict, Optional, Tuple, Any

from .settings import settings # To access API_SECRETS
from .models import ApiConfig, AuthAPIKeyConfig, AuthBasicConfig, AuthOAuth2ClientCredentialsConfig

logger = logging.getLogger(__name__)

# In-memory cache for OAuth2 tokens. Key: api_id, Value: {'token': '...', 'expires_at': timestamp}
# For a multi-worker/multi-process setup, a distributed cache (e.g., Redis) is recommended.
_oauth_token_cache: Dict[str, Dict[str, Any]] = {}


class AuthenticationError(Exception):
    """Custom exception for authentication failures."""
    pass

class AuthHandler:
    def __init__(self, api_config: ApiConfig):
        self.api_config = api_config
        self.auth_type = api_config.auth_type
        # auth_config_json is the raw dict from the database
        self.auth_config_data = api_config.auth_config_json 
        self.parsed_auth_config = self._parse_specific_auth_config()

    def _get_secret(self, env_var_ref: str) -> Optional[str]:
        """Retrieves secret value from environment variables using 'env:VAR_NAME' format."""
        if not env_var_ref or not isinstance(env_var_ref, str) or not env_var_ref.startswith("env:"):
            logger.error(f"Invalid secret reference format: '{env_var_ref}'. Expected 'env:YOUR_ENV_VARIABLE_NAME'.")
            return None
        
        env_var_name = env_var_ref[len("env:"):]
        secret_value = settings.API_SECRETS.get(env_var_name)
        
        if secret_value is None:
            logger.error(f"Secret environment variable '{env_var_name}' (referenced as '{env_var_ref}') not found for api_id: {self.api_config.api_id}.")
        return secret_value

    def _parse_specific_auth_config(self) -> Optional[Any]:
        """Parses the auth_config_data (from DB) into a specific Pydantic model based on auth_type."""
        if not self.auth_config_data:
            if self.auth_type != 'none':
                logger.warning(f"Auth type for {self.api_config.api_id} is '{self.auth_type}' but auth_config_json is empty/None.")
            return None # No config to parse

        try:
            if self.auth_type == 'api_key':
                return AuthAPIKeyConfig(**self.auth_config_data)
            elif self.auth_type == 'basic':
                return AuthBasicConfig(**self.auth_config_data)
            elif self.auth_type == 'oauth2_cc':
                return AuthOAuth2ClientCredentialsConfig(**self.auth_config_data)
            # If auth_type is 'none' or unknown, parsed_auth_config remains None
            return None 
        except Exception as e:
            logger.error(f"Failed to parse auth_config_json for {self.api_config.api_id} (type: {self.auth_type}): {self.auth_config_data}. Error: {e}", exc_info=True)
            # Raise a specific error to halt processing if auth config is critical and malformed
            raise AuthenticationError(f"Invalid or unparsable auth_config for {self.api_config.api_id}. Check DB configuration.")

    def get_auth_details_for_request(self) -> Tuple[Dict[str, str], Dict[str, Any], Optional[Tuple[str, str]]]:
        """
        Returns a tuple of (auth_headers, auth_query_params, basic_auth_tuple).
        Manages token fetching/refreshing for OAuth2.
        basic_auth_tuple is (username, password) for requests library's `auth` param.
        """
        auth_headers: Dict[str, str] = {}
        auth_query_params: Dict[str, Any] = {}
        basic_auth_tuple: Optional[Tuple[str,str]] = None

        if self.auth_type == 'none' or not self.parsed_auth_config:
            logger.debug(f"No authentication configured or needed for api_id: {self.api_config.api_id}")
            return auth_headers, auth_query_params, basic_auth_tuple

        try:
            if isinstance(self.parsed_auth_config, AuthAPIKeyConfig):
                conf = self.parsed_auth_config
                api_key = self._get_secret(conf.key_value_env)
                if api_key is None: # _get_secret returns None if not found and logs error
                    raise AuthenticationError(f"API key (env: {conf.key_value_env}) not found for {self.api_config.api_id}")
                
                if conf.key_location == 'header':
                    auth_headers[conf.key_name_in_request] = api_key
                elif conf.key_location == 'query':
                    auth_query_params[conf.key_name_in_request] = api_key
                else:
                    raise AuthenticationError(f"Invalid api_key location: {conf.key_location} for {self.api_config.api_id}")

            elif isinstance(self.parsed_auth_config, AuthBasicConfig):
                conf = self.parsed_auth_config
                username = self._get_secret(conf.username_env)
                password = self._get_secret(conf.password_env)
                if username is None or password is None: # Password can be empty, but must be configured
                    raise AuthenticationError(f"Username or password (env: {conf.username_env}/{conf.password_env}) not found for Basic Auth for {self.api_config.api_id}")
                basic_auth_tuple = (username, password)

            elif isinstance(self.parsed_auth_config, AuthOAuth2ClientCredentialsConfig):
                conf = self.parsed_auth_config
                token = self._get_or_refresh_oauth_token(conf)
                auth_headers[conf.token_header_name] = f"Bearer {token}"
            
        except AuthenticationError:
            raise # Re-raise specific auth errors to be handled by the caller
        except Exception as e:
            logger.error(f"Unexpected error during authentication preparation for {self.api_config.api_id}: {e}", exc_info=True)
            raise AuthenticationError(f"General auth processing error for {self.api_config.api_id}")

        return auth_headers, auth_query_params, basic_auth_tuple

    def _get_or_refresh_oauth_token(self, config: AuthOAuth2ClientCredentialsConfig) -> str:
        api_id = self.api_config.api_id
        # Simple in-memory cache key including client_id to differentiate if multiple cfgs use same IDP but different clients
        # A more robust cache key might involve more elements of the config if they affect token uniqueness.
        cache_key = f"oauth2_{api_id}_{config.client_id_env}"
        cached_token_info = _oauth_token_cache.get(cache_key)

        # Check cache first
        if cached_token_info and cached_token_info['expires_at'] > time.time():
            logger.debug(f"Using cached OAuth token for {api_id} (cache key: {cache_key})")
            return cached_token_info['token']

        logger.info(f"Fetching new OAuth token for {api_id} from {config.token_url} (cache key: {cache_key})")
        
        client_id = self._get_secret(config.client_id_env)
        client_secret = self._get_secret(config.client_secret_env)

        if client_id is None or client_secret is None:
            raise AuthenticationError(f"Client ID (env: {config.client_id_env}) or Secret (env: {config.client_secret_env}) not found for OAuth2 for {api_id}")

        payload = {
            'grant_type': config.grant_type,
            'client_id': client_id,
            'client_secret': client_secret,
        }
        if config.scopes: # Ensure scopes is not empty list if provided
            payload['scope'] = ' '.join(config.scopes)
        
        if config.additional_token_params:
            payload.update(config.additional_token_params)

        try:
            # Using str(config.token_url) because HttpUrl might not be directly usable by requests
            response = requests.post(str(config.token_url), data=payload, timeout=self.api_config.timeout_sec)
            response.raise_for_status() 
            
            token_data = response.json()
            access_token = token_data.get('access_token')
            # Handle expires_in carefully, ensure it's an int
            expires_in_raw = token_data.get('expires_in')
            try:
                expires_in = int(expires_in_raw) if expires_in_raw is not None else 3600
            except ValueError:
                logger.warning(f"OAuth token 'expires_in' for {api_id} is not an int: '{expires_in_raw}'. Defaulting to 3600s.")
                expires_in = 3600

            if not access_token:
                logger.error(f"OAuth response for {api_id} missing 'access_token'. Response: {token_data}")
                raise AuthenticationError(f"OAuth token missing 'access_token' in response for {api_id}")

            # Store with a buffer (e.g., 60 seconds) before actual expiry
            # Add a small jitter to expiry to prevent multiple workers trying to refresh at the exact same microsecond
            buffer_seconds = 60
            expires_at = time.time() + expires_in - buffer_seconds 
            _oauth_token_cache[cache_key] = {'token': access_token, 'expires_at': expires_at}
            
            logger.info(f"Successfully fetched OAuth token for {api_id}. Apparent expiry: {time.ctime(time.time() + expires_in)}, Cached until: {time.ctime(expires_at)}")
            return access_token

        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error fetching OAuth token for {api_id} from {config.token_url}: {e}", exc_info=True)
            if cache_key in _oauth_token_cache: del _oauth_token_cache[cache_key] # Invalidate cache on error
            raise AuthenticationError(f"OAuth token request failed for {api_id}: {e}")
        except (ValueError, KeyError) as e: # For JSON parsing errors or missing keys in response
            logger.error(f"Error parsing OAuth token JSON response for {api_id}: {e}. Response text: {response.text if 'response' in locals() else 'N/A'}", exc_info=True)
            if cache_key in _oauth_token_cache: del _oauth_token_cache[cache_key]
            raise AuthenticationError(f"Invalid OAuth token response (JSON parsing or key error) for {api_id}: {e}")
        except Exception as e: # Catch-all for other unexpected errors during token fetch
            logger.error(f"Unexpected error during OAuth token fetch for {api_id}: {e}", exc_info=True)
            if cache_key in _oauth_token_cache: del _oauth_token_cache[cache_key]
            raise AuthenticationError(f"Unexpected error fetching OAuth token for {api_id}: {e}") 