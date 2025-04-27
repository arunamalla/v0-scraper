import warnings
import requests
from urllib3.exceptions import InsecureRequestWarning
import time

def safe_request(url, method='get', timeout=30, **kwargs):
    """Make a request with fallback to disable SSL verification if needed.
    
    Args:
        url: URL to request
        method: HTTP method (get, post, head, etc.)
        timeout: Request timeout in seconds
        **kwargs: Additional arguments to pass to requests
        
    Returns:
        Response object
    """
    # Suppress only the single InsecureRequestWarning
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', InsecureRequestWarning)
        
        try:
            # First try with SSL verification
            request_method = getattr(requests, method.lower())
            response = request_method(url, timeout=timeout, **kwargs)
            return response
        except requests.exceptions.SSLError:
            # If SSL verification fails, try again without verification
            if 'verify' not in kwargs:
                kwargs['verify'] = False
                return request_method(url, timeout=timeout, **kwargs)
            else:
                # If verify is already set, just re-raise the exception
                raise

def retry_request(url, method='get', max_retries=3, retry_delay=5, **kwargs):
    """Make a request with automatic retries.
    
    Args:
        url: URL to request
        method: HTTP method (get, post, head, etc.)
        max_retries: Maximum number of retries
        retry_delay: Delay between retries in seconds
        **kwargs: Additional arguments to pass to requests
        
    Returns:
        Response object or None if all retries failed
    """
    for attempt in range(max_retries):
        try:
            response = safe_request(url, method=method, **kwargs)
            
            # Check if we got a successful response
            if response and response.status_code < 400:
                return response
                
            # If we got a 5xx error, retry
            if response and 500 <= response.status_code < 600:
                print(f"Got {response.status_code} error, retrying ({attempt+1}/{max_retries})...")
                time.sleep(retry_delay)
                continue
                
            # For other status codes, return the response
            return response
            
        except requests.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request failed: {str(e)}, retrying ({attempt+1}/{max_retries})...")
                time.sleep(retry_delay)
            else:
                print(f"Request failed after {max_retries} attempts: {str(e)}")
                return None
    
    return None
