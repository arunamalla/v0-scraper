import warnings
import requests
from urllib3.exceptions import InsecureRequestWarning

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
