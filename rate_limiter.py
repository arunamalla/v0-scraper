import time
from logger import Logger

class RateLimiter:
    """Rate limiter to prevent overloading servers."""
    
    def __init__(self, rate_limit_seconds=3, logger=None):
        """Initialize the rate limiter.
        
        Args:
            rate_limit_seconds: Time to wait between requests
            logger: Logger instance
        """
        self.rate_limit_seconds = rate_limit_seconds
        self.logger = logger or Logger()
        self.last_request_time = 0
    
    def limit(self):
        """Apply rate limiting.
        
        Ensures that at least rate_limit_seconds have passed since the last request.
        """
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < self.rate_limit_seconds:
            wait_time = self.rate_limit_seconds - elapsed
            self.logger.debug(f"Rate limiting: waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)
        
        self.last_request_time = time.time()
