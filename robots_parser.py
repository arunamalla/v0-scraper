import requests
from urllib.parse import urlparse
from logger import Logger
from error_handler import ErrorHandler

class RobotsParser:
    """Parse robots.txt and check if crawling is allowed."""
    
    def __init__(self, logger=None, error_handler=None):
        """Initialize the robots parser.
        
        Args:
            logger: Logger instance
            error_handler: ErrorHandler instance
        """
        self.logger = logger or Logger()
        self.error_handler = error_handler or ErrorHandler(self.logger)
        self.robots_cache = {}  # Cache robots.txt content
    
    def fetch_robots_txt(self, url):
        """Fetch robots.txt content from a website.
        
        Args:
            url: The URL to check
            
        Returns:
            The content of robots.txt or empty string if not found
        """
        try:
            # Parse the URL to get the domain
            parsed_url = urlparse(url)
            domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            # Check if we've already fetched this robots.txt
            if domain in self.robots_cache:
                return self.robots_cache[domain]
            
            # Fetch robots.txt
            robots_url = f"{domain}/robots.txt"
            self.logger.info(f"Fetching robots.txt from {robots_url}")
            
            try:
                # First try with SSL verification
                response = requests.get(robots_url, timeout=10)
                if response.status_code == 200:
                    robots_content = response.text
                    self.robots_cache[domain] = robots_content
                    return robots_content
            except requests.exceptions.SSLError as ssl_err:
                # If SSL verification fails, try again without verification
                self.logger.warning(f"SSL verification failed for {robots_url}, retrying without verification")
                response = requests.get(robots_url, timeout=10, verify=False)
                if response.status_code == 200:
                    robots_content = response.text
                    self.robots_cache[domain] = robots_content
                    return robots_content
            except requests.RequestException as req_err:
                self.logger.warning(f"Failed to fetch robots.txt from {robots_url}: {req_err}")
                return ""
            
            self.logger.warning(f"Failed to fetch robots.txt from {robots_url}: {response.status_code}")
            return ""
            
        except Exception as e:
            self.error_handler.handle_request_error(e, url, "Robots.txt fetch")
            return ""
    
    def is_allowed(self, user_agent, url):
        """Check if crawling is allowed for the given URL.
        
        Args:
            user_agent: The user agent to check
            url: The URL to check
            
        Returns:
            True if crawling is allowed, False otherwise
        """
        robots_txt = self.fetch_robots_txt(url)
        if not robots_txt:
            # If we can't fetch robots.txt, assume crawling is allowed
            return True
        
        # Parse robots.txt
        current_agent = None
        for line in robots_txt.split('\n'):
            line = line.strip().lower()
            if not line or line.startswith('#'):
                continue
                
            if line.startswith('user-agent:'):
                current_agent = line.split(':', 1)[1].strip()
            elif line.startswith('disallow:') and (current_agent == user_agent or current_agent == '*'):
                disallow_path = line.split(':', 1)[1].strip()
                if disallow_path and url.lower().startswith(urlparse(url).scheme + "://" + urlparse(url).netloc + disallow_path):
                    self.logger.warning(f"URL {url} is disallowed by robots.txt")
                    return False
        
        return True
