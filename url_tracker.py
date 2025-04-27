import os
import json
from typing import Set, Dict, List, Any
from datetime import datetime
from logger import Logger
from error_handler import ErrorHandler

class URLTracker:
    """Track visited URLs across scraping sessions."""
    
    def __init__(self, data_dir="data", logger=None, error_handler=None):
        """Initialize the URL tracker.
        
        Args:
            data_dir: Directory to store tracking files
            logger: Logger instance
            error_handler: ErrorHandler instance
        """
        self.data_dir = data_dir
        self.logger = logger or Logger()
        self.error_handler = error_handler or ErrorHandler(self.logger)
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        # File paths for tracking
        self.customer_urls_file = os.path.join(data_dir, "visited_customer_urls.json")
        self.job_urls_file = os.path.join(data_dir, "visited_job_urls.json")
        self.checkpoint_file = os.path.join(data_dir, "scraper_checkpoint.json")
        
        # Initialize tracking sets
        self.visited_customer_urls = self._load_urls(self.customer_urls_file)
        self.visited_job_urls = self._load_urls(self.job_urls_file)
        
        self.logger.info(f"Loaded {len(self.visited_customer_urls)} visited customer URLs")
        self.logger.info(f"Loaded {len(self.visited_job_urls)} visited job URLs")
    
    def _load_urls(self, file_path: str) -> Set[str]:
        """Load visited URLs from a file.
        
        Args:
            file_path: Path to the file containing visited URLs
            
        Returns:
            Set of visited URLs
        """
        if not os.path.exists(file_path):
            return set()
            
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
                return set(data.get('urls', []))
        except Exception as e:
            self.error_handler.handle_error(e, f"Error loading URLs from {file_path}")
            return set()
    
    def _save_urls(self, urls: Set[str], file_path: str) -> bool:
        """Save visited URLs to a file.
        
        Args:
            urls: Set of URLs to save
            file_path: Path to save the URLs to
            
        Returns:
            True if successful, False otherwise
        """
        try:
            data = {
                'urls': list(urls),
                'last_updated': datetime.now().isoformat()
            }
            
            with open(file_path, 'w') as file:
                json.dump(data, file, indent=2)
                
            return True
        except Exception as e:
            self.error_handler.handle_error(e, f"Error saving URLs to {file_path}")
            return False
    
    def is_customer_url_visited(self, url: str) -> bool:
        """Check if a customer URL has been visited.
        
        Args:
            url: URL to check
            
        Returns:
            True if the URL has been visited, False otherwise
        """
        return url in self.visited_customer_urls
    
    def is_job_url_visited(self, url: str) -> bool:
        """Check if a job URL has been visited.
        
        Args:
            url: URL to check
            
        Returns:
            True if the URL has been visited, False otherwise
        """
        return url in self.visited_job_urls
    
    def add_customer_url(self, url: str) -> None:
        """Add a customer URL to the visited set.
        
        Args:
            url: URL to add
        """
        self.visited_customer_urls.add(url)
        self._save_urls(self.visited_customer_urls, self.customer_urls_file)
    
    def add_job_url(self, url: str) -> None:
        """Add a job URL to the visited set.
        
        Args:
            url: URL to add
        """
        self.visited_job_urls.add(url)
        self._save_urls(self.visited_job_urls, self.job_urls_file)
    
    def save_checkpoint(self, checkpoint_data: Dict[str, Any]) -> bool:
        """Save a checkpoint for resuming scraping.
        
        Args:
            checkpoint_data: Dictionary containing checkpoint data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Add timestamp to checkpoint
            checkpoint_data['timestamp'] = datetime.now().isoformat()
            
            with open(self.checkpoint_file, 'w') as file:
                json.dump(checkpoint_data, file, indent=2)
                
            self.logger.info(f"Checkpoint saved: {checkpoint_data.get('stage', 'unknown')}")
            return True
        except Exception as e:
            self.error_handler.handle_error(e, "Error saving checkpoint")
            return False
    
    def load_checkpoint(self) -> Dict[str, Any]:
        """Load the latest checkpoint.
        
        Returns:
            Dictionary containing checkpoint data, or empty dict if no checkpoint exists
        """
        if not os.path.exists(self.checkpoint_file):
            return {}
            
        try:
            with open(self.checkpoint_file, 'r') as file:
                checkpoint = json.load(file)
                
            self.logger.info(f"Loaded checkpoint from {checkpoint.get('timestamp', 'unknown date')}")
            return checkpoint
        except Exception as e:
            self.error_handler.handle_error(e, "Error loading checkpoint")
            return {}
    
    def clear_checkpoint(self) -> bool:
        """Clear the current checkpoint.
        
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(self.checkpoint_file):
            return True
            
        try:
            os.remove(self.checkpoint_file)
            self.logger.info("Checkpoint cleared")
            return True
        except Exception as e:
            self.error_handler.handle_error(e, "Error clearing checkpoint")
            return False
    
    def get_unvisited_urls(self, all_urls: List[str], url_type: str = 'customer') -> List[str]:
        """Get a list of URLs that haven't been visited yet.
        
        Args:
            all_urls: List of all URLs to check
            url_type: Type of URL ('customer' or 'job')
            
        Returns:
            List of unvisited URLs
        """
        if url_type == 'customer':
            return [url for url in all_urls if url not in self.visited_customer_urls]
        elif url_type == 'job':
            return [url for url in all_urls if url not in self.visited_job_urls]
        else:
            self.logger.warning(f"Unknown URL type: {url_type}")
            return []
