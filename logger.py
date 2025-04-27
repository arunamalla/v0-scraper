import logging
import os
from datetime import datetime

class Logger:
    """Custom logger for the Oracle scraper."""
    
    def __init__(self, log_dir="logs"):
        """Initialize the logger.
        
        Args:
            log_dir: Directory to store log files
        """
        # Create logs directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Create a unique log file name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"oracle_scraper_{timestamp}.log")
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger("OracleScraper")
    
    def info(self, message):
        """Log an info message."""
        self.logger.info(message)
    
    def error(self, message):
        """Log an error message."""
        self.logger.error(message)
    
    def warning(self, message):
        """Log a warning message."""
        self.logger.warning(message)
    
    def debug(self, message):
        """Log a debug message."""
        self.logger.debug(message)
