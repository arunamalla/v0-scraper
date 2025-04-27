import traceback
import sys
from logger import Logger

class ErrorHandler:
    """Error handler for the Oracle scraper."""
    
    def __init__(self, logger=None):
        """Initialize the error handler.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger or Logger()
    
    def handle_error(self, error, context="", exit_on_error=False):
        """Handle an error.
        
        Args:
            error: The error that occurred
            context: Additional context about where the error occurred
            exit_on_error: Whether to exit the program on error
        """
        error_type = type(error).__name__
        error_message = str(error)
        traceback_str = traceback.format_exc()
        
        # Log the error with context
        error_log = f"{context}: {error_type} - {error_message}\n{traceback_str}"
        self.logger.error(error_log)
        
        if exit_on_error:
            self.logger.error("Exiting due to critical error")
            sys.exit(1)
        
        return error_log
    
    def handle_request_error(self, error, url, context=""):
        """Handle a request-specific error.
        
        Args:
            error: The error that occurred
            url: The URL that was being requested
            context: Additional context about where the error occurred
        """
        error_context = f"{context} - Failed to request {url}"
        return self.handle_error(error, error_context)
