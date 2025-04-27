import json
import os
from logger import Logger
from error_handler import ErrorHandler

class DataManager:
    """Manage data storage and retrieval."""
    
    def __init__(self, data_dir="data", logger=None, error_handler=None):
        """Initialize the data manager.
        
        Args:
            data_dir: Directory to store data files
            logger: Logger instance
            error_handler: ErrorHandler instance
        """
        self.data_dir = data_dir
        self.logger = logger or Logger()
        self.error_handler = error_handler or ErrorHandler(self.logger)
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
    
    def save_data(self, data, filename):
        """Save data to a JSON file.
        
        Args:
            data: The data to save
            filename: The filename to save to
            
        Returns:
            True if successful, False otherwise
        """
        try:
            file_path = os.path.join(self.data_dir, filename)
            self.logger.info(f"Saving data to {file_path}")
            
            with open(file_path, 'w', encoding='utf-8') as file:
                json.dump(data, file, indent=2, ensure_ascii=False)
            
            return True
        except Exception as e:
            self.error_handler.handle_error(e, f"Failed to save data to {filename}")
            return False
    
    def load_data(self, filename):
        """Load data from a JSON file.
        
        Args:
            filename: The filename to load from
            
        Returns:
            The loaded data or None if the file doesn't exist or an error occurs
        """
        try:
            file_path = os.path.join(self.data_dir, filename)
            
            if not os.path.exists(file_path):
                self.logger.warning(f"File {file_path} does not exist")
                return None
            
            self.logger.info(f"Loading data from {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as file:
                return json.load(file)
        except Exception as e:
            self.error_handler.handle_error(e, f"Failed to load data from {filename}")
            return None
