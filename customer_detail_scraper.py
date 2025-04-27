import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import concurrent.futures
from typing import List, Dict, Any, Set
import threading

from logger import Logger
from error_handler import ErrorHandler
from data_manager import DataManager
from rate_limiter import RateLimiter
from robots_parser import RobotsParser
from url_tracker import URLTracker
from utils import safe_request

class CustomerDetailScraper:
    """Scrape detailed information for each customer."""
    
    def __init__(self, rate_limit_seconds=3, logger=None, error_handler=None, data_manager=None, url_tracker=None):
        """Initialize the customer detail scraper.
        
        Args:
            rate_limit_seconds: Time to wait between requests
            logger: Logger instance
            error_handler: ErrorHandler instance
            data_manager: DataManager instance
            url_tracker: URLTracker instance
        """
        self.logger = logger or Logger()
        self.error_handler = error_handler or ErrorHandler(self.logger)
        self.data_manager = data_manager or DataManager(logger=self.logger, error_handler=self.error_handler)
        self.rate_limiter = RateLimiter(rate_limit_seconds, self.logger)
        self.robots_parser = RobotsParser(self.logger, self.error_handler)
        self.url_tracker = url_tracker or URLTracker(logger=self.logger, error_handler=self.error_handler)
        
        self.customer_details = []
        self.lock = threading.Lock()  # Lock for thread safety
    
    def scrape_customer_details(self, customers_data, output_file="customer_details.json", limit=None, max_workers=5, resume=True):
        """Scrape detailed information for each customer.
        
        Args:
            customers_data: List of customer data from OracleCustomerScraper
            output_file: File to save the customer details to
            limit: Maximum number of customers to process
            max_workers: Maximum number of worker threads
            resume: Whether to resume from a checkpoint
            
        Returns:
            List of customer details
        """
        if not customers_data:
            self.logger.error("No customer data provided")
            return []
        
        # Load existing data if resuming
        if resume:
            checkpoint = self.url_tracker.load_checkpoint()
            if checkpoint and checkpoint.get('stage') == 'customer_details':
                self.logger.info("Resuming customer details scraping from checkpoint")
                self.customer_details = self.data_manager.load_data(output_file) or []
                
                # Get the last processed index
                last_index = checkpoint.get('last_index', 0)
                
                # If we have data and haven't finished, we can skip some customers
                if self.customer_details and last_index < len(customers_data):
                    self.logger.info(f"Loaded {len(self.customer_details)} customer details from previous session")
                    self.logger.info(f"Resuming from index {last_index}")
                    
                    # Adjust customers_data to start from where we left off
                    customers_data = customers_data[last_index:]
        
        # Process only a subset if limit is specified
        customers_to_process = customers_data[:limit] if limit is not None else customers_data
        
        # Filter out already visited URLs
        unvisited_customers = []
        for customer in customers_to_process:
            link = customer.get("link")
            if link and not self.url_tracker.is_customer_url_visited(link):
                unvisited_customers.append(customer)
        
        self.logger.info(f"Scraping details for {len(unvisited_customers)} unvisited customers out of {len(customers_to_process)} total")
        
        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks
            future_to_customer = {
                executor.submit(self._process_customer, i, customer, len(unvisited_customers), output_file): customer
                for i, customer in enumerate(unvisited_customers)
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_customer):
                customer = future_to_customer[future]
                try:
                    future.result()  # This will re-raise any exceptions from the thread
                except Exception as e:
                    self.error_handler.handle_error(e, f"Error processing customer {customer.get('title')}")
        
        # Final save
        self.data_manager.save_data(self.customer_details, output_file)
        
        # Clear checkpoint since we're done with this stage
        self.url_tracker.clear_checkpoint()
        
        return self.customer_details
    
    def _process_customer(self, index, customer, total, output_file):
        """Process a single customer.
        
        Args:
            index: Index of the customer in the list
            customer: Customer data
            total: Total number of customers
            output_file: File to save the customer details to
        """
        link = customer.get("link")
        if not link:
            self.logger.warning(f"No link found for customer {index}")
            return
        
        # Check if we've already visited this URL
        if self.url_tracker.is_customer_url_visited(link):
            self.logger.info(f"Already visited {link}, skipping")
            return
        
        self.logger.info(f"Processing customer {index+1}/{total}: {customer.get('title')}")
        
        # Check if crawling is allowed
        if not self.robots_parser.is_allowed('*', link):
            self.logger.warning(f"Crawling not allowed for {link}")
            return
        
        try:
            # Apply rate limiting
            self.rate_limiter.limit()
            
            # Scrape customer details
            customer_details = self._scrape_single_customer(link)
            
            if customer_details:
                # Merge the basic customer data with the detailed data
                merged_data = {**customer, **customer_details}
                
                # Thread-safe update of customer_details list
                with self.lock:
                    self.customer_details.append(merged_data)
                    
                    # Save checkpoint
                    self.url_tracker.save_checkpoint({
                        'stage': 'customer_details',
                        'last_index': index,
                        'customers_count': len(self.customer_details)
                    })
                    
                    # Save after each successful scrape to avoid losing data
                    self.data_manager.save_data(self.customer_details, output_file)
                
                # Mark this URL as visited
                self.url_tracker.add_customer_url(link)
                
        except Exception as e:
            self.error_handler.handle_error(e, f"Error processing customer {customer.get('title')}")
    
    def _scrape_single_customer(self, url):
        """Scrape detailed information for a single customer.
        
        Args:
            url: URL of the customer page
            
        Returns:
            Dictionary of customer details
        """
        try:
            self.logger.info(f"Fetching {url}")
            response = safe_request(url, timeout=30)
            
            if not response:
                self.logger.error(f"Failed to fetch {url}")
                return {}
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract products used
            products = []
            products_div = soup.find('div', attrs={'data-trackas': 'rc42:products-used'})
            if products_div:
                product_links = products_div.find_all('a')
                products = [a.text.strip() for a in product_links]
                self.logger.info(f"Found {len(products)} products")
            
            # Extract company website
            company_url = None
            company_name = None
            learn_more_div = soup.find('div', attrs={'data-trackas': 'rc42:learn-more'})
            if learn_more_div:
                learn_more_link = learn_more_div.find('a')
                if learn_more_link:
                    company_url = learn_more_link.get('href')
                    company_name = learn_more_link.text.replace(", opens in new tab", "").strip()
                    self.logger.info(f"Found company website: {company_name} - {company_url}")
            
            # Extract case study content
            case_study_content = ""
            content_div = soup.find('div', class_='cb41w1')
            if content_div:
                paragraphs = content_div.find_all('p')
                case_study_content = "\n\n".join([p.text.strip() for p in paragraphs])
            
            # Extract customer quote if available
            quote = ""
            quote_div = soup.find('div', class_='cb41quote')
            if quote_div:
                quote = quote_div.text.strip()
            
            return {
                'company_name': company_name,
                'company_url': company_url,
                'products_used': products,
                'case_study_content': case_study_content,
                'quote': quote
            }
            
        except requests.RequestException as e:
            self.error_handler.handle_request_error(e, url, "Customer detail scraping")
            return {}
