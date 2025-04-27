import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from logger import Logger
from error_handler import ErrorHandler
from data_manager import DataManager
from rate_limiter import RateLimiter
from robots_parser import RobotsParser

class CustomerDetailScraper:
    """Scrape detailed information for each customer."""
    
    def __init__(self, rate_limit_seconds=3, logger=None, error_handler=None, data_manager=None):
        """Initialize the customer detail scraper.
        
        Args:
            rate_limit_seconds: Time to wait between requests
            logger: Logger instance
            error_handler: ErrorHandler instance
            data_manager: DataManager instance
        """
        self.logger = logger or Logger()
        self.error_handler = error_handler or ErrorHandler(self.logger)
        self.data_manager = data_manager or DataManager(logger=self.logger, error_handler=self.error_handler)
        self.rate_limiter = RateLimiter(rate_limit_seconds, self.logger)
        self.robots_parser = RobotsParser(self.logger, self.error_handler)
        
        self.visited_urls = set()
        self.customer_details = []
    
    def scrape_customer_details(self, customers_data, output_file="customer_details.json", limit=None):
        """Scrape detailed information for each customer.
        
        Args:
            customers_data: List of customer data from OracleCustomerScraper
            output_file: File to save the customer details to
            limit: Maximum number of customers to process
            
        Returns:
            List of customer details
        """
        if not customers_data:
            self.logger.error("No customer data provided")
            return []
        
        self.logger.info(f"Scraping details for {len(customers_data) if limit is None else min(limit, len(customers_data))} customers")
        
        # Process only a subset if limit is specified
        customers_to_process = customers_data[:limit] if limit is not None else customers_data
        
        for i, customer in enumerate(customers_to_process):
            link = customer.get("link")
            if not link:
                self.logger.warning(f"No link found for customer {i}")
                continue
                
            if link in self.visited_urls:
                self.logger.info(f"Already visited {link}, skipping")
                continue
                
            self.logger.info(f"Processing customer {i+1}/{len(customers_to_process)}: {customer.get('title')}")
            
            # Check if crawling is allowed
            if not self.robots_parser.is_allowed('*', link):
                self.logger.warning(f"Crawling not allowed for {link}")
                continue
            
            try:
                # Apply rate limiting
                self.rate_limiter.limit()
                
                # Scrape customer details
                customer_details = self._scrape_single_customer(link)
                
                if customer_details:
                    # Merge the basic customer data with the detailed data
                    merged_data = {**customer, **customer_details}
                    self.customer_details.append(merged_data)
                    
                    # Save after each successful scrape to avoid losing data
                    self.data_manager.save_data(self.customer_details, output_file)
                    
            except Exception as e:
                self.error_handler.handle_error(e, f"Error processing customer {customer.get('title')}")
        
        # Final save
        self.data_manager.save_data(self.customer_details, output_file)
        return self.customer_details
    
    def _scrape_single_customer(self, url):
        """Scrape detailed information for a single customer.
        
        Args:
            url: URL of the customer page
            
        Returns:
            Dictionary of customer details
        """
        self.visited_urls.add(url)
        
        try:
            self.logger.info(f"Fetching {url}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
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
