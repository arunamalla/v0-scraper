import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re

from logger import Logger
from error_handler import ErrorHandler
from data_manager import DataManager
from rate_limiter import RateLimiter
from robots_parser import RobotsParser

class JobScraper:
    """Scrape career URLs from customer websites."""
    
    def __init__(self, rate_limit_seconds=3, logger=None, error_handler=None, data_manager=None):
        """Initialize the job scraper.
        
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
        self.career_data = []
        
        # Common career page patterns
        self.career_patterns = [
            r'/careers?/?$',
            r'/jobs/?$',
            r'/join-us/?$',
            r'/work-with-us/?$',
            r'/employment/?$',
            r'/opportunities/?$',
            r'/join-our-team/?$',
            r'/work-for-us/?$'
        ]
    
    def scrape_career_urls(self, customer_details, output_file="career_urls.json", limit=None):
        """Scrape career URLs from customer websites.
        
        Args:
            customer_details: List of customer details from CustomerDetailScraper
            output_file: File to save the career URLs to
            limit: Maximum number of customers to process
            
        Returns:
            List of career data
        """
        if not customer_details:
            self.logger.error("No customer details provided")
            return []
        
        self.logger.info(f"Scraping career URLs for {len(customer_details) if limit is None else min(limit, len(customer_details))} customers")
        
        # Process only a subset if limit is specified
        customers_to_process = customer_details[:limit] if limit is not None else customer_details
        
        for i, customer in enumerate(customers_to_process):
            company_url = customer.get("company_url")
            company_name = customer.get("company_name") or customer.get("title")
            
            if not company_url:
                self.logger.warning(f"No company URL found for {company_name}")
                continue
                
            if company_url in self.visited_urls:
                self.logger.info(f"Already visited {company_url}, skipping")
                continue
                
            self.logger.info(f"Processing company {i+1}/{len(customers_to_process)}: {company_name}")
            
            # Check if crawling is allowed
            if not self.robots_parser.is_allowed('*', company_url):
                self.logger.warning(f"Crawling not allowed for {company_url}")
                continue
            
            try:
                # Apply rate limiting
                self.rate_limiter.limit()
                
                # Find career URL
                career_url = self._find_career_url(company_url, company_name)
                
                if career_url:
                    career_data = {
                        "company_name": company_name,
                        "company_url": company_url,
                        "career_url": career_url
                    }
                    self.career_data.append(career_data)
                    
                    # Save after each successful scrape to avoid losing data
                    self.data_manager.save_data(self.career_data, output_file)
                    
            except Exception as e:
                self.error_handler.handle_error(e, f"Error processing company {company_name}")
        
        # Final save
        self.data_manager.save_data(self.career_data, output_file)
        return self.career_data
    
    def _find_career_url(self, company_url, company_name):
        """Find the career URL for a company.
        
        Args:
            company_url: URL of the company website
            company_name: Name of the company
            
        Returns:
            Career URL if found, None otherwise
        """
        self.visited_urls.add(company_url)
        
        try:
            self.logger.info(f"Fetching {company_url}")
            response = requests.get(company_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Method 1: Look for common career page links
            career_keywords = ['career', 'careers', 'jobs', 'job', 'join', 'work with us', 'employment', 'opportunities']
            
            for link in soup.find_all('a'):
                href = link.get('href')
                text = link.get_text().lower().strip()
                
                if not href:
                    continue
                
                # Check if the link text contains career keywords
                if any(keyword in text for keyword in career_keywords):
                    # Make sure the URL is absolute
                    if not href.startswith(('http://', 'https://')):
                        href = urljoin(company_url, href)
                    
                    self.logger.info(f"Found career URL via keywords: {href}")
                    return href
            
            # Method 2: Check common career URL patterns
            parsed_url = urlparse(company_url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            for pattern in self.career_patterns:
                career_url = urljoin(base_url, re.sub(r'^/', '', pattern))
                
                try:
                    # Check if the URL exists
                    self.rate_limiter.limit()
                    response = requests.head(career_url, timeout=10)
                    
                    if response.status_code < 400:
                        self.logger.info(f"Found career URL via pattern: {career_url}")
                        return career_url
                except:
                    # Ignore errors when checking pattern URLs
                    pass
            
            self.logger.warning(f"No career URL found for {company_name}")
            return None
            
        except requests.RequestException as e:
            self.error_handler.handle_request_error(e, company_url, "Career URL scraping")
            return None
