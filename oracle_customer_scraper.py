import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import time
from urllib.parse import urljoin
import concurrent.futures
from typing import List, Dict, Any, Set

from logger import Logger
from error_handler import ErrorHandler
from data_manager import DataManager
from rate_limiter import RateLimiter
from robots_parser import RobotsParser
from url_tracker import URLTracker
from utils import safe_request

class OracleCustomerScraper:
    """Scrape Oracle customer success stories."""
    
    def __init__(self, base_url="https://www.oracle.com", customers_path="/customers/", 
                 rate_limit_seconds=3, logger=None, error_handler=None, data_manager=None, url_tracker=None):
        """Initialize the Oracle customer scraper.
        
        Args:
            base_url: Base URL for Oracle website
            customers_path: Path to customers page
            rate_limit_seconds: Time to wait between requests
            logger: Logger instance
            error_handler: ErrorHandler instance
            data_manager: DataManager instance
            url_tracker: URLTracker instance
        """
        self.base_url = base_url
        self.customers_url = urljoin(base_url, customers_path)
        self.logger = logger or Logger()
        self.error_handler = error_handler or ErrorHandler(self.logger)
        self.data_manager = data_manager or DataManager(logger=self.logger, error_handler=self.error_handler)
        self.rate_limiter = RateLimiter(rate_limit_seconds, self.logger)
        self.robots_parser = RobotsParser(self.logger, self.error_handler)
        self.url_tracker = url_tracker or URLTracker(logger=self.logger, error_handler=self.error_handler)
        
        self.customers_data = []
    
    def scrape_customers_list(self, output_file="customers_list.json", resume=True):
        """Scrape the main customers page to get a list of all customers.
        
        Args:
            output_file: File to save the customer list to
            resume: Whether to resume from a checkpoint
            
        Returns:
            List of customer data
        """
        self.logger.info(f"Starting to scrape customer list from {self.customers_url}")
        
        # Check if crawling is allowed
        if not self.robots_parser.is_allowed('*', self.customers_url):
            self.logger.error(f"Crawling not allowed for {self.customers_url}")
            return []
        
        # Load existing data if resuming
        if resume:
            checkpoint = self.url_tracker.load_checkpoint()
            if checkpoint and checkpoint.get('stage') == 'customers_list':
                self.logger.info("Resuming customer list scraping from checkpoint")
                self.customers_data = self.data_manager.load_data(output_file) or []
                
                # If we have data, we can skip the initial scraping
                if self.customers_data:
                    self.logger.info(f"Loaded {len(self.customers_data)} customers from previous session")
                    return self.customers_data
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)  # Set headless=False for debugging
                page = browser.new_page()
                
                try:
                    self.logger.info(f"Navigating to {self.customers_url}")
                    page.goto(self.customers_url, timeout=60000)
                    
                    # Initial data extraction
                    self._extract_customer_data(page)
                    
                    # Save checkpoint after initial extraction
                    self.url_tracker.save_checkpoint({
                        'stage': 'customers_list',
                        'page_count': 1,
                        'customers_count': len(self.customers_data)
                    })
                    
                    # Save progress
                    self.data_manager.save_data(self.customers_data, output_file)
                    
                    # Click "See More" button until no more results
                    page_count = 1
                    while True:
                        see_more = page.locator("a[data-lbl*='see-more']")
                        if see_more.count() == 0:
                            self.logger.info("No more 'See More' buttons found")
                            break
                            
                        self.logger.info(f"Clicking 'See More' button (page {page_count})")
                        see_more.first.click()
                        time.sleep(5)  # Allow content to load
                        page_count += 1
                        
                        # Extract data from the new page
                        self._extract_customer_data(page)
                        
                        # Save checkpoint after each page
                        self.url_tracker.save_checkpoint({
                            'stage': 'customers_list',
                            'page_count': page_count,
                            'customers_count': len(self.customers_data)
                        })
                        
                        # Save progress after each page
                        self.data_manager.save_data(self.customers_data, output_file)
                    
                    self.logger.info(f"Found {len(self.customers_data)} customer entries")
                    
                except Exception as e:
                    self.error_handler.handle_error(e, "Error during customer list scraping")
                finally:
                    browser.close()
            
            # Final save
            self.data_manager.save_data(self.customers_data, output_file)
            
            # Clear checkpoint since we're done with this stage
            self.url_tracker.clear_checkpoint()
            
            return self.customers_data
            
        except Exception as e:
            self.error_handler.handle_error(e, "Failed to initialize browser")
            return []
    
    def _extract_customer_data(self, page):
        """Extract customer data from the current page.
        
        Args:
            page: Playwright page object
        """
        try:
            soup = BeautifulSoup(page.content(), 'html.parser')
            li_elements = soup.find_all("li", class_="rc05w3")
            
            self.logger.info(f"Found {len(li_elements)} customer entries on current page")
            
            for li in li_elements:
                try:
                    entry = {}
                    
                    # Extract title
                    title_element = li.find("div", class_="rc05heading")
                    entry["title"] = title_element.text.strip() if title_element else None
                    
                    # Extract industry
                    industry_element = li.find("span", class_="rc05def")
                    entry["industry"] = industry_element["title"] if industry_element and "title" in industry_element.attrs else None
                    
                    # Extract location
                    location_spans = li.find_all("span", class_="rc05def")
                    entry["location"] = location_spans[1]["title"] if len(location_spans) > 1 and "title" in location_spans[1].attrs else None
                    
                    # Extract company name and link
                    link_element = li.find("a")
                    if link_element:
                        entry["company"] = link_element.get("data-lbl")
                        link = link_element.get("href")
                        if link and not link.startswith(("http://", "https://")):
                            link = urljoin(self.base_url, link)
                        entry["link"] = link
                    
                    # Only add if we have a valid link and it's not already in our data
                    if entry.get("link") and not any(item.get("link") == entry.get("link") for item in self.customers_data):
                        self.customers_data.append(entry)
                
                except Exception as e:
                    self.error_handler.handle_error(e, "Error extracting customer data from list item")
        
        except Exception as e:
            self.error_handler.handle_error(e, "Error parsing page content")
