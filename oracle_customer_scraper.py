import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import time
from urllib.parse import urljoin

from logger import Logger
from error_handler import ErrorHandler
from data_manager import DataManager
from rate_limiter import RateLimiter
from robots_parser import RobotsParser

class OracleCustomerScraper:
    """Scrape Oracle customer success stories."""
    
    def __init__(self, base_url="https://www.oracle.com", customers_path="/customers/", 
                 rate_limit_seconds=3, logger=None, error_handler=None, data_manager=None):
        """Initialize the Oracle customer scraper.
        
        Args:
            base_url: Base URL for Oracle website
            customers_path: Path to customers page
            rate_limit_seconds: Time to wait between requests
            logger: Logger instance
            error_handler: ErrorHandler instance
            data_manager: DataManager instance
        """
        self.base_url = base_url
        self.customers_url = urljoin(base_url, customers_path)
        self.logger = logger or Logger()
        self.error_handler = error_handler or ErrorHandler(self.logger)
        self.data_manager = data_manager or DataManager(logger=self.logger, error_handler=self.error_handler)
        self.rate_limiter = RateLimiter(rate_limit_seconds, self.logger)
        self.robots_parser = RobotsParser(self.logger, self.error_handler)
        
        self.visited_urls = set()
        self.customers_data = []
    
    def scrape_customers_list(self, output_file="customers_list.json"):
        """Scrape the main customers page to get a list of all customers.
        
        Args:
            output_file: File to save the customer list to
            
        Returns:
            List of customer data
        """
        self.logger.info(f"Starting to scrape customer list from {self.customers_url}")
        
        # Check if crawling is allowed
        if not self.robots_parser.is_allowed('*', self.customers_url):
            self.logger.error(f"Crawling not allowed for {self.customers_url}")
            return []
        
        if self.customers_url in self.visited_urls:
            self.logger.info(f"Already visited {self.customers_url}, skipping")
            return self.customers_data
            
        self.visited_urls.add(self.customers_url)
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)  # Set headless=False for debugging
                page = browser.new_page()
                
                try:
                    self.logger.info(f"Navigating to {self.customers_url}")
                    page.goto(self.customers_url, timeout=60000)
                    
                    # Initial data extraction
                    self._extract_customer_data(page)
                    
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
                        
                        # Save progress after each page
                        self.data_manager.save_data(self.customers_data, output_file)
                    
                    self.logger.info(f"Found {len(self.customers_data)} customer entries")
                    
                except Exception as e:
                    self.error_handler.handle_error(e, "Error during customer list scraping")
                finally:
                    browser.close()
            
            # Final save
            self.data_manager.save_data(self.customers_data, output_file)
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
