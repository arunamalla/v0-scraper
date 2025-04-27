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
from utils import safe_request, retry_request

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
                    consecutive_failures = 0
                    max_consecutive_failures = 5
                    max_pages = 1000  # Set a high limit to ensure we get all customers

                    while page_count < max_pages:
                        try:
                            see_more = page.locator("a[data-lbl*='see-more']")
                            if see_more.count() == 0:
                                self.logger.info("No more 'See More' buttons found")
                                break
                                
                            self.logger.info(f"Clicking 'See More' button (page {page_count})")
                            see_more.first.click()
                            
                            # Wait for new content to load with increased timeout
                            page.wait_for_timeout(8000)  # Wait 8 seconds for content to load
                            
                            # Check if new content was loaded by comparing customer count before and after
                            customers_before = len(self.customers_data)
                            self._extract_customer_data(page)
                            customers_after = len(self.customers_data)
                            
                            if customers_after > customers_before:
                                # New customers were found, reset failure counter
                                consecutive_failures = 0
                                self.logger.info(f"Found {customers_after - customers_before} new customers (total: {customers_after})")
                            else:
                                # No new customers found, increment failure counter
                                consecutive_failures += 1
                                self.logger.warning(f"No new customers found after clicking 'See More'. Attempt {consecutive_failures}/{max_consecutive_failures}")
                                
                                if consecutive_failures >= max_consecutive_failures:
                                    self.logger.warning(f"Reached maximum consecutive failures ({max_consecutive_failures}). Stopping pagination.")
                                    break
                            
                            page_count += 1
                            
                            # Save checkpoint after each page
                            self.url_tracker.save_checkpoint({
                                'stage': 'customers_list',
                                'page_count': page_count,
                                'customers_count': len(self.customers_data)
                            })
                            
                            # Save progress after each page
                            self.data_manager.save_data(self.customers_data, output_file)
                            
                        except Exception as e:
                            self.error_handler.handle_error(e, f"Error during pagination (page {page_count})")
                            consecutive_failures += 1
                            
                            if consecutive_failures >= max_consecutive_failures:
                                self.logger.warning(f"Reached maximum consecutive failures ({max_consecutive_failures}). Stopping pagination.")
                                break
                                
                            # Try to recover by refreshing the page if we've had multiple failures
                            if consecutive_failures > 2:
                                try:
                                    self.logger.info("Attempting to refresh the page to recover")
                                    page.reload()
                                    page.wait_for_timeout(5000)  # Wait for page to reload
                                except Exception as refresh_error:
                                    self.error_handler.handle_error(refresh_error, "Error refreshing page")
                    
                    self.logger.info(f"Found {len(self.customers_data)} customer entries")
                    
                except Exception as e:
                    self.error_handler.handle_error(e, "Error during customer list scraping")
                finally:
                    browser.close()
            
            # Final save
            self.data_manager.save_data(self.customers_data, output_file)
            
            # Clear checkpoint since we're done with this stage
            self.url_tracker.clear_checkpoint()
            
            # If we didn't find many customers, try alternative methods
            if len(self.customers_data) < 50:  # Arbitrary threshold
                self.logger.warning(f"Only found {len(self.customers_data)} customers. Trying alternative methods.")
                self._try_alternative_scraping_methods(output_file)
            
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
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Try different selectors for customer list items
            selectors = [
                "li.rc05w3",  # Original selector
                "div.rc05w1 li",  # Alternative selector
                "div.customer-card",  # Another possible selector
                "div[data-customer]",  # Data attribute selector
                "a[href*='/customers/']"  # Link-based selector
            ]
            
            found_items = False
            
            for selector in selectors:
                elements = soup.select(selector)
                
                if elements:
                    self.logger.info(f"Found {len(elements)} customer entries using selector: {selector}")
                    found_items = True
                    
                    for element in elements:
                        try:
                            entry = {}
                            
                            # Extract title - try different approaches
                            title_element = element.find("div", class_="rc05heading") or element.find("h3") or element.find("h2")
                            if title_element:
                                entry["title"] = title_element.text.strip()
                            elif hasattr(element, 'text'):
                                # If no specific title element, use the text of the element itself
                                entry["title"] = element.text.strip()
                            
                            # Extract industry
                            industry_element = element.find("span", class_="rc05def") or element.find("span", class_="industry")
                            if industry_element and "title" in industry_element.attrs:
                                entry["industry"] = industry_element["title"]
                            elif industry_element:
                                entry["industry"] = industry_element.text.strip()
                            
                            # Extract location
                            location_spans = element.find_all("span", class_="rc05def") or element.find_all("span", class_="location")
                            if len(location_spans) > 1 and "title" in location_spans[1].attrs:
                                entry["location"] = location_spans[1]["title"]
                            elif len(location_spans) > 1:
                                entry["location"] = location_spans[1].text.strip()
                            
                            # Extract link
                            link_element = element.find("a") if element.name != 'a' else element
                            if link_element:
                                if element.name == 'a':
                                    entry["company"] = link_element.text.strip()
                                else:
                                    entry["company"] = link_element.get("data-lbl", "")
                                
                                link = link_element.get("href", "")
                                if link and not link.startswith(("http://", "https://")):
                                    link = urljoin(self.base_url, link)
                                entry["link"] = link
                            
                            # Only add if we have a valid link and it's not already in our data
                            if entry.get("link") and not any(item.get("link") == entry.get("link") for item in self.customers_data):
                                self.customers_data.append(entry)
                        
                        except Exception as e:
                            self.error_handler.handle_error(e, "Error extracting customer data from element")
                    
                    # If we found items with this selector, no need to try others
                    break
            
            if not found_items:
                self.logger.warning("No customer entries found with any selector")
                
                # Try to extract any links that might be customer links
                for link in soup.find_all("a"):
                    href = link.get("href", "")
                    if '/customers/' in href and not href.endswith('/customers/'):
                        # This might be a customer link
                        full_url = urljoin(self.base_url, href)
                        if not any(item.get("link") == full_url for item in self.customers_data):
                            self.customers_data.append({
                                "title": link.text.strip() or href.split('/')[-1].replace('-', ' ').title(),
                                "link": full_url
                            })
        
        except Exception as e:
            self.error_handler.handle_error(e, "Error parsing page content")

    def _try_alternative_scraping_methods(self, output_file):
        """Try alternative methods to scrape customers if the main method fails.
        
        Args:
            output_file: File to save the customer list to
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("Trying alternative scraping methods")
        
        # Method 1: Try to access the sitemap
        try:
            self.logger.info("Attempting to scrape from sitemap")
            sitemap_url = f"{self.base_url}/sitemap.xml"
            response = safe_request(sitemap_url)
            
            if response and response.status_code == 200:
                soup = BeautifulSoup(response.content, 'xml')
                urls = [loc.text for loc in soup.find_all('loc') if '/customers/' in loc.text]
                
                if urls:
                    self.logger.info(f"Found {len(urls)} customer URLs in sitemap")
                    
                    # Process each URL
                    for url in urls:
                        if not any(item.get("link") == url for item in self.customers_data):
                            # Extract customer name from URL
                            customer_name = url.split('/')[-1].replace('-', ' ').title()
                            
                            entry = {
                                "title": customer_name,
                                "link": url
                            }
                            
                            self.customers_data.append(entry)
                    
                    # Save the data
                    self.data_manager.save_data(self.customers_data, output_file)
                    return True
        except Exception as e:
            self.error_handler.handle_error(e, "Error scraping from sitemap")
        
        # Method 2: Try to access the customer stories API if it exists
        try:
            self.logger.info("Attempting to scrape from API")
            api_url = f"{self.base_url}/api/customers"
            response = safe_request(api_url)
            
            if response and response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 0:
                        self.logger.info(f"Found {len(data)} customers from API")
                        
                        for customer in data:
                            if isinstance(customer, dict):
                                entry = {
                                    "title": customer.get("name", "Unknown"),
                                    "industry": customer.get("industry"),
                                    "location": customer.get("location"),
                                    "link": urljoin(self.base_url, customer.get("url", ""))
                                }
                                
                                if entry["link"] and not any(item.get("link") == entry["link"] for item in self.customers_data):
                                    self.customers_data.append(entry)
                        
                        # Save the data
                        self.data_manager.save_data(self.customers_data, output_file)
                        return True
                except:
                    pass
        except Exception as e:
            self.error_handler.handle_error(e, "Error scraping from API")
        
        return False
