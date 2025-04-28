import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import time
import re
import json
import concurrent.futures
import threading
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Any, Set

from logger import Logger
from error_handler import ErrorHandler
from data_manager import DataManager
from rate_limiter import RateLimiter
from robots_parser import RobotsParser
from url_tracker import URLTracker
from utils import safe_request, retry_request

class JobListingsScraper:
    """Scrape job listings from company career pages."""
    
    def __init__(self, rate_limit_seconds=3, logger=None, error_handler=None, data_manager=None, url_tracker=None):
        """Initialize the job listings scraper.
        
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
        
        self.job_listings = []
        self.lock = threading.Lock()  # Lock for thread safety
        
        # Common job listing page identifiers
        self.job_page_identifiers = [
            "jobs", "careers", "positions", "vacancies", "opportunities", 
            "openings", "join", "work with us", "current openings", "apply"
        ]
        
        # Common job listing element selectors
        self.job_listing_selectors = [
            ".job-listing", ".job-item", ".job-card", ".job-posting", ".vacancy",
            ".career-opportunity", ".position", ".job", "div[data-job]", "li[data-job]",
            "tr.job-row", "div.job-row", "article.job"
        ]
    
    def scrape_job_listings(self, career_urls_file="career_urls.json", output_file="job_listings.json", 
                           limit=None, max_workers=5, resume=True):
        """Scrape job listings from company career pages.
        
        Args:
            career_urls_file: File containing career URLs
            output_file: File to save the job listings to
            limit: Maximum number of career URLs to process
            max_workers: Maximum number of worker threads
            resume: Whether to resume from a checkpoint
            
        Returns:
            List of job listings
        """
        # Load career URLs
        career_data = self.data_manager.load_data(career_urls_file)
        if not career_data:
            self.logger.error(f"No career URLs found in {career_urls_file}")
            return []
        
        self.logger.info(f"Loaded {len(career_data)} career URLs")
        
        # Load existing job listings if resuming
        if resume:
            checkpoint = self.url_tracker.load_checkpoint()
            if checkpoint and checkpoint.get('stage') == 'job_listings':
                self.logger.info("Resuming job listings scraping from checkpoint")
                self.job_listings = self.data_manager.load_data(output_file) or []
                
                # Get the last processed index
                last_index = checkpoint.get('last_index', 0)
                
                # If we have data and haven't finished, we can skip some career URLs
                if self.job_listings and last_index < len(career_data):
                    self.logger.info(f"Loaded {len(self.job_listings)} job listings from previous session")
                    self.logger.info(f"Resuming from index {last_index}")
                    
                    # Adjust career_data to start from where we left off
                    career_data = career_data[last_index:]
        
        # Process only a subset if limit is specified
        career_urls_to_process = career_data[:limit] if limit is not None else career_data
        
        # Filter out already visited URLs
        unvisited_career_urls = []
        for career_url_data in career_urls_to_process:
            career_url = career_url_data.get("career_url")
            if career_url and not self.url_tracker.is_job_url_visited(career_url + "_listings"):
                unvisited_career_urls.append(career_url_data)
        
        self.logger.info(f"Scraping job listings from {len(unvisited_career_urls)} unvisited career URLs out of {len(career_urls_to_process)} total")
        
        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks
            future_to_career = {
                executor.submit(self._process_career_url, i, career_url_data, len(unvisited_career_urls), output_file): career_url_data
                for i, career_url_data in enumerate(unvisited_career_urls)
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_career):
                career_url_data = future_to_career[future]
                try:
                    future.result()  # This will re-raise any exceptions from the thread
                except Exception as e:
                    self.error_handler.handle_error(e, f"Error processing career URL {career_url_data.get('career_url')}")
        
        # Final save
        self.data_manager.save_data(self.job_listings, output_file)
        
        # Clear checkpoint since we're done with this stage
        self.url_tracker.clear_checkpoint()
        
        return self.job_listings
    
    def _process_career_url(self, index, career_url_data, total, output_file):
        """Process a single career URL.
        
        Args:
            index: Index of the career URL in the list
            career_url_data: Career URL data
            total: Total number of career URLs
            output_file: File to save the job listings to
        """
        career_url = career_url_data.get("career_url")
        company_name = career_url_data.get("company_name")
        
        if not career_url:
            self.logger.warning(f"No career URL found for {company_name}")
            return
        
        # Mark this URL as visited to avoid re-scraping
        visit_key = career_url + "_listings"
        if self.url_tracker.is_job_url_visited(visit_key):
            self.logger.info(f"Already visited {career_url}, skipping")
            return
        
        self.logger.info(f"Processing career URL {index+1}/{total}: {company_name} - {career_url}")
        
        # Check if crawling is allowed
        if not self.robots_parser.is_allowed('*', career_url):
            self.logger.warning(f"Crawling not allowed for {career_url}")
            return
        
        try:
            # Apply rate limiting
            self.rate_limiter.limit()
            
            # First try to find job listings on the career page itself
            job_listings = self._scrape_job_listings_from_url(career_url, company_name)
            
            # If no job listings found, try to find job listing pages
            if not job_listings:
                job_listing_urls = self._find_job_listing_pages(career_url)
                
                for job_url in job_listing_urls:
                    # Apply rate limiting
                    self.rate_limiter.limit()
                    
                    # Scrape job listings from the job listing page
                    page_listings = self._scrape_job_listings_from_url(job_url, company_name)
                    job_listings.extend(page_listings)
            
            if job_listings:
                self.logger.info(f"Found {len(job_listings)} job listings for {company_name}")
                
                # Thread-safe update of job_listings list
                with self.lock:
                    self.job_listings.extend(job_listings)
                    
                    # Save checkpoint
                    self.url_tracker.save_checkpoint({
                        'stage': 'job_listings',
                        'last_index': index,
                        'job_listings_count': len(self.job_listings)
                    })
                    
                    # Save after each successful scrape to avoid losing data
                    self.data_manager.save_data(self.job_listings, output_file)
            else:
                self.logger.warning(f"No job listings found for {company_name}")
            
            # Mark this URL as visited
            self.url_tracker.add_job_url(visit_key)
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Error processing career URL {career_url}")
    
    def _find_job_listing_pages(self, career_url):
        """Find job listing pages from a career URL.
        
        Args:
            career_url: URL of the career page
            
        Returns:
            List of job listing page URLs
        """
        job_listing_urls = []
        
        try:
            self.logger.info(f"Looking for job listing pages on {career_url}")
            response = safe_request(career_url)
            
            if not response:
                self.logger.error(f"Failed to fetch {career_url}")
                return job_listing_urls
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for links to job listing pages
            for link in soup.find_all('a'):
                href = link.get('href')
                text = link.get_text().lower().strip()
                
                if not href:
                    continue
                
                # Check if the link text contains job listing page identifiers
                if any(identifier in text for identifier in self.job_page_identifiers):
                    # Make sure the URL is absolute
                    if not href.startswith(('http://', 'https://')):
                        href = urljoin(career_url, href)
                    
                    self.logger.info(f"Found job listing page: {href}")
                    job_listing_urls.append(href)
            
            # If no job listing pages found, try to find iframes that might contain job listings
            if not job_listing_urls:
                for iframe in soup.find_all('iframe'):
                    src = iframe.get('src')
                    if src:
                        # Make sure the URL is absolute
                        if not src.startswith(('http://', 'https://')):
                            src = urljoin(career_url, src)
                        
                        # Check if the iframe src contains job listing page identifiers
                        if any(identifier in src.lower() for identifier in self.job_page_identifiers):
                            self.logger.info(f"Found job listing iframe: {src}")
                            job_listing_urls.append(src)
            
            return job_listing_urls
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Error finding job listing pages for {career_url}")
            return job_listing_urls
    
    def _scrape_job_listings_from_url(self, url, company_name):
        """Scrape job listings from a URL.
        
        Args:
            url: URL to scrape
            company_name: Name of the company
            
        Returns:
            List of job listings
        """
        job_listings = []
        
        try:
            self.logger.info(f"Scraping job listings from {url}")
            
            # Try with regular requests first
            response = safe_request(url)
            
            if not response:
                self.logger.error(f"Failed to fetch {url}")
                return job_listings
            
            # Check if the page is likely to be a job board
            is_job_board = self._is_job_board(response.content)
            
            if is_job_board:
                # Try to extract job listings with BeautifulSoup
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Try different selectors for job listings
                for selector in self.job_listing_selectors:
                    job_elements = soup.select(selector)
                    
                    if job_elements:
                        self.logger.info(f"Found {len(job_elements)} job listings using selector: {selector}")
                        
                        for job_element in job_elements:
                            job_data = self._extract_job_data(job_element, url, company_name)
                            if job_data:
                                job_listings.append(job_data)
                        
                        # If we found job listings with this selector, no need to try others
                        break
                
                # If no job listings found with selectors, try a more generic approach
                if not job_listings:
                    job_listings = self._extract_job_listings_generic(soup, url, company_name)
                
                # If still no job listings found, try with Playwright for JavaScript-heavy pages
                if not job_listings:
                    self.logger.info(f"No job listings found with BeautifulSoup, trying with Playwright")
                    job_listings = self._scrape_job_listings_with_playwright(url, company_name)
            else:
                # Not a job board, try with Playwright
                self.logger.info(f"Page doesn't appear to be a job board, trying with Playwright")
                job_listings = self._scrape_job_listings_with_playwright(url, company_name)
            
            return job_listings
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Error scraping job listings from {url}")
            return job_listings
    
    def _is_job_board(self, content):
        """Check if a page is likely to be a job board.
        
        Args:
            content: Page content
            
        Returns:
            True if the page is likely to be a job board, False otherwise
        """
        try:
            # Convert content to string if it's bytes
            if isinstance(content, bytes):
                content_str = content.decode('utf-8', errors='ignore')
            else:
                content_str = content
            
            # Check for common job board indicators
            job_board_indicators = [
                "job", "career", "position", "vacancy", "opening", "apply",
                "job title", "job description", "requirements", "qualifications",
                "full-time", "part-time", "remote", "location", "department"
            ]
            
            # Count how many indicators are present
            indicator_count = sum(1 for indicator in job_board_indicators if indicator in content_str.lower())
            
            # If at least 3 indicators are present, it's likely a job board
            return indicator_count >= 3
            
        except Exception:
            # If there's an error, assume it's not a job board
            return False
    
    def _extract_job_data(self, job_element, base_url, company_name):
        """Extract job data from a job listing element.
        
        Args:
            job_element: BeautifulSoup element containing job data
            base_url: Base URL for resolving relative URLs
            company_name: Name of the company
            
        Returns:
            Dictionary containing job data
        """
        try:
            job_data = {
                "company_name": company_name,
                "source_url": base_url
            }
            
            # Extract job title
            title_element = job_element.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']) or job_element.find(class_=lambda c: c and any(x in c.lower() for x in ['title', 'position', 'role']))
            if title_element:
                job_data["title"] = title_element.get_text().strip()
            else:
                # Try to find the most prominent text in the element
                texts = [t for t in job_element.stripped_strings]
                if texts:
                    job_data["title"] = texts[0]
            
            # If no title found, skip this job
            if not job_data.get("title"):
                return None
            
            # Extract job URL
            link_element = job_element.find('a')
            if link_element and link_element.get('href'):
                href = link_element.get('href')
                if not href.startswith(('http://', 'https://')):
                    href = urljoin(base_url, href)
                job_data["url"] = href
            
            # Extract job location
            location_element = job_element.find(string=re.compile(r'location|city|state|country', re.I)) or job_element.find(class_=lambda c: c and 'location' in c.lower())
            if location_element:
                if location_element.parent and location_element.parent.name != 'a':
                    # If the location is not a link, get the text after the label
                    location_text = location_element.get_text().strip()
                    # Try to extract just the location part
                    location_match = re.search(r'(?:location|city|state|country)[:\s]+(.+)', location_text, re.I)
                    if location_match:
                        job_data["location"] = location_match.group(1).strip()
                    else:
                        job_data["location"] = location_text
                else:
                    # If the location is a link, get the next sibling's text
                    next_sibling = location_element.next_sibling
                    if next_sibling:
                        job_data["location"] = next_sibling.strip()
            
            # Extract job department
            department_element = job_element.find(string=re.compile(r'department|team|division', re.I)) or job_element.find(class_=lambda c: c and 'department' in c.lower())
            if department_element:
                if department_element.parent and department_element.parent.name != 'a':
                    # If the department is not a link, get the text after the label
                    department_text = department_element.get_text().strip()
                    # Try to extract just the department part
                    department_match = re.search(r'(?:department|team|division)[:\s]+(.+)', department_text, re.I)
                    if department_match:
                        job_data["department"] = department_match.group(1).strip()
                    else:
                        job_data["department"] = department_text
                else:
                    # If the department is a link, get the next sibling's text
                    next_sibling = department_element.next_sibling
                    if next_sibling:
                        job_data["department"] = next_sibling.strip()
            
            # Extract job type (full-time, part-time, etc.)
            job_type_element = job_element.find(string=re.compile(r'type|time|employment', re.I)) or job_element.find(class_=lambda c: c and 'type' in c.lower())
            if job_type_element:
                if job_type_element.parent and job_type_element.parent.name != 'a':
                    # If the job type is not a link, get the text after the label
                    job_type_text = job_type_element.get_text().strip()
                    # Try to extract just the job type part
                    job_type_match = re.search(r'(?:type|time|employment)[:\s]+(.+)', job_type_text, re.I)
                    if job_type_match:
                        job_data["job_type"] = job_type_match.group(1).strip()
                    else:
                        job_data["job_type"] = job_type_text
                else:
                    # If the job type is a link, get the next sibling's text
                    next_sibling = job_type_element.next_sibling
                    if next_sibling:
                        job_data["job_type"] = next_sibling.strip()
            
            # Extract job description
            description_element = job_element.find(class_=lambda c: c and 'description' in c.lower())
            if description_element:
                job_data["description"] = description_element.get_text().strip()
            
            # Extract job date
            date_element = job_element.find(string=re.compile(r'date|posted', re.I)) or job_element.find(class_=lambda c: c and ('date' in c.lower() or 'posted' in c.lower()))
            if date_element:
                if date_element.parent and date_element.parent.name != 'a':
                    # If the date is not a link, get the text after the label
                    date_text = date_element.get_text().strip()
                    # Try to extract just the date part
                    date_match = re.search(r'(?:date|posted)[:\s]+(.+)', date_text, re.I)
                    if date_match:
                        job_data["date_posted"] = date_match.group(1).strip()
                    else:
                        job_data["date_posted"] = date_text
                else:
                    # If the date is a link, get the next sibling's text
                    next_sibling = date_element.next_sibling
                    if next_sibling:
                        job_data["date_posted"] = next_sibling.strip()
            
            return job_data
            
        except Exception as e:
            self.error_handler.handle_error(e, "Error extracting job data")
            return None
    
    def _extract_job_listings_generic(self, soup, base_url, company_name):
        """Extract job listings using a more generic approach.
        
        Args:
            soup: BeautifulSoup object
            base_url: Base URL for resolving relative URLs
            company_name: Name of the company
            
        Returns:
            List of job listings
        """
        job_listings = []
        
        try:
            # Look for tables that might contain job listings
            tables = soup.find_all('table')
            for table in tables:
                # Check if this table is likely to contain job listings
                headers = [th.get_text().strip().lower() for th in table.find_all('th')]
                if headers and any(header in ['title', 'position', 'job', 'role'] for header in headers):
                    # This table likely contains job listings
                    rows = table.find_all('tr')[1:]  # Skip header row
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            job_data = {
                                "company_name": company_name,
                                "source_url": base_url
                            }
                            
                            # Try to map cells to job data based on headers
                            for i, header in enumerate(headers):
                                if i < len(cells):
                                    cell_text = cells[i].get_text().strip()
                                    if 'title' in header or 'position' in header or 'job' in header or 'role' in header:
                                        job_data["title"] = cell_text
                                    elif 'location' in header:
                                        job_data["location"] = cell_text
                                    elif 'department' in header or 'team' in header:
                                        job_data["department"] = cell_text
                                    elif 'type' in header or 'time' in header:
                                        job_data["job_type"] = cell_text
                                    elif 'date' in header or 'posted' in header:
                                        job_data["date_posted"] = cell_text
                            
                            # Extract job URL
                            link = row.find('a')
                            if link and link.get('href'):
                                href = link.get('href')
                                if not href.startswith(('http://', 'https://')):
                                    href = urljoin(base_url, href)
                                job_data["url"] = href
                            
                            # Only add if we have a title
                            if job_data.get("title"):
                                job_listings.append(job_data)
            
            # If no job listings found in tables, look for lists
            if not job_listings:
                lists = soup.find_all(['ul', 'ol'])
                for list_element in lists:
                    # Check if this list is likely to contain job listings
                    list_items = list_element.find_all('li')
                    if list_items and all(li.find('a') for li in list_items[:5]):
                        # This list likely contains job listings
                        for item in list_items:
                            link = item.find('a')
                            if link:
                                job_data = {
                                    "company_name": company_name,
                                    "source_url": base_url,
                                    "title": link.get_text().strip()
                                }
                                
                                href = link.get('href')
                                if href and not href.startswith(('http://', 'https://')):
                                    href = urljoin(base_url, href)
                                job_data["url"] = href
                                
                                # Try to extract location if it's in the list item
                                item_text = item.get_text()
                                location_match = re.search(r'$$([^)]+)$$', item_text)
                                if location_match:
                                    job_data["location"] = location_match.group(1).strip()
                                
                                # Only add if we have a title
                                if job_data.get("title"):
                                    job_listings.append(job_data)
            
            return job_listings
            
        except Exception as e:
            self.error_handler.handle_error(e, "Error extracting job listings with generic approach")
            return job_listings
    
    def _scrape_job_listings_with_playwright(self, url, company_name):
        """Scrape job listings using Playwright for JavaScript-heavy pages.
        
        Args:
            url: URL to scrape
            company_name: Name of the company
            
        Returns:
            List of job listings
        """
        job_listings = []
        
        try:
            self.logger.info(f"Scraping job listings with Playwright from {url}")
            
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                try:
                    page.goto(url, timeout=60000)
                    
                    # Wait for the page to load
                    page.wait_for_timeout(5000)
                    
                    # Try to find and click "Show all jobs" or similar buttons
                    show_all_buttons = [
                        "text=Show all", "text=View all", "text=See all", 
                        "text=All jobs", "text=All positions", "text=All openings"
                    ]
                    
                    for button_selector in show_all_buttons:
                        try:
                            if page.locator(button_selector).count() > 0:
                                self.logger.info(f"Clicking '{button_selector}' button")
                                page.click(button_selector)
                                page.wait_for_timeout(3000)
                                break
                        except:
                            pass
                    
                    # Handle pagination if present
                    has_pagination = False
                    pagination_selectors = [
                        "a.pagination", "ul.pagination", "div.pagination",
                        "a.pager", "ul.pager", "div.pager",
                        "button:has-text('Next')", "a:has-text('Next')"
                    ]
                    
                    for selector in pagination_selectors:
                        if page.locator(selector).count() > 0:
                            has_pagination = True
                            break
                    
                    if has_pagination:
                        self.logger.info("Pagination detected, processing all pages")
                        
                        page_num = 1
                        max_pages = 20  # Limit to 20 pages to avoid infinite loops
                        
                        while page_num <= max_pages:
                            # Extract job listings from current page
                            current_content = page.content()
                            soup = BeautifulSoup(current_content, 'html.parser')
                            
                            # Try different selectors for job listings
                            page_listings = []
                            for selector in self.job_listing_selectors:
                                job_elements = soup.select(selector)
                                
                                if job_elements:
                                    self.logger.info(f"Found {len(job_elements)} job listings on page {page_num} using selector: {selector}")
                                    
                                    for job_element in job_elements:
                                        job_data = self._extract_job_data(job_element, url, company_name)
                                        if job_data:
                                            page_listings.append(job_data)
                                    
                                    # If we found job listings with this selector, no need to try others
                                    break
                            
                            # If no job listings found with selectors, try a more generic approach
                            if not page_listings:
                                page_listings = self._extract_job_listings_generic(soup, url, company_name)
                            
                            job_listings.extend(page_listings)
                            
                            # Try to click the "Next" button
                            next_button_selectors = [
                                "button:has-text('Next')", "a:has-text('Next')",
                                "button.next", "a.next", "li.next a",
                                "button[aria-label='Next']", "a[aria-label='Next']"
                            ]
                            
                            clicked_next = False
                            for selector in next_button_selectors:
                                try:
                                    next_button = page.locator(selector)
                                    if next_button.count() > 0 and next_button.is_enabled() and next_button.is_visible():
                                        self.logger.info(f"Clicking 'Next' button (page {page_num})")
                                        next_button.click()
                                        page.wait_for_timeout(3000)
                                        clicked_next = True
                                        break
                                except:
                                    pass
                            
                            if not clicked_next:
                                self.logger.info(f"No more 'Next' button found, stopping at page {page_num}")
                                break
                            
                            page_num += 1
                    else:
                        # No pagination, just extract job listings from the current page
                        current_content = page.content()
                        soup = BeautifulSoup(current_content, 'html.parser')
                        
                        # Try different selectors for job listings
                        for selector in self.job_listing_selectors:
                            job_elements = soup.select(selector)
                            
                            if job_elements:
                                self.logger.info(f"Found {len(job_elements)} job listings using selector: {selector}")
                                
                                for job_element in job_elements:
                                    job_data = self._extract_job_data(job_element, url, company_name)
                                    if job_data:
                                        job_listings.append(job_data)
                                
                                # If we found job listings with this selector, no need to try others
                                break
                        
                        # If no job listings found with selectors, try a more generic approach
                        if not job_listings:
                            job_listings = self._extract_job_listings_generic(soup, url, company_name)
                
                except Exception as e:
                    self.error_handler.handle_error(e, f"Error scraping job listings with Playwright from {url}")
                finally:
                    browser.close()
            
            return job_listings
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Error initializing Playwright for {url}")
            return job_listings
