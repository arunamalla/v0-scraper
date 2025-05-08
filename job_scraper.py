import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
import concurrent.futures
import threading
from typing import List, Dict, Any, Set

from logger import Logger
from error_handler import ErrorHandler
from data_manager import DataManager
from rate_limiter import RateLimiter
from robots_parser import RobotsParser
from url_tracker import URLTracker
from utils import safe_request

class JobScraper:
    """Scrape career URLs from customer websites."""
    
    def __init__(self, rate_limit_seconds=3, logger=None, error_handler=None, data_manager=None, url_tracker=None):
        """Initialize the job scraper.
        
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
        
        self.career_data = []
        self.lock = threading.Lock()  # Lock for thread safety
        
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
    
    def scrape_career_urls(self, customer_details, output_file="career_urls.json", limit=None, max_workers=5, resume=True):
        """Scrape career URLs from customer websites.
        
        Args:
            customer_details: List of customer details from CustomerDetailScraper
            output_file: File to save the career URLs to
            limit: Maximum number of customers to process
            max_workers: Maximum number of worker threads
            resume: Whether to resume from a checkpoint
            
        Returns:
            List of career data
        """
        if not customer_details:
            self.logger.error("No customer details provided")
            return []
        
        # Load existing data if resuming
        if resume:
            checkpoint = self.url_tracker.load_checkpoint()
            if checkpoint and checkpoint.get('stage') == 'career_urls':
                self.logger.info("Resuming career URL scraping from checkpoint")
                self.career_data = self.data_manager.load_data(output_file) or []
                
                # Get the last processed index
                last_index = checkpoint.get('last_index', 0)
                
                # If we have data and haven't finished, we can skip some customers
                if self.career_data and last_index < len(customer_details):
                    self.logger.info(f"Loaded {len(self.career_data)} career URLs from previous session")
                    self.logger.info(f"Resuming from index {last_index}")
                    
                    # Adjust customer_details to start from where we left off
                    customer_details = customer_details[last_index:]
        
        # Process only a subset if limit is specified
        customers_to_process = customer_details[:limit] if limit is not None else customer_details
        
        # Filter out customers without company URLs
        customers_with_urls = []
        for customer in customers_to_process:
            company_url = customer.get("company_url")
            if company_url:
                # Check if we've already found a career URL for this company
                if not any(career.get("company_url") == company_url for career in self.career_data):
                    customers_with_urls.append(customer)
        
        self.logger.info(f"Scraping career URLs for {len(customers_with_urls)} companies out of {len(customers_to_process)} total")
        
        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks
            future_to_customer = {
                executor.submit(self._process_company, i, customer, len(customers_with_urls), output_file): customer
                for i, customer in enumerate(customers_with_urls)
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_customer):
                customer = future_to_customer[future]
                try:
                    future.result()  # This will re-raise any exceptions from the thread
                except Exception as e:
                    self.error_handler.handle_error(e, f"Error processing company {customer.get('company_name')}")
        
        # Final save
        self.data_manager.save_data(self.career_data, output_file)
        
        # Clear checkpoint since we're done with this stage
        self.url_tracker.clear_checkpoint()
        
        return self.career_data
    
    def _process_company(self, index, customer, total, output_file):
        """Process a single company.
        
        Args:
            index: Index of the company in the list
            customer: Customer data
            total: Total number of companies
            output_file: File to save the career URLs to
        """
        company_url = customer.get("company_url")
        company_name = customer.get("company_name") or customer.get("title")
        
        if not company_url:
            self.logger.warning(f"No company URL found for {company_name}")
            return
        
        # Check if we've already visited this URL
        if self.url_tracker.is_job_url_visited(company_url):
            self.logger.info(f"Already visited {company_url}, skipping")
            return
            
        self.logger.info(f"Processing company {index+1}/{total}: {company_name}")
        
        # Check if crawling is allowed
        if not self.robots_parser.is_allowed('*', company_url):
            self.logger.warning(f"Crawling not allowed for {company_url}")
            return
        
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
                
                # Thread-safe update of career_data list
                with self.lock:
                    self.career_data.append(career_data)
                    
                    # Save checkpoint
                    self.url_tracker.save_checkpoint({
                        'stage': 'career_urls',
                        'last_index': index,
                        'careers_count': len(self.career_data)
                    })
                    
                    # Save after each successful scrape to avoid losing data
                    self.data_manager.save_data(self.career_data, output_file)
                
                # Mark this URL as visited
                self.url_tracker.add_job_url(company_url)
                
        except Exception as e:
            self.error_handler.handle_error(e, f"Error processing company {company_name}")
    
    def _find_career_url(self, company_url, company_name):
        """Find the career URL for a company.
        
        Args:
            company_url: URL of the company website
            company_name: Name of the company
            
        Returns:
            Career URL if found, None otherwise
        """
        try:
            self.logger.info(f"Fetching {company_url}")
            response = safe_request(company_url, timeout=30)
        
            if not response:
                self.logger.error(f"Failed to fetch {company_url}")
                return None
        
            soup = BeautifulSoup(response.content, 'html.parser')
        
            # Method 1: Look for common career page links with expanded keywords
            career_keywords = [
                'career', 'careers', 'jobs', 'job', 'join', 'work with us', 'employment', 
                'opportunities', 'join our team', 'work for us', 'join the team', 
                'current openings', 'open positions', 'job opportunities', 'career opportunities',
                'we\'re hiring', 'job openings', 'vacancies', 'positions', 'recruitment',
                'join us', 'apply now', 'apply today', 'job search', 'career search'
            ]
        
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
        
            # Method 2: Check common career URL patterns with expanded patterns
            parsed_url = urlparse(company_url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        
            career_patterns = [
                r'/careers/?$',
                r'/jobs/?$',
                r'/join-us/?$',
                r'/work-with-us/?$',
                r'/employment/?$',
                r'/opportunities/?$',
                r'/join-our-team/?$',
                r'/work-for-us/?$',
                r'/join-the-team/?$',
                r'/current-openings/?$',
                r'/open-positions/?$',
                r'/job-opportunities/?$',
                r'/career-opportunities/?$',
                r'/were-hiring/?$',
                r'/job-openings/?$',
                r'/vacancies/?$',
                r'/positions/?$',
                r'/recruitment/?$',
                r'/apply-now/?$',
                r'/apply-today/?$',
                r'/job-search/?$',
                r'/career-search/?$',
                r'/about-us/careers/?$',
                r'/about/careers/?$',
                r'/company/careers/?$',
                r'/en/careers/?$',
                r'/us/careers/?$'
            ]
        
            for pattern in career_patterns:
                career_url = urljoin(base_url, re.sub(r'^/', '', pattern))
            
                try:
                    # Check if the URL exists
                    self.rate_limiter.limit()
                    head_response = safe_request(career_url, method='head', timeout=10)
                
                    if head_response and head_response.status_code < 400:
                        self.logger.info(f"Found career URL via pattern: {career_url}")
                        return career_url
                except:
                    # Ignore errors when checking pattern URLs
                    pass
        
            # Method 3: Check for links to common job platforms
            job_platform_domains = [
                'workday.com', 'lever.co', 'greenhouse.io', 'recruitee.com',
                'jobvite.com', 'smartrecruiters.com', 'taleo.net', 'brassring.com',
                'icims.com', 'successfactors.com', 'bamboohr.com', 'paylocity.com',
                'applytojob.com', 'recruitingbypaycor.com', 'ultipro.com', 'myworkdayjobs.com',
                'applicantpro.com', 'paycom.com', 'adp.com', 'indeed.com',
                'linkedin.com/company', 'glassdoor.com/Overview', 'monster.com', 'ziprecruiter.com'
            ]
        
            for link in soup.find_all('a'):
                href = link.get('href')
                if not href:
                    continue
                
                if any(platform in href.lower() for platform in job_platform_domains):
                    self.logger.info(f"Found career URL via job platform: {href}")
                    return href
        
            # Method 4: Look for career links in the footer
            footer = soup.find(['footer', 'div'], class_=lambda c: c and 'footer' in c.lower())
            if footer:
                for link in footer.find_all('a'):
                    href = link.get('href')
                    text = link.get_text().lower().strip()
                
                    if not href:
                        continue
                
                    if any(keyword in text for keyword in career_keywords):
                        # Make sure the URL is absolute
                        if not href.startswith(('http://', 'https://')):
                            href = urljoin(company_url, href)
                
                    self.logger.info(f"Found career URL in footer: {href}")
                    return href
        
            # Method 5: Check for "About Us" page which might contain career links
            about_links = []
            for link in soup.find_all('a'):
                href = link.get('href')
                text = link.get_text().lower().strip()
            
                if not href:
                    continue
            
                if 'about' in text or 'about us' in text or 'about-us' in href.lower() or 'about' in href.lower():
                    # Make sure the URL is absolute
                    if not href.startswith(('http://', 'https://')):
                        href = urljoin(company_url, href)
                
                about_links.append(href)
        
            # Check the first few about links for career links
            for about_link in about_links[:3]:  # Limit to first 3 to avoid too many requests
                try:
                    self.rate_limiter.limit()
                    about_response = safe_request(about_link, timeout=30)
                
                    if about_response:
                        about_soup = BeautifulSoup(about_response.content, 'html.parser')
                    
                        for link in about_soup.find_all('a'):
                            href = link.get('href')
                            text = link.get_text().lower().strip()
                        
                            if not href:
                                continue
                            
                            if any(keyword in text for keyword in career_keywords):
                                # Make sure the URL is absolute
                                if not href.startswith(('http://', 'https://')):
                                    href = urljoin(about_link, href)
                                
                                self.logger.info(f"Found career URL in about page: {href}")
                                return href
                except:
                    # Ignore errors when checking about pages
                    pass
        
            self.logger.warning(f"No career URL found for {company_name}")
            return None
    
        except requests.RequestException as e:
            self.error_handler.handle_request_error(e, company_url, "Career URL scraping")
            return None
