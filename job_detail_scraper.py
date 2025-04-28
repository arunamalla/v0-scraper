import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import time
import re
import json
import concurrent.futures
import threading
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Any, Set, Optional

from logger import Logger
from error_handler import ErrorHandler
from data_manager import DataManager
from rate_limiter import RateLimiter
from robots_parser import RobotsParser
from url_tracker import URLTracker
from utils import safe_request, retry_request

class JobDetailScraper:
    """Scrape detailed job descriptions from job listing URLs."""
    
    def __init__(self, rate_limit_seconds=3, logger=None, error_handler=None, data_manager=None, url_tracker=None):
        """Initialize the job detail scraper.
        
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
        
        self.job_details = []
        self.lock = threading.Lock()  # Lock for thread safety
        
        # Common job detail section identifiers
        self.job_section_identifiers = {
            "description": ["description", "overview", "summary", "about", "the role", "the position", "job details"],
            "responsibilities": ["responsibilities", "duties", "what you'll do", "key responsibilities", "job responsibilities", "your responsibilities"],
            "requirements": ["requirements", "qualifications", "skills", "what you need", "what we're looking for", "who you are", "required skills"],
            "benefits": ["benefits", "perks", "what we offer", "compensation", "why join us", "what's in it for you"],
            "application": ["apply", "application", "how to apply", "next steps", "application process"],
            "company": ["about us", "our company", "who we are", "company overview", "our team"],
            "salary": ["salary", "compensation", "pay", "wage", "remuneration", "package"],
            "location": ["location", "where you'll work", "work location", "office location"],
            "employment_type": ["employment type", "job type", "contract type", "work type", "position type"],
            "deadline": ["deadline", "closing date", "application deadline", "apply by", "valid until"]
        }
        
        # Common job detail section selectors
        self.job_section_selectors = {
            "description": ["div.job-description", "div.description", "section.job-description", "#job-description", ".description", "[data-test='job-description']"],
            "responsibilities": ["div.responsibilities", "div.duties", "section.responsibilities", "#responsibilities", ".responsibilities", "[data-test='responsibilities']"],
            "requirements": ["div.requirements", "div.qualifications", "section.requirements", "#requirements", ".requirements", "[data-test='requirements']"],
            "benefits": ["div.benefits", "div.perks", "section.benefits", "#benefits", ".benefits", "[data-test='benefits']"],
            "application": ["div.application", "div.apply", "section.application", "#application", ".application", "[data-test='application']"],
            "company": ["div.company", "div.about-us", "section.company", "#company", ".company", "[data-test='company']"],
            "salary": ["div.salary", "div.compensation", "section.salary", "#salary", ".salary", "[data-test='salary']"],
            "location": ["div.location", "div.work-location", "section.location", "#location", ".location", "[data-test='location']"],
            "employment_type": ["div.employment-type", "div.job-type", "section.employment-type", "#employment-type", ".employment-type", "[data-test='employment-type']"],
            "deadline": ["div.deadline", "div.closing-date", "section.deadline", "#deadline", ".deadline", "[data-test='deadline']"]
        }
    
    def scrape_job_details(self, job_listings_file="job_listings.json", output_file="job_details.json", 
                          limit=None, max_workers=5, resume=True):
        """Scrape detailed job descriptions from job listing URLs.
        
        Args:
            job_listings_file: File containing job listings
            output_file: File to save the job details to
            limit: Maximum number of job listings to process
            max_workers: Maximum number of worker threads
            resume: Whether to resume from a checkpoint
            
        Returns:
            List of job details
        """
        # Load job listings
        job_listings = self.data_manager.load_data(job_listings_file)
        if not job_listings:
            self.logger.error(f"No job listings found in {job_listings_file}")
            return []
        
        self.logger.info(f"Loaded {len(job_listings)} job listings")
        
        # Load existing job details if resuming
        if resume:
            checkpoint = self.url_tracker.load_checkpoint()
            if checkpoint and checkpoint.get('stage') == 'job_details':
                self.logger.info("Resuming job details scraping from checkpoint")
                self.job_details = self.data_manager.load_data(output_file) or []
                
                # Get the last processed index
                last_index = checkpoint.get('last_index', 0)
                
                # If we have data and haven't finished, we can skip some job listings
                if self.job_details and last_index < len(job_listings):
                    self.logger.info(f"Loaded {len(self.job_details)} job details from previous session")
                    self.logger.info(f"Resuming from index {last_index}")
                    
                    # Adjust job_listings to start from where we left off
                    job_listings = job_listings[last_index:]
        
        # Process only a subset if limit is specified
        job_listings_to_process = job_listings[:limit] if limit is not None else job_listings
        
        # Filter out job listings without URLs
        job_listings_with_urls = []
        for job_listing in job_listings_to_process:
            url = job_listing.get("url")
            if url:
                # Check if we've already processed this URL
                if not any(detail.get("url") == url for detail in self.job_details):
                    job_listings_with_urls.append(job_listing)
        
        self.logger.info(f"Scraping details for {len(job_listings_with_urls)} job listings out of {len(job_listings_to_process)} total")
        
        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit tasks
            future_to_job = {
                executor.submit(self._process_job_listing, i, job_listing, len(job_listings_with_urls), output_file): job_listing
                for i, job_listing in enumerate(job_listings_with_urls)
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_job):
                job_listing = future_to_job[future]
                try:
                    future.result()  # This will re-raise any exceptions from the thread
                except Exception as e:
                    self.error_handler.handle_error(e, f"Error processing job listing {job_listing.get('title')}")
        
        # Final save
        self.data_manager.save_data(self.job_details, output_file)
        
        # Clear checkpoint since we're done with this stage
        self.url_tracker.clear_checkpoint()
        
        return self.job_details
    
    def _process_job_listing(self, index, job_listing, total, output_file):
        """Process a single job listing.
        
        Args:
            index: Index of the job listing in the list
            job_listing: Job listing data
            total: Total number of job listings
            output_file: File to save the job details to
        """
        url = job_listing.get("url")
        title = job_listing.get("title")
        company_name = job_listing.get("company_name")
        
        if not url:
            self.logger.warning(f"No URL found for job listing {title}")
            return
        
        self.logger.info(f"Processing job listing {index+1}/{total}: {title} - {company_name}")
        
        # Check if crawling is allowed
        if not self.robots_parser.is_allowed('*', url):
            self.logger.warning(f"Crawling not allowed for {url}")
            return
        
        try:
            # Apply rate limiting
            self.rate_limiter.limit()
            
            # Scrape job details
            job_detail = self._scrape_job_detail(url, job_listing)
            
            if job_detail:
                self.logger.info(f"Successfully scraped details for {title}")
                
                # Thread-safe update of job_details list
                with self.lock:
                    self.job_details.append(job_detail)
                    
                    # Save checkpoint
                    self.url_tracker.save_checkpoint({
                        'stage': 'job_details',
                        'last_index': index,
                        'job_details_count': len(self.job_details)
                    })
                    
                    # Save after each successful scrape to avoid losing data
                    self.data_manager.save_data(self.job_details, output_file)
            else:
                self.logger.warning(f"Failed to scrape details for {title}")
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Error processing job listing {title}")
    
    def _scrape_job_detail(self, url, job_listing):
        """Scrape detailed job description from a URL.
        
        Args:
            url: URL of the job listing
            job_listing: Basic job listing data
            
        Returns:
            Dictionary containing job details
        """
        try:
            self.logger.info(f"Scraping job details from {url}")
            
            # Try with regular requests first
            response = safe_request(url)
            
            if not response:
                self.logger.error(f"Failed to fetch {url}")
                return None
            
            # Start with the basic job listing data
            job_detail = job_listing.copy()
            
            # Add timestamp
            job_detail["scraped_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Try to extract job details with BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract job details
            extracted_details = self._extract_job_details(soup, url)
            
            if extracted_details:
                # Merge extracted details with job listing data
                job_detail.update(extracted_details)
                return job_detail
            
            # If no details extracted, try with Playwright
            self.logger.info(f"No job details extracted with BeautifulSoup, trying with Playwright")
            playwright_details = self._scrape_job_detail_with_playwright(url, job_listing)
            
            if playwright_details:
                # Merge playwright details with job listing data
                job_detail.update(playwright_details)
                return job_detail
            
            # If still no details, return the original job listing with a flag
            job_detail["details_extracted"] = False
            return job_detail
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Error scraping job details from {url}")
            return None
    
    def _extract_job_details(self, soup, url):
        """Extract job details from BeautifulSoup object.
        
        Args:
            soup: BeautifulSoup object
            url: URL of the job listing
            
        Returns:
            Dictionary containing job details
        """
        job_details = {
            "details_extracted": True,
            "sections": {}
        }
        
        try:
            # Method 1: Try to extract sections using section identifiers and selectors
            for section_name, identifiers in self.job_section_identifiers.items():
                # Try selectors first
                section_content = None
                for selector in self.job_section_selectors.get(section_name, []):
                    elements = soup.select(selector)
                    if elements:
                        section_content = "\n".join([elem.get_text(separator="\n").strip() for elem in elements])
                        break
                
                # If no content found with selectors, try identifiers
                if not section_content:
                    for identifier in identifiers:
                        # Look for headings containing the identifier
                        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                            heading_text = heading.get_text().lower().strip()
                            if identifier in heading_text:
                                # Found a heading, get the content that follows
                                section_content = self._get_section_content(heading)
                                break
                        
                        if section_content:
                            break
                        
                        # Look for divs with class or id containing the identifier
                        for attr in ['class', 'id']:
                            elements = soup.find_all(attrs={attr: lambda x: x and any(id_str in x.lower() for id_str in [identifier])})
                            if elements:
                                section_content = "\n".join([elem.get_text(separator="\n").strip() for elem in elements])
                                break
                        
                        if section_content:
                            break
                
                # If content found, add to job details
                if section_content:
                    job_details["sections"][section_name] = section_content
            
            # Method 2: If no sections found, try to extract the main content
            if not job_details["sections"]:
                # Look for the main content area
                main_content = None
                
                # Try common content selectors
                content_selectors = [
                    "div.job-description", "div.description", "article.job-description",
                    "div.job-detail", "div.job-details", "div.job-content",
                    "div.job-posting", "div.job-post", "div.job",
                    "main", "article", "#content", ".content"
                ]
                
                for selector in content_selectors:
                    elements = soup.select(selector)
                    if elements:
                        main_content = elements[0].get_text(separator="\n").strip()
                        break
                
                if main_content:
                    job_details["full_description"] = main_content
                    
                    # Try to split the content into sections based on headings
                    sections = self._split_content_into_sections(soup)
                    if sections:
                        job_details["sections"] = sections
            
            # Method 3: Extract structured data if available
            structured_data = self._extract_structured_data(soup)
            if structured_data:
                # Merge structured data with job details
                for key, value in structured_data.items():
                    if key not in job_details or not job_details[key]:
                        job_details[key] = value
            
            # Extract contact information
            contact_info = self._extract_contact_info(soup)
            if contact_info:
                job_details["contact_info"] = contact_info
            
            # Extract application deadline
            deadline = self._extract_deadline(soup)
            if deadline:
                job_details["application_deadline"] = deadline
            
            # Extract salary information
            salary = self._extract_salary(soup)
            if salary:
                job_details["salary"] = salary
            
            return job_details
            
        except Exception as e:
            self.error_handler.handle_error(e, "Error extracting job details")
            return {"details_extracted": False}
    
    def _get_section_content(self, heading):
        """Get the content that follows a heading.
        
        Args:
            heading: BeautifulSoup element representing a heading
            
        Returns:
            String containing the section content
        """
        content = []
        current = heading.next_sibling
        
        # Get all elements until the next heading or end of section
        while current and not (current.name and current.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            if hasattr(current, 'get_text'):
                text = current.get_text(separator="\n").strip()
                if text:
                    content.append(text)
            elif isinstance(current, str) and current.strip():
                content.append(current.strip())
            
            current = current.next_sibling
        
        return "\n".join(content)
    
    def _split_content_into_sections(self, soup):
        """Split content into sections based on headings.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Dictionary containing sections
        """
        sections = {}
        
        # Find all headings
        headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        
        for heading in headings:
            heading_text = heading.get_text().lower().strip()
            
            # Check if this heading corresponds to a known section
            section_name = None
            for name, identifiers in self.job_section_identifiers.items():
                if any(identifier in heading_text for identifier in identifiers):
                    section_name = name
                    break
            
            if section_name:
                # Get the content that follows this heading
                section_content = self._get_section_content(heading)
                if section_content:
                    sections[section_name] = section_content
        
        return sections
    
    def _extract_structured_data(self, soup):
        """Extract structured data from the page.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Dictionary containing structured data
        """
        structured_data = {}
        
        try:
            # Look for JSON-LD
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string)
                    
                    # Check if this is job posting data
                    if isinstance(data, dict) and data.get('@type') == 'JobPosting':
                        # Extract relevant fields
                        if 'title' in data:
                            structured_data['title'] = data['title']
                        
                        if 'description' in data:
                            structured_data['full_description'] = data['description']
                        
                        if 'datePosted' in data:
                            structured_data['date_posted'] = data['datePosted']
                        
                        if 'validThrough' in data:
                            structured_data['application_deadline'] = data['validThrough']
                        
                        if 'employmentType' in data:
                            structured_data['employment_type'] = data['employmentType']
                        
                        if 'hiringOrganization' in data and isinstance(data['hiringOrganization'], dict):
                            structured_data['company_name'] = data['hiringOrganization'].get('name')
                        
                        if 'jobLocation' in data and isinstance(data['jobLocation'], dict):
                            address = data['jobLocation'].get('address')
                            if isinstance(address, dict):
                                location_parts = []
                                for field in ['streetAddress', 'addressLocality', 'addressRegion', 'postalCode', 'addressCountry']:
                                    if field in address:
                                        location_parts.append(address[field])
                                
                                if location_parts:
                                    structured_data['location'] = ', '.join(location_parts)
                        
                        if 'baseSalary' in data and isinstance(data['baseSalary'], dict):
                            salary = data['baseSalary']
                            if 'value' in salary:
                                value = salary['value']
                                if isinstance(value, dict):
                                    min_value = value.get('minValue')
                                    max_value = value.get('maxValue')
                                    unit = value.get('unitText')
                                    
                                    if min_value and max_value:
                                        structured_data['salary'] = f"{min_value}-{max_value} {unit if unit else ''}"
                                    elif min_value:
                                        structured_data['salary'] = f"{min_value}+ {unit if unit else ''}"
                                    elif max_value:
                                        structured_data['salary'] = f"Up to {max_value} {unit if unit else ''}"
                        
                        # Found job posting data, no need to check other scripts
                        break
                except:
                    # Ignore errors in JSON parsing
                    pass
        
        except Exception as e:
            self.error_handler.handle_error(e, "Error extracting structured data")
        
        return structured_data
    
    def _extract_contact_info(self, soup):
        """Extract contact information from the page.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Dictionary containing contact information
        """
        contact_info = {}
        
        try:
            # Look for email addresses
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = []
            
            # Check text content
            page_text = soup.get_text()
            email_matches = re.findall(email_pattern, page_text)
            emails.extend(email_matches)
            
            # Check mailto links
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('mailto:'):
                    email = href[7:]  # Remove 'mailto:'
                    if '@' in email and email not in emails:
                        emails.append(email)
            
            if emails:
                contact_info['email'] = emails[0]  # Just take the first email
            
            # Look for phone numbers
            phone_pattern = r'(?:\+\d{1,3}[-.\s]?)?(?:$$?\d{3}$$?[-.\s]?)?\d{3}[-.\s]?\d{4}'
            phones = []
            
            phone_matches = re.findall(phone_pattern, page_text)
            phones.extend(phone_matches)
            
            # Check tel links
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('tel:'):
                    phone = href[4:]  # Remove 'tel:'
                    if phone not in phones:
                        phones.append(phone)
            
            if phones:
                contact_info['phone'] = phones[0]  # Just take the first phone number
            
            # Look for contact person
            contact_person = None
            
            # Check for common contact person indicators
            contact_indicators = ['contact', 'recruiter', 'hiring manager', 'for more information']
            for indicator in contact_indicators:
                if indicator in page_text.lower():
                    # Look for a name near the indicator
                    index = page_text.lower().find(indicator)
                    if index != -1:
                        # Get the text around the indicator
                        context = page_text[max(0, index - 50):min(len(page_text), index + 100)]
                        # Look for a name pattern (e.g., "John Smith")
                        name_match = re.search(r'([A-Z][a-z]+ [A-Z][a-z]+)', context)
                        if name_match:
                            contact_person = name_match.group(1)
                            break
            
            if contact_person:
                contact_info['contact_person'] = contact_person
        
        except Exception as e:
            self.error_handler.handle_error(e, "Error extracting contact information")
        
        return contact_info if contact_info else None
    
    def _extract_deadline(self, soup):
        """Extract application deadline from the page.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            String containing the application deadline
        """
        try:
            # Look for deadline indicators
            deadline_indicators = ['deadline', 'closing date', 'apply by', 'valid until', 'applications close']
            page_text = soup.get_text().lower()
            
            for indicator in deadline_indicators:
                if indicator in page_text:
                    # Look for a date near the indicator
                    index = page_text.find(indicator)
                    if index != -1:
                        # Get the text after the indicator
                        context = page_text[index:min(len(page_text), index + 100)]
                        
                        # Look for date patterns
                        # Format: MM/DD/YYYY or DD/MM/YYYY
                        date_match = re.search(r'\b\d{1,2}/\d{1,2}/\d{2,4}\b', context)
                        if date_match:
                            return date_match.group(0)
                        
                        # Format: Month DD, YYYY
                        date_match = re.search(r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{2,4}\b', context, re.IGNORECASE)
                        if date_match:
                            return date_match.group(0)
                        
                        # Format: YYYY-MM-DD
                        date_match = re.search(r'\b\d{4}-\d{1,2}-\d{1,2}\b', context)
                        if date_match:
                            return date_match.group(0)
        
        except Exception as e:
            self.error_handler.handle_error(e, "Error extracting application deadline")
        
        return None
    
    def _extract_salary(self, soup):
        """Extract salary information from the page.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            String containing the salary information
        """
        try:
            # Look for salary indicators
            salary_indicators = ['salary', 'compensation', 'pay', 'wage', 'remuneration', 'package']
            page_text = soup.get_text().lower()
            
            for indicator in salary_indicators:
                if indicator in page_text:
                    # Look for salary information near the indicator
                    index = page_text.find(indicator)
                    if index != -1:
                        # Get the text around the indicator
                        context = page_text[max(0, index - 20):min(len(page_text), index + 100)]
                        
                        # Look for currency symbols followed by numbers
                        salary_match = re.search(r'[$€£¥]\s*\d+[,\d]*(?:\.\d+)?(?:\s*-\s*[$€£¥]?\s*\d+[,\d]*(?:\.\d+)?)?(?:\s*[kK])?', context)
                        if salary_match:
                            return salary_match.group(0)
                        
                        # Look for numbers followed by k or K
                        salary_match = re.search(r'\d+[,\d]*(?:\.\d+)?\s*[kK](?:\s*-\s*\d+[,\d]*(?:\.\d+)?\s*[kK])?', context)
                        if salary_match:
                            return salary_match.group(0)
                        
                        # Look for salary ranges
                        salary_match = re.search(r'\d+[,\d]*(?:\.\d+)?(?:\s*-\s*\d+[,\d]*(?:\.\d+)?)', context)
                        if salary_match:
                            return salary_match.group(0)
        
        except Exception as e:
            self.error_handler.handle_error(e, "Error extracting salary information")
        
        return None
    
    def _scrape_job_detail_with_playwright(self, url, job_listing):
        """Scrape job details using Playwright for JavaScript-heavy pages.
        
        Args:
            url: URL of the job listing
            job_listing: Basic job listing data
            
        Returns:
            Dictionary containing job details
        """
        try:
            self.logger.info(f"Scraping job details with Playwright from {url}")
            
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                try:
                    page.goto(url, timeout=60000)
                    
                    # Wait for the page to load
                    page.wait_for_timeout(5000)
                    
                    # Get the page content
                    content = page.content()
                    soup = BeautifulSoup(content, 'html.parser')
                    
                    # Extract job details
                    job_details = self._extract_job_details(soup, url)
                    
                    return job_details
                    
                except Exception as e:
                    self.error_handler.handle_error(e, f"Error scraping job details with Playwright from {url}")
                    return None
                finally:
                    browser.close()
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Error initializing Playwright for {url}")
            return None
