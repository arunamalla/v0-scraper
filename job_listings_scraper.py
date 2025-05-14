import json
import logging
import re
import time
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from platform_scrapers import PlatformScraperFactory

logger = logging.getLogger(__name__)

class JobListingsScraper:
    """Scraper to extract job listings from career pages."""
    
    def __init__(self, headless: bool = True, timeout: int = 30):
        """Initialize the job listings scraper.
        
        Args:
            headless: Whether to run the browser in headless mode.
            timeout: Default timeout for waiting for elements.
        """
        self.timeout = timeout
        self.headless = headless
        
    def _create_driver(self) -> webdriver.Chrome:
        """Create and configure a Chrome webdriver."""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        
        return webdriver.Chrome(options=chrome_options)
    
    def _find_job_listing_pages(self, driver: webdriver.Chrome, url: str) -> List[str]:
        """Find job listing pages from a careers URL.
        
        Args:
            driver: Selenium webdriver instance.
            url: URL of the careers page.
            
        Returns:
            List of URLs to job listing pages.
        """
        job_pages = []
        
        # Common link text patterns that lead to job listings
        job_link_patterns = [
            r'job',
            r'career',
            r'position',
            r'opening',
            r'opportunit',
            r'vacanc',
            r'employ',
            r'work with us',
            r'join us',
            r'current opening',
            r'available position',
            r'find.*job',
            r'search.*job',
            r'view.*job',
            r'browse.*job',
            r'explore.*career',
            r'join.*team'
        ]
        
        # Compiled regular expressions for faster matching
        job_link_regexes = [re.compile(pattern, re.IGNORECASE) for pattern in job_link_patterns]
        
        # Extract all links from the page
        elements = driver.find_elements(By.TAG_NAME, "a")
        
        for element in elements:
            try:
                href = element.get_attribute("href")
                text = element.text.strip()
                
                if not href or not text:
                    continue
                    
                # Check if link text matches any of our patterns
                for regex in job_link_regexes:
                    if regex.search(text):
                        job_pages.append(href)
                        logger.debug(f"Found job listing page: {href} with text: {text}")
                        break
            except Exception as e:
                logger.debug(f"Error extracting link: {str(e)}")
                continue
        
        # If we didn't find any job links via direct text matching, try other methods
        if not job_pages:
            # Method 2: Look for common URL patterns in all links
            all_links = []
            elements = driver.find_elements(By.TAG_NAME, "a")
            
            for element in elements:
                try:
                    href = element.get_attribute("href")
                    if href:
                        all_links.append(href)
                except Exception:
                    continue
            
            url_patterns = [
                r'/jobs/?',
                r'/careers?/',
                r'/careers?/jobs',
                r'/opportunities',
                r'/openings',
                r'/positions',
                r'/vacancies',
                r'/recruiting',
                r'/work-with-us',
                r'/join-us',
                r'/joinourteam',
                r'/employment',
                r'/career-search',
                r'/job-search',
                r'/career-opportunities'
            ]
            
            url_regexes = [re.compile(pattern, re.IGNORECASE) for pattern in url_patterns]
            
            for link in all_links:
                for regex in url_regexes:
                    if regex.search(link):
                        job_pages.append(link)
                        logger.debug(f"Found job listing page by URL pattern: {link}")
                        break
        
        # Method 3: Look for buttons that might lead to job listings
        if not job_pages:
            button_selectors = [
                "button",
                ".btn",
                ".button",
                "[role='button']",
                "a.cta",
                "a.btn",
                "a.button"
            ]
            
            for selector in button_selectors:
                buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                
                for button in buttons:
                    try:
                        text = button.text.strip().lower()
                        
                        # Check for job-related button text
                        if any(re.search(regex, text) for regex in job_link_regexes):
                            # If it's a link, add the href
                            if button.tag_name == "a":
                                href = button.get_attribute("href")
                                if href:
                                    job_pages.append(href)
                                    logger.debug(f"Found job listing page from button link: {href}")
                            # Otherwise try to click it and get the current URL after navigation
                            else:
                                current_url = driver.current_url
                                button.click()
                                time.sleep(2)  # Wait for navigation
                                
                                # If we navigated to a new page, record it
                                if driver.current_url != current_url:
                                    job_pages.append(driver.current_url)
                                    logger.debug(f"Found job listing page after button click: {driver.current_url}")
                                    
                                # Go back to continue searching
                                driver.back()
                                time.sleep(1)
                    except Exception as e:
                        logger.debug(f"Error with button: {str(e)}")
                        continue
        
        # Convert relative URLs to absolute
        job_pages = [urljoin(url, page) if not page.startswith(('http://', 'https://')) else page for page in job_pages]
        
        # Remove duplicates while preserving order
        seen = set()
        unique_job_pages = []
        for page in job_pages:
            if page not in seen:
                seen.add(page)
                unique_job_pages.append(page)
        
        logger.info(f"Found {len(unique_job_pages)} potential job listing pages for {url}")
        return unique_job_pages
    
    def _extract_job_listings(self, driver: webdriver.Chrome, url: str) -> List[Dict[str, str]]:
        """Extract job listings from a job listings page.
        
        Args:
            driver: Selenium webdriver instance.
            url: URL of the job listings page.
            
        Returns:
            List of dictionaries containing job information.
        """
        # First, check if this is a known job platform
        platform_scraper = PlatformScraperFactory.create_scraper(url)
        if platform_scraper:
            # Use the specialized platform scraper
            return platform_scraper.scrape_job_listings(url)
        
        # If no specialized scraper was found, use the generic scraper
        job_listings = []
        
        try:
            # Scroll to load all job listings (for lazy-loaded content)
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)  # Wait for page to load
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            # Parse the page source
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Common CSS selectors for job listings containers
            container_selectors = [
                ".jobs-list", 
                ".job-listings", 
                ".careers-list",
                ".job-list",
                ".openings",
                ".positions",
                ".vacancies",
                "ul.jobs",
                "div.jobs",
                "[data-job-list]",
                ".job-container"
            ]
            
            # Try to find a container for job listings
            container = None
            for selector in container_selectors:
                container = soup.select_one(selector)
                if container:
                    break
            
            # If no container found, use the whole body
            if not container:
                container = soup.body
            
            # Common CSS selectors for individual job elements
            job_selectors = [
                ".job-item",
                ".job-listing",
                ".job-card",
                ".job",
                ".opening",
                ".position",
                ".vacancy",
                "[data-job-id]",
                "[data-job]",
                ".career-item",
                "li.job",
                "div.job"
            ]
            
            # Try to find job elements using these selectors
            job_elements = []
            for selector in job_selectors:
                job_elements = container.select(selector)
                if job_elements:
                    break
            
            # If no job elements found using selectors, try to find them by looking for links with job-related text
            if not job_elements:
                job_elements = []
                job_link_patterns = [
                    r'job',
                    r'position',
                    r'opening',
                    r'apply',
                    r'career'
                ]
                
                job_link_regexes = [re.compile(pattern, re.IGNORECASE) for pattern in job_link_patterns]
                
                # Find links that might be job listings
                links = container.find_all('a')
                for link in links:
                    if link.text and any(regex.search(link.text) for regex in job_link_regexes):
                        # Check if this link is wrapped in a container element
                        parent = link.parent
                        if parent and parent.name in ['div', 'li', 'article', 'section']:
                            job_elements.append(parent)
                        else:
                            # Use the link itself
                            job_elements.append(link)
            
            # Process each job element
            for job_element in job_elements:
                job_data = {}
                
                # Extract job title
                title_elements = job_element.select('h2, h3, h4, .job-title, .position-title')
                if title_elements:
                    job_data['title'] = title_elements[0].get_text(strip=True)
                else:
                    # Try finding an 'a' tag that might contain the job title
                    link = job_element.find('a')
                    if link and link.text:
                        job_data['title'] = link.get_text(strip=True)
                
                # Extract job URL
                links = job_element.find_all('a')
                for link in links:
                    href = link.get('href')
                    if href:
                        # Handle relative URLs
                        job_url = urljoin(url, href) if not href.startswith(('http://', 'https://')) else href
                        job_data['url'] = job_url
                        break
                
                # Extract job location
                location_elements = job_element.select('.location, .job-location, .position-location')
                if location_elements:
                    job_data['location'] = location_elements[0].get_text(strip=True)
                
                # Extract job department/category
                department_elements = job_element.select('.department, .category, .job-department, .job-category')
                if department_elements:
                    job_data['department'] = department_elements[0].get_text(strip=True)
                
                # Only add if we have at least a title and URL
                if 'title' in job_data and 'url' in job_data:
                    job_listings.append(job_data)
            
            logger.info(f"Extracted {len(job_listings)} job listings from {url}")
            
        except Exception as e:
            logger.error(f"Error extracting job listings from {url}: {str(e)}")
        
        return job_listings
    
    def scrape(self, careers_urls: List[str]) -> List[Dict[str, str]]:
        """Scrape job listings from a list of career URLs.
        
        Args:
            careers_urls: List of career page URLs to scrape.
            
        Returns:
            List of dictionaries containing job information.
        """
        all_job_listings = []
        processed_urls = set()  # To avoid processing the same URL multiple times
        
        for career_url in careers_urls:
            logger.info(f"Processing career URL: {career_url}")
            
            driver = self._create_driver()
            
            try:
                # Navigate to the career page
                driver.get(career_url)
                WebDriverWait(driver, self.timeout).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Find job listing pages
                job_listing_pages = self._find_job_listing_pages(driver, career_url)
                
                # If no job listing pages found, try to extract job listings from the current page
                if not job_listing_pages:
                    logger.info(f"No job listing pages found, trying to extract listings from {career_url}")
                    job_listings = self._extract_job_listings(driver, career_url)
                    all_job_listings.extend(job_listings)
                    
                # Otherwise, visit each job listing page and extract listings
                else:
                    for job_page_url in job_listing_pages:
                        if job_page_url in processed_urls:
                            logger.info(f"Skipping already processed URL: {job_page_url}")
                            continue
                            
                        logger.info(f"Visiting job listing page: {job_page_url}")
                        
                        try:
                            driver.get(job_page_url)
                            WebDriverWait(driver, self.timeout).until(
                                EC.presence_of_element_located((By.TAG_NAME, "body"))
                            )
                            
                            job_listings = self._extract_job_listings(driver, job_page_url)
                            all_job_listings.extend(job_listings)
                            
                            processed_urls.add(job_page_url)
                            
                        except Exception as e:
                            logger.error(f"Error processing job page {job_page_url}: {str(e)}")
                
            except Exception as e:
                logger.error(f"Error processing career URL {career_url}: {str(e)}")
            finally:
                driver.quit()
        
        # Remove duplicates by URL
        unique_listings = []
        seen_urls = set()
        
        for listing in all_job_listings:
            if listing.get('url') not in seen_urls:
                seen_urls.add(listing.get('url'))
                unique_listings.append(listing)
        
        logger.info(f"Total unique job listings found: {len(unique_listings)}")
        return unique_listings
    
    def save_to_file(self, job_listings: List[Dict[str, str]], filename: str) -> None:
        """Save job listings to a JSON file.
        
        Args:
            job_listings: List of dictionaries containing job information.
            filename: Path to the output file.
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(job_listings, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved {len(job_listings)} job listings to {filename}")
