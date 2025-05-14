import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

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

class JobDetailScraper:
    """Scraper to extract detailed information from job pages."""
    
    def __init__(self, headless: bool = True, timeout: int = 30, max_workers: int = 5):
        """Initialize the job detail scraper.
        
        Args:
            headless: Whether to run the browser in headless mode.
            timeout: Default timeout for waiting for elements.
            max_workers: Maximum number of parallel workers for scraping.
        """
        self.timeout = timeout
        self.headless = headless
        self.max_workers = max_workers
        
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
    
    def _extract_job_details(self, job_url: str) -> Dict[str, str]:
        """Extract detailed information from a job page.
        
        Args:
            job_url: URL of the job page.
            
        Returns:
            Dictionary containing detailed job information.
        """
        # First, check if this is a known job platform
        platform_scraper = PlatformScraperFactory.create_scraper(job_url)
        if platform_scraper:
            # Use the specialized platform scraper
            return platform_scraper.scrape_job_details(job_url)
        
        # If no specialized scraper was found, use the generic scraper
        logger.info(f"Scraping job details from: {job_url}")
        job_details = {"url": job_url}
        driver = self._create_driver()
        
        try:
            driver.get(job_url)
            WebDriverWait(driver, self.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Parse the page source
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Extract job title
            title_selectors = [
                "h1", 
                "h1.job-title", 
                ".job-title", 
                ".position-title",
                "[data-job-title]",
                ".headline h1",
                ".job-header h1",
                ".job-details h1"
            ]
            
            for selector in title_selectors:
                title_elem = soup.select_one(selector)
                if title_elem:
                    job_details["title"] = title_elem.get_text(strip=True)
                    break
            
            # Extract job description
            description_selectors = [
                ".job-description", 
                ".description", 
                "#job-description",
                "[data-job-description]",
                ".job-details .description",
                ".job-content",
                ".about-position",
                ".job-details-content",
                "article",
                "main"
            ]
            
            for selector in description_selectors:
                desc_elem = soup.select_one(selector)
                if desc_elem:
                    job_details["description"] = desc_elem.get_text(strip=True)
                    break
            
            # Extract job location
            location_selectors = [
                ".job-location", 
                ".location", 
                "[data-job-location]",
                ".job-details .location",
                ".job-meta .location",
                ".job-info .location"
            ]
            
            for selector in location_selectors:
                location_elem = soup.select_one(selector)
                if location_elem:
                    job_details["location"] = location_elem.get_text(strip=True)
                    break
            
            # Extract job department/category
            department_selectors = [
                ".job-department", 
                ".department", 
                ".category",
                "[data-job-department]",
                ".job-details .department",
                ".job-meta .department",
                ".job-info .department"
            ]
            
            for selector in department_selectors:
                department_elem = soup.select_one(selector)
                if department_elem:
                    job_details["department"] = department_elem.get_text(strip=True)
                    break
            
            # Extract job type (full-time, part-time, etc.)
            job_type_selectors = [
                ".job-type", 
                ".employment-type", 
                ".type",
                "[data-job-type]",
                ".job-details .type",
                ".job-meta .type",
                ".job-info .type"
            ]
            
            for selector in job_type_selectors:
                job_type_elem = soup.select_one(selector)
                if job_type_elem:
                    job_details["job_type"] = job_type_elem.get_text(strip=True)
                    break
            
            # Extract application information
            apply_selectors = [
                ".apply", 
                "#apply", 
                ".application",
                "[data-apply]",
                ".job-apply",
                ".apply-button",
                ".application-details"
            ]
            
            for selector in apply_selectors:
                apply_elem = soup.select_one(selector)
                if apply_elem:
                    job_details["application_info"] = apply_elem.get_text(strip=True)
                    
                    # Check for application URL
                    apply_link = apply_elem.find('a')
                    if apply_link and apply_link.has_attr('href'):
                        job_details["application_url"] = apply_link['href']
                    break
            
            logger.info(f"Successfully scraped details for job: {job_details.get('title', 'Unknown Title')}")
            
        except Exception as e:
            logger.error(f"Error scraping job details from {job_url}: {str(e)}")
        finally:
            driver.quit()
            
        return job_details
    
    def scrape(self, job_listings: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Scrape detailed information for a list of job listings.
        
        Args:
            job_listings: List of dictionaries containing job information with URLs.
            
        Returns:
            List of dictionaries containing detailed job information.
        """
        job_details_list = []
        
        # Extract URLs from job listings
        job_urls = [job.get('url') for job in job_listings if job.get('url')]
        logger.info(f"Scraping details for {len(job_urls)} job listings")
        
        # Use ThreadPoolExecutor for parallel scraping
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            job_details_list = list(executor.map(self._extract_job_details, job_urls))
        
        logger.info(f"Successfully scraped details for {len(job_details_list)} jobs")
        return job_details_list
    
    def save_to_file(self, job_details: List[Dict[str, str]], filename: str) -> None:
        """Save job details to a JSON file.
        
        Args:
            job_details: List of dictionaries containing detailed job information.
            filename: Path to the output file.
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(job_details, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved {len(job_details)} job details to {filename}")
