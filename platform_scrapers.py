import logging
import re
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger(__name__)

class BasePlatformScraper(ABC):
    """Base class for platform-specific job scrapers."""
    
    def __init__(self, headless: bool = True, timeout: int = 30):
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
    
    def is_platform_url(self, url: str) -> bool:
        """Check if the URL belongs to this platform."""
        return self._check_platform_url(url)
    
    @abstractmethod
    def _check_platform_url(self, url: str) -> bool:
        """Platform-specific URL check."""
        pass
    
    @abstractmethod
    def scrape_job_listings(self, url: str) -> List[Dict[str, str]]:
        """Scrape job listings from the platform."""
        pass
    
    @abstractmethod
    def scrape_job_details(self, url: str) -> Dict[str, str]:
        """Scrape detailed job information from a specific job URL."""
        pass

class WorkdayScraper(BasePlatformScraper):
    """Specialized scraper for Workday job platform."""
    
    def _check_platform_url(self, url: str) -> bool:
        """Check if the URL is a Workday career site."""
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        path = parsed_url.path.lower()
        
        workday_patterns = [
            r'myworkdayjobs\.com',
            r'\.wd\d+\.myworkdayjobs\.com',
            r'workday\.com'
        ]
        
        for pattern in workday_patterns:
            if re.search(pattern, domain):
                return True
                
        # Check for common Workday paths
        workday_paths = ['/careers', '/talent', '/jobs']
        for workday_path in workday_paths:
            if workday_path in path:
                return True
                
        return False
    
    def scrape_job_listings(self, url: str) -> List[Dict[str, str]]:
        """Scrape job listings from Workday career site."""
        logger.info(f"Scraping Workday job listings from: {url}")
        job_listings = []
        driver = self._create_driver()
        
        try:
            driver.get(url)
            WebDriverWait(driver, self.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for job listings to load (adjust selector based on Workday structure)
            try:
                WebDriverWait(driver, self.timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-automation-id='jobListingCard']"))
                )
            except TimeoutException:
                # Try alternative selectors
                selectors = [
                    ".css-s0yeke",  # Common job card class
                    ".WLLR",  # Another common job listing class
                    "[role='listitem']",  # List items that might be job cards
                    ".job-listing"  # Generic class
                ]
                
                for selector in selectors:
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        break
                    except TimeoutException:
                        continue
            
            # Scroll to load all job listings (lazy loading)
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)  # Wait for page to load
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
            
            # Extract job listings
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Try different selectors based on Workday's varying structures
            job_cards = soup.select("[data-automation-id='jobListingCard']")
            if not job_cards:
                job_cards = soup.select(".css-s0yeke")  # Alternative class
            if not job_cards:
                job_cards = soup.select(".WLLR")  # Another alternative class
            if not job_cards:
                job_cards = soup.select("[role='listitem']")  # Generic list items
                
            for job_card in job_cards:
                job_data = {"platform": "Workday"}
                
                # Extract job title
                title_elem = job_card.select_one("[data-automation-id='jobTitle']")
                if not title_elem:
                    title_elem = job_card.find("a")
                    
                if title_elem:
                    job_data["title"] = title_elem.get_text(strip=True)
                    
                    # Extract job URL
                    if title_elem.name == "a" and title_elem.has_attr("href"):
                        job_url = title_elem["href"]
                        # Handle relative URLs
                        if not job_url.startswith(('http://', 'https://')):
                            job_url = urljoin(url, job_url)
                        job_data["url"] = job_url
                
                # Extract location
                location_elem = job_card.select_one("[data-automation-id='locationLabel']")
                if location_elem:
                    job_data["location"] = location_elem.get_text(strip=True)
                else:
                    # Try to find location in the card text
                    location_text = job_card.find(string=re.compile(r'location', re.IGNORECASE))
                    if location_text and location_text.parent:
                        job_data["location"] = location_text.parent.get_text(strip=True).replace("Location:", "").strip()
                
                # Extract job ID if available
                job_id_elem = job_card.select_one("[data-automation-id='jobRequisitionId']")
                if job_id_elem:
                    job_data["job_id"] = job_id_elem.get_text(strip=True)
                
                if "title" in job_data and "url" in job_data:
                    job_listings.append(job_data)
            
            logger.info(f"Found {len(job_listings)} job listings on Workday platform")
            
        except Exception as e:
            logger.error(f"Error scraping Workday job listings: {str(e)}")
        finally:
            driver.quit()
            
        return job_listings
    
    def scrape_job_details(self, url: str) -> Dict[str, str]:
        """Scrape detailed job information from a specific Workday job URL."""
        logger.info(f"Scraping Workday job details from: {url}")
        job_details = {"platform": "Workday", "url": url}
        driver = self._create_driver()
        
        try:
            driver.get(url)
            WebDriverWait(driver, self.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for job details to load
            selectors = [
                "[data-automation-id='jobPostingHeader']",
                "[data-automation-id='jobPostingDescription']",
                ".job-details",
                ".job-description"
            ]
            
            for selector in selectors:
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    break
                except TimeoutException:
                    continue
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Extract job title
            title_elem = soup.select_one("[data-automation-id='jobTitle']")
            if title_elem:
                job_details["title"] = title_elem.get_text(strip=True)
            
            # Extract job description
            description_elem = soup.select_one("[data-automation-id='jobPostingDescription']")
            if description_elem:
                job_details["description"] = description_elem.get_text(strip=True)
            else:
                # Try alternative selectors
                alternative_selectors = [
                    "[data-automation-id='job-description-text']",
                    "[data-automation-id='jobDescription']",
                    ".job-description"
                ]
                for selector in alternative_selectors:
                    description_elem = soup.select_one(selector)
                    if description_elem:
                        job_details["description"] = description_elem.get_text(strip=True)
                        break
            
            # Extract other job details
            # Location
            location_elem = soup.select_one("[data-automation-id='locationLabel']")
            if location_elem:
                job_details["location"] = location_elem.get_text(strip=True)
            
            # Job ID/Requisition ID
            job_id_elem = soup.select_one("[data-automation-id='jobRequisitionId']")
            if job_id_elem:
                job_details["job_id"] = job_id_elem.get_text(strip=True)
            
            # Posted date
            posted_date_elem = soup.select_one("[data-automation-id='postedOn']")
            if posted_date_elem:
                job_details["posted_date"] = posted_date_elem.get_text(strip=True)
            
            logger.info(f"Successfully scraped details for job: {job_details.get('title', 'Unknown Title')}")
            
        except Exception as e:
            logger.error(f"Error scraping Workday job details: {str(e)}")
        finally:
            driver.quit()
            
        return job_details


class LeverScraper(BasePlatformScraper):
    """Specialized scraper for Lever job platform."""
    
    def _check_platform_url(self, url: str) -> bool:
        """Check if the URL is a Lever career site."""
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        path = parsed_url.path.lower()
        
        lever_patterns = [
            r'lever\.co',
            r'jobs\.lever\.co'
        ]
        
        for pattern in lever_patterns:
            if re.search(pattern, domain):
                return True
                
        # Check for common Lever paths
        lever_paths = ['/jobs', '/careers', '/lever']
        for lever_path in lever_paths:
            if lever_path in path:
                return True
                
        return False
    
    def scrape_job_listings(self, url: str) -> List[Dict[str, str]]:
        """Scrape job listings from Lever career site."""
        logger.info(f"Scraping Lever job listings from: {url}")
        job_listings = []
        driver = self._create_driver()
        
        try:
            driver.get(url)
            WebDriverWait(driver, self.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for job listings to load
            try:
                WebDriverWait(driver, self.timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".lever-job"))
                )
            except TimeoutException:
                # Try alternative selectors
                selectors = [
                    ".posting",
                    ".job-posting",
                    ".job-position",
                    "[data-qa='job-posting']"
                ]
                
                for selector in selectors:
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        break
                    except TimeoutException:
                        continue
            
            # Extract job listings
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Try different selectors based on Lever's varying structures
            job_cards = soup.select(".lever-job")
            if not job_cards:
                job_cards = soup.select(".posting")
            if not job_cards:
                job_cards = soup.select(".job-posting")
            if not job_cards:
                job_cards = soup.select("[data-qa='job-posting']")
                
            for job_card in job_cards:
                job_data = {"platform": "Lever"}
                
                # Extract job title and URL
                title_elem = job_card.select_one("a[data-qa='job-posting-link'], h5 a, .posting-title a, .job-title a")
                if title_elem:
                    job_data["title"] = title_elem.get_text(strip=True)
                    
                    # Extract job URL
                    if title_elem.has_attr("href"):
                        job_url = title_elem["href"]
                        # Handle relative URLs
                        if not job_url.startswith(('http://', 'https://')):
                            job_url = urljoin(url, job_url)
                        job_data["url"] = job_url
                
                # Extract location
                location_elem = job_card.select_one(".posting-location, .location, [data-qa='posting-location']")
                if location_elem:
                    job_data["location"] = location_elem.get_text(strip=True)
                
                # Extract department/team
                team_elem = job_card.select_one(".posting-department, .team, [data-qa='posting-team']")
                if team_elem:
                    job_data["team"] = team_elem.get_text(strip=True)
                
                # Extract commitment (full-time, part-time, etc.)
                commitment_elem = job_card.select_one(".posting-commitment, .commitment, [data-qa='posting-commitment']")
                if commitment_elem:
                    job_data["commitment"] = commitment_elem.get_text(strip=True)
                
                if "title" in job_data and "url" in job_data:
                    job_listings.append(job_data)
            
            logger.info(f"Found {len(job_listings)} job listings on Lever platform")
            
        except Exception as e:
            logger.error(f"Error scraping Lever job listings: {str(e)}")
        finally:
            driver.quit()
            
        return job_listings
    
    def scrape_job_details(self, url: str) -> Dict[str, str]:
        """Scrape detailed job information from a specific Lever job URL."""
        logger.info(f"Scraping Lever job details from: {url}")
        job_details = {"platform": "Lever", "url": url}
        driver = self._create_driver()
        
        try:
            driver.get(url)
            WebDriverWait(driver, self.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for job details to load
            selectors = [
                ".posting-headline",
                ".content-wrapper",
                ".posting-page"
            ]
            
            for selector in selectors:
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    break
                except TimeoutException:
                    continue
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Extract job title
            title_elem = soup.select_one(".posting-headline h2")
            if title_elem:
                job_details["title"] = title_elem.get_text(strip=True)
            
            # Extract job location, team, commitment
            info_elements = soup.select(".posting-categories .category")
            for info_elem in info_elements:
                category_label = info_elem.select_one(".category-label")
                if category_label:
                    label_text = category_label.get_text(strip=True).lower()
                    value_elem = info_elem.select_one(".category-text")
                    if value_elem:
                        value_text = value_elem.get_text(strip=True)
                        
                        if "location" in label_text:
                            job_details["location"] = value_text
                        elif "team" in label_text or "department" in label_text:
                            job_details["team"] = value_text
                        elif "commitment" in label_text or "type" in label_text:
                            job_details["commitment"] = value_text
            
            # Extract job description
            description_elem = soup.select_one(".posting-description")
            if description_elem:
                sections = description_elem.select(".section")
                full_description = ""
                
                for section in sections:
                    section_title = section.select_one("h3")
                    section_content = section.select_one(".section-wrapper")
                    
                    if section_title and section_content:
                        title_text = section_title.get_text(strip=True)
                        content_text = section_content.get_text(strip=True)
                        full_description += f"{title_text}\n{content_text}\n\n"
                    elif section_content:
                        content_text = section_content.get_text(strip=True)
                        full_description += f"{content_text}\n\n"
                
                job_details["description"] = full_description.strip()
            
            logger.info(f"Successfully scraped details for job: {job_details.get('title', 'Unknown Title')}")
            
        except Exception as e:
            logger.error(f"Error scraping Lever job details: {str(e)}")
        finally:
            driver.quit()
            
        return job_details


class GreenHouseScraper(BasePlatformScraper):
    """Specialized scraper for Greenhouse job platform."""
    
    def _check_platform_url(self, url: str) -> bool:
        """Check if the URL is a Greenhouse career site."""
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.lower()
        path = parsed_url.path.lower()
        
        greenhouse_patterns = [
            r'greenhouse\.io',
            r'boards\.greenhouse\.io'
        ]
        
        for pattern in greenhouse_patterns:
            if re.search(pattern, domain):
                return True
                
        # Check for common Greenhouse paths
        greenhouse_paths = ['/jobs', '/careers', '/greenhouse']
        for greenhouse_path in greenhouse_paths:
            if greenhouse_path in path:
                return True
                
        return False
    
    def scrape_job_listings(self, url: str) -> List[Dict[str, str]]:
        """Scrape job listings from Greenhouse career site."""
        logger.info(f"Scraping Greenhouse job listings from: {url}")
        job_listings = []
        driver = self._create_driver()
        
        try:
            driver.get(url)
            WebDriverWait(driver, self.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for job listings to load
            try:
                WebDriverWait(driver, self.timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".opening"))
                )
            except TimeoutException:
                # Try alternative selectors
                selectors = [
                    ".position",
                    ".job",
                    ".posting",
                    "[data-qa='job-listing']"
                ]
                
                for selector in selectors:
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        break
                    except TimeoutException:
                        continue
            
            # Extract job listings
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Try different selectors based on Greenhouse's varying structures
            job_cards = soup.select(".opening")
            if not job_cards:
                job_cards = soup.select(".position")
            if not job_cards:
                job_cards = soup.select(".job")
            if not job_cards:
                job_cards = soup.select("[data-qa='job-listing']")
                
            for job_card in job_cards:
                job_data = {"platform": "Greenhouse"}
                
                # Extract job title and URL
                title_elem = job_card.select_one("a")
                if title_elem:
                    job_data["title"] = title_elem.get_text(strip=True)
                    
                    # Extract job URL
                    if title_elem.has_attr("href"):
                        job_url = title_elem["href"]
                        # Handle relative URLs
                        if not job_url.startswith(('http://', 'https://')):
                            job_url = urljoin(url, job_url)
                        job_data["url"] = job_url
                
                # Extract location
                location_elem = job_card.select_one(".location, .job-location")
                if location_elem:
                    job_data["location"] = location_elem.get_text(strip=True)
                
                # Extract department
                department_elem = job_card.select_one(".department, .job-department")
                if department_elem:
                    job_data["department"] = department_elem.get_text(strip=True)
                
                if "title" in job_data and "url" in job_data:
                    job_listings.append(job_data)
            
            logger.info(f"Found {len(job_listings)} job listings on Greenhouse platform")
            
        except Exception as e:
            logger.error(f"Error scraping Greenhouse job listings: {str(e)}")
        finally:
            driver.quit()
            
        return job_listings
    
    def scrape_job_details(self, url: str) -> Dict[str, str]:
        """Scrape detailed job information from a specific Greenhouse job URL."""
        logger.info(f"Scraping Greenhouse job details from: {url}")
        job_details = {"platform": "Greenhouse", "url": url}
        driver = self._create_driver()
        
        try:
            driver.get(url)
            WebDriverWait(driver, self.timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for job details to load
            selectors = [
                "#content",
                ".main",
                ".job-description"
            ]
            
            for selector in selectors:
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    break
                except TimeoutException:
                    continue
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Extract job title
            title_elem = soup.select_one(".app-title, h1.heading")
            if title_elem:
                job_details["title"] = title_elem.get_text(strip=True)
            
            # Extract job location
            location_elem = soup.select_one(".location, .job-location")
            if location_elem:
                job_details["location"] = location_elem.get_text(strip=True)
            
            # Extract department
            department_elem = soup.select_one(".department, .job-department")
            if department_elem:
                job_details["department"] = department_elem.get_text(strip=True)
            
            # Extract job description
            description_elem = soup.select_one("#content, .job-description, .section-description")
            if description_elem:
                job_details["description"] = description_elem.get_text(strip=True)
            
            # Extract application details
            apply_elem = soup.select_one("#apply, .apply")
            if apply_elem:
                job_details["application_info"] = apply_elem.get_text(strip=True)
            
            logger.info(f"Successfully scraped details for job: {job_details.get('title', 'Unknown Title')}")
            
        except Exception as e:
            logger.error(f"Error scraping Greenhouse job details: {str(e)}")
        finally:
            driver.quit()
            
        return job_details


class PlatformScraperFactory:
    """Factory class to create platform-specific scrapers."""
    
    @staticmethod
    def create_scraper(url: str) -> Optional[BasePlatformScraper]:
        """Create the appropriate scraper for the given job platform URL."""
        scrapers = [
            WorkdayScraper(),
            LeverScraper(),
            GreenHouseScraper()
        ]
        
        for scraper in scrapers:
            if scraper.is_platform_url(url):
                logger.info(f"Using specialized scraper for URL: {url}")
                return scraper
                
        logger.info(f"No specialized scraper found for URL: {url}")
        return None
