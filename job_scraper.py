import csv
import json
import logging
import re
import time
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger(__name__)

class JobScraper:
    """Scraper to extract company information and career URLs."""
    
    def __init__(self, headless: bool = True, timeout: int = 30):
        """Initialize the job scraper.
        
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
    
    def _find_career_url(self, driver: webdriver.Chrome, company_url: str) -> Optional[str]:
        """Find the careers page URL from a company website.
        
        Args:
            driver: Selenium webdriver instance.
            company_url: URL of the company website.
            
        Returns:
            URL of the careers page, or None if not found.
        """
        try:
            # Common link text patterns that lead to career pages
            career_link_patterns = [
                r'career',
                r'job',
                r'work.*us',
                r'join.*us',
                r'position',
                r'opening',
                r'opportunit',
                r'employ',
                r'recruitment',
                r'talent',
                r'vacanc'
            ]
            
            # Compiled regular expressions for faster matching
            career_link_regexes = [re.compile(pattern, re.IGNORECASE) for pattern in career_link_patterns]
            
            # Extract all links from the page
            elements = driver.find_elements(By.TAG_NAME, "a")
            
            for element in elements:
                try:
                    href = element.get_attribute("href")
                    text = element.text.strip()
                    
                    if not href or not text:
                        continue
                        
                    # Check if link text matches any of our patterns
                    for regex in career_link_regexes:
                        if regex.search(text):
                            logger.debug(f"Found career link: {href} with text: {text}")
                            return href
                except Exception as e:
                    logger.debug(f"Error extracting link: {str(e)}")
                    continue
            
            # Method 2: Look for links with career URLs
            for element in elements:
                try:
                    href = element.get_attribute("href")
                    if not href:
                        continue
                        
                    # Parse the URL
                    parsed_url = urlparse(href)
                    
                    # Check if the URL contains career-related keywords
                    for pattern in career_link_patterns:
                        if re.search(pattern, parsed_url.path, re.IGNORECASE):
                            logger.debug(f"Found career link by URL path: {href}")
                            return href
                except Exception as e:
                    logger.debug(f"Error checking URL pattern: {str(e)}")
                    continue
            
            # Method 3: Check common career URL patterns based on company domain
            parsed_company_url = urlparse(company_url)
            base_domain = parsed_company_url.netloc
            
            common_career_paths = [
                f"https://{base_domain}/careers",
                f"https://{base_domain}/jobs",
                f"https://{base_domain}/work-with-us",
                f"https://{base_domain}/join-us",
                f"https://{base_domain}/company/careers",
                f"https://{base_domain}/en/careers",
                f"https://{base_domain}/about/careers",
                f"https://careers.{base_domain}"
            ]
            
            for career_path in common_career_paths:
                try:
                    response = requests.head(career_path, timeout=5)
                    if response.status_code < 400:  # Successful response
                        logger.debug(f"Found career link by common pattern: {career_path}")
                        return career_path
                except Exception:
                    continue
            
            logger.info(f"No career URL found for {company_url}")
            return None
            
        except Exception as e:
            logger.error(f"Error finding career URL for {company_url}: {str(e)}")
            return None
    
    def _extract_company_info(self, driver: webdriver.Chrome, url: str) -> Dict[str, str]:
        """Extract company information from its website.
        
        Args:
            driver: Selenium webdriver instance.
            url: URL of the company website.
            
        Returns:
            Dictionary containing company information.
        """
        company_info = {'url': url}
        
        try:
            # Find company name
            title = driver.title
            if title:
                # Strip common suffixes like "| Home", "- Official Website", etc.
                title = re.sub(r'\s*[\|\-–]\s*.*$', '', title).strip()
                company_info['name'] = title
            
            # Get meta description
            meta_desc = driver.find_element(By.CSS_SELECTOR, "meta[name='description']").get_attribute("content")
            if meta_desc:
                company_info['description'] = meta_desc
            
            # Try to find company logo
            logo_selectors = [
                "img[id*='logo']",
                "img[class*='logo']",
                "img[alt*='logo']",
                "img[src*='logo']",
                "img[class*='brand']",
                "img.logo",
                "a.navbar-brand img",
                "header img",
                ".header img",
                ".navbar img"
            ]
            
            for selector in logo_selectors:
                try:
                    logo = driver.find_element(By.CSS_SELECTOR, selector)
                    logo_url = logo.get_attribute("src")
                    if logo_url:
                        company_info['logo_url'] = logo_url
                        break
                except Exception:
                    continue
            
            # Find careers URL
            career_url = self._find_career_url(driver, url)
            if career_url:
                company_info['career_url'] = career_url
            
            logger.info(f"Extracted company info for {url}: {company_info.get('name', 'Unknown')}")
            return company_info
            
        except Exception as e:
            logger.error(f"Error extracting company info for {url}: {str(e)}")
            return company_info
    
    def scrape(self, input_file: str) -> List[Dict[str, str]]:
        """Scrape company information and career URLs from a list of URLs.
        
        Args:
            input_file: Path to a CSV file containing company URLs.
            
        Returns:
            List of dictionaries containing company information.
        """
        companies = []
        
        # Read URLs from the input file
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            urls = [row[0] for row in reader if row]
        
        logger.info(f"Loaded {len(urls)} URLs from {input_file}")
        
        # Use a single webdriver for all URLs
        driver = self._create_driver()
        
        try:
            for url in urls:
                logger.info(f"Processing URL: {url}")
                
                try:
                    # Navigate to the URL
                    driver.get(url)
                    WebDriverWait(driver, self.timeout).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    
                    # Extract company information
                    company_info = self._extract_company_info(driver, url)
                    
                    companies.append(company_info)
                    
                except Exception as e:
                    logger.error(f"Error processing URL {url}: {str(e)}")
                    # Still add the URL to the results, even if we couldn't extract info
                    companies.append({'url': url, 'error': str(e)})
                
                # Sleep to avoid overloading the server
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Unexpected error during scraping: {str(e)}")
        finally:
            driver.quit()
            
        logger.info(f"Scraped information for {len(companies)} companies")
        return companies
    
    def save_to_file(self, companies: List[Dict[str, str]], filename: str) -> None:
        """Save company information to a JSON file.
        
        Args:
            companies: List of dictionaries containing company information.
            filename: Path to the output file.
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(companies, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved {len(companies)} companies to {filename}")
