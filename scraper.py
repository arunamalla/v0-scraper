import json
import os
import re
import time
import logging
from typing import Dict, List, Optional, Set, Any

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("oracle_scraper.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Constants
ORACLE_BASE_URL = "https://www.oracle.com"
CUSTOMERS_URL = f"{ORACLE_BASE_URL}/customers/"
MASTER_SLUGS_FILE = "data/masterslugs.json"
CUSTOMER_DETAILS_FILE = "data/customer_details.json"

class RobotsParser:
    """Parse robots.txt and check if crawling is allowed."""
    
    def fetch_robots_txt(self, url: str) -> str:
        """Fetch robots.txt content from a website."""
        parsed_url = url.split('/')
        if len(parsed_url) >= 3:
            robots_url = f"{parsed_url[0]}//{parsed_url[2]}/robots.txt"
            try:
                response = requests.get(robots_url, timeout=10)
                if response.status_code == 200:
                    return response.text
            except requests.RequestException as e:
                logger.error(f"Error fetching robots.txt: {e}")
        return ""
    
    def is_allowed(self, user_agent: str, url: str) -> bool:
        """Check if crawling is allowed for the given URL."""
        robots_txt = self.fetch_robots_txt(url)
        
        # Simple robots.txt parsing - this is a basic implementation
        # For production, consider using a dedicated library
        current_agent = None
        for line in robots_txt.split('\n'):
            line = line.strip().lower()
            if not line or line.startswith('#'):
                continue
                
            if line.startswith('user-agent:'):
                current_agent = line.split(':', 1)[1].strip()
            elif line.startswith('disallow:') and (current_agent == user_agent or current_agent == '*'):
                disallow_path = line.split(':', 1)[1].strip()
                if disallow_path and url.startswith(ORACLE_BASE_URL + disallow_path):
                    return False
                    
        return True

class OracleCustomerScraper:
    """Scrape Oracle customer success stories."""
    
    def __init__(self, rate_limit_seconds: int = 3):
        """Initialize the scraper with rate limiting."""
        self.visited: Set[str] = set()
        self.rate_limit_seconds = rate_limit_seconds
        self.customers_data: List[Dict[str, Any]] = []
        self.customer_details: List[Dict[str, Any]] = []
        
        # Create data directory if it doesn't exist
        os.makedirs("data", exist_ok=True)
    
    def rate_limit(self) -> None:
        """Implement rate limiting to be respectful to the server."""
        time.sleep(self.rate_limit_seconds)
    
    def scrape_customers_list(self) -> None:
        """Scrape the main customers page to get a list of all customers."""
        logger.info(f"Starting to scrape customer list from {CUSTOMERS_URL}")
        
        if CUSTOMERS_URL in self.visited:
            logger.info(f"Already visited {CUSTOMERS_URL}, skipping")
            return
            
        self.visited.add(CUSTOMERS_URL)
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)  # Set headless=False for debugging
            page = browser.new_page()
            
            try:
                logger.info(f"Navigating to {CUSTOMERS_URL}")
                page.goto(CUSTOMERS_URL, timeout=60000)
                
                # Initial data extraction
                self._extract_customer_data(page)
                
                # Click "See More" button until no more results
                while True:
                    see_more = page.locator("a[data-lbl*='see-more']")
                    if see_more.count() == 0:
                        logger.info("No more 'See More' buttons found")
                        break
                        
                    logger.info("Clicking 'See More' button")
                    see_more.first.click()
                    time.sleep(5)  # Allow content to load
                    
                    # Extract data from the new page
                    self._extract_customer_data(page)
                
                logger.info(f"Found {len(self.customers_data)} customer entries")
                self._save_customers_data()
                
            except Exception as e:
                logger.error(f"Error scraping customers list: {str(e)}")
            finally:
                browser.close()
    
    def _extract_customer_data(self, page) -> None:
        """Extract customer data from the current page."""
        soup = BeautifulSoup(page.content(), 'html.parser')
        li_elements = soup.find_all("li", class_="rc05w3")
        
        for li in li_elements:
            entry = {}
            entry["title"] = li.find("div", class_="rc05heading").text.strip() if li.find("div", class_="rc05heading") else None
            entry["industry"] = li.find("span", class_="rc05def")["title"] if li.find("span", class_="rc05def") else None
            
            location_spans = li.find_all("span", class_="rc05def")
            entry["location"] = location_spans[1]["title"] if len(location_spans) > 1 else None
            
            entry["company"] = li.find("a")["data-lbl"] if li.find("a") else None
            
            link = li.find("a")["href"] if li.find("a") else None
            if link and not link.startswith("http"):
                link = f"{ORACLE_BASE_URL}{link}"
            entry["link"] = link
            
            # Only add if we have a valid link and it's not already in our data
            if link and not any(item["link"] == link for item in self.customers_data):
                self.customers_data.append(entry)
    
    def _save_customers_data(self) -> None:
        """Save the customers data to a JSON file."""
        logger.info(f"Saving customer data to {MASTER_SLUGS_FILE}")
        os.makedirs(os.path.dirname(MASTER_SLUGS_FILE), exist_ok=True)
        
        with open(MASTER_SLUGS_FILE, "w") as file:
            json.dump(self.customers_data, file, indent=2)
    
    def scrape_customer_details(self, limit: Optional[int] = None) -> None:
        """Scrape detailed information for each customer."""
        if not os.path.exists(MASTER_SLUGS_FILE):
            logger.error(f"Master slugs file {MASTER_SLUGS_FILE} not found")
            return
            
        with open(MASTER_SLUGS_FILE, "r") as file:
            self.customers_data = json.load(file)
        
        logger.info(f"Loaded {len(self.customers_data)} customers from {MASTER_SLUGS_FILE}")
        
        # Process only a subset if limit is specified
        customers_to_process = self.customers_data[:limit] if limit else self.customers_data
        
        for i, customer in enumerate(customers_to_process):
            link = customer.get("link")
            if not link:
                continue
                
            logger.info(f"Processing customer {i+1}/{len(customers_to_process)}: {customer.get('title')}")
            
            try:
                self.rate_limit()
                customer_details = self._scrape_single_customer(link)
                
                if customer_details:
                    # Merge the basic customer data with the detailed data
                    merged_data = {**customer, **customer_details}
                    self.customer_details.append(merged_data)
                    
                    # Save after each successful scrape to avoid losing data
                    self._save_customer_details()
                    
            except Exception as e:
                logger.error(f"Error processing customer {customer.get('title')}: {str(e)}")
    
    def _scrape_single_customer(self, url: str) -> Dict[str, Any]:
        """Scrape detailed information for a single customer."""
        if url in self.visited:
            logger.info(f"Already visited {url}, skipping")
            return {}
            
        self.visited.add(url)
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract products used
            products_div = soup.find('div', attrs={'data-trackas': 'rc42:products-used'})
            products = []
            if products_div:
                products = [a.text.strip() for a in products_div.find_all('a')]
                logger.info(f"Found {len(products)} products")
            
            # Extract company website
            learn_more_div = soup.find('div', attrs={'data-trackas': 'rc42:learn-more'})
            company_url = None
            company_name = None
            
            if learn_more_div:
                learn_more_link = learn_more_div.find('a')
                if learn_more_link:
                    company_url = learn_more_link.get('href')
                    company_name = learn_more_link.text.replace(", opens in new tab", "").strip()
                    logger.info(f"Found company website: {company_name} - {company_url}")
            
            # Extract case study content
            case_study_content = ""
            content_div = soup.find('div', class_='cb41w1')
            if content_div:
                paragraphs = content_div.find_all('p')
                case_study_content = "\n\n".join([p.text.strip() for p in paragraphs])
            
            # Extract customer quote if available
            quote = ""
            quote_div = soup.find('div', class_='cb41quote')
            if quote_div:
                quote = quote_div.text.strip()
            
            return {
                'company_name': company_name,
                'company_url': company_url,
                'products_used': products,
                'case_study_content': case_study_content,
                'quote': quote
            }
            
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return {}
    
    def _save_customer_details(self) -> None:
        """Save the customer details to a JSON file."""
        logger.info(f"Saving customer details to {CUSTOMER_DETAILS_FILE}")
        
        with open(CUSTOMER_DETAILS_FILE, "w") as file:
            json.dump(self.customer_details, file, indent=2)
    
    def generate_wordpress_content(self) -> None:
        """Generate WordPress content for each customer."""
        if not os.path.exists(CUSTOMER_DETAILS_FILE):
            logger.error(f"Customer details file {CUSTOMER_DETAILS_FILE} not found")
            return
            
        with open(CUSTOMER_DETAILS_FILE, "r") as file:
            self.customer_details = json.load(file)
        
        logger.info(f"Generating WordPress content for {len(self.customer_details)} customers")
        
        for customer in self.customer_details:
            title = customer.get('title', 'Unknown Customer')
            company_name = customer.get('company_name', title)
            industry = customer.get('industry', 'Technology')
            location = customer.get('location', 'Global')
            products = customer.get('products_used', [])
            case_study = customer.get('case_study_content', '')
            quote = customer.get('quote', '')
            company_url = customer.get('company_url', '')
            
            # Generate HTML content for WordPress
            content = f"""
            <h1>{title}</h1>
            
            <div class="customer-meta">
                <p><strong>Industry:</strong> {industry}</p>
                <p><strong>Location:</strong> {location}</p>
                <p><strong>Website:</strong> <a href="{company_url}" target="_blank" rel="noopener noreferrer">{company_name}</a></p>
            </div>
            
            <div class="customer-content">
                {case_study}
            </div>
            
            {f'<blockquote class="customer-quote">{quote}</blockquote>' if quote else ''}
            
            <h2>Oracle Products Used</h2>
            <ul class="products-list">
                {' '.join([f'<li>{product}</li>' for product in products])}
            </ul>
            """
            
            # Here you would call your WordPress API to create/update the page
            # For now, we'll just log the content
            logger.info(f"Generated WordPress content for {title}")
            
            # Uncomment to implement WordPress integration
            # self._create_wordpress_page(title, content)
    
    def _create_wordpress_page(self, title: str, content: str) -> None:
        """Create a WordPress page with the given title and content."""
        # This would be implemented with your WordPress API
        pass

def main():
    """Main function to run the scraper."""
    # Check if robots.txt allows crawling
    robots_parser = RobotsParser()
    if not robots_parser.is_allowed('*', CUSTOMERS_URL):
        logger.error(f"Crawling not allowed for {CUSTOMERS_URL} according to robots.txt")
        return
        
    scraper = OracleCustomerScraper(rate_limit_seconds=3)
    
    # Step 1: Scrape the list of customers
    scraper.scrape_customers_list()
    
    # Step 2: Scrape detailed information for each customer
    # Limit to 5 customers for testing
    scraper.scrape_customer_details(limit=5)
    
    # Step 3: Generate WordPress content
    scraper.generate_wordpress_content()

if __name__ == "__main__":
    main()
