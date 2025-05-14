import argparse
import json
import logging
import os
import sys
from typing import Dict, List

from job_detail_scraper import JobDetailScraper
from job_listings_scraper import JobListingsScraper
from job_scraper import JobScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('scraper.log')
    ]
)

logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Oracle Customer Success Scraper')
    parser.add_argument('--input', '-i', type=str, help='Input CSV file with URLs', default='urls.csv')
    parser.add_argument('--output-dir', '-o', type=str, help='Output directory for scraped data', default='output')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode', default=True)
    parser.add_argument('--timeout', type=int, help='Timeout in seconds', default=30)
    parser.add_argument('--workers', type=int, help='Number of parallel workers', default=5)
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Step 1: Scrape customer companies and their career URLs
    logger.info("Step 1: Scraping customer companies and career URLs")
    job_scraper = JobScraper(headless=args.headless, timeout=args.timeout)
    
    companies = job_scraper.scrape(args.input)
    job_scraper.save_to_file(companies, os.path.join(args.output_dir, 'companies.json'))
    
    # Extract career URLs from companies
    career_urls = [company.get('career_url') for company in companies if company.get('career_url')]
    
    # Step 2: Scrape job listings from career URLs
    logger.info("Step 2: Scraping job listings from career URLs")
    job_listings_scraper = JobListingsScraper(headless=args.headless, timeout=args.timeout)
    
    job_listings = job_listings_scraper.scrape(career_urls)
    job_listings_scraper.save_to_file(job_listings, os.path.join(args.output_dir, 'job_listings.json'))
    
    # Step 3: Scrape detailed job information from job listings
    logger.info("Step 3: Scraping detailed job information")
    job_detail_scraper = JobDetailScraper(
        headless=args.headless, 
        timeout=args.timeout, 
        max_workers=args.workers
    )
    
    job_details = job_detail_scraper.scrape(job_listings)
    job_detail_scraper.save_to_file(job_details, os.path.join(args.output_dir, 'job_details.json'))
    
    logger.info("Scraping completed successfully")

if __name__ == "__main__":
    main()
