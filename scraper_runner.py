import os
import argparse
import logging
import sys
import json
from job_listings_scraper import JobListingsScraper
from job_detail_scraper import JobDetailScraper

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
    parser = argparse.ArgumentParser(description='Oracle Customer Job Scraper')
    parser.add_argument('--seed', '-s', type=str, help='Seed JSON file with Oracle customers', default='seed_urls.json')
    parser.add_argument('--output-dir', '-o', type=str, help='Output directory for scraped data', default='output')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode', default=True)
    parser.add_argument('--timeout', type=int, help='Timeout in seconds', default=30)
    parser.add_argument('--workers', type=int, help='Number of parallel workers', default=5)
    parser.add_argument('--skip-listings', action='store_true', help='Skip job listings scraping', default=False)
    parser.add_argument('--skip-details', action='store_true', help='Skip job details scraping', default=False)
    
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Load the seed URLs
    with open(args.seed, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Extract career URLs directly from the seed file
    # In a real scenario, you would first run job_scraper.py to find career URLs
    # Here we're simulating that by using the URLs directly
    career_urls = [customer['url'] + '/careers' for customer in data['oracle_customers']]
    logger.info(f"Using {len(career_urls)} career URLs from seed file")
    
    # Step 1: Scrape job listings from career URLs
    if not args.skip_listings:
        logger.info("Step 1: Scraping job listings from career URLs")
        job_listings_scraper = JobListingsScraper(headless=args.headless, timeout=args.timeout)
        
        job_listings = job_listings_scraper.scrape(career_urls)
        job_listings_scraper.save_to_file(job_listings, os.path.join(args.output_dir, 'job_listings.json'))
    else:
        # Load existing job listings
        with open(os.path.join(args.output_dir, 'job_listings.json'), 'r', encoding='utf-8') as f:
            job_listings = json.load(f)
        logger.info(f"Loaded {len(job_listings)} existing job listings")
    
    # Step 2: Scrape detailed job information from job listings
    if not args.skip_details:
        logger.info("Step 2: Scraping detailed job information")
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
