import os
import argparse
import time
from logger import Logger
from error_handler import ErrorHandler
from data_manager import DataManager
from oracle_customer_scraper import OracleCustomerScraper
from customer_detail_scraper import CustomerDetailScraper
from job_scraper import JobScraper
from job_listings_scraper import JobListingsScraper
from job_detail_scraper import JobDetailScraper
from url_tracker import URLTracker

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Oracle Customer Success Scraper')
    
    parser.add_argument('--rate-limit', type=int, default=3,
                        help='Rate limit in seconds between requests (default: 3)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit the number of customers to process (default: all)')
    parser.add_argument('--data-dir', type=str, default='data',
                        help='Directory to store data files (default: data)')
    parser.add_argument('--log-dir', type=str, default='logs',
                        help='Directory to store log files (default: logs)')
    parser.add_argument('--skip-customers', action='store_true',
                        help='Skip scraping the customers list')
    parser.add_argument('--skip-details', action='store_true',
                        help='Skip scraping customer details')
    parser.add_argument('--skip-careers', action='store_true',
                        help='Skip scraping career URLs')
    parser.add_argument('--skip-job-listings', action='store_true',
                        help='Skip scraping job listings')
    parser.add_argument('--skip-job-details', action='store_true',
                        help='Skip scraping job details')
    parser.add_argument('--threads', type=int, default=5,
                        help='Number of threads to use for parallel scraping (default: 5)')
    parser.add_argument('--no-resume', action='store_true',
                        help='Do not resume from previous checkpoint')
    parser.add_argument('--min-customers', type=int, default=100,
                        help='Minimum number of customers expected (default: 100)')
    
    return parser.parse_args()

def main():
    """Main function to run the scraper."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Create directories
    os.makedirs(args.data_dir, exist_ok=True)
    os.makedirs(args.log_dir, exist_ok=True)
    
    # Initialize shared components
    logger = Logger(args.log_dir)
    error_handler = ErrorHandler(logger)
    data_manager = DataManager(args.data_dir, logger, error_handler)
    url_tracker = URLTracker(args.data_dir, logger, error_handler)
    
    # Display warning about SSL verification
    logger.warning("SSL certificate verification will be disabled for sites with certificate issues. This is less secure but necessary for scraping some sites.")
    
    # File paths
    customers_file = os.path.join(args.data_dir, "customers_list.json")
    details_file = os.path.join(args.data_dir, "customer_details.json")
    careers_file = os.path.join(args.data_dir, "career_urls.json")
    job_listings_file = os.path.join(args.data_dir, "job_listings.json")
    job_details_file = os.path.join(args.data_dir, "job_details.json")
    
    # Check if we should resume
    resume = not args.no_resume
    
    # Step 1: Scrape the list of customers
    customers_data = []
    if not args.skip_customers:
        logger.info("Starting to scrape customers list")
        
        # Add retry mechanism
        max_retries = 3
        retry_count = 0
        min_expected_customers = args.min_customers
        
        while retry_count < max_retries:
            oracle_scraper = OracleCustomerScraper(
                rate_limit_seconds=args.rate_limit,
                logger=logger,
                error_handler=error_handler,
                data_manager=data_manager,
                url_tracker=url_tracker
            )
            customers_data = oracle_scraper.scrape_customers_list(output_file="customers_list.json", resume=resume)
            
            if len(customers_data) >= min_expected_customers:
                logger.info(f"Successfully scraped {len(customers_data)} customers")
                break
            
            retry_count += 1
            if retry_count < max_retries:
                logger.warning(f"Only found {len(customers_data)} customers. Retrying ({retry_count}/{max_retries})...")
                time.sleep(60)  # Wait a minute before retrying
        
        if len(customers_data) < min_expected_customers:
            logger.warning(f"Could only find {len(customers_data)} customers after {max_retries} attempts. Continuing with what we have.")
    else:
        logger.info("Skipping customers list scraping, loading from file")
        customers_data = data_manager.load_data("customers_list.json") or []
    
    # Step 2: Scrape detailed information for each customer
    customer_details = []
    if not args.skip_details and customers_data:
        logger.info("Starting to scrape customer details")
        detail_scraper = CustomerDetailScraper(
            rate_limit_seconds=args.rate_limit,
            logger=logger,
            error_handler=error_handler,
            data_manager=data_manager,
            url_tracker=url_tracker
        )
        customer_details = detail_scraper.scrape_customer_details(
            customers_data,
            output_file="customer_details.json",
            limit=args.limit,
            max_workers=args.threads,
            resume=resume
        )
    else:
        logger.info("Skipping customer details scraping, loading from file")
        customer_details = data_manager.load_data("customer_details.json") or []
    
    # Step 3: Scrape career URLs
    career_urls = []
    if not args.skip_careers and customer_details:
        logger.info("Starting to scrape career URLs")
        job_scraper = JobScraper(
            rate_limit_seconds=args.rate_limit,
            logger=logger,
            error_handler=error_handler,
            data_manager=data_manager,
            url_tracker=url_tracker
        )
        career_urls = job_scraper.scrape_career_urls(
            customer_details,
            output_file="career_urls.json",
            limit=args.limit,
            max_workers=args.threads,
            resume=resume
        )
    else:
        logger.info("Skipping career URLs scraping, loading from file")
        career_urls = data_manager.load_data("career_urls.json") or []
    
    # Step 4: Scrape job listings
    job_listings = []
    if not args.skip_job_listings and career_urls:
        logger.info("Starting to scrape job listings")
        job_listings_scraper = JobListingsScraper(
            rate_limit_seconds=args.rate_limit,
            logger=logger,
            error_handler=error_handler,
            data_manager=data_manager,
            url_tracker=url_tracker
        )
        job_listings = job_listings_scraper.scrape_job_listings(
            career_urls_file="career_urls.json",
            output_file="job_listings.json",
            limit=args.limit,
            max_workers=args.threads,
            resume=resume
        )
    else:
        logger.info("Skipping job listings scraping, loading from file")
        job_listings = data_manager.load_data("job_listings.json") or []
    
    # Step 5: Scrape job details
    if not args.skip_job_details and job_listings:
        logger.info("Starting to scrape job details")
        job_detail_scraper = JobDetailScraper(
            rate_limit_seconds=args.rate_limit,
            logger=logger,
            error_handler=error_handler,
            data_manager=data_manager,
            url_tracker=url_tracker
        )
        job_detail_scraper.scrape_job_details(
            job_listings_file="job_listings.json",
            output_file="job_details.json",
            limit=args.limit,
            max_workers=args.threads,
            resume=resume
        )
    else:
        logger.info("Skipping job details scraping")
    
    logger.info("Scraping completed")

if __name__ == "__main__":
    main()
