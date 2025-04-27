# Oracle Customer Success Scraper

This project scrapes customer success stories from Oracle's website and extracts career URLs from customer websites.

## Features

- Scrapes the Oracle Customers page to get a list of all customers
- Extracts detailed information for each customer
- Finds career URLs on customer websites
- Respects robots.txt and implements rate limiting
- Handles SSL certificate verification issues
- Uses multithreading for faster scraping
- Implements resume functionality to continue from where it left off
- Tracks visited URLs to avoid re-scraping
- Modular design with separate components for different responsibilities

## Project Structure

- `main.py`: Entry point for the scraper
- `logger.py`: Handles logging
- `error_handler.py`: Handles errors
- `data_manager.py`: Manages data storage and retrieval
- `rate_limiter.py`: Implements rate limiting
- `robots_parser.py`: Checks if crawling is allowed
- `url_tracker.py`: Tracks visited URLs and manages checkpoints
- `oracle_customer_scraper.py`: Scrapes the main Oracle customers page
- `customer_detail_scraper.py`: Scrapes individual customer pages
- `job_scraper.py`: Extracts career URLs from customer websites
- `utils.py`: Utility functions for making requests with SSL fallback

## Setup

1. Install dependencies:

\`\`\`bash
pip install requests beautifulsoup4 playwright
playwright install chromium
\`\`\`

2. Create directories:

\`\`\`bash
mkdir -p data logs
\`\`\`

3. Run the scraper:

\`\`\`bash
python main.py
\`\`\`

## Command Line Arguments

- `--rate-limit`: Rate limit in seconds between requests (default: 3)
- `--limit`: Limit the number of customers to process (default: all)
- `--data-dir`: Directory to store data files (default: data)
- `--log-dir`: Directory to store log files (default: logs)
- `--skip-customers`: Skip scraping the customers list
- `--skip-details`: Skip scraping customer details
- `--skip-careers`: Skip scraping career URLs
- `--threads`: Number of threads to use for parallel scraping (default: 5)
- `--no-resume`: Do not resume from previous checkpoint

## Example Usage

Scrape everything:
\`\`\`bash
python main.py
\`\`\`

Scrape only the first 10 customers:
\`\`\`bash
python main.py --limit 10
\`\`\`

Skip scraping the customers list and use existing data:
\`\`\`bash
python main.py --skip-customers
\`\`\`

Use more threads for faster scraping:
\`\`\`bash
python main.py --threads 10
\`\`\`

Start from scratch (don't resume):
\`\`\`bash
python main.py --no-resume
\`\`\`

## Output Files

- `data/customers_list.json`: List of all customers
- `data/customer_details.json`: Detailed information for each customer
- `data/career_urls.json`: Career URLs for each customer
- `data/visited_customer_urls.json`: Tracking of visited customer URLs
- `data/visited_job_urls.json`: Tracking of visited job URLs
- `data/scraper_checkpoint.json`: Checkpoint for resuming scraping

## Multithreading

The scraper uses Python's `concurrent.futures` module with ThreadPoolExecutor to parallelize the scraping process. This significantly speeds up the scraping of customer details and career URLs. The number of threads can be controlled with the `--threads` command-line argument.

## Resume Functionality

If the scraper is interrupted for any reason, it can resume from where it left off. It maintains a checkpoint file that records the current stage and progress. When restarted, it will load this checkpoint and continue from there. This can be disabled with the `--no-resume` flag.

## URL Tracking

The scraper keeps track of all visited URLs to avoid re-scraping the same pages. This is especially useful when new customers are added to Oracle's website, as the scraper will only process the new ones. The tracking is persistent across runs.

## Notes

- This scraper is designed to be respectful of servers by implementing rate limiting
- Always check robots.txt before scraping any website
- SSL certificate verification is disabled for sites with certificate issues
- This is for educational purposes only
