# Oracle Customer Success Scraper

This project scrapes customer success stories from Oracle's website and extracts career URLs from customer websites.

## Features

- Scrapes the Oracle Customers page to get a list of all customers
- Extracts detailed information for each customer
- Finds career URLs on customer websites
- Respects robots.txt and implements rate limiting
- Modular design with separate components for different responsibilities

## Project Structure

- `main.py`: Entry point for the scraper
- `logger.py`: Handles logging
- `error_handler.py`: Handles errors
- `data_manager.py`: Manages data storage and retrieval
- `rate_limiter.py`: Implements rate limiting
- `robots_parser.py`: Checks if crawling is allowed
- `oracle_customer_scraper.py`: Scrapes the main Oracle customers page
- `customer_detail_scraper.py`: Scrapes individual customer pages
- `job_scraper.py`: Extracts career URLs from customer websites

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

## Output Files

- `data/customers_list.json`: List of all customers
- `data/customer_details.json`: Detailed information for each customer
- `data/career_urls.json`: Career URLs for each customer

## Notes

- This scraper is designed to be respectful of servers by implementing rate limiting
- Always check robots.txt before scraping any website
- This is for educational purposes only
