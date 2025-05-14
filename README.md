# Oracle Customer Success Scraper

A comprehensive web scraping tool designed to extract job listings from Oracle customer websites. This tool includes specialized scrapers for common job platforms like Workday, Lever, and Greenhouse.

## Features

- **Company Information Scraping**: Extract company details and career URLs
- **Job Listings Scraping**: Extract job listings from career pages
- **Job Details Scraping**: Extract detailed job information
- **Platform-Specific Scrapers**: Specialized scrapers for Workday, Lever, and Greenhouse
- **Parallel Processing**: Multi-threaded job detail scraping
- **Robust Error Handling**: Comprehensive error handling and logging
- **Flexible Input/Output**: Support for CSV and JSON input/output

## Installation

1. Clone the repository:
\`\`\`bash
git clone https://github.com/yourusername/oracle-customer-scraper.git
cd oracle-customer-scraper
\`\`\`

2. Install the required dependencies:
\`\`\`bash
pip install -r requirements.txt
\`\`\`

## Usage

### Basic Usage

\`\`\`bash
python main.py --input urls.csv --output-dir output
\`\`\`

### Using Seed URLs

\`\`\`bash
# First, extract URLs from the seed file
python url_extractor.py

# Then run the main scraper
python main.py --input urls.csv --output-dir output
\`\`\`

### Using the Simplified Runner

\`\`\`bash
python scraper_runner.py --seed seed_urls.json --output-dir output
\`\`\`

### Command Line Arguments

- `--input`, `-i`: Input CSV file with URLs (default: urls.csv)
- `--output-dir`, `-o`: Output directory for scraped data (default: output)
- `--headless`: Run in headless mode (default: True)
- `--timeout`: Timeout in seconds (default: 30)
- `--workers`: Number of parallel workers (default: 5)
- `--seed`, `-s`: Seed JSON file with Oracle customers (for scraper_runner.py)
- `--skip-listings`: Skip job listings scraping (for scraper_runner.py)
- `--skip-details`: Skip job details scraping (for scraper_runner.py)

## Project Structure

- `job_scraper.py`: Scrapes company information and career URLs
- `job_listings_scraper.py`: Scrapes job listings from career pages
- `job_detail_scraper.py`: Scrapes detailed job information
- `platform_scrapers.py`: Contains specialized scrapers for job platforms
- `main.py`: Main entry point for the scraper
- `url_extractor.py`: Utility to extract URLs from seed file
- `scraper_runner.py`: Simplified runner for the scraper
- `seed_urls.json`: Seed file with Oracle customer information

## Output

The scraper generates three JSON files:

1. `companies.json`: Contains company information and career URLs
2. `job_listings.json`: Contains job listings from career pages
3. `job_details.json`: Contains detailed job information

## License

This project is licensed under the MIT License - see the LICENSE file for details.
