import json
import csv
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def extract_urls_from_seed(seed_file, output_file):
    """
    Extract URLs from the seed_urls.json file and save them to a CSV file
    for use with the job scraper.
    
    Args:
        seed_file: Path to the seed_urls.json file
        output_file: Path to the output CSV file
    """
    # Load the seed URLs
    with open(seed_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Extract the URLs
    urls = [customer['url'] for customer in data['oracle_customers']]
    
    # Write the URLs to a CSV file
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        for url in urls:
            writer.writerow([url])
    
    logger.info(f"Extracted {len(urls)} URLs to {output_file}")

if __name__ == "__main__":
    extract_urls_from_seed("seed_urls.json", "urls.csv")
