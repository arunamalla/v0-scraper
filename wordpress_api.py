import requests
import logging
import json
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class WordPressAPI:
    """Class to interact with WordPress REST API."""
    
    def __init__(self, base_url: str = None, username: str = None, password: str = None):
        """Initialize the WordPress API client.
        
        Args:
            base_url: The base URL of the WordPress site
            username: WordPress username
            password: WordPress application password
        """
        self.base_url = base_url or "https://your-wordpress-site.com/wp-json/wp/v2"
        self.username = username or "your_username"
        self.password = password or "your_app_password"
        self.auth = (self.username, self.password)
    
    def create_page(self, title: str, content: str, status: str = "draft", 
                   featured_media: int = 0, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a WordPress page.
        
        Args:
            title: The title of the page
            content: The HTML content of the page
            status: The status of the page (draft, publish, etc.)
            featured_media: ID of the featured image
            meta: Additional metadata for the page
            
        Returns:
            The response from the WordPress API
        """
        endpoint = f"{self.base_url}/pages"
        
        data = {
            "title": title,
            "content": content,
            "status": status
        }
        
        if featured_media:
            data["featured_media"] = featured_media
            
        if meta:
            data["meta"] = meta
        
        try:
            response = requests.post(endpoint, json=data, auth=self.auth)
            response.raise_for_status()
            logger.info(f"Successfully created page: {title}")
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error creating page {title}: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            return {"error": str(e)}
    
    def update_page(self, page_id: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing WordPress page.
        
        Args:
            page_id: The ID of the page to update
            data: The data to update
            
        Returns:
            The response from the WordPress API
        """
        endpoint = f"{self.base_url}/pages/{page_id}"
        
        try:
            response = requests.post(endpoint, json=data, auth=self.auth)
            response.raise_for_status()
            logger.info(f"Successfully updated page ID: {page_id}")
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error updating page {page_id}: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            return {"error": str(e)}
    
    def upload_media(self, file_path: str, title: str = None) -> Dict[str, Any]:
        """Upload media to WordPress.
        
        Args:
            file_path: Path to the file to upload
            title: Title for the media
            
        Returns:
            The response from the WordPress API
        """
        endpoint = f"{self.base_url}/media"
        
        headers = {
            "Content-Disposition": f'attachment; filename="{file_path.split("/")[-1]}"'
        }
        
        data = {}
        if title:
            data["title"] = title
        
        try:
            with open(file_path, 'rb') as file:
                response = requests.post(
                    endpoint,
                    headers=headers,
                    data=data,
                    files={"file": file},
                    auth=self.auth
                )
                response.raise_for_status()
                logger.info(f"Successfully uploaded media: {file_path}")
                return response.json()
        except (requests.RequestException, IOError) as e:
            logger.error(f"Error uploading media {file_path}: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            return {"error": str(e)}
    
    def create_customer_profile(self, customer_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a customer profile page.
        
        Args:
            customer_data: Data for the customer profile
            
        Returns:
            The response from the WordPress API
        """
        title = customer_data.get('title', 'Unknown Customer')
        company_name = customer_data.get('company_name', title)
        industry = customer_data.get('industry', 'Technology')
        location = customer_data.get('location', 'Global')
        products = customer_data.get('products_used', [])
        case_study = customer_data.get('case_study_content', '')
        quote = customer_data.get('quote', '')
        company_url = customer_data.get('company_url', '')
        
        # Generate HTML content
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
        
        # Add schema.org JSON-LD for SEO
        schema = {
            "@context": "https://schema.org",
            "@type": "Organization",
            "name": company_name,
            "description": case_study[:160] if case_study else f"{company_name} is an Oracle customer.",
            "url": company_url,
            "industry": industry,
            "location": {
                "@type": "Place",
                "address": {
                    "@type": "PostalAddress",
                    "addressLocality": location
                }
            }
        }
        
        content += f"""
        <script type="application/ld+json">
        {json.dumps(schema, indent=2)}
        </script>
        """
        
        # Create the page
        return self.create_page(
            title=title,
            content=content,
            status="draft",
            meta={
                "customer_industry": industry,
                "customer_location": location,
                "customer_products": ", ".join(products)
            }
        )
