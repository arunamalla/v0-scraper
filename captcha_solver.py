import os
import time
import base64
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from logger import Logger
from error_handler import ErrorHandler
from utils import safe_request

class CaptchaSolver:
    """Detect and solve CAPTCHAs encountered during scraping."""
    
    # Common CAPTCHA identifiers in HTML
    CAPTCHA_IDENTIFIERS = [
        # Text indicators
        'captcha', 'recaptcha', 'hcaptcha', 'security check', 'bot check', 'human verification',
        # Element IDs and classes
        'g-recaptcha', 'h-captcha', 'captcha-container', 'captcha-box',
        # Image alt text
        'captcha image', 'security image',
    ]
    
    # reCAPTCHA specific identifiers
    RECAPTCHA_IDENTIFIERS = [
        'g-recaptcha', 'recaptcha', 'google.com/recaptcha'
    ]
    
    # hCaptcha specific identifiers
    HCAPTCHA_IDENTIFIERS = [
        'h-captcha', 'hcaptcha.com'
    ]
    
    def __init__(self, api_key=None, service='2captcha', logger=None, error_handler=None):
        """Initialize the CAPTCHA solver.
        
        Args:
            api_key: API key for the CAPTCHA solving service
            service: CAPTCHA solving service to use ('2captcha', 'anti-captcha', etc.)
            logger: Logger instance
            error_handler: ErrorHandler instance
        """
        self.api_key = api_key or os.environ.get('CAPTCHA_API_KEY')
        self.service = service
        self.logger = logger or Logger()
        self.error_handler = error_handler or ErrorHandler(self.logger)
        
        # Service-specific API endpoints
        self.service_endpoints = {
            '2captcha': {
                'submit': 'https://2captcha.com/in.php',
                'retrieve': 'https://2captcha.com/res.php'
            },
            'anti-captcha': {
                'submit': 'https://api.anti-captcha.com/createTask',
                'retrieve': 'https://api.anti-captcha.com/getTaskResult'
            }
        }
        
        if not self.api_key:
            self.logger.warning("No CAPTCHA API key provided. CAPTCHA solving will be limited.")
    
    def detect_captcha(self, response):
        """Detect if a response contains a CAPTCHA.
        
        Args:
            response: Response object from requests
            
        Returns:
            Dictionary with captcha_type and captcha_data if detected, None otherwise
        """
        if not response or not response.content:
            return None
            
        try:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Check for common CAPTCHA indicators in the HTML
            page_text = soup.get_text().lower()
            page_html = str(soup).lower()
            
            # Check if any CAPTCHA identifier is in the page
            if any(identifier in page_text or identifier in page_html for identifier in self.CAPTCHA_IDENTIFIERS):
                self.logger.info(f"CAPTCHA detected on {response.url}")
                
                # Determine the type of CAPTCHA
                if any(identifier in page_text or identifier in page_html for identifier in self.RECAPTCHA_IDENTIFIERS):
                    # Extract reCAPTCHA site key
                    site_key = self._extract_recaptcha_site_key(soup)
                    if site_key:
                        self.logger.info(f"reCAPTCHA detected with site key: {site_key}")
                        return {
                            'captcha_type': 'recaptcha',
                            'site_key': site_key,
                            'url': response.url
                        }
                
                elif any(identifier in page_text or identifier in page_html for identifier in self.HCAPTCHA_IDENTIFIERS):
                    # Extract hCaptcha site key
                    site_key = self._extract_hcaptcha_site_key(soup)
                    if site_key:
                        self.logger.info(f"hCaptcha detected with site key: {site_key}")
                        return {
                            'captcha_type': 'hcaptcha',
                            'site_key': site_key,
                            'url': response.url
                        }
                
                # Check for image CAPTCHA
                img_captcha = self._extract_image_captcha(soup, response.url)
                if img_captcha:
                    self.logger.info(f"Image CAPTCHA detected")
                    return {
                        'captcha_type': 'image',
                        'image_url': img_captcha,
                        'url': response.url
                    }
                
                # Generic CAPTCHA detected but couldn't determine type
                self.logger.warning(f"CAPTCHA detected but type could not be determined on {response.url}")
                return {
                    'captcha_type': 'unknown',
                    'url': response.url
                }
            
            return None
            
        except Exception as e:
            self.error_handler.handle_error(e, "Error detecting CAPTCHA")
            return None
    
    def solve_captcha(self, captcha_data):
        """Solve a detected CAPTCHA.
        
        Args:
            captcha_data: Dictionary with captcha_type and other required data
            
        Returns:
            CAPTCHA solution or None if solving failed
        """
        if not self.api_key:
            self.logger.error("Cannot solve CAPTCHA: No API key provided")
            return None
            
        try:
            captcha_type = captcha_data.get('captcha_type')
            
            if captcha_type == 'recaptcha':
                return self._solve_recaptcha(captcha_data)
            elif captcha_type == 'hcaptcha':
                return self._solve_hcaptcha(captcha_data)
            elif captcha_type == 'image':
                return self._solve_image_captcha(captcha_data)
            else:
                self.logger.error(f"Unsupported CAPTCHA type: {captcha_type}")
                return None
                
        except Exception as e:
            self.error_handler.handle_error(e, "Error solving CAPTCHA")
            return None
    
    def handle_captcha_page(self, session, url, method='get', data=None, headers=None):
        """Handle a page that might contain a CAPTCHA.
        
        Args:
            session: Requests session
            url: URL to request
            method: HTTP method (get, post, etc.)
            data: Form data for POST requests
            headers: Request headers
            
        Returns:
            Response object after handling any CAPTCHAs
        """
        try:
            # Make the initial request
            request_method = getattr(session, method.lower())
            response = request_method(url, data=data, headers=headers, verify=False)
            
            # Check if the response contains a CAPTCHA
            captcha_data = self.detect_captcha(response)
            if not captcha_data:
                return response
                
            self.logger.info(f"Handling CAPTCHA on {url}")
            
            # Solve the CAPTCHA
            solution = self.solve_captcha(captcha_data)
            if not solution:
                self.logger.error("Failed to solve CAPTCHA")
                return response
                
            # Submit the CAPTCHA solution
            if captcha_data['captcha_type'] == 'recaptcha' or captcha_data['captcha_type'] == 'hcaptcha':
                # For reCAPTCHA and hCaptcha, we need to find the form and submit it with the solution
                soup = BeautifulSoup(response.content, 'html.parser')
                form = soup.find('form')
                
                if form:
                    # Extract form action and method
                    form_action = form.get('action')
                    form_method = form.get('method', 'post').lower()
                    
                    # If form action is relative, make it absolute
                    if form_action and not form_action.startswith(('http://', 'https://')):
                        parsed_url = urlparse(url)
                        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                        form_action = f"{base_url}{form_action if form_action.startswith('/') else '/' + form_action}"
                    
                    # If no form action, use the current URL
                    form_action = form_action or url
                    
                    # Extract form fields
                    form_data = {}
                    for input_field in form.find_all('input'):
                        name = input_field.get('name')
                        value = input_field.get('value', '')
                        if name:
                            form_data[name] = value
                    
                    # Add the CAPTCHA solution
                    if captcha_data['captcha_type'] == 'recaptcha':
                        form_data['g-recaptcha-response'] = solution
                    elif captcha_data['captcha_type'] == 'hcaptcha':
                        form_data['h-captcha-response'] = solution
                    
                    # Submit the form
                    self.logger.info(f"Submitting form with CAPTCHA solution to {form_action}")
                    form_method_func = getattr(session, form_method)
                    return form_method_func(form_action, data=form_data, headers=headers, verify=False)
                else:
                    self.logger.warning("No form found to submit CAPTCHA solution")
            
            elif captcha_data['captcha_type'] == 'image':
                # For image CAPTCHAs, we need to find the form and submit it with the solution
                soup = BeautifulSoup(response.content, 'html.parser')
                form = soup.find('form')
                
                if form:
                    # Extract form action and method
                    form_action = form.get('action')
                    form_method = form.get('method', 'post').lower()
                    
                    # If form action is relative, make it absolute
                    if form_action and not form_action.startswith(('http://', 'https://')):
                        parsed_url = urlparse(url)
                        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                        form_action = f"{base_url}{form_action if form_action.startswith('/') else '/' + form_action}"
                    
                    # If no form action, use the current URL
                    form_action = form_action or url
                    
                    # Extract form fields
                    form_data = {}
                    for input_field in form.find_all('input'):
                        name = input_field.get('name')
                        value = input_field.get('value', '')
                        if name:
                            form_data[name] = value
                    
                    # Find the CAPTCHA input field
                    captcha_input = None
                    for input_field in form.find_all('input'):
                        name = input_field.get('name', '').lower()
                        if 'captcha' in name:
                            captcha_input = name
                            break
                    
                    if captcha_input:
                        form_data[captcha_input] = solution
                        
                        # Submit the form
                        self.logger.info(f"Submitting form with image CAPTCHA solution to {form_action}")
                        form_method_func = getattr(session, form_method)
                        return form_method_func(form_action, data=form_data, headers=headers, verify=False)
                    else:
                        self.logger.warning("No CAPTCHA input field found in form")
                else:
                    self.logger.warning("No form found to submit CAPTCHA solution")
            
            return response
            
        except Exception as e:
            self.error_handler.handle_error(e, f"Error handling CAPTCHA page: {url}")
            return None
    
    def _extract_recaptcha_site_key(self, soup):
        """Extract reCAPTCHA site key from HTML.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Site key or None if not found
        """
        # Look for the div with class g-recaptcha
        recaptcha_div = soup.find('div', class_='g-recaptcha')
        if recaptcha_div and 'data-sitekey' in recaptcha_div.attrs:
            return recaptcha_div['data-sitekey']
        
        # Look for reCAPTCHA in script tags
        for script in soup.find_all('script'):
            script_text = script.string if script.string else ''
            if 'recaptcha' in script_text.lower() and 'sitekey' in script_text.lower():
                # Try to extract the site key using a simple regex-like approach
                parts = script_text.split('sitekey')
                if len(parts) > 1:
                    # Find the first quote after sitekey
                    quote_pos = parts[1].find('"')
                    if quote_pos != -1:
                        # Find the closing quote
                        end_quote_pos = parts[1].find('"', quote_pos + 1)
                        if end_quote_pos != -1:
                            return parts[1][quote_pos + 1:end_quote_pos]
        
        return None
    
    def _extract_hcaptcha_site_key(self, soup):
        """Extract hCaptcha site key from HTML.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Site key or None if not found
        """
        # Look for the div with class h-captcha
        hcaptcha_div = soup.find('div', class_='h-captcha')
        if hcaptcha_div and 'data-sitekey' in hcaptcha_div.attrs:
            return hcaptcha_div['data-sitekey']
        
        # Look for hCaptcha in script tags
        for script in soup.find_all('script'):
            script_text = script.string if script.string else ''
            if 'hcaptcha' in script_text.lower() and 'sitekey' in script_text.lower():
                # Try to extract the site key using a simple regex-like approach
                parts = script_text.split('sitekey')
                if len(parts) > 1:
                    # Find the first quote after sitekey
                    quote_pos = parts[1].find('"')
                    if quote_pos != -1:
                        # Find the closing quote
                        end_quote_pos = parts[1].find('"', quote_pos + 1)
                        if end_quote_pos != -1:
                            return parts[1][quote_pos + 1:end_quote_pos]
        
        return None
    
    def _extract_image_captcha(self, soup, base_url):
        """Extract image CAPTCHA URL from HTML.
        
        Args:
            soup: BeautifulSoup object
            base_url: Base URL for resolving relative URLs
            
        Returns:
            Image URL or None if not found
        """
        # Look for img tags with captcha in the src, alt, or class
        for img in soup.find_all('img'):
            src = img.get('src', '')
            alt = img.get('alt', '')
            img_class = img.get('class', [])
            
            if any('captcha' in attr.lower() for attr in [src, alt] + (img_class if isinstance(img_class, list) else [img_class])):
                # If the src is relative, make it absolute
                if src and not src.startswith(('http://', 'https://', 'data:')):
                    parsed_url = urlparse(base_url)
                    base_domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
                    src = f"{base_domain}{src if src.startswith('/') else '/' + src}"
                
                return src
        
        return None
    
    def _solve_recaptcha(self, captcha_data):
        """Solve reCAPTCHA using a CAPTCHA solving service.
        
        Args:
            captcha_data: Dictionary with site_key and url
            
        Returns:
            reCAPTCHA solution or None if solving failed
        """
        site_key = captcha_data.get('site_key')
        url = captcha_data.get('url')
        
        if not site_key or not url:
            self.logger.error("Missing site key or URL for reCAPTCHA solving")
            return None
        
        if self.service == '2captcha':
            try:
                # Submit the reCAPTCHA to 2captcha
                submit_url = f"{self.service_endpoints['2captcha']['submit']}?key={self.api_key}&method=userrecaptcha&googlekey={site_key}&pageurl={url}&json=1"
                response = safe_request(submit_url)
                response_json = response.json()
                
                if response_json.get('status') == 1:
                    request_id = response_json.get('request')
                    
                    # Wait for the solution
                    self.logger.info(f"Waiting for reCAPTCHA solution (request ID: {request_id})")
                    
                    # Poll for the result
                    for _ in range(30):  # Try for 5 minutes (30 * 10 seconds)
                        time.sleep(10)  # Wait 10 seconds between polls
                        
                        result_url = f"{self.service_endpoints['2captcha']['retrieve']}?key={self.api_key}&action=get&id={request_id}&json=1"
                        result_response = safe_request(result_url)
                        result_json = result_response.json()
                        
                        if result_json.get('status') == 1:
                            self.logger.info("reCAPTCHA solved successfully")
                            return result_json.get('request')
                    
                    self.logger.error("Timeout waiting for reCAPTCHA solution")
                    return None
                else:
                    self.logger.error(f"Error submitting reCAPTCHA: {response_json.get('error_text')}")
                    return None
                    
            except Exception as e:
                self.error_handler.handle_error(e, "Error solving reCAPTCHA with 2captcha")
                return None
        
        elif self.service == 'anti-captcha':
            try:
                # Submit the reCAPTCHA to Anti-Captcha
                submit_data = {
                    "clientKey": self.api_key,
                    "task": {
                        "type": "NoCaptchaTaskProxyless",
                        "websiteURL": url,
                        "websiteKey": site_key
                    }
                }
                
                response = safe_request(
                    self.service_endpoints['anti-captcha']['submit'],
                    method='post',
                    json=submit_data
                )
                response_json = response.json()
                
                if response_json.get('errorId') == 0:
                    task_id = response_json.get('taskId')
                    
                    # Wait for the solution
                    self.logger.info(f"Waiting for reCAPTCHA solution (task ID: {task_id})")
                    
                    # Poll for the result
                    for _ in range(30):  # Try for 5 minutes (30 * 10 seconds)
                        time.sleep(10)  # Wait 10 seconds between polls
                        
                        result_data = {
                            "clientKey": self.api_key,
                            "taskId": task_id
                        }
                        
                        result_response = safe_request(
                            self.service_endpoints['anti-captcha']['retrieve'],
                            method='post',
                            json=result_data
                        )
                        result_json = result_response.json()
                        
                        if result_json.get('errorId') == 0 and result_json.get('status') == 'ready':
                            self.logger.info("reCAPTCHA solved successfully")
                            return result_json.get('solution', {}).get('gRecaptchaResponse')
                    
                    self.logger.error("Timeout waiting for reCAPTCHA solution")
                    return None
                else:
                    self.logger.error(f"Error submitting reCAPTCHA: {response_json.get('errorDescription')}")
                    return None
                    
            except Exception as e:
                self.error_handler.handle_error(e, "Error solving reCAPTCHA with Anti-Captcha")
                return None
        
        else:
            self.logger.error(f"Unsupported CAPTCHA solving service: {self.service}")
            return None
    
    def _solve_hcaptcha(self, captcha_data):
        """Solve hCaptcha using a CAPTCHA solving service.
        
        Args:
            captcha_data: Dictionary with site_key and url
            
        Returns:
            hCaptcha solution or None if solving failed
        """
        site_key = captcha_data.get('site_key')
        url = captcha_data.get('url')
        
        if not site_key or not url:
            self.logger.error("Missing site key or URL for hCaptcha solving")
            return None
        
        if self.service == '2captcha':
            try:
                # Submit the hCaptcha to 2captcha
                submit_url = f"{self.service_endpoints['2captcha']['submit']}?key={self.api_key}&method=hcaptcha&sitekey={site_key}&pageurl={url}&json=1"
                response = safe_request(submit_url)
                response_json = response.json()
                
                if response_json.get('status') == 1:
                    request_id = response_json.get('request')
                    
                    # Wait for the solution
                    self.logger.info(f"Waiting for hCaptcha solution (request ID: {request_id})")
                    
                    # Poll for the result
                    for _ in range(30):  # Try for 5 minutes (30 * 10 seconds)
                        time.sleep(10)  # Wait 10 seconds between polls
                        
                        result_url = f"{self.service_endpoints['2captcha']['retrieve']}?key={self.api_key}&action=get&id={request_id}&json=1"
                        result_response = safe_request(result_url)
                        result_json = result_response.json()
                        
                        if result_json.get('status') == 1:
                            self.logger.info("hCaptcha solved successfully")
                            return result_json.get('request')
                    
                    self.logger.error("Timeout waiting for hCaptcha solution")
                    return None
                else:
                    self.logger.error(f"Error submitting hCaptcha: {response_json.get('error_text')}")
                    return None
                    
            except Exception as e:
                self.error_handler.handle_error(e, "Error solving hCaptcha with 2captcha")
                return None
        
        elif self.service == 'anti-captcha':
            try:
                # Submit the hCaptcha to Anti-Captcha
                submit_data = {
                    "clientKey": self.api_key,
                    "task": {
                        "type": "HCaptchaTaskProxyless",
                        "websiteURL": url,
                        "websiteKey": site_key
                    }
                }
                
                response = safe_request(
                    self.service_endpoints['anti-captcha']['submit'],
                    method='post',
                    json=submit_data
                )
                response_json = response.json()
                
                if response_json.get('errorId') == 0:
                    task_id = response_json.get('taskId')
                    
                    # Wait for the solution
                    self.logger.info(f"Waiting for hCaptcha solution (task ID: {task_id})")
                    
                    # Poll for the result
                    for _ in range(30):  # Try for 5 minutes (30 * 10 seconds)
                        time.sleep(10)  # Wait 10 seconds between polls
                        
                        result_data = {
                            "clientKey": self.api_key,
                            "taskId": task_id
                        }
                        
                        result_response = safe_request(
                            self.service_endpoints['anti-captcha']['retrieve'],
                            method='post',
                            json=result_data
                        )
                        result_json = result_response.json()
                        
                        if result_json.get('errorId') == 0 and result_json.get('status') == 'ready':
                            self.logger.info("hCaptcha solved successfully")
                            return result_json.get('solution', {}).get('gRecaptchaResponse')
                    
                    self.logger.error("Timeout waiting for hCaptcha solution")
                    return None
                else:
                    self.logger.error(f"Error submitting hCaptcha: {response_json.get('errorDescription')}")
                    return None
                    
            except Exception as e:
                self.error_handler.handle_error(e, "Error solving hCaptcha with Anti-Captcha")
                return None
        
        else:
            self.logger.error(f"Unsupported CAPTCHA solving service: {self.service}")
            return None
    
    def _solve_image_captcha(self, captcha_data):
        """Solve image CAPTCHA using a CAPTCHA solving service.
        
        Args:
            captcha_data: Dictionary with image_url
            
        Returns:
            Image CAPTCHA solution or None if solving failed
        """
        image_url = captcha_data.get('image_url')
        
        if not image_url:
            self.logger.error("Missing image URL for image CAPTCHA solving")
            return None
        
        try:
            # Download the image
            if image_url.startswith('data:'):
                # Handle data URLs
                image_data = image_url.split(',')[1]
                image_content = base64.b64decode(image_data)
            else:
                # Handle regular URLs
                image_response = safe_request(image_url)
                image_content = image_response.content
            
            # Encode the image as base64
            image_base64 = base64.b64encode(image_content).decode('utf-8')
            
            if self.service == '2captcha':
                # Submit the image CAPTCHA to 2captcha
                submit_url = f"{self.service_endpoints['2captcha']['submit']}?key={self.api_key}&method=base64&body={image_base64}&json=1"
                response = safe_request(submit_url)
                response_json = response.json()
                
                if response_json.get('status') == 1:
                    request_id = response_json.get('request')
                    
                    # Wait for the solution
                    self.logger.info(f"Waiting for image CAPTCHA solution (request ID: {request_id})")
                    
                    # Poll for the result
                    for _ in range(30):  # Try for 5 minutes (30 * 10 seconds)
                        time.sleep(10)  # Wait 10 seconds between polls
                        
                        result_url = f"{self.service_endpoints['2captcha']['retrieve']}?key={self.api_key}&action=get&id={request_id}&json=1"
                        result_response = safe_request(result_url)
                        result_json = result_response.json()
                        
                        if result_json.get('status') == 1:
                            self.logger.info("Image CAPTCHA solved successfully")
                            return result_json.get('request')
                    
                    self.logger.error("Timeout waiting for image CAPTCHA solution")
                    return None
                else:
                    self.logger.error(f"Error submitting image CAPTCHA: {response_json.get('error_text')}")
                    return None
            
            elif self.service == 'anti-captcha':
                # Submit the image CAPTCHA to Anti-Captcha
                submit_data = {
                    "clientKey": self.api_key,
                    "task": {
                        "type": "ImageToTextTask",
                        "body": image_base64,
                        "phrase": False,
                        "case": False,
                        "numeric": 0,
                        "math": False,
                        "minLength": 0,
                        "maxLength": 0
                    }
                }
                
                response = safe_request(
                    self.service_endpoints['anti-captcha']['submit'],
                    method='post',
                    json=submit_data
                )
                response_json = response.json()
                
                if response_json.get('errorId') == 0:
                    task_id = response_json.get('taskId')
                    
                    # Wait for the solution
                    self.logger.info(f"Waiting for image CAPTCHA solution (task ID: {task_id})")
                    
                    # Poll for the result
                    for _ in range(30):  # Try for 5 minutes (30 * 10 seconds)
                        time.sleep(10)  # Wait 10 seconds between polls
                        
                        result_data = {
                            "clientKey": self.api_key,
                            "taskId": task_id
                        }
                        
                        result_response = safe_request(
                            self.service_endpoints['anti-captcha']['retrieve'],
                            method='post',
                            json=result_data
                        )
                        result_json = result_response.json()
                        
                        if result_json.get('errorId') == 0 and result_json.get('status') == 'ready':
                            self.logger.info("Image CAPTCHA solved successfully")
                            return result_json.get('solution', {}).get('text')
                    
                    self.logger.error("Timeout waiting for image CAPTCHA solution")
                    return None
                else:
                    self.logger.error(f"Error submitting image CAPTCHA: {response_json.get('errorDescription')}")
                    return None
            
            else:
                self.logger.error(f"Unsupported CAPTCHA solving service: {self.service}")
                return None
                
        except Exception as e:
            self.error_handler.handle_error(e, "Error solving image CAPTCHA")
            return None
