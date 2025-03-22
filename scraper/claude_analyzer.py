import os
import re
import yaml
import requests
import logging
import json
import time
from bs4 import BeautifulSoup
from typing import List, Dict, Tuple
from urllib.parse import urljoin

# Define prompt templates directly in this file to avoid import issues
MANUFACTURER_DETECTION_PROMPT = """
URL: {url}
Title: {title}

Content: {truncated_content}

Question: Is this webpage about a manufacturer or does it contain a list of manufacturers or brands?

Respond using this exact format without any explanations:

# RESPONSE_START
IS_MANUFACTURER: yes  # Replace with 'no' if not a manufacturer page
# RESPONSE_END
"""

# New prompt template for page type analysis
PAGE_TYPE_ANALYSIS_PROMPT = """
URL: {url}

Content: {content}

Question: Analyze this webpage and determine if it is one of these types:
1. Brand page (main page about a manufacturer/brand with their products/categories)
2. Brand category page (page dedicated to a specific product category from a brand)
3. Inconclusive (not enough information to make a determination)
4. Other (not related to brands/manufacturers or their product categories)

Respond using this exact format without any explanations:

# RESPONSE_START
PAGE_TYPE: brand_page  # Replace with 'brand_category_page', 'inconclusive', or 'other' as appropriate
# RESPONSE_END
"""

# New prompt template for category page info extraction
CATEGORY_PAGE_INFO_PROMPT = """
URL: {url}
Title: {title}

Content: {content}

Question: Extract the manufacturer name and product category from this webpage.

Respond using this exact format:

# RESPONSE_START
MANUFACTURER: [manufacturer name]
CATEGORY: [category name]
WEBSITE: [official website if found, or empty]
# RESPONSE_END
"""

# System prompts
MANUFACTURER_SYSTEM_PROMPT = """You are a specialized AI trained to analyze web content and identify manufacturers and their product categories.
Your task is to analyze structured navigation data from web pages and identify manufacturers/brands ONLY."""

CATEGORY_SYSTEM_PROMPT = """You are a specialized AI trained to analyze web content and identify product categories for a specific manufacturer.
Your task is to analyze structured navigation data from web pages and identify product categories belonging to the manufacturer."""

PAGE_TYPE_SYSTEM_PROMPT = """You are a specialized AI trained to analyze web content and classify pages related to manufacturers and their products.
Your task is to determine if a page is:
1. A brand page (main page about a manufacturer/brand)
2. A brand category page (page for a specific product category from a brand)
3. Inconclusive (not enough information to make a determination)
4. Other (not related to brands/manufacturers)

Be conservative in your classification - only classify as brand or category page if you're confident."""

# General system prompt used for other tasks
SYSTEM_PROMPT = """You are Claude, an AI assistant specialized in analyzing web content to extract structured information about manufacturers and their product categories.

Your task is to carefully analyze the provided content and extract the requested information following the exact format specified in each instruction."""

# Import other less essential prompt templates from the original file
try:
    from scraper.prompt_templates import (
        MANUFACTURER_EXTRACTION_PROMPT, CATEGORY_EXTRACTION_PROMPT, 
        CATEGORY_VALIDATION_PROMPT, TRANSLATION_PROMPT, TRANSLATION_BATCH_PROMPT, TRANSLATION_SYSTEM_PROMPT
    )
except ImportError:
    # Fallback minimal templates if imports fail
    MANUFACTURER_EXTRACTION_PROMPT = "Extract manufacturers from this content: {content}"
    CATEGORY_EXTRACTION_PROMPT = "Extract categories for {manufacturer_name} from this content: {content}"
    CATEGORY_VALIDATION_PROMPT = "Validate these categories for {manufacturer}: {categories}"
    TRANSLATION_PROMPT = "Translate {category} to {target_language}"
    TRANSLATION_BATCH_PROMPT = "Translate these categories to {target_language}:\n{categories_line_by_line}"
    TRANSLATION_SYSTEM_PROMPT = "You are a specialized translator for product categories."

class ClaudeAnalyzer:
    """A class to handle Claude API interactions for analyzing web content"""
    
    def __init__(self, api_key, model, sonnet_model, logger=None):
        """Initialize the Claude analyzer"""
        self.api_key = api_key
        self.model = model
        self.sonnet_model = sonnet_model
        # Fallback models for rate limiting scenarios
        self.fallback_models = [
            'claude-3-5-haiku-20241022',  # Primary model
            'claude-3-haiku-20240307',    # Secondary model
            'claude-3-5-sonnet-20241022'  # Tertiary model
        ]
        self.current_model_index = 0
        
        # Exponential backoff parameters
        self.base_wait_time = 2  # Base wait time in seconds
        self.max_retries = 5     # Maximum number of retries
        self.max_wait_time = 60  # Maximum wait time in seconds
        
        self.logger = logger or logging.getLogger('claude_analyzer')
        self.category_count = 0  # Track categories found in initial pass
        
        # API call tracking for rate limiting
        self.api_calls = 0
        self.last_model_switch_time = time.time()

    def is_manufacturer_page(self, url, title, content):
        """
        Check if the page is a manufacturer page using Claude AI with exponential backoff and model fallback.
        
        This method analyzes the page content to determine if it's about manufacturers or contains
        manufacturer information. It sends a structured prompt to the Claude API and parses the response.
        
        Process flow:
        1. Truncate content to fit API limits (2000 chars)
        2. Format prompt with page details using MANUFACTURER_DETECTION_PROMPT template
        3. Make API request with exponential backoff and model fallback
        4. Parse response to determine if it's a manufacturer page
        
        Args:
            url: The URL of the page being analyzed
            title: The title of the page
            content: The text content extracted from the page
            
        Returns:
            Boolean indicating if the page is about manufacturers
        """
        try:
            self.logger.info(f"MANUFACTURER CHECK: Analyzing if {url} is a manufacturer page")
            
            # Prepare a truncated version of the content
            original_length = len(content)
            truncated_content = content[:2000] + "..." if original_length > 2000 else content
            
            if original_length > 2000:
                self.logger.info(f"Content truncated from {original_length} to 2000 characters for API limits")
            
            # Format the prompt with the page details
            self.logger.info("Formatting manufacturer detection prompt with page URL, title, and content")
            prompt = MANUFACTURER_DETECTION_PROMPT.format(
                url=url,
                title=title,
                truncated_content=truncated_content
            )
            
            # Prepare headers for API call
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            self.logger.info("Preparing to send manufacturer detection request to Claude API")
            
            # Implement exponential backoff with model fallback
            retry_count = 0
            wait_time = self.base_wait_time
            success = False
            response = None
            
            while not success and retry_count < self.max_retries:
                # Select current model based on fallback status
                current_model = self.fallback_models[self.current_model_index]
                
                # Prepare payload with current model
                payload = {
                    "model": current_model,
                    "max_tokens": 50,
                    "system": MANUFACTURER_SYSTEM_PROMPT,
                    "messages": [
                        {"role": "user", "content": prompt}
                    ]
                }
                
                # Track API calls
                self.api_calls += 1
                
                # Make API request
                self.logger.info(f"Making Claude API call with model {current_model} (attempt {retry_count+1}/{self.max_retries})")
                try:
                    response = requests.post(
                        "https://api.anthropic.com/v1/messages",
                        headers=headers,
                        json=payload,
                        timeout=30  # Add timeout to prevent hanging
                    )
                    
                    # Check for rate limiting or other errors
                    if response.status_code == 200:
                        # Success!
                        success = True
                    elif response.status_code == 429 or response.status_code >= 500:
                        # Rate limit or server error - implement backoff and model fallback
                        error_msg = f"API rate limit or server error: {response.status_code} - {response.text}"
                        self.logger.warning(error_msg)
                        
                        # Try fallback model if available
                        if self.current_model_index < len(self.fallback_models) - 1:
                            self.current_model_index += 1
                            self.logger.info(f"Switching to fallback model: {self.fallback_models[self.current_model_index]}")
                            # Reset wait time when switching models
                            wait_time = self.base_wait_time
                        else:
                            # Wait with exponential backoff
                            self.logger.info(f"Waiting {wait_time} seconds before retry")
                            time.sleep(wait_time)
                            # Increase wait time exponentially, but cap at max_wait_time
                            wait_time = min(wait_time * 2, self.max_wait_time)
                    else:
                        # Other error - log and retry
                        self.logger.error(f"API error: {response.status_code} - {response.text}")
                        time.sleep(wait_time)
                        wait_time = min(wait_time * 2, self.max_wait_time)
                        
                except (requests.RequestException, ConnectionError, TimeoutError) as req_err:
                    # Network error - retry with backoff
                    self.logger.error(f"Request error: {str(req_err)}")
                    time.sleep(wait_time)
                    wait_time = min(wait_time * 2, self.max_wait_time)
                    
                retry_count += 1
                
            # If we've exhausted all retries and still failed, return False
            if not success:
                self.logger.error(f"Failed to get response from Claude API after {self.max_retries} attempts")
                return False
                
            # Periodically try to switch back to the primary model
            current_time = time.time()
            if self.current_model_index > 0 and (current_time - self.last_model_switch_time) > 3600:  # Try every hour
                self.current_model_index = 0
                self.last_model_switch_time = current_time
                self.logger.info(f"Attempting to switch back to primary model: {self.fallback_models[0]}")

            
            # STEP 4: Process the successful response from Claude API
            self.logger.info("STEP 4: Processing Claude API response for manufacturer detection")
            response_data = response.json()
            answer = response_data["content"][0]["text"].strip()
            
            # Log a preview of the response for debugging
            answer_preview = answer[:100] + '...' if len(answer) > 100 else answer
            self.logger.info(f"Claude API response preview: {answer_preview}")
            
            # STEP 5: Parse the response to extract the manufacturer detection result
            self.logger.info("STEP 5: Parsing response to determine if it's a manufacturer page")
            
            # The response should follow the delimited format specified in the prompt template:
            # # RESPONSE_START
            # IS_MANUFACTURER: yes/no
            # # RESPONSE_END
            
            # Extract content between RESPONSE_START and RESPONSE_END markers
            response_pattern = r'# RESPONSE_START\s+(.*?)\s*# RESPONSE_END'
            response_match = re.search(response_pattern, answer, re.DOTALL)
            
            if response_match:
                # Successfully found the delimited response
                content = response_match.group(1).strip()
                self.logger.info(f"Found delimited response: {content}")
                
                # Extract the IS_MANUFACTURER field (yes/no)
                is_mfr_match = re.search(r'IS_MANUFACTURER:\s*(yes|no)', content, re.IGNORECASE)
                if is_mfr_match:
                    # Successfully found the IS_MANUFACTURER field
                    result_text = is_mfr_match.group(1).strip().lower()
                    result = result_text == 'yes'
                    
                    # Log the final determination
                    if result:
                        self.logger.info(f"✅ {url} IS identified as a manufacturer page")
                    else:
                        self.logger.info(f"❌ {url} is NOT a manufacturer page")
                    
                    return result
                else:
                    # Missing the IS_MANUFACTURER field
                    self.logger.error(f"Error: Missing IS_MANUFACTURER field in response: {content}")
                    self.logger.info(f"❌ {url} defaulting to NOT a manufacturer page due to parsing error")
                    return False
            else:
                # Missing the delimited response format
                self.logger.error(f"Error: Response missing RESPONSE_START/END markers: {answer}")
                self.logger.info(f"❌ {url} defaulting to NOT a manufacturer page due to formatting error")
                return False
            
        except Exception as e:
            self.logger.error(f"Error analyzing with Claude Haiku: {str(e)}")
            return False
    
    def _extract_structured_content(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract content from important HTML structures"""
        structured_content = []
        
        # Extract navigation menus (often contain category hierarchies)
        nav_elements = soup.find_all(['nav', 'ul', 'ol'])
        for nav in nav_elements:
            if len(nav.get_text(strip=True)) > 50:  # Only consider substantial nav elements
                structured_content.append({
                    'type': 'navigation',
                    'content': nav.get_text(' ', strip=True)
                })
        
        # Extract product listing sections
        product_sections = soup.find_all(['div', 'section'], class_=lambda x: x and any(term in str(x).lower() for term in ['product', 'category', 'catalog']))
        for section in product_sections:
            structured_content.append({
                'type': 'product_section',
                'content': section.get_text(' ', strip=True)
            })
        
        # Extract breadcrumb navigation
        breadcrumbs = soup.find_all(['nav', 'div'], class_=lambda x: x and 'breadcrumb' in str(x).lower())
        for crumb in breadcrumbs:
            structured_content.append({
                'type': 'breadcrumb',
                'content': crumb.get_text(' > ', strip=True)
            })
        
        return structured_content

    def _extract_prioritized_content(self, soup: BeautifulSoup, max_chars: int = 3000) -> Tuple[str, List[str]]:
        """
        Extract content from HTML, prioritizing structured data up to max_chars.
        Returns tuple of (prioritized_content, remaining_chunks)
        """
        content_parts = []
        remaining_chunks = []
        current_length = 0
        
        # Priority 1: Navigation menus and category lists
        nav_elements = soup.find_all(['nav', 'ul', 'ol'], 
                                   class_=lambda x: x and any(term in str(x).lower() 
                                   for term in ['category', 'product', 'menu', 'nav']))
        for nav in nav_elements:
            text = nav.get_text(' ', strip=True)
            if len(text) > 50:  # Only substantial content
                if current_length + len(text) <= max_chars:
                    content_parts.append(text)
                    current_length += len(text)
                else:
                    remaining_chunks.append(text)
        
        # Priority 2: Breadcrumbs and structured hierarchies
        breadcrumbs = soup.find_all(['nav', 'div'], 
                                  class_=lambda x: x and 'breadcrumb' in str(x).lower())
        for crumb in breadcrumbs:
            text = crumb.get_text(' > ', strip=True)
            if current_length + len(text) <= max_chars:
                content_parts.append(text)
                current_length += len(text)
            else:
                remaining_chunks.append(text)
        
        # Priority 3: Product sections and category grids
        product_sections = soup.find_all(['div', 'section'], 
                                       class_=lambda x: x and any(term in str(x).lower() 
                                       for term in ['product', 'category', 'catalog']))
        for section in product_sections:
            text = section.get_text(' ', strip=True)
            if len(text) > 50:
                if current_length + len(text) <= max_chars:
                    content_parts.append(text)
                    current_length += len(text)
                else:
                    remaining_chunks.append(text)
        
        # Priority 4: Tables with relevant headers
        tables = soup.find_all('table')
        for table in tables:
            headers = table.find_all('th')
            if any(term in str(headers).lower() for term in ['product', 'category', 'brand', 'manufacturer']):
                text = table.get_text(' ', strip=True)
                if current_length + len(text) <= max_chars:
                    content_parts.append(text)
                    current_length += len(text)
                else:
                    remaining_chunks.append(text)
        
        # If we still have room, add any remaining relevant text
        if current_length < max_chars:
            main_content = soup.find('main') or soup.find('article') or soup.find('div', class_='content')
            if main_content:
                text = main_content.get_text(' ', strip=True)
                if current_length + len(text) <= max_chars:
                    content_parts.append(text)
                else:
                    # Only take what we can fit
                    space_left = max_chars - current_length
                    content_parts.append(text[:space_left])
                    remaining_chunks.append(text[space_left:])
        
        return ' '.join(content_parts), remaining_chunks

    def _extract_manufacturers(self, url: str, content: str) -> List[Dict]:
        """
        Extract manufacturer information from structured content with exponential backoff and model fallback.
        
        This method processes the page content to extract detailed manufacturer information using Claude AI.
        It implements a resilient approach with exponential backoff and model fallback to handle API limits.
        
        Process flow:
        1. Format prompt with page content using MANUFACTURER_EXTRACTION_PROMPT template
        2. Make API request with exponential backoff and model fallback
        3. Parse the response to extract structured manufacturer data
        4. Validate and format the extracted data
        
        Args:
            url: The URL of the page being analyzed
            content: The structured content extracted from the page
            
        Returns:
            List of dictionaries containing manufacturer information
        """
        try:
            self.logger.info(f"MANUFACTURER EXTRACTION: Starting extraction from {url}")
            
            # STEP 1: Format the prompt with the structured content
            self.logger.info("STEP 1: Formatting manufacturer extraction prompt")
            content_preview = content[:100] + '...' if len(content) > 100 else content
            self.logger.info(f"Content preview for extraction: {content_preview}")
            prompt = MANUFACTURER_EXTRACTION_PROMPT.format(content=content)
            
            # Prepare headers for API call
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            self.logger.info("STEP 2: Preparing to send manufacturer extraction request to Claude API")
            
            # Implement exponential backoff with model fallback
            retry_count = 0
            wait_time = self.base_wait_time
            success = False
            response = None
            
            while not success and retry_count < self.max_retries:
                # Select current model based on fallback status
                current_model = self.fallback_models[self.current_model_index]
                
                # Prepare payload with current model
                payload = {
                    "model": current_model,
                    "max_tokens": 4000,
                    "system": MANUFACTURER_SYSTEM_PROMPT,
                    "messages": [
                        {"role": "user", "content": prompt}
                    ]
                }
                
                # Track API calls
                self.api_calls += 1
                
                # Make API request
                self.logger.info(f"Making Claude API call with model {current_model} for manufacturer extraction (attempt {retry_count+1}/{self.max_retries})")
                try:
                    response = requests.post(
                        "https://api.anthropic.com/v1/messages",
                        headers=headers,
                        json=payload,
                        timeout=30  # Add timeout to prevent hanging
                    )
                    
                    # Check for rate limiting or other errors
                    if response.status_code == 200:
                        # Success!
                        success = True
                    elif response.status_code == 429 or response.status_code >= 500:
                        # Rate limit or server error - implement backoff and model fallback
                        error_msg = f"API rate limit or server error: {response.status_code} - {response.text}"
                        self.logger.warning(error_msg)
                        
                        # Try fallback model if available
                        if self.current_model_index < len(self.fallback_models) - 1:
                            self.current_model_index += 1
                            self.logger.info(f"Switching to fallback model: {self.fallback_models[self.current_model_index]}")
                            # Reset wait time when switching models
                            wait_time = self.base_wait_time
                        else:
                            # Wait with exponential backoff
                            self.logger.info(f"Waiting {wait_time} seconds before retry")
                            time.sleep(wait_time)
                            # Increase wait time exponentially, but cap at max_wait_time
                            wait_time = min(wait_time * 2, self.max_wait_time)
                    else:
                        # Other error - log and retry
                        self.logger.error(f"API error: {response.status_code} - {response.text}")
                        time.sleep(wait_time)
                        wait_time = min(wait_time * 2, self.max_wait_time)
                        
                except (requests.RequestException, ConnectionError, TimeoutError) as req_err:
                    # Network error - retry with backoff
                    self.logger.error(f"Request error: {str(req_err)}")
                    time.sleep(wait_time)
                    wait_time = min(wait_time * 2, self.max_wait_time)
                    
                retry_count += 1
                
            # If we've exhausted all retries and still failed, return empty list
            if not success:
                self.logger.error(f"Failed to get response from Claude API after {self.max_retries} attempts")
                return []
                
            # STEP 3: Process successful response from Claude API
            self.logger.info("STEP 3: Processing Claude API response for manufacturer extraction")
            response_data = response.json()
            result = response_data["content"][0]["text"]
            
            # Log a preview of the response for debugging
            result_preview = result[:150] + '...' if len(result) > 150 else result
            self.logger.info(f"Claude API response preview: {result_preview}")
            
            # Periodically try to switch back to the primary model
            current_time = time.time()
            if self.current_model_index > 0 and (current_time - self.last_model_switch_time) > 3600:  # Try every hour
                self.current_model_index = 0
                self.last_model_switch_time = current_time
                self.logger.info(f"Attempting to switch back to primary model: {self.fallback_models[0]}")
            
            # STEP 4: Parse the delimited response to extract structured manufacturer data
            self.logger.info("STEP 4: Parsing response to extract manufacturer information")
            self.logger.info("Looking for properly formatted JSON data between RESPONSE_START and RESPONSE_END markers")
            parsed_data = self._parse_manufacturer_response(result)
            
            # STEP 5: Validate and return the extracted manufacturer data
            if parsed_data and parsed_data.get('manufacturers'):
                manufacturer_count = len(parsed_data['manufacturers'])
                self.logger.info(f"✅ Successfully extracted {manufacturer_count} manufacturers from {url}")
                
                # Log a summary of each manufacturer found
                for i, manufacturer in enumerate(parsed_data['manufacturers']):
                    name = manufacturer.get('name', 'Unknown')
                    categories_count = len(manufacturer.get('categories', []))
                    self.logger.info(f"  Manufacturer #{i+1}: {name} with {categories_count} categories")
                    
                return parsed_data['manufacturers']
            else:
                self.logger.info(f"❌ No manufacturers found in {url} or response format invalid")
                return []
                
        except Exception as e:
            self.logger.error(f"Error extracting manufacturers: {str(e)}")
            return []

    def _extract_categories(self, url: str, manufacturer_name: str, content: str) -> List[str]:
        """Extract category information for a specific manufacturer with exponential backoff and model fallback"""
        try:
            # Format the prompt with the structured content
            prompt = CATEGORY_EXTRACTION_PROMPT.format(
                content=content,
                manufacturer_name=manufacturer_name
            )
            
            # Prepare headers for API call
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            # Implement exponential backoff with model fallback
            retry_count = 0
            wait_time = self.base_wait_time
            success = False
            response = None
            
            while not success and retry_count < self.max_retries:
                # Select current model based on fallback status
                current_model = self.fallback_models[self.current_model_index]
                
                # Prepare payload with current model
                payload = {
                    "model": current_model,
                    "max_tokens": 4000,
                    "system": CATEGORY_SYSTEM_PROMPT,
                    "messages": [
                        {"role": "user", "content": prompt}
                    ]
                }
                
                # Track API calls
                self.api_calls += 1
                
                # Make API request
                self.logger.info(f"Making Claude API call with model {current_model} for category extraction (attempt {retry_count+1}/{self.max_retries})")
                try:
                    response = requests.post(
                        "https://api.anthropic.com/v1/messages",
                        headers=headers,
                        json=payload,
                        timeout=30  # Add timeout to prevent hanging
                    )
                    
                    # Check for rate limiting or other errors
                    if response.status_code == 200:
                        # Success!
                        success = True
                    elif response.status_code == 429 or response.status_code >= 500:
                        # Rate limit or server error - implement backoff and model fallback
                        error_msg = f"API rate limit or server error: {response.status_code} - {response.text}"
                        self.logger.warning(error_msg)
                        
                        # Try fallback model if available
                        if self.current_model_index < len(self.fallback_models) - 1:
                            self.current_model_index += 1
                            self.logger.info(f"Switching to fallback model: {self.fallback_models[self.current_model_index]}")
                            # Reset wait time when switching models
                            wait_time = self.base_wait_time
                        else:
                            # Wait with exponential backoff
                            self.logger.info(f"Waiting {wait_time} seconds before retry")
                            time.sleep(wait_time)
                            # Increase wait time exponentially, but cap at max_wait_time
                            wait_time = min(wait_time * 2, self.max_wait_time)
                    else:
                        # Other error - log and retry
                        self.logger.error(f"API error: {response.status_code} - {response.text}")
                        time.sleep(wait_time)
                        wait_time = min(wait_time * 2, self.max_wait_time)
                        
                except (requests.RequestException, ConnectionError, TimeoutError) as req_err:
                    # Network error - retry with backoff
                    self.logger.error(f"Request error: {str(req_err)}")
                    time.sleep(wait_time)
                    wait_time = min(wait_time * 2, self.max_wait_time)
                    
                retry_count += 1
                
            # If we've exhausted all retries and still failed, return empty list
            if not success:
                self.logger.error(f"Failed to get response from Claude API after {self.max_retries} attempts")
                return []
                
            # Process successful response
            response_data = response.json()
            result = response_data["content"][0]["text"]
            
            # Periodically try to switch back to the primary model
            current_time = time.time()
            if self.current_model_index > 0 and (current_time - self.last_model_switch_time) > 3600:  # Try every hour
                self.current_model_index = 0
                self.last_model_switch_time = current_time
                self.logger.info(f"Attempting to switch back to primary model: {self.fallback_models[0]}")
            
            # Parse the delimited response
            categories = self._parse_category_response(result)
            
            if categories:
                self.logger.info(f"Found {len(categories)} raw categories for {manufacturer_name} in {url}")
                
                # Validate the extracted categories to ensure they are legitimate product categories
                validated_categories = self.validate_categories(categories, manufacturer_name)
                
                if validated_categories:
                    self.logger.info(f"Validated {len(validated_categories)} categories for {manufacturer_name} in {url}")
                    return validated_categories
                else:
                    self.logger.info(f"No valid categories found after validation for {manufacturer_name} in {url}")
                    return []
            else:
                self.logger.info(f"No categories found for {manufacturer_name} in {url}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error extracting categories: {str(e)}")
            return []

    def _parse_manufacturer_response(self, response_text: str) -> Dict:
        """Parse manufacturer response using strict delimited format"""
        try:
            # Initialize result structure
            manufacturers = []

            # Find content between RESPONSE_START and RESPONSE_END
            match = re.search(r'# RESPONSE_START\s*(.*?)\s*# RESPONSE_END', response_text, re.DOTALL)
            if not match:
                self.logger.warning("No valid response delimiters found")
                return {"manufacturers": []}

            content = match.group(1).strip()
            if not content:
                return {"manufacturers": []}

            # Split into manufacturer blocks
            manufacturer_blocks = re.finditer(r'# MANUFACTURER_START\s*(.*?)\s*# MANUFACTURER_END', content, re.DOTALL)
            
            for block in manufacturer_blocks:
                manufacturer_data = block.group(1).strip()
                if not manufacturer_data:
                    continue

                # Initialize manufacturer dict
                current_manufacturer = {
                    "name": "",
                    "urls": []
                }

                # Parse manufacturer data
                for line in manufacturer_data.split('\n'):
                    line = line.strip()
                    if line.startswith('NAME:'):
                        current_manufacturer["name"] = line.replace('NAME:', '').strip()
                    elif line.startswith('URL:'):
                        url = line.replace('URL:', '').strip()
                        if url:
                            current_manufacturer["urls"].append(url)

                # Add manufacturer if we have a name
                if current_manufacturer["name"]:
                    manufacturers.append(current_manufacturer)

            return {"manufacturers": manufacturers}

        except Exception as e:
            self.logger.error(f"Error parsing manufacturer response: {str(e)}")
            return {"manufacturers": []}
            
    def _parse_category_response(self, response_text: str) -> List[str]:
        """Parse category response using delimited format"""
        try:
            categories = []

            # Find content between RESPONSE_START and RESPONSE_END
            match = re.search(r'# RESPONSE_START\s*(.*?)\s*# RESPONSE_END', response_text, re.DOTALL)
            if not match:
                self.logger.warning("No valid category response delimiters found")
                return []

            content = match.group(1).strip()
            if not content:
                return []

            # Extract all categories
            for line in content.split('\n'):
                line = line.strip()
                if line.startswith('CATEGORY:'):
                    category = line.replace('CATEGORY:', '').strip()
                    if category:
                        categories.append(category)

            return categories

        except Exception as e:
            self.logger.error(f"Error parsing category response: {str(e)}")
            return []

    def extract_manufacturers(self, url: str, title: str, content: str, html_content: str = None) -> Dict:
        """Extract manufacturer and category information from structured navigation elements"""
        try:
            if not html_content:
                self.logger.warning(f"No HTML content provided for {url}, falling back to text analysis")
                return {"manufacturers": []}

            soup = BeautifulSoup(html_content, 'html.parser')
            structured_content = []
            
            # 1. Extract navigation menus
            for nav in soup.find_all('nav'):
                links = nav.find_all('a')
                if links:
                    link_data = []
                    for link in links:
                        if link.get_text(strip=True):
                            href = urljoin(url, link.get('href', '')) if link.get('href') else ''
                            link_data.append(f"{link.get_text(strip=True)} [{href}]")
                    if link_data:
                        structured_content.append(f"Navigation Menu Links:\n" + "\n".join(link_data))

            # 2. Extract sidebar/menu lists
            for div in soup.find_all(['div', 'aside'], class_=lambda x: x and any(term in str(x).lower() 
                                   for term in ['sidebar', 'menu', 'navigation'])):
                for list_elem in div.find_all(['ul', 'ol']):
                    links = list_elem.find_all('a')
                    if links:
                        link_data = []
                        for link in links:
                            if link.get_text(strip=True):
                                href = urljoin(url, link.get('href', '')) if link.get('href') else ''
                                link_data.append(f"{link.get_text(strip=True)} [{href}]")
                        if link_data:
                            structured_content.append(f"Menu List Links:\n" + "\n".join(link_data))

            # 3. Extract category/product listings
            for section in soup.find_all(['div', 'section'], class_=lambda x: x and any(term in str(x).lower() 
                                       for term in ['category', 'product', 'catalog', 'listing'])):
                links = section.find_all('a')
                if links:
                    link_data = []
                    for link in links:
                        if link.get_text(strip=True):
                            href = urljoin(url, link.get('href', '')) if link.get('href') else ''
                            link_data.append(f"{link.get_text(strip=True)} [{href}]")
                    if link_data:
                        structured_content.append(f"Category/Product Links:\n" + "\n".join(link_data))

            # 4. Extract breadcrumb navigation
            for crumb in soup.find_all(['nav', 'div', 'ol'], class_=lambda x: x and 'breadcrumb' in str(x).lower()):
                links = crumb.find_all('a')
                if links:
                    link_data = []
                    for link in links:
                        if link.get_text(strip=True):
                            href = urljoin(url, link.get('href', '')) if link.get('href') else ''
                            link_data.append(f"{link.get_text(strip=True)} [{href}]")
                    if link_data:
                        structured_content.append(f"Breadcrumb Path:\n" + " > ".join(link_data))

            # 5. Extract links with manufacturer/brand-related classes or paths
            manufacturer_links = []
            # Class-based detection
            for link in soup.find_all('a', class_=lambda x: x and any(term in str(x).lower() 
                                    for term in ['brand', 'manufacturer', 'vendor'])):
                if link.get_text(strip=True):
                    href = urljoin(url, link.get('href', '')) if link.get('href') else ''
                    manufacturer_links.append(f"{link.get_text(strip=True)} [{href}]")
            
            # URL path-based detection
            for link in soup.find_all('a', href=lambda x: x and any(term in str(x).lower() 
                                    for term in ['/brand/', '/manufacturer/', '/vendor/'])):
                if link.get_text(strip=True):
                    href = urljoin(url, link.get('href', '')) if link.get('href') else ''
                    manufacturer_links.append(f"{link.get_text(strip=True)} [{href}]")
            
            if manufacturer_links:
                structured_content.append(f"Manufacturer/Brand Links:\n" + "\n".join(manufacturer_links))

            # 6. Extract page title and main headings
            if soup.title:
                structured_content.insert(0, f"Page Title: {soup.title.string.strip()}")
            
            headings = []
            for h in soup.find_all(['h1', 'h2']):
                # Check if heading contains a link
                heading_link = h.find('a')
                if heading_link and heading_link.get('href'):
                    href = urljoin(url, heading_link.get('href'))
                    headings.append(f"{h.get_text(strip=True)} [{href}]")
                else:
                    headings.append(h.get_text(strip=True))
            
            if headings:
                structured_content.insert(1, f"Main Headings:\n" + "\n".join(headings))

            # Only proceed if we found structured content
            if not structured_content:
                self.logger.info(f"No structured content found in {url}")
                return {"manufacturers": []}

            # Add current URL for context
            structured_content.insert(0, f"Current Page: {url}")

            # Prepare the structured content for Claude
            structured_text = "\n\n".join(structured_content)
            self.logger.debug(f"Extracted structured content ({len(structured_text)} chars)")

            # Step 1: Extract manufacturers first
            manufacturers = self._extract_manufacturers(url, structured_text)
            if not manufacturers:
                self.logger.info(f"No manufacturers identified in {url}")
                return {"manufacturers": []}
            
            # Step 2: For each manufacturer, extract categories in a separate call
            result_manufacturers = []
            for manufacturer in manufacturers:
                manufacturer_name = manufacturer.get('name')
                if not manufacturer_name:
                    continue
                    
                # Extract categories for this manufacturer
                categories = self._extract_categories(url, manufacturer_name, structured_text)
                
                # Add to result with categories
                result_manufacturers.append({
                    "name": manufacturer_name,
                    "categories": categories,
                    "urls": manufacturer.get('urls', [])
                })
                
            # Return combined result
            if result_manufacturers:
                total_manufacturers = len(result_manufacturers)
                total_categories = sum(len(m.get('categories', [])) for m in result_manufacturers)
                self.logger.info(f"Found {total_manufacturers} manufacturers with {total_categories} categories in {url}")
                return {"manufacturers": result_manufacturers}

            self.logger.info(f"No valid manufacturers with categories identified in {url}")
            return {"manufacturers": []}
            
        except Exception as e:
            self.logger.error(f"Error extracting manufacturers from HTML: {str(e)}")
            return {"manufacturers": []}

    def _extract_manufacturers_initial(self, url: str, title: str, content: str) -> Dict:
        """Extract manufacturer and category information using Claude with exponential backoff and model fallback"""
        try:
            # Prepare a truncated version of the content to avoid token limits
            truncated_content = content[:4000] + "..." if len(content) > 4000 else content
            
            # Format the prompt with the page details
            prompt = MANUFACTURER_EXTRACTION_PROMPT.format(
                url=url,
                title=title,
                truncated_content=truncated_content
            )
            
            # Prepare headers for API call
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            # Implement exponential backoff with model fallback
            retry_count = 0
            wait_time = self.base_wait_time
            success = False
            response = None
            result = None
            
            while not success and retry_count < self.max_retries:
                # Select current model based on fallback status
                current_model = self.fallback_models[self.current_model_index]
                
                # Prepare payload with current model
                payload = {
                    "model": current_model,
                    "max_tokens": 4000,
                    "system": MANUFACTURER_SYSTEM_PROMPT,
                    "messages": [
                        {"role": "user", "content": prompt}
                    ]
                }
                
                # Track API calls
                self.api_calls += 1
                
                # Make API request
                self.logger.info(f"Making Claude API call with model {current_model} for initial manufacturer extraction (attempt {retry_count+1}/{self.max_retries})")
                try:
                    response = requests.post(
                        "https://api.anthropic.com/v1/messages",
                        headers=headers,
                        json=payload,
                        timeout=30  # Add timeout to prevent hanging
                    )
                    
                    # Check for rate limiting or other errors
                    if response.status_code == 200:
                        # Success!
                        success = True
                        response_data = response.json()
                        result = response_data["content"][0]["text"]
                    elif response.status_code == 429 or response.status_code >= 500:
                        # Rate limit or server error - implement backoff and model fallback
                        error_msg = f"API rate limit or server error: {response.status_code} - {response.text}"
                        self.logger.warning(error_msg)
                        
                        # Try fallback model if available
                        if self.current_model_index < len(self.fallback_models) - 1:
                            self.current_model_index += 1
                            self.logger.info(f"Switching to fallback model: {self.fallback_models[self.current_model_index]}")
                            # Reset wait time when switching models
                            wait_time = self.base_wait_time
                        else:
                            # Wait with exponential backoff
                            self.logger.info(f"Waiting {wait_time} seconds before retry")
                            time.sleep(wait_time)
                            # Increase wait time exponentially, but cap at max_wait_time
                            wait_time = min(wait_time * 2, self.max_wait_time)
                    else:
                        # Other error - log and retry
                        self.logger.error(f"API error: {response.status_code} - {response.text}")
                        time.sleep(wait_time)
                        wait_time = min(wait_time * 2, self.max_wait_time)
                        
                except (requests.RequestException, ConnectionError, TimeoutError) as req_err:
                    # Network error - retry with backoff
                    self.logger.error(f"Request error: {str(req_err)}")
                    time.sleep(wait_time)
                    wait_time = min(wait_time * 2, self.max_wait_time)
                    
                retry_count += 1
                
            # If we've exhausted all retries and still failed, return None
            if not success or not result:
                self.logger.error(f"Failed to get response from Claude API after {self.max_retries} attempts")
                return None
                
            # Periodically try to switch back to the primary model
            current_time = time.time()
            if self.current_model_index > 0 and (current_time - self.last_model_switch_time) > 3600:  # Try every hour
                self.current_model_index = 0
                self.last_model_switch_time = current_time
                self.logger.info(f"Attempting to switch back to primary model: {self.fallback_models[0]}")
            
            self.logger.info(f"Claude manufacturer extraction for {url}: Response received")
            
            # Parse the response to extract manufacturers and categories
            parsed_data = self._parse_delimited_response(result)
            if parsed_data:
                self.logger.info(f"Extracted {len(parsed_data.get('manufacturers', []))} manufacturers from {url}")
            else:
                self.logger.warning(f"No manufacturers extracted from {url}")
                
            return parsed_data
            
        except Exception as e:
            self.logger.error(f"Error analyzing with Claude Haiku extraction: {str(e)}")
            return None
    
    def analyze_page_type(self, url: str, content: str) -> Dict:
        """
        Analyze a page to determine if it's a brand page, brand category page, inconclusive, or other.
        
        This method implements the new page type classification system that categorizes pages into:
        1. Brand page - Main page about a manufacturer/brand with their products/categories
        2. Brand category page - Page dedicated to a specific product category from a brand
        3. Inconclusive - Not enough information to make a determination
        4. Other - Not related to brands/manufacturers or their product categories
        
        Args:
            url: The URL of the page being analyzed
            content: The content extracted from the page (can be metadata, navigation, or full content)
            
        Returns:
            Dictionary with the page type classification result
        """
        try:
            self.logger.info(f"PAGE TYPE ANALYSIS: Analyzing page type for {url}")
            
            # Prepare a truncated version of the content if needed
            original_length = len(content)
            truncated_content = content[:3000] + "..." if original_length > 3000 else content
            
            if original_length > 3000:
                self.logger.info(f"Content truncated from {original_length} to 3000 characters for API limits")
            
            # Format the prompt with the page details
            self.logger.info("Formatting page type analysis prompt with content")
            prompt = PAGE_TYPE_ANALYSIS_PROMPT.format(
                url=url,
                content=truncated_content
            )
            
            # Prepare headers for API call
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            self.logger.info("Preparing to send page type analysis request to Claude API")
            
            # Implement exponential backoff with model fallback
            retry_count = 0
            wait_time = self.base_wait_time
            success = False
            response = None
            
            while not success and retry_count < self.max_retries:
                # Select current model based on fallback status
                current_model = self.fallback_models[self.current_model_index]
                
                # Prepare payload with current model
                payload = {
                    "model": current_model,
                    "max_tokens": 100,
                    "system": PAGE_TYPE_SYSTEM_PROMPT,
                    "messages": [
                        {"role": "user", "content": prompt}
                    ]
                }
                
                # Track API calls
                self.api_calls += 1
                
                # Make API request
                self.logger.info(f"Making Claude API call with model {current_model} (attempt {retry_count+1}/{self.max_retries})")
                try:
                    response = requests.post(
                        "https://api.anthropic.com/v1/messages",
                        headers=headers,
                        json=payload,
                        timeout=30  # Add timeout to prevent hanging
                    )
                    
                    # Check for rate limiting or other errors
                    if response.status_code == 200:
                        # Success!
                        success = True
                    elif response.status_code == 429 or response.status_code >= 500:
                        # Rate limit or server error - implement backoff and model fallback
                        error_msg = f"API rate limit or server error: {response.status_code} - {response.text}"
                        self.logger.warning(error_msg)
                        
                        # Try fallback model if available
                        if self.current_model_index < len(self.fallback_models) - 1:
                            self.current_model_index += 1
                            self.logger.info(f"Switching to fallback model: {self.fallback_models[self.current_model_index]}")
                            # Reset wait time when switching models
                            wait_time = self.base_wait_time
                        else:
                            # Wait with exponential backoff
                            self.logger.info(f"Waiting {wait_time} seconds before retry")
                            time.sleep(wait_time)
                            # Increase wait time exponentially, but cap at max_wait_time
                            wait_time = min(wait_time * 2, self.max_wait_time)
                    else:
                        # Other error - log and retry
                        self.logger.error(f"API error: {response.status_code} - {response.text}")
                        time.sleep(wait_time)
                        wait_time = min(wait_time * 2, self.max_wait_time)
                        
                except (requests.RequestException, ConnectionError, TimeoutError) as req_err:
                    # Network error - retry with backoff
                    self.logger.error(f"Request error: {str(req_err)}")
                    time.sleep(wait_time)
                    wait_time = min(wait_time * 2, self.max_wait_time)
                    
                retry_count += 1
                
            # If we've exhausted all retries and still failed, return default
            if not success:
                self.logger.error(f"Failed to get response from Claude API after {self.max_retries} attempts")
                return {"page_type": "inconclusive"}
                
            # Periodically try to switch back to the primary model
            current_time = time.time()
            if self.current_model_index > 0 and (current_time - self.last_model_switch_time) > 3600:  # Try every hour
                self.current_model_index = 0
                self.last_model_switch_time = current_time
                self.logger.info(f"Attempting to switch back to primary model: {self.fallback_models[0]}")
            
            # Process the successful response from Claude API
            self.logger.info("Processing Claude API response for page type analysis")
            response_data = response.json()
            answer = response_data["content"][0]["text"].strip()
            
            # Log a preview of the response for debugging
            answer_preview = answer[:100] + '...' if len(answer) > 100 else answer
            self.logger.info(f"Claude API response preview: {answer_preview}")
            
            # Parse the response to extract the page type result
            self.logger.info("Parsing response to determine page type")
            
            # Extract content between RESPONSE_START and RESPONSE_END markers
            response_pattern = r'# RESPONSE_START\s+(.*?)\s*# RESPONSE_END'
            response_match = re.search(response_pattern, answer, re.DOTALL)
            
            if response_match:
                # Successfully found the delimited response
                content = response_match.group(1).strip()
                self.logger.info(f"Found delimited response: {content}")
                
                # Extract the PAGE_TYPE field - now including 'inconclusive' as an option
                page_type_match = re.search(r'PAGE_TYPE:\s*(brand_page|brand_category_page|inconclusive|other)', content, re.IGNORECASE)
                if page_type_match:
                    # Successfully found the PAGE_TYPE field
                    page_type = page_type_match.group(1).strip().lower()
                    
                    # Log the final determination
                    self.logger.info(f"Page type determined: {page_type}")
                    
                    return {"page_type": page_type}
                else:
                    # Missing the PAGE_TYPE field
                    self.logger.error(f"Error: Missing PAGE_TYPE field in response: {content}")
                    self.logger.info(f"Defaulting to 'inconclusive' due to parsing error")
                    return {"page_type": "inconclusive"}
            else:
                # Missing the delimited response format
                self.logger.error(f"Error: Response missing RESPONSE_START/END markers: {answer}")
                self.logger.info(f"Defaulting to 'inconclusive' due to formatting error")
                return {"page_type": "inconclusive"}
            
        except Exception as e:
            self.logger.error(f"Error in page type analysis: {str(e)}")
            return {"page_type": "inconclusive"}
    
    def extract_category_page_info(self, url: str, title: str, content: str) -> Dict:
        """
        Extract manufacturer name and category from a brand category page.
        
        This method is specifically designed for pages that have been classified as
        'brand_category_page' and extracts the essential information without the
        full manufacturer data extraction process.
        
        Args:
            url: The URL of the page being analyzed
            title: The title of the page
            content: The content extracted from the page
            
        Returns:
            Dictionary with manufacturer name and category
        """
        try:
            self.logger.info(f"CATEGORY PAGE INFO: Extracting info from {url}")
            
            # Prepare a truncated version of the content if needed
            original_length = len(content)
            truncated_content = content[:2000] + "..." if original_length > 2000 else content
            
            if original_length > 2000:
                self.logger.info(f"Content truncated from {original_length} to 2000 characters for API limits")
            
            # Format the prompt with the page details
            self.logger.info("Formatting category page info extraction prompt")
            prompt = CATEGORY_PAGE_INFO_PROMPT.format(
                url=url,
                title=title,
                content=truncated_content
            )
            
            # Prepare headers for API call
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            self.logger.info("Preparing to send category page info request to Claude API")
            
            # Implement exponential backoff with model fallback
            retry_count = 0
            wait_time = self.base_wait_time
            success = False
            response = None
            
            while not success and retry_count < self.max_retries:
                # Select current model based on fallback status
                current_model = self.fallback_models[self.current_model_index]
                
                # Prepare payload with current model
                payload = {
                    "model": current_model,
                    "max_tokens": 100,
                    "system": SYSTEM_PROMPT,
                    "messages": [
                        {"role": "user", "content": prompt}
                    ]
                }
                
                # Track API calls
                self.api_calls += 1
                
                # Make API request
                self.logger.info(f"Making Claude API call with model {current_model} (attempt {retry_count+1}/{self.max_retries})")
                try:
                    response = requests.post(
                        "https://api.anthropic.com/v1/messages",
                        headers=headers,
                        json=payload,
                        timeout=30  # Add timeout to prevent hanging
                    )
                    
                    # Check for rate limiting or other errors
                    if response.status_code == 200:
                        # Success!
                        success = True
                    elif response.status_code == 429 or response.status_code >= 500:
                        # Rate limit or server error - implement backoff and model fallback
                        error_msg = f"API rate limit or server error: {response.status_code} - {response.text}"
                        self.logger.warning(error_msg)
                        
                        # Try fallback model if available
                        if self.current_model_index < len(self.fallback_models) - 1:
                            self.current_model_index += 1
                            self.logger.info(f"Switching to fallback model: {self.fallback_models[self.current_model_index]}")
                            # Reset wait time when switching models
                            wait_time = self.base_wait_time
                        else:
                            # Wait with exponential backoff
                            self.logger.info(f"Waiting {wait_time} seconds before retry")
                            time.sleep(wait_time)
                            # Increase wait time exponentially, but cap at max_wait_time
                            wait_time = min(wait_time * 2, self.max_wait_time)
                    else:
                        # Other error - log and retry
                        self.logger.error(f"API error: {response.status_code} - {response.text}")
                        time.sleep(wait_time)
                        wait_time = min(wait_time * 2, self.max_wait_time)
                        
                except (requests.RequestException, ConnectionError, TimeoutError) as req_err:
                    # Network error - retry with backoff
                    self.logger.error(f"Request error: {str(req_err)}")
                    time.sleep(wait_time)
                    wait_time = min(wait_time * 2, self.max_wait_time)
                    
                retry_count += 1
                
            # If we've exhausted all retries and still failed, return empty result
            if not success:
                self.logger.error(f"Failed to get response from Claude API after {self.max_retries} attempts")
                return {}
                
            # Process the successful response from Claude API
            self.logger.info("Processing Claude API response for category page info")
            response_data = response.json()
            answer = response_data["content"][0]["text"].strip()
            
            # Log a preview of the response for debugging
            answer_preview = answer[:100] + '...' if len(answer) > 100 else answer
            self.logger.info(f"Claude API response preview: {answer_preview}")
            
            # Parse the response to extract the manufacturer and category
            self.logger.info("Parsing response to extract manufacturer and category")
            
            # Extract content between RESPONSE_START and RESPONSE_END markers
            response_pattern = r'# RESPONSE_START\s+(.*?)\s*# RESPONSE_END'
            response_match = re.search(response_pattern, answer, re.DOTALL)
            
            if response_match:
                # Successfully found the delimited response
                content = response_match.group(1).strip()
                self.logger.info(f"Found delimited response: {content}")
                
                # Extract the MANUFACTURER field
                manufacturer_match = re.search(r'MANUFACTURER:\s*(.+?)(?:\n|$)', content)
                category_match = re.search(r'CATEGORY:\s*(.+?)(?:\n|$)', content)
                website_match = re.search(r'WEBSITE:\s*(.+?)(?:\n|$)', content)
                
                result = {}
                
                if manufacturer_match:
                    manufacturer = manufacturer_match.group(1).strip()
                    if manufacturer and manufacturer != "[manufacturer name]":
                        result["manufacturer"] = manufacturer
                
                if category_match:
                    category = category_match.group(1).strip()
                    if category and category != "[category name]":
                        result["category"] = category
                
                if website_match:
                    website = website_match.group(1).strip()
                    if website and website != "[official website if found, or empty]":
                        result["website"] = website
                
                if "manufacturer" in result and "category" in result:
                    self.logger.info(f"Successfully extracted manufacturer '{result['manufacturer']}' and category '{result['category']}'")
                    return result
                else:
                    self.logger.error("Failed to extract both manufacturer and category")
                    return {}
            else:
                # Missing the delimited response format
                self.logger.error(f"Error: Response missing RESPONSE_START/END markers: {answer}")
                return {}
            
        except Exception as e:
            self.logger.error(f"Error in category page info extraction: {str(e)}")
            return {}

    def validate_categories(self, categories: List[str], manufacturer_name: str) -> List[str]:
        """
        Validate and normalize a list of extracted categories to ensure they are legitimate product categories.
        
        This method performs the following validations:
        1. Removes UI elements and navigation controls
        2. Normalizes category names (standardizes format, removes duplicates)
        3. Ensures categories are actual product lines, not marketing terms
        4. Removes very short or generic terms
        
        Args:
            categories: List of extracted category names
            manufacturer_name: Name of the manufacturer for context
            
        Returns:
            List of validated and normalized category names
        """
        # Early return if no categories
        if not categories:
            return []
            
        self.logger.info(f"CATEGORY VALIDATION: Starting validation of {len(categories)} categories for {manufacturer_name}")
        
        # Common UI elements and navigation controls to filter out - use exact matches only
        ui_elements = [
            'home', 'menu', 'search', 'login', 'register', 'contact', 'about', 'support',
            'next', 'previous', 'view all', 'show more', 'back', 'forward', 'privacy', 'terms',
            'cookie', 'sitemap', 'faq', 'help', 'cart', 'checkout', 'account', 'profile',
            'facebook', 'twitter', 'instagram', 'youtube', 'linkedin'
        ]
        
        # Filter out obvious UI elements and normalize categories
        filtered_categories = []
        for category in categories:
            # Skip very short categories (likely not valid)
            if len(category) < 3:
                self.logger.info(f"Filtering out short category: '{category}'")
                continue
                
            # Skip categories that EXACTLY match common UI elements (case-insensitive)
            # But don't filter out legitimate product categories that might contain these words
            is_ui_element = False
            if category.lower() in [ui.lower() for ui in ui_elements]:
                is_ui_element = True
                self.logger.info(f"Filtering out UI element: '{category}'")
            
            # Don't filter out product categories like 'Home Automation Systems'
            # which contain UI words but are legitimate categories
                    
            if not is_ui_element:
                # Add the category if it's not already in the list (case-insensitive check)
                if not any(cat.lower() == category.lower() for cat in filtered_categories):
                    filtered_categories.append(category)
        
        # If we have more than 5 categories, use Claude to validate them
        # This helps with more complex validation that simple rules might miss
        if len(filtered_categories) > 5 and hasattr(self, 'api_key') and self.api_key:
            try:
                # Check if CATEGORY_VALIDATION_PROMPT is defined
                if 'CATEGORY_VALIDATION_PROMPT' not in globals():
                    self.logger.warning("CATEGORY_VALIDATION_PROMPT not defined, skipping Claude validation")
                    return filtered_categories
                    
                self.logger.info(f"Using Claude to validate {len(filtered_categories)} categories")
                
                # Format the prompt with the categories and manufacturer name
                categories_list = [f"- {cat}" for cat in filtered_categories]
                categories_text = '\n'.join(categories_list)
                
                # Import the prompt template if needed
                from scraper.prompt_templates import CATEGORY_VALIDATION_PROMPT
                
                prompt = CATEGORY_VALIDATION_PROMPT.format(
                    manufacturer=manufacturer_name,
                    categories=categories_text
                )
                
                # Prepare headers for API call
                headers = {
                    "x-api-key": self.api_key,
                    "content-type": "application/json",
                    "anthropic-version": "2023-06-01"
                }
                
                # Use the Sonnet model for better validation quality
                payload = {
                    "model": self.sonnet_model,  # Use Sonnet for better validation
                    "max_tokens": 2000,
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a specialized translator for product categories."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                }
                
                # Make API request
                self.logger.info(f"Making Claude API call with model {self.sonnet_model} for category validation")
                response = requests.post(
                    "https://api.anthropic.com/v1/messages",
                    headers=headers,
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    # Process the response
                    response_data = response.json()
                    result = response_data["content"][0]["text"]
                    
                    # Parse the validated categories
                    validated_categories = self._parse_category_response(result)
                    
                    if validated_categories:
                        self.logger.info(f"Successfully validated categories: {len(validated_categories)} valid out of {len(filtered_categories)} original")
                        return validated_categories
                    else:
                        self.logger.warning(f"No valid categories found after validation")
                        return filtered_categories  # Fall back to filtered categories
                else:
                    self.logger.error(f"API error during category validation: {response.status_code} - {response.text}")
                    return filtered_categories  # Fall back to filtered categories
                    
            except Exception as e:
                self.logger.error(f"Error during category validation: {str(e)}")
                return filtered_categories  # Fall back to filtered categories
        
        self.logger.info(f"Completed category validation: {len(filtered_categories)} valid categories")
        return filtered_categories
        
    def translate_categories_batch(self, categories_text: str, manufacturer_name: str, target_lang: str, response_format='delimited') -> List[str]:
        """
        Translate multiple categories at once to reduce API calls.
        
        Args:
            categories_text: Newline-separated categories to translate
            manufacturer_name: Manufacturer name to preserve
            target_lang: Target language code
            response_format: Format of the response ('delimited' or 'json')
            
        Returns:
            List of translated categories
        """
        try:
            # Map language codes to full names
            language_names = {
                'es': 'Spanish',
                'pt': 'Portuguese',
                'fr': 'French',
                'it': 'Italian'
            }
            target_language = language_names.get(target_lang, 'Spanish')
            
            # Split categories into lines
            categories_list = [cat.strip() for cat in categories_text.split('\n') if cat.strip()]
            
            # Prepare the categories_line_by_line format for the prompt template
            categories_line_by_line = '\n'.join([f"{cat} → [Translation]" for cat in categories_list])
            
            # Format the prompt for batch translation
            prompt = TRANSLATION_BATCH_PROMPT.format(
                categories=categories_text,
                manufacturer_name=manufacturer_name,
                target_language=target_language,
                categories_line_by_line=categories_line_by_line
            )
            
            self.logger.info(f"Preparing to translate {len(categories_list)} categories to {target_language}")
            self.logger.debug(f"Translation prompt: {prompt[:200]}...")
            
            # Prepare headers
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            # Implement exponential backoff with model fallback
            retry_count = 0
            wait_time = self.base_wait_time
            success = False
            response = None
            
            while not success and retry_count < self.max_retries:
                current_model = self.fallback_models[self.current_model_index]
                
                # Prepare payload - use appropriate max_tokens for translations
                payload = {
                    "model": current_model,
                    "max_tokens": 1000,  # Increased to handle multiple translations
                    "messages": [
                        {
                            "role": "system",
                            "content": TRANSLATION_SYSTEM_PROMPT
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                }
                
                self.api_calls += 1
                self.logger.info(f"Making batch translation API call with model {current_model} (attempt {retry_count+1}/{self.max_retries})")
                
                try:
                    response = requests.post(
                        "https://api.anthropic.com/v1/messages",
                        headers=headers,
                        json=payload,
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        success = True
                        response_data = response.json()
                        result = response_data["content"][0]["text"]
                        
                        if response_format == 'delimited':
                            # Parse translations between delimiters
                            match = re.search(r'# RESPONSE_START\s*(.+?)\s*# RESPONSE_END', result, re.DOTALL)
                            if match:
                                # Extract the translated lines
                                translation_lines = [line.strip() for line in match.group(1).strip().split('\n') if line.strip()]
                                
                                # Parse each line to extract the translation part
                                translations = []
                                for line in translation_lines:
                                    # Look for the arrow separator and extract the translation
                                    arrow_match = re.search(r'→\s*(.+)$', line)
                                    if arrow_match:
                                        translation = arrow_match.group(1).strip()
                                        # Remove any brackets that might have been included
                                        translation = re.sub(r'^\[|\]$', '', translation).strip()
                                        translations.append(translation)
                                    else:
                                        # If no arrow found, log warning but don't fail
                                        self.logger.warning(f"Could not parse translation from line: {line}")
                                
                                # Verify we have the right number of translations
                                if len(translations) != len(categories_list):
                                    self.logger.warning(f"Expected {len(categories_list)} translations but got {len(translations)}")
                                    # If we have fewer translations than categories, pad with originals
                                    if len(translations) < len(categories_list):
                                        translations.extend(categories_list[len(translations):])
                                    # If we have more translations than categories, truncate
                                    elif len(translations) > len(categories_list):
                                        translations = translations[:len(categories_list)]
                                
                                self.logger.info(f"Successfully translated {len(translations)} categories to {target_language}")
                                return translations
                            else:
                                raise ValueError("No valid translations found in response")
                        elif response_format == 'json':
                            # Parse JSON response
                            try:
                                result_json = json.loads(result)
                                translations = result_json.get('translations', [])
                                if len(translations) != len(categories_list):
                                    self.logger.warning(f"Expected {len(categories_list)} translations but got {len(translations)}")
                                    # If we have fewer translations than categories, pad with originals
                                    if len(translations) < len(categories_list):
                                        translations.extend(categories_list[len(translations):])
                                    # If we have more translations than categories, truncate
                                    elif len(translations) > len(categories_list):
                                        translations = translations[:len(categories_list)]
                                
                                self.logger.info(f"Successfully translated {len(translations)} categories to {target_language}")
                                return translations
                            except json.JSONDecodeError:
                                self.logger.error(f"Failed to parse JSON response: {result}")
                                raise ValueError("Invalid JSON response")
                        else:
                            raise ValueError("Invalid response format")
                            
                    elif response.status_code == 429:
                        if self.current_model_index < len(self.fallback_models) - 1:
                            self.current_model_index += 1
                            self.logger.warning(f"Rate limited - falling back to {self.fallback_models[self.current_model_index]}")
                        else:
                            self.logger.warning(f"Rate limited on all models - waiting {wait_time}s")
                            time.sleep(wait_time)
                            wait_time = min(wait_time * 2, self.max_wait_time)
                        retry_count += 1
                    else:
                        self.logger.error(f"API error: {response.status_code} - {response.text}")
                        time.sleep(wait_time)
                        wait_time = min(wait_time * 2, self.max_wait_time)
                        retry_count += 1
                        
                except Exception as e:
                    self.logger.error(f"Request error: {str(e)}")
                    time.sleep(wait_time)
                    wait_time = min(wait_time * 2, self.max_wait_time)
                    retry_count += 1
            
            if not success:
                self.logger.error(f"Failed to translate categories after {self.max_retries} attempts")
                # Return original categories as fallback
                return categories_list
                
        except Exception as e:
            self.logger.error(f"Batch translation error: {str(e)}")
            # Return original categories as fallback
            if isinstance(categories_text, str):
                return [cat.strip() for cat in categories_text.split('\n') if cat.strip()]
            return []
    
    def translate_category(self, category: str, manufacturer_name: str, target_lang: str) -> str:
        """
        Legacy method for single category translation.
        Now uses batch translation under the hood for efficiency.
        
        Args:
            category: Original category name
            manufacturer_name: Manufacturer name to preserve
            target_lang: Target language code (e.g., 'es', 'pt', 'fr', 'it')
            
        Returns:
            Translated category name
        """
        try:
            # Skip translation for very short categories or those containing numbers
            if len(category) < 3 or any(char.isdigit() for char in category):
                self.logger.info(f"Skipping translation for short or numeric category: {category}")
                return category
                
            # Use batch translation with a single category
            translations = self.translate_categories_batch(category, manufacturer_name, target_lang)
            
            if translations and len(translations) > 0:
                translated = translations[0]
                
                # Validate translation
                if translated and len(translated) >= 3:
                    # Check if manufacturer name is preserved
                    if manufacturer_name.lower() in translated.lower():
                        self.logger.info(f"Translated '{category}' to '{translated}' ({target_lang})")
                        return translated
                    else:
                        # Add manufacturer name if missing
                        translated = f"{manufacturer_name} {translated}"
                        self.logger.info(f"Added manufacturer to translation: '{translated}' ({target_lang})")
                        return translated
                        
            self.logger.warning(f"Failed to get valid translation for '{category}'")
            return category
            
        except Exception as e:
            self.logger.error(f"Error translating category: {str(e)}")
            return category
