import os
import re
import yaml
import requests
import logging
import json
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

# System prompts
MANUFACTURER_SYSTEM_PROMPT = """You are a specialized AI trained to analyze web content and identify manufacturers and their product categories.
Your task is to analyze structured navigation data from web pages and identify manufacturers/brands ONLY."""

CATEGORY_SYSTEM_PROMPT = """You are a specialized AI trained to analyze web content and identify product categories for a specific manufacturer.
Your task is to analyze structured navigation data from web pages and identify product categories belonging to the manufacturer."""

# General system prompt used for other tasks
SYSTEM_PROMPT = """You are Claude, an AI assistant specialized in analyzing web content to extract structured information about manufacturers and their product categories.

Your task is to carefully analyze the provided content and extract the requested information following the exact format specified in each instruction."""

# Import other less essential prompt templates from the original file
try:
    from scraper.prompt_templates import (
        MANUFACTURER_EXTRACTION_PROMPT, CATEGORY_EXTRACTION_PROMPT, 
        CATEGORY_VALIDATION_PROMPT, TRANSLATION_PROMPT
    )
except ImportError:
    # Fallback minimal templates if imports fail
    MANUFACTURER_EXTRACTION_PROMPT = "Extract manufacturers from this content: {content}"
    CATEGORY_EXTRACTION_PROMPT = "Extract categories for {manufacturer_name} from this content: {content}"
    CATEGORY_VALIDATION_PROMPT = "Validate these categories for {manufacturer}: {categories}"
    TRANSLATION_PROMPT = "Translate {category} to {target_language}"


class ClaudeAnalyzer:
    """A class to handle Claude API interactions for analyzing web content"""
    
    def __init__(self, api_key, model, sonnet_model, logger=None):
        """Initialize the Claude analyzer"""
        self.api_key = api_key
        self.model = model
        self.sonnet_model = sonnet_model
        self.logger = logger or logging.getLogger('claude_analyzer')
        self.category_count = 0  # Track categories found in initial pass

    def is_manufacturer_page(self, url, title, content):
        """Check if the page is a manufacturer page using Claude Haiku"""
        try:
            # Prepare a truncated version of the content
            truncated_content = content[:2000] + "..." if len(content) > 2000 else content
            
            # Use the module-level imports for prompt templates
            
            # Format the prompt with the page details
            prompt = MANUFACTURER_DETECTION_PROMPT.format(
                url=url,
                title=title,
                truncated_content=truncated_content
            )
            
            # Use requests to make a direct API call
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            payload = {
                "model": self.model,  # Using Haiku model for this task
                "max_tokens": 50,
                "system": MANUFACTURER_SYSTEM_PROMPT,
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=payload
            )
            
            # Handle the response
            if response.status_code == 200:
                response_data = response.json()
                answer = response_data["content"][0]["text"].strip()
                
                # Log the response
                self.logger.info(f"Claude Haiku manufacturer detection for {url}: {answer}")
                
                # Parse the response for manufacturer detection
                # Check for delimited format
                response_pattern = r'# RESPONSE_START\s+(.*?)\s*# RESPONSE_END'
                response_match = re.search(response_pattern, answer, re.DOTALL)
                
                if response_match:
                    content = response_match.group(1).strip()
                    # Look for IS_MANUFACTURER: yes/no
                    is_mfr_match = re.search(r'IS_MANUFACTURER:\s*(yes|no)', content, re.IGNORECASE)
                    if is_mfr_match:
                        result = is_mfr_match.group(1).strip().lower() == 'yes'
                        self.logger.info(f"Manufacturer detection result for {url}: {result}")
                        return result
                    else:
                        self.logger.error(f"Missing IS_MANUFACTURER field in response: {content}")
                        return False
                else:
                    self.logger.error(f"Response missing RESPONSE_START/END markers: {answer}")
                    return False
            else:
                self.logger.error(f"Error from Anthropic API: {response.status_code} - {response.text}")
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
        """Extract manufacturer information from structured content"""
        try:
            # Use the module-level imports for prompt templates
            
            # Format the prompt with the structured content
            prompt = MANUFACTURER_EXTRACTION_PROMPT.format(content=content)
            
            # Make direct API call to Claude
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            payload = {
                "model": self.model,
                "max_tokens": 4000,
                "system": MANUFACTURER_SYSTEM_PROMPT,
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=payload
            )
            
            if response.status_code == 200:
                response_data = response.json()
                result = response_data["content"][0]["text"]
                
                # Parse the delimited response
                parsed_data = self._parse_manufacturer_response(result)
                if parsed_data and parsed_data.get('manufacturers'):
                    self.logger.info(f"Found {len(parsed_data['manufacturers'])} manufacturers in {url}")
                    return parsed_data['manufacturers']
                else:
                    self.logger.info(f"No manufacturers found in {url}")
                    return []
            else:
                self.logger.error(f"Error from Anthropic API: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error extracting manufacturers: {str(e)}")
            return []

    def _extract_categories(self, url: str, manufacturer_name: str, content: str) -> List[str]:
        """Extract category information for a specific manufacturer"""
        try:
            # Use the module-level imports for prompt templates
            
            # Format the prompt with the structured content
            prompt = CATEGORY_EXTRACTION_PROMPT.format(
                content=content,
                manufacturer_name=manufacturer_name
            )
            
            # Make direct API call to Claude
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            payload = {
                "model": self.model,
                "max_tokens": 4000,
                "system": CATEGORY_SYSTEM_PROMPT,
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=payload
            )
            
            if response.status_code == 200:
                response_data = response.json()
                result = response_data["content"][0]["text"]
                
                # Parse the delimited response
                categories = self._parse_category_response(result)
                if categories:
                    self.logger.info(f"Found {len(categories)} categories for {manufacturer_name} in {url}")
                    return categories
                else:
                    self.logger.info(f"No categories found for {manufacturer_name} in {url}")
                    return []
            else:
                self.logger.error(f"Error from Anthropic API: {response.status_code} - {response.text}")
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
        """Extract manufacturer and category information using Claude Sonnet"""
        try:
            # Prepare a truncated version of the content to avoid token limits
            truncated_content = content[:4000] + "..." if len(content) > 4000 else content
            
            # Use the module-level imports for prompt templates
            
            # Format the prompt with the page details
            prompt = MANUFACTURER_EXTRACTION_PROMPT.format(
                url=url,
                title=title,
                truncated_content=truncated_content
            )
            
            # Use requests to make a direct API call
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            payload = {
                "model": self.model,  # Using Haiku model for all operations
                "max_tokens": 4000,
                "system": MANUFACTURER_SYSTEM_PROMPT,
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=payload
            )
            
            # Handle the response
            if response.status_code == 200:
                response_data = response.json()
                result = response_data["content"][0]["text"]
                self.logger.info(f"Claude Haiku manufacturer extraction for {url}: Response received")
                
                # Parse the response to extract manufacturers and categories
                parsed_data = self._parse_delimited_response(result)
                if parsed_data:
                    self.logger.info(f"Extracted {len(parsed_data.get('manufacturers', []))} manufacturers from {url}")
                else:
                    self.logger.warning(f"No manufacturers extracted from {url}")
                    
                return parsed_data
            else:
                self.logger.error(f"Error from Anthropic API: {response.status_code} - {response.text}")
                return None
            
        except Exception as e:
            self.logger.error(f"Error analyzing with Claude Haiku extraction: {str(e)}")
            return None
    
    def translate_category(self, category: str, manufacturer_name: str, target_lang: str) -> str:
        """
        Translate a category name to the target language.
        
        Args:
            category: Original category name
            manufacturer_name: Manufacturer name to preserve
            target_lang: Target language code (e.g., 'es', 'pt', 'fr', 'it')
            
        Returns:
            Translated category name
        """
        try:
            # Map language codes to full names
            language_names = {
                'es': 'Spanish',
                'pt': 'Portuguese',
                'fr': 'French',
                'it': 'Italian'
            }
            
            # Get full language name
            target_language = language_names.get(target_lang, 'Spanish')  # Default to Spanish if unknown
            
            # Use the module-level imports for prompt templates
            
            # Format the prompt with the category and target language
            prompt = TRANSLATION_PROMPT.format(
                category=category,
                target_language=target_language
            )
            
            # Use requests to make a direct API call
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            payload = {
                "model": self.model,
                "max_tokens": 100,
                "messages": [
                    {
                        "role": "system",
                        "content": SYSTEM_PROMPT
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
            
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=payload
            )
            
            # Handle the response
            if response.status_code == 200:
                response_data = response.json()
                result = response_data["content"][0]["text"].strip()
                
                # Parse the response for the translation
                response_pattern = r'# RESPONSE_START\s+TRANSLATED:\s*(.*?)\s*# RESPONSE_END'
                response_match = re.search(response_pattern, result, re.DOTALL)
                
                if response_match:
                    translated = response_match.group(1).strip()
                    
                    # Validate translation contains manufacturer name
                    if manufacturer_name.lower() not in translated.lower():
                        self.logger.warning(f"Translation missing manufacturer name: {translated}")
                        return category
                    
                    self.logger.info(f"Translated to {target_lang}: {translated}")
                    return translated
                else:
                    self.logger.error(f"Invalid translation response format: {result}")
                    return category
            else:
                self.logger.error(f"Error from Anthropic API: {response.status_code} - {response.text}")
                return category
                
        except Exception as e:
            self.logger.error(f"Error translating category: {str(e)}")
            return category
