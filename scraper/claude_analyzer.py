import os
import re
import yaml
import requests
import logging

class ClaudeAnalyzer:
    """A class to handle Claude API interactions for analyzing web content"""
    
    def __init__(self, api_key, model, sonnet_model, logger=None):
        """Initialize the Claude analyzer"""
        self.api_key = api_key
        self.model = model
        self.sonnet_model = sonnet_model
        self.logger = logger or logging.getLogger('claude_analyzer')
    
    def is_manufacturer_page(self, url, title, content):
        """Check if the page is a manufacturer page using Claude"""
        try:
            # Prepare a truncated version of the content
            truncated_content = content[:2000] + "..." if len(content) > 2000 else content
            
            # Import prompt templates
            from scraper.prompt_templates import MANUFACTURER_DETECTION_PROMPT, SYSTEM_PROMPT
            
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
                "model": self.model,
                "max_tokens": 10,
                "system": "You are a data extraction assistant specialized in identifying manufacturers and their specific product categories. Always ensure categories are associated with their manufacturer names for clarity.",
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
                self.logger.info(f"Claude analysis for {url}: {answer}")
                
                return self._parse_delimited_response(answer)
            else:
                self.logger.error(f"Error from Anthropic API: {response.status_code} - {response.text}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error analyzing with Claude: {str(e)}")
            return False
    
    def extract_manufacturers(self, url, title, content):
        """Extract manufacturer and category information using Claude 3.7 Sonnet"""
        try:
            # Prepare a truncated version of the content to avoid token limits
            truncated_content = content[:4000] + "..." if len(content) > 4000 else content
            
            prompt = f"""URL: {url}\nTitle: {title}\n\nContent: {truncated_content}\n\nTask: Extract manufacturer names and product categories from this webpage. \n\nIf this is a manufacturer/brand page, identify:\n1. The manufacturer/brand name(s)\n2. All product categories associated with each manufacturer - be specific and detailed (e.g., 'Samsung washing machines' rather than just 'washing machines')\n3. The manufacturer's website URL if mentioned\n\nIt's CRITICAL that each category is specific to the manufacturer and not generic. For example:\n- GOOD: "LG Refrigerators", "Samsung Smartphones", "Sony Televisions"\n- BAD: "Refrigerators", "Smartphones", "Televisions"\n\nFormat your response as YAML:\n\nmanufacturers:\n  - name: Manufacturer Name\n    categories:\n      - ManufacturerName Category1\n      - ManufacturerName Category2\n    website: https://manufacturer-website.com  # or null if not found\n\nIf no manufacturers or categories are found, return an empty list like this:\n\nmanufacturers: []\n"""
            
            # Use requests to make a direct API call
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            payload = {
                "model": self.sonnet_model,
                "max_tokens": 4000,
                "system": "You are a data extraction assistant specialized in identifying manufacturers and their specific product categories. Always ensure categories are associated with their manufacturer names for clarity.",
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
                return self._parse_delimited_response(result)
            else:
                self.logger.error(f"Error from Anthropic API: {response.status_code} - {response.text}")
                return None
            
        except Exception as e:
            self.logger.error(f"Error analyzing with Claude Sonnet: {str(e)}")
            return None
    
    def _parse_delimited_response(self, response_text):
        """Parse response using strict delimited format"""
        try:
            # Extract content between RESPONSE_START and RESPONSE_END
            response_pattern = r'# RESPONSE_START\s+([\s\S]*?)# RESPONSE_END'
            response_match = re.search(response_pattern, response_text)
            
            if not response_match:
                self.logger.error("Response missing RESPONSE_START/END markers")
                return {"manufacturers": []}
                
            content = response_match.group(1).strip()
            self.logger.info(f"Found delimited content:\n{content[:500]}...")
            
            # For manufacturer detection response
            if 'IS_MANUFACTURER:' in content:
                is_mfr_match = re.search(r'IS_MANUFACTURER:\s*(yes|no)', content, re.IGNORECASE)
                if not is_mfr_match:
                    self.logger.error("Invalid manufacturer detection response format")
                    return False
                return is_mfr_match.group(1).strip().lower() == 'yes'
            
            # For manufacturer data extraction
            manufacturers = []
            manufacturer_pattern = r'# MANUFACTURER_START\s+([\s\S]*?)# MANUFACTURER_END'
            
            for match in re.finditer(manufacturer_pattern, content, re.DOTALL):
                manufacturer_block = match.group(1).strip()
                self.logger.info(f"Found manufacturer block:\n{manufacturer_block}")
                
                # Extract manufacturer name (required)
                name_match = re.search(r'NAME:\s*(.*?)(?:\n|$)', manufacturer_block)
                if not name_match:
                    self.logger.warning("Manufacturer block missing NAME field, skipping")
                    continue
                    
                name = name_match.group(1).strip()
                
                # Find all categories (at least one required)
                categories = []
                for category_match in re.finditer(r'CATEGORY:\s*(.*?)(?:\n|$)', manufacturer_block):
                    category = category_match.group(1).strip()
                    # Validate category includes manufacturer name
                    if name.lower() in category.lower():
                        categories.append(category)
                    else:
                        self.logger.warning(f"Skipping category '{category}' - must include manufacturer name '{name}'")
                
                if not categories:
                    self.logger.warning(f"No valid categories found for manufacturer {name}, skipping")
                    continue
                
                # Extract website (optional)
                website = None
                website_match = re.search(r'WEBSITE:\s*(.*?)(?:\n|$)', manufacturer_block)
                if website_match:
                    website = website_match.group(1).strip()
                
                # Build manufacturer object
                manufacturer = {
                    "name": name,
                    "categories": categories
                }
                
                if website:
                    manufacturer["website"] = website
                    
                manufacturers.append(manufacturer)
            
            self.logger.info(f"Successfully parsed {len(manufacturers)} manufacturers")
            return {"manufacturers": manufacturers}
            
            # If no manufacturers found, return empty list
            self.logger.info("No manufacturers found in delimited response")
            return {"manufacturers": []}
            
        except Exception as e:
            self.logger.error(f"Error parsing delimited response: {str(e)}")
            return {"manufacturers": []}
            
            # Parse manufacturer information from delimited format
            manufacturers = []
            manufacturer_pattern = r'# MANUFACTURER_START\s+([\s\S]*?)# MANUFACTURER_END'
            
            for match in re.finditer(manufacturer_pattern, content, re.DOTALL):
                manufacturer_block = match.group(1).strip()
                self.logger.info(f"Found manufacturer block:\n{manufacturer_block}")
                
                # Extract manufacturer name
                name_match = re.search(r'NAME:\s*(.*?)(?:\n|$)', manufacturer_block)
                if not name_match:
                    self.logger.warning("Manufacturer block missing NAME field, skipping")
                    continue
                    
                name = name_match.group(1).strip()
                
                # Find all categories
                categories = []
                for category_match in re.finditer(r'CATEGORY:\s*(.*?)(?:\n|$)', manufacturer_block):
                    categories.append(category_match.group(1).strip())
                
                # Extract website if present
                website = None
                website_match = re.search(r'WEBSITE:\s*(.*?)(?:\n|$)', manufacturer_block)
                if website_match:
                    website = website_match.group(1).strip()
                
                # Build manufacturer object
                manufacturer = {
                    "name": name,
                    "categories": categories
                }
                
                if website:
                    manufacturer["website"] = website
                    
                manufacturers.append(manufacturer)
            
            if manufacturers:
                self.logger.info(f"Extracted {len(manufacturers)} manufacturers using delimited format")
                return {"manufacturers": manufacturers}
            
            # APPROACH 4: Last resort - try regex extraction on the entire content
            return self._extract_with_regex(response_text)
            
        except Exception as e:
            self.logger.error(f"Error parsing delimited response: {str(e)}")
            # Fall back to extracting whatever we can from the text
            return self._extract_with_regex(response_text)
    
    def _extract_with_regex(self, text):
        """Extract data using regex patterns when YAML parsing fails"""
        self.logger.info("Attempting to extract data with regex patterns")
        
        # Check if this is an is_manufacturer response
        if 'is_manufacturer' in text.lower():
            is_mfr = re.search(r'is_manufacturer:\s*(yes|true|y|1)', text.lower())
            if is_mfr:
                return True
            return False
        
        # Try to extract manufacturer data
        manufacturers = []
        
        # Look for manufacturer patterns
        # Pattern 1: name: Manufacturer Name
        manufacturer_patterns = [
            r'name:\s*([^\n]+)',  # Simple name: value pattern
            r'- name:\s*([^\n]+)'  # List item name pattern
        ]
        
        # Find all potential manufacturer names
        all_manufacturer_names = []
        for pattern in manufacturer_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                name = match.group(1).strip().strip('"').strip("'")
                if name and len(name) < 100:  # Basic sanity check
                    all_manufacturer_names.append(name)
        
        # Now extract categories and websites for each manufacturer
        for name in all_manufacturer_names:
            # Find the segment of text that might contain this manufacturer's details
            name_pos = text.find(f"name: {name}")
            if name_pos == -1:
                name_pos = text.find(f"- name: {name}")
            
            if name_pos != -1:
                # Extract text from this position to the next manufacturer or end
                next_name_pos = text.find("name:", name_pos + 10)
                if next_name_pos == -1:
                    segment = text[name_pos:]
                else:
                    segment = text[name_pos:next_name_pos]
                
                # Extract categories
                categories = []
                category_matches = re.finditer(r'- ([^\n]+?)(?=\n|$)', segment)
                for match in category_matches:
                    category = match.group(1).strip()
                    if category and len(category) < 100 and name.lower() in category.lower():
                        categories.append(category)
                
                # Extract website
                website_match = re.search(r'website:\s*(https?://[^\s\n"]+)', segment)
                website = website_match.group(1) if website_match else None
                
                # Create manufacturer entry
                manufacturer = {
                    'name': name,
                    'categories': categories,
                    'website': website
                }
                manufacturers.append(manufacturer)
        
        # If we found any manufacturers, return them structured properly
        if manufacturers:
            return {'manufacturers': manufacturers}
        
        # Fallback to empty list
        return {'manufacturers': []}
