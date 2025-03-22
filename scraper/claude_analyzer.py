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
        """Check if the page is a manufacturer page using Claude Haiku"""
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
                "model": self.model,  # Using Haiku model for this task
                "max_tokens": 50,
                "system": SYSTEM_PROMPT,
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
    
    def extract_manufacturers(self, url, title, content):
        """Extract manufacturer and category information using Claude Sonnet"""
        try:
            # Prepare a truncated version of the content to avoid token limits
            truncated_content = content[:4000] + "..." if len(content) > 4000 else content
            
            # Import prompt templates
            from scraper.prompt_templates import MANUFACTURER_EXTRACTION_PROMPT, SYSTEM_PROMPT
            
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
                "system": SYSTEM_PROMPT,
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
            self.logger.info(f"Found delimited content of length {len(content)}")
            
            # For manufacturer detection response (is_manufacturer_page method)
            if 'IS_MANUFACTURER:' in content:
                is_mfr_match = re.search(r'IS_MANUFACTURER:\s*(yes|no)', content, re.IGNORECASE)
                if not is_mfr_match:
                    self.logger.error("Invalid manufacturer detection response format")
                    return False
                return is_mfr_match.group(1).strip().lower() == 'yes'
            
            # For manufacturer data extraction (extract_manufacturers method)
            manufacturers = []
            manufacturer_pattern = r'# MANUFACTURER_START\s+([\s\S]*?)# MANUFACTURER_END'
            manufacturer_matches = list(re.finditer(manufacturer_pattern, content, re.DOTALL))
            
            if not manufacturer_matches:
                self.logger.info("No MANUFACTURER_START/END blocks found in response")
                # Try YAML parsing as fallback
                try:
                    # Check if content looks like YAML
                    if 'manufacturers:' in content:
                        yaml_data = yaml.safe_load(content)
                        if yaml_data and 'manufacturers' in yaml_data:
                            self.logger.info(f"Parsed {len(yaml_data['manufacturers'])} manufacturers from YAML")
                            return yaml_data
                except Exception as yaml_error:
                    self.logger.warning(f"Failed to parse as YAML: {str(yaml_error)}")
                    
                # If we reach here, try regex extraction as last resort
                return self._extract_with_regex(response_text)
            
            # Process each manufacturer block
            for match in manufacturer_matches:
                manufacturer_block = match.group(1).strip()
                self.logger.info(f"Processing manufacturer block of length {len(manufacturer_block)}")
                
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
            
            if manufacturers:
                self.logger.info(f"Successfully parsed {len(manufacturers)} manufacturers")
                return {"manufacturers": manufacturers}
            
            # If no manufacturers found after processing all blocks, return empty list
            self.logger.info("No valid manufacturers found in delimited response")
            return {"manufacturers": []}
            
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
        
    def translate_text(self, text, source_lang, target_lang, preserve_text=None):
        """
        Translate text from source language to target language using Claude.
        
        Args:
            text: Text to translate
            source_lang: Source language (e.g., 'english')
            target_lang: Target language (e.g., 'spanish')
            preserve_text: Text to preserve in the translation (e.g., manufacturer name)
            
        Returns:
            Translated text
        """
        try:
            # Skip translation if text is too short
            if len(text) < 3:
                return text
                
            # For Spanish, check if it already contains Spanish words
            if target_lang.lower() == 'spanish':
                spanish_indicators = ['de', 'para', 'con', 'del', 'y', 'las', 'los']
                if any(word in text.lower().split() for word in spanish_indicators):
                    self.logger.info(f"Text '{text}' appears to already be in Spanish, skipping translation")
                    return text
            
            # Prepare the translation prompt
            base_prompt = "Translate the following text from {0} to {1}:\n\n{2}"
            prompt = base_prompt.format(source_lang, target_lang, text)
            
            # Add instruction to preserve specific text if provided
            if preserve_text:
                prompt += "\n\nImportant: Preserve the term '{0}' exactly as it appears in the original text.".format(preserve_text)
            
            # Use requests to make a direct API call
            headers = {
                "x-api-key": self.api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            payload = {
                "model": self.model,  # Using Haiku model for simple translation
                "max_tokens": 100,
                "system": "You are a professional translator. Translate text accurately while preserving any technical terms, brand names, or proper nouns.",
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
                translated_text = response_data["content"][0]["text"].strip()
                
                # Remove any instructions that might have been included in the response
                if "Important:" in translated_text:
                    translated_text = translated_text.split("Important:")[0].strip()
                if "Importante:" in translated_text:
                    translated_text = translated_text.split("Importante:")[0].strip()
                
                # Verify the preserved text is still present if specified
                if preserve_text and preserve_text.lower() not in translated_text.lower():
                    self.logger.warning(f"Translation lost preserved text '{preserve_text}': '{translated_text}'")
                    # Try to reinsert the preserved text
                    translated_text = translated_text.replace(
                        preserve_text.lower().capitalize(), 
                        preserve_text
                    )
                
                # Logging moved to CompetitorScraper to avoid duplication
                return translated_text
            else:
                self.logger.error(f"Error from Anthropic API: {response.status_code} - {response.text}")
                return text  # Return original text on error
        except Exception as e:
            self.logger.error(f"Error translating text: {str(e)}")
            return text  # Return original text on error
