import os
import re
import time
import json
import yaml
import hashlib
import logging
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Import the new ConfigManager
from utils.config_manager import ConfigManager

class ClaudeAnalyzer:
    """
    Claude Analyzer for web content.
    
    This class provides methods to analyze web content using the Claude API,
    with a focus on identifying manufacturer pages and extracting structured data.
    It implements a four-step analysis process to ensure accurate results while
    minimizing API calls, resulting in a more efficient and cost-effective analysis process.
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize the Claude Analyzer.
        
        Args:
            config_path: Optional path to configuration file
        """
        # Set up logging
        self.logger = logging.getLogger(__name__)
        
        # Load configuration
        self.config_manager = ConfigManager()
        
        # Initialize statistics
        self.stats = {
            'pages_analyzed': 0,
            'manufacturer_pages_found': 0,
            'categories_found': 0,
            'categories_translated': 0,
            'claude_api_calls': 0,
            'claude_api_prompt_tokens': 0,
            'claude_api_completion_tokens': 0,
            'claude_api_total_tokens': 0,
            'claude_api_errors': 0
        }
        
        # Get API key from environment or config
        self.claude_api_key = os.environ.get('CLAUDE_API_KEY')
        if not self.claude_api_key:
            self.claude_api_key = self.config_manager.get('claude_analyzer.api_key', '')
            
        if not self.claude_api_key:
            raise ValueError("Claude API key is required. Set CLAUDE_API_KEY environment variable or configure it in config.yaml")
                
        self.claude_model = self.config_manager.get('claude_analyzer.claude_model', 'claude-3-5-haiku-20241022')
        
        # Set up response cache
        self.response_cache = {}
        
        # Get prompt templates
        self.prompts = self.config_manager.get('prompt_templates', {})
        
        # Set up caching
        self.enable_cache = self.config_manager.get('claude_analyzer.enable_cache', True)
        self.cache_dir = self.config_manager.get('claude_analyzer.cache_dir', '.cache')
        self.cache = {}
        
        # Load keywords
        self.positive_keywords = self.config_manager.get('claude_analyzer.positive_keywords', [])
        self.negative_keywords = self.config_manager.get('claude_analyzer.negative_keywords', [])
        
        # Set up translation
        self.translation_enabled = self.config_manager.get('translation.enabled', True)
        if not self.translation_enabled:
            self.translation_enabled = self.config_manager.get('claude_analyzer.translation_enabled', True)
            
        self.target_languages = self.config_manager.get('languages.targets', {})
        
        # Initialize translation manager if translation is enabled
        self.translation_manager = None
        if self.translation_enabled and self.target_languages:
            try:
                from scraper.translation_manager import TranslationManager
                self.translation_manager = TranslationManager()
                
                # Ensure the translation manager is properly initialized
                if not self.translation_manager.api_key:
                    self.translation_manager.api_key = self.claude_api_key
                    
                if not self.translation_manager.enabled:
                    self.translation_manager.enabled = True
                    
                self.logger.info(f"Translation enabled for languages: {', '.join(self.target_languages.keys())}")
            except Exception as e:
                self.logger.error(f"Failed to initialize TranslationManager: {str(e)}")
                self.translation_enabled = False
        
        self.logger.info(f"Claude Analyzer initialized with Claude model: {self.claude_model}")
        self.logger.info(f"Caching {'enabled' if self.enable_cache else 'disabled'}")
        self.logger.info(f"Translation {'enabled' if self.translation_enabled else 'disabled'}")
    
    def _load_config(self, config_path: str) -> Dict:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        try:
            # Use default config path if none provided
            if not config_path:
                config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')
            
            self.logger.info(f"Loading configuration from {config_path}")
            
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            
            return self.config
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _get_cache_key(self, system_prompt: str, user_prompt: str, extra_data: str = "") -> str:
        """
        Generate a cache key for a Claude API call.
        
        Args:
            system_prompt: System prompt
            user_prompt: User prompt
            extra_data: Any additional data to include in the key
            
        Returns:
            Cache key as a string
        """
        # Combine all inputs into a single string
        combined = f"{system_prompt}|{user_prompt}|{extra_data}"
        
        # Generate MD5 hash
        return hashlib.md5(combined.encode('utf-8')).hexdigest()
    
    def _check_cache(self, cache_key: str) -> Optional[str]:
        """
        Check if a response is cached in memory.
        
        Args:
            cache_key: Cache key to check
            
        Returns:
            Cached response or None if not found
        """
        if not self.enable_cache:
            return None
        
        try:
            # Check if key exists in memory cache and is not expired
            if cache_key in self.cache:
                cache_entry = self.cache[cache_key]
                expiration_time = cache_entry['timestamp'] + timedelta(seconds=86400)
                
                if datetime.now() < expiration_time:
                    self.stats['cache_hits'] += 1
                    self.logger.debug(f"Cache hit for key: {cache_key[:8]}...")
                    return cache_entry['response']
                else:
                    # Remove expired entry
                    del self.cache[cache_key]
        
        except Exception as e:
            self.logger.error(f"Error checking cache: {e}")
        
        return None
    
    def _update_cache(self, cache_key: str, response: str) -> None:
        """
        Update the in-memory cache with a new response.
        
        Args:
            cache_key: Cache key
            response: Response to cache
        """
        if not self.enable_cache:
            return
        
        try:
            # Store in memory cache with timestamp
            self.cache[cache_key] = {
                'response': response,
                'timestamp': datetime.now()
            }
            
            # Simple cache size management - limit to 1000 entries
            if len(self.cache) > 1000:
                # Remove oldest entries
                sorted_keys = sorted(self.cache.keys(), 
                                    key=lambda k: self.cache[k]['timestamp'])
                for old_key in sorted_keys[:100]:  # Remove oldest 100 entries
                    del self.cache[old_key]
                
        except Exception as e:
            self.logger.error(f"Error updating cache: {e}")
    
    @retry(
        retry=retry_if_exception_type((requests.exceptions.RequestException, ValueError)),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5)
    )
    def _make_api_request(self, payload: Dict) -> Dict:
        """
        Make API request with retry logic.
        
        Args:
            payload: Request payload
            
        Returns:
            API response
        """
        try:
            # Validate payload to ensure it's properly formatted
            if not isinstance(payload, dict):
                raise ValueError("Payload must be a dictionary")
            
            if 'messages' not in payload or not isinstance(payload['messages'], list):
                raise ValueError("Payload must contain a 'messages' list")
            
            # Ensure messages are properly formatted
            for msg in payload['messages']:
                if not isinstance(msg, dict) or 'role' not in msg or 'content' not in msg:
                    raise ValueError("Each message must have 'role' and 'content' fields")
                
                # Ensure content is not too large (Claude has a token limit)
                if len(msg.get('content', '')) > 100000:  # Arbitrary limit to prevent huge requests
                    msg['content'] = msg['content'][:100000] + "... [content truncated]"
            
            # Make request
            response = requests.post(
                self.api_base_url,
                headers=self.headers,
                json=payload,
                timeout=60
            )
            
            # Handle HTTP errors gracefully
            if response.status_code != 200:
                error_message = f"Claude API HTTP error: {response.status_code} - {response.text}"
                self.logger.error(error_message)
                
                # Handle rate limiting specially
                if response.status_code == 429:
                    retry_after = int(response.headers.get('retry-after', 60))
                    self.logger.warning(f"Rate limited. Waiting {retry_after} seconds")
                    time.sleep(retry_after)
                    raise requests.exceptions.RequestException(f"Rate limited: {error_message}")
                
                # For other errors, raise the appropriate exception
                response.raise_for_status()  # This will raise an HTTPError
            
            # Parse response
            try:
                response_data = response.json()
                return response_data
            except ValueError as e:
                self.logger.error(f"Failed to parse JSON response: {e}")
                raise ValueError(f"Invalid JSON response: {response.text}")
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error in API request: {e}")
            # Convert to RequestException for retry mechanism
            raise requests.exceptions.RequestException(f"Error in API request: {str(e)}")
    
    def _call_claude_api(self, prompt: str, max_retries: int = 3) -> str:
        """
        Call Claude API with retry logic and caching.
        
        Args:
            prompt: Prompt to send to Claude API
            max_retries: Maximum number of retries
            
        Returns:
            Claude API response
        """
        self.logger.debug(f"Calling Claude API with prompt length: {len(prompt)}")
        
        # Check cache first
        cache_key = hashlib.md5(prompt.encode()).hexdigest()
        if cache_key in self.response_cache:
            self.logger.debug("Using cached Claude API response")
            self.stats['claude_api_cache_hits'] += 1
            return self.response_cache[cache_key]
        
        # Increment API call counter
        self.stats['claude_api_calls'] += 1
        
        # Prepare request
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.claude_api_key,
            "anthropic-version": "2023-06-01"
        }
        
        # Use the messages API format for Claude 3.5 Haiku
        body = {
            "model": self.claude_model,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 1000,
            "temperature": 0.0
        }
        
        # Make API request with retries
        retry_count = 0
        while retry_count < max_retries:
            try:
                response = requests.post(
                    "https://api.anthropic.com/v1/messages",
                    headers=headers,
                    json=body,
                    timeout=30
                )
                
                # Check if request was successful
                if response.status_code == 200:
                    # Parse response
                    response_json = response.json()
                    response_text = response_json.get('content', [{}])[0].get('text', '').strip()
                    
                    # Cache response
                    self.response_cache[cache_key] = response_text
                    
                    # Log token usage
                    usage = response_json.get('usage', {})
                    prompt_tokens = usage.get('input_tokens', 0)
                    completion_tokens = usage.get('output_tokens', 0)
                    self.stats['claude_api_prompt_tokens'] += prompt_tokens
                    self.stats['claude_api_completion_tokens'] += completion_tokens
                    self.stats['claude_api_total_tokens'] += prompt_tokens + completion_tokens
                    
                    self.logger.debug(f"Claude API response length: {len(response_text)}")
                    return response_text
                else:
                    # Log error and retry
                    self.logger.warning(f"Claude API request failed with status code {response.status_code}: {response.text}")
                    retry_count += 1
                    time.sleep(2 ** retry_count)  # Exponential backoff
            except Exception as e:
                self.logger.error(f"Error calling Claude API: {str(e)}")
                retry_count += 1
                time.sleep(2 ** retry_count)  # Exponential backoff
        
        # If we get here, all retries failed
        self.logger.error(f"All retries failed when calling Claude API")
        self.stats['claude_api_errors'] += 1
        return ""
    
    def keyword_filter(self, url: str, title: str, content: str) -> Dict:
        """
        Filter pages based on keywords to quickly categorize pages without using Claude API.
        This method only examines titles, metadata, and headers - not the full content.
        
        Args:
            url: Page URL
            title: Page title
            content: Page content (will be used to extract metadata and headers only)
            
        Returns:
            Dictionary with filter results:
                - passed: Boolean indicating if the page passed the filter
                - page_type: One of 'brand_page', 'brand_category_page', 'other', or 'inconclusive'
                - positive_keywords: List of positive keywords found
                - negative_keywords: List of negative keywords found
        """
        self.logger.info(f"Applying keyword filter to URL: {url}")
        
        # Extract metadata and headers from content
        metadata = self._extract_metadata_from_content(content)
        
        # Get keywords from config
        positive_keywords = self.positive_keywords
        negative_keywords = self.negative_keywords
        
        # Check for positive keywords in title
        positive_matches_title = []
        for keyword in positive_keywords:
            if keyword.lower() in title.lower():
                positive_matches_title.append(keyword)
        
        # Check for positive keywords in metadata
        positive_matches_metadata = []
        for keyword in positive_keywords:
            if keyword.lower() in metadata.lower():
                positive_matches_metadata.append(keyword)
        
        # Check for negative keywords in title
        negative_matches_title = []
        for keyword in negative_keywords:
            if keyword.lower() in title.lower():
                negative_matches_title.append(keyword)
        
        # Check for negative keywords in metadata
        negative_matches_metadata = []
        for keyword in negative_keywords:
            if keyword.lower() in metadata.lower():
                negative_matches_metadata.append(keyword)
        
        # Combine matches
        positive_matches = list(set(positive_matches_title + positive_matches_metadata))
        negative_matches = list(set(negative_matches_title + negative_matches_metadata))
        
        # Log the matches for debugging
        if positive_matches:
            self.logger.debug(f"Positive keywords found: {positive_matches}")
        
        if negative_matches:
            self.logger.debug(f"Negative keywords found: {negative_matches}")
        
        # Check URL patterns for brand or category indicators
        brand_url_patterns = ['/brand/', '/brands/', '/manufacturer/', '/manufacturers/', 
                             '/about/', '/company/', '/about-us/', '/our-company/']
        url_suggests_brand = any(pattern in url.lower() for pattern in brand_url_patterns)
        
        # Check if title contains brand-related terms
        brand_title_terms = ['brand', 'brands', 'manufacturer', 'manufacturers', 'official', 
                            'company', 'about us', 'about', 'our story', 'history']
        title_suggests_brand = any(term in title.lower() for term in brand_title_terms)
        
        # Check for category-related patterns in URL
        category_url_patterns = ['/category/', '/categories/', '/products/', '/catalog/', 
                                '/collection/', '/collections/', '/range/', '/series/']
        url_suggests_category = any(pattern in url.lower() for pattern in category_url_patterns)
        
        # Check for category-related terms in title
        category_title_terms = ['category', 'categories', 'products', 'catalog', 
                               'collection', 'collections', 'range', 'series']
        title_suggests_category = any(term in title.lower() for term in category_title_terms)
        
        # SIMPLIFIED FILTERING LOGIC
        
        # Strong indicators for brand pages
        if url_suggests_brand or title_suggests_brand:
            self.logger.info(f"URL or title suggests a brand page")
            return {
                'passed': True,
                'page_type': 'brand_page',
                'positive_keywords': positive_matches,
                'negative_keywords': negative_matches
            }
        
        # Strong indicators for category pages
        if url_suggests_category or title_suggests_category:
            self.logger.info(f"URL or title suggests a category page")
            return {
                'passed': True,
                'page_type': 'brand_category_page',
                'positive_keywords': positive_matches,
                'negative_keywords': negative_matches
            }
        
        # If we have positive keywords, it's worth checking
        if positive_matches:
            self.logger.info(f"Page has positive keywords, needs further analysis")
            return {
                'passed': True,
                'page_type': 'inconclusive',
                'positive_keywords': positive_matches,
                'negative_keywords': negative_matches
            }
        
        # Default case: mark as inconclusive for further analysis
        self.logger.info(f"Page has no clear indicators, marking as inconclusive for further analysis")
        return {
            'passed': True,
            'page_type': 'inconclusive',
            'positive_keywords': positive_matches,
            'negative_keywords': negative_matches
        }
    
    def analyze_metadata(self, url: str, title: str, metadata: str, keyword_filter_result: Dict = None) -> Dict:
        """
        Analyze page metadata using Claude API to determine if a page is a manufacturer page.
        
        Args:
            url: Page URL
            title: Page title
            metadata: Page metadata
            keyword_filter_result: Result from keyword_filter method
            
        Returns:
            Dictionary with analysis results:
                - page_type: One of 'brand_page', 'brand_category_page', 'other', or 'inconclusive'
                - confidence: Confidence score (0-1)
        """
        self.logger.info(f"Step 2: Analyzing metadata for URL: {url}")
        
        # If we have a keyword filter result with a definitive page type, use it for context
        page_type_hint = ""
        if keyword_filter_result and keyword_filter_result.get('page_type') in ['brand_page', 'brand_category_page']:
            page_type_hint = f"The keyword analysis suggests this might be a {keyword_filter_result.get('page_type').replace('_', ' ')}."
            if keyword_filter_result.get('positive_keywords'):
                page_type_hint += f" Positive keywords found: {', '.join(keyword_filter_result.get('positive_keywords'))}."
        
        # Prepare prompt
        system_prompt = """You are an AI assistant that analyzes web page metadata to determine if a page is related to a manufacturer/brand.
Your task is to classify the page into one of three categories:
1. Brand page: A page about a manufacturer/brand, typically the main page or about page.
2. Brand category page: A page listing products or categories from a specific manufacturer/brand.
3. Other: Not related to a manufacturer/brand.

Respond ONLY with one of these exact classifications: "brand_page", "brand_category_page", or "other".
"""
        
        user_prompt = f"""URL: {url}
Title: {title}
Metadata: {metadata}
{page_type_hint}

Based on this information, classify this page as "brand_page", "brand_category_page", or "other".
"""
        
        # Call Claude API
        try:
            response = self._call_claude_api(system_prompt + user_prompt)
            
            # Extract page type from response
            page_type = response.strip().lower()
            
            # Validate page type
            valid_page_types = ['brand_page', 'brand_category_page', 'other']
            if page_type not in valid_page_types:
                # Try to extract the page type from the response
                for valid_type in valid_page_types:
                    if valid_type in page_type:
                        page_type = valid_type
                        break
                else:
                    self.logger.warning(f"Invalid page type from Claude API: {page_type}, marking as inconclusive")
                    page_type = 'inconclusive'
            
            self.logger.info(f"Metadata analysis result: {page_type}")
            
            return {
                'page_type': page_type,
                'confidence': 0.8 if page_type != 'inconclusive' else 0.5
            }
        except Exception as e:
            self.logger.error(f"Error analyzing metadata: {str(e)}")
            return {
                'page_type': 'inconclusive',
                'confidence': 0.0
            }
    
    def analyze_content(self, url: str, title: str, content: str) -> Dict:
        """
        Analyze truncated page content and headers.
        
        Args:
            url: Page URL
            title: Page title
            content: Page content
            
        Returns:
            Dictionary with analysis results
        """
        self.logger.info(f"Step 4: Content analysis for URL: {url}")
        
        # Extract main content (truncated)
        main_content = self._extract_main_content(content)
        
        # Extract key elements
        key_elements = self._extract_key_elements(content)
        
        # Prepare prompt
        system_prompt = """You are an AI assistant that analyzes web page content to determine if a page is related to a manufacturer/brand.
Your task is to classify the page into one of three categories:
1. Brand page: A page about a manufacturer/brand, typically the main page or about page.
2. Brand category page: A page listing products or categories from a specific manufacturer/brand.
3. Other: Not related to a manufacturer/brand.

Respond ONLY with one of these exact classifications: "brand_page", "brand_category_page", or "other".
"""
        
        user_prompt = f"""URL: {url}
Title: {title}
Content:
{main_content}
Key Elements:
{key_elements}

Based on this information, classify this page as "brand_page", "brand_category_page", or "other".
"""
        
        # Call Claude API
        try:
            response = self._call_claude_api(system_prompt + user_prompt)
            
            # Parse response
            page_type = response.strip().lower()
            
            # Validate page type
            valid_page_types = ['brand_page', 'brand_category_page', 'other']
            if page_type not in valid_page_types:
                # Try to extract the page type from the response
                for valid_type in valid_page_types:
                    if valid_type in page_type:
                        page_type = valid_type
                        break
                else:
                    self.logger.warning(f"Invalid page type from Claude API: {page_type}, marking as inconclusive")
                    page_type = 'inconclusive'
            
            self.logger.info(f"Content analysis result: {page_type}")
            
            return {
                'page_type': page_type,
                'confidence': 0.8 if page_type != 'inconclusive' else 0.5
            }
        except Exception as e:
            self.logger.error(f"Error analyzing content: {str(e)}")
            return {
                'page_type': 'inconclusive',
                'confidence': 0.0
            }
    
    def extract_manufacturer_data(self, url: str, title: str, content: str, page_type: str) -> Dict:
        """
        Extract manufacturer data from a page.
        
        Args:
            url: Page URL
            title: Page title
            content: Page content
            page_type: Page type ('brand_page' or 'brand_category_page')
            
        Returns:
            Dictionary containing manufacturer_name and categories
        """
        self.logger.info(f"Extracting manufacturer data for page type: {page_type}")
        
        # Extract manufacturer name
        manufacturer_name = self._extract_manufacturer_name(url, title)
        
        # Extract categories based on page type
        if page_type == 'brand_page':
            self.logger.info(f"Extracting manufacturer data for brand page: {url}")
            categories = self._extract_brand_categories(url, title, content)
        elif page_type == 'brand_category_page':
            self.logger.info(f"Extracting manufacturer data for brand category page: {url}")
            categories = self._extract_category_from_page(url, title, content)
        else:
            self.logger.warning(f"Unknown page type for manufacturer data extraction: {page_type}")
            categories = []
        
        # Ensure categories is a list, not a dictionary
        if isinstance(categories, dict) and 'categories' in categories:
            result = categories
            # Make sure manufacturer_name is set
            if 'manufacturer_name' not in result:
                result['manufacturer_name'] = manufacturer_name
        else:
            # Convert non-list to list if needed
            if not isinstance(categories, list):
                categories = [categories] if categories else []
                
            # Create result dictionary with expected keys
            result = {
                'manufacturer_name': manufacturer_name,
                'categories': categories,
                'url': url
            }
        
        self.logger.info(f"Extracted data: {result}")
        return result
    
    def analyze_page(self, url: str, title: str, content: str, metadata: str = None) -> Dict:
        """
        Analyze a page to determine if it's a manufacturer page and extract relevant data.
        
        This method follows a 3-step process:
        1. Keyword filtering (no Claude API call)
        2. Metadata analysis (Claude API call with title and metadata)
        3. Content analysis (Claude API call with truncated content)
        4. Category extraction (if applicable)
        5. Batch translation of categories (if enabled)
        
        Args:
            url: Page URL
            title: Page title
            content: Page content
            metadata: Optional page metadata (headers, meta tags)
            
        Returns:
            Dictionary with analysis results
        """
        self.logger.info(f"Analyzing page: {url}")
        
        # Update statistics
        self.stats['pages_analyzed'] += 1
        
        # Extract metadata if not provided
        if metadata is None:
            metadata = self._extract_metadata_from_content(content)
        
        # Step 1: Keyword filtering
        keyword_result = self.keyword_filter(url, title, content)
        
        # If keyword filter indicates this is definitely not a manufacturer page, skip further analysis
        if not keyword_result['passed']:
            self.logger.info(f"Page failed keyword filter: {url}")
            return {
                'is_manufacturer_page': False,
                'page_type': 'other',
                'categories': [],
                'analysis_method': 'keyword_filter'
            }
        
        # If keyword filter gives a definitive result, use it
        if keyword_result['page_type'] in ['brand_page', 'brand_category_page']:
            page_type = keyword_result['page_type']
            is_manufacturer_page = page_type == 'brand_page'
            is_category_page = page_type == 'brand_category_page'
            
            # For manufacturer pages, extract categories
            extracted_data = self.extract_manufacturer_data(url, title, content, page_type)
            
            # Translate categories in batch if enabled
            translated_categories = {}
            if self.translation_enabled and self.translation_manager and 'categories' in extracted_data:
                self.logger.info(f"Translating {len(extracted_data['categories'])} categories for {extracted_data.get('manufacturer_name', self._extract_manufacturer_name(url, title))}")
                translation_data = {'manufacturer_name': extracted_data.get('manufacturer_name', self._extract_manufacturer_name(url, title)), 'categories': extracted_data['categories'], 'url': url}
                translated_categories = self._batch_translate_categories(translation_data)
                self.logger.info(f"Translation completed. Results: {translated_categories}")
            
            # Build response with all extracted data
            result = {
                'is_manufacturer_page': is_manufacturer_page,
                'is_category_page': is_category_page,
                'page_type': page_type,
                'manufacturer_name': extracted_data.get('manufacturer_name', self._extract_manufacturer_name(url, title)),
                'categories': extracted_data.get('categories', []),
                'translated_categories': translated_categories,
                'analysis_method': 'keyword_filter'
            }
            
            # Add any additional data from extraction
            for key, value in extracted_data.items():
                if key not in ['categories', 'manufacturer_name']:
                    result[key] = value
            
            return result
            
        # Step 2: Metadata analysis
        try:
            metadata_result = self.analyze_metadata(url, title, metadata)
            
            # If metadata analysis gives a definitive result, use it
            if metadata_result['page_type'] != 'inconclusive':
                page_type = metadata_result['page_type']
                is_manufacturer_page = page_type == 'brand_page'
                is_category_page = page_type == 'brand_category_page'
                
                # For manufacturer pages, extract categories
                extracted_data = self.extract_manufacturer_data(url, title, content, page_type)
                
                # Translate categories in batch if enabled
                translated_categories = {}
                if self.translation_enabled and self.translation_manager and 'categories' in extracted_data:
                    self.logger.info(f"Translating {len(extracted_data['categories'])} categories for {extracted_data.get('manufacturer_name', self._extract_manufacturer_name(url, title))}")
                    translation_data = {'manufacturer_name': extracted_data.get('manufacturer_name', self._extract_manufacturer_name(url, title)), 'categories': extracted_data['categories'], 'url': url}
                    translated_categories = self._batch_translate_categories(translation_data)
                    self.logger.info(f"Translation completed. Results: {translated_categories}")
                
                # Build result
                result = {
                    'is_manufacturer_page': is_manufacturer_page,
                    'is_category_page': is_category_page,
                    'page_type': page_type,
                    'manufacturer_name': extracted_data.get('manufacturer_name', self._extract_manufacturer_name(url, title)),
                    'categories': extracted_data.get('categories', []),
                    'translated_categories': translated_categories,
                    'analysis_method': 'metadata_analysis'
                }
                
                # Add any additional data
                for key, value in extracted_data.items():
                    if key not in ['categories', 'manufacturer_name']:
                        result[key] = value
                
                return result
        except Exception as e:
            self.logger.error(f"Error in metadata analysis: {str(e)}")
        
        # Step 3: Content analysis
        try:
            content_result = self.analyze_content(url, title, content)
            
            page_type = content_result['page_type']
            is_manufacturer_page = page_type == 'brand_page'
            is_category_page = page_type == 'brand_category_page'
            
            # For manufacturer pages, extract categories
            extracted_data = self.extract_manufacturer_data(url, title, content, page_type)
            
            # Translate categories in batch if enabled
            translated_categories = {}
            if self.translation_enabled and self.translation_manager and 'categories' in extracted_data:
                self.logger.info(f"Translating {len(extracted_data['categories'])} categories for {extracted_data.get('manufacturer_name', self._extract_manufacturer_name(url, title))}")
                translation_data = {'manufacturer_name': extracted_data.get('manufacturer_name', self._extract_manufacturer_name(url, title)), 'categories': extracted_data['categories'], 'url': url}
                translated_categories = self._batch_translate_categories(translation_data)
                self.logger.info(f"Translation completed. Results: {translated_categories}")
            
            # Build result
            result = {
                'is_manufacturer_page': is_manufacturer_page,
                'is_category_page': is_category_page,
                'page_type': page_type,
                'manufacturer_name': extracted_data.get('manufacturer_name', self._extract_manufacturer_name(url, title)),
                'categories': extracted_data.get('categories', []),
                'translated_categories': translated_categories,
                'analysis_method': 'content_analysis'
            }
            
            # Add any additional data
            for key, value in extracted_data.items():
                if key not in ['categories', 'manufacturer_name']:
                    result[key] = value
            
            return result
        except Exception as e:
            self.logger.error(f"Error in content analysis: {str(e)}")
        
        # Default: not a manufacturer page
        self.logger.info(f"All analysis methods failed, marking as not a manufacturer page: {url}")
        return {
            'is_manufacturer_page': False,
            'is_category_page': False,
            'page_type': 'other',
            'categories': [],
            'translated_categories': {},
            'analysis_method': 'default'
        }
    
    def _batch_translate_categories(self, data: Dict) -> Dict[str, List[str]]:
        """
        Translate a batch of categories to all target languages.
        
        Args:
            data: Dictionary containing manufacturer_name, categories, and url
            
        Returns:
            Dictionary mapping language codes to lists of translated categories
        """
        translated_results = {}
        
        # Skip if translation is not enabled or no categories to translate
        if not self.translation_enabled:
            self.logger.warning("Translation skipped: not enabled")
            return translated_results
            
        if not self.translation_manager:
            self.logger.warning("Translation skipped: translation manager not initialized")
            return translated_results
            
        if not self.translation_manager.enabled:
            self.logger.warning("Translation skipped: translation manager is disabled")
            return translated_results
            
        if not self.translation_manager.api_key:
            self.logger.warning("Translation skipped: translation manager has no API key")
            return translated_results
            
        # Extract manufacturer name and category list
        manufacturer_name = data.get('manufacturer_name', '')
        categories = data.get('categories', [])
        url = data.get('url', '')
        
        if not manufacturer_name or not categories:
            self.logger.warning(f"Missing manufacturer name or categories for translation: {data}")
            return translated_results
            
        self.logger.debug(f"Categories to translate: {categories}")
        
        # Translate to each target language
        for lang_code, lang_name in self.target_languages.items():
            # Skip English if it's the source language
            if lang_code == 'en':
                translated_results[lang_code] = categories
                continue
                
            self.logger.info(f"Batch translating {len(categories)} categories to {lang_name}")
            
            try:
                # Use the translation_manager's translate_list method to translate all categories at once
                translated_categories = self.translation_manager.translate_list(categories, 'en', lang_code)
                
                if translated_categories:
                    self.logger.info(f"Successfully translated {len(categories)} categories to {lang_name}")
                    self.stats['categories_translated'] += len(translated_categories)
                    translated_results[lang_code] = translated_categories
                else:
                    self.logger.warning(f"No translations returned for {lang_name}")
                    translated_results[lang_code] = categories  # Use original as fallback
                
            except Exception as e:
                self.logger.error(f"Error batch translating to {lang_name}: {str(e)}")
                # Print the full exception traceback for debugging
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                # Use original categories as fallback
                translated_results[lang_code] = categories
        
        return translated_results
    
    def _extract_key_elements(self, content: str) -> str:
        """
        Extract key elements from HTML content that are likely to contain valuable information
        about manufacturers and product categories.
        
        Extracts:
        - Headers (h1, h2, h3)
        - List items from unordered and ordered lists (ul/ol)
        - Strong/bold text
        - Breadcrumb navigation
        - Menu items
        
        Args:
            content: HTML content
            
        Returns:
            Extracted key elements as a string
        """
        key_elements = []
        
        try:
            # Parse HTML content
            soup = BeautifulSoup(content, 'html.parser')
            
            # Extract headers (h1, h2, h3)
            for header in soup.find_all(['h1', 'h2', 'h3']):
                header_text = header.get_text(strip=True)
                if header_text:
                    key_elements.append(f"{header.name}: {header_text}")
            
            # Extract breadcrumb navigation
            breadcrumbs = []
            for breadcrumb in soup.find_all(class_=lambda c: c and ('breadcrumb' in c.lower() or 'bread-crumb' in c.lower())):
                for item in breadcrumb.find_all(['li', 'a', 'span']):
                    text = item.get_text(strip=True)
                    if text and text not in breadcrumbs and len(text) < 50:  # Avoid duplicates and long text
                        breadcrumbs.append(text)
            
            if breadcrumbs:
                key_elements.append("Breadcrumb: " + " > ".join(breadcrumbs))
            
            # Extract list items from unordered and ordered lists
            # Focus on short lists that are likely to be categories or menu items
            for list_tag in soup.find_all(['ul', 'ol']):
                # Skip very long lists (likely not category lists)
                list_items = list_tag.find_all('li')
                if len(list_items) > 15:
                    continue
                
                # Check if this list is likely a menu or category list
                list_text = list_tag.get_text(strip=True)
                if len(list_text) > 500:  # Skip long text lists
                    continue
                
                # Extract items
                items = []
                for item in list_items:
                    item_text = item.get_text(strip=True)
                    if item_text and len(item_text) < 50:  # Skip very long items
                        items.append(item_text)
                
                if items:
                    key_elements.append(f"List: {', '.join(items)}")
            
            # Extract strong/bold text that might indicate important categories or features
            for strong in soup.find_all(['strong', 'b']):
                # Skip if inside a header (already captured)
                if strong.find_parent(['h1', 'h2', 'h3']):
                    continue
                
                strong_text = strong.get_text(strip=True)
                if strong_text and len(strong_text) < 50:  # Skip very long text
                    key_elements.append(f"Strong: {strong_text}")
            
            # Extract menu items (common class names for menus)
            menu_classes = ['menu', 'nav', 'navigation', 'navbar', 'main-menu', 'categories']
            for menu_class in menu_classes:
                for menu in soup.find_all(class_=lambda c: c and menu_class in c.lower()):
                    menu_items = []
                    for item in menu.find_all('a'):
                        item_text = item.get_text(strip=True)
                        if item_text and len(item_text) < 30:  # Skip very long menu items
                            menu_items.append(item_text)
                    
                    if menu_items:
                        key_elements.append(f"Menu: {', '.join(menu_items[:10])}")  # Limit to 10 items
                        break  # Only get one menu of each type
            
            self.logger.debug(f"Extracted {len(key_elements)} key elements")
            
        except Exception as e:
            self.logger.error(f"Error extracting key elements: {str(e)}")
        
        return "\n".join(key_elements)

    def _extract_main_content(self, content: str, max_chars: int = 1500) -> str:
        """
        Extract the main content from HTML, removing headers, footers, navigation, etc.
        Truncates the content to the specified maximum number of characters.
        
        Args:
            content: HTML content
            max_chars: Maximum number of characters to return
            
        Returns:
            Extracted main content as a string
        """
        try:
            # Parse HTML content
            soup = BeautifulSoup(content, 'html.parser')
            
            # Remove script, style, nav, footer, header tags
            for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'iframe']):
                tag.decompose()
            
            # Get the text content
            text = soup.get_text(separator=' ', strip=True)
            
            # Truncate to max_chars
            if len(text) > max_chars:
                text = text[:max_chars] + "..."
            
            return text
            
        except Exception as e:
            self.logger.error(f"Error extracting main content: {str(e)}")
            return content[:max_chars] if content else ""

    def _extract_metadata_from_content(self, content: str) -> str:
        """
        Extract metadata from HTML content, including meta tags, headers, and title.
        
        Args:
            content: HTML content
            
        Returns:
            Extracted metadata as a string
        """
        metadata = []
        
        try:
            # Parse HTML content
            soup = BeautifulSoup(content, 'html.parser')
            
            # Extract title
            title = soup.title.string if soup.title else ""
            if title:
                metadata.append(f"Title: {title.strip()}")
            
            # Extract meta tags
            for meta in soup.find_all('meta'):
                name = meta.get('name', meta.get('property', ''))
                content = meta.get('content', '')
                if name and content:
                    metadata.append(f"Meta {name}: {content}")
            
            # Extract key elements
            key_elements = self._extract_key_elements(content)
            if key_elements:
                metadata.append(f"Key Elements:\n{key_elements}")
            
            # Extract Open Graph tags (often contain valuable information)
            for meta in soup.find_all('meta', property=lambda x: x and x.startswith('og:')):
                content = meta.get('content', '')
                if content:
                    property_name = meta.get('property', '')
                    metadata.append(f"OG {property_name}: {content}")
            
            self.logger.debug(f"Extracted {len(metadata)} metadata items")
            
        except Exception as e:
            self.logger.error(f"Error extracting metadata: {str(e)}")
        
        return "\n".join(metadata)

    def get_statistics(self) -> Dict:
        """
        Get statistics about Claude Analyzer usage.
        
        Returns:
            Dictionary with usage statistics
        """
        return self.stats
    
    def reset_statistics(self) -> None:
        """Reset usage statistics."""
        self.stats = {
            'pages_analyzed': 0,
            'manufacturer_pages_found': 0,
            'categories_found': 0,
            'categories_translated': 0,
            'claude_api_calls': 0,
            'claude_api_prompt_tokens': 0,
            'claude_api_completion_tokens': 0,
            'claude_api_total_tokens': 0,
            'claude_api_errors': 0
        }
        self.logger.info("Statistics reset")

    def _extract_manufacturer_name(self, url: str, title: str) -> str:
        """
        Extract manufacturer name from URL or title.
        
        Args:
            url: Page URL
            title: Page title
            
        Returns:
            Manufacturer name
        """
        manufacturer_name = None
        
        # Check for common patterns in URL
        url_parts = url.split('/')
        for i, part in enumerate(url_parts):
            if part.lower() in ['brand', 'brands', 'manufacturer', 'manufacturers'] and i+1 < len(url_parts):
                manufacturer_name = url_parts[i+1].replace('-', ' ').replace('_', ' ').title()
                break
        
        # If no manufacturer name found in URL, try to extract from title
        if not manufacturer_name:
            # Look for patterns like "Brand Name - Categories" or "Categories - Brand Name"
            title_parts = title.split('-')
            if len(title_parts) > 1:
                # Assume the brand name is either the first or last part
                manufacturer_name = title_parts[0].strip()
        
        return manufacturer_name

    def _extract_brand_categories(self, url: str, title: str, content: str) -> List[str]:
        """
        Extract categories from a brand page.
        
        Args:
            url: Page URL
            title: Page title
            content: Page content
            
        Returns:
            List of categories
        """
        self.logger.info(f"Extracting categories from brand page: {url}")
        
        # Extract key elements from the content
        key_elements = self._extract_key_elements(content)
        
        # Extract main content (truncated)
        main_content = self._extract_main_content(content, max_chars=1500)
        
        # Extract manufacturer name
        manufacturer_name = self._extract_manufacturer_name(url, title)
        
        # Prepare prompt for Claude API
        system_prompt = """You are an expert in analyzing manufacturer websites and extracting product categories.
Your task is to identify all product categories from a manufacturer's website.
Respond with ONLY a list of product categories, one per line, with no additional text or explanation.
IMPORTANT: Always prefix each category with the manufacturer name (e.g., "Samsung TVs" not just "TVs").
Focus on main product categories, not individual products or subcategories.
"""
        
        user_prompt = f"""URL: {url}
Title: {title}

Key Elements:
{key_elements}

Truncated Content:
{main_content}

Extract all product categories from this manufacturer page. 
The manufacturer name is: {manufacturer_name}
List ONLY the main product categories, one per line, with the manufacturer name as prefix.
Example format: "{manufacturer_name} Laptops" not just "Laptops"
"""
        
        # Call Claude API
        try:
            response = self._call_claude_api(system_prompt + user_prompt)
            
            # Parse categories - one per line
            categories = []
            if response:
                for line in response.strip().split('\n'):
                    category = line.strip()
                    if category:
                        categories.append(category)
            
            # Update statistics
            self.stats['categories_found'] += len(categories)
            
            return categories
            
        except Exception as e:
            self.logger.error(f"Error extracting categories from brand page: {str(e)}")
            return []
            
    def _extract_category_from_page(self, url: str, title: str, content: str) -> List[str]:
        """
        Extract the main category from a category page.
        
        Args:
            url: Page URL
            title: Page title
            content: Page content
            
        Returns:
            List of categories (should be a single category)
        """
        self.logger.info(f"Extracting category from category page: {url}")
        
        # Extract key elements
        key_elements = self._extract_key_elements(content)
        
        # Extract manufacturer name
        manufacturer_name = self._extract_manufacturer_name(url, title)
        
        # Prepare prompt for Claude API
        system_prompt = """You are an expert in analyzing manufacturer websites and extracting product categories.
Your task is to identify the main product category from this specific category page.
Respond with ONLY the category name, nothing else.
"""
        
        user_prompt = f"""URL: {url}
Title: {title}

Key Elements:
{key_elements}

Based on this information, what is the SINGLE main product category represented on this page?
Respond with ONLY the category name, no explanations or additional text.
"""
        
        # Call Claude API
        try:
            response = self._call_claude_api(system_prompt + user_prompt)
            
            # Clean up response
            category = response.strip() if response else None
            
            # Format with manufacturer name prefix
            if category and manufacturer_name:
                category = f"{manufacturer_name} {category}"
                
            categories = [category] if category else []
            
            self.logger.info(f"Extracted category: {category}")
            
            return categories
            
        except Exception as e:
            self.logger.error(f"Error extracting category from category page: {str(e)}")
            return []
