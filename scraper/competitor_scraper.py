#!/usr/bin/env python3
"""
Competitor Scraper for NDAIVI

This scraper crawls competitor websites to extract manufacturer and product category information,
utilizing Claude AI for content analysis and data extraction.

Improvements:
- Enhanced error handling and logging
- Better AI prompt engineering
- Optimized crawling strategy with priority queue
- Improved database session management
- Secure API key handling
- Comprehensive statistics tracking
"""

import requests
import yaml
import logging
import time
import os
import uuid
import heapq
from urllib.parse import urlparse, urljoin
import datetime
import signal
import threading
from typing import Optional, List, Dict, Any, Tuple, Set
from collections import defaultdict
from bs4 import BeautifulSoup
from sqlalchemy import func, desc
from sqlalchemy.orm import Session
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import anthropic
from anthropic import AnthropicError

# Add the project root to the Python path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_manager import get_db_manager
from database.schema import init_db, Category, Manufacturer, CrawlStatus, ScraperLog, ScraperSession
from database.statistics_manager import StatisticsManager
import json

# Global shutdown and suspension flags for signal handling
_SHUTDOWN_REQUESTED = False
_SUSPEND_REQUESTED = False

def global_shutdown_handler(sig, frame):
    """Global signal handler for graceful shutdown."""
    global _SHUTDOWN_REQUESTED
    _SHUTDOWN_REQUESTED = True
    logging.getLogger('competitor_scraper').info(f"Received shutdown signal {sig}, finishing gracefully...")

def global_suspend_handler(sig, frame):
    """Global signal handler for process suspension (CTRL+Z)."""
    global _SUSPEND_REQUESTED
    _SUSPEND_REQUESTED = True
    logging.getLogger('competitor_scraper').info(f"Received suspension signal {sig}, marking session as interrupted...")
    signal.signal(signal.SIGTSTP, signal.SIG_DFL)
    os.kill(os.getpid(), signal.SIGTSTP)

# Register global signal handlers
signal.signal(signal.SIGINT, global_shutdown_handler)  # Ctrl+C
signal.signal(signal.SIGTERM, global_shutdown_handler)  # kill command
try:
    signal.signal(signal.SIGTSTP, global_suspend_handler)  # Ctrl+Z
except AttributeError:
    # SIGTSTP not available on Windows
    pass


class UrlPriority:
    """
    Class for determining URL crawling priority based on various factors.
    
    This helps optimize the crawling order to find manufacturer information faster.
    """
    
    # Keyword weights for prioritization
    PRIORITY_KEYWORDS = {
        'manufacturer': 10,
        'brand': 8,
        'vendor': 8,
        'supplier': 7,
        'partner': 6,
        'directory': 6,
        'catalog': 5,
        'product': 4,
        'category': 3
    }
    
    @staticmethod
    def calculate_priority(url: str, depth: int, content_hint: Optional[str] = None) -> float:
        """
        Calculate a crawling priority score for a URL.
        
        Lower scores indicate higher priority.
        
        Args:
            url: The URL to calculate priority for
            depth: The link depth from the starting point
            content_hint: Optional title or other content hint from the page
            
        Returns:
            A priority score (lower is higher priority)
        """
        # Base priority based on depth
        priority = depth * 10.0
        
        # Apply keyword boosts to lower the priority score (higher priority)
        url_lower = url.lower()
        for keyword, boost in UrlPriority.PRIORITY_KEYWORDS.items():
            if keyword in url_lower:
                priority -= boost
        
        # Apply content hint boosts if available
        if content_hint and isinstance(content_hint, str):
            content_lower = content_hint.lower()
            for keyword, boost in UrlPriority.PRIORITY_KEYWORDS.items():
                if keyword in content_lower:
                    priority -= boost * 0.5  # Apply half boost for content hints
        
        # Limit minimum priority value
        return max(1.0, priority)


class PriorityUrlQueue:
    """
    Priority queue for URLs to be crawled.
    
    This optimizes the crawling order to find manufacturer information faster.
    """
    
    def __init__(self):
        """Initialize the priority queue."""
        self._queue = []  # heapq priority queue with entries: (priority, counter, url, depth)
        self._counter = 0  # Unique counter for stable sorting of same-priority items
        self._url_set = set()  # Fast lookup for checking if URLs are in queue
    
    def push(self, url: str, depth: int, priority: Optional[float] = None, content_hint: Optional[str] = None):
        """
        Add a URL to the queue with calculated priority.
        
        Args:
            url: The URL to add
            depth: Current crawl depth
            priority: Optional explicit priority (lower is higher priority)
            content_hint: Optional content hint for priority calculation
        
        Returns:
            True if URL was added, False if it was already in the queue
        """
        if url in self._url_set:
            return False
        
        if priority is None:
            # Calculate priority based on URL characteristics and depth
            priority = UrlPriority.calculate_priority(url, depth, content_hint)
        
        # Use a counter to ensure stable ordering for same-priority items
        entry = (priority, self._counter, url, depth)
        self._counter += 1
        
        heapq.heappush(self._queue, entry)
        self._url_set.add(url)
        return True
    
    def pop(self) -> Tuple[str, int]:
        """
        Get the highest priority URL from the queue.
        
        Returns:
            Tuple of (url, depth) for the highest priority URL
        
        Raises:
            IndexError: If the queue is empty
        """
        if not self._queue:
            raise IndexError("Priority queue is empty")
        
        # Get and return the highest priority (lowest score) URL
        _, _, url, depth = heapq.heappop(self._queue)
        self._url_set.remove(url)
        return url, depth
    
    def __len__(self) -> int:
        """Get the number of URLs in the queue."""
        return len(self._queue)
    
    def __contains__(self, url: str) -> bool:
        """Check if a URL is in the queue."""
        return url in self._url_set


class CompetitorScraper:
    """Web scraper for extracting manufacturer information from competitor websites."""
    
    def __init__(self, config_path='config.yaml'):
        """
        Initialize the competitor scraper.
        
        Args:
            config_path: Path to the configuration YAML file
        """
        # Setup logging first
        self._setup_logging()
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Initialize database with singleton database manager
        self._initialize_database()
        
        # Initialize AI client
        self._setup_ai_clients()
        
        # Set up crawling variables
        self.visited_urls = self._load_visited_urls()
        self.url_queue = PriorityUrlQueue()
        self.current_depth = 0
        
        # Throttling settings
        self.last_request_time = 0
        self.min_request_interval = self.config['scraper'].get('min_request_interval', 0.5)
        
        # Session ID for tracing
        self.session_uuid = str(uuid.uuid4())
        
        # Initialize StatisticsManager
        self.statistics_manager = StatisticsManager('scraper', self.db_manager)
        self.session_id = self.statistics_manager.session_id
        
        # Initialize control flags
        self.shutdown_requested = False
        self.suspend_handled = False
        
        self.logger.info(f"Competitor Scraper initialized successfully (session {self.session_uuid})")

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to the configuration file
            
        Returns:
            Dictionary containing the configuration
        """
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                # Only log if logger is already initialized
                if hasattr(self, 'logger'):
                    self.logger.info(f"Configuration loaded from {config_path}")
                return config
        except Exception as e:
            error_msg = f"Failed to load configuration from {config_path}: {str(e)}"
            if hasattr(self, 'logger'):
                self.logger.error(error_msg)
            raise ValueError(error_msg)
    
    def _setup_logging(self):
        """Set up logging configuration."""
        log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'competitor_scraper.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
        )
        self.logger = logging.getLogger('competitor_scraper')
    
    def _initialize_database(self):
        """Initialize the database connection."""
        try:
            db_path = self.config['database']['path']
            db_dir = os.path.dirname(db_path)
            
            # Create database directory if it doesn't exist
            if not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
                self.logger.info(f"Created database directory {db_dir}")
            
            # Initialize database if it doesn't exist
            if not os.path.exists(db_path):
                self.logger.info(f"Database not found. Initializing database at {db_path}")
                config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')
                init_db(db_path, config_path=config_path)
                self.logger.info("Database initialized successfully")
            
            # Get database manager singleton
            self.db_manager = get_db_manager(db_path)
            if not hasattr(self.db_manager, '_initialized') or not self.db_manager._initialized:
                self.db_manager.initialize(db_path)
            
            self.logger.info(f"Database manager initialized for {db_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database manager: {str(e)}")
            raise
    
    def _setup_ai_clients(self):
        """Initialize Anthropic API client."""
        anthropic_config = self.config['ai_apis']['anthropic']
        
        # Get API key securely from environment or config
        self.anthropic_api_key = os.environ.get('ANTHROPIC_API_KEY', anthropic_config.get('api_key'))
        if not self.anthropic_api_key:
            self.logger.error("No Anthropic API key found in environment or config")
            raise ValueError(
                "Anthropic API key is required. Set ANTHROPIC_API_KEY environment variable or in config.yaml"
            )
        
        # Get model names from config
        self.anthropic_model = anthropic_config.get('model', 'claude-3-haiku-20240307')
        self.anthropic_sonnet_model = anthropic_config.get('sonnet_model', 'claude-3-sonnet-20240229')
        
        # Initialize client
        self.anthropic_client = anthropic.Anthropic(api_key=self.anthropic_api_key)
        
        self.logger.info("Anthropic API configuration loaded successfully")
    
    def _log_to_db(self, level: str, message: str):
        """
        Log message to database.
        
        Args:
            level: Log level (INFO, WARNING, ERROR)
            message: Log message text
        """
        # Truncate long messages to avoid database issues
        if len(message) > 2000:
            message = message[:1997] + '...'
        
        try:
            with self.db_manager.session() as session:
                log_entry = ScraperLog(
                    timestamp=datetime.datetime.now(),
                    level=level,
                    message=message
                )
                session.add(log_entry)
                session.commit()
        except Exception as e:
            self.logger.warning(f"Failed to log to database: {str(e)}")
    
    def _load_visited_urls(self) -> Set[str]:
        """
        Load previously visited URLs from the database.
        
        Returns:
            Set of visited URL strings
        """
        visited_urls = set()
        try:
            with self.db_manager.session() as session:
                visited_records = session.query(CrawlStatus).filter_by(visited=True).all()
                for record in visited_records:
                    visited_urls.add(record.url)
            self.logger.info(f"Loaded {len(visited_urls)} previously visited URLs from database")
        except Exception as e:
            self.logger.error(f"Error loading visited URLs: {str(e)}")
        
        return visited_urls
    
    def _is_excluded_url(self, url: str) -> bool:
        """
        Check if a URL should be excluded from crawling based on patterns.
        
        Args:
            url: URL to check
            
        Returns:
            True if the URL should be excluded, False otherwise
        """
        parsed_url = urlparse(url)
        target_domain = urlparse(self.config['target_url']).netloc
        
        # Check if the domain is different from the target domain
        if parsed_url.netloc and parsed_url.netloc != target_domain:
            return True
        
        # Check for file extensions to exclude
        path = parsed_url.path.lower()
        excluded_extensions = [
            '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.css', '.js', '.xml',
            '.zip', '.rar', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx'
        ]
        if any(path.endswith(ext) for ext in excluded_extensions):
            return True
        
        # Check for negative keywords from config
        negative_keywords = self.config.get('keywords', {}).get('negative', [])
        if any(keyword in url.lower() for keyword in negative_keywords):
            return True
        
        # Check for common paths to exclude
        common_exclusions = [
            '/search', '/login', '/register', '/cart', '/checkout', '/account',
            '/privacy', '/terms', '/contact', '/about', '/faq', '/help',
            'javascript:', 'mailto:', 'tel:', '#', '?'
        ]
        if any(pattern in url.lower() for pattern in common_exclusions):
            return True
        
        return False
    
    def _check_url_in_database(self, url: str) -> Tuple[bool, bool]:
        """
        Check if a URL exists in the database and if it has been visited.
        
        Args:
            url: URL to check
            
        Returns:
            Tuple of (exists_in_db, has_been_visited)
        """
        try:
            with self.db_manager.session() as session:
                status = session.query(CrawlStatus).filter(CrawlStatus.url == url).first()
                if status:
                    return True, status.visited
                return False, False
        except Exception as e:
            self.logger.error(f"Error checking URL in database: {str(e)}")
            return False, False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10),
          retry=retry_if_exception_type(requests.RequestException))
    def _fetch_url(self, url: str) -> Optional[str]:
        """
        Fetch URL content with retries and error handling.
        
        Args:
            url: URL to fetch
            
        Returns:
            HTML content as string, or None if the fetch failed
        """
        # Get configuration values
        max_retries = self.config['scraper'].get('max_retries', 3)
        retry_delay = self.config['scraper'].get('retry_delay', 1)
        timeout = self.config['scraper'].get('request_timeout', 30)
        user_agent = self.config['crawling'].get('user_agent', 'NDAIVI Scraper')
        
        # Set up headers
        headers = {'User-Agent': user_agent}
        
        # Respect rate limiting
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        
        # Retry loop
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    time.sleep(retry_delay * attempt)  # Progressive delay
                
                # Record request time for rate limiting
                self.last_request_time = time.time()
                
                # Make the request
                response = requests.get(url, headers=headers, timeout=timeout)
                
                if response.status_code == 200:
                    self.statistics_manager.increment_stat('urls_processed')
                    return response.text
                else:
                    self.logger.warning(f"HTTP error {response.status_code} for URL: {url}")
                    self.statistics_manager.increment_stat('http_errors')
            except requests.exceptions.ConnectionError as e:
                self.logger.warning(f"Connection error for URL {url}: {str(e)}")
                self.statistics_manager.increment_stat('connection_errors')
            except requests.exceptions.Timeout as e:
                self.logger.warning(f"Timeout error for URL {url}: {str(e)}")
                self.statistics_manager.increment_stat('timeout_errors')
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request error for URL {url}: {str(e)}")
            
            if attempt < max_retries:
                self.logger.info(f"Retrying URL {url} (attempt {attempt+1}/{max_retries})")
            else:
                self.logger.error(f"Failed to fetch URL {url} after {max_retries} retries")
        
        return None
    
    def _extract_text_content(self, soup: BeautifulSoup) -> str:
        """
        Extract readable text content from the page.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Cleaned text content as string
        """
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.extract()
        
        # Get text and clean it
        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        
        return '\n'.join(chunk for chunk in chunks if chunk)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10),
           retry=retry_if_exception_type(AnthropicError))
    def _analyze_with_anthropic(self, url: str, title: str, content: str) -> bool:
        """
        Use Claude Haiku to determine if the page is a manufacturer page.
        
        Args:
            url: URL of the page
            title: Page title
            content: Page text content
            
        Returns:
            True if the page is a manufacturer page, False otherwise
        """
        try:
            # Truncate content to avoid token limits
            truncated_content = content[:2000] + "..." if len(content) > 2000 else content
            
            # Craft an improved prompt for more reliable analysis
            prompt = (
                f"Task: Analyze the following webpage content to determine if it lists or describes manufacturers "
                f"or their products.\n\n"
                f"URL: {url}\n"
                f"Title: {title}\n\n"
                f"Content excerpt:\n{truncated_content}\n\n"
                f"A manufacturer page typically lists company names that make products, possibly with product categories, "
                f"brands, or industry information. Look for lists, tables, or descriptions of companies that produce "
                f"or supply products.\n\n"
                f"Respond with ONLY 'yes' if this is a manufacturer page, or 'no' otherwise."
            )
            
            response = self.anthropic_client.messages.create(
                model=self.anthropic_model,
                max_tokens=10,
                messages=[{"role": "user", "content": prompt}]
            )
            
            result = response.content[0].text.strip().lower()
            
            if result not in ['yes', 'no']:
                self.logger.warning(f"Unexpected Claude Haiku response for {url}: {result}, treating as 'no'")
                return False
            
            self.logger.info(f"Claude Haiku analysis for {url}: {result}")
            self.statistics_manager.increment_stat('ai_queries')
            return result == 'yes'
        
        except AnthropicError as e:
            self.logger.error(f"Anthropic API error for {url}: {str(e)}")
            self._log_to_db("ERROR", f"Anthropic API error: {str(e)}")
            self.statistics_manager.increment_stat('ai_errors')
            raise
        
        except Exception as e:
            self.logger.error(f"Error analyzing with Claude Haiku for {url}: {str(e)}")
            self._log_to_db("ERROR", f"Error analyzing with Claude Haiku: {str(e)}")
            self.statistics_manager.increment_stat('ai_errors')
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10),
           retry=retry_if_exception_type(AnthropicError))
    def _analyze_with_claude_sonnet(self, url: str, title: str, content: str) -> List[Dict[str, Any]]:
        """
        Use Claude Sonnet to extract manufacturer and category information.
        
        Args:
            url: URL of the page
            title: Page title
            content: Page text content
            
        Returns:
            List of dictionaries with manufacturer and category information
        """
        try:
            # Truncate content to avoid token limits
            truncated_content = content[:4000] + "..." if len(content) > 4000 else content
            
            # Craft an improved prompt for more reliable extraction
            prompt = (
                f"Task: Extract manufacturer names and associated product categories from the following webpage content.\n\n"
                f"URL: {url}\n"
                f"Title: {title}\n\n"
                f"Content excerpt:\n{truncated_content}\n\n"
                f"Instructions:\n"
                f"1. Identify company names that manufacture or supply products\n"
                f"2. For each manufacturer, identify their product categories\n"
                f"3. Each category name must include the manufacturer name as part of it\n"
                f"4. Return the data as a JSON array with this exact structure:\n"
                f"[\n"
                f"  {{\n"
                f"    \"manufacturer\": \"CompanyName\",\n"
                f"    \"categories\": [\"CompanyName Product Category 1\", \"CompanyName Product Category 2\"]\n"
                f"  }}\n"
                f"]\n\n"
                f"If no manufacturers are found, return an empty array: []\n"
                f"Your response must be valid JSON only, with no additional text."
            )
            
            response = self.anthropic_client.messages.create(
                model=self.anthropic_sonnet_model,
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            result_text = response.content[0].text.strip()
            
            # Extract JSON from the response if needed
            if not result_text.startswith('['):
                # Try to find JSON array in the response
                import re
                json_match = re.search(r'\[(.*?)\]', result_text, re.DOTALL)
                if json_match:
                    result_text = json_match.group(0)
                else:
                    self.logger.warning(f"Invalid JSON response format for {url}: {result_text}")
                    return []
            
            # Parse the JSON response
            try:
                result = json.loads(result_text)
                if not isinstance(result, list):
                    self.logger.warning(f"Invalid response type for {url}: {type(result)}, expected list")
                    return []
                
                self.logger.info(f"Claude Sonnet extracted {len(result)} manufacturers from {url}")
                self.statistics_manager.increment_stat('ai_extractions')
                return result
            
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON parsing error for {url}: {str(e)}\nResponse: {result_text}")
                return []
        
        except AnthropicError as e:
            self.logger.error(f"Anthropic API error for {url}: {str(e)}")
            self._log_to_db("ERROR", f"Anthropic API error: {str(e)}")
            self.statistics_manager.increment_stat('ai_errors')
            raise
        
        except Exception as e:
            self.logger.error(f"Error analyzing with Claude Sonnet for {url}: {str(e)}")
            self._log_to_db("ERROR", f"Error analyzing with Claude Sonnet: {str(e)}")
            self.statistics_manager.increment_stat('ai_errors')
            return []
    
    def _translate_category(self, category: str, manufacturer_name: str) -> str:
        """
        Translate a category name to the target language, preserving manufacturer name.
        
        Args:
            category: Original category name
            manufacturer_name: Manufacturer name to preserve
            
        Returns:
            Translated category name
        """
        target_lang = list(self.config['languages']['targets'].keys())[0]  # Use first target language
        if target_lang == self.config['languages']['source']:
            return category
        
        try:
            # Detect if already in target language
            spanish_indicators = ['de', 'para', 'con', 'del', 'y', 'las', 'los']
            if any(word in category.lower().split() for word in spanish_indicators):
                self.logger.info(f"Category '{category}' appears to be in Spanish, skipping translation")
                return category
            
            # Create a prompt that ensures manufacturer name preservation
            prompt = (
                f"Translate this product category from English to {target_lang}: \"{category}\"\n"
                f"Important rules:\n"
                f"1. Preserve the manufacturer name '{manufacturer_name}' exactly as-is\n"
                f"2. Only translate descriptive words\n"
                f"3. Keep proper nouns unchanged\n"
                f"4. Return only the translated category text"
            )
            
            response = self.anthropic_client.messages.create(
                model=self.anthropic_model,
                max_tokens=100,
                messages=[{"role": "user", "content": prompt}]
            )
            
            translated = response.content[0].text.strip()
            
            # Verify manufacturer name is preserved
            if manufacturer_name.lower() in translated.lower():
                self.logger.info(f"Translated '{category}' to '{translated}'")
                self.statistics_manager.increment_stat('translations')
                return translated
            else:
                self.logger.warning(f"Translation lost manufacturer name: '{translated}'. Using original.")
                return category
        
        except Exception as e:
            self.logger.error(f"Error translating category '{category}': {str(e)}")
            return category
    
    def _process_extracted_data(self, response_data: List[Dict[str, Any]], source_url: str):
        """
        Process and save extracted manufacturer and category data.
        
        Args:
            response_data: List of dictionaries with manufacturer and category data
            source_url: Source URL where the data was extracted from
        """
        if not response_data:
            self.logger.info(f"No manufacturers found in {source_url}")
            return
        
        manufacturers_added = 0
        categories_added = 0
        
        try:
            with self.db_manager.session() as session:
                for item in response_data:
                    manufacturer_name = item.get('manufacturer')
                    if not manufacturer_name:
                        self.logger.warning(f"Missing manufacturer name in data: {item}")
                        continue
                    
                    manufacturer_name = manufacturer_name.strip()
                    categories = item.get('categories', [])
                    if not categories:
                        self.logger.warning(f"No categories found for manufacturer {manufacturer_name}")
                        continue
                    
                    # Check for existing manufacturer
                    manufacturer = session.query(Manufacturer).filter(
                        func.lower(Manufacturer.name) == func.lower(manufacturer_name)
                    ).first()
                    
                    if not manufacturer:
                        manufacturer = Manufacturer(name=manufacturer_name)
                        session.add(manufacturer)
                        manufacturers_added += 1
                        self.statistics_manager.increment_stat('manufacturers_extracted')
                        self.logger.info(f"Added new manufacturer: {manufacturer_name}")
                    else:
                        self.logger.debug(f"Manufacturer {manufacturer_name} already exists")
                    
                    # Process categories
                    categories_processed = 0
                    for category_name in categories:
                        # Validate category contains manufacturer name
                        if manufacturer_name.lower() not in category_name.lower():
                            self.logger.warning(f"Skipping category '{category_name}' - missing manufacturer name")
                            continue
                        
                        # Translate category if needed
                        translated_category = self._translate_category(category_name, manufacturer_name)
                        
                        # Check for existing category
                        category = session.query(Category).filter(
                            func.lower(Category.name) == func.lower(translated_category)
                        ).first()
                        
                        if not category:
                            category = Category(name=translated_category)
                            session.add(category)
                            categories_added += 1
                            self.statistics_manager.increment_stat('categories_extracted')
                            self.logger.debug(f"Added new category: {translated_category}")
                        
                        # Associate category with manufacturer if not already associated
                        if category not in manufacturer.categories:
                            manufacturer.categories.append(category)
                            categories_processed += 1
                            self.logger.debug(f"Associated '{translated_category}' with '{manufacturer_name}'")
                    
                    # Set website if applicable
                    manufacturer_website = item.get('website')
                    if manufacturer_website:
                        # Don't set internal links as manufacturer websites
                        target_domain = urlparse(self.config['target_url']).netloc
                        website_domain = urlparse(manufacturer_website).netloc
                        if target_domain in website_domain:
                            manufacturer_website = None
                    
                    # If not explicitly provided, check if source URL might be manufacturer website
                    if not manufacturer_website:
                        source_domain = urlparse(source_url).netloc
                        target_domain = urlparse(self.config['target_url']).netloc
                        if manufacturer_name.lower() in source_domain.lower() and source_domain != target_domain:
                            manufacturer_website = source_url
                    
                    # Update manufacturer website if we found a valid one
                    if manufacturer_website and not manufacturer.website:
                        manufacturer.website = manufacturer_website
                        self.statistics_manager.increment_stat('websites_found')
                        self.logger.info(f"Set website for '{manufacturer_name}': {manufacturer_website}")
                
                # Commit changes to database
                session.commit()
            
            self.logger.info(
                f"Processed {len(response_data)} manufacturers from {source_url} "
                f"(Added: {manufacturers_added} manufacturers, {categories_added} categories)"
            )
            self._log_to_db(
                "INFO", 
                f"Saved {manufacturers_added} manufacturers and {categories_added} categories from {source_url}"
            )
        
        except Exception as e:
            self.logger.error(f"Error processing extracted data for {source_url}: {str(e)}")
            self._log_to_db("ERROR", f"Error processing extracted data: {str(e)}")
    
    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> Dict[str, str]:
        """
        Extract links from a BeautifulSoup object.
        
        Args:
            soup: BeautifulSoup object of the page
            base_url: Base URL for resolving relative links
            
        Returns:
            Dictionary mapping URLs to their anchor text
        """
        extracted_links = {}
        try:
            for link in soup.find_all('a', href=True):
                href = link['href']
                anchor_text = link.get_text().strip()
                
                # Skip empty or javascript links
                if not href or href.startswith('javascript:') or href == '#':
                    continue
                
                # Resolve relative URLs
                absolute_url = urljoin(base_url, href) if not href.startswith(('http://', 'https://')) else href
                
                # Clean URL (remove fragments)
                parsed_url = urlparse(absolute_url)
                clean_url = parsed_url._replace(fragment='').geturl()
                
                # Check domain restrictions
                stay_within_domain = self.config['crawling'].get('stay_within_domain', False)
                if stay_within_domain and not self._is_same_domain(clean_url, base_url):
                    continue
                
                # Check exclusion patterns
                if self._is_excluded_url(clean_url):
                    continue
                
                # Add to extracted links dict
                if clean_url not in extracted_links:
                    extracted_links[clean_url] = anchor_text
            
            self.logger.debug(f"Extracted {len(extracted_links)} links from {base_url}")
            return extracted_links
        
        except Exception as e:
            self.logger.error(f"Error extracting links from {base_url}: {str(e)}")
            return {}
    
    def _is_same_domain(self, url1: str, url2: str) -> bool:
        """
        Check if two URLs belong to the same domain.
        
        Args:
            url1: First URL
            url2: Second URL
            
        Returns:
            True if both URLs have the same domain, False otherwise
        """
        return urlparse(url1).netloc == urlparse(url2).netloc
    
    def check_shutdown_requested(self) -> bool:
        """
        Check if shutdown or suspension was requested via signal handler.
        
        Returns:
            True if shutdown was requested, False otherwise
        """
        global _SHUTDOWN_REQUESTED, _SUSPEND_REQUESTED
        
        # Handle suspension request
        if _SUSPEND_REQUESTED and not self.suspend_handled:
            self.suspend_handled = True
            self.logger.info("Suspension requested via CTRL+Z, marking session as interrupted...")
            self._log_to_db("INFO", "Process suspension requested via CTRL+Z")
            
            if self.session_id:
                try:
                    with self.db_manager.session() as session:
                        scraper_session = session.query(ScraperSession).filter_by(id=self.session_id).first()
                        if scraper_session and scraper_session.status == 'running':
                            scraper_session.status = 'interrupted'
                            scraper_session.end_time = datetime.datetime.now()
                            session.commit()
                            self.logger.info(f"Marked session {self.session_id} as interrupted due to CTRL+Z")
                except Exception as e:
                    self.logger.error(f"Error updating session status during suspension: {str(e)}")
        
        # Handle shutdown request
        if _SHUTDOWN_REQUESTED and not self.shutdown_requested:
            self.shutdown_requested = True
            self.logger.info("Shutdown requested via signal, finishing gracefully...")
            self._log_to_db("INFO", "Shutdown requested via signal")
            self.close()
        
        return self.shutdown_requested
    
    def _mark_url_visited(self, url: str, is_manufacturer: bool = False, depth: Optional[int] = None) -> bool:
        """
        Mark a URL as visited in the database and in-memory set.
        
        Args:
            url: URL to mark as visited
            is_manufacturer: Whether the URL is a manufacturer page
            depth: Crawl depth of the URL
            
        Returns:
            True if the operation was successful, False otherwise
        """
        if url not in self.visited_urls:
            self.visited_urls.add(url)
        
        try:
            with self.db_manager.session() as session:
                status = session.query(CrawlStatus).filter(CrawlStatus.url == url).first()
                current_time = datetime.datetime.now()
                
                if status:
                    # Update existing record
                    status.visited = True
                    status.last_visited = current_time
                    status.is_manufacturer_page = is_manufacturer
                    if depth is not None and (status.depth is None or depth < status.depth):
                        status.depth = depth
                    self.logger.debug(f"Updated existing URL in database as visited: {url}")
                else:
                    # Create new record
                    url_depth = depth if depth is not None else 0
                    status = CrawlStatus(
                        url=url,
                        visited=True,
                        depth=url_depth,
                        last_visited=current_time,
                        is_manufacturer_page=is_manufacturer
                    )
                    session.add(status)
                    self.logger.debug(f"Added new URL to database as visited: {url} with depth {url_depth}")
                
                session.commit()
                return True
        
        except Exception as e:
            self.logger.error(f"Error marking URL as visited in database: {url}, Error: {str(e)}")
            return False
    
    def _load_initial_urls(self):
        """Load initial URLs from configuration."""
        initial_urls = self.config['crawling'].get('initial_urls', [self.config['target_url']])
        added_count = 0
        
        for url in initial_urls:
            if url not in self.visited_urls:
                self.url_queue.push(url, 0, priority=1.0)  # High priority for initial URLs
                added_count += 1
            
            try:
                with self.db_manager.session() as session:
                    existing_status = session.query(CrawlStatus).filter_by(url=url).first()
                    if not existing_status:
                        status = CrawlStatus(url=url, depth=0)
                        session.add(status)
                        session.commit()
            except Exception as e:
                self.logger.warning(f"Error adding initial URL to database: {str(e)}")
        
        self.logger.info(f"Added {added_count} initial URLs to the queue")
    
    def _load_unvisited_urls(self):
        """Load unvisited URLs from the database to resume scraping."""
        try:
            with self.db_manager.session() as session:
                # Get unvisited URLs sorted by depth (shallow first)
                unvisited_records = (
                    session.query(CrawlStatus)
                    .filter_by(visited=False)
                    .order_by(CrawlStatus.depth)
                    .limit(1000)
                    .all()
                )
                
                added_count = 0
                for record in unvisited_records:
                    if record.url not in self.visited_urls:
                        # Priority based on depth
                        priority = record.depth * 10.0 if record.depth is not None else 100.0
                        self.url_queue.push(record.url, record.depth or 0, priority=priority)
                        added_count += 1
                
                # If no unvisited URLs, add target URL as fallback
                if added_count == 0 and len(self.url_queue) == 0:
                    target_url = self.config['target_url']
                    self.logger.info(f"No unvisited URLs found in database, adding target URL: {target_url}")
                    
                    self.url_queue.push(target_url, 0, priority=1.0)
                    
                    existing_status = session.query(CrawlStatus).filter_by(url=target_url).first()
                    if not existing_status:
                        status = CrawlStatus(url=target_url, depth=0)
                        session.add(status)
                        session.commit()
                    
                    added_count = 1
                
                self.logger.info(f"Loaded {added_count} unvisited URLs from database")
        
        except Exception as e:
            self.logger.error(f"Error loading unvisited URLs: {str(e)}")
            
            # Add target URL as fallback
            if len(self.url_queue) == 0:
                target_url = self.config['target_url']
                self.logger.info(f"Adding target URL as fallback after error: {target_url}")
                self.url_queue.push(target_url, 0, priority=1.0)
    
    def generate_sitemap_files(self):
        """Generate sitemap files from database information."""
        try:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            # Get visited URLs from database
            sitemap = []
            with self.db_manager.session() as session:
                entries = session.query(CrawlStatus).filter_by(visited=True).all()
                for entry in entries:
                    if entry.url:
                        sitemap.append({
                            'url': entry.url,
                            'title': entry.title if entry.title else "",
                            'depth': entry.depth if entry.depth is not None else 0,
                            'links': entry.outgoing_links if entry.outgoing_links is not None else 0,
                            'is_manufacturer': entry.is_manufacturer_page
                        })
            
            if not sitemap:
                self.logger.warning("No sitemap data found in database")
                return
            
            # Save JSON sitemap
            sitemap_path = os.path.join(output_dir, 'sitemap.json')
            with open(sitemap_path, 'w') as f:
                json.dump(sitemap, f, indent=2)
            
            # Save XML sitemap
            xml_path = os.path.join(output_dir, 'sitemap.xml')
            with open(xml_path, 'w') as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
                for entry in sitemap:
                    f.write('  <url>\n')
                    f.write(f'    <loc>{entry["url"]}</loc>\n')
                    f.write('  </url>\n')
                f.write('</urlset>')
            
            self.logger.info(f"Sitemap generated with {len(sitemap)} URLs")
            self._log_to_db("INFO", f"Sitemap generated with {len(sitemap)} URLs")
        
        except Exception as e:
            self.logger.error(f"Error generating sitemap: {str(e)}")
            self._log_to_db("ERROR", f"Error generating sitemap: {str(e)}")
    
    def process_batch(self, max_pages: int = 0, max_runtime_minutes: int = 0) -> Dict[str, Any]:
        """
        Process a batch of URLs with time and page limits.
        
        Args:
            max_pages: Maximum number of pages to process (0 for unlimited)
            max_runtime_minutes: Maximum runtime in minutes (0 for unlimited)
            
        Returns:
            Dictionary of batch statistics
        """
        self.logger.info(f"Starting batch processing with max_pages={max_pages}, max_runtime_minutes={max_runtime_minutes}")
        self.statistics_manager.start_batch()
        
        # Initialize URL queue if empty
        if len(self.url_queue) == 0:
            self._load_initial_urls()
            self._load_unvisited_urls()
        
        # Process the batch
        batch_stats = self._process_batch(max_pages, max_runtime_minutes)
        
        # Update session status in database
        try:
            self.statistics_manager.update_session_in_database(completed=True)
            self.logger.info(f"Updated session {self.session_id} - marked as completed")
        except Exception as e:
            self.logger.error(f"Failed to update scraper session: {str(e)}")
        
        self.logger.info(f"Crawling finished. Processed {batch_stats.get('pages_crawled', 0)} pages.")
        return batch_stats
    
    def _process_batch(self, max_pages: int = 0, max_runtime_minutes: int = 0) -> Dict[str, Any]:
        """
        Process a batch of URLs with time and page limits.
        
        Args:
            max_pages: Maximum number of pages to process (0 for unlimited)
            max_runtime_minutes: Maximum runtime in minutes (0 for unlimited)
            
        Returns:
            Dictionary of batch statistics
        """
        # Calculate end time if runtime limit is set
        end_time = time.time() + (max_runtime_minutes * 60) if max_runtime_minutes > 0 else None
        self.logger.info(f"URL queue contains {len(self.url_queue)} URLs to process")
        
        # Initialize batch variables
        pages_crawled = 0
        batch_start_time = time.time()
        
        # Main processing loop
        while len(self.url_queue) > 0 and (max_pages == 0 or pages_crawled < max_pages):
            # Check for shutdown/suspension
            if self.check_shutdown_requested():
                self.logger.info("Shutdown requested, stopping batch processing")
                break
            
            # Check runtime limit
            if end_time and time.time() >= end_time:
                self.logger.info(f"Reached maximum runtime of {max_runtime_minutes} minutes")
                break
            
            try:
                # Get next URL from priority queue
                url, depth = self.url_queue.pop()
                
                # Skip if already visited
                url_exists, is_visited = self._check_url_in_database(url)
                if is_visited or url in self.visited_urls:
                    self.logger.debug(f"Skipping already visited URL: {url}")
                    continue
                
                # Skip excluded URLs
                if self._is_excluded_url(url):
                    self.logger.debug(f"Skipping excluded URL: {url}")
                    continue
                
                # Process the URL
                self.logger.info(f"Processing URL: {url} [Queue: {len(self.url_queue)}]")
                
                # Ensure URL is in database
                if not url_exists:
                    with self.db_manager.session() as session:
                        status = CrawlStatus(url=url, depth=depth)
                        session.add(status)
                        session.commit()
                
                # Fetch content
                content = self._fetch_url(url)
                if not content:
                    self.logger.warning(f"Failed to fetch content for URL: {url}")
                    continue
                
                # Parse with BeautifulSoup
                soup = BeautifulSoup(content, 'html.parser')
                title = soup.title.text.strip() if soup.title else "No title"
                self.logger.info(f"Page title: {title}")
                
                # Extract text content for analysis
                content_text = self._extract_text_content(soup)
                
                # Analyze with Claude Haiku to determine if it's a manufacturer page
                is_manufacturer = self._analyze_with_anthropic(url, title, content_text)
                if is_manufacturer:
                    self.logger.info(f"Found manufacturer page: {url}")
                    
                    # Extract detailed manufacturer data with Claude Sonnet
                    extracted_data = self._analyze_with_claude_sonnet(url, title, content_text)
                    
                    # Process and save the extracted data
                    self._process_extracted_data(extracted_data, url)
                    self.statistics_manager.increment_stat('manufacturers_found')
                
                # Extract and process links
                links = self._extract_links(soup, url)
                self.logger.info(f"Extracted {len(links)} links from {url}")
                
                # Update outgoing link count in database
                with self.db_manager.session() as session:
                    status = session.query(CrawlStatus).filter(CrawlStatus.url == url).first()
                    if status:
                        status.outgoing_links = len(links)
                        status.title = title
                        session.commit()
                
                # Add extracted links to queue
                added_links = 0
                for link_url, anchor_text in links.items():
                    # Skip if already visited or in queue
                    if link_url in self.visited_urls:
                        continue
                    
                    link_in_queue = link_url in self.url_queue
                    link_exists, link_visited = self._check_url_in_database(link_url)
                    
                    if not link_visited and not link_in_queue:
                        # Add to queue with properly calculated priority
                        self.url_queue.push(link_url, depth + 1, content_hint=anchor_text)
                        added_links += 1
                        
                        # Add to database if not already there
                        if not link_exists:
                            with self.db_manager.session() as session:
                                status = CrawlStatus(
                                    url=link_url,
                                    depth=depth + 1,
                                    parent_url=url
                                )
                                session.add(status)
                                session.commit()
                
                self.logger.info(f"Added {added_links} new links to the queue")
                
                # Mark URL as visited
                self.visited_urls.add(url)
                self._mark_url_visited(url, is_manufacturer, depth)
                
                # Update statistics
                pages_crawled += 1
                self.statistics_manager.increment_stat('pages_crawled')
                
                # Log progress periodically
                if pages_crawled % 10 == 0 or max_pages < 10:
                    self.logger.info(f"Progress: {pages_crawled} pages crawled, {len(self.url_queue)} URLs in queue")
            
            except Exception as e:
                self.logger.error(f"Error processing URL: {str(e)}")
                import traceback
                self.logger.debug(f"Crawling error details: {traceback.format_exc()}")
        
        # Generate sitemap
        self.generate_sitemap_files()
        
        # Finalize statistics
        self.statistics_manager.end_batch()
        
        # Determine final status
        if self.check_shutdown_requested():
            self.statistics_manager.update_stat('status', 'interrupted')
            status_message = "Crawling interrupted by user"
        else:
            self.statistics_manager.update_stat('status', 'completed')
            status_message = f"Crawling finished. Processed {pages_crawled} pages."
        
        self.logger.info(status_message)
        return self.statistics_manager.get_all_stats()
    
    def start_crawling(self, max_pages: Optional[int] = None) -> Dict[str, Any]:
        """
        Start the crawling process.
        
        Args:
            max_pages: Maximum number of pages to process (None for unlimited)
            
        Returns:
            Dictionary of statistics about the crawling session
        """
        self.logger.info("Starting crawling process")
        self.session_id = self.statistics_manager.session_id
        self.logger.info(f"Using scraper session with ID {self.session_id}")
        
        # Reset shutdown flag
        self.shutdown_requested = False
        self.check_shutdown_requested()
        
        # Initialize URL queue if empty
        if len(self.url_queue) == 0:
            self._load_initial_urls()
            self._load_unvisited_urls()
        
        # Process the batch
        batch_stats = self.process_batch(max_pages=max_pages)
        
        # Update session status in database
        try:
            self.statistics_manager.update_session_in_database(completed=True)
            self.logger.info(f"Updated session {self.session_id} - marked as completed")
        except Exception as e:
            self.logger.error(f"Failed to update scraper session: {str(e)}")
        
        self.logger.info(f"Crawling finished. Processed {batch_stats.get('pages_crawled', 0)} pages.")
        return batch_stats
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the scraping process.
        
        Returns:
            Dictionary of statistics
        """
        return self.statistics_manager.get_batch_stats()
    
    def get_top_manufacturers_by_category_count(self, limit: int = 5) -> List[Tuple[str, int]]:
        """
        Get the top manufacturers by category count.
        
        Args:
            limit: Maximum number of manufacturers to return
            
        Returns:
            List of tuples containing (manufacturer_name, category_count)
        """
        try:
            with self.db_manager.session() as session:
                query = session.query(
                    Manufacturer.name,
                    func.count(Category.id).label('category_count')
                ).join(
                    Manufacturer.categories
                ).group_by(
                    Manufacturer.id
                ).order_by(
                    desc('category_count')
                ).limit(limit)
                
                results = query.all()
                return [(name, count) for name, count in results]
        except Exception as e:
            self.logger.error(f"Error getting top manufacturers: {str(e)}")
            return []
    
    def close(self):
        """Close the scraper and release resources."""
        self.logger.info("Closing scraper and database connections")
        
        # Mark session as interrupted if still running
        if self.session_id:
            try:
                with self.db_manager.session() as session:
                    scraper_session = session.query(ScraperSession).filter_by(id=self.session_id).first()
                    if scraper_session and scraper_session.status == 'running':
                        scraper_session.status = 'interrupted'
                        scraper_session.end_time = datetime.datetime.now()
                        session.commit()
                        self.logger.info(f"Marked session {self.session_id} as interrupted during close")
            except Exception as e:
                self.logger.error(f"Error updating session status during close: {str(e)}")
        
        # Close database connections
        try:
            self.db_manager.shutdown()
            self.logger.info("Database connections closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing database connections: {str(e)}")
        
        self.logger.info("Scraper closed successfully")


if __name__ == "__main__":
    # When run directly, start the scraper
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Create the scraper
        scraper = CompetitorScraper()
        
        # Parse command-line arguments
        import argparse
        parser = argparse.ArgumentParser(description='NDAIVI Competitor Scraper')
        parser.add_argument('--max-pages', type=int, default=0, help='Maximum number of pages to crawl (0 for unlimited)')
        parser.add_argument('--max-runtime', type=int, default=0, help='Maximum runtime in minutes (0 for unlimited)')
        args = parser.parse_args()
        
        # Start crawling with parsed arguments
        stats = scraper.process_batch(max_pages=args.max_pages, max_runtime_minutes=args.max_runtime)
        
        # Output final statistics
        print("\nCrawling Statistics:")
        print(f"Pages Crawled: {stats.get('pages_crawled', 0)}")
        print(f"Manufacturers Found: {stats.get('manufacturers_found', 0)}")
        print(f"Manufacturers Extracted: {stats.get('manufacturers_extracted', 0)}")
        print(f"Categories Extracted: {stats.get('categories_extracted', 0)}")
        print(f"Websites Found: {stats.get('websites_found', 0)}")
        print(f"Status: {stats.get('status', 'unknown')}")
        
        # Show top manufacturers
        top_manufacturers = scraper.get_top_manufacturers_by_category_count(10)
        if top_manufacturers:
            print("\nTop Manufacturers by Category Count:")
            for name, count in top_manufacturers:
                print(f"  {name}: {count} categories")
    
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
    
    finally:
        # Ensure resources are properly released
        if 'scraper' in locals():
            scraper.close()