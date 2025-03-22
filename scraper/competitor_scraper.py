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
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import anthropic
from anthropic import AnthropicError
import re
import json

# Add the project root to the Python path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_manager import get_db_manager
from database.schema import Base, Manufacturer, Category, CrawlStatus, ScraperSession, ScraperLog
from database.statistics_manager import StatisticsManager

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
        'category': 3,
        # New category-specific keywords
        'products': 5,
        'categories': 4,
        'brands': 8,
        'manufacturers': 10,
        'suppliers': 7,
        'partners': 6,
        'catalog': 5,
        'shop': 3,
        'store': 3
    }
    
    # HTML structure weights
    HTML_WEIGHTS = {
        'nav': 3,  # Navigation menus often contain category hierarchies
        'breadcrumb': 2,  # Breadcrumbs show category structure
        'sidebar': 2,  # Sidebars often contain category lists
        'menu': 2,  # Menu structures often have categories
        'list': 1,  # Lists might contain categories
        'table': 1  # Tables might contain product/category info
    }
    
    # URL path segment weights
    PATH_WEIGHTS = {
        'catalog': 4,
        'products': 4,
        'categories': 4,
        'brands': 6,
        'manufacturers': 8
    }
    
    @staticmethod
    def calculate_priority(url: str, depth: int, content_hint: Optional[str] = None, html_structure: Optional[Dict] = None) -> float:
        """
        Calculate a crawling priority score for a URL with enhanced analysis.
        
        Lower scores indicate higher priority.
        
        Args:
            url: The URL to calculate priority for
            depth: The link depth from the starting point
            content_hint: Optional title or other content hint from the page
            html_structure: Optional dict containing info about HTML structure
            
        Returns:
            A priority score (lower is higher priority)
        """
        # Base priority based on depth
        priority = depth * 10.0
        
        # Parse URL for analysis
        parsed_url = urlparse(url)
        path_segments = parsed_url.path.lower().split('/')
        
        # Apply path segment boosts
        for segment in path_segments:
            for key, boost in UrlPriority.PATH_WEIGHTS.items():
                if key in segment:
                    priority -= boost
        
        # Apply keyword boosts to URL
        url_lower = url.lower()
        for keyword, boost in UrlPriority.PRIORITY_KEYWORDS.items():
            if keyword in url_lower:
                priority -= boost
        
        # Apply content hint boosts if available
        if content_hint and isinstance(content_hint, str):
            content_lower = content_hint.lower()
            for keyword, boost in UrlPriority.PRIORITY_KEYWORDS.items():
                if keyword in content_lower:
                    priority -= boost * 0.5  # Half boost for content hints
        
        # Apply HTML structure boosts if available
        if html_structure:
            for element_type, count in html_structure.items():
                if element_type.lower() in UrlPriority.HTML_WEIGHTS:
                    boost = UrlPriority.HTML_WEIGHTS[element_type.lower()]
                    priority -= boost * min(count, 3)  # Cap the boost for each type
        
        # Special adjustments
        if 'category' in path_segments or 'categories' in path_segments:
            priority -= 5  # Extra boost for explicit category pages
        
        if parsed_url.query and any(param in parsed_url.query.lower() 
                                  for param in ['category', 'brand', 'manufacturer']):
            priority -= 3  # Boost for relevant query parameters
        
        # Limit minimum priority value
        return max(1.0, priority)


class PriorityUrlQueue:
    """
    Priority queue for URLs to be crawled with enhanced prioritization.
    """
    
    def __init__(self):
        """Initialize the priority queue."""
        self._queue = []  # heapq priority queue with entries: (priority, counter, url, depth, metadata)
        self._counter = 0  # Unique counter for stable sorting
        self._url_set = set()  # Fast lookup for URLs
        self._url_metadata = {}  # Store metadata about URLs
    
    def __contains__(self, url):
        """Enable 'in' operator support for direct URL checking
        
        Args:
            url: URL to check
            
        Returns:
            True if the URL is in the queue, False otherwise
        """
        return url in self._url_set
        
    def has_url(self, url: str) -> bool:
        """Check if a URL is in the queue
        
        Args:
            url: URL to check
            
        Returns:
            True if the URL is in the queue, False otherwise
        """
        return url in self._url_set
    
    def push(self, url: str, depth: int, priority: Optional[float] = None, 
             content_hint: Optional[str] = None, html_structure: Optional[Dict] = None,
             metadata: Optional[Dict] = None):
        """
        Add a URL to the queue with calculated priority and metadata.
        
        Args:
            url: The URL to add
            depth: Current crawl depth
            priority: Optional explicit priority (lower is higher priority)
            content_hint: Optional content hint for priority calculation
            html_structure: Optional dict containing info about HTML structure
            metadata: Optional metadata about the URL
        
        Returns:
            True if URL was added, False if it was already in queue
        """
        if url in self._url_set:
            return False
        
        if priority is None:
            # Calculate priority based on all available information
            priority = UrlPriority.calculate_priority(url, depth, content_hint, html_structure)
        
        # Store metadata
        if metadata:
            self._url_metadata[url] = metadata
        
        # Use a counter to ensure stable ordering for same-priority items
        entry = (priority, self._counter, url, depth)
        self._counter += 1
        
        heapq.heappush(self._queue, entry)
        self._url_set.add(url)
        return True
    
    def pop(self) -> Tuple[str, int, Optional[Dict]]:
        """
        Get the highest priority URL from the queue.
        
        Returns:
            Tuple of (url, depth, metadata) for the highest priority URL
        
        Raises:
            IndexError: If the queue is empty
        """
        if not self._queue:
            raise IndexError("Priority queue is empty")
        
        # Get and return the highest priority (lowest score) URL
        _, _, url, depth = heapq.heappop(self._queue)
        self._url_set.remove(url)
        
        # Get and remove metadata if it exists
        metadata = self._url_metadata.pop(url, None)
        
        return url, depth, metadata
    
    def update_priority(self, url: str, new_priority: float):
        """Update the priority of a URL in the queue"""
        if url not in self._url_set:
            return False
        
        # Find and update the entry
        for i, (_, counter, u, depth) in enumerate(self._queue):
            if u == url:
                self._queue[i] = (new_priority, counter, url, depth)
                heapq.heapify(self._queue)
                return True
        return False
    
    def __len__(self) -> int:
        """Get the number of URLs in the queue."""
        return len(self._queue)
    
    def __contains__(self, url: str) -> bool:
        """Check if a URL is in the queue."""
        return url in self._url_set
    
    def has_url(self, url: str) -> bool:
        """Check if a URL is in the queue - compatibility method."""
        return url in self._url_set
    
    def get_depth(self, url: str) -> Optional[int]:
        """Get the depth of a URL if it's in the queue."""
        if url not in self._url_set:
            return None
        
        # Find the URL in the queue and return its depth
        for _, _, u, depth in self._queue:
            if u == url:
                return depth
        return None


class CompetitorScraper:
    """Web scraper for extracting manufacturer information from competitor websites."""
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize the competitor scraper.
        
        Args:
            config_path: Path to the configuration YAML file
        """
        # Setup logging first
        self._setup_logging()
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Initialize database
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
        
        # Initialize control flags
        self.shutdown_requested = False
        
        # Set up request headers
        user_agent = self.config['crawling'].get('user_agent', 'NDAIVI Scraper')
        self.headers = {'User-Agent': user_agent}
        self.timeout = self.config['scraper'].get('request_timeout', 30)
        
        # Initialize caching and other internal variables
        self._initialize_caching()
        
        # Initialize StatisticsManager AFTER database initialization
        # Only create a new session when actually starting a crawl
        self.statistics_manager = StatisticsManager('scraper', self.db_manager, create_session=False)
        self.session_id = None
        
        self.logger.info(f"Competitor Scraper initialized successfully (session {self.session_uuid})")

    # URL queue methods are accessed directly through self.url_queue
        
    def _initialize_caching(self):
        """Initialize caching and other internal variables"""
        self.suspend_handled = False
        
        # Add caching
        self._manufacturer_cache = {}  # Cache for manufacturer lookups
        self._category_cache = {}      # Cache for category lookups
        self._batch_size = 10          # Number of URLs to process in each batch
        
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
        self.anthropic_sonnet_model = anthropic_config.get('sonnet_model', 'claude-3-5-sonnet-20240620')
        
        # Initialize Claude analyzer with direct API access
        from scraper.claude_analyzer import ClaudeAnalyzer
        self.claude_analyzer = ClaudeAnalyzer(
            api_key=self.anthropic_api_key,
            model=self.anthropic_model,
            sonnet_model=self.anthropic_sonnet_model,
            logger=self.logger
        )
        
        self.logger.info("Anthropic API configuration loaded successfully")
    
    def _initialize_database(self):
        """Initialize the database connection."""
        try:
            db_path = self.config['database']['path']
            db_dir = os.path.dirname(db_path)
            
            # Create database directory if it doesn't exist
            if not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
                self.logger.info(f"Created database directory {db_dir}")
            
            # Store config path for later use
            self.config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')
            
            # Initialize database manager
            self.db_manager = get_db_manager(db_path)
            if not hasattr(self.db_manager, '_initialized') or not self.db_manager._initialized:
                self.db_manager.initialize(db_path)
            
            # Create database and tables if they don't exist
            if not os.path.exists(db_path):
                self.logger.info(f"Database not found. Initializing database at {db_path}")
                Base.metadata.create_all(self.db_manager.engine)
                self.logger.info("Database initialized successfully")
            else:
                # Ensure language tables exist even if database already exists
                Base.metadata.create_all(self.db_manager.engine)
            
            self.logger.info(f"Database manager initialized for {db_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database manager: {str(e)}")
            raise
    
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
        Also resets URLs that were marked as visited but not fully processed.
        
        Returns:
            Set of visited URL strings
        """
        visited_urls = set()
        try:
            with self.db_manager.session() as session:
                # Get all visited URLs that were fully processed
                visited_records = session.query(CrawlStatus).filter(
                    CrawlStatus.visited == True,
                    CrawlStatus.is_manufacturer_page.isnot(None)  # Must have been analyzed
                ).all()
                
                for record in visited_records:
                    visited_urls.add(record.url)
                
                # Reset URLs that were marked visited but not fully processed
                incomplete_records = session.query(CrawlStatus).filter(
                    CrawlStatus.visited == True,
                    CrawlStatus.is_manufacturer_page.is_(None)  # Not analyzed yet
                ).all()
                
                for record in incomplete_records:
                    record.visited = False
                    self.logger.debug(f"Reset incomplete URL: {record.url}")
                
                if incomplete_records:
                    session.commit()
                    self.logger.info(f"Reset {len(incomplete_records)} incompletely processed URLs")
                
                self.logger.info(f"Loaded {len(visited_urls)} fully processed URLs from database")
                
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
                response = requests.get(url, headers=self.headers, timeout=timeout)
                
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
          retry=retry_if_exception_type(requests.RequestException))
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
            # Use the claude_analyzer to check if it's a manufacturer page
            result = self.claude_analyzer.is_manufacturer_page(url, title, content)
            
            # is_manufacturer_page returns a boolean directly
            self.logger.info(f"Claude Haiku analysis for {url}: {result}")
            
            # Update statistics - make sure this stat exists in StatisticsManager
            try:
                self.statistics_manager.increment_stat('pages_analyzed')
            except Exception as stats_error:
                self.logger.warning(f"Could not update statistics: {str(stats_error)}")
                
            return result
        
        except Exception as e:
            self.logger.error(f"Error analyzing with Claude Haiku for {url}: {str(e)}")
            self._log_to_db("ERROR", f"Error analyzing with Claude Haiku: {str(e)}")
            try:
                self.statistics_manager.increment_stat('errors')
            except:
                pass
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _analyze_with_claude_haiku(self, url: str, title: str, content: str) -> List[Dict[str, Any]]:
        """
        Use Claude Haiku to extract manufacturer and category information.
        
        Args:
            url: URL of the page
            title: Page title
            content: Page text content
            
        Returns:
            List of dictionaries with manufacturer and category information
        """
        try:
            # Use the claude_analyzer to extract manufacturer information
            result = self.claude_analyzer.extract_manufacturers(url, title, content)
            
            if result and 'manufacturers' in result:
                manufacturers = result['manufacturers']
                # Convert from ClaudeAnalyzer format to CompetitorScraper format
                converted_result = [
                    {
                        "manufacturer": mfr["name"],
                        "categories": mfr["categories"]
                    }
                    for mfr in manufacturers
                ]
                
                self.logger.info(f"Claude Sonnet extracted {len(converted_result)} manufacturers from {url}")
                
                # Update statistics with existing keys
                try:
                    self.statistics_manager.increment_stat('manufacturers_extracted', len(converted_result))
                except Exception as stats_error:
                    self.logger.warning(f"Could not update statistics: {str(stats_error)}")
                    
                return converted_result
            
            return []
        
        except Exception as e:
            self.logger.error(f"Error analyzing with Claude Sonnet for {url}: {str(e)}")
            self._log_to_db("ERROR", f"Error analyzing with Claude Sonnet: {str(e)}")
            
            # Update error statistics with existing keys
            try:
                self.statistics_manager.increment_stat('errors')
            except:
                pass
                
            return []
    
    def _translate_categories_batch(self, categories: List[str], manufacturer_name: str) -> Dict[str, Dict[str, str]]:
        """
        Translate a batch of categories to all target languages at once.
        
        Args:
            categories: List of category names to translate
            manufacturer_name: Manufacturer name to preserve
            
        Returns:
            Dictionary mapping category to its translations dict
        """
        translations = {}
        source_lang = self.config['languages']['source']
        target_langs = self.config['languages']['targets']
        
        # Initialize categories to translate list
        categories_to_translate = []
        
        # Initialize translations dict
        for category in categories:
            translations[category] = {source_lang: category}  # Add source version
            categories_to_translate.append(category)
        
        if not categories_to_translate:
            return translations
            
        # Batch translate categories
        max_retries = 3
        retry_count = 0
        success = False
        
        while not success and retry_count < max_retries:
            try:
                # Format categories with manufacturer name
                categories_text = '\n'.join(categories_to_translate)
                self.logger.info(f"Batch translating categories:\n{categories_text}")
                
                # Translate to each target language
                for target_lang in target_langs:
                    if target_lang == source_lang:
                        continue
                        
                    self.logger.info(f"Translating batch to {target_lang}")
                    # Get translations in batch
                    translated_batch = self.claude_analyzer.translate_categories_batch(
                        categories_text, manufacturer_name, target_lang, response_format='delimited')
                    
                    # Process translations
                    if translated_batch:
                        for orig, trans in zip(categories_to_translate, translated_batch):
                            translations[orig][target_lang] = trans
                            self.logger.info(f"Translated '{orig}' -> '{trans}' ({target_lang})")
                            self.statistics_manager.increment_stat('translations')
                                
                        success = True
                    else:
                        self.logger.error("No translations received")
                        raise ValueError("No translations received")
                        
            except Exception as e:
                self.logger.error(f"Batch translation error: {str(e)} (attempt {retry_count + 1}/{max_retries})")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)  # Exponential backoff
                else:
                    self.logger.error(f"Failed to translate categories after {max_retries} attempts")
                    return translations  # Return what we have instead of raising
                    
        return translations
    
    def _translate_category(self, category: str, manufacturer_name: str) -> Dict[str, str]:
        """
        Translate a single category to all target languages.
        Uses batch translation under the hood for efficiency.
        """
        try:
            # Use batch translation with a single category
            self.logger.info(f"Translating category: '{category}'")
            batch_results = self._translate_categories_batch([category], manufacturer_name)
            
            if category in batch_results:
                translations = batch_results[category]
                self.logger.info(f"Got translations for '{category}': {translations}")
                
                # Store base category in main categories table
                source_lang = self.config['languages']['source']
                base_category = category  # Use original name as base
                
                # Find or create base category
                category = self.db_manager.session.query(Category).filter(
                    func.lower(Category.name) == func.lower(base_category)
                ).first()
                
                if not category:
                    category = Category(name=base_category)
                    self.db_manager.session.add(category)
                    self.db_manager.session.flush()
                    self.statistics_manager.increment_stat('categories_extracted')
                    self.logger.info(f"Created new category: {base_category}")
                
                # Associate with manufacturer
                if manufacturer not in category.manufacturers:
                    category.manufacturers.append(manufacturer)
                    self.logger.info(f"Associated category '{base_category}' with manufacturer '{manufacturer.name}'")
                
                # Store translations
                for lang, translated_name in translations.items():
                    if lang == source_lang:
                        continue
                        
                    category_table = Base.metadata.tables.get(f'category_{lang}')
                    if not category_table:
                        continue
                    
                    # Create translation entry
                    translation_entry = category_table.insert().values(category_id=category.id, category_name=translated_name)
                    self.db_manager.session.execute(translation_entry)
                    self.statistics_manager.increment_stat('translations')
                    self.logger.info(f"Added translation for '{base_category}' in {lang}: '{translated_name}'")
                
                self.db_manager.session.commit()
                
            else:
                self.logger.error(f"No translations found for '{category}'")
                return {}
            
        except Exception as e:
            self.logger.error(f"Translation failed for category '{category}': {str(e)}")
            return {}
    
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
        
        # Log initial extraction summary
        total_manufacturers = len(response_data)
        total_categories = sum(len(item.get('categories', [])) for item in response_data)
        self.logger.info(f"Claude Haiku extracted {total_manufacturers} manufacturers and {total_categories} categories from {source_url}")
        
        manufacturers_added = 0
        categories_added = 0
        translations_added = 0
        
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
                    
                    # Log manufacturer and categories
                    self.logger.info(f"Extracted manufacturer: {manufacturer_name} with {len(categories)} categories:")
                    for i, category_name in enumerate(categories, 1):
                        self.logger.info(f"  {i}. {category_name}")
                    
                    # Process categories
                    self.logger.info(f"Processing {len(categories)} categories for manufacturer '{manufacturer_name}'")
                    categories_processed = 0
                    for i, category_name in enumerate(categories, 1):
                        self.logger.info(f"  Category {i}: {category_name}")
                        
                        # Basic category validation
                        if len(category_name) < 3:
                            self.logger.warning(f"Skipping category '{category_name}' - too short")
                            continue
                            
                        # Add manufacturer name if not present
                        if manufacturer_name.lower() not in category_name.lower():
                            category_name = f"{manufacturer_name} {category_name}"
                            self.logger.info(f"Added manufacturer name to category: '{category_name}'")
                        
                        try:
                            # Get translations for all target languages
                            translations = self._translate_category(category_name, manufacturer_name)
                            self.logger.info(f"Got translations for '{category_name}': {translations}")
                            
                            # Store base category in main categories table
                            source_lang = self.config['languages']['source']
                            base_category = category_name  # Use original name as base
                            
                            # Find or create base category
                            category = session.query(Category).filter(
                                func.lower(Category.name) == func.lower(base_category)
                            ).first()
                            
                            if not category:
                                category = Category(name=base_category)
                                session.add(category)
                                session.flush()
                                categories_added += 1
                                self.statistics_manager.increment_stat('categories_extracted')
                                self.logger.info(f"Created new category: {base_category}")
                            
                            # Associate with manufacturer
                            if manufacturer not in category.manufacturers:
                                category.manufacturers.append(manufacturer)
                                categories_processed += 1
                            
                            # Store translations
                            for lang, translated_name in translations.items():
                                if lang == source_lang:
                                    continue
                                    
                                category_table = Base.metadata.tables.get(f'category_{lang}')
                                if not category_table:
                                    continue
                                
                                # Create translation entry
                                translation_entry = category_table.insert().values(category_id=category.id, category_name=translated_name)
                                session.execute(translation_entry)
                                translations_added += 1
                                self.logger.info(f"Added translation for '{base_category}' in {lang}: '{translated_name}'")
                            
                            session.commit()
                            
                        except Exception as e:
                            session.rollback()
                            self.logger.error(f"Error processing category '{category_name}': {str(e)}")
                            continue
                    
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
                f"(Added: {manufacturers_added} manufacturers, {categories_added} categories, {translations_added} translations)"
            )
            self._log_to_db(
                "INFO", 
                f"Saved {manufacturers_added} manufacturers, {categories_added} categories, and {translations_added} translations from {source_url}"
            )
        
        except Exception as e:
            self.logger.error(f"Error processing extracted data for {source_url}: {str(e)}")
            self._log_to_db("ERROR", f"Error processing extracted data: {str(e)}")
    
    def _store_manufacturer_data(self, manufacturer_data_list: List[Dict]) -> None:
        """
        Store extracted manufacturer data in the database.
        
        Args:
            manufacturer_data_list: List of dictionaries with manufacturer data
        """
        if not manufacturer_data_list:
            return
            
        self.logger.info(f"Storing {len(manufacturer_data_list)} manufacturer records in the database")
        
        try:
            with self.db_manager.session() as session:
                for mfr_data in manufacturer_data_list:
                    try:
                        # Extract data
                        manufacturer_name = mfr_data.get('manufacturer', '')
                        website = mfr_data.get('website', '')
                        categories = mfr_data.get('categories', [])
                        
                        # Skip invalid data
                        if not manufacturer_name:
                            continue
                            
                        # Get or create manufacturer record
                        manufacturer = self._get_or_create_manufacturer(session, manufacturer_name, website)
                        
                        # Process categories for this manufacturer
                        for category_name in categories:
                            if not category_name:
                                continue
                                
                            # Get or create category with proper error handling
                            try:
                                self._get_or_create_category(session, manufacturer, category_name)
                            except Exception as cat_err:
                                self.logger.error(f"Error adding category '{category_name}': {str(cat_err)}")
                                continue
                                
                        # Commit transaction
                        session.commit()
                        self.logger.info(f"Successfully stored manufacturer '{manufacturer_name}' with {len(categories)} categories")
                        
                    except Exception as e:
                        session.rollback()
                        self.logger.error(f"Error storing manufacturer data: {str(e)}")
                        continue
                        
        except Exception as db_err:
            self.logger.error(f"Database error storing manufacturer data: {str(db_err)}")
    
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
    
    def _extract_html_structure(self, soup: BeautifulSoup) -> Dict[str, int]:
        """Extract information about HTML structure for priority calculation"""
        structure = defaultdict(int)
        
        # Count navigation elements
        structure['nav'] = len(soup.find_all('nav'))
        
        # Count and analyze lists
        lists = soup.find_all(['ul', 'ol'])
        structure['list'] = len([l for l in lists if len(l.find_all('li')) > 3])
        
        # Count menu-like elements
        structure['menu'] = len(soup.find_all(class_=lambda x: x and any(term in str(x).lower() for term in ['nav', 'menu', 'main'])))
        
        # Count sidebar elements
        structure['sidebar'] = len(soup.find_all(class_=lambda x: x and 'sidebar' in str(x).lower()))
        
        # Count breadcrumb elements
        structure['breadcrumb'] = len(soup.find_all(class_=lambda x: x and 'breadcrumb' in str(x).lower()))
        
        # Count tables
        structure['table'] = len(soup.find_all('table'))
        
        return dict(structure)

    def _extract_navigation_elements(self, soup: BeautifulSoup) -> str:
        """
        Extract navigation elements, breadcrumbs, and link patterns from a page.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            String containing navigation data formatted for analysis
        """
        nav_data = []
        
        # Extract navigation menus
        for nav in soup.find_all(['nav', 'ul'], class_=lambda x: x and any(term in str(x).lower() for term in ['nav', 'menu', 'main-menu'])):
            nav_text = nav.get_text(' | ', strip=True)
            if len(nav_text) > 20:  # Only consider substantial navigation elements
                nav_data.append(f"Navigation: {nav_text[:500]}")
        
        # Extract breadcrumbs
        breadcrumbs = soup.find_all(['nav', 'div', 'ol'], class_=lambda x: x and 'breadcrumb' in str(x).lower())
        for crumb in breadcrumbs:
            nav_data.append(f"Breadcrumb: {crumb.get_text(' > ', strip=True)}")
        
        # Extract link patterns
        links = soup.find_all('a', href=True)
        link_texts = [link.get_text(strip=True) for link in links if link.get_text(strip=True)]
        link_texts = [text for text in link_texts if len(text) > 2][:30]  # Limit to 30 link texts
        
        if link_texts:
            nav_data.append(f"Link texts: {' | '.join(link_texts)}")
        
        return "\n".join(nav_data)
    
    def _extract_main_content(self, soup: BeautifulSoup) -> str:
        """
        Extract the main content from a page.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Extracted main content as a string
        """
        # Try to find main content section
        main_content = ""
        
        # Look for main content containers
        main_elements = soup.find_all(['main', 'article', 'section', 'div'], 
                                      class_=lambda x: x and any(term in str(x).lower() for term in 
                                                               ['main', 'content', 'body', 'products']))
        
        if main_elements:
            # Use the largest content section by text length
            main_element = max(main_elements, key=lambda x: len(x.get_text()))
            main_content = main_element.get_text(' ', strip=True)
        else:
            # Fallback: take body text excluding common non-content elements
            body = soup.find('body')
            if body:
                # Create a copy to avoid modifying the original soup
                body_copy = copy.copy(body)
                
                # Remove non-content elements
                for element in body_copy.find_all(['header', 'footer', 'nav', 'aside']):
                    element.decompose()
                
                main_content = body_copy.get_text(' ', strip=True)
        
        return main_content
    
    def _analyze_page(self, url: str, html_content: str) -> Tuple[bool, List[Dict], List[Tuple]]:
        """
        Analyze a page for manufacturer information and extract links using a 4-step analysis system.
        
        The analysis process follows these steps:
        1. Analyze title, headers, and metadata with keywords to discard useless pages (no Claude)
        2. If it passes initial filter, send to Claude for analysis of title and metadata
        3. Analyze href links, navigation, breadcrumbs, etc.
        4. If still inconclusive, send a truncated page to Claude
        
        Pages are classified into three categories:
        - Brand page: Uses full analysis to extract all categories
        - Brand category page: Analyzes headers only and adds categories if not already present
        - Other: Skipped completely
        
        Returns:
            Tuple containing:
            - Boolean indicating if it's a manufacturer page (brand page or brand category page)
            - List of extracted manufacturer data dictionaries
            - List of new URLs to crawl with metadata
        """
        try:
            # STEP 1: Parse HTML and perform initial keyword-based filtering
            self.logger.info(f"STEP 1: Parsing HTML and performing initial keyword filtering for {url}")
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract page title
            title = soup.title.string if soup.title else ""
            self.logger.info(f"Page title: {title}")
            
            # 1. First check for obvious non-manufacturer pages by URL pattern
            url_lower = url.lower()
            title_lower = title.lower()
            
            # Common patterns for non-manufacturer pages
            skip_patterns = [
                '/about', '/contact', '/privacy', '/terms', '/cookie', '/login', '/register',
                '/cart', '/checkout', '/shipping', '/returns', '/faq', '/help', '/support',
                '/search', '/sitemap', '/blog', '/news', '/press', '/media', '/careers',
                '/jobs', '/legal', '/copyright', '/account', '/profile', '/disclaimer'
            ]
            
            # Check URL for skip patterns
            if any(pattern in url_lower for pattern in skip_patterns):
                self.logger.info(f"Skipping page due to URL pattern match: {url}")
                return False, [], self._extract_links_for_crawling(soup, url)
                
            # Check title for obvious non-manufacturer indicators
            skip_title_patterns = [
                'about us', 'contact us', 'privacy policy', 'terms of service',
                'shopping cart', 'checkout', 'login', 'register', 'my account',
                'faq', 'help center', 'support center', 'shipping info',
                'return policy', 'blog post', 'news article', 'press release'
            ]
            
            if any(pattern in title_lower for pattern in skip_title_patterns):
                self.logger.info(f"Skipping page due to title pattern match: {title}")
                return False, [], self._extract_links_for_crawling(soup, url)
            
            # Extract meta description and keywords
            meta_description = ""
            meta_keywords = ""
            
            meta_desc_tag = soup.find('meta', attrs={'name': 'description'})
            if meta_desc_tag and 'content' in meta_desc_tag.attrs:
                meta_description = meta_desc_tag['content']
                
            meta_keywords_tag = soup.find('meta', attrs={'name': 'keywords'})
            if meta_keywords_tag and 'content' in meta_keywords_tag.attrs:
                meta_keywords = meta_keywords_tag['content']
            
            # Extract headers (h1, h2, h3)
            headers = []
            for h_tag in soup.find_all(['h1', 'h2', 'h3']):
                headers.append(h_tag.get_text().strip())
            
            # 2. Look for strong manufacturer/brand indicators
            manufacturer_indicators = {
                'strong': [
                    'product catalog', 'product range', 'product line', 'our products',
                    'product categories', 'product series', 'brand overview',
                    'manufacturer overview', 'official manufacturer', 'official brand'
                ],
                'medium': [
                    'products', 'categories', 'models', 'series', 'catalog',
                    'product list', 'product overview', 'brand products'
                ]
            }
            
            # Combine text for analysis
            combined_text = f"{title} {meta_description} {meta_keywords} {' '.join(headers)}".lower()
            
            # Count indicators
            strong_matches = [ind for ind in manufacturer_indicators['strong'] if ind in combined_text]
            medium_matches = [ind for ind in manufacturer_indicators['medium'] if ind in combined_text]
            
            # Calculate score
            score = len(strong_matches) * 2 + len(medium_matches)
            
            # Additional scoring based on page structure
            # Check for product grids or lists
            product_grid = soup.find(class_=lambda x: x and any(term in str(x).lower() for term in ['product-grid', 'product-list', 'category-grid']))
            if product_grid:
                score += 2
                
            # Check for category navigation
            category_nav = soup.find(class_=lambda x: x and any(term in str(x).lower() for term in ['category-nav', 'product-nav', 'product-menu']))
            if category_nav:
                score += 1
                
            # Check for breadcrumbs with product/category indicators
            breadcrumb = soup.find(class_=lambda x: x and 'breadcrumb' in str(x).lower())
            if breadcrumb and any(term in breadcrumb.get_text().lower() for term in ['products', 'categories', 'models']):
                score += 1
                
            # Log analysis results
            self.logger.info(f"Page analysis score: {score}")
            self.logger.info(f"Strong indicators found: {strong_matches}")
            self.logger.info(f"Medium indicators found: {medium_matches}")
            
            # Determine if page should be analyzed further
            threshold = 2  # Require at least 2 points to proceed
            if score < threshold:
                self.logger.info(f"Page failed initial filter with score {score}, skipping further analysis")
                return False, [], self._extract_links_for_crawling(soup, url)
                
            self.logger.info(f"Page passed initial filter with score {score}, proceeding to detailed analysis")
            
            # STEP 2: If page passes initial filter, send title and metadata to Claude
            self.logger.info(f"STEP 2: Page passed initial filter, analyzing with Claude (title and metadata)")
            
            # Prepare content for Claude analysis (title + meta + headers)
            metadata_content = f"Title: {title}\nMeta Description: {meta_description}\nMeta Keywords: {meta_keywords}\nHeaders: {' | '.join(headers[:10])}"
            
            # Initialize page type to inconclusive
            page_type = "inconclusive"
            
            try:
                # Use Claude to analyze page type from metadata
                analysis_result = self.claude_analyzer.analyze_page_type(url, metadata_content)
                page_type = analysis_result.get('page_type', 'inconclusive')
                self.logger.info(f"Claude determined page type from metadata: {page_type}")
                
                # If we have a conclusive result (not 'inconclusive'), process it
                if page_type != "inconclusive":
                    is_manufacturer = page_type in ["brand_page", "brand_category_page"]
                    
                    # If it's a brand page or category page, process accordingly
                    if is_manufacturer:
                        extracted_data = self._process_by_page_type(url, soup, page_type, title, metadata_content)
                        return True, extracted_data, self._extract_links_for_crawling(soup, url)
                    elif page_type == "other":
                        self.logger.info("Claude determined this is not a manufacturer page, skipping further analysis")
                        return False, [], self._extract_links_for_crawling(soup, url)
            except Exception as api_err:
                self.logger.error(f"Error in initial Claude analysis: {str(api_err)}")
                self.logger.info("Continuing with more detailed analysis despite API error")
            
            # STEP 3: Analysis of navigation elements if metadata was inconclusive
            self.logger.info(f"STEP 3: Analyzing navigation elements for {url}")
            
            # Extract navigation elements
            nav_elements = self._extract_navigation_elements(soup)
            
            if nav_elements:
                try:
                    # Analyze navigation elements with Claude
                    analysis_result = self.claude_analyzer.analyze_page_type(url, nav_elements)
                    page_type = analysis_result.get('page_type', 'inconclusive')
                    self.logger.info(f"Claude determined page type from navigation: {page_type}")
                    
                    # If we have a conclusive result from navigation elements
                    if page_type != "inconclusive":
                        is_manufacturer = page_type in ["brand_page", "brand_category_page"]
                        
                        if is_manufacturer:
                            extracted_data = self._process_by_page_type(url, soup, page_type, title, nav_elements)
                            return True, extracted_data, self._extract_links_for_crawling(soup, url)
                        elif page_type == "other":
                            self.logger.info("Claude determined this is not a manufacturer page, skipping further analysis")
                            return False, [], self._extract_links_for_crawling(soup, url)
                except Exception as api_err:
                    self.logger.error(f"Error in navigation-based Claude analysis: {str(api_err)}")
                    self.logger.info("Continuing to step 4 due to API error")
            else:
                self.logger.info("No substantial navigation elements found, proceeding to step 4")
            
            # STEP 4: Last resort - analyze truncated page content
            self.logger.info(f"STEP 4: Analyzing truncated page content for {url}")
            
            # Extract main content
            main_content = self._extract_main_content(soup)
            
            try:
                # Analyze truncated content with Claude
                analysis_result = self.claude_analyzer.analyze_page_type(url, main_content)
                page_type = analysis_result.get('page_type', 'inconclusive')
                self.logger.info(f"Claude determined page type from truncated content: {page_type}")
                
                # Final determination
                is_manufacturer = page_type in ["brand_page", "brand_category_page"]
                
                if is_manufacturer:
                    extracted_data = self._process_by_page_type(url, soup, page_type, title, main_content)
                    return True, extracted_data, self._extract_links_for_crawling(soup, url)
                else:
                    # If inconclusive or other after all steps, treat as non-manufacturer
                    self.logger.info(f"Final determination: {page_type}, treating as non-manufacturer page")
                    return False, [], self._extract_links_for_crawling(soup, url)
            except Exception as api_err:
                self.logger.error(f"Error in full page Claude analysis: {str(api_err)}")
                self.logger.info("Defaulting to non-manufacturer page due to analysis errors")
                return False, [], self._extract_links_for_crawling(soup, url)
        
        except Exception as e:
            self.logger.error(f"Error analyzing page {url}: {str(e)}", exc_info=True)
            # Return empty results but don't fail completely
            return False, [], []
    
    def _extract_links_for_crawling(self, soup: BeautifulSoup, base_url: str) -> List[Tuple]:
        """
        Extract links for further crawling.
        
        Args:
            soup: BeautifulSoup object of the page
            base_url: Base URL for resolving relative links
            
        Returns:
            List of tuples (url, content_hint, html_structure, metadata)
        """
        new_urls = []
        try:
            # Extract HTML structure information
            html_structure = self._extract_html_structure(soup)
            
            # Find all links in the page
            all_links = soup.find_all('a', href=True)
            self.logger.info(f"Found {len(all_links)} total links on the page")
            
            # Process each link
            for link in all_links:
                href = link['href']
                if href:
                    # Convert relative URLs to absolute
                    absolute_url = urljoin(base_url, href) if not href.startswith(('http://', 'https://')) else href
                    
                    # Check if we should crawl this URL based on domain and exclusion rules
                    if self._should_crawl_url(absolute_url):
                        # Get content hint from link text and title
                        link_text = link.get_text().strip()
                        link_title = link.get('title', '')
                        content_hint = f"{link_text} {link_title}"
                        
                        # Store link metadata for prioritization
                        metadata = {
                            'text': link_text,
                            'title': link_title,
                            'class': link.get('class', []),
                            'parent_tag': link.parent.name if link.parent else None
                        }
                        
                        # Add URL to the list of new URLs to crawl
                        new_urls.append((absolute_url, content_hint, html_structure, metadata))
            
            # Log summary of URLs to be crawled
            self.logger.info(f"Added {len(new_urls)} URLs to the crawl queue")
            if new_urls:
                sample_urls = [url for url, _, _, _ in new_urls[:3]]
                self.logger.info(f"Sample URLs: {', '.join(sample_urls)}{' ...' if len(new_urls) > 3 else ''}")
                
        except Exception as link_err:
            self.logger.error(f"Error extracting links: {str(link_err)}")
        
        return new_urls
    
    def _should_crawl_url(self, url: str) -> bool:
        """Check if a URL should be crawled based on the current configuration"""
        # Check if the URL is already in the queue or has been visited
        if url in self.visited_urls or self.url_queue.has_url(url):
            return False
        
        # Check if the URL matches the target domain
        target_domain = urlparse(self.config['target_url']).netloc
        if urlparse(url).netloc != target_domain:
            return False
        
        # Check if the URL has a valid scheme
        if not urlparse(url).scheme:
            return False
        
        return True
        
    def _process_by_page_type(self, url: str, soup: BeautifulSoup, page_type: str, content: str = None, manufacturer_data: List[Dict] = None) -> List[Dict]:
        """
        Process the page based on its identified type (brand_page, brand_category_page, or other).
        
        Args:
            url: URL of the page
            soup: BeautifulSoup object of the page
            page_type: Type of page ('brand_page', 'brand_category_page', 'inconclusive', or 'other')
            content: Optional pre-extracted content (to avoid re-processing)
            manufacturer_data: Optional pre-extracted manufacturer data (to avoid re-processing)
            
        Returns:
            List of dictionaries with extracted manufacturer data
        """
        result_data = []
        
        try:
            self.logger.info(f"Processing page type: {page_type} for URL: {url}")
            
            # Skip processing for 'other' pages entirely
            if page_type == 'other':
                self.logger.info(f"Skipping 'other' page type: {url}")
                return []
                
            # Extract page title
            title = soup.title.string if soup.title else ""
            
            # Get content if not provided
            if content is None:
                content = self._extract_text_content(soup)
                
            # For brand pages, perform full analysis to extract all categories
            if page_type == 'brand_page':
                self.logger.info(f"Processing manufacturer page: {url}")
                
                # Use pre-extracted data if available
                if manufacturer_data:
                    result_data = manufacturer_data
                else:
                    # Extract navigation elements for deeper analysis
                    nav_elements = self._extract_navigation_elements(soup)
                    
                    # Combine with main content for analysis
                    main_content = self._extract_main_content(soup)
                    
                    # Prepare content for analysis
                    analysis_content = f"URL: {url}\nTitle: {title}\n\nNavigation Elements:\n{nav_elements}\n\nMain Content:\n{main_content[:5000]}"
                    
                    # Use Claude Haiku to extract manufacturer and category data
                    result_data = self._analyze_with_claude_haiku(url, title, analysis_content)
                    
                # Translate all categories for each manufacturer
                for mfr_data in result_data:
                    if 'categories' in mfr_data and mfr_data['categories']:
                        self.logger.info(f"Translating {len(mfr_data['categories'])} categories for manufacturer: {mfr_data.get('manufacturer', 'Unknown')}")
                        # Use batch translation for better efficiency
                        translations = self._translate_categories_batch(mfr_data['categories'], mfr_data.get('manufacturer', ''))
                        if translations:
                            mfr_data['translations'] = translations
                    
            # For brand category pages, extract headers and add new categories
            elif page_type == 'brand_category_page':
                self.logger.info(f"Processing brand category page: {url}")
                
                # Extract headers which often contain category names
                headers = []
                for h_tag in soup.find_all(['h1', 'h2', 'h3']):
                    header_text = h_tag.get_text(strip=True)
                    if header_text and len(header_text) < 100:  # Avoid very long headers
                        headers.append(header_text)
                
                # If no headers found, try to use navigation elements
                if not headers:
                    nav_elements = self._extract_navigation_elements(soup)
                    # Use simplified content focusing on navigation
                    analysis_content = f"URL: {url}\nTitle: {title}\n\nNavigation Elements:\n{nav_elements}\n\nPlease identify the product category from this page."
                    category_data = self._analyze_with_claude_haiku(url, title, analysis_content)
                    result_data = category_data
                else:
                    # Create a manufacturer record with categories from headers
                    # Try to extract manufacturer name from title or URL
                    domain = urlparse(url).netloc
                    domain_parts = domain.split('.')
                    potential_brand = domain_parts[0] if domain_parts[0] not in ['www', 'shop', 'store'] else domain_parts[1]
                    
                    result_data = [{
                        'manufacturer': potential_brand.title(),
                        'website': f"https://{domain}",
                        'categories': [h for h in headers if len(h) > 3 and len(h) < 50],  # Filter out too short/long headers
                        'source_url': url
                    }]
                    
                    # Translate categories
                    if result_data[0]['categories']:
                        translations = self._translate_categories_batch(result_data[0]['categories'], result_data[0]['manufacturer'])
                        if translations:
                            result_data[0]['translations'] = translations
                
            # For inconclusive pages, log and return empty
            elif page_type == 'inconclusive':
                self.logger.info(f"Skipping inconclusive page: {url}")
                return []
            
            return result_data
            
        except Exception as e:
            self.logger.error(f"Error processing page type {page_type} for {url}: {str(e)}")
            self._log_to_db("ERROR", f"Error processing page type {page_type}: {str(e)}")
            return []

    def _is_same_domain(self, url1: str, url2: str) -> bool:
        """
        Check if two URLs belong to the same domain.
        
        Args:
            url1: First URL to compare
            url2: Second URL to compare
            
        Returns:
            True if both URLs have the same domain, False otherwise
        """
        try:
            domain1 = urlparse(url1).netloc
            domain2 = urlparse(url2).netloc
            
            # Remove 'www.' prefix if present for comparison
            if domain1.startswith('www.'):
                domain1 = domain1[4:]
            if domain2.startswith('www.'):
                domain2 = domain2[4:]
                
            return domain1 == domain2
        except Exception as e:
            self.logger.error(f"Error comparing domains: {str(e)}")
            return False

    def _batch_process_data(self, data_batch: List[Dict]):
        """Process a batch of extracted data efficiently"""
        try:
            with self.db_manager.session() as session:
                # Group by manufacturer to minimize database queries
                manufacturer_data = {}
                for item in data_batch:
                    mfr_name = item['data'].get('manufacturer') or item['data'].get('name')
                    if mfr_name not in manufacturer_data:
                        manufacturer_data[mfr_name] = {
                            'website': item['data'].get('website'),
                            'categories': set(),
                            'urls': []
                        }
                    manufacturer_data[mfr_name]['categories'].update(
                        item['data'].get('categories', [])
                    )
                    manufacturer_data[mfr_name]['urls'].append(item['url'])
                
                # Batch create/update manufacturers and categories
                for mfr_name, mfr_info in manufacturer_data.items():
                    try:
                        manufacturer = self._get_or_create_manufacturer(
                            session, 
                            name=mfr_name,
                            website=mfr_info['website']
                        )
                        
                        # Batch create categories
                        categories = []
                        for category_name in mfr_info['categories']:
                            try:
                                category = self._get_or_create_category(
                                    session,
                                    manufacturer=manufacturer,
                                    name=category_name
                                )
                                categories.append(category)
                            except Exception as e:
                                self.logger.warning(
                                    f"Error processing category {category_name}: {str(e)}"
                                )
                                continue
                        
                        # Batch update statistics
                        if categories:
                            self.statistics_manager.update_multiple_stats({
                                "categories_extracted": len(categories),
                                "manufacturers_found": 1
                            })
                        
                        # Update URL priorities
                        if len(categories) > 5:
                            for url in mfr_info['urls']:
                                self._update_url_priority(url, session, priority_boost=-5)
                        
                        # Update manufacturer statistics
                        self.statistics_manager.update_multiple_stats({
                            "manufacturers_extracted": 1,
                            "websites_found": 1 if mfr_info['website'] else 0
                        })
                            
                    except Exception as e:
                        self.logger.error(f"Error processing manufacturer {mfr_name}: {str(e)}")
                        continue
                
                # Commit all changes
                try:
                    session.commit()
                except Exception as e:
                    self.logger.error(f"Error committing batch: {str(e)}")
                    session.rollback()
                    raise
                    
        except Exception as e:
            self.logger.error(f"Error in batch data processing: {str(e)}")
            if 'session' in locals():
                session.rollback()

    def _get_or_create_manufacturer(self, session, name: str, website: str = None) -> Manufacturer:
        """Get or create a manufacturer record with caching"""
        cache_key = name.lower()
        if cache_key in self._manufacturer_cache:
            return self._manufacturer_cache[cache_key]
            
        manufacturer = session.query(Manufacturer).filter_by(name=name).first()
        if not manufacturer:
            manufacturer = Manufacturer(name=name, website=website)
            session.add(manufacturer)
            session.flush()
        
        self._manufacturer_cache[cache_key] = manufacturer
        return manufacturer

    def _get_or_create_category(self, session, manufacturer: Manufacturer, name: str) -> Category:
        """Get or create a category record with caching and proper error handling"""
        try:
            # First check the cache
            cache_key = f"{manufacturer.id}:{name.lower()}"
            if cache_key in self._category_cache:
                return self._category_cache[cache_key]
            
            # Look for an existing category with this name using case-insensitive search
            try:
                category = session.query(Category).filter(
                    func.lower(Category.name) == func.lower(name)
                ).first()
                
                if not category:
                    # Create new category with proper error handling
                    category = Category(name=name)
                    session.add(category)
                    try:
                        session.flush()  # Get the ID
                        self.logger.debug(f"Created new category: {name}")
                        self.statistics_manager.increment_stat('categories_extracted')
                    except SQLAlchemyError as flush_err:
                        session.rollback()
                        self.logger.warning(f"Error creating category '{name}': {str(flush_err)}")
                        raise
                
                # Handle many-to-many relationship
                if category not in manufacturer.categories:
                    manufacturer.categories.append(category)
                    self.logger.debug(f"Associated category '{name}' with manufacturer '{manufacturer.name}'")
                
                # Update cache and return
                self._category_cache[cache_key] = category
                return category
                
            except SQLAlchemyError as db_err:
                self.logger.error(f"Database error in _get_or_create_category for '{name}': {str(db_err)}")
                raise
                
        except Exception as e:
            self.logger.error(f"Unexpected error in _get_or_create_category for '{name}': {str(e)}")
            raise

    def _update_url_priority(self, url: str, session, priority_boost: float):
        """Update the priority of a URL based on its content value"""
        try:
            # Update in database
            crawl_status = session.query(CrawlStatus).filter_by(url=url).first()
            if crawl_status and crawl_status.priority:
                crawl_status.priority += priority_boost
                session.commit()
            
            # Update in queue if present
            if url in self.url_queue:
                self.url_queue.update_priority(url, priority_boost)
                
        except Exception as e:
            self.logger.error(f"Error updating URL priority for {url}: {str(e)}")
            session.rollback()

    def _load_initial_urls(self) -> None:
        """
        Load initial seed URLs from the configuration.
        These are the starting points for the crawler.
        """
        initial_urls = self.config['scraper'].get('initial_urls', [])
        if not initial_urls:
            initial_urls = ['https://www.manualslib.com']  # Default seed URL
            
        self.logger.info(f"Adding {len(initial_urls)} initial URLs to the queue")
        
        for url in initial_urls:
            if url not in self.url_queue:
                # Use push() with depth 0 for initial URLs
                self.url_queue.push(url, depth=0)
                
        self.logger.info(f"Added {len(initial_urls)} initial URLs to the queue")

    def _load_unvisited_urls(self) -> None:
        """
        Load unvisited URLs from the database.
        These are URLs that were discovered in previous runs but not fully processed.
        """
        try:
            with self.db_manager.session() as session:
                unvisited_urls = session.query(CrawlStatus).filter(
                    CrawlStatus.visited == False  # Use visited flag instead of status
                ).all()
                
                if unvisited_urls:
                    self.logger.info(f"Loading {len(unvisited_urls)} unvisited URLs from database")
                    for url_status in unvisited_urls:
                        if url_status.url not in self.url_queue:
                            # Use stored depth if available, otherwise default to 1
                            depth = url_status.depth if url_status.depth is not None else 1
                            self.url_queue.push(url_status.url, depth=depth)
                            
                    self.logger.info(f"Loaded {len(unvisited_urls)} unvisited URLs from database")
                else:
                    self.logger.info("No unvisited URLs found in database")
                
        except Exception as e:
            self.logger.error(f"Error loading unvisited URLs: {str(e)}")
            if 'session' in locals():
                session.rollback()
        finally:
            if 'session' in locals():
                session.close()

    def check_shutdown_requested(self) -> bool:
        """Check if shutdown was requested."""
        return self.shutdown_requested
        
    def request_shutdown(self):
        """Request a graceful shutdown of the crawler."""
        self.logger.info("Shutdown requested")
        self.shutdown_requested = True
        
    def start_crawling(self, max_pages: Optional[int] = None, max_runtime_minutes: Optional[int] = None):
        """
        Start the crawling process.
        
        Args:
            max_pages: Maximum number of pages to crawl (None for unlimited)
            max_runtime_minutes: Maximum runtime in minutes (None for unlimited)
        """
        # Create a new session in statistics manager
        self.statistics_manager.create_session()
        self.session_id = self.statistics_manager.session_id
        
        # Update session parameters
        self.statistics_manager.update_multiple_stats({
            "max_pages": max_pages,
            "max_runtime_minutes": max_runtime_minutes
        })
        
        # Reset shutdown flag
        self.shutdown_requested = False
        
        # Load initial URLs if queue is empty
        if len(self.url_queue) == 0:
            self._load_initial_urls()
            self._load_unvisited_urls()
        
        self.logger.info(f"Starting crawler with URL queue of {len(self.url_queue)} URLs")
        self.logger.info(f"Scraper ID: {self.session_uuid}, Statistics Session ID: {self.session_id}")
        
        # Initialize statistics
        stats = {
            "start_time": time.time(),
            "pages_visited": 0,
            "manufacturers_found": 0,
            "categories_found": 0,
            "errors": 0
        }
        
        # Update statistics manager with initial stats
        self.statistics_manager.update_multiple_stats({
            "start_time": datetime.datetime.now(),  
            "status": "running"
        })
        
        try:
            # Process pages in batches for better control
            batch_size = self.config['scraper'].get('batch_size', 10)
            pages_remaining = max_pages if max_pages is not None else float('inf')
            
            while pages_remaining > 0 and len(self.url_queue) > 0:
                # Check if shutdown was requested
                if self.check_shutdown_requested():
                    self.logger.info("Shutdown requested, stopping crawler")
                    # Update status before breaking
                    self.statistics_manager.update_stat('status', 'stopped')
                    break
                
                # Process a batch of pages
                current_batch_size = min(batch_size, pages_remaining) if max_pages is not None else batch_size
                
                batch_stats = self.process_batch(max_pages=current_batch_size)
                
                # Update overall stats
                stats["pages_visited"] += batch_stats["pages_processed"]
                stats["manufacturers_found"] += batch_stats["manufacturers_found"]
                stats["categories_found"] += batch_stats["categories_found"]
                stats["errors"] += batch_stats["errors"]
                
                # Update progress
                if max_pages is not None:
                    pages_remaining -= batch_stats["pages_processed"]
                    progress = ((max_pages - pages_remaining) / max_pages) * 100
                    self.logger.info(f"Progress: {progress:.1f}% ({max_pages - pages_remaining}/{max_pages} pages)")
                
                # Check runtime limit
                if max_runtime_minutes is not None:
                    elapsed_minutes = (time.time() - stats["start_time"]) / 60
                    if elapsed_minutes >= max_runtime_minutes:
                        self.logger.info(f"Maximum runtime of {max_runtime_minutes} minutes reached")
                        break
            
            # Update final statistics
            end_time = datetime.datetime.now()
            runtime_seconds = int((time.time() - stats["start_time"]))
            
            self.statistics_manager.update_multiple_stats({
                "end_time": end_time,
                "runtime_seconds": runtime_seconds,
                "status": "completed",
                "urls_processed": stats["pages_visited"],
                "manufacturers_found": stats["manufacturers_found"],
                "categories_extracted": stats["categories_found"],
                "errors": stats["errors"]
            })
            
            self.logger.info("Crawling completed successfully")
            self.logger.info(f"Pages visited: {stats['pages_visited']}")
            self.logger.info(f"Manufacturers found: {stats['manufacturers_found']}")
            self.logger.info(f"Categories found: {stats['categories_found']}")
            self.logger.info(f"Errors: {stats['errors']}")
            self.logger.info(f"Runtime: {runtime_seconds} seconds")
            
        except Exception as e:
            self.logger.error(f"Error in crawler: {str(e)}")
            self.statistics_manager.update_multiple_stats({
                "status": "error",
                "error": str(e),
                "end_time": datetime.datetime.now()
            })
            raise
    
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

    def process_batch(self, max_pages: int) -> Dict[str, int]:
        """
        Process a batch of URLs from the queue.
        
        Args:
            max_pages: Maximum number of pages to process in this batch
            
        Returns:
            Dictionary containing batch statistics
        """
        batch_stats = {
            "pages_processed": 0,
            "manufacturers_found": 0,
            "categories_found": 0,
            "errors": 0
        }
        
        try:
            for _ in range(max_pages):
                if len(self.url_queue) == 0:
                    break
                    
                # Get next URL with highest priority
                url, depth, metadata = self.url_queue.pop()
                
                try:
                    # Process the URL
                    page_stats = self._process_url(url, depth)
                    
                    # Update batch statistics
                    batch_stats["pages_processed"] += 1
                    batch_stats["manufacturers_found"] += page_stats.get("manufacturers_found", 0)
                    batch_stats["categories_found"] += page_stats.get("categories_found", 0)
                    batch_stats["errors"] += page_stats.get("errors", 0)
                    
                    # Update statistics manager
                    self.statistics_manager.update_multiple_stats({
                        "pages_crawled": 1,
                        "manufacturers_found": page_stats.get("manufacturers_found", 0),
                        "categories_extracted": page_stats.get("categories_found", 0),
                        "errors": page_stats.get("errors", 0)
                    })
                    
                except Exception as e:
                    self.logger.error(f"Error processing URL {url}: {str(e)}")
                    batch_stats["errors"] += 1
                    self.statistics_manager.increment_stat("errors")
                    continue
                
                # Throttle requests
                time.sleep(self.min_request_interval)
                
        except Exception as e:
            self.logger.error(f"Error in batch processing: {str(e)}")
            batch_stats["errors"] += 1
            self.statistics_manager.increment_stat("errors")
            
        return batch_stats
    
    def _process_url(self, url: str, depth: int) -> Dict[str, int]:
        """
        Process a single URL, analyzing it for manufacturers and categories.
        
        Args:
            url: URL to process
            depth: Current depth in the crawl
            
        Returns:
            Dictionary containing statistics for this URL
        """
        stats = {
            "manufacturers_found": 0,
            "categories_found": 0,
            "errors": 0
        }
        
        try:
            # First check if URL has already been processed
            with self.db_manager.session() as session:
                url_status = session.query(CrawlStatus).filter(CrawlStatus.url == url).first()
                if url_status and url_status.visited:
                    self.logger.debug(f"URL {url} already processed, skipping")
                    return stats
            
            # Fetch and parse the page
            response = self._fetch_url(url)
            if not response:
                stats["errors"] += 1
                return stats
                
            # Quick check for manufacturer indicators
            if not self._has_manufacturer_indicators(response):
                self.logger.debug(f"URL {url} has no manufacturer indicators, skipping")
                self._mark_url_visited(url)
                return stats
            
            # Analyze with Claude for manufacturer detection
            is_manufacturer = self._analyze_manufacturer_page(response)
            if not is_manufacturer:
                self._mark_url_visited(url)
                return stats
                
            # Extract manufacturer info and categories
            mfr_info = self._extract_manufacturer_info(response)
            if mfr_info:
                stats["manufacturers_found"] += 1
                categories = self._extract_categories(response)
                if categories:
                    stats["categories_found"] = len(categories)
                    
                # Store manufacturer and categories
                self._store_manufacturer_data(mfr_info, categories)
                
                # Extract and queue additional URLs if within depth limit
                if depth < self.max_depth:
                    new_urls = self._extract_urls(response)
                    for new_url in new_urls:
                        if new_url not in self.url_queue:
                            self.url_queue.push(new_url, depth=depth+1)
            
            # Mark URL as visited
            self._mark_url_visited(url)
            
        except Exception as e:
            self.logger.error(f"Error processing URL {url}: {str(e)}")
            stats["errors"] += 1
            
        return stats
    
    def _fetch_url(self, url: str) -> Optional[requests.Response]:
        """
        Fetch a URL with error handling and retries.
        
        Args:
            url: URL to fetch
            
        Returns:
            Response object if successful, None otherwise
        """
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error fetching {url}: {str(e)}")
            self.statistics_manager.increment_stat("http_errors")
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error fetching {url}: {str(e)}")
            self.statistics_manager.increment_stat("connection_errors")
        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout fetching {url}: {str(e)}")
            self.statistics_manager.increment_stat("timeout_errors")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching {url}: {str(e)}")
            self.statistics_manager.increment_stat("errors")
        return None

    def _has_manufacturer_indicators(self, response: requests.Response) -> bool:
        """
        Quick check for manufacturer indicators in page content.
        
        Args:
            response: Response object from requests
            
        Returns:
            True if page has manufacturer indicators, False otherwise
        """
        # Extract title and metadata
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.title.string.lower() if soup.title else ""
        meta_desc = soup.find('meta', {'name': 'description'})
        meta_desc = meta_desc['content'].lower() if meta_desc else ""
        
        # Keywords that indicate a manufacturer page
        indicators = ['manufacturer', 'brand', 'company', 'about us', 'products']
        
        # Check title and meta description
        for indicator in indicators:
            if indicator in title or indicator in meta_desc:
                return True
                
        return False

    def _analyze_manufacturer_page(self, response: requests.Response) -> bool:
        """
        Analyze page content with Claude to determine if it's a manufacturer page.
        
        Args:
            response: Response object from requests
            
        Returns:
            True if page is determined to be a manufacturer page, False otherwise
        """
        try:
            # Extract relevant content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Get title and metadata
            title = soup.title.string if soup.title else ""
            meta_desc = soup.find('meta', {'name': 'description'})
            meta_desc = meta_desc['content'] if meta_desc else ""
            
            # Get main content sections
            main_content = []
            for tag in ['h1', 'h2', 'h3']:
                headers = soup.find_all(tag)
                main_content.extend([h.get_text() for h in headers])
            
            # Analyze with Claude
            analysis = self._analyze_with_claude({
                'url': response.url,
                'title': title,
                'meta_description': meta_desc,
                'main_content': main_content
            })
            
            return analysis.get('is_manufacturer', False)
            
        except Exception as e:
            self.logger.error(f"Error analyzing manufacturer page: {str(e)}")
            return False

    def _extract_manufacturer_info(self, response: requests.Response) -> Optional[Dict]:
        """
        Extract manufacturer information from page content.
        
        Args:
            response: Response object from requests
            
        Returns:
            Dictionary containing manufacturer info if successful, None otherwise
        """
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract basic info
            info = {
                'name': self._extract_manufacturer_name(soup),
                'website': response.url,
                'description': self._extract_description(soup),
                'logo_url': self._extract_logo_url(soup),
                'contact_info': self._extract_contact_info(soup)
            }
            
            return info if info['name'] else None
            
        except Exception as e:
            self.logger.error(f"Error extracting manufacturer info: {str(e)}")
            return None

    def _extract_categories(self, response: requests.Response) -> List[str]:
        """
        Extract product categories from page content.
        
        Args:
            response: Response object from requests
            
        Returns:
            List of category names
        """
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            categories = set()
            
            # Look for category links in navigation
            nav_elements = soup.find_all(['nav', 'ul', 'div'], class_=lambda x: x and 'nav' in x.lower())
            for nav in nav_elements:
                links = nav.find_all('a')
                for link in links:
                    text = link.get_text().strip()
                    if text and len(text) < 50:  # Avoid long text that's probably not a category
                        categories.add(text)
            
            return list(categories)
            
        except Exception as e:
            self.logger.error(f"Error extracting categories: {str(e)}")
            return []

    def _store_manufacturer_data(self, mfr_info: Dict, categories: List[str]) -> None:
        """
        Store manufacturer and category data in the database.
        
        Args:
            mfr_info: Dictionary containing manufacturer information
            categories: List of category names
        """
        try:
            with self.db_manager.session() as session:
                # Create or update manufacturer
                manufacturer = session.query(Manufacturer).filter(
                    Manufacturer.name == mfr_info['name']
                ).first()
                
                if not manufacturer:
                    manufacturer = Manufacturer(
                        name=mfr_info['name'],
                        website=mfr_info['website'],
                        description=mfr_info['description'],
                        logo_url=mfr_info['logo_url'],
                        contact_info=json.dumps(mfr_info['contact_info'])
                    )
                    session.add(manufacturer)
                    session.flush()
                
                # Create or update categories
                for category_name in categories:
                    category = session.query(Category).filter(
                        Category.name == category_name
                    ).first()
                    
                    if not category:
                        category = Category(name=category_name)
                        session.add(category)
                        session.flush()
                    
                    # Link manufacturer to category if not already linked
                    if category not in manufacturer.categories:
                        manufacturer.categories.append(category)
                
                session.commit()
                
        except Exception as e:
            self.logger.error(f"Error storing manufacturer data: {str(e)}")

    def _mark_url_visited(self, url: str) -> None:
        """
        Mark a URL as visited in the database.
        
        Args:
            url: URL to mark as visited
        """
        try:
            with self.db_manager.session() as session:
                url_status = session.query(CrawlStatus).filter(
                    CrawlStatus.url == url
                ).first()
                
                if url_status:
                    url_status.visited = True
                    url_status.last_visited = datetime.datetime.now()
                else:
                    url_status = CrawlStatus(
                        url=url,
                        visited=True,
                        last_visited=datetime.datetime.now()
                    )
                    session.add(url_status)
                
                session.commit()
                
        except Exception as e:
            self.logger.error(f"Error marking URL as visited: {str(e)}")

    def _extract_urls(self, response: requests.Response) -> List[str]:
        """
        Extract URLs from page content.
        
        Args:
            response: Response object from requests
            
        Returns:
            List of URLs
        """
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            base_url = response.url
            urls = set()
            
            for link in soup.find_all('a', href=True):
                url = urljoin(base_url, link['href'])
                if self._is_valid_url(url):
                    urls.add(url)
            
            return list(urls)
            
        except Exception as e:
            self.logger.error(f"Error extracting URLs: {str(e)}")
            return []

    def _is_valid_url(self, url: str) -> bool:
        """
        Check if a URL is valid and should be crawled.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL is valid, False otherwise
        """
        try:
            parsed = urlparse(url)
            return all([
                parsed.scheme in ['http', 'https'],
                parsed.netloc,
                not any(ext in parsed.path.lower() for ext in self.excluded_extensions),
                not any(term in url.lower() for term in self.excluded_terms)
            ])
        except Exception:
            return False

    def _extract_manufacturer_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract manufacturer name from page content."""
        # Try common locations for manufacturer name
        locations = [
            soup.find('meta', {'property': 'og:site_name'}),
            soup.find('meta', {'name': 'author'}),
            soup.find('h1'),
            soup.find('title')
        ]
        
        for loc in locations:
            if loc:
                name = loc.get('content', '') if loc.name == 'meta' else loc.get_text()
                name = name.strip()
                if name:
                    return name
        return None

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract manufacturer description from page content."""
        # Try common locations for description
        locations = [
            soup.find('meta', {'name': 'description'}),
            soup.find('meta', {'property': 'og:description'}),
            soup.find(class_=lambda x: x and 'about' in x.lower())
        ]
        
        for loc in locations:
            if loc:
                desc = loc.get('content', '') if loc.name == 'meta' else loc.get_text()
                desc = desc.strip()
                if desc:
                    return desc
        return None

    def _extract_logo_url(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract manufacturer logo URL from page content."""
        # Try common locations for logo
        locations = [
            soup.find('meta', {'property': 'og:image'}),
            soup.find('link', {'rel': 'icon'}),
            soup.find('img', class_=lambda x: x and 'logo' in x.lower())
        ]
        
        for loc in locations:
            if loc:
                url = loc.get('content') or loc.get('href') or loc.get('src')
                if url:
                    return urljoin(self.base_url, url)
        return None

    def _extract_contact_info(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract manufacturer contact information from page content."""
        contact_info = {}
        
        # Look for contact information in common locations
        contact_section = soup.find(class_=lambda x: x and 'contact' in x.lower())
        if contact_section:
            # Extract email
            email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
            emails = re.findall(email_pattern, contact_section.get_text())
            if emails:
                contact_info['email'] = emails[0]
            
            # Extract phone
            phone_pattern = r'\+?[\d\s-]{10,}'
            phones = re.findall(phone_pattern, contact_section.get_text())
            if phones:
                contact_info['phone'] = phones[0].strip()
            
            # Extract address
            address_elem = contact_section.find(class_=lambda x: x and 'address' in x.lower())
            if address_elem:
                contact_info['address'] = address_elem.get_text().strip()
        
        return contact_info

    def _analyze_with_claude(self, content: Dict) -> Dict[str, Any]:
        """
        Analyze page content with Claude to determine page type and extract information.
        
        Args:
            content: Dictionary containing page content to analyze
            
        Returns:
            Dictionary containing analysis results
        """
        try:
            # Load prompt template
            with open(os.path.join(os.path.dirname(__file__), 'prompt_templates.py')) as f:
                prompt_templates = {}
                exec(f.read(), prompt_templates)
            
            # Format prompt with content
            prompt = prompt_templates['MANUFACTURER_ANALYSIS_PROMPT'].format(
                url=content['url'],
                title=content['title'],
                meta_description=content['meta_description'],
                main_content='\n'.join(content['main_content'])
            )
            
            # Get response from Claude
            response = self.claude_analyzer.analyze(prompt)
            
            # Parse response
            try:
                result = response.splitlines()
                return {
                    'is_manufacturer': 'yes' in result,
                    'is_category': 'category' in result,
                    'manufacturer_info': result[2] if len(result) > 2 else '',
                    'categories': result[3:] if len(result) > 3 else []
                }
            except Exception:
                self.logger.error("Failed to parse Claude response")
                return {'is_manufacturer': False}
                
        except Exception as e:
            self.logger.error(f"Error in Claude analysis: {str(e)}")
            return {'is_manufacturer': False}

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
        stats = scraper.process_batch(max_pages=args.max_pages)
        
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