# Example usage
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create crawler
    crawler = WebCrawler()
    
    # Define callbacks
    def on_content_extracted(url, depth, title, content, metadata):
        print(f"Processed: {url} (depth {depth}) - {title}")
        
    # Set callbacks
    crawler.callbacks['on_content_extracted'] = [on_content_extracted]
    
    # Configure logging to file
    log_file = os.environ.get('NDAIVI_LOG_FILE', '/var/ndaivimanuales/logs/ndaivi.log')
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(file_handler)
    
    # Get target website from environment variable or use default
    target_website = os.environ.get('NDAIVI_TARGET_WEBSITE', 'manualslib.com')
    max_urls = int(os.environ.get('NDAIVI_MAX_URLS', '1000'))
    
    # Set more verbose logging
    logging.getLogger().setLevel(logging.DEBUG)
    
    # Start crawling with https protocol
    start_url = f"https://{target_website}"
    logging.info(f"Starting crawl of {start_url} with max_urls={max_urls}")
    print(f"Starting crawl of {start_url} with max_urls={max_urls}")
    
    # Initialize database and crawler state
    db_path = os.environ.get('NDAIVI_DB_PATH', '/var/ndaivimanuales/scraper/data/crawler.db')
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Add direct request testing for the target website
    try:
        print(f"Testing direct HTTP connection to {start_url}...")
        logging.info(f"Testing direct HTTP connection to {start_url}...")
        test_response = requests.get(start_url, timeout=30, 
                                   headers={'User-Agent': 'Mozilla/5.0 (compatible; NDaiviBot/1.0; +https://ndaivi.com/bot)'})
        logging.info(f"Direct connection test result: Status {test_response.status_code}, Content-Type: {test_response.headers.get('Content-Type')}")
        print(f"Connection successful! Status: {test_response.status_code}, Length: {len(test_response.text)} bytes")
        
        # Save sample of the content for debugging
        content_preview = test_response.text[:500] + '...' if len(test_response.text) > 500 else test_response.text
        logging.debug(f"Content preview: {content_preview}")
        
        # Extract sample links for debugging
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(test_response.text, 'html.parser')
        sample_links = []
        for link in soup.find_all('a', href=True)[:10]:
            full_url = urljoin(start_url, link['href'])
            sample_links.append(full_url)
            print(f"Found link: {full_url}")
            
        logging.info(f"Sample links: {sample_links}")
    except Exception as e:
        logging.error(f"Failed to connect to {start_url}: {str(e)}")
        print(f"Error connecting to {start_url}: {str(e)}")
    
    # Setup callbacks for detailed logging
    def on_url_added(url, priority, depth):
        logging.info(f"Added URL to queue: {url} (priority: {priority}, depth: {depth})")
        print(f"Queue: Added {url}")
    
    def on_url_fetched(url, status_code, content_type):
        logging.info(f"Fetched URL: {url} (status: {status_code}, type: {content_type})")
        print(f"Fetched: {url} - Status {status_code}")
    
    def on_content_extracted(url, depth, title, content, metadata):
        logging.info(f"Extracted content from: {url} (depth: {depth}, title: {title})")
        print(f"Processed: {url} - {title}")
    
    def on_error(url, error):
        logging.error(f"Error processing {url}: {error}")
        print(f"Error: {url} - {error}")
    
    # Set all callbacks
    crawler.callbacks['on_url_added'] = [on_url_added]
    crawler.callbacks['on_url_fetched'] = [on_url_fetched]
    crawler.callbacks['on_content_extracted'] = [on_content_extracted]
    crawler.callbacks['on_error'] = [on_error]
    
    # Force single domain mode for manualslib.com
    crawler.config['single_domain_mode'] = True
    crawler.config['allowed_domains'] = [target_website]
    crawler.config['max_urls'] = max_urls
    crawler.config['max_depth'] = 5
    crawler.config['request_delay'] = 1.0
    crawler.config['user_agent'] = 'Mozilla/5.0 (compatible; NDaiviBot/1.0; +https://ndaivi.com/bot)'
    
    # Initialize with sample links (if we found them in our test)
    if 'sample_links' in locals() and sample_links:
        for link in sample_links:
            crawler.add_url(link, depth=1, priority=0.8)
            
    # Add original start URL
    crawler.add_url(start_url, depth=0, priority=1.0)
    
    # Start crawling with increased limits
    crawler.crawl(start_url=None, max_urls=max_urls)  # Don't pass start_url again - we already added it
    
    # Print stats
    print("Crawl complete!")
    stats = crawler.get_stats()
    print(f"URLs processed: {stats['urls_processed']}")
    print(f"URLs failed: {stats['urls_failed']}")
    print(f"Domains visited: {stats['domains_discovered']}")
    print(f"Duration: {stats['elapsed_time']:.2f} seconds")#!/usr/bin/env python3
"""
Web Crawler for NDAIVI

This module provides a standalone web crawler implementation that can be used
independently from the competitor scraper. It handles URL management, content
extraction, and crawling logic.

Features:
- Priority-based URL queue
- Domain validation and restrictions
- Configurable crawl depth and limits
- Robust error handling and logging
- Session management
- SQLite database for state persistence
- Stats events for monitoring
"""

import json
import logging
import os
import re
import time
import threading
import signal
import heapq
import requests
import select
from urllib.parse import urlparse, urljoin, ParseResult
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple, Optional, Any, Union, Set
import urllib.robotparser
import sys
import traceback

# Import the new modules
from db_manager import CrawlDatabase, DomainManager
try:
    from stats_manager import StatsManager
except ImportError:
    # Create a simple StatsManager if not available
    class StatsManager:
        def __init__(self, *args, **kwargs):
            pass
        def record_event(self, *args, **kwargs):
            pass

# Add the parent directory to the path to import from utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config_manager import ConfigManager

# Global flags for signal handling
SHUTDOWN_FLAG = False
SUSPEND_FLAG = False

def handle_interrupt(signum, frame):
    """
    Handle keyboard interrupt (Ctrl+C) gracefully.
    
    Args:
        signum: Signal number
        frame: Current stack frame
    """
    global SHUTDOWN_FLAG
    print("\nShutdown signal received. Stopping gracefully...")
    SHUTDOWN_FLAG = True

def handle_suspend(signum, frame):
    """
    Handle suspension signal (SIGUSR1) to pause processing.
    
    Args:
        signum: Signal number
        frame: Current stack frame
    """
    global SUSPEND_FLAG
    SUSPEND_FLAG = not SUSPEND_FLAG
    status = "suspended" if SUSPEND_FLAG else "resumed"
    print(f"\nCrawler {status}.")

# Register global signal handlers
signal.signal(signal.SIGINT, handle_interrupt)  # Ctrl+C
signal.signal(signal.SIGTERM, handle_interrupt)  # kill command
signal.signal(signal.SIGUSR1, handle_suspend)  # SIGUSR1 for suspension


class VisitedUrlManager:
    """
    Manager for visited URLs in the crawler database.
    """
    
    def __init__(self, db: CrawlDatabase, logger=None):
        """
        Initialize the visited URL manager.
        
        Args:
            db: Database manager instance
            logger: Logger instance for logging activities
        """
        self.db = db
        self.logger = logger or logging.getLogger(__name__)
        self._cache = set()  # In-memory cache for faster lookups
    
    def add(self, url: str, domain: str, depth: int, status_code: int, 
            content_type: str = None, content_length: int = 0, title: str = None, content: str = None, headers: str = None) -> bool:
        """
        Add a URL to the visited URLs list.
        
        Args:
            url: URL that was visited
            domain: Domain of the URL
            depth: Depth at which the URL was visited
            status_code: HTTP status code from the visit
            content_type: Content type of the response
            content_length: Content length in bytes
            title: Title of the page
            content: Page content
            headers: HTTP headers
            
        Returns:
            True if URL was added, False otherwise
        """
        try:
            # Add to in-memory cache
            self._cache.add(url)
            
            # Add to database
            self.db.execute('''
                INSERT INTO pages 
                (url, domain, status_code, content_type, content_length, title, content, headers, crawled_at) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (url, domain, status_code, content_type, content_length, title, content, headers))
            
            # Update domain stats
            self.db.execute('''
                UPDATE domains SET 
                last_seen = CURRENT_TIMESTAMP,
                robots_checked = 1
                WHERE domain = ?
            ''', (domain,))
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to add visited URL {url}: {e}")
            return False
    
    def exists(self, url: str) -> bool:
        """
        Check if a URL exists in the visited URLs list.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL exists, False otherwise
        """
        # Check in-memory cache first for performance
        if url in self._cache:
            return True
        
        try:
            # Check database - using pages table
            self.db.execute('SELECT 1 FROM pages WHERE url = ? LIMIT 1', (url,))
            result = self.db.fetchone()
            
            # If found in database, add to cache
            if result:
                self._cache.add(url)
                return True
            
            return False
        except Exception as e:
            self.logger.error(f"Failed to check if URL {url} exists: {str(e)}")
            return False
    
    def get_unanalyzed_urls(self, limit: int = 10) -> List[Dict]:
        """
        Get a list of URLs that have been visited but not analyzed.
        
        Args:
            limit: Maximum number of URLs to return
            
        Returns:
            List of dictionaries with URL information
        """
        try:
            self.db.execute('''
                SELECT url, domain, status_code, content_type, content_length, title, content, headers, crawled_at
                FROM pages
                WHERE status_code = 200 AND analyzed = 0
                ORDER BY crawled_at ASC
                LIMIT ?
            ''', (limit,))
            
            results = []
            rows = self.db.fetchall()
            for row in rows:
                results.append({
                    'url': row[0],
                    'domain': row[1],
                    'status_code': row[2],
                    'content_type': row[3],
                    'content_length': row[4],
                    'title': row[5],
                    'content': row[6],
                    'headers': row[7],
                    'crawled_at': row[8]
                })
            return results
        except Exception as e:
            self.logger.error(f"Failed to get unanalyzed URLs: {e}")
            return []
    
    def mark_as_analyzed(self, url: str) -> bool:
        """
        Mark a URL as analyzed.
        
        Args:
            url: URL to mark as analyzed
            
        Returns:
            bool: True if URL was marked as analyzed, False otherwise
        """
        try:
            self.db.execute('UPDATE pages SET analyzed = 1 WHERE url = ?', (url,))
            return True
        except Exception as e:
            self.logger.error(f"Failed to mark URL {url} as analyzed: {e}")
            return False
    
    def get_stats(self) -> Dict:
        """
        Get statistics about visited URLs.
        
        Returns:
            Dictionary with statistics
        """
        stats = {
            'total': 0,
            'success': 0,
            'error': 0,
            'analyzed': 0,
            'unanalyzed': 0
        }
        
        try:
            self.db.execute('SELECT COUNT(*) FROM pages')
            stats['total'] = self.db.fetchone()[0]
            
            self.db.execute('SELECT COUNT(*) FROM pages WHERE status_code >= 200 AND status_code < 300')
            stats['success'] = self.db.fetchone()[0]
            
            self.db.execute('SELECT COUNT(*) FROM pages WHERE status_code >= 400 OR status_code = 0')
            stats['error'] = self.db.fetchone()[0]
            
            self.db.execute('SELECT COUNT(*) FROM pages WHERE analyzed = 1')
            stats['analyzed'] = self.db.fetchone()[0]
            
            self.db.execute('SELECT COUNT(*) FROM pages WHERE analyzed = 0 AND status_code = 200')
            stats['unanalyzed'] = self.db.fetchone()[0]
            
            return stats
        except Exception as e:
            self.logger.error(f"Failed to get visited URL stats: {e}")
            return stats


class UrlPriority:
    """
    Utility class for calculating URL priorities.
    
    This class provides methods to calculate the priority of a URL based on various factors
    such as URL structure, depth, and anchor text.
    """
    
    @staticmethod
    def calculate_priority(url, depth=0, anchor_text=None, source_url=None):
        """
        Calculate the priority of a URL.
        
        Args:
            url: URL to calculate priority for
            depth: Depth of the URL in the crawl tree
            anchor_text: Text of the anchor linking to this URL
            source_url: URL of the page containing the link
            
        Returns:
            float: Priority value (higher = more important)
        """
        priority = 1.0
        
        # Depth penalty: deeper URLs get lower priority
        depth_factor = max(1.0, 1.0 + depth * 0.1)
        priority /= depth_factor
        
        # URL structure bonus
        url_structure_bonus = UrlPriority._get_url_structure_bonus(url)
        priority *= url_structure_bonus
        
        # Anchor text bonus
        if anchor_text:
            anchor_bonus = UrlPriority._get_anchor_text_bonus(anchor_text)
            priority *= anchor_bonus
            
        # Source URL similarity bonus
        if source_url:
            similarity_bonus = UrlPriority._get_source_similarity_bonus(url, source_url)
            priority *= similarity_bonus
            
        return priority
        
    @staticmethod
    def _get_url_structure_bonus(url):
        """
        Calculate bonus based on URL structure.
        
        Args:
            url: URL to analyze
            
        Returns:
            float: Bonus factor
        """
        # Parse URL
        parsed = urlparse(url)
        path = parsed.path
        
        # Shorter paths are generally more important
        path_parts = [p for p in path.split('/') if p]
        path_depth = len(path_parts)
        
        # Calculate path depth penalty (shorter paths get higher priority)
        path_bonus = 1.0 / max(1.0, path_depth * 0.5)
        
        # Check for common important paths
        if path == '/' or path == '':
            path_bonus *= 2.0  # Homepage bonus
        elif path.endswith('/'):
            path_bonus *= 1.2  # Directory listing bonus
        elif any(path.endswith(ext) for ext in ['.html', '.htm', '.php', '.asp', '.aspx']):
            path_bonus *= 1.1  # HTML page bonus
            
        # Check for common low-value paths
        if any(part in path_parts for part in ['search', 'tag', 'category', 'archive']):
            path_bonus *= 0.8  # Lower priority for search/tag/category pages
        if any(path.endswith(ext) for ext in ['.js', '.css', '.jpg', '.jpeg', '.png', '.gif']):
            path_bonus *= 0.5  # Lower priority for assets
            
        # Check for query parameters
        if parsed.query:
            # URLs with many query parameters are often less important
            query_params = parsed.query.split('&')
            if len(query_params) > 3:
                path_bonus *= 0.7
                
        return path_bonus
        
    @staticmethod
    def _get_anchor_text_bonus(anchor_text):
        """
        Calculate bonus based on anchor text.
        
        Args:
            anchor_text: Text of the anchor
            
        Returns:
            float: Bonus factor
        """
        if not anchor_text:
            return 1.0
            
        # Clean and normalize anchor text
        text = anchor_text.lower().strip()
        
        # Empty anchor text
        if not text:
            return 0.9  # Slight penalty
            
        # Check for important keywords in anchor text
        important_keywords = ['home', 'index', 'main', 'about', 'contact', 'product', 'service', 'manual', 'guide']
        if any(keyword in text for keyword in important_keywords):
            return 1.5  # Bonus for important keywords
            
        # Check for navigational terms
        navigational = ['click here', 'read more', 'learn more', 'more info', 'details', 'next', 'previous']
        if any(nav in text for nav in navigational):
            return 0.8  # Penalty for generic navigational text
            
        # Longer anchor text often indicates more descriptive links
        if len(text) > 20:
            return 1.2  # Bonus for longer, more descriptive anchor text
            
        return 1.0
        
    @staticmethod
    def _get_source_similarity_bonus(url, source_url):
        """
        Calculate bonus based on similarity to source URL.
        
        Args:
            url: URL to analyze
            source_url: Source URL
            
        Returns:
            float: Bonus factor
        """
        # Parse URLs
        parsed_url = urlparse(url)
        parsed_source = urlparse(source_url)
        
        # Same domain bonus
        if parsed_url.netloc == parsed_source.netloc:
            # Same path prefix bonus
            source_path_parts = parsed_source.path.split('/')
            url_path_parts = parsed_url.path.split('/')
            
            # Calculate path similarity (how many path segments are shared)
            common_segments = 0
            for i in range(min(len(source_path_parts), len(url_path_parts))):
                if source_path_parts[i] == url_path_parts[i]:
                    common_segments += 1
                else:
                    break
                    
            # Calculate similarity bonus based on common path segments
            path_similarity = common_segments / max(1, len(source_path_parts))
            return 1.0 + path_similarity * 0.5
        else:
            # Different domain, no bonus
            return 0.8  # Slight penalty for external links


class PriorityUrlQueue:
    """
    Priority queue for URLs to be crawled.
    
    This queue maintains URLs ordered by priority (higher priority values are processed first).
    """
    
    def __init__(self):
        """
        Initialize the priority queue.
        """
        self.queue = []  # List of (priority, url, depth, metadata) tuples
        self.url_set = set()  # Set of URLs for O(1) existence check
        self.lock = threading.Lock()  # Thread safety lock
        
    def add(self, url, depth=0, priority=0.0, metadata=None):
        """
        Add a URL to the queue with the given priority.
        
        Args:
            url: URL to add
            depth: Depth of the URL in the crawl tree
            priority: Priority of the URL (higher = more important)
            metadata: Additional metadata about the URL
            
        Returns:
            bool: True if URL was added, False if it was already in the queue
        """
        with self.lock:
            # Check if URL is already in queue
            if url in self.url_set:
                return False
                
            # Add URL to queue
            heapq.heappush(self.queue, (-priority, url, depth, metadata))
            self.url_set.add(url)
            return True
            
    def pop(self):
        """
        Get the highest priority URL from the queue.
        
        Returns:
            tuple: (url, depth, metadata) or None if queue is empty
        """
        with self.lock:
            if not self.queue:
                return None
                
            # Get highest priority URL
            neg_priority, url, depth, metadata = heapq.heappop(self.queue)
            self.url_set.remove(url)
            
            return url, depth, metadata
            
    def is_empty(self):
        """
        Check if the queue is empty.
        
        Returns:
            bool: True if queue is empty, False otherwise
        """
        with self.lock:
            return len(self.queue) == 0
            
    def size(self):
        """
        Get the number of URLs in the queue.
        
        Returns:
            int: Number of URLs in the queue
        """
        with self.lock:
            return len(self.queue)
            
    def contains(self, url):
        """
        Check if a URL is already in the queue.
        
        Args:
            url: URL to check
            
        Returns:
            bool: True if URL is in the queue, False otherwise
        """
        with self.lock:
            return url in self.url_set
            
    def clear(self):
        """
        Clear the queue.
        """
        with self.lock:
            self.queue = []
            self.url_set = set()


class ContentExtractor:
    """
    Handles fetching and extracting content from web pages.
    """
    
    def __init__(self, user_agent=None, timeout=30, max_retries=3, logger=None, config=None):
        """
        Initialize the content extractor.
        
        Args:
            user_agent: User agent string to use for requests
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries for failed requests
            logger: Logger instance
            config: Configuration dictionary
        """
        self.user_agent = user_agent or 'Mozilla/5.0 (compatible; NDaiviBot/1.0; +https://ndaivi.com/bot)'
        self.timeout = timeout
        self.max_retries = max_retries
        self.logger = logger or logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.user_agent})
        self.config = config or {}
        
    def fetch(self, url):
        """
        Fetch content from a URL with robust error handling but preserving HTML structure.
        
        Args:
            url: URL to fetch
            
        Returns:
            dict: Response data including status code, content type, and content
        """
        # Create result with explicit non-None defaults for everything
        result = {
            'url': url,
            'status_code': 0,
            'content_type': '',
            'content_length': 0,
            'content': '',
            'headers': {},
            'error': None
        }
        
        # Add detailed debug logging
        self.logger.info(f"Starting fetch for URL: {url}")
        
        # Single try-except around the entire function
        try:
            # Make a basic request using the session
            response = self.session.get(
                url, 
                timeout=self.timeout,
                allow_redirects=True
            )
            
            # Set the status code
            result['status_code'] = response.status_code if response.status_code is not None else 0
            self.logger.debug(f"Response status code: {result['status_code']}")
            
            # Get and process headers
            if response.headers:
                result['headers'] = dict(response.headers)
                if 'Content-Type' in response.headers:
                    content_type = response.headers.get('Content-Type', '')
                    # Split only if it contains a semicolon
                    if ';' in content_type:
                        result['content_type'] = content_type.split(';')[0].strip()
                    else:
                        result['content_type'] = content_type
            
            # Get content length
            if response.content:
                result['content_length'] = len(response.content)
                self.logger.debug(f"Content length: {result['content_length']} bytes")
            
            # Store the actual HTML content - critical for link extraction
            if response.text:
                result['content'] = response.text
                self.logger.debug(f"Content extracted, length: {len(result['content'])}")
            
            # Success message with basic info
            self.logger.info(f"Successfully fetched URL {url} with status {result['status_code']}")
            
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            self.logger.error(f"Error fetching URL {url}: {type(e).__name__}: {str(e)}\n{tb}")
            result['error'] = str(e)
        
        return result
        
    def extract_links(self, content, base_url):
        """
        Extract links from HTML content.
        
        Args:
            content: HTML content to extract links from
            base_url: Base URL for resolving relative links
            
        Returns:
            list: List of extracted link URLs
        """
        if not content:
            self.logger.warning(f"No content to extract links from for {base_url}")
            return []
            
        # Get domain of base URL
        base_domain = urlparse(base_url).netloc if base_url else ''
        self.logger.info(f"Extracting links from content for {base_url} (domain: {base_domain})")
        
        links = []
        
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # Find all <a> tags
            a_tags = soup.find_all('a')
            self.logger.info(f"Found {len(a_tags)} <a> tags in content")
            
            # Extract href attributes
            for i, a in enumerate(a_tags):
                # Skip tags without href
                if not a.has_attr('href'):
                    continue
                    
                href = a['href']
                
                # Skip empty hrefs
                if not href or href.isspace():
                    continue
                    
                # Skip javascript: links and anchors
                if href.startswith('javascript:') or href.startswith('#'):
                    continue
                    
                # Skip mailto: links
                if href.startswith('mailto:'):
                    continue
                    
                # Normalize and resolve relative links
                # Handle basic URL normalization here to avoid filtering valid links
                href = href.strip()
                
                # Debug the link we're processing
                self.logger.info(f"Processing link #{i}: {href}")
                
                try:
                    # Resolve relative URLs
                    resolved_url = urljoin(base_url, href)
                    
                    # Parse the URL to validate components
                    parsed_url = urlparse(resolved_url)
                    
                    # Skip invalid URLs
                    if not parsed_url.scheme or not parsed_url.netloc:
                        self.logger.info(f"Skipping invalid URL: {resolved_url} (missing scheme or netloc)")
                        continue
                        
                    # Skip non-HTTP/HTTPS URLs
                    if parsed_url.scheme not in ['http', 'https']:
                        self.logger.info(f"Skipping non-HTTP/HTTPS URL: {resolved_url}")
                        continue
                    
                    # Add resolved URL to links list
                    links.append(resolved_url)
                    self.logger.info(f"Added link: {resolved_url}")
                    
                except Exception as e:
                    self.logger.error(f"Error resolving URL {href}: {str(e)}")
                    continue
            
            # Remove duplicates while maintaining order
            unique_links = []
            seen = set()
            for link in links:
                if link not in seen:
                    unique_links.append(link)
                    seen.add(link)
            
            self.logger.info(f"Extracted {len(unique_links)} unique links from {base_url}")
            return unique_links
            
        except Exception as e:
            self.logger.error(f"Error extracting links from {base_url}: {str(e)}")
            return []
            
    def extract_text(self, html_content):
        """
        Extract plain text from HTML content.
        
        Args:
            html_content: HTML content to extract text from
            
        Returns:
            str: Extracted text
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for element in soup(['script', 'style', 'head', 'title', 'meta', '[document]']):
                element.extract()
            
            # Get text
            text = soup.get_text()
            
            # Break into lines and remove leading and trailing space on each
            lines = (line.strip() for line in text.splitlines())
            
            # Break multi-headlines into a line each
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            
            # Drop blank lines
            text = '\n'.join(chunk for chunk in chunks if chunk)
            
            return text
            
        except Exception as e:
            self.logger.error(f"Error extracting text: {str(e)}")
            return ""


class WebCrawler:
    """
    Advanced web crawler with configurable behavior and prioritization.
    
    This crawler focuses on discovering links and storing them in a database.
    It does not perform content analysis, which is handled by external components.
    """
    
    def __init__(self, config=None, db=None, logger=None, stats_callback=None):
        """
        Initialize the web crawler.
        
        Args:
            config: Configuration dictionary
            db: Database instance
            logger: Logger instance
            stats_callback: Callback function for stats updates
        """
        # Set up logger
        self.logger = logger or logging.getLogger(__name__)
        
        # Set up configuration
        self.config = config or {}
        
        # Initialize database
        self.db = db
        self.use_database = self.config.get('use_database', True)
        
        # Initialize URL queue
        self.url_queue = PriorityUrlQueue()
        
        # Initialize URL queue manager if database is enabled
        if self.use_database and self.db:
            self.url_queue_manager = UrlQueueManager(self.db, self.logger)
        else:
            self.url_queue_manager = None
            
        # Initialize visited URLs manager
        self.visited_urls = set() if not self.use_database or not self.db else VisitedUrlManager(self.db, self.logger)
        
        # Initialize robots.txt cache
        self.robots_parsers = {}
        
        # Initialize backlog and lock
        self.backlog = []
        self.backlog_lock = threading.Lock()
        
        # Initialize session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config.get('user_agent', 'NDaiviBot/1.0'),
        })
        
        # Initialize content extractor
        self.content_extractor = ContentExtractor(
            user_agent=self.config.get('user_agent'),
            timeout=self.config.get('timeout'),
            max_retries=self.config.get('max_retries'),
            logger=self.logger,
            config=self.config
        )
        
        # Initialize URL priority calculator
        self.url_priority = UrlPriority()
        
        # Initialize state variables
        self.running = False
        self.paused = False
        self.request_delay_override = None  # Override for dynamic speed control
        self.start_time = None
        self.end_time = None
        self.stats = {
            'start_time': None,
            'end_time': None,
            'elapsed_time': 0,
            'urls_processed': 0,
            'urls_queued': 0,
            'urls_failed': 0,
            'domains_discovered': set(),
            'processing_times': []
        }
        
        # Initialize callbacks
        self.callbacks = {
            'on_url_processed': [],
            'on_content_extracted': [],
            'on_stats_updated': []
        }
        
        # Initialize start domain
        self.start_domain = None
        
        # Store the stats callback
        self.stats_callback = stats_callback
        
    def add_url(self, url, depth=0, priority=0.0, metadata=None):
        """
        Add a URL to the crawl queue.
        
        Args:
            url: URL to add
            depth: Depth of the URL in the crawl tree
            priority: Priority of the URL (higher = more important)
            metadata: Additional metadata about the URL
            
        Returns:
            bool: True if URL was added, False otherwise
        """
        # Normalize URL
        url = self._normalize_url(url)
        
        if not url:
            self.logger.debug(f"Skipping invalid URL: {url}")
            return False
            
        # Parse URL to get domain
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        # Set start domain on first URL if we don't have one
        if self.start_domain is None and depth == 0:
            self.start_domain = domain
            self.logger.info(f"Setting start domain to: {domain}")
            
        # Early return for invalid URLs
        if not domain:
            self.logger.warning(f"Could not parse domain from URL: {url}")
            return False
            
        # Check if URL has already been visited
        if self.visited_urls and isinstance(self.visited_urls, VisitedUrlManager) and self.visited_urls.exists(url):
            self.logger.debug(f"Skipping already visited URL: {url}")
            return False
            
        # Check if URL is already in the queue
        if self.url_queue.contains(url):
            self.logger.debug(f"Skipping URL already in queue: {url}")
            return False
            
        # Check if URL is already in the database queue
        if self.url_queue_manager and self.url_queue_manager.exists(url):
            self.logger.debug(f"Skipping URL already in database queue: {url}")
            return False
            
        # Check if domain is allowed
        if not self._is_domain_allowed(domain):
            self.logger.debug(f"Skipping URL with disallowed domain: {url}")
            return False
            
        # Check if URL is allowed
        if not self._is_url_allowed(url):
            self.logger.debug(f"Skipping disallowed URL: {url}")
            return False
            
        # Check if URL is allowed by robots.txt
        if not self.is_allowed_by_robots(url):
            self.logger.debug(f"Skipping URL disallowed by robots.txt: {url}")
            return False
            
        # Calculate priority if not provided
        if priority == 0.0:
            priority = self.calculate_priority(url, depth, metadata.get('anchor_text') if metadata else None, 
                                              metadata.get('source_url') if metadata else None)
            
        # Add to in-memory queue
        self.url_queue.add(url, depth, priority, metadata)
        
        # Add to database queue
        try:
            if self.url_queue_manager:
                source_url = metadata.get('source_url') if metadata else None
                anchor_text = metadata.get('anchor_text') if metadata else None
                self.url_queue_manager.add(url, domain, depth, priority, source_url, anchor_text, json.dumps(metadata) if metadata else None)
        except Exception as e:
            self.logger.error(f"Error adding URL to database queue: {str(e)}")
            
        # Increment the urls_queued counter
        self.stats['urls_queued'] += 1
        
        # Track domain
        if domain not in self.stats['domains_discovered']:
            self.stats['domains_discovered'].add(domain)
            
        self.logger.debug(f"Added URL to queue: {url} (depth: {depth}, priority: {priority:.2f})")
        return True
    
    def process_url(self, url: str, depth: int = 0) -> Dict:
        """
        Process a URL: fetch content, extract links, and update database.
        
        Args:
            url: URL to process
            depth: Depth of the URL in the crawl tree
            
        Returns:
            dict: Results of processing the URL
        """
        # Initialize result with default values
        result = {
            'url': url,
            'domain': '',
            'depth': depth,
            'status': 'error',
            'error': None,
            'links_extracted': 0,
            'links_added': 0,
            'content_type': '',
            'content_length': 0,
            'processing_time': 0,
            'status_code': 0,
            'title': '',
            'content': '',
            'links': []
        }
        
        # Initialize all stats entries we'll use to prevent KeyErrors
        if 'urls_processed' not in self.stats:
            self.stats['urls_processed'] = 0
        if 'urls_failed' not in self.stats:
            self.stats['urls_failed'] = 0
        if 'processing_times' not in self.stats:
            self.stats['processing_times'] = []
        if 'content_types' not in self.stats:
            self.stats['content_types'] = {}
        if 'status_codes' not in self.stats:
            self.stats['status_codes'] = {}
        
        start_time = time.time()
        
        try:
            # Parse URL to get domain
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            result['domain'] = domain
            
            # Log that we're processing this URL
            self.logger.info(f"Processing URL: {url} (depth: {depth}, domain: {domain})")
            
            # Check if URL is already visited
            if isinstance(self.visited_urls, VisitedUrlManager):
                try:
                    if self.visited_urls.exists(url):
                        self.logger.info(f"URL {url} already visited, skipping")
                        result['status'] = 'skipped'
                        result['error'] = 'URL already visited'
                        return result
                except Exception as e:
                    self.logger.error(f"Error checking if URL exists: {str(e)}")
            elif url in self.visited_urls:
                self.logger.info(f"URL {url} already visited, skipping")
                result['status'] = 'skipped'
                result['error'] = 'URL already visited'
                return result
                
            # Check if we've reached the max depth
            max_depth = self.config.get('max_depth')
            if max_depth is not None and depth > max_depth:
                self.logger.info(f"URL {url} exceeds max depth {max_depth}, skipping")
                result['status'] = 'skipped'
                result['error'] = f"Exceeds max depth {max_depth}"
                return result
                
            # Check robots.txt
            respect_robots = self.config.get('respect_robots_txt', False)
            if respect_robots:
                try:
                    if not self.is_allowed_by_robots(url):
                        self.logger.info(f"URL {url} disallowed by robots.txt, skipping")
                        result['status'] = 'skipped'
                        result['error'] = 'Disallowed by robots.txt'
                        return result
                except Exception as e:
                    self.logger.error(f"Error checking robots.txt: {str(e)}")
                
            # Fetch content - ensuring we have a valid response
            self.logger.info(f"Fetching content for URL: {url}")
            try:
                response = self.content_extractor.fetch(url)
                
                # Defensive check - ensure response is a dictionary with expected fields
                if not isinstance(response, dict):
                    response = {
                        'url': url,
                        'status_code': 0,
                        'content_type': '',
                        'content_length': 0,
                        'content': '',
                        'headers': {},
                        'error': 'Invalid response format'
                    }
            except Exception as fetch_error:
                self.logger.error(f"Error in fetch method: {str(fetch_error)}")
                response = {
                    'url': url,
                    'status_code': 0,
                    'content_type': '',
                    'content_length': 0,
                    'content': '',
                    'headers': {},
                    'error': str(fetch_error)
                }
            
            # Check for fetch errors
            if response.get('error'):
                self.logger.warning(f"Failed to fetch content for URL: {url} - {response.get('error')}")
                result['status'] = 'error'
                result['error'] = f"Failed to fetch content: {response.get('error')}"
                self.stats['urls_failed'] = int(self.stats.get('urls_failed', 0)) + 1
                return result
                
            # Get content type and length (with safe defaults)
            result['content_type'] = response.get('content_type', '')
            result['content_length'] = int(response.get('content_length', 0))
            result['status_code'] = int(response.get('status_code', 0))
            result['content'] = response.get('content', '')
            
            # Try to extract title
            try:
                if result['content'] and result['content_type'] and result['content_type'].startswith('text/html'):
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(result['content'], 'html.parser')
                    title_tag = soup.find('title')
                    if title_tag:
                        result['title'] = title_tag.text.strip()
            except Exception as e:
                self.logger.error(f"Error extracting title: {str(e)}")
            
            # Update stats for content type and status code
            content_type = result['content_type']
            if content_type:
                self.stats['content_types'][content_type] = self.stats['content_types'].get(content_type, 0) + 1
            
            status_code = result['status_code']
            if status_code:
                status_code_str = str(status_code)
                self.stats['status_codes'][status_code_str] = self.stats['status_codes'].get(status_code_str, 0) + 1
            
            # Extract links if this is HTML content
            content_type = response.get('content_type', '')
            if content_type and content_type.startswith('text/html'):
                self.logger.info(f"Extracting links from URL: {url}")
                content = response.get('content', '')
                extracted_links = []
                
                if content:
                    try:
                        links = self.content_extractor.extract_links(content, url)
                        
                        # Ensure links is not None and filter out None values
                        if links is not None:
                            extracted_links = [link for link in links if link is not None and link]
                        else:
                            extracted_links = []
                    except Exception as e:
                        self.logger.error(f"Error extracting links: {str(e)}")
                        extracted_links = []
                    
                # Set links_extracted count and store links in result
                result['links_extracted'] = len(extracted_links)
                result['links'] = extracted_links
                
                # Add links to queue if not empty
                if extracted_links:
                    links_added = 0
                    
                    for link_url in extracted_links:
                        # Safety check - skip empty URLs
                        if not link_url:
                            continue
                            
                        # Check if we've reached the max URLs limit before adding more links
                        try:
                            if self.has_reached_limit():
                                self.logger.info(f"Reached URL limit, stopping link extraction for {url}")
                                break
                        except Exception as e:
                            self.logger.error(f"Error checking URL limit: {str(e)}")
                            break
                            
                        # Add link to queue with try/except
                        try:
                            link_metadata = {
                                'source_url': url,
                                'anchor_text': ''  # We could extract this from the HTML if needed
                            }
                            if self.add_url(link_url, depth + 1, metadata=link_metadata):
                                links_added += 1
                        except Exception as e:
                            self.logger.error(f"Error adding URL {link_url} to queue: {str(e)}")
                            
                    result['links_added'] = links_added
                    self.logger.info(f"Added {links_added} links from URL: {url}")
                else:
                    self.logger.info(f"No links extracted from URL: {url}")
            
            # Add URL to visited URLs
            try:
                if isinstance(self.visited_urls, VisitedUrlManager):
                    try:
                        self.visited_urls.add(
                            url,
                            domain,
                            depth,
                            int(response.get('status_code', 0)),
                            response.get('content_type', ''),
                            int(response.get('content_length', 0)),
                            result.get('title', ''),
                            response.get('content', '')
                        )
                    except Exception as e:
                        self.logger.error(f"Error adding URL to visited URLs: {str(e)}")
                else:
                    self.visited_urls.add(url)
            except Exception as e:
                self.logger.error(f"Error processing URL domain: {str(e)}")
            
            # Call callbacks with try/except blocks
            try:
                for callback in self.callbacks.get('on_url_processed', []):
                    callback(url, result)
            except Exception as e:
                self.logger.error(f"Error in on_url_processed callback: {str(e)}")
                
            # Call content extracted callbacks
            try:
                for callback in self.callbacks.get('on_content_extracted', []):
                    callback(url, depth, result.get('title', ''), result.get('content', ''), {})
            except Exception as e:
                self.logger.error(f"Error in on_content_extracted callback: {str(e)}")
            
            # Update stats
            self.stats['urls_processed'] = int(self.stats.get('urls_processed', 0)) + 1
            
            # Set status to success
            result['status'] = 'success'
            
        except Exception as e:
            self.logger.error(f"Error processing URL {url}: {str(e)}")
            result['status'] = 'error'
            result['error'] = str(e)
            self.stats['urls_failed'] = int(self.stats.get('urls_failed', 0)) + 1
            
        finally:
            # Calculate processing time
            end_time = time.time()
            processing_time = end_time - start_time
            result['processing_time'] = processing_time
            
            # Add to processing times list (up to 100 entries)
            self.stats['processing_times'].append(processing_time)
            if len(self.stats['processing_times']) > 100:
                self.stats['processing_times'] = self.stats['processing_times'][-100:]
            
            # Update stats
            self._update_stats()
            
            # Return result
            return result
    
    def process_next_url(self) -> Dict:
        """
        Process the next URL in the queue.
        
        Returns:
            Dict: Results of processing the URL, or None if queue is empty
        """
        # First check in-memory queue for faster processing
        url_data = None
        url_id = None
        
        # Try to get a URL that hasn't been visited yet
        max_attempts = 5  # Limit the number of attempts to avoid infinite loops
        attempts = 0
        
        while attempts < max_attempts:
            attempts += 1
            
            # Get next URL from queue
            url_data = self.url_queue_manager.get_next() if self.url_queue_manager else None
            if not url_data:
                self.logger.debug("URL queue is empty, no more URLs to process")
                break
            
            # Skip if already visited
            if isinstance(self.visited_urls, VisitedUrlManager) and self.visited_urls.exists(url_data['url']):
                self.logger.debug(f"Skipping already visited URL: {url_data['url']}")
                url_data = None
                continue
            
            # Store URL ID for marking complete later
            url_id = url_data.get('id')
            self.logger.debug(f"Got URL from queue: {url_data['url']}")
            break
        
        # If we couldn't find a non-visited URL after max attempts
        if not url_data:
            self.logger.debug("No unvisited URLs found in queue")
            return None
        
        # Process the URL
        try:
            result = self.process_url(url_data['url'], url_data.get('depth', 0))
            result['url_id'] = url_id
            
            # Mark URL as complete in the queue
            if url_id and self.url_queue_manager:
                try:
                    if result['status'] == 'success':
                        self.url_queue_manager.mark_complete(url_id)
                    else:
                        self.url_queue_manager.mark_failed(url_id)
                except Exception as e:
                    self.logger.error(f"Error marking URL status: {str(e)}")
            
            return result
        except Exception as e:
            self.logger.error(f"Error processing URL {url_data['url']}: {str(e)}")
            # Mark as failed in queue
            if url_id and self.url_queue_manager:
                try:
                    self.url_queue_manager.mark_failed(url_id)
                except Exception as mark_e:
                    self.logger.error(f"Error marking URL as failed: {str(mark_e)}")
                    
            return {'status': 'error', 'url_id': url_id, 'error': str(e)}
            
    def handle_command(self, command: str) -> None:
        """
        Handle commands received from the coordinator.
        
        Args:
            command: Command string to process
        """
        try:
            if command.startswith('SPEED:'):
                # Extract delay value from command
                delay_str = command.split(':')[1].strip()
                delay = float(delay_str)
                
                # Update request delay override
                self.request_delay_override = delay
                self.logger.info(f"Set request delay override to {delay} seconds")
        except Exception as e:
            self.logger.error(f"Error handling command '{command}': {str(e)}")
            
    def check_stdin_commands(self) -> None:
        """
        Check stdin for any pending commands from the coordinator.
        """
        # Check if there's data available on stdin
        try:
            if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
                command = sys.stdin.readline().strip()
                if command:
                    self.handle_command(command)
        except Exception as e:
            self.logger.error(f"Error handling command: {str(e)}")
    
    def crawl(self, start_url=None, max_urls=None, max_depth=None):
        """
        Start crawling from a given URL.
        
        Args:
            start_url: URL to start crawling from
            max_urls: Maximum number of URLs to crawl
            max_depth: Maximum depth to crawl
            
        Returns:
            dict: Crawl statistics
        """
        # Set start time
        self.stats['start_time'] = time.time()
        
        # Override config values if provided
        if max_urls is not None:
            self.config['max_urls'] = max_urls
            
        if max_depth is not None:
            self.config['max_depth'] = max_depth
            
        # Add start URL to queue if provided
        if start_url:
            self.add_url(start_url, 0, 1.0)
            
        # Start crawling
        self.logger.info("Starting crawl")
        
        # Process URLs until queue is empty or max URLs reached
        while True:
            # Check for commands from coordinator
            self.check_stdin_commands()
            
            # Check if we've reached the max URLs limit (0 means unlimited)
            max_urls = self.config.get('max_urls')
            if max_urls is not None and max_urls > 0 and self.stats['urls_processed'] >= max_urls:
                self.logger.info(f"Reached max URLs limit of {max_urls}")
                break
                
            # Process next URL
            result = self.process_next_url()
            
            # If no more URLs, break
            if not result:
                self.logger.info("No more URLs to process")
                break
                
            # Update stats
            self.stats['urls_processed'] += 1
            
            # Call stats callback if provided
            if self.stats_callback:
                self.stats_callback(self.stats)
                
            # Log progress
            if self.stats['urls_processed'] % 10 == 0:
                self.logger.info(f"Processed {self.stats['urls_processed']} URLs")
                
        # Set end time
        self.stats['end_time'] = time.time()
        self.stats['elapsed_time'] = self.stats['end_time'] - self.stats['start_time']
        
        # Log final stats
        self.logger.info(f"Crawl complete. Processed {self.stats['urls_processed']} URLs in {self.stats['elapsed_time']:.2f} seconds")
        
        return self.stats
    
    def get_unanalyzed_urls(self, limit=10) -> List[Dict]:
        """
        Get a list of URLs that have been visited but not analyzed.
        
        Args:
            limit: Maximum number of URLs to return
            
        Returns:
            List of dictionaries with URL information
        """
        if isinstance(self.visited_urls, VisitedUrlManager):
            return self.visited_urls.get_unanalyzed_urls(limit)
        else:
            return []
    
    def mark_url_as_analyzed(self, url: str) -> bool:
        """
        Mark a URL as analyzed.
        
        Args:
            url: URL to mark as analyzed
            
        Returns:
            bool: True if URL was marked as analyzed, False otherwise
        """
        if isinstance(self.visited_urls, VisitedUrlManager):
            return self.visited_urls.mark_as_analyzed(url)
        else:
            return False
    
    def get_stats(self) -> Dict:
        """
        Get current crawler statistics.
        
        Args:
            None
            
        Returns:
            Dict: Current crawler statistics
        """
        # Calculate derived stats
        if self.stats['processing_times'] and len(self.stats['processing_times']) > 0:
            # Make sure we don't have any None values in processing_times
            valid_times = [t for t in self.stats['processing_times'] if t is not None]
            avg_time = sum(valid_times) / len(valid_times) if valid_times else 0
        else:
            avg_time = 0
        
        # Update elapsed time
        if self.stats['start_time']:
            if self.stats['end_time']:
                self.stats['elapsed_time'] = self.stats['end_time'] - self.stats['start_time']
            else:
                self.stats['elapsed_time'] = time.time() - self.stats['start_time']
        
        # Get database stats with safe default values
        if isinstance(self.visited_urls, VisitedUrlManager):
            visited_stats = self.visited_urls.get_stats() or {'total': 0, 'success': 0, 'error': 0, 'analyzed': 0, 'unanalyzed': 0}
        else:
            visited_stats = {
                'total': 0,
                'success': 0,
                'error': 0,
                'analyzed': 0,
                'unanalyzed': 0
            }
        
        if self.url_queue_manager:
            queue_stats = self.url_queue_manager.get_stats() or {'total': 0, 'pending': 0, 'complete': 0, 'failed': 0, 'domains': 0}
        else:
            queue_stats = {
                'total': 0,
                'pending': 0,
                'complete': 0,
                'failed': 0,
                'domains': 0
            }
        
        # Combine stats with safe handling for all values
        combined_stats = {
            'urls_processed': self.stats.get('urls_processed', 0),
            'urls_queued': queue_stats.get('total', 0),
            'urls_failed': self.stats.get('urls_failed', 0),
            'domains_discovered': len(self.stats.get('domains_discovered', set())),
            'avg_processing_time': avg_time,
            'elapsed_time': self.stats.get('elapsed_time', 0),
            'analyzed_urls': visited_stats.get('analyzed', 0),
            'unanalyzed_urls': visited_stats.get('unanalyzed', 0)
        }
        
        return combined_stats
    
    def close(self):
        """Close the crawler and release resources."""
        if self.session:
            self.session.close()
        
        self.logger.info("Crawler closed")
    
    def reset(self):
        """
        Reset the crawler state, clearing the URL queue and statistics.
        """
        self.logger.info("Resetting crawler state")
        
        # Clear the URL queue
        self.url_queue.clear()
        
        # Reset statistics
        self.stats = {
            'start_time': None,
            'end_time': None,
            'elapsed_time': 0,
            'urls_processed': 0,
            'urls_queued': 0,
            'urls_failed': 0,
            'domains_discovered': set(),
            'processing_times': []
        }
        
        # Reset the start domain
        self.start_domain = None
        
        # If using database, clear the URL queue in the database
        if self.use_database and self.url_queue_manager:
            try:
                # This would require adding a method to clear the queue in the database
                # For now, we'll just log that this should be implemented
                self.logger.warning("Database queue reset not implemented")
            except Exception as e:
                self.logger.error(f"Error resetting database queue: {e}")
        
        self.logger.info("Crawler state reset complete")
    
    def _update_stats(self):
        """Update and store current stats."""
        stats = self.get_stats()
        if self.stats_callback:
            self.stats_callback(stats)
    
    def _should_stop(self):
        """Check if the crawler should stop."""
        # Check if we've reached the max URLs limit
        # A max_urls of 0 means unlimited (never stop based on URL count)
        max_urls = self.config.get('max_urls')
        if max_urls is not None and max_urls > 0:
            if self.stats['urls_processed'] >= max_urls:
                self.logger.info(f"Reached max URLs limit of {max_urls}, stopping crawler")
                return True
                
        # This can be extended with more conditions
        return False
    
    def _is_domain_allowed(self, domain: str) -> bool:
        """
        Check if a domain is allowed.
        
        Args:
            domain: Domain to check
            
        Returns:
            bool: True if domain is allowed, False otherwise
        """
        # Debug the domain checking
        self.logger.info(f"Checking if domain is allowed: {domain}")
        self.logger.info(f"Allowed domains: {self.config.get('allowed_domains', [])}")
        self.logger.info(f"Disallowed domains: {self.config.get('disallowed_domains', [])}")
        self.logger.info(f"Start domain: {getattr(self, 'start_domain', 'Not set')}")
        self.logger.info(f"Restrict to start domain: {self.config.get('restrict_to_start_domain', False)}")
        
        # If there's a start domain and we're restricting to it, check against it
        if self.config.get('restrict_to_start_domain', False) and hasattr(self, 'start_domain') and self.start_domain:
            # Check for exact domain match
            exact_match = domain == self.start_domain
            
            # Check if subdomains are allowed and if this is a subdomain
            allow_subdomains = self.config.get('allow_subdomains', False)
            is_subdomain = domain.endswith(f".{self.start_domain}")
            
            if exact_match or (allow_subdomains and is_subdomain):
                self.logger.info(f"Domain {domain} matches start domain restrictions")
                return True
            else:
                self.logger.info(f"Domain {domain} doesn't match start domain restrictions")
                return False
        
        # Check against allowed domains list
        allowed_domains = self.config.get('allowed_domains', [])
        if allowed_domains:
            for allowed_domain in allowed_domains:
                if domain == allowed_domain or (self.config.get('allow_subdomains', False) and domain.endswith(f".{allowed_domain}")):
                    self.logger.info(f"Domain {domain} is in allowed domains list")
                    return True
            
            # If we have an allowed domains list and the domain isn't in it, disallow
            self.logger.info(f"Domain {domain} is not in allowed domains list")
            return False
            
        # Check against disallowed domains list
        disallowed_domains = self.config.get('disallowed_domains', [])
        for disallowed_domain in disallowed_domains:
            if domain == disallowed_domain or domain.endswith(f".{disallowed_domain}"):
                self.logger.info(f"Domain {domain} is in disallowed domains list")
                return False
                
        # Default to allowed
        self.logger.info(f"Domain {domain} is allowed by default")
        return True
    
    def _is_url_allowed(self, url: str) -> bool:
        """
        Check if a URL is allowed.
        
        Args:
            url: URL to check
            
        Returns:
            bool: True if URL is allowed, False otherwise
        """
        # Debug URL checking
        self.logger.info(f"Checking if URL is allowed: {url}")
        self.logger.info(f"Allowed patterns: {self.config.get('allowed_url_patterns', [])}")
        self.logger.info(f"Disallowed patterns: {self.config.get('disallowed_url_patterns', [])}")
        
        # URL must be valid
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                self.logger.info(f"URL {url} lacks scheme or netloc")
                return False
        except Exception as e:
            self.logger.info(f"Error parsing URL {url}: {str(e)}")
            return False
        
        # Check against allowed patterns
        allowed_patterns = self.config.get('allowed_url_patterns', [])
        if allowed_patterns:
            for pattern in allowed_patterns:
                if re.search(pattern, url):
                    self.logger.info(f"URL {url} matches allowed pattern {pattern}")
                    return True
            
            self.logger.info(f"URL {url} doesn't match any allowed patterns")
            return False
        
        # Check against disallowed patterns
        disallowed_patterns = self.config.get('disallowed_url_patterns', [])
        for pattern in disallowed_patterns:
            if re.search(pattern, url):
                self.logger.info(f"URL {url} matches disallowed pattern {pattern}")
                return False
        
        # Default to allowed
        self.logger.info(f"URL {url} is allowed by default")
        return True
    
    def is_allowed_by_robots(self, url: str) -> bool:
        """
        Check if a URL is allowed by the robots.txt file.
        
        Args:
            url: URL to check
            
        Returns:
            bool: True if URL is allowed, False otherwise
        """
        try:
            # If robots.txt checking is disabled, always allow
            if not self.config.get('respect_robots_txt', True):
                return True
                
            # Parse URL to get domain
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            
            if not domain:
                self.logger.warning(f"Could not parse domain from URL: {url}")
                return True
                
            # Get robots parser for this domain
            robots_parser = self._get_robots_parser(domain)
            
            if robots_parser:
                user_agent = self.config.get('user_agent', 'NDaiviBot')
                allowed = robots_parser.can_fetch(user_agent, url)
                
                if not allowed:
                    self.logger.info(f"URL {url} disallowed by robots.txt for {domain}")
                
                return allowed
            else:
                self.logger.debug(f"No robots parser available for {domain}, allowing URL: {url}")
                # If no parser, allow all
                return True
        except Exception as e:
            self.logger.error(f"Error checking robots.txt for URL {url}: {str(e)}")
            # If there's an error, allow the URL
            return True
    
    def _get_robots_parser(self, domain: str):
        """
        Get or create a robots.txt parser for a domain.
        
        Args:
            domain: Domain to get robots parser for
            
        Returns:
            RobotFileParser: Robots parser for the domain
        """
        # Initialize robots parsers dict if it doesn't exist
        if not hasattr(self, 'robots_parsers'):
            self.robots_parsers = {}
            
        # Return existing parser if available
        if domain in self.robots_parsers:
            return self.robots_parsers[domain]
            
        # If robots.txt checking is disabled, always allow
        if not self.config.get('respect_robots_txt'):
            parser = urllib.robotparser.RobotFileParser()
            parser.allow_all = True
            self.robots_parsers[domain] = parser
            return parser
            
        # Create a new parser
        try:
            parser = urllib.robotparser.RobotFileParser()
            # Try HTTPS first, then fallback to HTTP if needed
            robots_url = f"https://{domain}/robots.txt"
            self.logger.info(f"Attempting to fetch robots.txt from: {robots_url}")
            parser.set_url(robots_url)
            parser.read()
            
            # Check if the parser has rules by testing a URL
            test_url = f"https://{domain}/"
            can_fetch = parser.can_fetch(self.config.get('user_agent', 'NDaiviBot'), test_url)
            
            # If can_fetch returns True but there might not be any rules (default allow)
            # Try HTTP as fallback
            if can_fetch:
                self.logger.info(f"Successfully fetched robots.txt for domain {domain}")
            else:
                self.logger.info(f"No rules or disallowed in HTTPS robots.txt, trying HTTP for domain {domain}")
                robots_url = f"http://{domain}/robots.txt"
                parser.set_url(robots_url)
                parser.read()
            
            self.robots_parsers[domain] = parser
        except Exception as e:
            self.logger.error(f"Error fetching robots.txt for domain {domain}: {str(e)}")
            parser = urllib.robotparser.RobotFileParser()
            parser.allow_all = True
            self.robots_parsers[domain] = parser
            
        return self.robots_parsers[domain]
    
    def get_backlog_batch(self, batch_size=10) -> List[Dict]:
        """
        Get a batch of URLs from the backlog for analysis.
        
        Args:
            batch_size: Maximum number of URLs to return
            
        Returns:
            List of dictionaries with URL information
        """
        with self.backlog_lock:
            # Get unanalyzed URLs to fill the backlog if needed
            if len(self.backlog) < self.config.get('backlog_size', 100):
                unanalyzed = self.get_unanalyzed_urls(
                    limit=self.config.get('backlog_size', 100) - len(self.backlog)
                )
                
                # Add to backlog
                for url_data in unanalyzed:
                    if url_data not in self.backlog:
                        self.backlog.append(url_data)
            
            # Return a batch from the backlog
            if not self.backlog:
                return []
                
            # Sort backlog by priority if available
            self.backlog.sort(key=lambda x: x.get('priority', 0), reverse=True)
            
            # Get batch
            batch = self.backlog[:batch_size]
            
            return batch
    
    def mark_as_processed(self, url: str) -> bool:
        """
        Mark a URL as processed and remove it from the backlog.
        
        Args:
            url: URL to mark as processed
            
        Returns:
            bool: True if URL was marked as processed, False otherwise
        """
        with self.backlog_lock:
            # Mark as analyzed in database
            success = self.mark_url_as_analyzed(url)
            
            # Remove from backlog
            self.backlog = [item for item in self.backlog if item['url'] != url]
            
            # Check backlog threshold
            backlog_size = len(self.backlog)
            threshold = self.config.get('backlog_size', 100) * self.config.get('backlog_threshold', 0.5)
            
            if backlog_size <= threshold:
                self.logger.info(f"Backlog below threshold ({backlog_size}/{self.config.get('backlog_size', 100)}), triggering crawl")
                # Trigger crawling more URLs in a separate thread
                threading.Thread(target=self._fill_backlog).start()
            
            return success
    
    def _fill_backlog(self, count=10):
        """
        Fill the backlog with more URLs by crawling.
        
        Args:
            count: Number of URLs to crawl
        """
        try:
            for _ in range(count):
                result = self.process_next_url()
                if not result or result.get('status') != 'success':
                    break
        except Exception as e:
            self.logger.error(f"Error filling backlog: {str(e)}")
    
    def get_backlog_stats(self) -> Dict:
        """
        Get statistics about the backlog.
        
        Args:
            None
            
        Returns:
            Dict: Backlog statistics
        """
        with self.backlog_lock:
            backlog_size = self.config.get('backlog_size', 100)
            backlog_threshold = self.config.get('backlog_threshold', 0.5)
            return {
                'backlog_size': len(self.backlog),
                'backlog_capacity': backlog_size,
                'backlog_threshold': backlog_threshold,
                'backlog_threshold_count': int(backlog_size * backlog_threshold),
                'backlog_usage': len(self.backlog) / backlog_size if backlog_size > 0 else 0
            }
    
    def update_config(self, new_config: Dict) -> None:
        """
        Update the crawler configuration.
        
        Args:
            new_config: New configuration dictionary
        """
        self.logger.info("Updating crawler configuration")
        
        # Update config
        self.config.update(new_config)
        
        # Update content extractor
        self.content_extractor.user_agent = self.config.get('user_agent')
        self.content_extractor.timeout = self.config.get('timeout')
        
        self.logger.info("Crawler configuration updated")

    def has_reached_limit(self) -> bool:
        """
        Check if the crawler has reached its URL limit.
        
        Returns:
            bool: True if the limit has been reached, False otherwise
        """
        # Check if we've reached the max URLs limit
        max_urls = self.config.get('max_urls')
        if max_urls is not None and max_urls > 0:
            total_urls = self.stats['urls_processed']
            if total_urls >= max_urls:
                self.logger.info(f"Reached max URLs limit of {max_urls}")
                return True
                
        # Check if we've reached the max queued URLs limit
        max_queued_urls = self.config.get('max_queued_urls')
        if max_queued_urls is not None and max_queued_urls > 0:
            if self.stats['urls_queued'] >= max_queued_urls:
                self.logger.info(f"Reached max queued URLs limit of {max_queued_urls}")
                return True
                
        return False

    def calculate_priority(self, url, depth=0, anchor_text=None, source_url=None):
        """
        Calculate the priority of a URL.
        
        Args:
            url: URL to calculate priority for
            depth: Depth of the URL in the crawl tree
            anchor_text: Text of the anchor linking to this URL
            source_url: URL of the page containing the link
            
        Returns:
            float: Priority value (higher = more important)
        """
        return self.url_priority.calculate_priority(url, depth, anchor_text, source_url)

    def _normalize_url(self, url: str) -> str:
        """
        Normalize a URL by removing fragments, default ports, etc.
        
        Args:
            url: URL to normalize
            
        Returns:
            str: Normalized URL or None if invalid
        """
        try:
            # Parse URL
            parsed = urlparse(url)
            
            # Skip non-HTTP(S) URLs
            if parsed.scheme not in ('http', 'https'):
                self.logger.debug(f"Skipping non-HTTP URL: {url}")
                return None
                
            # Rebuild URL without fragments and with normalized path
            normalized = ParseResult(
                scheme=parsed.scheme.lower(),
                netloc=parsed.netloc.lower(),
                path=parsed.path if parsed.path else '/',
                params=parsed.params,
                query=parsed.query,
                fragment=''  # Remove fragments
            ).geturl()
            
            return normalized
        except Exception as e:
            self.logger.error(f"Error normalizing URL {url}: {str(e)}")
            return None
        self.db = db
        self.logger = logger or logging.getLogger(__name__)
        
    def add(self, url: str, domain: str, depth: int = 0, priority: float = 0.0, 
            source_url: str = None, anchor_text: str = None, metadata: str = None) -> bool:
        """
        Add a URL to the queue.
        
        Args:
            url: URL to add
            domain: Domain of the URL
            depth: Depth of the URL in the crawl tree
            priority: Priority of the URL (higher = more important)
            source_url: URL of the page containing the link
            anchor_text: Text of the anchor linking to this URL
            metadata: Additional metadata about the URL
            
        Returns:
            bool: True if URL was added, False otherwise
        """
        try:
            # Check if URL is already in queue
            if self.exists(url):
                return False
                
            # Add URL to queue
            self.db.execute('''
                INSERT INTO url_queue 
                (url, domain, depth, priority, source_url, anchor_text, metadata, status) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (url, domain, depth, priority, source_url, anchor_text, metadata, 'pending'))
            
            return True
        except Exception as e:
            self.logger.error(f"Error adding URL to queue: {str(e)}")
            return False
            
    def exists(self, url: str) -> bool:
        """
        Check if a URL exists in the queue.
        
        Args:
            url: URL to check
            
        Returns:
            bool: True if URL exists, False otherwise
        """
        try:
            self.db.execute("SELECT COUNT(*) FROM url_queue WHERE url = ?", (url,))
            count = self.db.fetchone()[0]
            return count > 0
        except Exception as e:
            self.logger.error(f"Error checking if URL exists in queue: {str(e)}")
            return False
            
    def get_all_queued(self, status: str = 'pending', limit: int = None) -> List[Dict]:
        """
        Get all queued URLs with the given status.
        
        Args:
            status: Status of URLs to retrieve (default: 'pending')
            limit: Maximum number of URLs to retrieve (default: None)
            
        Returns:
            List[Dict]: List of URL dictionaries
        """
        try:
            query = "SELECT id, url, domain, depth, priority, metadata FROM url_queue WHERE status = ?"
            params = [status]
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
                
            self.db.execute(query, tuple(params))
            rows = self.db.fetchall()
            
            result = []
            for row in rows:
                url_data = {
                    'id': row[0],
                    'url': row[1],
                    'domain': row[2],
                    'depth': row[3],
                    'priority': row[4],
                    'metadata': json.loads(row[5]) if row[5] else None
                }
                result.append(url_data)
                
            return result
        except Exception as e:
            self.logger.error(f"Error getting queued URLs: {str(e)}")
            return []

    def get_next(self) -> Dict:
        """
        Get the next URL from the queue.
        
        Returns:
            Dict: URL data or None if queue is empty
        """
        try:
            # Get highest priority URL
            self.db.execute("""
                SELECT id, url, domain, depth, priority, metadata 
                FROM url_queue 
                WHERE status = 'pending' 
                ORDER BY priority DESC, id ASC 
                LIMIT 1
            """)
            
            row = self.db.fetchone()
            if not row:
                return None
                
            url_data = {
                'id': row[0],
                'url': row[1],
                'domain': row[2],
                'depth': row[3],
                'priority': row[4],
                'metadata': json.loads(row[5]) if row[5] else None
            }
            
            return url_data
        except Exception as e:
            self.logger.error(f"Error getting next URL from queue: {str(e)}")
            return None
            
    def mark_complete(self, url_id: int) -> bool:
        """
        Mark a URL as complete.
        
        Args:
            url_id: ID of the URL to mark as complete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.db.execute(
                "UPDATE url_queue SET status = 'complete' WHERE id = ?",
                (url_id,)
            )
            return True
        except Exception as e:
            self.logger.error(f"Error marking URL as complete: {str(e)}")
            return False
            
    def mark_failed(self, url_id: int) -> bool:
        """
        Mark a URL as failed.
        
        Args:
            url_id: ID of the URL to mark as failed
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.db.execute(
                "UPDATE url_queue SET status = 'failed' WHERE id = ?",
                (url_id,)
            )
            return True
        except Exception as e:
            self.logger.error(f"Error marking URL as failed: {str(e)}")
            return False

    def get_stats(self) -> Dict:
        """
        Get statistics about the URL queue.
        
        Args:
            None
            
        Returns:
            Dict: Statistics about the URL queue
        """
        stats = {
            'total': 0,
            'pending': 0,
            'complete': 0,
            'failed': 0,
            'domains': 0
        }
        
        try:
            # Get total count
            self.db.execute("SELECT COUNT(*) FROM url_queue")
            stats['total'] = self.db.fetchone()[0]
            
            # Get pending count
            self.db.execute("SELECT COUNT(*) FROM url_queue WHERE status = 'pending'")
            stats['pending'] = self.db.fetchone()[0]
            
            # Get complete count
            self.db.execute("SELECT COUNT(*) FROM url_queue WHERE status = 'complete'")
            stats['complete'] = self.db.fetchone()[0]
            
            # Get failed count
            self.db.execute("SELECT COUNT(*) FROM url_queue WHERE status = 'failed'")
            stats['failed'] = self.db.fetchone()[0]
            
            # Get domain count
            self.db.execute("SELECT COUNT(DISTINCT domain) FROM url_queue")
            stats['domains'] = self.db.fetchone()[0]
            
            return stats
        except Exception as e:
            self.logger.error(f"Error getting URL queue stats: {str(e)}")
            return stats