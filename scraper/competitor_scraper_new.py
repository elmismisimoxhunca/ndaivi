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
from urllib.parse import urlparse, urljoin, ParseResult
import datetime
import signal
import threading
from typing import Optional, List, Dict, Any, Tuple, Set
from collections import defaultdict
from bs4 import BeautifulSoup
from sqlalchemy import func, desc, create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import re
import json
import argparse
from requests.exceptions import RequestException

# Add the project root to the Python path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_manager import get_db_manager
from database.schema import Base, Manufacturer, Category, CrawlStatus, ScraperSession, ScraperLog, CrawlQueue, ErrorLog
from database.statistics_manager import StatisticsManager

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
    print(f"\nScraper {status}.")

# Register global signal handlers
signal.signal(signal.SIGINT, handle_interrupt)  # Ctrl+C
signal.signal(signal.SIGTERM, handle_interrupt)  # kill command
signal.signal(signal.SIGUSR1, handle_suspend)  # SIGUSR1 for suspension

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
    
    @staticmethod
    def calculate_seed_priority(url: str) -> float:
        """
        Calculate a priority score specifically for seed URLs.
        
        Seed URLs are given higher priority than regular URLs.
        
        Args:
            url: The seed URL to calculate priority for
            
        Returns:
            A priority score (lower is higher priority)
        """
        # Start with a base priority of 0 for seed URLs
        priority = 0.0
        
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
        
        # Special adjustments for seed URLs
        if 'brand' in url_lower or 'manufacturer' in url_lower:
            priority -= 15  # Extra boost for brand/manufacturer pages
            
        # Ensure seed URLs have higher priority than regular URLs
        return max(-50.0, priority)  # Cap at -50 to ensure high priority


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
    
    def is_empty(self) -> bool:
        """Check if the queue is empty.
        
        Returns:
            True if the queue is empty, False otherwise
        """
        return len(self._queue) == 0


class ContentExtractor:
    """
    Extract and process HTML content from web pages.
    
    This class handles all content extraction operations, including text extraction,
    link extraction, and HTML structure analysis.
    """
    
    def __init__(self, user_agent: str, timeout: int, logger):
        """
        Initialize the ContentExtractor.
        
        Args:
            user_agent: User agent string for HTTP requests
            timeout: Timeout for HTTP requests in seconds
            logger: Logger instance for logging extraction activities
        """
        self.user_agent = user_agent
        self.timeout = timeout
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})
    
    def fetch_url(self, url: str) -> Tuple[str, str, Dict]:
        """
        Fetch a URL and return its content.
        
        Args:
            url: URL to fetch
            
        Returns:
            Tuple of (title, content, metadata)
        """
        try:
            self.logger.debug(f"Fetching URL: {url}")
            
            # Make request
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract title
            title = soup.find('title')
            title_text = title.text.strip() if title else ""
            
            # Extract content
            content = self.extract_text_content(soup)
            
            # Extract metadata
            metadata = {}
            for meta in soup.find_all('meta'):
                name = meta.get('name', meta.get('property', ''))
                content = meta.get('content', '')
                if name and content:
                    metadata[name] = content
            
            return title_text, content, metadata
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching URL {url}: {str(e)}")
            raise
    
    def extract_text_content(self, soup: BeautifulSoup) -> str:
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
    
    def extract_links(self, soup: BeautifulSoup, base_url: str) -> Dict[str, str]:
        """
        Extract links from a BeautifulSoup object.
        
        Args:
            soup: BeautifulSoup object of the page
            base_url: Base URL for resolving relative links
            
        Returns:
            Dictionary mapping URLs to their anchor text
        """
        links = {}
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            # Skip empty or javascript links
            if not href or href.startswith('javascript:') or href == '#':
                continue
                
            # Resolve relative URLs
            full_url = urljoin(base_url, href)
            
            # Get the anchor text
            text = a_tag.get_text().strip()
            if not text and a_tag.find('img'):
                # Use alt text for image links
                img = a_tag.find('img')
                text = img.get('alt', '') if img else ''
            
            links[full_url] = text
        
        return links
    
    def extract_html_structure(self, soup: BeautifulSoup) -> Dict[str, int]:
        """
        Extract information about HTML structure for priority calculation.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Dictionary with counts of relevant HTML elements
        """
        structure = {}
        
        # Count navigation elements
        structure['nav'] = len(soup.find_all('nav'))
        
        # Count menu elements
        structure['menu'] = len(soup.find_all(['menu', 'ul', 'ol'], class_=lambda c: c and 
                                             any(menu_class in str(c).lower() 
                                                 for menu_class in ['menu', 'nav', 'navigation'])))
        
        # Count sidebar elements
        structure['sidebar'] = len(soup.find_all(class_=lambda c: c and any(sidebar_class in str(c).lower() 
                                                                          for sidebar_class in ['sidebar', 'side'])))
        
        # Count breadcrumb elements
        structure['breadcrumb'] = len(soup.find_all(class_=lambda c: c and 'breadcrumb' in str(c).lower()))
        
        # Count list elements
        structure['list'] = len(soup.find_all(['ul', 'ol']))
        
        # Count table elements
        structure['table'] = len(soup.find_all('table'))
        
        return structure
    
    def extract_navigation_elements(self, soup: BeautifulSoup) -> str:
        """
        Extract navigation elements, breadcrumbs, and link patterns from a page.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            String containing navigation data formatted for analysis
        """
        nav_data = []
        
        # Extract breadcrumbs
        breadcrumbs = soup.find_all(class_=lambda c: c and 'breadcrumb' in str(c).lower())
        if breadcrumbs:
            nav_data.append("Breadcrumbs:")
            for breadcrumb in breadcrumbs:
                links = breadcrumb.find_all('a')
                crumb_text = ' > '.join(link.get_text().strip() for link in links if link.get_text().strip())
                if crumb_text:
                    nav_data.append(f"  {crumb_text}")
        
        # Extract navigation menus
        nav_elements = soup.find_all(['nav', 'div'], class_=lambda c: c and 
                                    any(nav_class in str(c).lower() 
                                        for nav_class in ['nav', 'menu', 'navigation']))
        if nav_elements:
            nav_data.append("\nNavigation Menus:")
            for nav in nav_elements[:3]:  # Limit to first 3 navigation elements
                links = nav.find_all('a')
                menu_items = [link.get_text().strip() for link in links if link.get_text().strip()]
                if menu_items:
                    nav_data.append(f"  Menu Items: {', '.join(menu_items[:10])}")  # Limit items
        
        # Extract category lists
        category_lists = soup.find_all(['ul', 'ol'], class_=lambda c: c and 
                                      any(cat_class in str(c).lower() 
                                          for cat_class in ['category', 'categories', 'product']))
        if category_lists:
            nav_data.append("\nCategory Lists:")
            for cat_list in category_lists[:2]:  # Limit to first 2 category lists
                items = cat_list.find_all('li')
                cat_items = [item.get_text().strip() for item in items if item.get_text().strip()]
                if cat_items:
                    nav_data.append(f"  Categories: {', '.join(cat_items[:10])}")  # Limit items
        
        return '\n'.join(nav_data)
    
    def extract_main_content(self, soup: BeautifulSoup) -> str:
        """
        Extract the main content from a page.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Extracted main content as a string
        """
        # Try to find main content containers
        main_content = None
        
        # Look for main content by common IDs and classes
        for container_id in ['main-content', 'content', 'main', 'article']:
            content = soup.find(id=container_id)
            if content:
                main_content = content
                break
        
        # If not found by ID, try common classes
        if not main_content:
            for container_class in ['main-content', 'content', 'main', 'article']:
                content = soup.find(class_=container_class)
                if content:
                    main_content = content
                    break
        
        # If still not found, try HTML5 semantic elements
        if not main_content:
            main_content = soup.find('main') or soup.find('article')
        
        # If we found main content, extract text from it
        if main_content:
            # Remove navigation, footer, and sidebar elements from the main content
            for elem in main_content.find_all(['nav', 'footer', 'aside']):
                elem.extract()
            
            # Extract text from the main content
            return self.extract_text_content(main_content)
        
        # If no main content container found, extract from body with some cleaning
        body = soup.find('body')
        if body:
            # Remove common non-content elements
            for elem in body.find_all(['nav', 'header', 'footer', 'aside']):
                elem.extract()
            
            return self.extract_text_content(body)
        
        # Fallback to extracting from the entire document
        return self.extract_text_content(soup)
    
    def extract_manufacturer_name(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract manufacturer name from page content.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Manufacturer name if found, None otherwise
        """
        # Try to find manufacturer name in meta tags
        meta_tags = soup.find_all('meta', property=['og:site_name', 'og:title'])
        for tag in meta_tags:
            content = tag.get('content', '')
            if content:
                return content.strip()
        
        # Try to find in title
        title_tag = soup.find('title')
        if title_tag and title_tag.string:
            return title_tag.string.strip()
        
        # Try to find in header or logo
        logo = soup.find('a', class_=lambda c: c and 'logo' in str(c).lower())
        if logo:
            return logo.get_text().strip()
        
        # Try common header patterns
        header = soup.find('header')
        if header:
            h1 = header.find('h1')
            if h1:
                return h1.get_text().strip()
        
        return None
    
    def extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract manufacturer description from page content.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Description if found, None otherwise
        """
        # Try meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return meta_desc['content'].strip()
        
        # Try Open Graph description
        og_desc = soup.find('meta', property='og:description')
        if og_desc and og_desc.get('content'):
            return og_desc['content'].strip()
        
        # Try to find an about section
        about_section = soup.find(id=lambda i: i and 'about' in str(i).lower())
        if about_section:
            return self.extract_text_content(about_section)
        
        # Try to find a description section
        desc_section = soup.find(class_=lambda c: c and any(desc in str(c).lower() 
                                                         for desc in ['description', 'about', 'company']))
        if desc_section:
            return self.extract_text_content(desc_section)
        
        return None
    
    def extract_logo_url(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """
        Extract manufacturer logo URL from page content.
        
        Args:
            soup: BeautifulSoup object of the page
            base_url: Base URL for resolving relative links
            
        Returns:
            Logo URL if found, None otherwise
        """
        # Try to find logo in common locations
        logo_img = None
        
        # Check for logo class
        logo_img = soup.find('img', class_=lambda c: c and 'logo' in str(c).lower())
        
        # Check for logo in header
        if not logo_img:
            header = soup.find('header')
            if header:
                logo_img = header.find('img')
        
        # Check for logo in common containers
        if not logo_img:
            for container_id in ['logo', 'header-logo', 'site-logo']:
                container = soup.find(id=container_id)
                if container:
                    logo_img = container.find('img')
                    if logo_img:
                        break
        
        # If we found a logo image, get its URL
        if logo_img and logo_img.get('src'):
            return urljoin(base_url, logo_img['src'])
        
        return None
    
    def extract_contact_info(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        Extract manufacturer contact information from page content.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Dictionary with contact information
        """
        contact_info = {}
        
        # Extract email addresses
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, str(soup))
        if emails:
            contact_info['email'] = emails[0]  # Take the first email
        
        # Extract phone numbers
        phone_pattern = r'\b(?:\+\d{1,3}[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}\b'
        phones = re.findall(phone_pattern, str(soup))
        if phones:
            contact_info['phone'] = phones[0]  # Take the first phone number
        
        # Extract address
        address_container = soup.find(class_=lambda c: c and any(addr in str(c).lower() 
                                                              for addr in ['address', 'location', 'contact']))
        if address_container:
            address = address_container.get_text().strip()
            if address:
                contact_info['address'] = address
        
        return contact_info


class ClaudeAnalyzer:
    """
    Handle interactions with Claude AI for content analysis and data extraction.
    
    This class encapsulates all Claude API interactions, including manufacturer detection,
    category extraction, and translation functionality.
    """
    
    def __init__(self, api_key: str, model: str, sonnet_model: str, logger):
        """
        Initialize the Claude analyzer.
        
        Args:
            api_key: Anthropic API key
            model: Claude model to use for basic analysis
            sonnet_model: Claude model to use for more complex analysis
            logger: Logger instance for logging API interactions
        """
        self.api_key = api_key
        self.model = model
        self.sonnet_model = sonnet_model
        self.logger = logger
        
        # We'll use direct API calls instead of the SDK to avoid compatibility issues
        self.api_base_url = "https://api.anthropic.com/v1/messages"
        self.headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }
        
        # Cache for API responses to avoid duplicate calls
        self._response_cache = {}
        
        self.logger.info(f"Claude Analyzer initialized with models: {model} and {sonnet_model}")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10),
          retry=retry_if_exception_type(RequestException))
    def is_manufacturer_page(self, url: str, title: str, content: str) -> bool:
        """
        Determine if a page is a manufacturer page.
        
        Args:
            url: URL of the page
            title: Page title
            content: Page text content
            
        Returns:
            True if the page is a manufacturer page, False otherwise
        """
        # Create a cache key
        cache_key = f"is_mfr_{hash(url + title)}"
        
        # Check cache first
        if cache_key in self._response_cache:
            self.logger.info(f"Using cached result for {url}")
            return self._response_cache[cache_key]
        
        # Prepare a truncated version of content to avoid token limits
        truncated_content = content[:5000] if content else ""
        
        try:
            # Construct the prompt
            prompt = f"""
            You are analyzing a webpage to determine if it's a manufacturer page.
            
            URL: {url}
            Title: {title}
            
            Content excerpt:
            {truncated_content}
            
            A manufacturer page is one that:
            1. Represents a company that makes physical products
            2. Contains information about product categories or specific products
            3. Is NOT a retailer, distributor, or marketplace that primarily sells products made by others
            
            Based on the URL, title, and content, is this a manufacturer page? 
            Answer with only YES or NO.
            """
            
            # Call the Claude API directly
            payload = {
                "model": self.model,
                "max_tokens": 100,
                "temperature": 0,
                "system": "You are a helpful AI assistant that analyzes web pages to identify manufacturer information.",
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            response = requests.post(
                self.api_base_url,
                headers=self.headers,
                json=payload
            )
            
            response.raise_for_status()
            response_data = response.json()
            
            # Process the response
            result_text = response_data['content'][0]['text'].strip().upper()
            is_manufacturer = "YES" in result_text
            
            # Cache the result
            self._response_cache[cache_key] = is_manufacturer
            
            self.logger.info(f"Claude analysis for {url}: {'Manufacturer page' if is_manufacturer else 'Not a manufacturer page'}")
            return is_manufacturer
            
        except Exception as e:
            self.logger.error(f"Error in Claude analysis for {url}: {str(e)}")
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10),
          retry=retry_if_exception_type(RequestException))
    def extract_manufacturers(self, url: str, title: str, content: str) -> Dict[str, Any]:
        """
        Extract manufacturer and category information from page content.
        
        Args:
            url: URL of the page
            title: Page title
            content: Page text content
            
        Returns:
            Dictionary with extracted manufacturer information
        """
        # Create a cache key
        cache_key = f"extract_{hash(url + title)}"
        
        # Check cache first
        if cache_key in self._response_cache:
            self.logger.info(f"Using cached extraction for {url}")
            return self._response_cache[cache_key]
        
        # Prepare a truncated version of content to avoid token limits
        truncated_content = content[:10000] if content else ""
        
        try:
            # Construct the prompt
            prompt = f"""
            You are analyzing a webpage to extract manufacturer and product category information.
            
            URL: {url}
            Title: {title}
            
            Content excerpt:
            {truncated_content}
            
            Extract the following information:
            1. Manufacturer name(s)
            2. Product categories for each manufacturer
            
            Format your response as JSON:
            {{
                "manufacturers": [
                    {{
                        "name": "Manufacturer Name",
                        "categories": ["Category 1", "Category 2", ...]
                    }},
                    ...
                ]
            }}
            
            If no manufacturers are found, return an empty array.
            """
            
            # Call the Claude API directly
            payload = {
                "model": self.sonnet_model,  # Use the more powerful model for extraction
                "max_tokens": 1000,
                "temperature": 0,
                "system": "You are a helpful AI assistant that extracts structured manufacturer and product category information from web pages.",
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            response = requests.post(
                self.api_base_url,
                headers=self.headers,
                json=payload
            )
            
            response.raise_for_status()
            response_data = response.json()
            
            # Process the response
            result_text = response_data['content'][0]['text']
            
            # Extract JSON from the response
            json_match = re.search(r'```json\s*(.*?)\s*```', result_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to find JSON without code blocks
                json_match = re.search(r'({.*})', result_text, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                else:
                    json_str = result_text
            
            # Parse the JSON
            try:
                result = json.loads(json_str)
                
                # Validate the structure
                if 'manufacturers' not in result:
                    result = {'manufacturers': []}
                
                # Cache the result
                self._response_cache[cache_key] = result
                
                self.logger.info(f"Extracted {len(result['manufacturers'])} manufacturers from {url}")
                return result
                
            except json.JSONDecodeError:
                self.logger.error(f"Failed to parse JSON from Claude response for {url}")
                return {'manufacturers': []}
            
        except Exception as e:
            self.logger.error(f"Error in Claude extraction for {url}: {str(e)}")
            return {'manufacturers': []}
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10),
          retry=retry_if_exception_type(RequestException))
    def translate_categories_batch(self, categories_text: str, manufacturer_name: str, 
                                  target_lang: str, response_format: str = 'delimited') -> List[str]:
        """
        Translate a batch of categories to a target language.
        
        Args:
            categories_text: Newline-separated list of categories
            manufacturer_name: Manufacturer name to preserve in translation
            target_lang: Target language code
            response_format: Format of the response ('delimited' or 'json')
            
        Returns:
            List of translated categories
        """
        try:
            # Create a cache key
            cache_key = f"translate_{hash(categories_text + target_lang)}"
            
            # Check cache first
            if cache_key in self._response_cache:
                self.logger.info(f"Using cached translations for {target_lang}")
                return self._response_cache[cache_key]
            
            # Construct the prompt
            prompt = f"""
            Translate the following product categories to {target_lang}.
            Keep the manufacturer name "{manufacturer_name}" unchanged in the translation.
            
            Categories:
            {categories_text}
            
            Return only the translations, one per line, in the same order as the input.
            """
            
            # Call the Claude API directly
            payload = {
                "model": self.model,
                "max_tokens": 1000,
                "temperature": 0,
                "system": f"You are a helpful AI assistant that translates product categories to {target_lang}.",
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            response = requests.post(
                self.api_base_url,
                headers=self.headers,
                json=payload
            )
            
            response.raise_for_status()
            response_data = response.json()
            
            # Process the response
            result_text = response_data['content'][0]['text'].strip()
            
            # Extract translations based on the requested format
            if response_format == 'json':
                # Try to parse as JSON
                try:
                    json_match = re.search(r'```json\s*(.*?)\s*```', result_text, re.DOTALL)
                    if json_match:
                        json_str = json_match.group(1)
                    else:
                        json_match = re.search(r'({.*})', result_text, re.DOTALL)
                        if json_match:
                            json_str = json_match.group(1)
                        else:
                            json_str = result_text
                    
                    translations = json.loads(json_str)
                    if isinstance(translations, dict) and 'translations' in translations:
                        translations = translations['translations']
                    
                except json.JSONDecodeError:
                    # Fallback to line-by-line parsing
                    translations = [line.strip() for line in result_text.split('\n') if line.strip()]
            else:
                # Simple line-by-line parsing
                translations = [line.strip() for line in result_text.split('\n') if line.strip()]
            
            # Filter out any non-translation lines (like numbering or explanations)
            categories = categories_text.strip().split('\n')
            if len(translations) > len(categories):
                translations = translations[:len(categories)]
            elif len(translations) < len(categories):
                # Pad with original categories if translations are missing
                translations.extend(categories[len(translations):])
            
            # Cache the result
            self._response_cache[cache_key] = translations
            
            self.logger.info(f"Translated {len(translations)} categories to {target_lang}")
            return translations
            
        except Exception as e:
            self.logger.error(f"Error in translation to {target_lang}: {str(e)}")
            # Return the original categories as fallback
            return categories_text.strip().split('\n')


class DatabaseHandler:
    """
    Handle all database operations for the competitor scraper.
    
    This class encapsulates database connections, transactions, and CRUD operations
    for manufacturers, categories, and URLs.
    """
    
    def __init__(self, db_url: str, logger):
        """
        Initialize the database handler.
        
        Args:
            db_url: SQLAlchemy database URL
            logger: Logger instance for logging database operations
        """
        self.db_url = db_url
        self.logger = logger
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        
        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)
        
        self.logger.info("Database handler initialized")
    
    def get_session(self):
        """
        Get a database session.
        
        Returns:
            SQLAlchemy session object
        """
        return self.Session()
    
    def add_url_to_queue(self, url: str, priority: float = 0.0, domain: str = None) -> None:
        """
        Add a URL to the crawl queue.
        
        Args:
            url: URL to add
            priority: Priority value (lower is higher priority)
            domain: Domain of the URL
        """
        try:
            with self.get_session() as session:
                # Check if URL already exists
                existing_url = session.query(CrawlQueue).filter_by(url=url).first()
                
                if existing_url:
                    self.logger.debug(f"URL {url} already in queue, updating priority")
                    existing_url.priority = priority
                else:
                    # Extract domain if not provided
                    if not domain:
                        parsed_url = urlparse(url)
                        domain = parsed_url.netloc
                    
                    # Add new URL
                    new_url = CrawlQueue(
                        url=url,
                        domain=domain,
                        priority=priority,
                        status='pending',
                        added_at=datetime.datetime.now()
                    )
                    session.add(new_url)
                
                session.commit()
                self.logger.debug(f"Added/updated URL in queue: {url} with priority {priority}")
        
        except Exception as e:
            self.logger.error(f"Error adding URL to queue: {str(e)}")
    
    def get_next_url(self) -> Optional[Tuple[str, float]]:
        """
        Get the next URL to crawl from the queue.
        
        Returns:
            Tuple of (URL, priority) or None if queue is empty
        """
        try:
            with self.get_session() as session:
                # Get the highest priority (lowest value) URL
                next_url = session.query(CrawlQueue).filter_by(
                    status='pending'
                ).order_by(
                    CrawlQueue.priority, CrawlQueue.added_at
                ).first()
                
                if next_url:
                    # Mark as in progress
                    next_url.status = 'in_progress'
                    next_url.started_at = datetime.datetime.now()
                    session.commit()
                    
                    self.logger.debug(f"Retrieved next URL from queue: {next_url.url}")
                    return next_url.url, next_url.priority
                else:
                    self.logger.debug("No URLs in queue")
                    return None
        
        except Exception as e:
            self.logger.error(f"Error getting next URL: {str(e)}")
            return None
    
    def mark_url_completed(self, url: str, success: bool = True) -> None:
        """
        Mark a URL as completed in the queue.
        
        Args:
            url: URL to mark
            success: Whether the crawl was successful
        """
        try:
            with self.get_session() as session:
                url_record = session.query(CrawlQueue).filter_by(url=url).first()
                
                if url_record:
                    url_record.status = 'completed' if success else 'failed'
                    url_record.completed_at = datetime.datetime.now()
                    session.commit()
                    
                    self.logger.debug(f"Marked URL as {'completed' if success else 'failed'}: {url}")
                else:
                    self.logger.warning(f"Attempted to mark non-existent URL as completed: {url}")
        
        except Exception as e:
            self.logger.error(f"Error marking URL as completed: {str(e)}")
    
    def add_manufacturer(self, name: str, website: str = None) -> Optional[int]:
        """
        Add a manufacturer to the database.
        
        Args:
            name: Manufacturer name
            website: Manufacturer website URL
            
        Returns:
            Manufacturer ID if successful, None otherwise
        """
        try:
            with self.get_session() as session:
                # Check if manufacturer already exists
                existing_mfr = session.query(Manufacturer).filter_by(name=name).first()
                
                if existing_mfr:
                    # Update website if provided and different
                    if website and existing_mfr.website != website:
                        existing_mfr.website = website
                        session.commit()
                    
                    self.logger.debug(f"Manufacturer already exists: {name}")
                    return existing_mfr.id
                
                # Add new manufacturer
                new_mfr = Manufacturer(
                    name=name,
                    website=website,
                    added_at=datetime.datetime.now()
                )
                session.add(new_mfr)
                session.commit()
                
                self.logger.info(f"Added new manufacturer: {name}")
                return new_mfr.id
        
        except Exception as e:
            self.logger.error(f"Error adding manufacturer: {str(e)}")
            return None
    
    def add_categories(self, manufacturer_id: int, categories: List[str], 
                      language: str = 'en') -> List[int]:
        """
        Add categories for a manufacturer.
        
        Args:
            manufacturer_id: Manufacturer ID
            categories: List of category names
            language: Language code
            
        Returns:
            List of category IDs
        """
        category_ids = []
        
        try:
            with self.get_session() as session:
                # Check if manufacturer exists
                manufacturer = session.query(Manufacturer).filter_by(id=manufacturer_id).first()
                
                if not manufacturer:
                    self.logger.error(f"Manufacturer with ID {manufacturer_id} not found")
                    return []
                
                # Process each category
                for category_name in categories:
                    # Check if category already exists for this manufacturer
                    existing_category = session.query(Category).filter_by(
                        manufacturer_id=manufacturer_id,
                        name=category_name,
                        language=language
                    ).first()
                    
                    if existing_category:
                        category_ids.append(existing_category.id)
                        continue
                    
                    # Add new category
                    new_category = Category(
                        manufacturer_id=manufacturer_id,
                        name=category_name,
                        language=language,
                        added_at=datetime.datetime.now()
                    )
                    session.add(new_category)
                    session.flush()  # Get ID without committing
                    
                    category_ids.append(new_category.id)
                
                session.commit()
                self.logger.info(f"Added {len(category_ids)} categories for manufacturer {manufacturer_id}")
                
                return category_ids
        
        except Exception as e:
            self.logger.error(f"Error adding categories: {str(e)}")
            return []
    
    def add_translated_categories(self, manufacturer_id: int, categories: Dict[str, List[str]]) -> None:
        """
        Add translated categories for a manufacturer.
        
        Args:
            manufacturer_id: Manufacturer ID
            categories: Dictionary mapping language codes to lists of category names
        """
        try:
            for language, category_list in categories.items():
                self.add_categories(manufacturer_id, category_list, language)
                
            self.logger.info(f"Added translations for manufacturer {manufacturer_id} in {len(categories)} languages")
        
        except Exception as e:
            self.logger.error(f"Error adding translated categories: {str(e)}")
    
    def log_error(self, url: str, error_type: str, error_message: str, session_id: Optional[int] = None) -> None:
        """
        Log an error to the database.
        
        Args:
            url: URL where the error occurred
            error_type: Type of error
            error_message: Error message
            session_id: Optional session ID to associate with the error
        """
        try:
            with self.get_session() as session:
                error_log = ErrorLog(
                    url=url,
                    error_type=error_type,
                    error_message=error_message,
                    timestamp=datetime.datetime.now(),
                    session_id=session_id
                )
                session.add(error_log)
                session.commit()
                
                self.logger.debug(f"Logged error for {url}: {error_type}")
        
        except Exception as e:
            self.logger.error(f"Error logging error: {str(e)}")
    
    def get_pending_url_count(self) -> int:
        """
        Get the count of pending URLs in the queue.
        
        Returns:
            Number of pending URLs
        """
        try:
            with self.get_session() as session:
                return session.query(CrawlQueue).filter_by(status='pending').count()
        
        except Exception as e:
            self.logger.error(f"Error getting pending URL count: {str(e)}")
            return 0
    
    def get_completed_url_count(self) -> int:
        """
        Get the count of completed URLs in the queue.
        
        Returns:
            Number of completed URLs
        """
        try:
            with self.get_session() as session:
                return session.query(CrawlQueue).filter_by(status='completed').count()
        
        except Exception as e:
            self.logger.error(f"Error getting completed URL count: {str(e)}")
            return 0
    
    def get_manufacturer_count(self) -> int:
        """
        Get the count of manufacturers in the database.
        
        Returns:
            Number of manufacturers
        """
        try:
            with self.get_session() as session:
                return session.query(Manufacturer).count()
        
        except Exception as e:
            self.logger.error(f"Error getting manufacturer count: {str(e)}")
            return 0
    
    def get_category_count(self) -> int:
        """
        Get the count of categories in the database.
        
        Returns:
            Number of categories
        """
        try:
            with self.get_session() as session:
                return session.query(Category).count()
        
        except Exception as e:
            self.logger.error(f"Error getting category count: {str(e)}")
            return 0


class ConfigManager:
    """
    Load and manage configuration for the competitor scraper.
    
    This class handles loading configuration from YAML files, validating settings,
    and providing access to configuration values.
    """
    
    def __init__(self, config_path: str, logger):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to the configuration YAML file
            logger: Logger instance for logging configuration operations
        """
        self.config_path = config_path
        self.logger = logger
        self.config = {}
        
        # Load configuration
        self._load_config()
        
        self.logger.info(f"Configuration loaded from {config_path}")
    
    def _load_config(self) -> None:
        """
        Load configuration from the YAML file.
        """
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            
            # Validate configuration
            self._validate_config()
            
        except Exception as e:
            self.logger.error(f"Error loading configuration: {str(e)}")
            raise ValueError(f"Failed to load configuration from {self.config_path}: {str(e)}")
    
    def _validate_config(self) -> None:
        """
        Validate the loaded configuration.
        
        Raises:
            ValueError: If configuration is invalid
        """
        required_sections = ['database', 'crawling', 'ai_apis']
        required_fields = {
            'database': ['path'],
            'crawling': ['max_depth', 'delay_between_requests', 'user_agent'],
            'ai_apis': ['anthropic']
        }
        
        # Check required sections
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required configuration section: {section}")
            
            # Check required fields in each section
            for field in required_fields[section]:
                if field not in self.config[section]:
                    raise ValueError(f"Missing required configuration field: {section}.{field}")
        
        # Validate specific fields
        if 'max_urls_per_domain' in self.config['crawling']:
            if not isinstance(self.config['crawling']['max_urls_per_domain'], int) or self.config['crawling']['max_urls_per_domain'] <= 0:
                raise ValueError("max_urls_per_domain must be a positive integer")
        
        if not isinstance(self.config['crawling']['max_depth'], int) or self.config['crawling']['max_depth'] <= 0:
            raise ValueError("max_depth must be a positive integer")
        
        if 'request_timeout' in self.config['crawling']:
            if not isinstance(self.config['crawling']['request_timeout'], (int, float)) or self.config['crawling']['request_timeout'] <= 0:
                raise ValueError("request_timeout must be a positive number")
        
        # Check for target languages
        if 'languages' in self.config and 'targets' in self.config['languages']:
            if not isinstance(self.config['languages']['targets'], dict) or not self.config['languages']['targets']:
                raise ValueError("languages.targets must be a non-empty dictionary")
    
    def get_database_url(self) -> str:
        """
        Get the database URL.
        
        Returns:
            Database URL formatted for SQLAlchemy
        """
        db_path = self.config['database']['path']
        # Convert relative path to absolute path if needed
        if not os.path.isabs(db_path):
            db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), db_path)
        
        # Format as SQLAlchemy URL
        return f'sqlite:///{db_path}'
    
    def get_claude_api_key(self) -> str:
        """
        Get the Claude API key.
        
        Returns:
            Claude API key
        """
        # First check environment variable
        api_key = os.environ.get('ANTHROPIC_API_KEY')
        
        # Fall back to config file if not in environment
        if not api_key:
            api_key = self.config['ai_apis']['anthropic']['api_key']
        
        return api_key
    
    def get_claude_models(self) -> Tuple[str, str]:
        """
        Get the Claude model names.
        
        Returns:
            Tuple of (standard model, sonnet model)
        """
        return (
            self.config['ai_apis']['anthropic']['model'],
            self.config['ai_apis']['anthropic']['sonnet_model']
        )
    
    def get_scraper_settings(self) -> Dict[str, Any]:
        """
        Get scraper settings.
        
        Returns:
            Dictionary with scraper settings
        """
        return self.config['crawling']
    
    def get_target_languages(self) -> List[str]:
        """
        Get target languages for translation.
        
        Returns:
            List of language codes
        """
        if 'languages' in self.config and 'targets' in self.config['languages']:
            return list(self.config['languages']['targets'].keys())
        return []
    
    def get_user_agent(self) -> str:
        """
        Get the user agent string for HTTP requests.
        
        Returns:
            User agent string
        """
        return self.config['crawling'].get('user_agent', 'Mozilla/5.0 (compatible; NDAIVI/1.0)')
    
    def get_request_timeout(self) -> float:
        """
        Get the HTTP request timeout.
        
        Returns:
            Request timeout in seconds
        """
        return self.config['crawling'].get('request_timeout', 30.0)
    
    def get_max_urls_per_domain(self) -> int:
        """
        Get the maximum number of URLs to crawl per domain.
        
        Returns:
            Maximum URLs per domain
        """
        return self.config['crawling'].get('max_urls_per_domain', 100)
    
    def get_max_depth(self) -> int:
        """
        Get the maximum crawl depth.
        
        Returns:
            Maximum crawl depth
        """
        return self.config['crawling'].get('max_depth', 3)
    
    def get_seed_urls(self) -> List[str]:
        """
        Get seed URLs for crawling.
        
        Returns:
            List of seed URLs
        """
        # Check in various possible config locations
        seed_urls = []
        
        # First check in scraper section
        if 'scraper' in self.config and 'seed_urls' in self.config['scraper']:
            seed_urls = self.config['scraper']['seed_urls']
            self.logger.debug(f"Found seed URLs in scraper section: {seed_urls}")
        
        # Then check in competitor_scraper section
        elif 'competitor_scraper' in self.config and 'seed_urls' in self.config['competitor_scraper']:
            seed_urls = self.config['competitor_scraper']['seed_urls']
            self.logger.debug(f"Found seed URLs in competitor_scraper section: {seed_urls}")
        
        # Finally check in crawling section
        elif 'crawling' in self.config and 'seed_urls' in self.config['crawling']:
            seed_urls = self.config['crawling']['seed_urls']
            self.logger.debug(f"Found seed URLs in crawling section: {seed_urls}")
        
        return seed_urls


class StatisticsManager:
    """
    Track and report statistics for the scraper.
    
    This class manages session tracking, performance metrics, and reporting
    for the scraper's operation.
    """
    
    def __init__(self, db_handler: DatabaseHandler, logger):
        """
        Initialize the statistics manager.
        
        Args:
            db_handler: Database handler instance
            logger: Logger instance for logging statistics operations
        """
        self.db_handler = db_handler
        self.logger = logger
        self.session_id = None
        self.start_time = None
        self.url_count = 0
        self.manufacturer_count = 0
        self.category_count = 0
        self.error_count = 0
        
        # Performance metrics
        self.request_times = []
        self.processing_times = []
        self.ai_request_times = []
        
        self.logger.info("Statistics manager initialized")
    
    def start_session(self, session_type: str = 'scraper') -> int:
        """
        Start a new scraper session.
        
        Args:
            session_type: Type of session ('scraper', 'finder', or 'combined')
            
        Returns:
            Session ID
        """
        try:
            self.start_time = datetime.datetime.now()
            
            with self.db_handler.get_session() as session:
                new_session = ScraperSession(
                    start_time=self.start_time,
                    status='running',
                    session_type=session_type
                )
                session.add(new_session)
                session.commit()
                
                self.session_id = new_session.id
                self.logger.info(f"Started new scraper session with ID {self.session_id}")
                
                return self.session_id
        
        except Exception as e:
            self.logger.error(f"Error starting session: {str(e)}")
            return -1
    
    def end_session(self, status: str = 'completed') -> None:
        """
        End the current scraper session.
        
        Args:
            status: Session status ('completed', 'stopped', 'error')
        """
        if not self.session_id:
            self.logger.warning("Attempted to end session, but no active session found")
            return
        
        try:
            end_time = datetime.datetime.now()
            duration = (end_time - self.start_time).total_seconds()
            
            with self.db_handler.get_session() as session:
                scraper_session = session.query(ScraperSession).filter_by(id=self.session_id).first()
                
                if scraper_session:
                    scraper_session.end_time = end_time
                    scraper_session.duration = duration
                    scraper_session.status = status
                    scraper_session.url_count = self.url_count
                    scraper_session.manufacturer_count = self.manufacturer_count
                    scraper_session.category_count = self.category_count
                    scraper_session.error_count = self.error_count
                    
                    session.commit()
                    
                    self.logger.info(f"Ended session {self.session_id} with status '{status}' after {duration:.2f} seconds")
                else:
                    self.logger.warning(f"Session {self.session_id} not found when trying to end it")
            
            # Reset session data
            self.session_id = None
            self.start_time = None
            self.url_count = 0
            self.manufacturer_count = 0
            self.category_count = 0
            self.error_count = 0
            self.request_times = []
            self.processing_times = []
            self.ai_request_times = []
        
        except Exception as e:
            self.logger.error(f"Error ending session: {str(e)}")
    
    def log_url_processed(self, url: str, success: bool, duration: float, session_id: Optional[int] = None) -> None:
        """
        Log a processed URL.
        
        Args:
            url: Processed URL
            success: Whether processing was successful
            duration: Processing duration in seconds
            session_id: Optional session ID to associate with the log
        """
        try:
            # Use the provided session_id or the instance's session_id
            session_id_to_use = session_id if session_id is not None else self.session_id
            
            if not session_id_to_use:
                self.logger.warning(f"Attempted to log URL {url}, but no session ID provided")
                return
            
            self.url_count += 1
            self.request_times.append(duration)
            
            with self.db_handler.get_session() as session:
                # Update the session record
                scraper_session = session.query(ScraperSession).filter_by(id=session_id_to_use).first()
                if scraper_session:
                    scraper_session.urls_processed = (scraper_session.urls_processed or 0) + 1
                    if not success:
                        scraper_session.errors = (scraper_session.errors or 0) + 1
                
                # Create log entry
                log_entry = ScraperLog(
                    level="INFO" if success else "ERROR",
                    message=f"Processed URL: {url} (success={success}, duration={duration:.2f}s)"
                )
                session.add(log_entry)
                session.commit()
                
                self.logger.debug(f"Logged URL {url} (success={success}, duration={duration:.2f}s)")
        
        except Exception as e:
            self.logger.error(f"Error logging URL: {str(e)}")
    
    def log_manufacturer_found(self, manufacturer_name: str, url: str, session_id: Optional[int] = None) -> None:
        """
        Log a found manufacturer.
        
        Args:
            manufacturer_name: Name of the manufacturer
            url: URL where the manufacturer was found
            session_id: Optional session ID to associate with the log
        """
        try:
            # Use the provided session_id or the instance's session_id
            session_id_to_use = session_id if session_id is not None else self.session_id
            
            if not session_id_to_use:
                self.logger.warning(f"Attempted to log manufacturer {manufacturer_name}, but no session ID provided")
                return
            
            self.manufacturer_count += 1
            self.logger.info(f"Found manufacturer: {manufacturer_name} at {url}")
        
        except Exception as e:
            self.logger.error(f"Error logging manufacturer: {str(e)}")
    
    def log_categories_found(self, manufacturer_name: str, categories: List[str], url: str, session_id: Optional[int] = None) -> None:
        """
        Log found categories.
        
        Args:
            manufacturer_name: Name of the manufacturer
            categories: List of category names
            url: URL where the categories were found
            session_id: Optional session ID to associate with the log
        """
        try:
            # Use the provided session_id or the instance's session_id
            session_id_to_use = session_id if session_id is not None else self.session_id
            
            if not session_id_to_use:
                self.logger.warning(f"Attempted to log categories for {manufacturer_name}, but no session ID provided")
                return
            
            self.category_count += len(categories)
            self.logger.info(f"Found {len(categories)} categories for {manufacturer_name} at {url}")
        
        except Exception as e:
            self.logger.error(f"Error logging categories: {str(e)}")
    
    def log_error(self, url: str, error_type: str, error_message: str, session_id: Optional[int] = None) -> None:
        """
        Log an error.
        
        Args:
            url: URL where the error occurred
            error_type: Type of error
            error_message: Error message
            session_id: Optional session ID to associate with the error
        """
        try:
            # Use the provided session_id or the instance's session_id
            session_id_to_use = session_id if session_id is not None else self.session_id
            
            if not session_id_to_use:
                self.logger.warning(f"Attempted to log error for {url}, but no session ID provided")
                return
            
            self.error_count += 1
            
            # Log to database via the DB handler
            self.db_handler.log_error(url, error_type, error_message, session_id_to_use)
            
            self.logger.error(f"Error at {url}: {error_type} - {error_message}")
        
        except Exception as e:
            self.logger.error(f"Error logging error: {str(e)}")
    
    def log_ai_request(self, request_type: str, duration: float, tokens: int = 0, session_id: Optional[int] = None) -> None:
        """
        Log an AI API request.
        
        Args:
            request_type: Type of request (e.g., 'manufacturer_detection', 'category_extraction')
            duration: Request duration in seconds
            tokens: Number of tokens used
            session_id: Optional session ID to associate with the log
        """
        try:
            # Use the provided session_id or the instance's session_id
            session_id_to_use = session_id if session_id is not None else self.session_id
            
            if not session_id_to_use:
                self.logger.warning(f"Attempted to log AI request, but no session ID provided")
                return
            
            self.ai_request_times.append(duration)
            
            # Update session record
            with self.db_handler.get_session() as session:
                scraper_session = session.query(ScraperSession).filter_by(id=session_id_to_use).first()
                if scraper_session:
                    # Update token count if applicable
                    if tokens > 0 and hasattr(scraper_session, 'tokens_used'):
                        scraper_session.tokens_used = (scraper_session.tokens_used or 0) + tokens
                    session.commit()
            
            self.logger.debug(f"AI request ({request_type}): {duration:.2f}s, {tokens} tokens")
        
        except Exception as e:
            self.logger.error(f"Error logging AI request: {str(e)}")
    
    def get_current_stats(self) -> Dict[str, Any]:
        """
        Get current statistics for the session.
        
        Returns:
            Dictionary with current statistics
        """
        if not self.session_id:
            return {
                'status': 'no_active_session',
                'message': 'No active scraper session'
            }
        
        try:
            current_time = datetime.datetime.now()
            duration = (current_time - self.start_time).total_seconds()
            
            # Calculate averages
            avg_request_time = sum(self.request_times) / max(len(self.request_times), 1)
            avg_ai_request_time = sum(self.ai_request_times) / max(len(self.ai_request_times), 1)
            
            # Get database counts
            pending_urls = self.db_handler.get_pending_url_count()
            completed_urls = self.db_handler.get_completed_url_count()
            total_manufacturers = self.db_handler.get_manufacturer_count()
            total_categories = self.db_handler.get_category_count()
            
            return {
                'status': 'active',
                'session_id': self.session_id,
                'start_time': self.start_time.isoformat(),
                'duration': duration,
                'urls_processed': self.url_count,
                'manufacturers_found': self.manufacturer_count,
                'categories_found': self.category_count,
                'errors': self.error_count,
                'avg_request_time': avg_request_time,
                'avg_ai_request_time': avg_ai_request_time,
                'pending_urls': pending_urls,
                'completed_urls': completed_urls,
                'total_manufacturers': total_manufacturers,
                'total_categories': total_categories
            }
        
        except Exception as e:
            self.logger.error(f"Error getting current stats: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error getting statistics: {str(e)}"
            }
    
    def get_session_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get history of scraper sessions.
        
        Args:
            limit: Maximum number of sessions to return
            
        Returns:
            List of session data dictionaries
        """
        try:
            with self.db_handler.get_session() as session:
                sessions = session.query(ScraperSession).order_by(
                    ScraperSession.start_time.desc()
                ).limit(limit).all()
                
                history = []
                for s in sessions:
                    history.append({
                        'id': s.id,
                        'start_time': s.start_time.isoformat() if s.start_time else None,
                        'end_time': s.end_time.isoformat() if s.end_time else None,
                        'duration': s.duration,
                        'status': s.status,
                        'url_count': s.url_count,
                        'manufacturer_count': s.manufacturer_count,
                        'category_count': s.category_count,
                        'error_count': s.error_count
                    })
                
                return history
        
        except Exception as e:
            self.logger.error(f"Error getting session history: {str(e)}")
            return []
    
    def print_stats(self) -> None:
        """
        Print current statistics to the console.
        """
        stats = self.get_current_stats()
        
        if stats['status'] == 'no_active_session':
            print("No active scraper session")
            
            # Print history instead
            history = self.get_session_history(5)
            if history:
                print("\nRecent sessions:")
                for i, session in enumerate(history):
                    print(f"{i+1}. Session {session['id']} ({session['status']})")
                    print(f"   Started: {session['start_time']}")
                    if session['end_time']:
                        print(f"   Ended: {session['end_time']}")
                    if session['duration']:
                        print(f"   Duration: {session['duration']:.2f} seconds")
                    print(f"   URLs: {session['url_count']}")
                    print(f"   Manufacturers: {session['manufacturer_count']}")
                    print(f"   Categories: {session['category_count']}")
                    print(f"   Errors: {session['error_count']}")
                    print()
            else:
                print("No session history found")
            
            return
        
        print("\n=== Scraper Statistics ===")
        print(f"Session ID: {stats['session_id']}")
        print(f"Running for: {stats['duration']:.2f} seconds")
        print(f"URLs processed: {stats['urls_processed']}")
        print(f"Manufacturers found: {stats['manufacturers_found']}")
        print(f"Categories found: {stats['categories_found']}")
        print(f"Errors: {stats['errors']}")
        print(f"Average request time: {stats['avg_request_time']:.2f} seconds")
        print(f"Average AI request time: {stats['avg_ai_request_time']:.2f} seconds")
        print(f"Pending URLs: {stats['pending_urls']}")
        print(f"Completed URLs: {stats['completed_urls']}")
        print(f"Total manufacturers in DB: {stats['total_manufacturers']}")
        print(f"Total categories in DB: {stats['total_categories']}")
        print("===========================\n")


class CompetitorScraper:
    """
    Main class for the competitor scraper.
    
    This class coordinates the overall scraping process, managing the URL queue,
    content extraction, AI analysis, database operations, and statistics tracking.
    """
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize the competitor scraper.
        
        Args:
            config_path: Path to the configuration YAML file
        """
        # Set up logging
        self.logger = self._setup_logging()
        self.logger.info("Initializing CompetitorScraper")
        
        # Load configuration
        self.config_manager = ConfigManager(config_path, self.logger)
        
        # Initialize database handler
        db_path = self.config_manager.get_database_url()
        self.db_handler = DatabaseHandler(db_path, self.logger)
        
        # Initialize statistics manager
        self.stats_manager = StatisticsManager(self.db_handler, self.logger)
        
        # Initialize URL priority calculator
        self.url_priority = UrlPriority()
        
        # Initialize URL queue
        self.url_queue = PriorityUrlQueue()
        
        # Initialize content extractor
        user_agent = self.config_manager.get_user_agent()
        timeout = self.config_manager.get_request_timeout()
        self.content_extractor = ContentExtractor(user_agent, timeout, self.logger)
        
        # Initialize Claude analyzer
        claude_api_key = self.config_manager.get_claude_api_key()
        model, sonnet_model = self.config_manager.get_claude_models()
        self.claude_analyzer = ClaudeAnalyzer(claude_api_key, model, sonnet_model, self.logger)
        
        # Scraper settings
        self.max_urls_per_domain = self.config_manager.get_max_urls_per_domain()
        self.max_depth = self.config_manager.get_max_depth()
        self.target_languages = self.config_manager.get_target_languages()
        
        # State variables
        self.running = False
        self.suspended = False
        self.domain_url_counts = defaultdict(int)
        self.visited_urls = set()
        
        # Signal handlers are already registered globally at the top of the file
        
        self.logger.info("CompetitorScraper initialized")
    
    def _setup_logging(self) -> logging.Logger:
        """
        Set up logging for the scraper.
        
        Returns:
            Logger instance
        """
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        
        # Create logger
        logger = logging.getLogger('competitor_scraper')
        
        # Only set up handlers if they don't already exist
        if not logger.handlers:
            logger.setLevel(logging.DEBUG)
            
            # Create file handler
            file_handler = logging.FileHandler('logs/competitor_scraper.log')
            file_handler.setLevel(logging.DEBUG)
            
            # Create console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # Create formatter
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            # Add handlers to logger
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
            
            # Prevent propagation to root logger to avoid duplicate logs
            logger.propagate = False
        
        return logger
    
    def add_seed_urls(self, urls: List[str]) -> None:
        """
        Add seed URLs to the queue.
        
        Args:
            urls: List of seed URLs
        """
        for url in urls:
            # Normalize URL
            url = self._normalize_url(url)
            
            # Calculate priority
            priority = UrlPriority.calculate_seed_priority(url)
            
            # Add to queue
            self.url_queue.push(url, 0, priority)
            
            # Add to database
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            self.db_handler.add_url_to_queue(url, priority, domain)
            
            self.logger.info(f"Added seed URL: {url} with priority {priority}")
    
    def _normalize_url(self, url: str) -> str:
        """
        Normalize a URL.
        
        Args:
            url: URL to normalize
            
        Returns:
            Normalized URL
        """
        # Add scheme if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        # Parse and reconstruct to normalize
        parsed = urlparse(url)
        
        # Remove trailing slash from path
        path = parsed.path
        if path.endswith('/') and len(path) > 1:
            path = path[:-1]
        
        # Reconstruct URL
        normalized = ParseResult(
            scheme=parsed.scheme,
            netloc=parsed.netloc,
            path=path,
            params=parsed.params,
            query=parsed.query,
            fragment=''  # Remove fragments
        ).geturl()
        
        return normalized
    
    def start(self, seed_urls: List[str] = None, max_pages: int = None) -> None:
        """
        Start the scraper.
        
        Args:
            seed_urls: Optional list of seed URLs to add
            max_pages: Maximum number of pages to process in this run
        """
        self.logger.info("Starting scraper")
        
        # Start statistics session
        self.stats_manager.start_session(session_type='scraper')
        
        # Set running state
        self.running = True
        self.suspended = False
        
        # Add seed URLs if provided
        if seed_urls:
            self.add_seed_urls(seed_urls)
        
        # Add seed URLs from config if queue is empty
        if self.url_queue.is_empty():
            self.logger.debug("URL queue is empty, looking for seed URLs in config")
            config_seed_urls = self.config_manager.get_seed_urls()
            self.logger.debug(f"Found seed URLs in config: {config_seed_urls}")
            if config_seed_urls:
                self.logger.info(f"Adding {len(config_seed_urls)} seed URLs from config")
                self.add_seed_urls(config_seed_urls)
            else:
                self.logger.warning("No seed URLs provided or found in config")
                self.stop()
                return
        
        # Main scraping loop
        try:
            pages_processed = 0
            while self.running and not self.url_queue.is_empty():
                # Check for global shutdown signal
                if SHUTDOWN_FLAG:
                    self.logger.info("Shutdown signal received, stopping scraper")
                    break
                
                # Check for suspension
                if SUSPEND_FLAG or self.suspended:
                    self.suspended = True
                    self.logger.info("Scraper suspended, waiting for resume signal")
                    time.sleep(5)
                    continue
                
                # Check max pages limit
                if max_pages is not None and pages_processed >= max_pages:
                    self.logger.info(f"Reached max pages limit ({max_pages}), stopping")
                    break
                
                # Get next URL from queue
                url_info = self.url_queue.pop()
                if not url_info:
                    self.logger.info("URL queue is empty")
                    break
                
                url, depth, _ = url_info
                
                # Skip if already visited
                if url in self.visited_urls:
                    continue
                
                # Process URL
                self._process_url(url, depth)
                
                # Increment pages processed counter
                pages_processed += 1
                
                # Add to visited URLs
                self.visited_urls.add(url)
                
                # Small delay to avoid overwhelming servers
                time.sleep(0.5)
            
            self.logger.info("Scraper finished")
            
        except Exception as e:
            self.logger.error(f"Error in scraper main loop: {str(e)}")
            self.stats_manager.end_session('error')
            raise
        
        finally:
            # End statistics session
            if self.running:
                self.stats_manager.end_session('completed')
            
            # Reset state
            self.running = False
    
    def stop(self) -> None:
        """
        Stop the scraper.
        """
        self.logger.info("Stopping scraper")
        self.running = False
        
        # End statistics session
        self.stats_manager.end_session('stopped')
    
    def suspend(self) -> None:
        """
        Suspend the scraper.
        """
        self.logger.info("Suspending scraper")
        self.suspended = True
    
    def resume(self) -> None:
        """
        Resume the scraper.
        """
        self.logger.info("Resuming scraper")
        self.suspended = False
        global SUSPEND_FLAG
        SUSPEND_FLAG = False
    
    def _process_url(self, url: str, depth: int = 0) -> None:
        """
        Process a single URL: fetch content, analyze, and extract data.
        
        Args:
            url: URL to process
            depth: Current crawl depth
        """
        start_time = time.time()
        
        try:
            self.logger.info(f"Processing URL: {url} (depth={depth})")
            
            # Skip if URL has already been processed
            if url in self.visited_urls:
                self.logger.debug(f"URL already processed: {url}")
                return
            
            # Extract domain
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            
            # Check domain limits
            if domain in self.domain_url_counts and self.domain_url_counts[domain] >= self.max_urls_per_domain:
                self.logger.debug(f"Domain limit reached for {domain}")
                return
            
            # Update domain counter
            if domain not in self.domain_url_counts:
                self.domain_url_counts[domain] = 0
            self.domain_url_counts[domain] += 1
            
            # Fetch content
            try:
                html_content = self.content_extractor.fetch_url(url)
                if not html_content:
                    self.logger.warning(f"No content retrieved from URL: {url}")
                    self.db_handler.mark_url_completed(url, success=False)
                    return
            except Exception as e:
                self.logger.error(f"Error fetching URL {url}: {str(e)}")
                self.db_handler.mark_url_completed(url, success=False)
                return
            
            # Extract text and metadata
            title = html_content[0]
            text_content = html_content[1]
            metadata = html_content[2]
            
            # Analyze with Claude to detect manufacturer page
            is_manufacturer = self._analyze_manufacturer_page(url, title, text_content)
            
            if is_manufacturer:
                self._process_manufacturer_page(url, title, text_content)
            
            # Extract links for further crawling
            if depth < self.max_depth:
                self._extract_and_queue_links(url, text_content, depth)
            
            # Mark URL as completed
            self.db_handler.mark_url_completed(url, success=True)
            
            # Add to visited URLs
            self.visited_urls.add(url)
            
            # Log URL processing
            duration = time.time() - start_time
            self.stats_manager.log_url_processed(url, True, duration, self.stats_manager.session_id)
            
        except Exception as e:
            self.logger.error(f"Error processing URL {url}: {str(e)}")
            self.db_handler.mark_url_completed(url, success=False)
            self.stats_manager.log_error(url, "processing_error", str(e), self.stats_manager.session_id)
            self.stats_manager.log_url_processed(url, False, time.time() - start_time, self.stats_manager.session_id)
    
    def _analyze_manufacturer_page(self, url: str, title: str, content: str) -> bool:
        """
        Analyze if a page is a manufacturer page.
        
        Args:
            url: URL of the page
            title: Page title
            content: Page text content
            
        Returns:
            True if the page is a manufacturer page, False otherwise
        """
        try:
            start_time = time.time()
            
            # Use Claude to analyze the page
            is_manufacturer = self.claude_analyzer.is_manufacturer_page(url, title, content)
            
            if is_manufacturer:
                # Log AI request
                duration = time.time() - start_time
                self.stats_manager.log_ai_request('manufacturer_detection', duration, session_id=self.stats_manager.session_id)
                
                return is_manufacturer
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error analyzing manufacturer page {url}: {str(e)}")
            return False
    
    def _quick_manufacturer_check(self, title: str, content: str) -> bool:
        """
        Perform a quick keyword-based check for manufacturer pages.
        
        Args:
            title: Page title
            content: Page text content
            
        Returns:
            True if the page might be a manufacturer page, False otherwise
        """
        # Keywords that suggest a manufacturer page
        manufacturer_keywords = [
            'products', 'product categories', 'our products', 'product lines',
            'catalog', 'catalogue', 'manufacturing', 'manufacturer', 'we manufacture',
            'about us', 'company', 'factory', 'production'
        ]
        
        # Keywords that suggest NOT a manufacturer page
        non_manufacturer_keywords = [
            'shopping cart', 'add to cart', 'checkout', 'buy now', 'price',
            'login', 'sign in', 'register', 'account', 'blog', 'news', 'article',
            'privacy policy', 'terms of service', 'contact us', 'support'
        ]
        
        # Check title
        title_lower = title.lower()
        if any(keyword in title_lower for keyword in manufacturer_keywords):
            return True
        
        # Check first 1000 characters of content
        content_sample = content[:1000].lower()
        
        # Count manufacturer keywords
        mfr_keyword_count = sum(1 for keyword in manufacturer_keywords if keyword in content_sample)
        
        # Count non-manufacturer keywords
        non_mfr_keyword_count = sum(1 for keyword in non_manufacturer_keywords if keyword in content_sample)
        
        # If more manufacturer keywords than non-manufacturer keywords, it might be a manufacturer page
        return mfr_keyword_count > non_mfr_keyword_count
    
    def _process_manufacturer_page(self, url: str, title: str, content: str) -> None:
        """
        Process a manufacturer page: extract manufacturer and category information.
        
        Args:
            url: URL of the page
            title: Page title
            content: Page text content
        """
        try:
            start_time = time.time()
            
            # Use Claude to extract manufacturer information
            result = self.claude_analyzer.extract_manufacturers(url, title, content)
            
            # Log AI request
            duration = time.time() - start_time
            self.stats_manager.log_ai_request('manufacturer_extraction', duration, session_id=self.stats_manager.session_id)
            
            # Process each manufacturer
            for manufacturer in result.get('manufacturers', []):
                manufacturer_name = manufacturer.get('name')
                categories = manufacturer.get('categories', [])
                
                if not manufacturer_name:
                    continue
                
                # Log manufacturer found
                self.stats_manager.log_manufacturer_found(manufacturer_name, url, session_id=self.stats_manager.session_id)
                
                # Add manufacturer to database
                manufacturer_id = self.db_handler.add_manufacturer(manufacturer_name, url)
                
                if not manufacturer_id:
                    self.logger.error(f"Failed to add manufacturer {manufacturer_name} to database")
                    continue
                
                # Log categories found
                if categories:
                    self.stats_manager.log_categories_found(manufacturer_name, categories, url, session_id=self.stats_manager.session_id)
                    
                    # Add categories to database
                    self.db_handler.add_categories(manufacturer_id, categories)
                    
                    # Translate categories if needed
                    if self.target_languages and categories:
                        self._translate_categories(manufacturer_id, manufacturer_name, categories)
            
        except Exception as e:
            self.logger.error(f"Error extracting manufacturer info from {url}: {str(e)}")
            self.stats_manager.log_error(url, "extraction_error", str(e), self.stats_manager.session_id)
    
    def _translate_categories(self, manufacturer_id: int, manufacturer_name: str, categories: List[str]) -> None:
        """
        Translate categories to target languages.
        
        Args:
            manufacturer_id: Manufacturer ID
            manufacturer_name: Manufacturer name
            categories: List of category names in English
        """
        try:
            # Skip if no categories or no target languages
            if not categories or not self.target_languages:
                return
            
            # Prepare categories text
            categories_text = '\n'.join(categories)
            
            # Translate to each target language
            translations = {}
            for lang in self.target_languages:
                # Skip English
                if lang.lower() == 'en':
                    continue
                
                start_time = time.time()
                
                # Translate categories
                translated_categories = self.claude_analyzer.translate_categories_batch(
                    categories_text, manufacturer_name, lang
                )
                
                # Log AI request
                duration = time.time() - start_time
                self.stats_manager.log_ai_request(f'translation_{lang}', duration, session_id=self.stats_manager.session_id)
                
                # Add to translations dictionary
                translations[lang] = translated_categories
            
            # Add translated categories to database
            if translations:
                self.db_handler.add_translated_categories(manufacturer_id, translations)
                self.logger.info(f"Added translations for {manufacturer_name} in {len(translations)} languages")
            
        except Exception as e:
            self.logger.error(f"Error translating categories for {manufacturer_name}: {str(e)}")
            self.stats_manager.log_error(url, "translation_error", str(e), self.stats_manager.session_id)
    
    def _extract_and_queue_links(self, url: str, html_content: str, depth: int) -> None:
        """
        Extract links from HTML content and add them to the queue.
        
        Args:
            url: Source URL
            html_content: HTML content
            depth: Current crawl depth
        """
        try:
            # Parse the source URL
            parsed_source = urlparse(url)
            base_url = f"{parsed_source.scheme}://{parsed_source.netloc}"
            
            # Parse HTML content
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract links
            links = {}
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                # Skip empty or javascript links
                if not href or href.startswith('javascript:') or href == '#':
                    continue
                
                # Resolve relative URLs
                full_url = urljoin(base_url, href)
                
                # Get the anchor text
                text = a_tag.get_text().strip()
                if not text and a_tag.find('img'):
                    # Use alt text for image links
                    img = a_tag.find('img')
                    text = img.get('alt', '') if img else ''
                
                links[full_url] = text
            
            # Process each link
            for link in links:
                # Skip if already visited
                if link in self.visited_urls:
                    continue
                
                # Calculate priority
                priority = UrlPriority.calculate_priority(link, depth + 1)
                
                # Add to queue
                self.url_queue.push(link, depth + 1, priority)
                
                # Add to database
                parsed_link = urlparse(link)
                domain = parsed_link.netloc
                self.db_handler.add_url_to_queue(link, priority, domain)
            
            self.logger.debug(f"Extracted and queued {len(links)} links from {url}")
            
        except Exception as e:
            self.logger.error(f"Error extracting links from {url}: {str(e)}")
            self.stats_manager.log_error(url, "link_extraction_error", str(e), self.stats_manager.session_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get current statistics.
        
        Returns:
            Dictionary with current statistics
        """
        return self.stats_manager.get_current_stats()
    
    def print_stats(self) -> None:
        """
        Print current statistics to the console.
        """
        self.stats_manager.print_stats()
    
    def get_session_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get history of scraper sessions.
        
        Args:
            limit: Maximum number of sessions to return
            
        Returns:
            List of session data dictionaries
        """
        return self.stats_manager.get_session_history(limit)
    
    def interactive_mode(self) -> None:
        """
        Run the scraper in interactive mode.
        """
        print("\nCompetitor Scraper Interactive Mode")
        print("==================================")
        
        while True:
            print("\nAvailable commands:")
            print("1. start - Start the scraper")
            print("2. stop - Stop the scraper")
            print("3. suspend - Suspend the scraper")
            print("4. resume - Resume the scraper")
            print("5. stats - Show current statistics")
            print("6. history - Show session history")
            print("7. add <url> - Add a seed URL")
            print("8. exit - Exit interactive mode")
            
            command = input("\nEnter command: ").strip()
            
            if command == "start":
                seed_url = input("Enter seed URL (leave empty to use config): ").strip()
                if seed_url:
                    self.start([seed_url])
                else:
                    self.start()
                
            elif command == "stop":
                self.stop()
                
            elif command == "suspend":
                self.suspend()
                
            elif command == "resume":
                self.resume()
                
            elif command == "stats":
                self.print_stats()
                
            elif command == "history":
                history = self.get_session_history(5)
                if history:
                    print("\nRecent sessions:")
                    for i, session in enumerate(history):
                        print(f"{i+1}. Session {session['id']} ({session['status']})")
                        print(f"   Started: {session['start_time']}")
                        if session['end_time']:
                            print(f"   Ended: {session['end_time']}")
                        if session['duration']:
                            print(f"   Duration: {session['duration']:.2f} seconds")
                        print(f"   URLs: {session['url_count']}")
                        print(f"   Manufacturers: {session['manufacturer_count']}")
                        print(f"   Categories: {session['category_count']}")
                        print(f"   Errors: {session['error_count']}")
                        print()
                else:
                    print("No session history found")
                
            elif command.startswith("add "):
                url = command[4:].strip()
                if url:
                    self.add_seed_urls([url])
                    print(f"Added seed URL: {url}")
                else:
                    print("No URL provided")
                
            elif command == "exit":
                if self.running:
                    confirm = input("Scraper is still running. Are you sure you want to exit? (y/n): ").strip().lower()
                    if confirm != 'y':
                        continue
                    self.stop()
                print("Exiting interactive mode")
                break
                
            else:
                print("Unknown command")


def main():
    """
    Main entry point for the competitor scraper.
    """
    parser = argparse.ArgumentParser(description='Competitor Scraper')
    parser.add_argument('--config', '-c', type=str, default='config.yaml',
                       help='Path to configuration file')
    parser.add_argument('--interactive', '-i', action='store_true',
                       help='Run in interactive mode')
    parser.add_argument('--seed', '-s', type=str, nargs='+',
                       help='Seed URLs to start crawling')
    parser.add_argument('--max_pages', '-m', type=int,
                       help='Maximum number of pages to process in this run')
    
    args = parser.parse_args()
    
    # Create scraper
    scraper = CompetitorScraper(args.config)
    
    # Run in interactive mode if requested
    if args.interactive:
        scraper.interactive_mode()
    # Otherwise, start with provided seed URLs
    elif args.seed:
        scraper.start(args.seed, args.max_pages)
    # Or just start with config seed URLs
    else:
        scraper.start(max_pages=args.max_pages)


if __name__ == '__main__':
    main()
