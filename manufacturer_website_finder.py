#!/usr/bin/env python3

import os
import sys
import time
import yaml
import logging
import random
import requests
import argparse
import re
from typing import List, Dict, Any, Tuple, Optional
from urllib.parse import urlparse, quote_plus
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text, or_
from sqlalchemy.orm import sessionmaker

# Selenium imports for headless browser
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database.schema import init_db, Manufacturer, ScraperSession
from database.db_manager import get_db_manager
from scraper.claude_analyzer import ClaudeAnalyzer

class ManufacturerWebsiteFinder:
    """Stage 2 of the scraping engine: finds official websites for manufacturers.
    
    This class implements a multi-step process to find and validate manufacturer websites:
    1. Reads manufacturer names from the database
    2. Queries Claude Haiku for their websites with strict delimited format
    3. Falls back to search engines if Claude doesn't know
    4. Validates the website with Claude
    5. Updates the database if valid
    6. Implements anti-bot detection measures
    """
    
    # Search engines to alternate between
    SEARCH_ENGINES = [
        {
            "name": "Google",
            "url": "https://www.google.com/search?q={query}",
            "result_selector": "div.g div.yuRUbf > a"
        },
        {
            "name": "Bing",
            "url": "https://www.bing.com/search?q={query}",
            "result_selector": "#b_results li.b_algo h2 > a"
        },
        {
            "name": "Yandex",
            "url": "https://yandex.com/search/?text={query}",
            "result_selector": ".serp-item a.link"
        },
        {
            "name": "Yahoo",
            "url": "https://search.yahoo.com/search?p={query}",
            "result_selector": "#web ol li .compTitle h3 > a"
        },
        {
            "name": "Ecosia",
            "url": "https://www.ecosia.org/search?q={query}",
            "result_selector": ".result a.js-result-url"
        },
        {
            "name": "DuckDuckGo",
            "url": "https://duckduckgo.com/html/?q={query}",
            "result_selector": ".result__body .result__a"
        },
        {
            "name": "Baidu",
            "url": "https://www.baidu.com/s?wd={query}",
            "result_selector": ".result h3.t > a"
        }
    ]
    
    # User agent list for rotation
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Edg/92.0.902.78",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
    ]
    
    def __init__(self, config_path: str = 'config.yaml'):
        """Initialize the manufacturer website finder.
        
        Args:
            config_path: Path to the configuration file
        """
        self.logger = self._setup_logging()
        self.logger.info("Initializing ManufacturerWebsiteFinder")
        
        # Load configuration
        self.config = self._load_config(config_path)
        self.config_path = config_path
        
        # Initialize database
        self._setup_database()
        
        # Initialize Claude API
        self._setup_claude_api()
        
        # Initialize statistics
        self.stats = {
            "total_manufacturers": 0,
            "manufacturers_with_website": 0,
            "claude_found": 0,
            "search_engine_found": 0,
            "validation_failed": 0,
            "dns_resolution_errors": 0,
            "connection_errors": 0,
            "timeout_errors": 0,
            "search_engine_usage": {engine["name"]: 0 for engine in self.SEARCH_ENGINES}
        }
        
        # Current search engine index for alternating
        self.current_search_engine = 0
        
        # Session for making HTTP requests with anti-bot measures
        self.session = self._create_session()
        
        # Flag for shutdown request
        self.shutdown_requested = False
    
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration.
        
        Returns:
            Logger instance
        """
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, 'manufacturer_website_finder.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        return logging.getLogger('manufacturer_website_finder')
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file.
        
        Args:
            config_path: Path to the configuration file
            
        Returns:
            Configuration dictionary
        """
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                
            # Validate required configuration
            required_fields = [
                'database.path',
                'ai_apis.anthropic.api_key',
                'ai_apis.anthropic.model'
            ]
            
            for field in required_fields:
                parts = field.split('.')
                current = config
                for part in parts:
                    if part not in current:
                        raise ValueError(f"Missing required config field: {field}")
                    current = current[part]
            
            return config
            
        except Exception as e:
            self.logger.error(f"Error loading config: {str(e)}")
            raise
    
    def _setup_database(self):
        """Set up the database connection using DatabaseManager."""
        db_path = self.config['database']['path']
        
        # Create directory if it doesn't exist
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
            
        # Get the database manager singleton
        self.db_manager = get_db_manager(db_path)
        
        # Ensure the database manager is actually initialized
        
    def _cleanup_chrome_processes(self):
        """Attempt to clean up any orphaned Chrome or ChromeDriver processes.
        
        This helps prevent "chrome not reachable" errors by ensuring no zombie
        processes are interfering with new browser sessions.
        """
        try:
            self.logger.info("Cleaning up any orphaned Chrome processes")
            # Try to kill any leftover Chrome processes (platform-specific)
            import subprocess
            import platform
            
            if platform.system() == "Linux":
                subprocess.run(["pkill", "-f", "chromium"], stderr=subprocess.DEVNULL)
                subprocess.run(["pkill", "-f", "chromedriver"], stderr=subprocess.DEVNULL)
            # Could add Windows/Mac handlers here if needed
                
            # Give processes time to fully terminate
            time.sleep(1)
            self.logger.info("Chrome process cleanup completed")
        except Exception as e:
            self.logger.warning(f"Error during Chrome process cleanup: {str(e)}")
        if not hasattr(self.db_manager, '_initialized') or not self.db_manager._initialized:
            self.db_manager.initialize(db_path)
        
        self.logger.info(f"Database manager initialized for {db_path}")
    
    def _setup_claude_api(self):
        """Initialize the Claude API client."""
        # Anthropic setup
        anthropic_config = self.config['ai_apis']['anthropic']
        
        # Get API key from environment variables, fallback to config
        self.anthropic_api_key = os.environ.get('ANTHROPIC_API_KEY')
        if not self.anthropic_api_key:
            self.anthropic_api_key = anthropic_config.get('api_key')
            if not self.anthropic_api_key:
                self.logger.error("No Anthropic API key found in environment or config")
                raise ValueError("Anthropic API key is required. Set ANTHROPIC_API_KEY environment variable or in config.yaml")
        
        self.anthropic_model = anthropic_config['model']
        self.anthropic_sonnet_model = anthropic_config.get('sonnet_model', self.anthropic_model)
        
        # Initialize the Claude analyzer
        self.claude_analyzer = ClaudeAnalyzer(
            api_key=self.anthropic_api_key,
            model=self.anthropic_model,
            sonnet_model=self.anthropic_sonnet_model,
            logger=self.logger
        )
        
        self.logger.info("Anthropic API configuration loaded successfully")
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with anti-bot measures.
        
        Returns:
            Requests session with configured headers and cookies
        """
        session = requests.Session()
        
        # Set a random user agent
        user_agent = random.choice(self.USER_AGENTS)
        
        # Common headers that make the request look like it's coming from a browser
        session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'TE': 'Trailers'
        })
        
        return session
    
    def _rotate_user_agent(self):
        """Rotate the user agent in the session."""
        user_agent = random.choice(self.USER_AGENTS)
        self.session.headers.update({'User-Agent': user_agent})
        self.logger.debug(f"Rotated user agent to: {user_agent}")
        
    def _is_dns_resolution_error(self, error: Exception) -> bool:
        """Check if an error is a DNS resolution error.
        
        Args:
            error: The exception to check
            
        Returns:
            True if it's a DNS resolution error, False otherwise
        """
        error_str = str(error).lower()
        dns_error_indicators = [
            'name or service not known',
            'getaddrinfo failed',
            'nodename nor servname provided',
            'temporary failure in name resolution',
            'name resolution timed out',
            'could not resolve host',
            'no address associated with hostname',
            'errno 11001',  # Windows: No such host is known
            'errno -2',     # Linux: Name or service not known
            'errno -3',     # Linux: Temporary failure in name resolution
            'errno -5',     # Linux: No address associated with hostname
            'gaierror'      # getaddrinfo error
        ]
        
        return any(indicator in error_str for indicator in dns_error_indicators)
    
    def _access_with_selenium(self, url: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """Access a website using Selenium headless browser to bypass 403 Forbidden errors.
        
        Args:
            url: The URL to access
            
        Returns:
            Tuple of (success, html_content, final_url)
        """
        # Try simplified requests approach first
        try:
            self.logger.info(f"Trying direct requests approach for {url}")
            headers = {
                'User-Agent': random.choice(self.USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            response = self.session.get(url, headers=headers, timeout=15, allow_redirects=True)
            if response.status_code == 200:
                self.logger.info(f"Successfully accessed {url} with requests")
                return True, response.text, response.url
        except Exception as req_error:
            self.logger.warning(f"Requests approach failed for {url}: {str(req_error)}")
        
        # Fall back to Selenium if requests fails
        driver = None
        max_retries = 2
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.logger.info(f"Falling back to Selenium for {url} (attempt {retry_count + 1}/{max_retries})")
                
                # Configure Chrome options with minimal arguments for stability
                options = ChromeOptions()
                options.add_argument('--headless=new')  # Use new headless mode
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                # Specify the browser binary location as Chromium
                options.binary_location = "/usr/bin/chromium"
                # Add disable-gpu flag for better headless compatibility
                options.add_argument('--disable-gpu')
                # Add these options to improve stability and prevent zombie processes
                options.add_argument('--disable-extensions')
                options.add_argument('--disable-browser-side-navigation')
                options.add_argument('--disable-infobars')
                options.add_argument('--remote-debugging-port=9222')  # Enable debugging port
                options.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging'])
                options.add_experimental_option('detach', True)  # Detach browser process from driver
                
                # Add random user agent
                user_agent = random.choice(self.USER_AGENTS)
                options.add_argument(f'user-agent={user_agent}')
                
                # Create service with minimal logging to avoid memory issues
                # Ensure log directory exists
                log_dir = os.path.join(os.path.dirname(__file__), 'logs')
                os.makedirs(log_dir, exist_ok=True)
                
                # Set up Chrome service with ChromeDriverManager to get the appropriate driver
                try:
                    driver_path = ChromeDriverManager().install()
                    self.logger.info(f"Using ChromeDriver from: {driver_path}")
                    service = ChromeService(
                        executable_path=driver_path,
                        log_path=os.path.join(log_dir, 'chromedriver.log')
                    )
                except Exception as driver_err:
                    self.logger.warning(f"ChromeDriverManager failed: {str(driver_err)}. Using default ChromeDriver.")
                    service = ChromeService(
                        log_path=os.path.join(log_dir, 'chromedriver.log')
                    )
                
                # Set a timeout for driver creation to prevent hanging
                import signal
                
                def timeout_handler(signum, frame):
                    raise TimeoutException("Chrome driver initialization timed out")
                
                # Set timeout for driver initialization (30 seconds)
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(30)
                
                try:
                    # Initialize Chrome driver with timeout protection
                    self.logger.info("Initializing Chrome driver with timeout protection - using system Chromium")
                    
                    # Clean up any existing ChromeDriver processes to prevent conflicts
                    self._cleanup_chrome_processes()
                    
                    # Create a new driver with service and options
                    driver = webdriver.Chrome(service=service, options=options)
                    
                    # Cancel the timeout alarm once driver is created
                    signal.alarm(0)
                    
                    # Set page load and script timeouts
                    driver.set_page_load_timeout(30)
                    driver.set_script_timeout(30)
                    
                    self.logger.info("Chrome driver initialized successfully")
                    
                    # Execute CDP commands to prevent detection
                    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                        'source': '''
                            Object.defineProperty(navigator, 'webdriver', {
                                get: () => undefined
                            });
                        '''
                    })
                    
                    # Navigate to the URL with timeout protection
                    self.logger.info(f"Navigating to {url}")
                    driver.get(url)
                    
                    # Wait for the page to load (body element to be present)
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    
                    # Get the page source
                    html_content = driver.page_source
                    
                    # Get the final URL (after any redirects)
                    final_url = driver.current_url
                    
                    self.logger.info(f"Successfully accessed {url} with Selenium (final URL: {final_url})")
                    return True, html_content, final_url
                    
                except TimeoutException as timeout_err:
                    self.logger.warning(f"Timeout during Chrome operation: {str(timeout_err)}")
                    # Reset the alarm
                    signal.alarm(0)
                    raise
                    
                except Exception as chrome_err:
                    # Reset the alarm
                    signal.alarm(0)
                    self.logger.error(f"Error during Chrome operation: {str(chrome_err)}")
                    raise
                    
            except TimeoutException:
                self.logger.warning(f"Timeout accessing {url} with Selenium (attempt {retry_count + 1})")
                retry_count += 1
                # Wait before retry
                time.sleep(5)
                
            except WebDriverException as e:
                self.logger.warning(f"Selenium error accessing {url}: {str(e)} (attempt {retry_count + 1})")
                retry_count += 1
                # Wait before retry
                time.sleep(5)
                
            except Exception as e:
                self.logger.error(f"Error setting up Selenium for {url}: {str(e)}")
                # For general exceptions, don't retry
                return False, None, None
                
            finally:
                # Always close the driver
                if driver:
                    try:
                        # First ensure all windows are closed
                        for window_handle in driver.window_handles[:]: 
                            try:
                                driver.switch_to.window(window_handle)
                                driver.close()
                            except Exception:
                                pass
                        
                        # Then quit the driver
                        driver.quit()
                        self.logger.info("Chrome driver successfully closed")
                    except Exception as quit_error:
                        self.logger.warning(f"Error closing Chrome driver: {str(quit_error)}")
                        # Force process cleanup since the normal quit failed
                        self._cleanup_chrome_processes()
        
        # If we've exhausted all retries
        self.logger.error(f"Failed to access {url} with Selenium after {max_retries} attempts")
        
        # Fallback to requests if Selenium fails
        try:
            self.logger.info(f"Falling back to requests for {url}")
            response = self.session.get(url, timeout=15)
            if response.status_code == 200:
                self.logger.info(f"Successfully accessed {url} with requests fallback")
                return True, response.text, response.url
        except Exception as req_error:
            self.logger.warning(f"Requests fallback also failed for {url}: {str(req_error)}")
        
        return False, None, None
    
    def _verify_basic_connectivity(self, website_url: str, try_alternatives: bool = True) -> Tuple[bool, str]:
        """Verify basic connectivity to a website URL.
        
        This method attempts to make a basic connection to the website URL.
        If the initial connection fails, it tries alternative URL patterns
        (with/without www prefix, http vs https).
        
        Args:
            website_url: The URL to verify connectivity to
            try_alternatives: Whether to try alternative URL formats if initial attempt fails
            
        Returns:
            A tuple with (success_status, final_url)
        """
        # Store the last successful HTML content from Selenium if needed
        self._last_selenium_html = None
        
        # List of URLs to try
        urls_to_try = [website_url]
        
        # Add potential alternative URLs to try if the primary one fails
        if try_alternatives:
            # Create alternative versions of the URL (with/without www, http/https)
            domain_parts = website_url.split('/')
            if len(domain_parts) >= 3:
                domain = domain_parts[2]
                protocol = domain_parts[0].rstrip(':')
                
                # Try alternative domain formats (with/without www)
                alt_domains = []
                if domain.startswith('www.'):
                    alt_domains.append(domain[4:])  # Remove www.
                else:
                    alt_domains.append(f'www.{domain}')  # Add www.
                    
                # Try alternative protocol (http/https)
                alt_protocols = []
                if protocol == 'https':
                    alt_protocols.append('http')
                else:
                    alt_protocols.append('https')
                    
                # Add all combinations to our list
                for alt_protocol in alt_protocols:
                    for alt_domain in alt_domains:
                        alt_url = f"{alt_protocol}://{alt_domain}"
                        path = '/'.join(domain_parts[3:])
                        if path:
                            alt_url += f"/"
                        urls_to_try.append(alt_url)
            
        # Try each URL in sequence
        for url in urls_to_try:
            try:
                # Rotate user agent
                self._rotate_user_agent()
                
                # Set headers to mimic a real browser
                self.session.headers.update({
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1'
                })
                
                # Attempt to connect with a HEAD request first (lighter)
                # with a short timeout to quickly check accessibility
                self.logger.info(f"Checking basic connectivity to: {url}")
                response = self.session.head(url, timeout=10, allow_redirects=True)
                
                # If HEAD worked, try GET to confirm full content access
                if response.status_code < 400:
                    response = self.session.get(url, timeout=15, allow_redirects=True)
                    
                    if response.status_code == 200:
                        self.logger.info(f"Successfully connected to: {url}")
                        return True, url
                    elif response.status_code == 403:  # Forbidden - try with Selenium
                        self.logger.warning(f"Got 403 Forbidden for {url}, trying with Selenium")
                        selenium_success, html_content, final_url = self._access_with_selenium(url)
                        if selenium_success and html_content:
                            self.logger.info(f"Successfully accessed {url} with Selenium")
                            # Store the HTML content for later use
                            self._last_selenium_html = html_content
                            return True, final_url or url
                    else:
                        self.logger.warning(f"GET request failed with status code: {response.status_code} for {url}")
                elif response.status_code == 403:  # Forbidden - try with Selenium
                    self.logger.warning(f"HEAD request returned 403 Forbidden for {url}, trying with Selenium")
                    selenium_success, html_content, final_url = self._access_with_selenium(url)
                    if selenium_success and html_content:
                        self.logger.info(f"Successfully accessed {url} with Selenium")
                        # Store the HTML content for later use
                        self._last_selenium_html = html_content
                        return True, final_url or url
                else:
                    self.logger.warning(f"HEAD request failed with status code: {response.status_code} for {url}")
                    
            except requests.exceptions.ConnectionError as conn_err:
                if self._is_dns_resolution_error(conn_err):
                    self.logger.warning(f"DNS resolution error for {url}: {str(conn_err)}")
                    self.stats["dns_resolution_errors"] += 1
                else:
                    self.logger.warning(f"Connection error for {url}: {str(conn_err)}")
                    self.stats["connection_errors"] += 1
                    
            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout error connecting to {url}")
                self.stats["timeout_errors"] += 1
                
            except Exception as e:
                self.logger.warning(f"Error connecting to {url}: {str(e)}")
                
            # Small delay before trying next URL
            time.sleep(random.uniform(1.0, 2.0))
                
        # If we reach here, all connection attempts failed
        self.logger.error(f"All connection attempts failed for {website_url} and its alternatives")
        return False, website_url
    
    def _get_manufacturers_without_website(self) -> List[Manufacturer]:
        """Get manufacturers from the database that don't have a validated website.
        
        Returns:
            List of Manufacturer objects without validated websites, sorted alphabetically
        """
        try:
            with self.db_manager.session() as session:
                # Get manufacturers that either have no website or have a website but it's not validated
                manufacturers = session.query(Manufacturer).filter(
                    (Manufacturer.website == None) | 
                    (Manufacturer.website == '') |
                    (Manufacturer.website_validated == False)
                ).order_by(Manufacturer.name).all()  # Sort alphabetically
                
                # Update statistics
                self.stats["total_manufacturers"] = session.query(Manufacturer).count()
                self.stats["manufacturers_with_website"] = session.query(Manufacturer).filter(
                    Manufacturer.website != None,
                    Manufacturer.website != '',
                    Manufacturer.website_validated == True
                ).count()
                
                self.logger.info(f"Found {len(manufacturers)} manufacturers without validated websites")
                return manufacturers
                
        except Exception as e:
            self.logger.error(f"Error getting manufacturers without validated websites: {str(e)}")
            return []
    
    def _get_manufacturers_with_unvalidated_website(self) -> List[Manufacturer]:
        """Get manufacturers from the database that have an existing website but it's not validated.
        
        Returns:
            List of Manufacturer objects with unvalidated websites, sorted alphabetically
        """
        try:
            with self.db_manager.session() as session:
                # Get manufacturers that have a website but it's not validated
                manufacturers = session.query(Manufacturer).filter(
                    Manufacturer.website != None,
                    Manufacturer.website != '',
                    (Manufacturer.website_validated == False) | (Manufacturer.website_validated == None)
                ).order_by(Manufacturer.name).all()  # Sort alphabetically
                
                self.logger.info(f"Found {len(manufacturers)} manufacturers with unvalidated websites")
                return manufacturers
                
        except Exception as e:
            self.logger.error(f"Error getting manufacturers with unvalidated websites: {str(e)}")
            return []
    
    def _validate_with_claude_haiku(self, manufacturer_name: str, website_url: str, title: str, 
                                    content: str, manual_links_text: str) -> bool:
        """Validate a website using Claude Haiku API following strict delimited format.
        
        Args:
            manufacturer_name: The name of the manufacturer
            website_url: The URL of the website
            title: The title of the webpage
            content: The HTML content of the webpage
            manual_links_text: Text describing manual links found on the page
            
        Returns:
            True if the website is valid, False otherwise
        """
        self.logger.info(f"Validating with Claude API: {website_url} for {manufacturer_name}")
        
        try:
            # Log API key status to help debug authentication issues
            api_key_status = "available" if self.anthropic_api_key else "missing"
            self.logger.info(f"Claude API key status: {api_key_status}")
            
            if not self.anthropic_api_key:
                self.logger.error("Missing Claude API key. Check environment variables or config.")
                return False
                
            # Prepare the API request with proper authentication
            headers = {
                "x-api-key": self.anthropic_api_key,  # Use the proper API key set in _setup_claude_api
                "Content-Type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            # Prepare the prompt for Claude following strict delimited format requirements
            prompt = f"""URL: {website_url}
Title: {title}

Content excerpt: {content[:3000]}  # Limit content to avoid token limits

Potential manual links found:
{manual_links_text}

I need you to analyze if this is the official website for the manufacturer or brand named '{manufacturer_name}' and if it appears to have product manuals or user guides available.

You MUST respond using EXACTLY this format without ANY additional text or explanations:

# RESPONSE_START
IS_MANUFACTURER: yes  # or 'no' if not the official manufacturer website
HAS_MANUALS: yes  # or 'no' if no evidence of manuals
# RESPONSE_END
"""
            
            payload = {
                "model": "claude-3-haiku-20240307",
                "max_tokens": 1000,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
            
            # Make the API request with retries
            max_api_retries = 3
            api_retry_delay = 2  # seconds
            
            for api_attempt in range(max_api_retries):
                try:
                    self.logger.info(f"Making Claude API request (attempt {api_attempt+1}/{max_api_retries})")
                    api_response = requests.post(
                        "https://api.anthropic.com/v1/messages",
                        headers=headers,
                        json=payload,
                        timeout=20  # Increased timeout for API calls
                    )
                    
                    # Log response status
                    self.logger.info(f"Claude API response status: {api_response.status_code}")
                    
                    # Break if successful
                    if api_response.status_code == 200:
                        self.logger.info("Claude API request successful")
                        break
                        
                    # Handle rate limiting
                    if api_response.status_code == 429:
                        wait_time = api_retry_delay * (2 ** api_attempt)  # Exponential backoff
                        self.logger.warning(f"Rate limited by Claude API. Waiting {wait_time} seconds before retry.")
                        time.sleep(wait_time)
                    else:
                        self.logger.error(f"Error from Claude API: {api_response.status_code} - {api_response.text}")
                        if api_attempt < max_api_retries - 1:
                            time.sleep(api_retry_delay * (api_attempt + 1))
                
                except Exception as api_e:
                    self.logger.error(f"Exception calling Claude API: {str(api_e)}")
                    if api_attempt < max_api_retries - 1:
                        time.sleep(api_retry_delay * (api_attempt + 1))
            
            # Check if all API attempts failed
            if 'api_response' not in locals() or api_response.status_code != 200:
                self.logger.error(f"All Claude API attempts failed for {website_url}")
                return False
                
            # Process successful API response
            try:
                response_data = api_response.json()
                result = response_data["content"][0]["text"]
                
                # Log the complete response for debugging
                self.logger.debug(f"Complete Claude validation response: {result}")
                
                # Parse the delimited response
                import re
                
                # Extract IS_MANUFACTURER and HAS_MANUALS values
                is_manufacturer = False
                has_manuals = False
                
                # Check for IS_MANUFACTURER following strict delimited format
                manufacturer_pattern = r'IS_MANUFACTURER:\s*(yes|no|true|false)'
                manufacturer_match = re.search(manufacturer_pattern, result, re.IGNORECASE)
                if manufacturer_match:
                    is_manufacturer = manufacturer_match.group(1).lower() in ['yes', 'true']
                
                # Check for HAS_MANUALS
                manuals_pattern = r'HAS_MANUALS:\s*(yes|no|true|false)'
                manuals_match = re.search(manuals_pattern, result, re.IGNORECASE)
                if manuals_match:
                    has_manuals = manuals_match.group(1).lower() in ['yes', 'true']
                
                # Log the validation results
                self.logger.info(f"Website validation for {manufacturer_name} ({website_url}):\n" +
                               f"  Is manufacturer: {is_manufacturer}\n" +
                               f"  Has manuals: {has_manuals}")
                
                # Only consider valid if both conditions are met
                if is_manufacturer and has_manuals:
                    self.logger.info(f"Validated {website_url} as official website with manuals for {manufacturer_name}")
                    return True
                else:
                    reason = []
                    if not is_manufacturer:
                        reason.append("not the official website")
                    if not has_manuals:
                        reason.append("no evidence of manuals")
                    
                    self.logger.info(f"Validation failed: {website_url} - {', '.join(reason)}")
                    return False
                    
            except Exception as parse_e:
                self.logger.error(f"Error parsing Claude API response: {str(parse_e)}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error validating website with Claude: {str(e)}")
            return False
    
    def _validate_website_with_selenium(self, manufacturer_name: str, website_url: str) -> bool:
        """Validate a website using Selenium and Claude Haiku.
        
        Implements a metadata-first approach:
        1. First extracts and validates page metadata (title, meta tags, favicon)
        2. Only loads full page content if metadata validation is inconclusive
        
        Args:
            manufacturer_name: The name of the manufacturer
            website_url: The URL to validate
            
        Returns:
            True if the website is valid, False otherwise
        """
        self.logger.info(f"Validating website with Selenium: {website_url} for {manufacturer_name}")
        
        try:
            self.logger.info(f"DEBUGGING CHROMEDRIVER: Starting setup for URL {website_url}")
            # Set up Chrome options with advanced stability settings
            chrome_options = ChromeOptions()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            
            # Add stability improvements
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-infobars')
            chrome_options.add_argument('--disable-notifications')
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--disable-popup-blocking')
            chrome_options.add_argument('--remote-debugging-port=9222')  # Enable remote debugging
            chrome_options.add_argument('--disable-crash-reporter')  # Disable crash reporting
            chrome_options.add_argument('--disable-logging')  # Reduce logging noise
            chrome_options.add_argument('--log-level=3')  # Only fatal errors
            
            # Add page load strategy to wait only for critical resources
            chrome_options.page_load_strategy = 'eager'  # 'eager' waits for interactive instead of complete load
            
            self.logger.info("DEBUGGING CHROMEDRIVER: Enhanced Chrome options set up")
            
            # Add random user agent
            user_agent = random.choice(self.USER_AGENTS) if hasattr(self, 'USER_AGENTS') and self.USER_AGENTS else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            chrome_options.add_argument(f"user-agent={user_agent}")
            
            # Disable automation flags to avoid detection
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Create and configure the webdriver with better version handling
            try:
                # First try with custom ChromeDriver if available
                chromedriver_path = os.path.join(os.path.dirname(__file__), 'chromedriver')
                if os.path.exists(chromedriver_path):
                    self.logger.info(f"Using custom ChromeDriver at: {chromedriver_path}")
                    service = ChromeService(executable_path=chromedriver_path)
                    driver = webdriver.Chrome(service=service, options=chrome_options)
                else:
                    # Fall back to system ChromeDriver
                    self.logger.info("Using system ChromeDriver")
                    service = ChromeService()
                    driver = webdriver.Chrome(service=service, options=chrome_options)
            except Exception as driver_error:
                self.logger.warning(f"ChromeDriver initialization failed: {str(driver_error)}")
                raise
            
            try:
                # Set page load timeout
                driver.set_page_load_timeout(30)
                
                # Execute CDP commands to prevent detection
                driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                    'source': '''
                        Object.defineProperty(navigator, 'webdriver', {
                            get: () => undefined
                        });
                    '''
                })
                
                # Add detailed logs to diagnose freezing
                self.logger.info(f"Attempting to navigate to URL: {website_url}")
                try:
                    # Set a shorter timeout for navigation to prevent long freezes
                    driver.set_page_load_timeout(20)
                    driver.get(website_url)
                    self.logger.info(f"Successfully navigated to URL: {website_url}")
                except Exception as nav_error:
                    self.logger.error(f"Failed to navigate to URL: {website_url}. Error: {str(nav_error)}")
                    return False
                
                # Wait for the page to load with detailed logging
                self.logger.info("Waiting for body element to be present")
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, "body"))
                    )
                    self.logger.info("Body element found on page")
                except Exception as wait_error:
                    self.logger.error(f"Timeout waiting for body element: {str(wait_error)}")
                    return False
                
                # Get page title and current URL with logging
                try:
                    title = driver.title
                    self.logger.info(f"Page title: {title}")
                    
                    final_url = driver.current_url
                    self.logger.info(f"Final URL after redirects: {final_url}")
                    
                    # Get the page source with logging
                    self.logger.info("Retrieving page source")
                    content = driver.page_source
                    content_length = len(content) if content else 0
                    self.logger.info(f"Retrieved page source, length: {content_length} chars")
                except Exception as page_error:
                    self.logger.error(f"Error retrieving page data: {str(page_error)}")
                    return False
                
                # Look for potential manual links
                manual_links = []
                try:
                    # Find links containing keywords related to manuals
                    manual_keywords = ['manual', 'guide', 'instruction', 'support', 'download', 'pdf', 'documentation']
                    
                    for link in driver.find_elements(By.TAG_NAME, 'a'):
                        link_text = link.text.lower() if link.text else ''
                        link_href = link.get_attribute('href') or ''
                        
                        # Check if any manual keyword is in the link text or href
                        if any(keyword in link_text or keyword in link_href.lower() for keyword in manual_keywords):
                            manual_links.append({
                                'text': link.text or link_href,
                                'href': link_href
                            })
                except Exception as e:
                    self.logger.warning(f"Error finding manual links: {str(e)}")
                
                # Format manual links for the prompt
                manual_links_text = ""
                for link in manual_links[:10]:  # Limit to 10 links to avoid token limits
                    manual_links_text += f"- {link['text']}: {link['href']}\n"
                
                if not manual_links_text:
                    manual_links_text = "No potential manual links found."
                
                # Log redirection if it occurred
                if final_url != website_url:
                    self.logger.info(f"URL redirected from {website_url} to {final_url}")
                
                # Close the driver before validation to free resources
                driver.quit()
                driver = None
                
                # Validate with Claude Haiku
                is_valid = self._validate_with_claude_haiku(manufacturer_name, final_url, title, content, manual_links_text)
                
                return is_valid
                
            except TimeoutException:
                self.logger.warning(f"Timeout accessing {website_url} with Selenium")
                return False
                
            except WebDriverException as e:
                self.logger.warning(f"Selenium WebDriverException for {website_url}: {str(e)}")
                return False
                
            except Exception as e:
                self.logger.error(f"Error during Selenium browsing: {str(e)}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error setting up Selenium for {website_url}: {str(e)}")
            return False
        finally:
            # Always close the driver if it exists
            if 'driver' in locals() and driver is not None:
                try:
                    driver.quit()
                except:
                    pass
    
    def _update_manufacturer_website(self, manufacturer_id: int, website: str, validated: bool = False) -> bool:
        """Update the manufacturer's website and validation status in the database.
        
        Args:
            manufacturer_id: ID of the manufacturer to update
            website: Website URL to set
            validated: Whether the website has been validated as official with manuals
                      Default is False - only set to True when explicitly validated
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.db_manager.session() as session:
                manufacturer = session.query(Manufacturer).filter_by(id=manufacturer_id).first()
                if manufacturer:
                    manufacturer.website = website
                    manufacturer.website_validated = validated
                    self.logger.info(f"Updated website for manufacturer {manufacturer.name}: {website} (validated: {validated})")
                    return True
                else:
                    self.logger.warning(f"Manufacturer with ID {manufacturer_id} not found")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error updating manufacturer website: {str(e)}")
            return False
    
    def _search_and_validate_top_results(self, manufacturer_name: str, max_results: int = 3) -> Optional[str]:
        """Search Google for manufacturer websites and validate the top results using Selenium.
        
        Args:
            manufacturer_name: Name of the manufacturer
            max_results: Maximum number of search results to validate
            
        Returns:
            Validated website URL if found, None otherwise
        """
        self.logger.info(f"Searching and validating top {max_results} results for {manufacturer_name}")
        
        try:
            # Construct search queries in order of specificity
            search_queries = [
                f"{manufacturer_name} official website manuals",
                f"{manufacturer_name} official product support manuals",
                f"{manufacturer_name} official website"
            ]
            
            # Define known aggregator sites to skip
            aggregator_sites = [
                'manualslib.com', 'manualsonline.com', 'manualpdf.com', 'manualowl.com',
                'manualsworld.com', 'retrevo.com', 'manuals.support', 'manualscat.com',
                'manualsdir.com', 'manualguru.com', 'manualsbrain.com', 'amazon.com',
                'ebay.com', 'alibaba.com', 'walmart.com', 'youtube.com', 'facebook.com',
                'twitter.com', 'instagram.com', 'linkedin.com', 'pinterest.com',
                'reddit.com', 'quora.com', 'wikipedia.org'
            ]
            
            # Use a single function to create drivers to avoid duplication
            def create_chrome_driver():
                # Setup Chrome with enhanced stability options
                chrome_options = ChromeOptions()
                chrome_options.add_argument('--headless')
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--disable-gpu')
                
                # Add stability improvements
                chrome_options.add_argument('--disable-extensions')
                chrome_options.add_argument('--disable-infobars')
                chrome_options.add_argument('--disable-notifications')
                chrome_options.add_argument('--ignore-certificate-errors')
                chrome_options.add_argument('--disable-popup-blocking')
                chrome_options.add_argument('--disable-crash-reporter')
                chrome_options.add_argument('--disable-logging')
                chrome_options.add_argument('--log-level=3')  # Only fatal errors
                chrome_options.page_load_strategy = 'eager'  # 'eager' is less prone to hanging
                
                # Add random user agent
                user_agent = random.choice(self.USER_AGENTS) if hasattr(self, 'USER_AGENTS') and self.USER_AGENTS else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
                chrome_options.add_argument(f"user-agent={user_agent}")
                
                # Disable automation flags
                chrome_options.add_argument('--disable-blink-features=AutomationControlled')
                chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
                chrome_options.add_experimental_option('useAutomationExtension', False)
                
                try:
                    # Set path to Chromium explicitly
                    chrome_path = "/usr/bin/chromium"
                    if os.path.exists(chrome_path):
                        chrome_options.binary_location = chrome_path
                        self.logger.info(f"Using Chromium browser at: {chrome_path}")
                    
                    # First try with custom ChromeDriver if available
                    chromedriver_path = os.path.join(os.path.dirname(__file__), 'chromedriver')
                    if os.path.exists(chromedriver_path):
                        self.logger.info(f"Using custom ChromeDriver at: {chromedriver_path}")
                        service = ChromeService(
                            executable_path=chromedriver_path,
                            log_path=os.path.join(os.path.dirname(__file__), 'logs', 'chromedriver.log'),
                            service_args=['--verbose']
                        )
                    else:
                        # Fall back to system ChromeDriver
                        self.logger.info("Using system ChromeDriver")
                        service = ChromeService(
                            log_path=os.path.join(os.path.dirname(__file__), 'logs', 'chromedriver.log'),
                            service_args=['--verbose']
                        )
                    
                    # Create driver with timeouts
                    driver = webdriver.Chrome(service=service, options=chrome_options)
                    driver.set_page_load_timeout(30)
                    driver.set_script_timeout(30)
                    return driver
                    
                except Exception as driver_error:
                    self.logger.warning(f"ChromeDriver initialization failed: {str(driver_error)}")
                    return None
            
            # Create initial driver
            driver = create_chrome_driver()
            if not driver:
                self.logger.error("Could not initialize ChromeDriver, skipping search")
                return None
                
            # Track URLs we've already checked
            urls_checked = set()
            
            try:
                # Try each search query in sequence until we find valid results
                for query in search_queries:
                    if len(urls_checked) >= max_results:
                        break  # Exit if we've already checked enough URLs
                    
                    self.logger.info(f"Searching for: {query}")
                    
                    try:
                        # Navigate to Google and perform the search
                        driver.get("https://www.google.com/search?q=" + query.replace(' ', '+'))
                        
                        # Wait for search results to load (using a shorter timeout)
                        try:
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div.g, div[data-hveid]"))
                            )
                        except Exception as wait_error:
                            self.logger.warning(f"Timeout waiting for search results: {str(wait_error)}")
                            # Try to recover by getting a new driver
                            driver.quit()
                            driver = create_chrome_driver()
                            if not driver:
                                break
                            continue
                        
                        # Extract search result links safely
                        result_links = []
                        try:
                            # Try different selectors to handle Google's changing layout
                            selectors = ["div.g a", "div[data-hveid] a[href]", "#search a[href]"]
                            
                            for selector in selectors:
                                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                                if elements:
                                    break
                            
                            for link_element in elements:
                                try:
                                    url = link_element.get_attribute("href") or ""
                                    
                                    # Basic URL validation
                                    if not url or not url.startswith('http'):
                                        continue
                                        
                                    # Skip aggregator and irrelevant sites
                                    if any(agg in url.lower() for agg in aggregator_sites):
                                        continue
                                        
                                    # Skip news articles, PDFs, and other non-useful links
                                    if any(ext in url.lower() for ext in ['.pdf', '/news/', '/article/', 'blog.']):
                                        continue
                                        
                                    # Add this URL if we haven't already checked it
                                    if url not in urls_checked:
                                        result_links.append(url)
                                except Exception as link_error:
                                    # Continue to next link on error
                                    continue
                        except Exception as elements_error:
                            self.logger.warning(f"Error extracting search results: {str(elements_error)}")
                        
                        # Safely close the search driver after getting results
                        driver.quit()
                        driver = None
                        
                        # Process each result with a fresh driver for each validation
                        for url in result_links:
                            if len(urls_checked) >= max_results:
                                break
                                
                            urls_checked.add(url)
                            self.logger.info(f"Validating: {url}")
                            
                            # Validate with isolated driver instances to prevent crashes from affecting other validations
                            try:
                                is_valid = self._validate_website_with_selenium(manufacturer_name, url)
                                
                                if is_valid:
                                    self.logger.info(f"Found valid website for {manufacturer_name}: {url}")
                                    return url
                            except Exception as validation_error:
                                self.logger.error(f"Error validating {url}: {str(validation_error)}")
                                # Continue to next URL on error
                        
                        # Create a new driver for the next search query
                        driver = create_chrome_driver()
                        if not driver:
                            self.logger.error("Could not initialize ChromeDriver for next search query")
                            break
                            
                    except Exception as search_error:
                        self.logger.error(f"Error during search for {query}: {str(search_error)}")
                        # Try to recover by creating a new driver for the next query
                        if driver:
                            driver.quit()
                        driver = create_chrome_driver()
                        if not driver:
                            break
                
                # If we've checked all results and none are valid, return None
                self.logger.info(f"No valid website found for {manufacturer_name} after checking {len(urls_checked)} search results")
                return None
                
            except Exception as e:
                self.logger.error(f"Error during search and validation: {str(e)}")
                return None
            finally:
                # Always close the driver
                try:
                    driver.quit()
                except:
                    pass
                    
        except Exception as e:
            self.logger.error(f"Error setting up search for {manufacturer_name}: {str(e)}")
            return None
            
    def _query_claude_for_website(self, manufacturer_name: str) -> Optional[str]:
        """Query Claude Haiku for a manufacturer's website.
        
        Args:
            manufacturer_name: Name of the manufacturer
            
        Returns:
            Website URL if found, None otherwise
        """
        try:
            # Prepare the prompt for Claude
            prompt = f"""Find the official website URL for the manufacturer or brand named '{manufacturer_name}'.

If you know the official website, respond in this exact format:

# RESPONSE_START
WEBSITE: https://example.com
# RESPONSE_END

If you don't know or aren't sure, respond in this exact format:

# RESPONSE_START
WEBSITE: unknown
# RESPONSE_END

Do not include any explanations or additional text outside the delimited format.
"""
            
            # Use requests to make a direct API call
            headers = {
                "x-api-key": self.anthropic_api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            payload = {
                "model": self.anthropic_model,
                "max_tokens": 100,
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            # Make API call with retry logic for DNS and other errors
            max_api_retries = 3
            api_retry_delay = 2  # seconds
            
            for api_attempt in range(max_api_retries):
                try:
                    self.logger.info(f"Making Claude API request (attempt {api_attempt+1}/{max_api_retries})")
                    api_response = requests.post(
                        "https://api.anthropic.com/v1/messages",
                        headers=headers,
                        json=payload,
                        timeout=20  # Increased timeout for API calls
                    )
                    
                    # Break if successful
                    if api_response.status_code == 200:
                        break
                        
                    # Handle rate limiting
                    if api_response.status_code == 429:
                        wait_time = api_retry_delay * (2 ** api_attempt)  # Exponential backoff
                        self.logger.warning(f"Rate limited by Claude API. Waiting {wait_time} seconds before retry.")
                        time.sleep(wait_time)
                    else:
                        self.logger.error(f"Error from Claude API: {api_response.status_code} - {api_response.text}")
                        if api_attempt < max_api_retries - 1:
                            time.sleep(api_retry_delay * (api_attempt + 1))
                            
                except requests.exceptions.ConnectionError as conn_err:
                    # Check if it's a DNS resolution error
                    if self._is_dns_resolution_error(conn_err):
                        self.logger.warning(f"DNS resolution error when calling Claude API: {str(conn_err)}")
                        self.stats["dns_resolution_errors"] += 1
                    else:
                        self.logger.warning(f"Connection error when calling Claude API: {str(conn_err)}")
                        self.stats["connection_errors"] += 1
                        
                    # Use exponential backoff for retries
                    if api_attempt < max_api_retries - 1:
                        wait_time = api_retry_delay * (2 ** api_attempt)
                        self.logger.info(f"Waiting {wait_time} seconds before retry {api_attempt+1}/{max_api_retries}")
                        time.sleep(wait_time)
                        
                except requests.exceptions.Timeout as timeout_err:
                    self.logger.warning(f"Timeout error when calling Claude API: {str(timeout_err)}")
                    self.stats["timeout_errors"] += 1
                    
                    if api_attempt < max_api_retries - 1:
                        wait_time = api_retry_delay * (api_attempt + 1)
                        self.logger.info(f"Waiting {wait_time} seconds before retry {api_attempt+1}/{max_api_retries}")
                        time.sleep(wait_time)
            
            # Check if all API attempts failed
            if 'api_response' not in locals() or api_response.status_code != 200:
                self.logger.error(f"All Claude API attempts failed for {manufacturer_name}")
                return None
                
            # Handle the successful response
            try:
                response_data = api_response.json()
                result = response_data["content"][0]["text"]
                
                # Parse the delimited response
                import re
                response_pattern = r'# RESPONSE_START\s+WEBSITE:\s*([^\s#]+)\s*# RESPONSE_END'
                match = re.search(response_pattern, result, re.DOTALL)
                
                if match:
                    website = match.group(1).strip()
                    if website.lower() != 'unknown':
                        self.logger.info(f"Claude found website for {manufacturer_name}: {website}")
                        self.stats["claude_found"] += 1
                        return website
                    else:
                        self.logger.info(f"Claude doesn't know website for {manufacturer_name}")
                        return None
                else:
                    self.logger.warning(f"Failed to parse Claude response for {manufacturer_name}: {result}")
                    return None
            except Exception as parse_err:
                self.logger.error(f"Error parsing Claude API response: {str(parse_err)}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error querying Claude for website: {str(e)}")
            return None
    
    def _search_for_website(self, manufacturer_name: str) -> Optional[str]:
        """Search for a manufacturer's website using search engines.
        
        Args:
            manufacturer_name: Name of the manufacturer
            
        Returns:
            Website URL if found, None otherwise
        """
        try:
            # Try multiple search queries in order of specificity
            search_queries = [
                f"{manufacturer_name} official support website manuals",
                f"{manufacturer_name} official product manuals support",
                f"{manufacturer_name} official website user manuals",
                f"{manufacturer_name} official website",
                f"{manufacturer_name} technical support",
                f"{manufacturer_name} product documentation"
            ]
            
            # List of known manual aggregator sites to skip
            aggregator_sites = [
                'manualslib.com', 'manualsonline.com', 'manualowl.com', 'manualsworld.com',
                'retrevo.com', 'manualsbase.com', 'manualsdir.com', 'manualshelf.com',
                'manualsnet.com', 'manualscat.com', 'manualsdir.de', 'manual-directory.com',
                'manualsbrain.com', 'manuall.co.uk', 'manuall.nl', 'manuall.de',
                'amazon.com/manuals', 'ebay.com', 'alibaba.com', 'aliexpress.com'
            ]
            
            # Track already tried URLs to avoid duplicates
            tried_urls = set()
            
            for query in search_queries:
                encoded_query = quote_plus(query)
                
                # Try multiple search engines before giving up
                for attempt in range(min(3, len(self.SEARCH_ENGINES))):
                    try:
                        # Get the current search engine and rotate to the next one
                        search_engine = self.SEARCH_ENGINES[self.current_search_engine]
                        self.current_search_engine = (self.current_search_engine + 1) % len(self.SEARCH_ENGINES)
                        
                        # Update statistics
                        self.stats["search_engine_usage"][search_engine["name"]] += 1
                        
                        # Format the search URL
                        search_url = search_engine["url"].format(query=encoded_query)
                        
                        # Rotate user agent
                        self._rotate_user_agent()
                        
                        # Set additional headers to avoid detection
                        self.session.headers.update({
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                            'Accept-Language': 'en-US,en;q=0.5',
                            'Referer': 'https://www.google.com/',
                            'DNT': '1',
                            'Connection': 'keep-alive',
                            'Upgrade-Insecure-Requests': '1',
                            'Cache-Control': 'max-age=0'
                        })
                        
                        # Add random delay to avoid detection
                        time.sleep(random.uniform(2.0, 5.0))
                        
                        # Make the request with retry logic
                        max_retries = 2
                        retry_delay = 2  # seconds
                        
                        for retry in range(max_retries + 1):
                            try:
                                self.logger.info(f"Searching for '{query}' using {search_engine['name']} (attempt {retry+1})")
                                response = self.session.get(search_url, timeout=15, allow_redirects=True)
                                
                                if response.status_code == 200:
                                    break
                                    
                                if response.status_code == 403:  # Forbidden
                                    self.logger.warning(f"Search engine access forbidden: {search_engine['name']}")
                                    # Try a different search engine on next iteration
                                    break
                                    
                                self.logger.warning(f"Search engine request failed with status {response.status_code}, retrying...")
                                time.sleep(retry_delay * (retry + 1))
                                
                            except requests.exceptions.ConnectionError as conn_err:
                                # Check specifically for DNS resolution errors
                                if 'Name or service not known' in str(conn_err) or "getaddrinfo failed" in str(conn_err) or "nodename nor servname provided" in str(conn_err):
                                    self.logger.warning(f"DNS resolution error for search engine {search_engine['name']}: {str(conn_err)}")
                                    # Track DNS resolution errors
                                    self.stats["dns_resolution_errors"] = self.stats.get("dns_resolution_errors", 0) + 1
                                    # Use a longer wait for DNS issues
                                    wait_time = retry_delay * (2 ** retry)  # Exponential backoff
                                    if retry < max_retries:
                                        self.logger.info(f"Waiting {wait_time} seconds before retry {retry+1}/{max_retries}")
                                        time.sleep(wait_time)
                                else:
                                    self.logger.warning(f"Connection error for search engine {search_engine['name']}: {str(conn_err)}")
                                    if retry < max_retries:
                                        time.sleep(retry_delay * (retry + 1))
                            except requests.exceptions.Timeout as timeout_err:
                                self.logger.warning(f"Timeout error for search engine {search_engine['name']}: {str(timeout_err)}")
                                if retry < max_retries:
                                    time.sleep(retry_delay * (retry + 1))
                                    
                        # If all retries failed, continue to next search engine
                        if 'response' not in locals() or response.status_code != 200:
                            continue
                        
                        # Parse the HTML
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Extract all results (not just the first one)
                        result_links = soup.select(search_engine["result_selector"])
                        
                        if not result_links:
                            self.logger.warning(f"No results found for '{query}' using {search_engine['name']}")
                            continue
                        
                        # Try each of the top 5 results (increased from 3)
                        for idx, link in enumerate(result_links[:5]):
                            url = link.get('href')
                            if not url:
                                continue
                                
                            # Clean the URL (remove tracking parameters)
                            try:
                                parsed_url = urlparse(url)
                                clean_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
                                
                                # Normalize URL
                                if not clean_url.startswith('http'):
                                    if clean_url.startswith('//'):
                                        clean_url = 'https:' + clean_url
                                    else:
                                        clean_url = 'https://' + clean_url
                                        
                                # Remove trailing slash for consistency
                                clean_url = clean_url.rstrip('/')
                                
                                # Skip if we've already tried this URL
                                if clean_url in tried_urls:
                                    continue
                                    
                                tried_urls.add(clean_url)
                                
                                # Skip manualslib and similar aggregator sites
                                if any(domain in clean_url.lower() for domain in aggregator_sites):
                                    self.logger.info(f"Skipping manual aggregator site: {clean_url}")
                                    continue
                                
                                # Skip social media sites
                                if any(domain in clean_url.lower() for domain in ['facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com', 'youtube.com']):
                                    self.logger.info(f"Skipping social media site: {clean_url}")
                                    continue
                                
                                # Check if the URL contains the manufacturer name to increase relevance
                                # Convert to lowercase and remove spaces for comparison
                                manufacturer_normalized = manufacturer_name.lower().replace(' ', '')
                                domain_normalized = parsed_url.netloc.lower().replace(' ', '')
                                
                                # Prioritize URLs that contain the manufacturer name in the domain
                                if manufacturer_normalized in domain_normalized:
                                    self.logger.info(f"Found likely official website for {manufacturer_name}: {clean_url} (result #{idx+1})")
                                    self.stats["search_engine_found"] += 1
                                    return clean_url
                                    
                                # Otherwise, add to potential results
                                self.logger.info(f"Found potential website for {manufacturer_name}: {clean_url} (result #{idx+1})")
                                self.stats["search_engine_found"] += 1
                                return clean_url
                                
                            except Exception as url_e:
                                self.logger.warning(f"Error processing URL {url}: {str(url_e)}")
                                continue
                                
                    except Exception as engine_e:
                        self.logger.warning(f"Error with search engine {search_engine['name']}: {str(engine_e)}")
                        continue
            
            # If we get here, we've tried all queries and engines without success
            self.logger.warning(f"Could not find website for {manufacturer_name} after trying all search queries")
            return None
            
        except Exception as e:
            self.logger.error(f"Error searching for website: {str(e)}")
            return None
    
    def _validate_website(self, manufacturer_name: str, website_url: str) -> bool:
        """Validate if a website is actually the manufacturer's official website and contains manuals.
        
        Args:
            manufacturer_name: Name of the manufacturer
            website_url: Website URL to validate
            
        Returns:
            True if valid, False otherwise
        """
        self.logger.info(f"Starting validation for {manufacturer_name} website: {website_url}")
        
        # Normalize the URL
        if not website_url.startswith('http'):
            website_url = 'https://' + website_url
            
        # Remove trailing slashes for consistency
        website_url = website_url.rstrip('/')
        
        # STEP 1: Basic connectivity check - verify the website is accessible
        # This will try several variants of the URL if needed (with/without www, http/https)
        connectivity_success, final_url = self._verify_basic_connectivity(website_url)
        
        if not connectivity_success:
            self.logger.error(f"Basic connectivity check failed for {website_url}")
            return False
            
        # Update the URL to the one that worked (might be different from original if alternatives were tried)
        website_url = final_url
        self.logger.info(f"Basic connectivity check passed for {website_url}")
        
        # STEP 2: Content analysis with additional retry logic
        max_retries = 2  # Fewer retries needed since we already verified basic connectivity
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                # Rotate user agent on each attempt
                self._rotate_user_agent()
                
                # Add random delay between attempts
                time.sleep(random.uniform(1.0, 3.0))
                
                # Set additional headers to avoid 403 errors
                self.session.headers.update({
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Referer': 'https://www.google.com/',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Cache-Control': 'max-age=0'
                })
                
                # Check if we already have HTML content from Selenium in basic connectivity check
                if hasattr(self, '_last_selenium_html') and self._last_selenium_html:
                    self.logger.info(f"Using HTML content from previous Selenium access for {website_url}")
                    # Create a mock response object with the Selenium HTML content
                    class MockResponse:
                        def __init__(self, text, status_code=200):
                            self.text = text
                            self.status_code = status_code
                    
                    response = MockResponse(self._last_selenium_html)
                    break
                else:
                    # Fetch the website content with increased timeout
                    try:
                        response = self.session.get(website_url, timeout=15, allow_redirects=True)
                    except requests.exceptions.ConnectionError as conn_err:
                        # Check specifically for DNS resolution errors
                        if 'Name or service not known' in str(conn_err) or "getaddrinfo failed" in str(conn_err) or "nodename nor servname provided" in str(conn_err):
                            self.logger.warning(f"DNS resolution error for {website_url}: {str(conn_err)}")
                            # If this is the last retry attempt, log it as a more severe error
                            if attempt == max_retries - 1:
                                self.logger.error(f"All DNS resolution attempts failed for {website_url}")
                                self.stats["dns_resolution_errors"] = self.stats.get("dns_resolution_errors", 0) + 1
                        else:
                            self.logger.warning(f"Connection error for {website_url}: {str(conn_err)}")
                        
                        # Wait longer for DNS issues before retrying
                        wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                        self.logger.info(f"Waiting {wait_time} seconds before retry {attempt+1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    
                    # Check for success
                    if response.status_code == 200:
                        break
                    
                    # Handle common HTTP errors
                    if response.status_code == 403:  # Forbidden
                        self.logger.warning(f"Attempt {attempt+1}/{max_retries}: Access forbidden for {website_url}")
                        
                        # Try with Selenium on 403 errors
                        self.logger.info(f"Trying to access {website_url} with Selenium due to 403 error")
                        selenium_success, html_content, final_url = self._access_with_selenium(website_url)
                        if selenium_success and html_content:
                            self.logger.info(f"Successfully accessed {website_url} with Selenium")
                            # Create a mock response object with the Selenium HTML content
                            class MockResponse:
                                def __init__(self, text, status_code=200):
                                    self.text = text
                                    self.status_code = status_code
                            
                            response = MockResponse(html_content)
                            # Update the URL if it changed after redirects
                            if final_url and final_url != website_url:
                                website_url = final_url
                            break
                            
                        # If Selenium failed and this is the last attempt, try an alternative URL format
                        elif attempt == max_retries - 1:
                            alt_url = None
                            if website_url.startswith('https://www.'):
                                alt_url = website_url.replace('https://www.', 'https://')
                            elif website_url.startswith('https://'):
                                alt_url = website_url.replace('https://', 'https://www.')
                            
                        if alt_url:
                            self.logger.info(f"Trying alternative URL format: {alt_url}")
                            try:
                                response = self.session.get(alt_url, timeout=15, allow_redirects=True)
                                if response.status_code == 200:
                                    website_url = alt_url  # Use the successful alternative URL
                                    break
                            except Exception as e:
                                self.logger.warning(f"Failed with alternative URL: {str(e)}")
                    elif response.status_code == 404:  # Not Found
                        self.logger.warning(f"Attempt {attempt+1}/{max_retries}: Page not found for {website_url}")
                    else:
                        self.logger.warning(f"Attempt {attempt+1}/{max_retries}: Failed to fetch website {website_url}: {response.status_code}")
                        
                    # Increase delay between retries
                    time.sleep(retry_delay * (attempt + 1))
                    
            except requests.exceptions.ConnectionError as e:
                self.logger.warning(f"Attempt {attempt+1}/{max_retries}: Connection error for {website_url}: {str(e)}")
                if 'Name or service not known' in str(e) and attempt == max_retries - 1:
                    # Try alternative domain on last attempt for DNS resolution issues
                    try:
                        domain_parts = website_url.split('/')
                        if len(domain_parts) >= 3:
                            domain = domain_parts[2]
                            if domain.startswith('www.'):
                                alt_domain = domain[4:]
                            else:
                                alt_domain = 'www.' + domain
                                
                            alt_url = website_url.replace(domain, alt_domain)
                            self.logger.info(f"Trying alternative domain: {alt_url}")
                            
                            response = self.session.get(alt_url, timeout=15, allow_redirects=True)
                            if response.status_code == 200:
                                website_url = alt_url  # Use the successful alternative URL
                                break
                    except Exception as alt_e:
                        self.logger.warning(f"Failed with alternative domain: {str(alt_e)}")
                
                # Increase delay between retries
                time.sleep(retry_delay * (attempt + 1))
                
            except requests.exceptions.Timeout:
                self.logger.warning(f"Attempt {attempt+1}/{max_retries}: Timeout for {website_url}")
                # Increase delay between retries
                time.sleep(retry_delay * (attempt + 1))
                
            except Exception as e:
                self.logger.warning(f"Attempt {attempt+1}/{max_retries}: Error fetching {website_url}: {str(e)}")
                # Increase delay between retries
                time.sleep(retry_delay * (attempt + 1))
        
        # Check if all attempts failed
        if 'response' not in locals() or response.status_code != 200:
            self.logger.error(f"All attempts to validate {website_url} failed")
            return False
            
        try:
            # Parse the HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract title and content
            title = soup.title.string if soup.title else ""
            
            # Get the main content (first 5000 characters)
            content = soup.get_text(strip=True)[:5000]
            
            # Extract links that might indicate manual pages
            manual_links = []
            for link in soup.find_all('a'):
                link_text = link.get_text().lower()
                link_href = link.get('href', '')
                if any(term in link_text for term in ['manual', 'guide', 'support', 'documentation', 'help']):
                    manual_links.append(f"{link_text}: {link_href}")
            
            # Limit to first 10 manual links
            manual_links_text = "\n".join(manual_links[:10])
            
            # Prepare the prompt for Claude following strict delimited format requirements
            prompt = f"""URL: {website_url}
Title: {title}

Content excerpt: {content[:2000]}  # Limit content to avoid token limits

Potential manual links found:
{manual_links_text}

I need you to analyze if this is the official website for the manufacturer or brand named '{manufacturer_name}' and if it appears to have product manuals or user guides available.

You MUST respond using EXACTLY this format without ANY additional text or explanations:

# RESPONSE_START
IS_OFFICIAL: yes  # or 'no' if not official
HAS_MANUALS: yes  # or 'no' if no evidence of manuals
# RESPONSE_END
"""
            
            # Set up API call with proper headers
            headers = {
                "x-api-key": self.anthropic_api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            payload = {
                "model": self.anthropic_model,
                "max_tokens": 100,
                "system": "You are a data extraction assistant that ONLY responds in a strict delimited format. Never include explanations or text outside the delimited format. Always wrap responses between # RESPONSE_START and # RESPONSE_END markers.",
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            # Make API call with retry logic
            max_api_retries = 2
            api_retry_delay = 2  # seconds
            
            for api_attempt in range(max_api_retries + 1):
                try:
                    api_response = requests.post(
                        "https://api.anthropic.com/v1/messages",
                        headers=headers,
                        json=payload,
                        timeout=20  # Increased timeout for API calls
                    )
                    
                    # Break if successful
                    if api_response.status_code == 200:
                        break
                        
                    # Handle rate limiting
                    if api_response.status_code == 429:
                        wait_time = api_retry_delay * (api_attempt + 1)
                        self.logger.warning(f"Rate limited by Claude API. Waiting {wait_time} seconds before retry.")
                        time.sleep(wait_time)
                    else:
                        self.logger.error(f"Error from Claude API: {api_response.status_code} - {api_response.text}")
                        if api_attempt < max_api_retries:
                            time.sleep(api_retry_delay)
                        
                except Exception as api_e:
                    self.logger.error(f"Exception calling Claude API: {str(api_e)}")
                    if api_attempt < max_api_retries:
                        time.sleep(api_retry_delay)
            
            # Check if all API attempts failed
            if 'api_response' not in locals() or api_response.status_code != 200:
                self.logger.error(f"All Claude API attempts failed for {website_url}")
                return False
                
            # Process successful API response
            try:
                response_data = api_response.json()
                result = response_data["content"][0]["text"]
                
                # Log the complete response for debugging
                self.logger.debug(f"Complete Claude validation response: {result}")
                
                # Parse the delimited response
                import re
                
                # Extract IS_OFFICIAL and HAS_MANUALS values
                is_official = False
                has_manuals = False
                
                # Check for IS_OFFICIAL
                official_pattern = r'IS_OFFICIAL:\s*(yes|no|true|false)'
                official_match = re.search(official_pattern, result, re.IGNORECASE)
                if official_match:
                    is_official = official_match.group(1).lower() in ['yes', 'true']
                
                # Check for HAS_MANUALS
                manuals_pattern = r'HAS_MANUALS:\s*(yes|no|true|false)'
                manuals_match = re.search(manuals_pattern, result, re.IGNORECASE)
                if manuals_match:
                    has_manuals = manuals_match.group(1).lower() in ['yes', 'true']
                
                # Log the validation results
                self.logger.info(f"Website validation for {manufacturer_name} ({website_url}):\n" +
                               f"  Is official: {is_official}\n" +
                               f"  Has manuals: {has_manuals}")
                
                # Only consider valid if both conditions are met
                if is_official and has_manuals:
                    self.logger.info(f"Validated {website_url} as official website with manuals for {manufacturer_name}")
                    return True
                else:
                    reason = []
                    if not is_official:
                        reason.append("not the official website")
                    if not has_manuals:
                        reason.append("no evidence of manuals")
                    
                    self.logger.info(f"Validation failed: {website_url} - {', '.join(reason)}")
                    self.stats["validation_failed"] += 1
                    return False
                    
            except Exception as parse_e:
                self.logger.error(f"Error parsing Claude API response: {str(parse_e)}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error validating website: {str(e)}")
            return False
    
    def find_websites(self, limit: Optional[int] = None, continue_from: Optional[str] = None) -> Dict[str, Any]:
        """Find websites for manufacturers without validated websites following the Selenium workflow.
        
        Workflow:
        1. Check database for manufacturers with unvalidated websites.
        2. Visit the stated website with Selenium and validate with Claude Haiku.
        3. If validation is positive, move to the next manufacturer.
        4. If negative, ask Claude Haiku for the website, and repeat validation.
        5. If Haiku doesn't know or website doesn't exist, search Google and analyze top 3 results.
        6. If no valid website is found, mark as "No website found".
        7. Continue until no unvalidated websites remain.
        8. Then find websites for manufacturers without one at all.
        
        Args:
            limit: Optional limit on the number of manufacturers to process
            continue_from: Optional manufacturer name to continue from (alphabetically)
            
        Returns:
            Statistics dictionary
        """
        # Set a global timeout for the entire process
        import signal
        import platform
        
        # Only use SIGALRM on systems that support it (Linux)
        if platform.system() != 'Windows':
            def global_timeout_handler(signum, frame):
                self.logger.error("Global timeout reached - process taking too long")
                return
                
            # Set a 2-hour timeout for the entire process
            signal.signal(signal.SIGALRM, global_timeout_handler)
            signal.alarm(7200)  # 2 hours in seconds
        self.logger.info("Starting Selenium-based website finding process")
        start_time = time.time()
        
        # STAGE 1: Process manufacturers with unvalidated websites first
        self.logger.info("STAGE 1: Processing manufacturers with unvalidated websites")
        
        # Get manufacturers with unvalidated websites
        manufacturers_with_unvalidated = self._get_manufacturers_with_unvalidated_website()
        self.logger.info(f"Found {len(manufacturers_with_unvalidated)} manufacturers with unvalidated websites")
        
        # Initialize counters for this run
        processed_count = 0
        success_count = 0
        
        # Apply continue_from filter if specified
        if continue_from and manufacturers_with_unvalidated:
            self.logger.info(f"Continuing from manufacturer: {continue_from}")
            start_idx = 0
            for idx, m in enumerate(manufacturers_with_unvalidated):
                if m.name.lower() >= continue_from.lower():
                    start_idx = idx
                    break
            manufacturers_with_unvalidated = manufacturers_with_unvalidated[start_idx:]
            self.logger.info(f"Starting from index {start_idx} ({len(manufacturers_with_unvalidated)} manufacturers remaining)")
        
        # Apply limit if specified
        if limit and limit > 0 and manufacturers_with_unvalidated:
            manufacturers_with_unvalidated = manufacturers_with_unvalidated[:limit]
            self.logger.info(f"Limited to processing {limit} manufacturers")
        
        # STAGE 1: Process manufacturers with existing but unvalidated websites
        for manufacturer in manufacturers_with_unvalidated:
            # Check if shutdown was requested
            if self.shutdown_requested:
                self.logger.info("Shutdown requested, stopping processing")
                break
                
            try:
                processed_count += 1
                self.logger.info(f"Processing manufacturer with unvalidated website: {manufacturer.name} (ID: {manufacturer.id}) " + 
                               f"[{processed_count}/{len(manufacturers_with_unvalidated)}]")
                
                # Step 1: Extract the current website URL
                website_url = manufacturer.website.strip() if manufacturer.website else None
                
                if not website_url:
                    self.logger.warning(f"Manufacturer {manufacturer.name} has empty website URL marked as unvalidated")
                    continue
                
                # Step 2: Visit the website with Selenium and validate with Claude Haiku
                self.logger.info(f"Validating existing website for {manufacturer.name}: {website_url}")
                try:
                    is_valid = self._validate_website_with_selenium(manufacturer.name, website_url)
                except Exception as e:
                    self.logger.error(f"Error validating website for {manufacturer.name}: {str(e)}")
                    is_valid = False
                
                # Step 3: If validation is successful, update database and continue to next manufacturer
                if is_valid:
                    try:
                        self._update_manufacturer_website(manufacturer.id, website_url, True)
                        success_count += 1
                        self.logger.info(f"Successfully validated existing website for {manufacturer.name}")
                        continue
                    except Exception as e:
                        self.logger.error(f"Error updating database for {manufacturer.name}: {str(e)}")
                        continue
                
                # Step 4: If validation fails, ask Claude Haiku for the website
                self.logger.info(f"Validation failed for {manufacturer.name}. Querying Claude Haiku for website.")
                try:
                    claude_website = self._query_claude_for_website(manufacturer.name)
                except Exception as e:
                    self.logger.error(f"Error querying Claude for {manufacturer.name}: {str(e)}")
                    claude_website = None
                
                if claude_website and claude_website != website_url:
                    # Step 5: Validate the website suggested by Claude
                    self.logger.info(f"Validating Claude's suggestion for {manufacturer.name}: {claude_website}")
                    try:
                        is_valid = self._validate_website_with_selenium(manufacturer.name, claude_website)
                    except Exception as e:
                        self.logger.error(f"Error validating Claude's website for {manufacturer.name}: {str(e)}")
                        is_valid = False
                    
                    if is_valid:
                        try:
                            self._update_manufacturer_website(manufacturer.id, claude_website, True)
                            success_count += 1
                            self.logger.info(f"Successfully validated Claude's suggested website for {manufacturer.name}")
                            continue
                        except Exception as e:
                            self.logger.error(f"Error updating database with Claude's website for {manufacturer.name}: {str(e)}")
                            continue
                
                # Step 6: If Claude doesn't know or validation fails, search Google and validate top results
                self.logger.info(f"Searching and validating top results for {manufacturer.name}")
                try:
                    google_website = self._search_and_validate_top_results(manufacturer.name)
                except Exception as e:
                    self.logger.error(f"Error searching and validating top results for {manufacturer.name}: {str(e)}")
                    google_website = None
            
                if google_website:
                    try:
                        self._update_manufacturer_website(manufacturer.id, google_website, True)
                        success_count += 1
                        self.logger.info(f"Successfully found and validated website for {manufacturer.name} through Google search")
                    except Exception as e:
                        self.logger.error(f"Error updating database with Google's website for {manufacturer.name}: {str(e)}")
                else:
                    # Step 7: If no valid website found in top results, mark as "No website found"
                    try:
                        self._update_manufacturer_website(manufacturer.id, "No website found", True)
                        self.logger.info(f"No valid website found for {manufacturer.name}, marked as 'No website found'")
                    except Exception as e:
                        self.logger.error(f"Error updating database with 'No website found' for {manufacturer.name}: {str(e)}")
            except Exception as e:
                self.logger.error(f"Unexpected error processing manufacturer {manufacturer.name}: {str(e)}")
            
                # Log progress regularly
                if processed_count % 5 == 0 or processed_count == len(manufacturers_with_unvalidated):
                    self.logger.info(f"Progress STAGE 1: {processed_count}/{len(manufacturers_with_unvalidated)} " + 
                                   f"manufacturers processed, {success_count} successful")
                
                # Add random delay between manufacturers to avoid detection
                time.sleep(random.uniform(2.0, 5.0))
        
        # STAGE 2: Find websites for manufacturers without any website
        if not (limit and processed_count >= limit) or not manufacturers_with_unvalidated:
            self.logger.info("STAGE 2: Processing manufacturers without websites")
            
            # Get manufacturers without any website
            manufacturers_without_website = []
            with self.db_manager.session() as session:
                # Query manufacturers with no website or empty website string
                manufacturers_without_website = session.query(Manufacturer).filter(
                    or_(
                        Manufacturer.website == None, 
                        Manufacturer.website == '',
                        Manufacturer.website == 'No website found'  # We'll retry these too
                    )
                ).order_by(Manufacturer.name).all()
            
            self.logger.info(f"Found {len(manufacturers_without_website)} manufacturers without websites")
            
            # Apply limit if specified
            remaining_limit = None
            if limit and limit > 0:
                remaining_limit = limit - processed_count
                if remaining_limit <= 0:
                    self.logger.info(f"Limit of {limit} manufacturers reached. Skipping STAGE 2.")
                    manufacturers_without_website = []
                else:
                    manufacturers_without_website = manufacturers_without_website[:remaining_limit]
                    self.logger.info(f"Limited STAGE 2 to processing {remaining_limit} manufacturers")
            
            # Process each manufacturer without a website
            stage2_count = 0
            for manufacturer in manufacturers_without_website:
                stage2_count += 1
                processed_count += 1
                self.logger.info(f"Processing manufacturer without website: {manufacturer.name} (ID: {manufacturer.id}) " + 
                               f"[{stage2_count}/{len(manufacturers_without_website)}]")
                
                # Start from Step 4: Ask Claude Haiku for the website
                claude_website = self._query_claude_for_website(manufacturer.name)
                
                if claude_website:
                    # Step 5: Validate the website suggested by Claude
                    self.logger.info(f"Validating Claude's suggestion for {manufacturer.name}: {claude_website}")
                    is_valid = self._validate_website_with_selenium(manufacturer.name, claude_website)
                    
                    if is_valid:
                        self._update_manufacturer_website(manufacturer.id, claude_website, True)
                        success_count += 1
                        self.logger.info(f"Successfully validated Claude's suggested website for {manufacturer.name}")
                        continue
                
                # Step 6: If Claude doesn't know or validation fails, search Google and validate top results
                self.logger.info(f"Searching and validating top results for {manufacturer.name}")
                google_website = self._search_and_validate_top_results(manufacturer.name)
                
                if google_website:
                    self._update_manufacturer_website(manufacturer.id, google_website, True)
                    success_count += 1
                    self.logger.info(f"Successfully found and validated website for {manufacturer.name} through Google search")
                else:
                    # Step 7: If no valid website found in top results, mark as "No website found"
                    self._update_manufacturer_website(manufacturer.id, "No website found", True)
                    self.logger.info(f"No valid website found for {manufacturer.name}, marked as 'No website found'")
                
                # Log progress regularly
                if stage2_count % 5 == 0 or stage2_count == len(manufacturers_without_website):
                    self.logger.info(f"Progress STAGE 2: {stage2_count}/{len(manufacturers_without_website)} " + 
                                   f"manufacturers processed, {success_count} successful total")
                
                # Add random delay between manufacturers to avoid detection
                time.sleep(random.uniform(2.0, 5.0))
        
        # Calculate duration
        duration = time.time() - start_time
        hours, remainder = divmod(duration, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Update statistics
        self.stats["duration"] = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
        self.stats["processed_manufacturers"] = processed_count
        self.stats["successful_validations"] = success_count
        
        self.logger.info(f"Website finding process completed in {self.stats['duration']}")
        return self.stats
    
    def process_batch(self, max_pages=None, max_runtime_minutes=None):
        """
        Process a batch of manufacturers with time and page limits.
        
        Args:
            max_pages (int, optional): Maximum number of manufacturers to process in this batch.
                If None, no limit is applied.
            max_runtime_minutes (int, optional): Maximum runtime in minutes.
                If None, runs until max_pages is reached.
                
        Returns:
            dict: Statistics about the batch processing.
        """
        self.logger.info(f"Starting batch processing with max_pages={max_pages}, max_runtime_minutes={max_runtime_minutes}")
        
        # Initialize stats for this batch
        batch_start_time = time.time()
        self.stats['batch_start_time'] = batch_start_time
        
        # Calculate end time if max_runtime_minutes is specified
        end_time = None
        if max_runtime_minutes is not None and max_runtime_minutes > 0:
            end_time = time.time() + (max_runtime_minutes * 60)
            self.logger.info(f"Batch will run until {time.strftime('%H:%M:%S', time.localtime(end_time))}")
        
        # Get manufacturers without validated websites
        manufacturers_without_website = self._get_manufacturers_without_website()
        manufacturers_with_unvalidated = self._get_manufacturers_with_unvalidated_website()
        
        # Prioritize manufacturers with unvalidated websites first
        manufacturers_to_process = manufacturers_with_unvalidated + manufacturers_without_website
        
        # Apply limit if specified
        if max_pages and max_pages > 0 and manufacturers_to_process:
            manufacturers_to_process = manufacturers_to_process[:max_pages]
            self.logger.info(f"Limited to processing {max_pages} manufacturers")
        
        # Initialize counters for this run
        processed_count = 0
        success_count = 0
        
        # Process manufacturers until we reach the limit or run out of time
        for manufacturer in manufacturers_to_process:
            # Check if we've exceeded the maximum runtime
            if end_time and time.time() >= end_time:
                self.logger.info(f"Reached maximum runtime of {max_runtime_minutes} minutes")
                break
                
            try:
                processed_count += 1
                self.logger.info(f"Processing manufacturer: {manufacturer.name} (ID: {manufacturer.id}) " + 
                               f"[{processed_count}/{len(manufacturers_to_process)}]")
                
                # Extract the current website URL
                website_url = manufacturer.website.strip() if manufacturer.website else None
                
                # If manufacturer has an unvalidated website, validate it first
                if website_url and website_url != "No website found":
                    self.logger.info(f"Validating existing website for {manufacturer.name}: {website_url}")
                    try:
                        is_valid = self._validate_website_with_selenium(manufacturer.name, website_url)
                    except Exception as e:
                        self.logger.error(f"Error validating website for {manufacturer.name}: {str(e)}")
                        is_valid = False
                    
                    if is_valid:
                        try:
                            self._update_manufacturer_website(manufacturer.id, website_url, True)
                            success_count += 1
                            self.logger.info(f"Successfully validated existing website for {manufacturer.name}")
                            continue
                        except Exception as e:
                            self.logger.error(f"Error updating database for {manufacturer.name}: {str(e)}")
                            continue
                
                # If validation fails or no website, ask Claude Haiku for the website
                self.logger.info(f"Querying Claude Haiku for website for {manufacturer.name}")
                try:
                    claude_website = self._query_claude_for_website(manufacturer.name)
                except Exception as e:
                    self.logger.error(f"Error querying Claude for {manufacturer.name}: {str(e)}")
                    claude_website = None
                
                if claude_website and (not website_url or claude_website != website_url):
                    # Validate the website suggested by Claude
                    self.logger.info(f"Validating Claude's suggestion for {manufacturer.name}: {claude_website}")
                    try:
                        is_valid = self._validate_website_with_selenium(manufacturer.name, claude_website)
                    except Exception as e:
                        self.logger.error(f"Error validating Claude's website for {manufacturer.name}: {str(e)}")
                        is_valid = False
                    
                    if is_valid:
                        try:
                            self._update_manufacturer_website(manufacturer.id, claude_website, True)
                            success_count += 1
                            self.logger.info(f"Successfully validated Claude's suggested website for {manufacturer.name}")
                            continue
                        except Exception as e:
                            self.logger.error(f"Error updating database with Claude's website for {manufacturer.name}: {str(e)}")
                            continue
                
                # If Claude doesn't know or validation fails, search and validate top results
                self.logger.info(f"Searching and validating top results for {manufacturer.name}")
                try:
                    google_website = self._search_and_validate_top_results(manufacturer.name)
                except Exception as e:
                    self.logger.error(f"Error searching and validating top results for {manufacturer.name}: {str(e)}")
                    google_website = None
            
                if google_website:
                    try:
                        self._update_manufacturer_website(manufacturer.id, google_website, True)
                        success_count += 1
                        self.logger.info(f"Successfully found and validated website for {manufacturer.name} through search")
                    except Exception as e:
                        self.logger.error(f"Error updating database with search website for {manufacturer.name}: {str(e)}")
                else:
                    # If no valid website found in top results, mark as "No website found"
                    try:
                        self._update_manufacturer_website(manufacturer.id, "No website found", True)
                        self.logger.info(f"No valid website found for {manufacturer.name}, marked as 'No website found'")
                    except Exception as e:
                        self.logger.error(f"Error updating database with 'No website found' for {manufacturer.name}: {str(e)}")
            except Exception as e:
                self.logger.error(f"Unexpected error processing manufacturer {manufacturer.name}: {str(e)}")
            
            # Log progress regularly
            if processed_count % 5 == 0 or processed_count == len(manufacturers_to_process):
                self.logger.info(f"Progress: {processed_count}/{len(manufacturers_to_process)} " + 
                               f"manufacturers processed, {success_count} successful")
            
            # Add random delay between manufacturers to avoid detection
            time.sleep(random.uniform(2.0, 5.0))
        
        # Update stats
        batch_end_time = time.time()
        self.stats['batch_end_time'] = batch_end_time
        self.stats['batch_duration'] = batch_end_time - batch_start_time
        self.stats['processed_manufacturers'] = processed_count
        self.stats['successful_validations'] = success_count
        
        # Calculate duration in human-readable format
        duration = batch_end_time - batch_start_time
        hours, remainder = divmod(duration, 3600)
        minutes, seconds = divmod(remainder, 60)
        self.stats["duration"] = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
        
        self.logger.info(f"Batch processing completed in {self.stats['duration']}")
        return self.stats
    
    def display_statistics(self):
        """Display statistics about the website finding process."""
        print("\n" + "=" * 60)
        print("MANUFACTURER WEBSITE FINDER STATISTICS")
        print("=" * 60)
        
        # Database statistics
        print("\nDATABASE STATISTICS:")
        print(f"Total manufacturers in database: {self.stats['total_manufacturers']}")
        print(f"Manufacturers with websites before: {self.stats['manufacturers_with_website']}")
        
        # Process statistics
        if "processed_manufacturers" in self.stats:
            print("\nPROCESS STATISTICS:")
            print(f"Manufacturers processed: {self.stats['processed_manufacturers']}")
            print(f"Websites found via Claude: {self.stats['claude_found']}")
            print(f"Websites found via search engines: {self.stats['search_engine_found']}")
            print(f"Validation failures: {self.stats['validation_failed']}")
            
            # Calculate success rate
            success_count = self.stats['claude_found'] + self.stats['search_engine_found'] - self.stats['validation_failed']
            if self.stats['processed_manufacturers'] > 0:
                success_rate = (success_count / self.stats['processed_manufacturers']) * 100
                print(f"Success rate: {success_rate:.2f}%")
            
            # Search engine usage
            print("\nSEARCH ENGINE USAGE:")
            for engine, count in self.stats["search_engine_usage"].items():
                if count > 0:
                    print(f"  {engine}: {count} queries")
            
            # Duration
            if "duration" in self.stats:
                print(f"\nTotal duration: {self.stats['duration']}")
        
        print("=" * 60 + "\n")

def main():
    """Main entry point for the manufacturer website finder."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='NDAIVI Manufacturer Website Finder')
    parser.add_argument('--config', default='config.yaml', help='Path to configuration file')
    parser.add_argument('--limit', type=int, help='Limit the number of manufacturers to process')
    parser.add_argument('--continue-from', type=str, help='Continue processing from a specific manufacturer (alphabetically)')
    
    args = parser.parse_args()
    
    try:
        # Initialize the finder
        finder = ManufacturerWebsiteFinder(config_path=args.config)
        
        # Find websites
        finder.find_websites(limit=args.limit, continue_from=args.continue_from)
        
        # Display statistics
        finder.display_statistics()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
