import requests
import yaml
import logging
import time
import os
import re
import datetime
import requests
import random
import sqlite3
import signal
import threading
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import anthropic
from scraper.claude_analyzer import ClaudeAnalyzer
from sqlalchemy import func
from database.schema import Base, Manufacturer, Category, Product
from database.db_manager import get_db_manager
# Add the project root to the Python path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.schema import init_db, Category, Manufacturer, CrawlStatus, ScraperLog, ScraperSession
from database.statistics_manager import StatisticsManager
import json
from sqlalchemy import func, desc

# Global shutdown and suspension flags for signal handling
_SHUTDOWN_REQUESTED = False
_SUSPEND_REQUESTED = False

def global_shutdown_handler(sig, frame):
    """Global signal handler for graceful shutdown
    
    Args:
        sig: Signal number
        frame: Current stack frame
    """
    global _SHUTDOWN_REQUESTED
    _SHUTDOWN_REQUESTED = True
    print(f"Received shutdown signal {sig}, finishing gracefully...")

def global_suspend_handler(sig, frame):
    """Global signal handler for process suspension (CTRL+Z)
    
    Args:
        sig: Signal number
        frame: Current stack frame
    """
    global _SUSPEND_REQUESTED
    _SUSPEND_REQUESTED = True
    print(f"Received suspension signal {sig}, marking session as interrupted...")
    
    # After marking as interrupted, call the default handler to actually suspend
    # We'll use the default handler by resetting the signal and re-raising it
    signal.signal(signal.SIGTSTP, signal.SIG_DFL)
    os.kill(os.getpid(), signal.SIGTSTP)
    
# Register global signal handlers
signal.signal(signal.SIGINT, global_shutdown_handler)  # Ctrl+C
signal.signal(signal.SIGTERM, global_shutdown_handler)  # kill command

# Register our custom SIGTSTP handler that marks the session as interrupted before suspending
try:
    signal.signal(signal.SIGTSTP, global_suspend_handler)  # Ctrl+Z
except AttributeError:
    # SIGTSTP might not be available on all platforms
    pass

class CompetitorScraper:
    def __init__(self, config_path='config.yaml'):
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize database with our singleton database manager
        # This prevents race conditions and database locks
        try:
            # First, ensure the database exists with all required tables
            from database.schema import init_db
            db_path = self.config['database']['path']
            
            # Create database directory if it doesn't exist
            db_dir = os.path.dirname(db_path)
            if not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
                self.logger.info(f"Created database directory {db_dir}")
            
            # Initialize database if it doesn't exist or force initialization
            if not os.path.exists(db_path):
                self.logger.info(f"Database not found. Initializing database at {db_path}")
                init_db(db_path, config_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml'))
                self.logger.info("Database initialized successfully")
            
            # Now get the database manager singleton
            self.db_manager = get_db_manager(self.config['database']['path'])
            # Ensure the database manager is actually initialized
            if not hasattr(self.db_manager, '_initialized') or not self.db_manager._initialized:
                self.db_manager.initialize(self.config['database']['path'])
            self.logger.info(f"Database manager initialized for {self.config['database']['path']}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database manager: {str(e)}")
            raise
        
        # No separate session creation - we'll use the db_manager.session() context manager
        # This ensures proper locking and transaction handling
        
        # Initialize AI clients
        self._setup_ai_clients()
        
        # Set up crawling variables
        self.visited_urls = self._load_visited_urls()
        self.url_queue = []
        self.current_depth = 0
        
        # Initialize StatisticsManager
        self.statistics_manager = StatisticsManager(self.db_manager)
        
        self.logger.info("Competitor Scraper initialized successfully")
    
    def _load_config(self, config_path):
        """Load configuration from YAML file"""
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def _setup_logging(self):
        """Set up logging configuration"""
        log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, 'competitor_scraper.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger('competitor_scraper')
    
    def _setup_ai_clients(self):
        """Initialize AI API clients"""
        # Anthropic setup only - without proxy configuration
        anthropic_config = self.config['ai_apis']['anthropic']
        
        # Get API key from environment variables, fallback to config
        self.anthropic_api_key = os.environ.get('ANTHROPIC_API_KEY')
        if not self.anthropic_api_key:
            self.anthropic_api_key = anthropic_config.get('api_key')
            if not self.anthropic_api_key:
                self.logger.error("No Anthropic API key found in environment or config")
                raise ValueError("Anthropic API key is required. Set ANTHROPIC_API_KEY environment variable or in config.yaml")
        
        self.anthropic_model = anthropic_config['model']
        self.anthropic_sonnet_model = anthropic_config.get('sonnet_model')
        
        # Initialize the Claude analyzer
        self.claude_analyzer = ClaudeAnalyzer(
            api_key=self.anthropic_api_key,
            model=self.anthropic_model,
            sonnet_model=self.anthropic_sonnet_model
        )
        
        self.logger.info("Anthropic API configuration loaded successfully")

    def _log_to_db(self, level, message):
        """Log message to database using the database manager to prevent locks"""
        # Truncate extremely long messages to prevent excessive database load
        if len(message) > 2000:
            message = message[:1997] + '...'
            
        # Use our database manager to log to the database
        # This ensures all database access goes through a single connection
        # with proper locking to prevent race conditions
        try:
            # Use a context manager to ensure proper transaction handling
            with self.db_manager.session() as session:
                # Create a new log entry
                from database.schema import ScraperLog
                log_entry = ScraperLog(
                    timestamp=datetime.datetime.now(),
                    level=level,
                    message=message
                )
                session.add(log_entry)
                # No need to commit explicitly - the context manager handles it
                
        except Exception as e:
            # If database logging fails, log to file
            self.logger.warning(f"Failed to log to database: {str(e)}")
            
            # Additional detailed error info for debugging
            import traceback
            self.logger.debug(f"Database logging error details: {traceback.format_exc()}")

    
    def _load_visited_urls(self):
        """Load previously visited URLs from the database using our database manager"""
        visited_urls = set()
        try:
            # Use the database manager to ensure proper locking
            with self.db_manager.session() as session:
                # Get all URLs that have been visited
                visited_records = session.query(CrawlStatus).filter_by(visited=True).all()
                for record in visited_records:
                    visited_urls.add(record.url)
                
            self.logger.info(f"Loaded {len(visited_urls)} previously visited URLs from database")
        except Exception as e:
            self.logger.error(f"Error loading visited URLs: {str(e)}")
        
        return visited_urls
    
    def check_shutdown_requested(self):
        """Check if shutdown was requested via global signal handler
        
        Returns:
            bool: True if shutdown was requested, False otherwise
        """
        global _SHUTDOWN_REQUESTED, _SUSPEND_REQUESTED
        
        # Check for suspension request first
        if _SUSPEND_REQUESTED and not getattr(self, 'suspend_handled', False):
            self.suspend_handled = True
            self.logger.info("Suspension requested via CTRL+Z, marking session as interrupted...")
            self._log_to_db("INFO", "Process suspension requested via CTRL+Z")
            
            # Mark the current session as interrupted before suspension
            if hasattr(self, 'session_id') and self.session_id is not None:
                try:
                    with self.db_manager.session() as session:
                        scraper_session = session.query(ScraperSession).filter_by(id=self.session_id).first()
                        if scraper_session and scraper_session.status == 'running':
                            scraper_session.status = 'interrupted'
                            scraper_session.end_time = datetime.datetime.now()
                            self.logger.info(f"Marked session {self.session_id} as interrupted due to CTRL+Z")
                except Exception as e:
                    self.logger.error(f"Error updating session status during suspension: {str(e)}")
        
        # Then check for shutdown request
        if _SHUTDOWN_REQUESTED and not getattr(self, 'shutdown_requested', False):
            self.shutdown_requested = True
            self.logger.info("Shutdown requested via signal, finishing gracefully...")
            self._log_to_db("INFO", "Shutdown requested via signal")
            # Ensure we clean up resources when shutdown is requested
            self.close()
            
        return getattr(self, 'shutdown_requested', False)
    
    def start_crawling(self, max_pages=None):
        """
        Start the crawling process.
        
        Args:
            max_pages (int, optional): Maximum number of pages to process.
                If None, uses the value from config.
        """
        self.statistics_manager.start_session('scraper')
        # Log to file only first to avoid any startup database contention issues
        self.logger.info("Starting crawling process")
        
        # Store session ID from statistics manager
        self.session_id = self.statistics_manager.session_id
        self.logger.info(f"Created new scraper session with ID {self.session_id}")
        
        # Initialize shutdown flag
        self.shutdown_requested = False
        
        # Check if shutdown was already requested before we even started
        self.check_shutdown_requested()
        
        # Initialize the URL queue with the target URL and unvisited URLs from database
        if not self.url_queue:
            self._load_initial_urls()
            self._load_unvisited_urls()
            
        # Use process_batch to handle the actual crawling
        # This ensures that max_pages is properly respected
        batch_stats = self.process_batch(max_pages=max_pages)
        
        # Update session record with final status
        try:
            # Let the statistics manager handle the session update
            self.statistics_manager.update_session_in_database(completed=True)
            self.logger.info(f"Updated session {self.session_id} - marked as completed")
        except Exception as e:
            self.logger.error(f"Failed to update scraper session: {str(e)}")
            
        # Log completion
        self.logger.info(f"Crawling finished. Processed {batch_stats.get('pages_crawled', 0)} pages.")
        
        return batch_stats
    
    def generate_sitemap_files(self):
        """Generate sitemap files from database information"""
        try:
            # Create output directory if it doesn't exist
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            # Get sitemap data from database
            sitemap = []
            with self.db_manager.session() as session:
                # Get all visited URLs with title and outgoing links
                entries = session.query(CrawlStatus).filter_by(visited=True).all()
                
                for entry in entries:
                    if entry.url:
                        sitemap.append({
                            'url': entry.url,
                            'title': entry.title if entry.title else "",
                            'depth': entry.depth if entry.depth is not None else 0,
                            'links': entry.outgoing_links if entry.outgoing_links is not None else 0
                        })
            
            if not sitemap:
                self.logger.warning("No sitemap data found in database")
                return
                
            # Save as JSON
            sitemap_path = os.path.join(output_dir, 'sitemap.json')
            with open(sitemap_path, 'w') as f:
                json.dump(sitemap, f, indent=2)
            
            # Also save as XML (standard sitemap format)
            xml_path = os.path.join(output_dir, 'sitemap.xml')
            with open(xml_path, 'w') as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
                
                for entry in sitemap:
                    f.write('  <url>\n')
                    f.write(f'    <loc>{entry["url"]}</loc>\n')
                    f.write('  </url>\n')
                
                f.write('</urlset>')
            
            self.logger.info(f"Sitemap generated with {len(sitemap)} URLs and saved to {sitemap_path} and {xml_path}")
            self._log_to_db("INFO", f"Sitemap generated with {len(sitemap)} URLs and saved to {sitemap_path} and {xml_path}")
            
        except Exception as e:
            self.logger.error(f"Error generating sitemap: {str(e)}")
            self._log_to_db("ERROR", f"Error generating sitemap: {str(e)}")
    
    def _load_initial_urls(self):
        """Load initial URLs from configuration"""
        # Get initial URLs from config or use target_url if not defined
        initial_urls = self.config['crawling'].get('initial_urls', [self.config['target_url']])
        
        # Add URLs to the queue if not already visited
        added_count = 0
        for url in initial_urls:
            if url not in self.visited_urls:
                self.url_queue.append((url, 0))
                added_count += 1
            
            # Add to database if not exists using our database manager
            try:
                with self.db_manager.session() as session:
                    existing_status = session.query(CrawlStatus).filter_by(url=url).first()
                    if not existing_status:
                        status = CrawlStatus(url=url, depth=0)
                        session.add(status)
                    # No explicit commit needed - handled by context manager
            except Exception as e:
                self.logger.warning(f"Error adding URL to database: {str(e)}")
        self.logger.info(f"Added {added_count} initial URLs to the queue")
        
    def _load_unvisited_urls(self):
        """Load unvisited URLs from the database to resume scraping"""
        try:
            # Use our database manager to get unvisited URLs
            with self.db_manager.session() as session:
                # Get all URLs that have been added but not visited yet
                # Limit to a reasonable number to avoid memory issues
                unvisited_records = session.query(CrawlStatus).filter_by(visited=False).limit(1000).all()
                added_count = 0
                
                for record in unvisited_records:
                    if record.url not in self.visited_urls and not any(record.url == url for url, _ in self.url_queue):
                        self.url_queue.append((record.url, record.depth))
                        added_count += 1
                
                # If no URLs were loaded, add the initial URL as a fallback
                if added_count == 0 and len(self.url_queue) == 0:
                    target_url = self.config['target_url']
                    self.logger.info(f"No unvisited URLs found in database, adding target URL: {target_url}")
                    self.url_queue.append((target_url, 0))
                    
                    # Make sure it's in the database - reusing the same session
                    existing_status = session.query(CrawlStatus).filter_by(url=target_url).first()
                    if not existing_status:
                        status = CrawlStatus(url=target_url, depth=0)
                        session.add(status)
                        # No explicit commit needed - handled by context manager
                    added_count = 1
            
            self.logger.info(f"Loaded {added_count} unvisited URLs from database to resume scraping")
        except Exception as e:
            self.logger.error(f"Error loading unvisited URLs: {str(e)}")
    
    def _crawl_page(self, url):
        """Crawl a single page and return the soup and new URLs"""
        headers = {
            'User-Agent': self.config['crawling']['user_agent']
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract all links from the page
            new_urls = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                absolute_url = urljoin(url, href)
                
                # Only include URLs from the same domain
                if self._is_same_domain(url, absolute_url):
                    new_urls.append(absolute_url)
            
            return soup, new_urls
            
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {str(e)}")
            return None, []
    
    def _is_same_domain(self, url1, url2):
        """Check if two URLs belong to the same domain"""
        domain1 = urlparse(url1).netloc
        domain2 = urlparse(url2).netloc
        return domain1 == domain2
    
    def _process_page(self, url, soup, depth):
        """Process the page content to extract manufacturer and category information"""
        # Get the page title and content text
        title = soup.title.string if soup.title else ""
        content = self._extract_text_content(soup)
        
        # Check if the page matches any positive keywords
        positive_keywords = self.config['keywords']['positive']
        negative_keywords = self.config['keywords']['negative']
        
        # Check for positive keywords in URL, title, and content
        positive_match = any(keyword.lower() in url.lower() or 
                            keyword.lower() in title.lower() or 
                            keyword.lower() in content.lower() 
                            for keyword in positive_keywords)
        
        # Check for negative keywords
        negative_match = any(keyword.lower() in url.lower() or 
                            keyword.lower() in title.lower() 
                            for keyword in negative_keywords)
        
        # Update crawl status using our database manager
        try:
            with self.db_manager.session() as session:
                status = session.query(CrawlStatus).filter_by(url=url).first()
                
                if not status:
                    self.logger.warning(f"No status record found for URL: {url}")
                    return
                    
                if positive_match and not negative_match:
                    self.logger.info(f"Found potential manufacturer page: {url}")
                    # Use Claude Sonnet for detailed analysis
                    self._analyze_with_claude_sonnet(url, title, content)
                    status.analyzed = True
                    status.is_manufacturer_page = True
                else:
                    # Use Claude Haiku for cheaper analysis to determine if it's a manufacturer page
                    is_manufacturer = self._analyze_with_anthropic(url, title, content)
                    status.analyzed = True
                    status.is_manufacturer_page = is_manufacturer
                    
                    if is_manufacturer:
                        self.logger.info(f"Claude identified manufacturer page: {url}")
                        # Use Claude Sonnet for detailed analysis
                        self._analyze_with_claude_sonnet(url, title, content)
                # No explicit commit needed - handled by context manager
        except Exception as e:
            self.logger.warning(f"Error updating URL status: {str(e)}")
    
    def _extract_text_content(self, soup):
        """Extract readable text content from the page"""
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.extract()
        
        # Get text
        text = soup.get_text()
        
        # Break into lines and remove leading and trailing space on each
        lines = (line.strip() for line in text.splitlines())
        
        # Break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        
        # Drop blank lines
        text = '\n'.join(chunk for chunk in chunks if chunk)
        
        return text
    
    def _analyze_with_anthropic(self, url, title, content):
        """Use Claude Haiku to determine if the page is a manufacturer page"""
        try:
            # Import prompt templates
            from scraper.prompt_templates import MANUFACTURER_DETECTION_PROMPT, SYSTEM_PROMPT
            
            # Prepare a truncated version of the content to avoid token limits
            truncated_content = content[:2000] + "..." if len(content) > 2000 else content
            
            # Format the prompt with the page details
            prompt = MANUFACTURER_DETECTION_PROMPT.format(
                url=url,
                title=title,
                truncated_content=truncated_content
            )
            
            # Use requests to make a direct API call instead of using the client
            headers = {
                "x-api-key": self.anthropic_api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"  # Use the appropriate API version
            }
            
            payload = {
                "model": self.anthropic_model,
                "max_tokens": 10,
                "system": "You are a data extraction assistant specialized in identifying manufacturers and their specific product categories. Always ensure categories are associated with their manufacturer names for clarity.",
                "messages": [
                    {"role": "user", "content": prompt + " Respond in  format."}
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
                answer = response_data["content"][0]["text"].strip().lower()
                
                # Log the response
                self.logger.info(f"Claude analysis for {url}: {answer}")
                
                return answer == "yes"
            else:
                self.logger.error(f"Error from Anthropic API: {response.status_code} - {response.text}")
                return False
            
        except Exception as e:
            self.logger.error(f"Error analyzing with Claude: {str(e)}")
            self._log_to_db("ERROR", f"Error analyzing with Claude: {str(e)}")
            return False
    
    def _analyze_with_claude_sonnet(self, url, title, content):
        """Use Claude 3.7 Sonnet to extract manufacturer and category information"""
        try:
            # Import prompt templates
            from scraper.prompt_templates import MANUFACTURER_EXTRACTION_PROMPT, MANUFACTURER_DETECTION_PROMPT
            
            # Prepare a truncated version of the content to avoid token limits
            truncated_content = content[:4000] + "..." if len(content) > 4000 else content
            
            # Step 1: Check if this is a manufacturer page
            detection_prompt = MANUFACTURER_DETECTION_PROMPT.format(
                url=url,
                title=title,
                truncated_content=truncated_content
            )
            
            # Use requests to make a direct API call instead of using the client
            headers = {
                "x-api-key": self.anthropic_api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"  # Use the appropriate API version
            }
            
            # First check if it's a manufacturer page
            detection_payload = {
                "model": self.anthropic_sonnet_model,
                "max_tokens": 100,
                "system": "You are a data extraction assistant that ONLY responds in a strict delimited format between # RESPONSE_START and # RESPONSE_END markers.",
                "messages": [
                    {"role": "user", "content": detection_prompt}
                ]
            }
            
            detection_response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=detection_payload
            )
            
            if detection_response.status_code == 200:
                detection_data = detection_response.json()
                if 'content' in detection_data and len(detection_data['content']) > 0:
                    detection_result = detection_data['content'][0]['text']
                    
                    # Parse detection result
                    claude_analyzer = ClaudeAnalyzer(
                        api_key=self.anthropic_api_key,
                        model=self.anthropic_model,
                        sonnet_model=self.anthropic_sonnet_model,
                        logger=self.logger
                    )
                    is_manufacturer = claude_analyzer._parse_delimited_response(detection_result)
                    
                    if not is_manufacturer:
                        self.logger.info(f"Not a manufacturer page: {url}")
                        return
            else:
                self.logger.error(f"Error from Anthropic API during detection: {detection_response.status_code} - {detection_response.text}")
                return
            
            # Step 2: Extract manufacturer data
            extraction_prompt = MANUFACTURER_EXTRACTION_PROMPT.format(
                url=url,
                title=title,
                truncated_content=truncated_content
            )
            
            extraction_payload = {
                "model": self.anthropic_sonnet_model,
                "max_tokens": 4000,
                "system": "You are a data extraction assistant that ONLY responds in a strict delimited format between # RESPONSE_START and # RESPONSE_END markers.",
                "messages": [
                    {"role": "user", "content": extraction_prompt}
                ]
            }
            
            extraction_response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=extraction_payload
            )
            
            # Handle the extraction response
            if extraction_response.status_code == 200:
                extraction_data = extraction_response.json()
                if 'content' in extraction_data and len(extraction_data['content']) > 0:
                    result = extraction_data['content'][0]['text']
                    self.logger.info(f"Raw extraction response:\n{result[:500]}...")
                else:
                    self.logger.error("Invalid response format from Anthropic API")
                    return
            else:
                self.logger.error(f"Error from Anthropic API during extraction: {extraction_response.status_code} - {extraction_response.text}")
                return
            
            # Process the extracted data
            self._process_extracted_data(result, url)
            
        except Exception as e:
            self.logger.error(f"Error analyzing with Claude Sonnet: {str(e)}")
            self._log_to_db("ERROR", f"Error analyzing with Claude Sonnet: {str(e)}")
    
    def _translate_category(self, category, manufacturer_name):
        """Translate a category name directly using Claude API"""
        try:
            # Skip translation if already in Spanish (contains common Spanish words)
            spanish_indicators = ['de', 'para', 'con', 'del', 'y', 'las', 'los']
            if any(word in category.lower().split() for word in spanish_indicators):
                self.logger.info(f"Category '{category}' appears to already be in Spanish, skipping translation")
                return category
                
            # Prepare a prompt for translation that preserves the manufacturer name
            prompt = f"""
            Please translate this product category from English to Spanish: "{category}"
            
            Important rules:
            1. The manufacturer name '{manufacturer_name}' MUST be preserved exactly as-is
            2. Only translate the rest of the text (descriptive words)
            3. Keep proper nouns unchanged
            4. Response format must be ONLY the translated category with no explanation
            """
            
            # Use Claude API directly for translation
            headers = {
                "x-api-key": self.anthropic_api_key,
                "content-type": "application/json",
                "anthropic-version": "2023-06-01"
            }
            
            payload = {
                "model": self.anthropic_sonnet_model,  # Use the faster/smaller model for translations
                "max_tokens": 100,
                "system": "You are a precise translator from English to Spanish. Only respond with the translated text, nothing else.",
                "messages": [
                    {"role": "user", "content": prompt}
                ]
            }
            
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=payload,
                timeout=10  # Short timeout for quick translations
            )
            
            if response.status_code == 200:
                response_data = response.json()
                if 'content' in response_data and len(response_data['content']) > 0:
                    translated_text = response_data['content'][0]['text'].strip()
                    
                    # Verify manufacturer name is still present
                    if manufacturer_name.lower() in translated_text.lower():
                        self.logger.info(f"Successfully translated '{category}' to '{translated_text}'")
                        return translated_text
                    else:
                        self.logger.warning(f"Translation lost manufacturer name: '{translated_text}'. Using original.")
                        return category
            
            self.logger.error(f"Failed to translate category '{category}': {response.status_code}")
            return category  # Return original if translation fails
                
        except Exception as e:
            self.logger.error(f"Error translating category '{category}': {str(e)}")
            return category  # Return original on error
    
    def _process_extracted_data(self, response_text, source_url):
        """Process the extracted manufacturer and category data using the unified parser"""
        try:
            # Log the raw response for debugging
            self.logger.info(f"Raw Claude response for {source_url}: {response_text[:500]}...")
            
            # Use the ClaudeAnalyzer's unified parser
            claude_analyzer = ClaudeAnalyzer(
                api_key=self.anthropic_api_key,
                model=self.anthropic_model,
                sonnet_model=self.anthropic_sonnet_model,
                logger=self.logger
            )
            parsed_data = claude_analyzer._parse_delimited_response(response_text)
            
            # Log the parsed data
            self.logger.info(f"Parsed data: {parsed_data}")
            
            if not parsed_data or 'manufacturers' not in parsed_data or not parsed_data['manufacturers']:
                self.logger.info(f"No manufacturers found in {source_url}")
                return
            
            # ===== NEW APPROACH: PREPARE DATA FOR BATCH PROCESSING =====
            # Process and validate the data before saving
            processed_manufacturers = []
            
            # Process each manufacturer from the parsed data
            for mfr_data in parsed_data['manufacturers']:
                # Get manufacturer name
                manufacturer_name = mfr_data.get('name')
                if not manufacturer_name:
                    self.logger.warning(f"Missing manufacturer name in data: {mfr_data}")
                    continue
                
                # Normalize manufacturer name
                manufacturer_name = manufacturer_name.strip()
                
                # Validate categories - each must include manufacturer name
                categories = mfr_data.get('categories', [])
                if not categories:
                    self.logger.warning(f"No categories found for manufacturer {manufacturer_name}")
                    continue
                
                # Filter categories to ensure they include manufacturer name
                valid_categories = []
                for category in categories:
                    if manufacturer_name.lower() in category.lower():
                        # Translate the category immediately using Claude
                        translated_category = self._translate_category(category.strip(), manufacturer_name)
                        valid_categories.append(translated_category or category.strip())
                    else:
                        self.logger.warning(f"Skipping invalid category '{category}' - must include manufacturer name '{manufacturer_name}'")
                
                if not valid_categories:
                    self.logger.warning(f"No valid categories found for manufacturer {manufacturer_name}")
                    continue
                
                # Get manufacturer website
                manufacturer_website = mfr_data.get('website')
                
                # Skip website if it's the same domain as our target (competitor) website
                if manufacturer_website:
                    target_domain = urlparse(self.config['target_url']).netloc
                    website_domain = urlparse(manufacturer_website).netloc
                    
                    # Check if website is the same domain as our target or contains the target domain
                    if target_domain in website_domain:
                        self.logger.info(f"Skipping website {manufacturer_website} for {manufacturer_name} - matches target domain")
                        manufacturer_website = None
                
                # Only use source_url as a fallback if it's likely a manufacturer website
                if not manufacturer_website:
                    source_domain = urlparse(source_url).netloc
                    target_domain = urlparse(self.config['target_url']).netloc
                    
                    if manufacturer_name.lower() in source_domain.lower() and source_domain != target_domain:
                        manufacturer_website = source_url
                    else:
                        manufacturer_website = None
                
                # Add this manufacturer to our processed list
                processed_manufacturers.append({
                    'name': manufacturer_name,
                    'website': manufacturer_website,
                    'categories': valid_categories
                })
            
            # If we have manufacturers to process, save them directly to the database
            if processed_manufacturers:
                self.logger.info(f"Saving {len(processed_manufacturers)} manufacturers from {source_url} directly to database")
                
                # Process each manufacturer and save directly to database
                # using our database manager with proper locking
                saved_count = 0
                for mfr_data in processed_manufacturers:
                    try:
                        # Use the database manager session context to handle locking
                        with self.db_manager.session() as session:
                            # Check if manufacturer already exists
                            existing_manufacturer = session.query(Manufacturer).filter(
                                func.lower(Manufacturer.name) == func.lower(mfr_data['name'])
                            ).first()
                            
                            if existing_manufacturer:
                                self.logger.info(f"Manufacturer {mfr_data['name']} already exists, updating")
                                manufacturer = existing_manufacturer
                                # Update website if needed
                                if mfr_data['website'] and not manufacturer.website:
                                    manufacturer.website = mfr_data['website']
                            else:
                                # Create new manufacturer
                                manufacturer = Manufacturer(
                                    name=mfr_data['name'],
                                    website=mfr_data['website']
                                )
                                session.add(manufacturer)
                                session.flush()  # Get manufacturer ID without committing
                            
                            # Process categories - all translated already
                            for category_name in mfr_data['categories']:
                                # Check if category already exists for this manufacturer
                                existing_category = None
                                for cat in manufacturer.categories:
                                    if func.lower(cat.name) == func.lower(category_name):
                                        existing_category = cat
                                        break
                                
                                if not existing_category:
                                    # Check if category exists in general
                                    existing_category = session.query(Category).filter(
                                        func.lower(Category.name) == func.lower(category_name)
                                    ).first()
                                    
                                    if not existing_category:
                                        # Create new category
                                        existing_category = Category(name=category_name)
                                        session.add(existing_category)
                                        session.flush()
                                    
                                    # Add category to manufacturer
                                    manufacturer.categories.append(existing_category)
                            
                            # No explicit commit needed - the context manager handles it
                        
                        # If we got here, the transaction was successful
                        saved_count += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error saving manufacturer {mfr_data['name']}: {str(e)}")
                
                self.logger.info(f"Successfully saved {saved_count} of {len(processed_manufacturers)} manufacturers to database")
                self._log_to_db("INFO", f"Saved {saved_count} manufacturers from {source_url} to database")
            else:
                self.logger.info(f"No valid manufacturers found in {source_url}")
            
        except Exception as e:
            self.logger.error(f"Unhandled error in _process_extracted_data: {str(e)}")
            self._log_to_db("ERROR", f"Unhandled error in _process_extracted_data: {str(e)}")
            # No need to explicitly roll back - database manager's context manager handles this
    
    def process_batch(self, max_pages=0, max_runtime_minutes=0):
        """
        Process a batch of URLs with time and page limits.
        
        Args:
            max_pages (int, optional): Maximum number of pages to process in this batch.
                If 0, no limit is applied.
            max_runtime_minutes (int, optional): Maximum runtime in minutes.
                If 0, runs until max_pages is reached.
                
        Returns:
            dict: Statistics about the batch processing.
        """
        self.logger.info(f"Starting batch processing with max_pages={max_pages}, max_runtime_minutes={max_runtime_minutes}")
        
        # Start a new batch in the statistics manager
        self.statistics_manager.start_batch()
        
        # Initialize the URL queue with the target URL and unvisited URLs from database if empty
        if not self.url_queue:
            self._load_initial_urls()
        
        # Calculate end time if max_runtime_minutes is specified
        end_time = None
        if max_runtime_minutes > 0:
            end_time = time.time() + (max_runtime_minutes * 60)
            self.logger.info(f"Batch will run until {time.strftime('%H:%M:%S', time.localtime(end_time))}")
        
        # Process URLs until the queue is empty, we reach the maximum number of pages,
        # or we exceed the maximum runtime
        pages_crawled = 0
        while self.url_queue and (max_pages == 0 or pages_crawled < max_pages):
            # Check if shutdown was requested (either locally or via global flag)
            if self.check_shutdown_requested():
                self.logger.info("Shutdown requested, stopping batch processing")
                break
                
            # Check if we've exceeded the maximum runtime
            if end_time and time.time() >= end_time:
                self.logger.info(f"Reached maximum runtime of {max_runtime_minutes} minutes")
                break
                
            # Get the next URL from the queue
            url = self.url_queue.pop(0)
            
            # Skip if already visited
            if url in self.visited_urls:
                continue
                
            # Skip if URL is in excluded patterns
            if self._is_excluded_url(url):
                self.logger.debug(f"Skipping excluded URL: {url}")
                continue
            
            try:
                # Process the URL
                self.logger.info(f"Processing URL: {url} [Queue: {len(self.url_queue)}]")
                
                # Check if URL exists in database
                url_exists, is_visited = self._check_url_in_database(url)
                
                # Skip if already visited
                if is_visited:
                    continue
                    
                # Fetch the page content
                response = self._fetch_url(url)
                if not response:
                    continue
                    
                # Parse the page
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract page title for logging
                title = soup.title.text.strip() if soup.title else "No title"
                self.logger.info(f"Page title: {title}")
                
                # Check if this is a manufacturer page using Claude
                is_manufacturer, manufacturer_data = self._check_manufacturer_page(url, soup)
                
                # If it's a manufacturer page, extract and save the data
                if is_manufacturer and manufacturer_data:
                    self.logger.info(f"Found manufacturer page: {url}")
                    self.logger.info(f"Manufacturer data: {manufacturer_data}")
                    
                    # Save manufacturer to database
                    manufacturer_id = self._save_manufacturer(manufacturer_data)
                    
                    # Update statistics
                    self.statistics_manager.increment_stat('manufacturers_found')
                    
                    # Save categories if available
                    if 'categories' in manufacturer_data and manufacturer_data['categories']:
                        for category_name in manufacturer_data['categories']:
                            category_id = self._save_category(category_name)
                            if category_id and manufacturer_id:
                                self._link_manufacturer_to_category(manufacturer_id, category_id)
                                # Update statistics
                                self.statistics_manager.increment_stat('categories_found')
                
                # Extract links from the page
                links = self._extract_links(soup, url)
                
                # Add new links to the queue
                for link in links:
                    if link not in self.visited_urls and link not in self.url_queue:
                        self.url_queue.append(link)
                
                # Mark URL as visited
                self.visited_urls.add(url)
                self._mark_url_visited(url, is_manufacturer)
                
                # Increment pages crawled counter
                pages_crawled += 1
                
                # Update stats
                self.statistics_manager.increment_stat('pages_crawled')
                
                # Log progress every 10 pages
                if pages_crawled % 10 == 0:
                    self.logger.info(f"Progress: {pages_crawled} pages crawled, {len(self.url_queue)} URLs in queue")
            except Exception as e:
                self.logger.error(f"Error processing URL {url}: {str(e)}")
                import traceback
                self.logger.debug(f"Crawling error details: {traceback.format_exc()}")
        
        # End the batch in the statistics manager
        self.statistics_manager.end_batch()
        
        # Add shutdown status to stats
        if self.check_shutdown_requested():
            self.statistics_manager.update_stat('status', 'interrupted')
            status_message = "Crawling interrupted by user"
        else:
            self.statistics_manager.update_stat('status', 'completed')
            status_message = f"Crawling finished. Processed {pages_crawled} pages."
        
        self.logger.info(status_message)
        
        # Return the statistics
        return self.statistics_manager.get_all_stats()
    
    def get_statistics(self):
        """Get statistics about the scraping process"""
        return self.statistics_manager.get_batch_stats()
    
    def get_top_manufacturers_by_category_count(self, limit=5):
        """Get the top manufacturers by category count"""
        try:
            # Query to count categories per manufacturer using our database manager
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
        """Close the scraper and release resources"""
        self.logger.info("Closing scraper and database connections")
        
        # If we have an active session that wasn't properly closed, mark it as interrupted
        if hasattr(self, 'session_id') and self.session_id is not None:
            try:
                with self.db_manager.session() as session:
                    scraper_session = session.query(ScraperSession).filter_by(id=self.session_id).first()
                    if scraper_session and scraper_session.status == 'running':
                        scraper_session.status = 'interrupted'
                        scraper_session.end_time = datetime.datetime.now()
                        self.logger.info(f"Marked session {self.session_id} as interrupted during close")
            except Exception as e:
                self.logger.error(f"Error updating session status during close: {str(e)}")
        
        # Explicitly call the database manager's shutdown method to ensure
        # all connections are properly closed
        try:
            self.db_manager.shutdown()
            self.logger.info("Database connections closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing database connections: {str(e)}")
        
        self.logger.info("Scraper closed successfully")
