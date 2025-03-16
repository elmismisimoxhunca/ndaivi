#!/usr/bin/env python3

import os
import sys
import time
import yaml
import logging
import requests
import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.schema import init_db, Category, Manufacturer, CrawlStatus, ScraperLog
from scraper.claude_analyzer import ClaudeAnalyzer

class ClaudeScraper:
    """A web scraper that uses Claude AI to identify and extract manufacturer information"""
    
    def __init__(self, config):
        """Initialize the scraper with configuration"""
        self.config = config
        self.logger = logging.getLogger('claude_scraper')
        
        # Initialize database
        self._setup_database()
        
        # Initialize AI clients
        self._setup_ai_clients()
        
        # Initialize crawling variables
        self.visited_urls = set()
        self.url_queue = []
        self.sitemap = {}
        self.start_time = None
    
    def _setup_database(self):
        """Set up the database connection using DatabaseManager"""
        db_path = self.config['database']['path']
        
        # Create directory if it doesn't exist
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
            
        # Get the database manager singleton instead of creating our own session
        from database.db_manager import get_db_manager
        self.db_manager = get_db_manager(db_path)
        
        # Ensure the database manager is actually initialized
        if not hasattr(self.db_manager, '_initialized') or not self.db_manager._initialized:
            self.db_manager.initialize(db_path)
        
        self.logger.info(f"Database manager initialized for {db_path}")
    
    def _setup_ai_clients(self):
        """Initialize AI API clients"""
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
        self.anthropic_sonnet_model = anthropic_config.get('sonnet_model')
        
        # Initialize the Claude analyzer
        self.claude_analyzer = ClaudeAnalyzer(
            api_key=self.anthropic_api_key,
            model=self.anthropic_model,
            sonnet_model=self.anthropic_sonnet_model
        )
        
        self.logger.info("Anthropic API configuration loaded successfully")
    
    def run(self):
        """Run the scraper"""
        self.logger.info("Starting crawling process")
        
        # Set start time
        self.start_time = time.time()
        
        # Use the target_url as the primary initial URL
        target_url = self.config['target_url']
        self.logger.info(f"Using target URL from config: {target_url}")
        
        # Initialize URL queue with the target URL and any additional initial URLs
        initial_urls = [target_url]
        
        # Add any additional initial URLs from the crawling section if they're different from the target URL
        if 'initial_urls' in self.config['crawling']:
            additional_urls = [url for url in self.config['crawling']['initial_urls'] if url != target_url]
            if additional_urls:
                self.logger.info(f"Adding {len(additional_urls)} additional initial URLs")
                initial_urls.extend(additional_urls)
        
        self.url_queue.extend([(url, 0) for url in initial_urls])  # (url, depth)
        
        self.logger.info(f"Added {len(initial_urls)} initial URLs to the queue")
        
        # Get crawling limits
        max_pages = self.config['crawling']['max_pages_per_run']
        delay = self.config['crawling']['delay_between_requests']
        
        # Process URLs until queue is empty or max_pages is reached
        pages_crawled = 0
        while self.url_queue and (max_pages == 0 or pages_crawled < max_pages):
            # Get the next URL and depth from the queue
            url, depth = self.url_queue.pop(0)
            
            # Skip if already visited
            if url in self.visited_urls:
                continue
            
            # Mark as visited
            self.visited_urls.add(url)
            
            # Log the crawling
            self.logger.info(f"Crawling {url} at depth {depth}")
            
            # Fetch and process the page
            soup, new_urls = self._fetch_page(url)
            if soup:
                # Process the page
                self._process_page(url, soup, depth)
                
                # Add new URLs to the queue if not at max depth
                max_depth = self.config['crawling'].get('max_depth', 3)
                if depth < max_depth:
                    for new_url in new_urls:
                        if new_url not in self.visited_urls:
                            self.url_queue.append((new_url, depth + 1))
                
                # Update sitemap
                if self.config['crawling']['build_sitemap']:
                    self.sitemap[url] = {
                        'depth': depth,
                        'title': soup.title.string if soup.title else "",
                        'links': new_urls
                    }
            
            # Increment pages crawled
            pages_crawled += 1
            
            # Delay between requests
            time.sleep(delay)
        
        # Save sitemap
        if self.config['crawling']['build_sitemap']:
            self._save_sitemap()
        
        # Log crawling statistics
        duration = time.time() - self.start_time
        self.logger.info(f"Crawling finished. Processed {pages_crawled} pages.")
        
        # Print statistics
        self._print_statistics(pages_crawled, duration)
    
    def _fetch_page(self, url):
        """Fetch a page and return the soup and new URLs"""
        try:
            # Set up headers
            headers = {
                'User-Agent': self.config['crawling']['user_agent']
            }
            
            # Make the request
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # Parse the HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract links
            links = soup.find_all('a', href=True)
            new_urls = []
            
            for link in links:
                href = link['href']
                absolute_url = urljoin(url, href)
                
                # Skip fragments, mailto, etc.
                if not absolute_url.startswith('http'):
                    continue
                
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
        content = soup.get_text(separator=" ", strip=True)
        
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
        
        # Update crawl status using database manager for thread-safe access
        with self.db_manager.session() as session:
            status = session.query(CrawlStatus).filter_by(url=url).first()
            if not status:
                status = CrawlStatus(
                    url=url,
                    depth=depth,
                    last_visited=datetime.datetime.now(),
                    is_manufacturer_page=False
                )
                session.add(status)
            else:
                status.last_visited = datetime.datetime.now()
            
            if positive_match and not negative_match:
                self.logger.info(f"Found potential manufacturer page: {url}")
                
                # Use AI to analyze the page
                try:
                    # First check if this is a manufacturer page
                    is_manufacturer = self.claude_analyzer.is_manufacturer_page(url, title, content)
                    
                    if is_manufacturer:
                        # Extract detailed information using Claude Sonnet
                        data = self.claude_analyzer.extract_manufacturers(url, title, content)
                        if data:
                            # We need to pass the session to _process_extracted_data
                            # so it can use the same transaction
                            self._process_extracted_data(data, url, session)
                        
                        if status:
                            status.is_manufacturer_page = True
                except Exception as e:
                    self.logger.error(f"Error during AI analysis: {str(e)}")
            
            # No explicit commit needed - the context manager handles it
    
    def _process_extracted_data(self, data, source_url, session=None):
        """Process the extracted manufacturer and category data
        
        Args:
            data: The extracted data from Claude
            source_url: URL source of the data
            session: An optional active database session (if None, we'll create a new one)
        """
        try:
            if 'manufacturers' not in data or not data['manufacturers']:
                self.logger.info(f"No manufacturers found in {source_url}")
                return
            
            # Determine if we need to create our own session or use the provided one
            # This allows the method to be called both from within another session
            # (as in _process_url) or standalone
            use_own_session = session is None
            
            try:
                # Create a session if none was provided
                if use_own_session:
                    session = self.db_manager.session().__enter__()
                
                for mfr_data in data['manufacturers']:
                    # Get or create manufacturer
                    manufacturer_name = mfr_data.get('name')
                    if not manufacturer_name:
                        continue
                    
                    # Normalize manufacturer name
                    manufacturer_name = manufacturer_name.strip()
                    
                    manufacturer = session.query(Manufacturer).filter_by(name=manufacturer_name).first()
                    if not manufacturer:
                        manufacturer = Manufacturer(
                            name=manufacturer_name,
                            website=mfr_data.get('website')
                        )
                        session.add(manufacturer)
                        session.flush()  # Get the ID without committing
                        self.logger.info(f"Added new manufacturer: {manufacturer_name}")
                    elif not manufacturer.website and mfr_data.get('website'):
                        manufacturer.website = mfr_data.get('website')
                    
                    # Process categories
                    categories = mfr_data.get('categories', [])
                    for category_name in categories:
                        if not category_name:
                            continue
                        
                        # Normalize category name
                        category_name = category_name.strip()
                        
                        # Check if category exists
                        category = session.query(Category).filter_by(
                            name=category_name,
                            manufacturer_id=manufacturer.id
                        ).first()
                        
                        if not category:
                            category = Category(
                                name=category_name,
                                manufacturer_id=manufacturer.id
                            )
                            session.add(category)
                            self.logger.info(f"Added new category: {category_name} for {manufacturer_name}")
                
                # Only commit if we created our own session
                # Otherwise the caller will handle the commit
                if use_own_session:
                    session.commit()
            finally:
                # Clean up our session if we created one
                if use_own_session and session:
                    self.db_manager.session().__exit__(None, None, None)
            
        except Exception as e:
            self.logger.error(f"Error processing extracted data: {str(e)}")
            # Rollback on error only if we created our own session
            # Otherwise the caller will handle the rollback
            if use_own_session and session:
                session.rollback()
    
    def _save_sitemap(self):
        """Save the sitemap to JSON and XML files"""
        import json
        import xml.etree.ElementTree as ET
        from xml.dom import minidom
        
        # Create output directory if it doesn't exist
        output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        # Save JSON sitemap
        json_path = os.path.join(output_dir, 'sitemap.json')
        with open(json_path, 'w') as f:
            json.dump(self.sitemap, f, indent=2)
        
        # Create XML sitemap
        urlset = ET.Element('urlset', xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")
        
        for url, data in self.sitemap.items():
            url_element = ET.SubElement(urlset, 'url')
            loc = ET.SubElement(url_element, 'loc')
            loc.text = url
            
            lastmod = ET.SubElement(url_element, 'lastmod')
            lastmod.text = datetime.datetime.now().strftime("%Y-%m-%d")
            
            priority = ET.SubElement(url_element, 'priority')
            # Higher priority for lower depth
            priority.text = str(max(0.1, 1.0 - data['depth'] * 0.2))
        
        # Save XML sitemap
        xml_str = minidom.parseString(ET.tostring(urlset)).toprettyxml(indent="  ")
        xml_path = os.path.join(output_dir, 'sitemap.xml')
        with open(xml_path, 'w') as f:
            f.write(xml_str)
        
        self.logger.info(f"Sitemap saved to {json_path} and {xml_path}")
    
    def _print_statistics(self, pages_crawled, duration):
        """Print crawling statistics"""
        # Get counts from database
        total_urls = self.session.query(CrawlStatus).count()
        visited_urls = self.session.query(CrawlStatus).filter(CrawlStatus.last_visited != None).count()
        manufacturer_pages = self.session.query(CrawlStatus).filter_by(is_manufacturer_page=True).count()
        manufacturers = self.session.query(Manufacturer).count()
        categories = self.session.query(Category).count()
        manufacturers_with_website = self.session.query(Manufacturer).filter(Manufacturer.website != None).count()
        
        # Format duration
        hours, remainder = divmod(int(duration), 3600)
        minutes, seconds = divmod(remainder, 60)
        duration_str = f"{hours}h {minutes}m {seconds}s"
        
        # Print statistics
        print("\n" + "=" * 60)
        print("COMPETITOR SCRAPER STATISTICS")
        print("=" * 60 + "\n")
        
        print("CRAWLING STATISTICS:")
        print(f"Total URLs in database: {total_urls}")
        print(f"Visited URLs: {visited_urls}")
        print(f"URLs remaining in queue: {len(self.url_queue)}")
        print(f"Crawling duration: {duration_str}")
        print(f"Pages crawled in last run: {pages_crawled}")
        
        print("\nSITEMAP STATISTICS:")
        print(f"Total pages in sitemap: {len(self.sitemap)}")
        print("Pages by depth:")
        depth_counts = {}
        for url, data in self.sitemap.items():
            depth = data['depth']
            depth_counts[depth] = depth_counts.get(depth, 0) + 1
        for depth, count in sorted(depth_counts.items()):
            print(f"  Depth {depth}: {count} pages")
        
        print("\nDATA EXTRACTION STATISTICS:")
        print(f"Manufacturer pages identified: {manufacturer_pages}")
        print(f"Manufacturers extracted: {manufacturers}")
        print(f"Categories extracted: {categories}")
        print(f"Manufacturers with website: {manufacturers_with_website}")
        print("=" * 60)
