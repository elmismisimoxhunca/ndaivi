#!/usr/bin/env python3

"""
Minimal Web Crawler Test for NDAIVI

This script provides a simple crawler implementation that follows the WindsurfRules
requirements. It specifically tests the ability to crawl a website and write to SQLite.
"""

import os
import sys
import time
import logging
import sqlite3
import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/ndaivimanuales/logs/test_crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('ndaivi-test-crawler')

# Configuration
TARGET_WEBSITE = os.environ.get('NDAIVI_TARGET_WEBSITE', 'manualslib.com')
MAX_URLS = int(os.environ.get('NDAIVI_MAX_URLS', '10'))  # Default 10 for testing
DB_PATH = os.environ.get('NDAIVI_DB_PATH', '/var/ndaivimanuales/data/crawler.db')

class SimpleCrawler:
    """Simple web crawler for testing the NDAIVI system"""
    
    def __init__(self):
        """Initialize the crawler"""
        self.visited_urls = set()
        self.urls_to_visit = []
        self.max_urls = MAX_URLS
        self.db_connection = None
        self.db_cursor = None
        
    def init_db(self):
        """Initialize the SQLite database"""
        # Ensure directory exists
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        
        # Connect to database
        self.db_connection = sqlite3.connect(DB_PATH)
        self.db_cursor = self.db_connection.cursor()
        
        # Create tables if not exist
        self.db_cursor.execute("""
        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            domain TEXT,
            status_code INTEGER,
            crawled_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        self.db_connection.commit()
        logger.info(f"Database initialized at {DB_PATH}")
        
    def add_url(self, url):
        """Add URL to the crawler queue"""
        if url not in self.visited_urls and url not in self.urls_to_visit:
            self.urls_to_visit.append(url)
            logger.debug(f"Added URL to queue: {url}")
            
    def save_to_db(self, url, status_code):
        """Save crawled URL to the database"""
        try:
            domain = urlparse(url).netloc
            self.db_cursor.execute(
                "INSERT OR IGNORE INTO pages (url, domain, status_code) VALUES (?, ?, ?)",
                (url, domain, status_code)
            )
            self.db_connection.commit()
            logger.info(f"Saved to database: {url} (Status: {status_code})")
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
    
    def extract_links(self, url, html):
        """Extract links from HTML content and add to database"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            base_url = url
            
            links_found = 0
            links_added_to_db = 0
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                absolute_url = urljoin(base_url, href)
                
                # Only process URLs from the same domain
                if urlparse(absolute_url).netloc == urlparse(base_url).netloc:
                    # Add to crawl queue
                    self.add_url(absolute_url)
                    links_found += 1
                    
                    # Add to database with NULL status (will be updated when crawled)
                    try:
                        domain = urlparse(absolute_url).netloc
                        self.db_cursor.execute(
                            "INSERT OR IGNORE INTO pages (url, domain, status_code) VALUES (?, ?, NULL)",
                            (absolute_url, domain)
                        )
                        if self.db_cursor.rowcount > 0:
                            links_added_to_db += 1
                    except sqlite3.IntegrityError:
                        # URL already exists in database
                        pass
            
            # Commit all the new URLs at once for efficiency
            self.db_connection.commit()
            logger.info(f"Extracted {links_found} links from {url}, added {links_added_to_db} new URLs to database")
            
        except Exception as e:
            logger.error(f"Error extracting links from {url}: {e}")
    
    def crawl_url(self, url):
        """Crawl a URL and extract content"""
        try:
            logger.info(f"Crawling URL: {url}")
            
            # Send HTTP request
            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; NDaiviTestBot/1.0)'
            }
            response = requests.get(url, timeout=30, headers=headers)
            
            # Process response
            self.visited_urls.add(url)
            self.save_to_db(url, response.status_code)
            
            # Only extract links from successful responses
            if response.status_code == 200:
                self.extract_links(url, response.text)
                
            return True
            
        except Exception as e:
            logger.error(f"Error crawling {url}: {e}")
            return False
    
    def start(self):
        """Start the crawling process"""
        # Initialize database
        self.init_db()
        
        # Start with the target website
        start_url = f"https://{TARGET_WEBSITE}"
        self.add_url(start_url)
        
        logger.info(f"Starting crawl of {start_url} with max_urls={self.max_urls}")
        crawled_count = 0
        
        # Main crawling loop
        try:
            while self.urls_to_visit and crawled_count < self.max_urls:
                # Get the next URL to crawl
                url = self.urls_to_visit.pop(0)
                
                # Skip if already visited
                if url in self.visited_urls:
                    continue
                    
                # Crawl the URL
                success = self.crawl_url(url)
                if success:
                    crawled_count += 1
                    
                # Add a small delay to avoid overwhelming the server
                time.sleep(1)
                
            logger.info(f"Crawling complete. Processed {crawled_count} URLs.")
            
        except KeyboardInterrupt:
            logger.info("Crawling stopped by user")
        except Exception as e:
            logger.error(f"Error during crawling: {e}")
        finally:
            # Close database connection
            if self.db_connection:
                self.db_connection.close()
                
        return crawled_count

def main():
    """Run the crawler test"""
    logger.info("=== NDAIVI Minimal Crawler Test ===")
    
    # Create and run the crawler
    crawler = SimpleCrawler()
    urls_crawled = crawler.start()
    
    # Report results
    if urls_crawled > 0:
        logger.info(f"TEST PASSED: Successfully crawled {urls_crawled} URLs")
        return 0
    else:
        logger.error("TEST FAILED: No URLs were crawled")
        return 1
    
if __name__ == "__main__":
    sys.exit(main())
