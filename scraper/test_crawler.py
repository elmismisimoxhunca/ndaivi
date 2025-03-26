#!/usr/bin/env python3
"""
Test script for the web crawler with database integration.
This script tests the functionality of the web crawler with the new database
and configuration features.
"""

import os
import sys
import logging
import time
import argparse
from typing import Dict, Any

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scraper.web_crawler import WebCrawler
from utils.config_manager import ConfigManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/crawler_test.log')
    ]
)

# Set more restrictive log levels for noisy modules
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('bs4').setLevel(logging.WARNING)
logging.getLogger('content_extractor').setLevel(logging.INFO)

# Create a filter to prevent logging of large content
class ContentFilter(logging.Filter):
    def filter(self, record):
        if hasattr(record, 'msg') and isinstance(record.msg, str) and len(record.msg) > 500:
            record.msg = record.msg[:500] + '... [content truncated]'
        return True

# Apply filter to all handlers
for handler in logging.root.handlers:
    handler.addFilter(ContentFilter())

logger = logging.getLogger('crawler_test')

def stats_callback(stats: Dict[str, Any]) -> None:
    """
    Callback function for crawler stats updates.
    
    Args:
        stats: Dictionary containing crawler statistics
    """
    logger.info("Crawler Stats Update:")
    logger.info(f"  URLs Processed: {stats.get('urls_processed', 0)}")
    logger.info(f"  URLs Queued: {stats.get('urls_queued', 0)}")
    logger.info(f"  URLs Failed: {stats.get('urls_failed', 0)}")
    logger.info(f"  Domains Discovered: {stats.get('domains_discovered', 0)}")
    logger.info(f"  Average Processing Time: {stats.get('avg_processing_time', 0):.2f}s")
    logger.info(f"  Elapsed Time: {stats.get('elapsed_time', 0):.2f}s")

def main():
    """Main function to test the web crawler."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Test the web crawler with a specific URL')
    parser.add_argument('start_url', nargs='?', default="https://example.com", 
                        help='URL to start crawling from (default: https://example.com)')
    parser.add_argument('--max-urls', type=int, default=10, 
                        help='Maximum number of URLs to crawl (default: 10)')
    parser.add_argument('--max-depth', type=int, default=None, 
                        help='Maximum depth to crawl (default: use config value)')
    parser.add_argument('--delay', type=float, default=None, 
                        help='Delay between requests in seconds (default: use config value)')
    parser.add_argument('--timeout', type=int, default=None, 
                        help='Request timeout in seconds (default: use config value)')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                        default='INFO', help='Logging level (default: INFO)')
    parser.add_argument('--clean', action='store_true',
                        help='Clean database before starting (default: False)')
    
    args = parser.parse_args()
    
    # Set log level
    logger.setLevel(getattr(logging, args.log_level))
    
    # Ensure the data directory exists
    data_dir = os.path.abspath('scraper/data')
    logs_dir = os.path.abspath('logs')
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)
    
    # Load configuration
    config_manager = ConfigManager()
    crawler_config = config_manager.get('web_crawler')
    
    if not crawler_config:
        logger.error("Failed to load web crawler configuration")
        return
    
    # Set absolute database path
    if 'db_path' in crawler_config:
        if not os.path.isabs(crawler_config['db_path']):
            crawler_config['db_path'] = os.path.abspath(crawler_config['db_path'])
    else:
        crawler_config['db_path'] = os.path.join(data_dir, 'crawler.db')
    
    # Clean database if requested
    if args.clean and os.path.exists(crawler_config['db_path']):
        try:
            logger.info(f"Cleaning database: {crawler_config['db_path']}")
            os.remove(crawler_config['db_path'])
            # Also remove journal files
            journal_file = f"{crawler_config['db_path']}-journal"
            if os.path.exists(journal_file):
                os.remove(journal_file)
            logger.info("Database cleaned successfully")
        except Exception as e:
            logger.error(f"Failed to clean database: {e}")
    
    # Override config with command line arguments if provided
    if args.max_depth is not None:
        crawler_config['max_depth'] = args.max_depth
    if args.delay is not None:
        crawler_config['request_delay'] = args.delay
    if args.timeout is not None:
        crawler_config['timeout'] = args.timeout
    
    # Set max URLs from command line argument
    crawler_config['max_urls'] = args.max_urls
    
    # Ensure domain restriction is enabled
    crawler_config['restrict_to_start_domain'] = True
    crawler_config['allow_subdomains'] = False  # Only crawl exact domain, not subdomains
    
    # Create and configure the crawler
    start_url = args.start_url
    
    logger.info(f"Initializing crawler with start URL: {start_url}")
    logger.info(f"Using database: {crawler_config.get('db_path', 'default.db')}")
    logger.info(f"Max depth: {crawler_config.get('max_depth')}")
    logger.info(f"Request delay: {crawler_config.get('request_delay')}s")
    logger.info(f"Timeout: {crawler_config.get('timeout')}s")
    
    # Extract the domain from the start URL
    from urllib.parse import urlparse
    parsed_url = urlparse(start_url)
    start_domain = parsed_url.netloc
    
    # Add the start domain to allowed domains if not already present
    if 'allowed_domains' not in crawler_config or not crawler_config['allowed_domains']:
        crawler_config['allowed_domains'] = [start_domain]
    elif start_domain not in crawler_config['allowed_domains']:
        crawler_config['allowed_domains'].append(start_domain)
    
    crawler = WebCrawler(
        config=crawler_config,
        stats_callback=stats_callback
    )
    
    # Set the start domain explicitly
    crawler.start_domain = start_domain
    
    # Add the start URL
    crawler.add_url(start_url)
    
    # Start crawling
    try:
        logger.info(f"Starting crawl process (max URLs: {args.max_urls})")
        crawler.crawl(max_urls=args.max_urls)
    except KeyboardInterrupt:
        logger.info("Crawl interrupted by user")
    except Exception as e:
        logger.exception(f"Error during crawl: {e}")
    finally:
        # Get final stats
        stats = crawler.get_stats()
        logger.info("Final Crawler Stats:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        
        # Close the crawler
        crawler.close()
        logger.info("Crawler closed")

if __name__ == "__main__":
    main()
