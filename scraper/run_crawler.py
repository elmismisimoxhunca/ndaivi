#!/usr/bin/env python3
"""
Run script for the NDAIVI web crawler.
This script provides a command-line interface to run the web crawler with
various options and configurations.
"""

import os
import sys
import argparse
import logging
import time
from typing import Dict, Any, List, Optional

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
        logging.FileHandler('logs/crawler.log')
    ]
)

logger = logging.getLogger('crawler_runner')

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

def run_crawler(
    start_url: str,
    max_urls: Optional[int] = None,
    max_time: Optional[int] = None,
    config_overrides: Optional[Dict[str, Any]] = None
) -> None:
    """
    Run the web crawler with the specified parameters.
    
    Args:
        start_url: URL to start crawling from
        max_urls: Maximum number of URLs to process (None for unlimited)
        max_time: Maximum time to run in seconds (None for unlimited)
        config_overrides: Dictionary of configuration overrides
    """
    # Ensure the data directory exists
    os.makedirs(os.path.join('scraper', 'data'), exist_ok=True)
    
    # Load configuration
    config_manager = ConfigManager()
    crawler_config = config_manager.get('web_crawler', {})
    
    if not crawler_config:
        logger.error("Failed to load web crawler configuration")
        return
    
    # Apply configuration overrides
    if config_overrides:
        for key, value in config_overrides.items():
            crawler_config[key] = value
    
    # Create and configure the crawler
    logger.info(f"Initializing crawler with start URL: {start_url}")
    logger.info(f"Using database: {crawler_config.get('db_path', 'default.db')}")
    
    # Resolve relative database path if needed
    db_path = crawler_config.get('db_path')
    if db_path and not os.path.isabs(db_path):
        crawler_config['db_path'] = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', db_path))
    
    crawler = WebCrawler(
        config=crawler_config,
        stats_callback=stats_callback
    )
    
    # Add the start URL
    crawler.add_url(start_url)
    
    # Start crawling
    start_time = time.time()
    try:
        logger.info("Starting crawl process")
        
        if max_time is not None:
            end_time = start_time + max_time
            logger.info(f"Crawl will stop after {max_time} seconds")
            
            # Run with time limit
            while time.time() < end_time:
                if not crawler.process_next_url():
                    logger.info("No more URLs to process")
                    break
                
                # Check if we've reached the URL limit
                if max_urls is not None and crawler.get_stats()['urls_processed'] >= max_urls:
                    logger.info(f"Reached maximum URLs limit: {max_urls}")
                    break
        else:
            # Run with URL limit only
            crawler.crawl(max_urls=max_urls)
            
    except KeyboardInterrupt:
        logger.info("Crawl interrupted by user")
    except Exception as e:
        logger.exception(f"Error during crawl: {e}")
    finally:
        # Get final stats
        elapsed_time = time.time() - start_time
        stats = crawler.get_stats()
        
        logger.info(f"Crawl completed in {elapsed_time:.2f} seconds")
        logger.info("Final Crawler Stats:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        
        # Close the crawler
        crawler.close()
        logger.info("Crawler closed")

def main():
    """Main function to parse arguments and run the crawler."""
    parser = argparse.ArgumentParser(description='Run the NDAIVI web crawler')
    parser.add_argument('start_url', help='URL to start crawling from')
    parser.add_argument('--max-urls', type=int, help='Maximum number of URLs to process')
    parser.add_argument('--max-time', type=int, help='Maximum time to run in seconds')
    parser.add_argument('--max-depth', type=int, help='Maximum crawl depth')
    parser.add_argument('--single-domain', action='store_true', help='Restrict to the start domain only')
    parser.add_argument('--db-path', help='Path to the crawler database')
    parser.add_argument('--delay', type=float, help='Delay between requests in seconds')
    parser.add_argument('--user-agent', help='User agent string to use')
    parser.add_argument('--allowed-domains', nargs='+', help='List of allowed domains')
    parser.add_argument('--disallowed-domains', nargs='+', help='List of disallowed domains')
    
    args = parser.parse_args()
    
    # Build configuration overrides
    config_overrides = {}
    
    if args.max_depth is not None:
        config_overrides['max_depth'] = args.max_depth
    
    if args.single_domain:
        config_overrides['single_domain_mode'] = True
        config_overrides['restrict_to_start_domain'] = True
    
    if args.db_path:
        config_overrides['db_path'] = args.db_path
    
    if args.delay is not None:
        config_overrides['request_delay'] = args.delay
    
    if args.user_agent:
        config_overrides['user_agent'] = args.user_agent
    
    if args.allowed_domains:
        config_overrides['allowed_domains'] = args.allowed_domains
    
    if args.disallowed_domains:
        config_overrides['disallowed_domains'] = args.disallowed_domains
    
    # Run the crawler
    run_crawler(
        start_url=args.start_url,
        max_urls=args.max_urls,
        max_time=args.max_time,
        config_overrides=config_overrides
    )

if __name__ == "__main__":
    main()
