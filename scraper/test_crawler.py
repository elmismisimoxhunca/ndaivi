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
    # Ensure the data directory exists
    os.makedirs('scraper/data', exist_ok=True)
    
    # Load configuration
    config_manager = ConfigManager()
    crawler_config = config_manager.get_config('web_crawler')
    
    if not crawler_config:
        logger.error("Failed to load web crawler configuration")
        return
    
    # Create and configure the crawler
    start_url = "https://example.com"
    
    logger.info(f"Initializing crawler with start URL: {start_url}")
    logger.info(f"Using database: {crawler_config.get('db_path', 'default.db')}")
    
    crawler = WebCrawler(
        config=crawler_config,
        stats_callback=stats_callback
    )
    
    # Add the start URL
    crawler.add_url(start_url)
    
    # Start crawling
    try:
        logger.info("Starting crawl process")
        crawler.crawl(max_urls=10)  # Limit to 10 URLs for testing
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
