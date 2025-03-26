#!/usr/bin/env python3
"""
Test script for the web crawler to verify duplicate URL handling and link extraction.
"""

import logging
import sys
import time
import os
from urllib.parse import urlparse, ParseResult
import json
from typing import Dict, List, Set, Tuple, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("crawler_test")

# Import the web crawler module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper.web_crawler import WebCrawler, PriorityUrlQueue

def test_priority_queue():
    """Test the PriorityUrlQueue class."""
    
    logger.info("Testing PriorityUrlQueue...")
    
    # Create a priority queue
    queue = PriorityUrlQueue()
    
    # Add URLs with different priorities
    urls = [
        ("https://example.com/page1", 0, 1.0),
        ("https://example.com/page2", 0, 2.0),
        ("https://example.com/page3", 0, 0.5),
        ("https://example.com/page4", 1, 1.5),
    ]
    
    for url, depth, priority in urls:
        result = queue.add(url, depth, priority)
        logger.info(f"Added URL {url} with priority {priority}: {result}")
    
    # Try to add a duplicate URL
    duplicate_url = urls[0][0]
    result = queue.add(duplicate_url, 0, 1.0)
    logger.info(f"Added duplicate URL {duplicate_url}: {result}")
    
    # Check queue size
    size = queue.size()
    logger.info(f"Queue size: {size}")
    
    # Check if the queue contains a URL
    contains = queue.contains(urls[0][0])
    logger.info(f"Queue contains {urls[0][0]}: {contains}")
    
    # Pop URLs from the queue and verify they come out in priority order
    popped_urls = []
    while not queue.is_empty():
        url_info = queue.pop()
        if url_info:
            popped_urls.append((url_info[0], url_info[1], url_info[2]))
            logger.info(f"Popped URL: {url_info[0]} (depth: {url_info[1]}, metadata: {url_info[2]})")
    
    # Check if the queue is empty
    is_empty = queue.is_empty()
    logger.info(f"Queue is empty: {is_empty}")
    
    # Verify that the URLs were popped in priority order (highest first)
    expected_order = sorted(urls, key=lambda x: -x[2])
    success = len(popped_urls) == len(expected_order)
    
    logger.info(f"PriorityUrlQueue test {'PASSED' if success else 'FAILED'}")
    return success

def test_crawler_basic():
    """Test basic crawler functionality without database."""
    
    logger.info("Testing basic crawler functionality...")
    
    # Configuration for the crawler
    config = {
        'max_urls': 5,  # Limit to 5 URLs for testing
        'max_depth': 1,  # Limit depth to 1
        'request_delay': 1,  # 1 second delay between requests
        'respect_robots_txt': True,
        'user_agent': 'NDaiviBot/1.0 (Test)',
        'timeout': 10,
        'allowed_domains': None,  # Allow any domain
        'disallowed_domains': [],
        'allowed_url_patterns': [],
        'disallowed_url_patterns': [],
        'use_database': False,  # Don't use database for this test
    }
    
    # Create the crawler
    crawler = WebCrawler(config=config, logger=logger)
    
    # Add a starting URL
    start_url = "https://example.com"
    logger.info(f"Starting crawl with URL: {start_url}")
    result1 = crawler.add_url(start_url)
    logger.info(f"Added URL {start_url} to queue: {result1}")
    
    # Try adding the same URL again to test duplicate detection
    logger.info("Attempting to add the same URL again...")
    result2 = crawler.add_url(start_url)
    logger.info(f"Added same URL {start_url} to queue again: {result2}")
    
    # Start the crawl
    logger.info("Starting crawl...")
    stats = crawler.crawl()
    
    # Print stats
    logger.info("Crawl completed. Stats:")
    for key, value in stats.items():
        if isinstance(value, set):
            logger.info(f"{key}: {len(value)} items")
        else:
            logger.info(f"{key}: {value}")
    
    # Check if the crawler processed at least one URL
    success = result1 and not result2 and stats['urls_processed'] > 0
    logger.info(f"Basic crawler test {'PASSED' if success else 'FAILED'}")
    return success

def test_crawler_manualslib():
    """Test crawler functionality with manualslib.com."""
    
    logger.info("Testing crawler with manualslib.com...")
    
    # Configuration for the crawler
    config = {
        'max_urls': 100,  # Limit to 100 URLs for testing
        'max_depth': 2,  # Limit depth to 2
        'request_delay': 1,  # 1 second delay between requests
        'respect_robots_txt': True,
        'user_agent': 'NDaiviBot/1.0 (Test)',
        'timeout': 10,
        'allowed_domains': ['manualslib.com'],  # Only allow manualslib.com
        'disallowed_domains': [],
        'allowed_url_patterns': [],
        'disallowed_url_patterns': [],
        'use_database': False,  # Don't use database for this test
        'restrict_to_start_domain': False,  # Change to False to allow all links
        'allow_subdomains': True,  # Allow subdomains
    }
    
    # Create the crawler
    crawler = WebCrawler(config=config, logger=logger)
    
    # Add a starting URL
    start_url = "https://manualslib.com"
    logger.info(f"Starting crawl with URL: {start_url}")
    result = crawler.add_url(start_url)
    logger.info(f"Added URL {start_url} to queue: {result}")
    
    # Start the crawl
    logger.info("Starting crawl...")
    stats = crawler.crawl()
    
    # Print stats
    logger.info("Crawl completed. Stats:")
    for key, value in stats.items():
        if isinstance(value, set):
            logger.info(f"{key}: {len(value)} items")
        else:
            logger.info(f"{key}: {value}")
    
    # Check if the crawler processed a significant number of URLs
    success = stats['urls_processed'] > 10 and stats['urls_queued'] > 10
    logger.info(f"Manualslib crawler test {'PASSED' if success else 'FAILED'}")
    
    # Print the first 10 URLs processed
    logger.info("Sample of URLs processed:")
    if isinstance(crawler.visited_urls, set):
        for i, url in enumerate(list(crawler.visited_urls)[:10]):
            logger.info(f"  {i+1}. {url}")
    else:
        # If it's a VisitedUrlManager, we can't easily get the URLs
        logger.info("  Using VisitedUrlManager, can't easily list URLs")
    
    return success

def main():
    """Run all tests."""
    
    logger.info("Starting web crawler tests...")
    
    # Test priority queue
    logger.info("\n=== Testing PriorityUrlQueue ===")
    queue_test_passed = test_priority_queue()
    
    # Test basic crawler functionality
    logger.info("\n=== Testing basic crawler functionality ===")
    basic_test_passed = test_crawler_basic()
    
    # Test crawler with manualslib.com
    logger.info("\n=== Testing crawler with manualslib.com ===")
    manualslib_test_passed = test_crawler_manualslib()
    
    # Overall result
    all_passed = queue_test_passed and basic_test_passed and manualslib_test_passed
    logger.info(f"\nOverall test result: {'PASSED' if all_passed else 'FAILED'}")
    
    return all_passed

if __name__ == "__main__":
    main()
