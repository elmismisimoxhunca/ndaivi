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
import json
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

def get_backlog_batch(batch_size: int = 10) -> None:
    """
    Get a batch of URLs from the backlog for analysis and print them as JSON.
    
    Args:
        batch_size: Maximum number of URLs to return
    """
    # Load configuration
    config_manager = ConfigManager()
    crawler_config = config_manager.get('web_crawler', {})
    
    if not crawler_config:
        logger.error("Failed to load web crawler configuration")
        return
    
    # Create crawler
    crawler = WebCrawler(config=crawler_config)
    
    try:
        # Get batch from backlog
        batch = crawler.get_backlog_batch(batch_size=batch_size)
        
        # Print as JSON
        print(json.dumps(batch, indent=2))
        
        # Log stats
        logger.info(f"Retrieved {len(batch)} URLs from backlog")
        
        # Print backlog stats
        backlog_stats = crawler.get_backlog_stats()
        logger.info(f"Backlog stats: {backlog_stats.get('backlog_size')}/{backlog_stats.get('backlog_capacity')} URLs in backlog ({backlog_stats.get('backlog_usage')*100:.1f}%)")
    finally:
        # Close crawler
        crawler.close()

def mark_as_processed(url: str) -> None:
    """
    Mark a URL as processed and remove it from the backlog.
    
    Args:
        url: URL to mark as processed
    """
    # Load configuration
    config_manager = ConfigManager()
    crawler_config = config_manager.get('web_crawler', {})
    
    if not crawler_config:
        logger.error("Failed to load web crawler configuration")
        return
    
    # Create crawler
    crawler = WebCrawler(config=crawler_config)
    
    try:
        # Mark URL as processed
        success = crawler.mark_as_processed(url)
        
        if success:
            logger.info(f"Successfully marked URL as processed: {url}")
            print(f"Successfully marked URL as processed: {url}")
        else:
            logger.error(f"Failed to mark URL as processed: {url}")
            print(f"Failed to mark URL as processed: {url}")
        
        # Print backlog stats
        backlog_stats = crawler.get_backlog_stats()
        logger.info(f"Backlog stats: {backlog_stats.get('backlog_size')}/{backlog_stats.get('backlog_capacity')} URLs in backlog ({backlog_stats.get('backlog_usage')*100:.1f}%)")
    finally:
        # Close crawler
        crawler.close()

def update_config(config_updates: Dict[str, Any]) -> None:
    """
    Update the crawler configuration.
    
    Args:
        config_updates: Dictionary of configuration updates
    """
    # Load configuration
    config_manager = ConfigManager()
    crawler_config = config_manager.get('web_crawler', {})
    
    if not crawler_config:
        logger.error("Failed to load web crawler configuration")
        return
    
    # Create crawler
    crawler = WebCrawler(config=crawler_config)
    
    try:
        # Update config
        crawler.update_config(config_updates)
        logger.info(f"Updated crawler configuration: {config_updates}")
        print(f"Updated crawler configuration: {json.dumps(config_updates, indent=2)}")
    finally:
        # Close crawler
        crawler.close()

def main():
    """Main function to parse arguments and run the crawler."""
    parser = argparse.ArgumentParser(description='Run the NDAIVI web crawler')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Crawl command
    crawl_parser = subparsers.add_parser('crawl', help='Start crawling from a URL')
    crawl_parser.add_argument('start_url', help='URL to start crawling from')
    crawl_parser.add_argument('--max-urls', type=int, help='Maximum number of URLs to process')
    crawl_parser.add_argument('--max-time', type=int, help='Maximum time to run in seconds')
    crawl_parser.add_argument('--max-depth', type=int, help='Maximum crawl depth')
    crawl_parser.add_argument('--single-domain', action='store_true', help='Restrict to the start domain only')
    crawl_parser.add_argument('--db-path', help='Path to the crawler database')
    crawl_parser.add_argument('--delay', type=float, help='Delay between requests in seconds')
    crawl_parser.add_argument('--user-agent', help='User agent string to use')
    crawl_parser.add_argument('--allowed-domains', nargs='+', help='List of allowed domains')
    crawl_parser.add_argument('--disallowed-domains', nargs='+', help='List of disallowed domains')
    
    # Get backlog batch command
    backlog_parser = subparsers.add_parser('get-backlog', help='Get a batch of URLs from the backlog')
    backlog_parser.add_argument('--batch-size', type=int, default=10, help='Number of URLs to get from the backlog')
    
    # Mark as processed command
    mark_parser = subparsers.add_parser('mark-processed', help='Mark a URL as processed')
    mark_parser.add_argument('url', help='URL to mark as processed')
    
    # Update config command
    config_parser = subparsers.add_parser('update-config', help='Update the crawler configuration')
    config_parser.add_argument('--config-json', required=True, help='JSON string with configuration updates')
    
    args = parser.parse_args()
    
    # Ensure logs directory exists
    os.makedirs('logs', exist_ok=True)
    
    # Handle commands
    if args.command == 'crawl':
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
    elif args.command == 'get-backlog':
        get_backlog_batch(batch_size=args.batch_size)
    elif args.command == 'mark-processed':
        mark_as_processed(url=args.url)
    elif args.command == 'update-config':
        try:
            config_updates = json.loads(args.config_json)
            update_config(config_updates)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON format: {args.config_json}")
            print(f"Invalid JSON format: {args.config_json}")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
