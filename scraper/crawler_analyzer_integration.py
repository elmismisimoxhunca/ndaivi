#!/usr/bin/env python3
"""
Integration script for the web crawler and Claude analyzer.

This script demonstrates the workflow:
1. Enter link
2. Find other links and store them in the database
3. Decide which links to follow based on priority and depth
4. Track both "visited" and "analyzed" states for links
5. Mark links as analyzed when notified by the container script
"""

import os
import sys
import time
import logging
import argparse
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
        logging.FileHandler('logs/integration.log')
    ]
)

logger = logging.getLogger('crawler_analyzer_integration')

class AnalyzerSimulator:
    """
    Simulates the Claude analyzer component for demonstration purposes.
    In a real implementation, this would use the actual Claude analyzer.
    """
    
    def __init__(self, analysis_delay: float = 1.0):
        """
        Initialize the analyzer simulator.
        
        Args:
            analysis_delay: Simulated delay for analysis in seconds
        """
        self.analysis_delay = analysis_delay
        self.analyzed_count = 0
    
    def analyze_url(self, url_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Simulate analyzing a URL.
        
        Args:
            url_data: URL data from the crawler
            
        Returns:
            Analysis results
        """
        # Simulate analysis delay
        time.sleep(self.analysis_delay)
        
        url = url_data['url']
        logger.info(f"Analyzing URL: {url}")
        
        # Simulate analysis results
        self.analyzed_count += 1
        
        return {
            'url': url,
            'analysis_id': f"analysis_{self.analyzed_count}",
            'analysis_result': 'success',
            'content_type': 'product' if 'product' in url.lower() else 'category',
            'timestamp': time.time()
        }

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
    logger.info(f"  Analyzed URLs: {stats.get('analyzed_urls', 0)}")
    logger.info(f"  Unanalyzed URLs: {stats.get('unanalyzed_urls', 0)}")

def main():
    """Main function to run the crawler-analyzer integration."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='NDAIVI Crawler-Analyzer Integration')
    parser.add_argument('start_url', help='URL to start crawling from')
    parser.add_argument('--max-urls', type=int, default=10, help='Maximum number of URLs to process')
    parser.add_argument('--analysis-batch-size', type=int, default=3, help='Number of URLs to analyze in each batch')
    parser.add_argument('--analysis-delay', type=float, default=1.0, help='Simulated delay for analysis in seconds')
    args = parser.parse_args()
    
    # Ensure the logs directory exists
    os.makedirs('logs', exist_ok=True)
    
    # Load configuration
    config_manager = ConfigManager()
    crawler_config = config_manager.get_config('web_crawler')
    
    if not crawler_config:
        logger.error("Failed to load web crawler configuration")
        return
    
    # Ensure the data directory exists
    db_path = crawler_config.get('db_path', 'crawler.db')
    db_dir = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(db_dir, exist_ok=True)
    
    # Create the crawler
    logger.info(f"Initializing crawler with start URL: {args.start_url}")
    
    crawler = WebCrawler(
        config=crawler_config,
        stats_callback=stats_callback
    )
    
    # Create the analyzer simulator
    analyzer = AnalyzerSimulator(analysis_delay=args.analysis_delay)
    
    # Add the start URL
    crawler.add_url(args.start_url)
    
    # Start crawling and analysis process
    try:
        urls_processed = 0
        urls_analyzed = 0
        
        logger.info("Starting crawler-analyzer integration process")
        
        # Main processing loop
        while True:
            # Process URLs until we reach the limit
            while urls_processed < args.max_urls:
                # Process next URL
                result = crawler.process_next_url()
                
                # Check if we're done or should stop
                if not result:
                    logger.info("No more URLs to process")
                    break
                
                if result.get('status') == 'shutdown':
                    logger.info("Received shutdown signal")
                    break
                
                if result.get('status') == 'success':
                    urls_processed += 1
                    logger.info(f"Processed URL: {result.get('url')} ({urls_processed}/{args.max_urls})")
            
            # Get unanalyzed URLs
            unanalyzed_urls = crawler.get_unanalyzed_urls(limit=args.analysis_batch_size)
            
            if not unanalyzed_urls:
                if urls_processed >= args.max_urls or not crawler.process_next_url():
                    logger.info("No more URLs to analyze and crawling complete")
                    break
                continue
            
            # Analyze URLs
            logger.info(f"Found {len(unanalyzed_urls)} unanalyzed URLs")
            
            for url_data in unanalyzed_urls:
                # Analyze URL
                analysis_result = analyzer.analyze_url(url_data)
                
                # Mark URL as analyzed
                url = url_data['url']
                if crawler.mark_url_as_analyzed(url):
                    urls_analyzed += 1
                    logger.info(f"Marked URL as analyzed: {url} ({urls_analyzed} total)")
                else:
                    logger.warning(f"Failed to mark URL as analyzed: {url}")
            
            # Get updated stats
            stats = crawler.get_stats()
            logger.info(f"Current stats: {urls_processed} processed, {urls_analyzed} analyzed, {stats.get('urls_queued', 0)} queued")
            
            # Check if we're done
            if urls_processed >= args.max_urls and not unanalyzed_urls:
                logger.info("Reached maximum URLs and all URLs analyzed")
                break
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.exception(f"Error during integration process: {e}")
    finally:
        # Get final stats
        stats = crawler.get_stats()
        
        logger.info("Final Stats:")
        logger.info(f"  URLs Processed: {stats.get('urls_processed', 0)}")
        logger.info(f"  URLs Analyzed: {stats.get('analyzed_urls', 0)}")
        logger.info(f"  URLs Queued: {stats.get('urls_queued', 0)}")
        
        # Close the crawler
        crawler.close()
        logger.info("Crawler closed")

if __name__ == "__main__":
    main()
