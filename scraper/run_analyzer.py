#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import traceback
from typing import Optional

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scraper.claude_analyzer import ClaudeAnalyzer
from scraper.db_manager import DBManager
from utils.config_manager import ConfigManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('run_analyzer')


def analyze_url(url: str, config_path: Optional[str] = None) -> bool:
    """
    Analyze a specific URL using the Claude Analyzer.
    
    Args:
        url: The URL to analyze
        config_path: Optional path to the config file
        
    Returns:
        bool: True if analysis was successful, False otherwise
    """
    try:
        logger.info(f"Analyzing URL: {url}")
        
        # Initialize the analyzer
        analyzer = ClaudeAnalyzer(config_path)
        
        # Fetch the content and analyze it
        success = analyzer.analyze_url(url)
        
        if success:
            logger.info(f"Successfully analyzed URL: {url}")
        else:
            logger.error(f"Failed to analyze URL: {url}")
            
        return success
    except Exception as e:
        logger.error(f"Error analyzing URL {url}: {str(e)}")
        logger.error(traceback.format_exc())
        return False


def main():
    parser = argparse.ArgumentParser(description='Run the NDAIVI URL analyzer')
    parser.add_argument('--url', help='URL to analyze')
    parser.add_argument('--config', help='Path to config file')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of URLs to analyze in one batch')
    
    args = parser.parse_args()
    
    # If a specific URL is provided, analyze it
    if args.url:
        success = analyze_url(args.url, args.config)
        sys.exit(0 if success else 1)
    
    # Otherwise, get unanalyzed URLs from the database and analyze them
    try:
        logger.info("Starting analyzer in batch mode")
        
        # Initialize the database manager
        db_manager = DBManager()
        
        # Get unanalyzed URLs
        urls = db_manager.get_unanalyzed_urls(args.batch_size)
        
        if not urls:
            logger.info("No unanalyzed URLs found")
            sys.exit(0)
        
        logger.info(f"Found {len(urls)} unanalyzed URLs")
        
        # Initialize the analyzer
        analyzer = ClaudeAnalyzer(args.config)
        
        # Process each URL
        for url_data in urls:
            url = url_data['url']
            logger.info(f"Analyzing URL: {url}")
            
            try:
                # Analyze the URL
                success = analyzer.analyze_url(url)
                
                if success:
                    logger.info(f"Successfully analyzed URL: {url}")
                    # Mark the URL as analyzed
                    db_manager.mark_url_as_analyzed(url)
                else:
                    logger.error(f"Failed to analyze URL: {url}")
            except Exception as e:
                logger.error(f"Error analyzing URL {url}: {str(e)}")
                logger.error(traceback.format_exc())
        
        logger.info("Batch analysis completed")
        
    except Exception as e:
        logger.error(f"Error in analyzer batch mode: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
