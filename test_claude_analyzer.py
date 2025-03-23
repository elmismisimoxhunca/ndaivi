#!/usr/bin/env python3
"""
Test script for the Claude Analyzer component.

This script demonstrates the 3-step analysis process implemented in the ClaudeAnalyzer class:
1. Keyword filter (no Claude API call)
2. Metadata analysis (Claude API call with title and metadata)
3. Content analysis (Claude API call with truncated content)

Usage:
    python test_claude_analyzer.py <url>
"""

import os
import sys
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the ClaudeAnalyzer
from scraper.claude_analyzer import ClaudeAnalyzer
from utils.config_manager import ConfigManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_page_data(url: str):
    """
    Extract page data from a URL.
    
    Args:
        url: URL to extract data from
        
    Returns:
        Tuple of (title, content, metadata)
    """
    try:
        # Make request
        logger.info(f"Fetching URL: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract title
        title = soup.title.string if soup.title else ""
        logger.info(f"Title: {title}")
        
        # Extract content
        content = soup.get_text()
        logger.info(f"Content length: {len(content)} characters")
        
        # Extract metadata
        metadata = extract_metadata(soup)
        logger.info(f"Metadata length: {len(metadata)} characters")
        
        return title, content, metadata
    
    except Exception as e:
        logger.error(f"Error extracting page data: {e}")
        sys.exit(1)

def extract_metadata(soup: BeautifulSoup) -> str:
    """
    Extract metadata from HTML.
    
    Args:
        soup: BeautifulSoup object
        
    Returns:
        Metadata as a string
    """
    metadata = []
    
    # Extract meta tags
    for meta in soup.find_all('meta'):
        name = meta.get('name', meta.get('property', ''))
        content = meta.get('content', '')
        if name and content:
            metadata.append(f"{name}: {content}")
    
    # Extract headers
    for i in range(1, 7):
        for header in soup.find_all(f'h{i}'):
            text = header.get_text().strip()
            if text:
                metadata.append(f"H{i}: {text}")
    
    return "\n".join(metadata)

def check_api_key():
    """
    Check if the Claude API key is set.
    
    Returns:
        True if the API key is set, False otherwise
    """
    api_key = os.environ.get('CLAUDE_API_KEY')
    if not api_key:
        config_manager = ConfigManager()
        api_key = config_manager.get('claude_analyzer.api_key', '')
        
    if not api_key:
        logger.error("No Claude API key found in environment variables or config")
        logger.error("Please set the CLAUDE_API_KEY environment variable or configure it in config.yaml")
        return False
    
    return True

def main():
    """Main entry point."""
    # Check command line arguments
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <url>")
        sys.exit(1)
    
    # Get URL from command line
    url = sys.argv[1]
    
    # Check API key
    if not check_api_key():
        logger.error("Claude API key not set. Please set the CLAUDE_API_KEY environment variable.")
        sys.exit(1)
    
    try:
        # Extract page data
        title, content, metadata = extract_page_data(url)
        
        # Initialize analyzer
        logger.info("Initializing Claude Analyzer")
        analyzer = ClaudeAnalyzer()
        
        # Step 1: Keyword filter
        logger.info("Step 1: Keyword filter")
        filter_result = analyzer.keyword_filter(url, title, content)
        logger.info(f"Filter result: {filter_result}")
        
        # If keyword filter gives a definitive result, extract data if needed
        if filter_result['page_type'] in ['brand_page', 'brand_category_page']:
            page_type = filter_result['page_type']
            logger.info(f"Keyword filter identified page as a {page_type}")
            data = analyzer.extract_manufacturer_data(url, title, content, page_type)
            logger.info(f"Extracted data: {data}")
            sys.exit(0)
        
        # If we need further analysis, proceed to step 2
        if filter_result['passed']:
            logger.info("Keyword filter passed, proceeding to step 2")
            
            # Step 2: Metadata analysis
            logger.info("Step 2: Metadata analysis")
            metadata_result = analyzer.analyze_metadata(url, title, metadata)
            logger.info(f"Metadata analysis result: {metadata_result}")
            
            # If we got a definitive result, extract data if needed
            if metadata_result['page_type'] != 'inconclusive':
                page_type = metadata_result['page_type']
                logger.info(f"Metadata analysis identified page as a {page_type}")
                
                if page_type in ['brand_page', 'brand_category_page']:
                    data = analyzer.extract_manufacturer_data(url, title, content, page_type)
                    logger.info(f"Extracted data: {data}")
                    sys.exit(0)
            
            # If we need further analysis, proceed to step 3 (content analysis)
            if metadata_result['page_type'] == 'inconclusive':
                logger.info("Metadata analysis inconclusive, proceeding to step 3")
                
                # Step 3: Content analysis
                logger.info("Step 3: Content analysis")
                content_result = analyzer.analyze_content(url, title, content)
                logger.info(f"Content analysis result: {content_result}")
                
                # If we got a definitive result, extract data if needed
                if content_result['page_type'] != 'inconclusive':
                    page_type = content_result['page_type']
                    logger.info(f"Content analysis identified page as a {page_type}")
                    
                    if page_type in ['brand_page', 'brand_category_page']:
                        data = analyzer.extract_manufacturer_data(url, title, content, page_type)
                        logger.info(f"Extracted data: {data}")
                        sys.exit(0)
                else:
                    logger.info("Content analysis inconclusive, page is not a manufacturer page")
        else:
            logger.info("Keyword filter failed, page is not a manufacturer page")
        
        # If we get here, the page is not a manufacturer page
        logger.info("Page is not a manufacturer page")
        
    except Exception as e:
        logger.error(f"Error analyzing page: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
