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
    """Main function."""
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
        
        # Initialize analyzer with standard configuration
        analyzer = ClaudeAnalyzer()
        
        # Ensure translation is enabled
        analyzer.translation_enabled = True
        logger.info(f"Translation enabled: {analyzer.translation_enabled}")
        
        # Set target languages if not already set
        if not analyzer.target_languages:
            analyzer.target_languages = {"es": "Spanish"}
            logger.info(f"Setting target languages: {analyzer.target_languages}")
        
        # Use the full analyze_page method (includes batch translation)
        logger.info("Using full analyze_page method")
        result = analyzer.analyze_page(url, title, content, metadata)
        
        # Log result
        logger.info(f"Analysis result: page_type={result.get('page_type', 'unknown')}, is_manufacturer_page={result.get('is_manufacturer_page', False)}")
        
        # Extract categories
        if 'categories' in result:
            extracted_categories = result.get('categories', [])
            logger.info(f"Extracted categories: {extracted_categories}")
        
        # Get translation results
        if 'translated_categories' in result:
            logger.info("Translation results:")
            for lang, categories in result.get('translated_categories', {}).items():
                logger.info(f"  {lang}: {categories}")
        
    except Exception as e:
        logger.error(f"Error running test: {str(e)}")
        # Print traceback for debugging
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()
