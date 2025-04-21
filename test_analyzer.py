#!/usr/bin/env python3

"""
Test script for the Claude Analyzer component of NDAIVI

This script tests the analyzer's functionality independently, following WindsurfRule #7.
It loads the Claude API key from the .env file for improved security.
"""

import os
import sys
import json
import time
import logging
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/ndaivimanuales/logs/test_analyzer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('ndaivi-test-analyzer')

# Directories and files
WATCH_DIR = '/var/ndaivimanuales/tmp/analyzer_watch'
RESULTS_DIR = '/var/ndaivimanuales/tmp/analyzer_results'

def setup_environment():
    """Set up the test environment"""
    # Create necessary directories
    os.makedirs(WATCH_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    # Load environment variables from .env file
    env_file = Path('/var/ndaivimanuales/.env')
    if env_file.exists():
        load_dotenv(env_file)
        logger.info("Loaded environment variables from .env file")
    else:
        logger.warning(".env file not found at /var/ndaivimanuales/.env")
    
    # Check if Claude API key is available
    api_key = os.environ.get('CLAUDE_API_KEY')
    if not api_key:
        logger.error("CLAUDE_API_KEY not found in environment variables")
        logger.info("Please add CLAUDE_API_KEY to your .env file:")
        logger.info("CLAUDE_API_KEY=your_api_key_here")
        return False
    
    logger.info("Environment setup complete")
    return True

def create_test_batch():
    """Create a test batch of URLs for the analyzer"""
    batch_id = int(time.time())
    batch_file = os.path.join(WATCH_DIR, f"batch_{batch_id}.json")
    
    # Create a test batch with a few URLs
    test_urls = [
        "https://manualslib.com/brand/sony/",
        "https://manualslib.com/brand/samsung/",
        "https://manualslib.com/manual/12345/Example-Device.html"
    ]
    
    with open(batch_file, 'w') as f:
        json.dump(test_urls, f)
    
    logger.info(f"Created test batch file: {batch_file} with {len(test_urls)} URLs")
    return batch_file

def run_analyzer():
    """Run the Claude analyzer on a test batch"""
    from scraper.claude_analyzer import ClaudeAnalyzer
    
    try:
        # Initialize the analyzer
        logger.info("Initializing Claude Analyzer...")
        analyzer = ClaudeAnalyzer()
        
        # Verify translation is enabled
        if hasattr(analyzer, 'translation_manager') and analyzer.translation_manager:
            logger.info(f"Translation manager initialized: {analyzer.translation_manager.enabled}")
            logger.info(f"Target languages: {analyzer.translation_manager.target_languages}")
        else:
            logger.warning("Translation manager not initialized properly")
        
        # Create a test batch
        batch_file = create_test_batch()
        
        # Process the batch
        logger.info("Processing test batch...")
        with open(batch_file, 'r') as f:
            urls = json.load(f)
        
        # Process each URL
        results = []
        for url in urls:
            logger.info(f"Analyzing URL: {url}")
            try:
                # Fetch content from URL for analysis
                import requests
                from bs4 import BeautifulSoup
                
                logger.info(f"Fetching content from {url}")
                headers = {
                    'User-Agent': 'Mozilla/5.0 (compatible; NDaiviTestBot/1.0)'
                }
                response = requests.get(url, timeout=30, headers=headers)
                
                if response.status_code == 200:
                    # Parse HTML content
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Extract title and content
                    title = soup.title.string if soup.title else "No title"
                    content = soup.get_text(separator=" ", strip=True)
                    
                    # Extract metadata
                    metadata = {}
                    for meta in soup.find_all('meta'):
                        if meta.get('name'):
                            metadata[meta.get('name')] = meta.get('content', '')
                        elif meta.get('property'):
                            metadata[meta.get('property')] = meta.get('content', '')
                    
                    # Convert metadata to string format
                    metadata_str = '\n'.join([f"{k}: {v}" for k, v in metadata.items()])
                    
                    # Analyze the page using the correct method
                    logger.info(f"Analyzing content for {url}")
                    result = analyzer.analyze_page(url, title, content, metadata_str)
                    results.append(result)
                    logger.info(f"Analysis complete for {url}")
                else:
                    logger.error(f"Failed to fetch {url}: HTTP {response.status_code}")
                    results.append({
                        'url': url,
                        'status': 'error',
                        'error': f"HTTP error {response.status_code}"
                    })
            except Exception as e:
                logger.error(f"Error analyzing {url}: {e}")
                results.append({
                    'url': url,
                    'status': 'error',
                    'error': str(e)
                })
        
        # Write results to output directory
        result_file = os.path.join(RESULTS_DIR, f"result_{int(time.time())}.json")
        with open(result_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Wrote analysis results to {result_file}")
        return True
    except Exception as e:
        logger.error(f"Error running analyzer: {e}")
        return False

def main():
    """Main test function"""
    logger.info("=== NDAIVI Analyzer Test ===")
    
    # Set up the environment
    if not setup_environment():
        return 1
    
    # Run the analyzer
    success = run_analyzer()
    
    if success:
        logger.info("TEST PASSED: Analyzer ran successfully")
        return 0
    else:
        logger.error("TEST FAILED: Analyzer encountered errors")
        return 1

if __name__ == "__main__":
    sys.exit(main())
