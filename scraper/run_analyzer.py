#!/usr/bin/env python3

"""
Run script for the Claude Analyzer component of NDAIVI

This script watches for batch files in the analyzer_watch directory and
processes them using the ClaudeAnalyzer, then writes results to the
analyzer_results directory where the Go daemon expects to find them.

Follows WindsurfRule #5: Analyzer receives links from the Go daemon, never directly from SQLite.
Follows WindsurfRule #7: System is modular - each component can be run/tested independently.
"""

import os
import sys
import json
import time
import logging
from pathlib import Path
import traceback
from typing import List, Dict, Any
import requests
from bs4 import BeautifulSoup

# Add current directory to path to ensure imports work
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    # Import the analyzer - with better error handling
    from claude_analyzer import ClaudeAnalyzer
    print(f"Successfully imported ClaudeAnalyzer from {current_dir}")
except ImportError as e:
    print(f"ERROR importing ClaudeAnalyzer: {e}")
    # Log the Python path for debugging
    print(f"Python path: {sys.path}")
    # Try to list the directory content
    print(f"Directory content for {current_dir}:")
    for item in os.listdir(current_dir):
        print(f"  - {item}")
    sys.exit(1)

# Setup logging
log_file = os.getenv('NDAIVI_LOG_FILE', '/var/ndaivimanuales/logs/analyzer.log')
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('ndaivi-analyzer')
logger.info(f"Logging to {log_file}")

# Directories
WATCH_DIR = '/var/ndaivimanuales/tmp/analyzer_watch'
RESULTS_DIR = '/var/ndaivimanuales/tmp/analyzer_results'
ERRORS_DIR = '/var/ndaivimanuales/tmp/analyzer_errors'

def setup_environment():
    """Set up the analyzer environment"""
    # Create necessary directories
    os.makedirs(WATCH_DIR, exist_ok=True)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(ERRORS_DIR, exist_ok=True)
    
    # Load environment variables from .env file if it exists
    try:
        from dotenv import load_dotenv
        env_file = Path('/var/ndaivimanuales/.env')
        if env_file.exists():
            load_dotenv(env_file)
            logger.info("Loaded environment variables from .env file")
    except ImportError:
        logger.warning("python-dotenv not installed, cannot load from .env file")
    
    logger.info("Environment setup complete")
    return True

def fetch_url_content(url: str) -> Dict[str, Any]:
    """Fetch content from a URL for analysis"""
    try:
        logger.info(f"Fetching content from {url}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; NDaiviBot/1.0; +https://ndaivi.com/bot)'
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
            
            return {
                'url': url,
                'title': title,
                'content': content,
                'metadata': metadata_str,
                'status': 'success'
            }
        else:
            logger.error(f"Failed to fetch {url}: HTTP {response.status_code}")
            return {
                'url': url,
                'status': 'error',
                'error': f"HTTP error {response.status_code}"
            }
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
        return {
            'url': url,
            'status': 'error',
            'error': str(e)
        }

def process_batch_file(batch_file: str, analyzer: ClaudeAnalyzer) -> bool:
    """Process a batch file of URLs"""
    try:
        batch_path = os.path.join(WATCH_DIR, batch_file)
        logger.info(f"Processing batch file: {batch_path}")
        
        # Read the batch file
        with open(batch_path, 'r') as f:
            urls = json.load(f)
        
        if not urls or not isinstance(urls, list):
            logger.error(f"Invalid batch file format: {batch_path}")
            return False
        
        logger.info(f"Found {len(urls)} URLs in batch")
        
        # Process each URL
        results = []
        for url in urls:
            try:
                # Fetch content from URL
                content_data = fetch_url_content(url)
                
                if content_data['status'] == 'success':
                    # Analyze the page
                    logger.info(f"Analyzing content for {url}")
                    result = analyzer.analyze_page(
                        url, 
                        content_data['title'], 
                        content_data['content'], 
                        content_data['metadata']
                    )
                    
                    # Add status and URL to result
                    result['status'] = 'success'
                    result['url'] = url
                    
                    results.append(result)
                    logger.info(f"Analysis complete for {url}")
                else:
                    # Add error result
                    results.append(content_data)
            except Exception as e:
                logger.error(f"Error analyzing {url}: {e}")
                logger.error(traceback.format_exc())
                results.append({
                    'url': url,
                    'status': 'error',
                    'error': str(e)
                })
        
        # Write individual result files
        batch_id = batch_file.split('.')[0].split('_')[1]  # Extract batch ID from filename
        
        for i, result in enumerate(results):
            result_file = os.path.join(RESULTS_DIR, f"result_{batch_id}_{i}.json")
            with open(result_file, 'w') as f:
                json.dump(result, f, indent=2)
        
        logger.info(f"Wrote analysis results to {result_file}")
        
        # Remove the processed batch file
        os.remove(batch_path)
        logger.info(f"Removed processed batch file: {batch_path}")
        
        return True
    except Exception as e:
        logger.error(f"Error processing batch file {batch_file}: {e}")
        logger.error(traceback.format_exc())
        
        # Move to errors directory if there was a problem
        try:
            src_path = os.path.join(WATCH_DIR, batch_file)
            dst_path = os.path.join(ERRORS_DIR, batch_file)
            os.rename(src_path, dst_path)
            logger.info(f"Moved problematic batch file to {dst_path}")
        except Exception as move_err:
            logger.error(f"Error moving batch file to errors directory: {move_err}")
        
        return False

def watch_for_batches():
    """Watch the analyzer_watch directory for new batch files"""
    logger.info(f"Watching for batch files in {WATCH_DIR}")
    
    try:
        # Initialize the analyzer
        analyzer = ClaudeAnalyzer()
        logger.info("Claude Analyzer initialized")
        
        # Main watch loop
        while True:
            try:
                # List all JSON files in the watch directory
                batch_files = [f for f in os.listdir(WATCH_DIR) 
                             if f.endswith('.json') and f.startswith('batch_')]
                
                if batch_files:
                    logger.info(f"Found {len(batch_files)} batch files to process")
                    
                    # Process each batch file
                    for batch_file in sorted(batch_files):  # Process in order
                        process_batch_file(batch_file, analyzer)
                else:
                    # No batch files found, wait before checking again
                    time.sleep(5)  # Sleep for 5 seconds
            except Exception as e:
                logger.error(f"Error in watch loop: {e}")
                logger.error(traceback.format_exc())
                time.sleep(5)  # Sleep and try again
    except Exception as e:
        logger.error(f"Critical error in analyzer: {e}")
        logger.error(traceback.format_exc())
        return 1

def main():
    """Main entry point for the analyzer"""
    logger.info("=== NDAIVI Analyzer Starting ===")
    
    # Set up the environment
    if not setup_environment():
        return 1
    
    # Start watching for batch files
    return watch_for_batches()

if __name__ == "__main__":
    sys.exit(main())
