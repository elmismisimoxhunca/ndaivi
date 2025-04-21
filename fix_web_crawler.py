#!/usr/bin/env python3

import os
import sys

def fix_crawler():
    """Fix the web_crawler.py file to correct indentation issues"""
    input_file = '/var/ndaivimanuales/scraper/web_crawler.py'
    output_file = '/var/ndaivimanuales/scraper/web_crawler_fixed.py'
    
    with open(input_file, 'r') as f:
        lines = f.readlines()
    
    # Extract imports and headers
    header_lines = []
    import_lines = []
    class_lines = []
    main_lines = []
    current_section = 'header'
    
    for i, line in enumerate(lines):
        if i < 10:  # First few lines are header
            header_lines.append(line)
        elif line.startswith('import ') or line.startswith('from '):
            import_lines.append(line)
        elif line.strip().startswith('class '):
            current_section = 'class'
            class_lines.append(line)
        elif current_section == 'class':
            class_lines.append(line)
        else:
            main_lines.append(line)
    
    # Create a fixed file
    with open(output_file, 'w') as f:
        # Write headers
        for line in header_lines:
            f.write(line)
        
        # Write imports
        for line in import_lines:
            f.write(line)
        
        # Add missing imports
        missing_imports = [
            'from urllib.parse import urlparse, urljoin, ParseResult\n',
            'from bs4 import BeautifulSoup\n',
            'from typing import Dict, List, Tuple, Optional, Any, Union, Set\n'
        ]
        for imp in missing_imports:
            if imp not in import_lines:
                f.write(imp)
        
        # Write class definitions
        for line in class_lines:
            f.write(line)
        
        # Write main section at end of file
        f.write("\n# Example usage\n")
        f.write("if __name__ == \"__main__\":\n")
        f.write("    # Set up logging\n")
        f.write("    logging.basicConfig(\n")
        f.write("        level=logging.INFO,\n")
        f.write("        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'\n")
        f.write("    )\n\n")
        
        f.write("    # Set more verbose logging\n")
        f.write("    logging.getLogger().setLevel(logging.DEBUG)\n\n")
        
        f.write("    # Get target website from environment variable or use default\n")
        f.write("    target_website = os.environ.get('NDAIVI_TARGET_WEBSITE', 'manualslib.com')\n")
        f.write("    max_urls = int(os.environ.get('NDAIVI_MAX_URLS', '1000'))\n\n")
        
        f.write("    # Start crawling with https protocol\n")
        f.write("    start_url = f\"https://{target_website}\"\n")
        f.write("    logging.info(f\"Starting crawl of {start_url} with max_urls={max_urls}\")\n")
        f.write("    print(f\"Starting crawl of {start_url} with max_urls={max_urls}\")\n\n")
        
        f.write("    # Initialize database and crawler state\n")
        f.write("    db_path = os.environ.get('NDAIVI_DB_PATH', '/var/ndaivimanuales/data/crawler.db')\n")
        f.write("    db_dir = os.path.dirname(db_path)\n")
        f.write("    if not os.path.exists(db_dir):\n")
        f.write("        os.makedirs(db_dir)\n\n")
        
        f.write("    # Create and configure the crawler\n")
        f.write("    crawler = WebCrawler()\n\n")
        
        f.write("    # Define callbacks\n")
        f.write("    def on_content_extracted(url, depth, title, content, metadata):\n")
        f.write("        print(f\"Processed: {url} (depth {depth}) - {title}\")\n\n")
        
        f.write("    # Set callbacks\n")
        f.write("    crawler.callbacks['on_content_extracted'] = [on_content_extracted]\n\n")
        
        f.write("    # Configure logging to file\n")
        f.write("    log_file = os.environ.get('NDAIVI_LOG_FILE', '/var/ndaivimanuales/logs/crawler.log')\n")
        f.write("    log_dir = os.path.dirname(log_file)\n")
        f.write("    if not os.path.exists(log_dir):\n")
        f.write("        os.makedirs(log_dir)\n\n")
        
        f.write("    file_handler = logging.FileHandler(log_file)\n")
        f.write("    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))\n")
        f.write("    logging.getLogger().addHandler(file_handler)\n\n")
        
        f.write("    # Start the crawler\n")
        f.write("    try:\n")
        f.write("        crawler.start_crawl(start_url, max_urls=max_urls)\n")
        f.write("    except KeyboardInterrupt:\n")
        f.write("        logging.info(\"Crawler stopped by user\")\n")
        f.write("    except Exception as e:\n")
        f.write("        logging.error(f\"Crawler error: {e}\")\n")
    
    print(f"Fixed crawler written to {output_file}")
    return output_file

if __name__ == "__main__":
    fixed_file = fix_crawler()
    print(f"Now test the fixed crawler with: python3 {fixed_file}")
