#!/usr/bin/env python3
"""
Test script for the translation functionality in ClaudeAnalyzer.

This script tests the translation of categories extracted from brand and category pages
to ensure the translation system is working properly.
"""

import os
import sys
import logging
from dotenv import load_dotenv

# Add the parent directory to the path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the ClaudeAnalyzer
from scraper.claude_analyzer import ClaudeAnalyzer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_translation")

# Load environment variables
load_dotenv()

def test_direct_translation():
    """Test the translate_category method directly."""
    analyzer = ClaudeAnalyzer()
    
    # Enable translation
    analyzer.translation_enabled = True
    
    # Test categories to translate
    test_categories = [
        "Kitchen Appliances",
        "Home Entertainment",
        "Smart Devices",
        "Audio Equipment",
        "Washing Machines"
    ]
    
    logger.info("Testing direct translation of categories:")
    for category in test_categories:
        translated = analyzer.translate_category(category, source_lang="en", target_lang="es")
        logger.info(f"Original: '{category}' -> Translated: '{translated}'")
    
    # Test cache functionality
    logger.info("\nTesting translation cache:")
    for category in test_categories:
        start_time = __import__('time').time()
        translated = analyzer.translate_category(category, source_lang="en", target_lang="es")
        end_time = __import__('time').time()
        logger.info(f"Original: '{category}' -> Translated: '{translated}' (Time: {end_time - start_time:.4f}s)")

def test_brand_page_extraction():
    """Test translation during brand page extraction."""
    analyzer = ClaudeAnalyzer()
    
    # Enable translation
    analyzer.translation_enabled = True
    
    # Sample brand page HTML
    brand_page_html = """
    <html>
    <head>
        <title>Sony - Official Website</title>
        <meta name="description" content="Sony Corporation official website. Learn about our products and services.">
    </head>
    <body>
        <h1>Sony Products</h1>
        <div class="categories">
            <h2>Product Categories</h2>
            <ul>
                <li>Televisions</li>
                <li>Audio Systems</li>
                <li>Cameras</li>
                <li>Mobile Phones</li>
                <li>Gaming Consoles</li>
            </ul>
        </div>
    </body>
    </html>
    """
    
    logger.info("\nTesting translation during brand page extraction:")
    result = analyzer._extract_from_brand_page(
        url="https://www.sony.com",
        title="Sony - Official Website",
        content=brand_page_html
    )
    
    logger.info(f"Extracted manufacturer: {result.get('manufacturer_name')}")
    logger.info(f"Extracted categories: {result.get('categories', [])}")

def test_category_page_extraction():
    """Test translation during category page extraction."""
    analyzer = ClaudeAnalyzer()
    
    # Enable translation
    analyzer.translation_enabled = True
    
    # Sample category page HTML
    category_page_html = """
    <html>
    <head>
        <title>Sony - Televisions</title>
        <meta name="description" content="Explore Sony's range of televisions including 4K, OLED, and Smart TVs.">
    </head>
    <body>
        <h1>Sony Televisions</h1>
        <div class="subcategories">
            <h2>Television Types</h2>
            <ul>
                <li>4K Ultra HD TVs</li>
                <li>OLED TVs</li>
                <li>LED TVs</li>
                <li>Smart TVs</li>
            </ul>
        </div>
    </body>
    </html>
    """
    
    logger.info("\nTesting translation during category page extraction:")
    result = analyzer._extract_from_category_page(
        url="https://www.sony.com/televisions",
        title="Sony - Televisions",
        content=category_page_html
    )
    
    logger.info(f"Extracted manufacturer: {result.get('manufacturer_name')}")
    logger.info(f"Extracted categories: {result.get('categories', [])}")

def main():
    """Main function to run all tests."""
    logger.info("Starting translation tests")
    
    # Run tests
    test_direct_translation()
    test_brand_page_extraction()
    test_category_page_extraction()
    
    logger.info("All translation tests completed")

if __name__ == "__main__":
    main()
