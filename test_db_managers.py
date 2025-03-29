#!/usr/bin/env python3
"""
Test script for database managers.

This script tests the DBManager and MainDBManager classes to ensure
they correctly use separate databases for crawler and manual information.
"""

import os
import logging
import sys
from scraper.db_manager import DBManager, MainDBManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """
    Test the database managers.
    """
    logger.info("Testing database managers...")
    
    # Create test directory
    os.makedirs('test_data', exist_ok=True)
    
    # Initialize the crawler database manager
    crawler_db_path = os.path.join('test_data', 'test_crawler.db')
    logger.info(f"Initializing crawler database at: {crawler_db_path}")
    crawler_db = DBManager(db_path=crawler_db_path)
    
    # Initialize the main database manager
    main_db_path = os.path.join('test_data', 'test_main.db')
    logger.info(f"Initializing main database at: {main_db_path}")
    main_db = MainDBManager(db_path=main_db_path)
    
    # Verify that both databases were created
    logger.info("Checking if database files were created...")
    if os.path.exists(crawler_db_path):
        logger.info(f"✓ Crawler database created at: {crawler_db_path}")
    else:
        logger.error(f"✗ Crawler database not created at: {crawler_db_path}")
    
    if os.path.exists(main_db_path):
        logger.info(f"✓ Main database created at: {main_db_path}")
    else:
        logger.error(f"✗ Main database not created at: {main_db_path}")
    
    # Test crawler database operations
    logger.info("Testing crawler database operations...")
    try:
        # Add a test URL
        crawler_db.add_url(
            url="https://example.com",
            domain="example.com",
            status_code=200,
            content_type="text/html",
            title="Example Domain",
            links=["https://example.com/page1", "https://example.com/page2"],
            depth=0
        )
        logger.info("✓ Successfully added URL to crawler database")
    except Exception as e:
        logger.error(f"✗ Failed to add URL to crawler database: {e}")
    
    # Test main database operations
    logger.info("Testing main database operations...")
    try:
        # Add a test competitor
        competitor_id = main_db.add_competitor(
            name="Test Competitor",
            website="https://example.com",
            description="A test competitor"
        )
        if competitor_id:
            logger.info(f"✓ Successfully added competitor to main database with ID: {competitor_id}")
        else:
            logger.warning("Competitor may already exist in the database")
    except Exception as e:
        logger.error(f"✗ Failed to add competitor to main database: {e}")
    
    logger.info("Database manager tests completed.")

if __name__ == "__main__":
    main()
