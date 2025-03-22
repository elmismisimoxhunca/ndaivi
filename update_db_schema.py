#!/usr/bin/env python3

"""
Database Schema Update Script for NDAIVI

This script updates the database schema to add new columns to the ScraperSession table.
"""

import os
import sys
import logging
import sqlite3
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('db_update')

def update_scraper_sessions_table(db_path):
    """
    Update the scraper_sessions table to add new columns.
    
    Args:
        db_path: Path to the SQLite database file
    """
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if columns exist before adding them
        cursor.execute("PRAGMA table_info(scraper_sessions)")
        columns = [column[1] for column in cursor.fetchall()]
        
        # Add new columns if they don't exist
        if 'pages_crawled' not in columns:
            logger.info("Adding 'pages_crawled' column to scraper_sessions table")
            cursor.execute("ALTER TABLE scraper_sessions ADD COLUMN pages_crawled INTEGER DEFAULT 0")
        
        if 'pages_analyzed' not in columns:
            logger.info("Adding 'pages_analyzed' column to scraper_sessions table")
            cursor.execute("ALTER TABLE scraper_sessions ADD COLUMN pages_analyzed INTEGER DEFAULT 0")
        
        if 'errors' not in columns:
            logger.info("Adding 'errors' column to scraper_sessions table")
            cursor.execute("ALTER TABLE scraper_sessions ADD COLUMN errors INTEGER DEFAULT 0")
        
        # Commit the changes
        conn.commit()
        logger.info("Database schema updated successfully")
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        return False
    except Exception as e:
        logger.error(f"Error updating database schema: {e}")
        return False
    finally:
        if conn:
            conn.close()
    
    return True

def main():
    """
    Main function to update the database schema.
    """
    # Get the database path from the config or use default
    db_path = os.path.join(os.path.dirname(__file__), 'database', 'manuals.db')
    
    # Check if the database file exists
    if not os.path.exists(db_path):
        logger.error(f"Database file not found at {db_path}")
        return False
    
    # Update the schema
    if update_scraper_sessions_table(db_path):
        logger.info(f"Successfully updated database schema for {db_path}")
        return True
    else:
        logger.error(f"Failed to update database schema for {db_path}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
