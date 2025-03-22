#!/usr/bin/env python3

"""
Database migration script for NDAIVI

This script updates the database schema to include new columns added to the ScraperSession table.
"""

import os
import sqlite3
import logging
import argparse
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('db_migration')

# Default database path
DEFAULT_DB_PATH = 'database/manuals.db'

def migrate_database(db_path):
    """
    Migrate the database schema to add new columns to the scraper_sessions table.
    
    Args:
        db_path: Path to the SQLite database file
    """
    try:
        # Connect to the database
        logger.info(f"Connecting to database at {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get existing columns in the scraper_sessions table
        cursor.execute("PRAGMA table_info(scraper_sessions)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        logger.info(f"Existing columns: {existing_columns}")
        
        # Define new columns to add
        new_columns = {
            'runtime_seconds': 'INTEGER DEFAULT 0',
            'manufacturers_found': 'INTEGER DEFAULT 0',
            'translations': 'INTEGER DEFAULT 0',
            'manual_links_found': 'INTEGER DEFAULT 0',
            'http_errors': 'INTEGER DEFAULT 0',
            'connection_errors': 'INTEGER DEFAULT 0',
            'timeout_errors': 'INTEGER DEFAULT 0',
            'dns_resolution_errors': 'INTEGER DEFAULT 0',
            'max_runtime_minutes': 'INTEGER'
        }
        
        # Add missing columns
        for column_name, column_type in new_columns.items():
            if column_name not in existing_columns:
                logger.info(f"Adding column {column_name} to scraper_sessions table")
                cursor.execute(f"ALTER TABLE scraper_sessions ADD COLUMN {column_name} {column_type}")
            else:
                logger.info(f"Column {column_name} already exists in scraper_sessions table")
        
        # Commit changes
        conn.commit()
        logger.info("Database migration completed successfully")
        
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(description='Migrate NDAIVI database schema')
    parser.add_argument('--db-path', type=str, default=DEFAULT_DB_PATH,
                        help=f'Path to the SQLite database file (default: {DEFAULT_DB_PATH})')
    args = parser.parse_args()
    
    # Resolve the database path
    db_path = Path(args.db_path)
    if not db_path.is_absolute():
        # If path is relative, make it relative to the project root
        project_root = Path(__file__).parent.parent
        db_path = project_root / db_path
    
    # Check if database file exists
    if not db_path.exists():
        logger.error(f"Database file not found at {db_path}")
        return 1
    
    # Run migration
    try:
        migrate_database(str(db_path))
        return 0
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return 1

if __name__ == '__main__':
    exit(main())
