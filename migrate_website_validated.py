#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Migration script to add website_validated field to Manufacturer table
"""

import os
import sys
import logging
import argparse
from sqlalchemy import create_engine, Column, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('ndaivi.migration')

# Import database components
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.schema import Manufacturer, get_db_engine
from database.db_manager import DatabaseManager, get_db_manager

def migrate_website_validated(db_path):
    """
    Add website_validated column to Manufacturer table and set initial values.
    
    Args:
        db_path: Path to the SQLite database file
    """
    logger.info("Starting migration to add website_validated field to Manufacturer table")
    
    try:
        # Create engine and session
        engine = get_db_engine(db_path)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Check if column already exists
        inspector = inspect(engine)
        columns = [c['name'] for c in inspector.get_columns('manufacturers')]
        
        if 'website_validated' not in columns:
            # Add the column
            logger.info("Adding website_validated column to Manufacturer table")
            with engine.connect() as conn:
                conn.execute(text("ALTER TABLE manufacturers ADD COLUMN website_validated BOOLEAN DEFAULT 0"))
            
            # Update existing records - assume existing websites are not validated
            logger.info("Setting initial values for website_validated field")
            session.query(Manufacturer).filter(
                Manufacturer.website != None, 
                Manufacturer.website != ''
            ).update({Manufacturer.website_validated: False})
            
            session.commit()
            logger.info("Migration completed successfully")
        else:
            logger.info("website_validated column already exists, skipping migration")
            
    except Exception as e:
        logger.error(f"Error during migration: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='NDAIVI Website Validated Migration')
    parser.add_argument('--config', default='config.yaml', help='Path to configuration file')
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config_path = os.path.abspath(args.config)
        if not os.path.exists(config_path):
            logger.error(f"Config file not found: {config_path}")
            sys.exit(1)
            
        import yaml
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            
        # Get database path
        db_path = os.path.abspath(config['database']['path'])
        logger.info(f"Using database at: {db_path}")
        
        # Run migration
        migrate_website_validated(db_path)
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # Add missing imports for the migration script
    from sqlalchemy import inspect, text
    main()
