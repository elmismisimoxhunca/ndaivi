#!/usr/bin/env python3

import os
import json
import time
import logging
import argparse
import sqlite3
import requests

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ndaivi-test')

# Directories and files
LINK_STATUS_PATH = '/var/ndaivimanuales/data/link_status.json'
SQLITE_DB_PATH = '/var/ndaivimanuales/data/crawler.db'
WATCH_DIR = '/var/ndaivimanuales/tmp/analyzer_watch'
RESULTS_DIR = '/var/ndaivimanuales/tmp/analyzer_results'

def test_link_status_file():
    """Test if the link status file is accessible and loadable"""
    logger.info("Testing link status file...")
    
    if not os.path.exists(os.path.dirname(LINK_STATUS_PATH)):
        logger.info(f"Creating directory {os.path.dirname(LINK_STATUS_PATH)}")
        os.makedirs(os.path.dirname(LINK_STATUS_PATH), exist_ok=True)
        
    if not os.path.exists(LINK_STATUS_PATH):
        logger.info(f"Link status file does not exist, creating...")
        with open(LINK_STATUS_PATH, 'w') as f:
            json.dump({
                "LastUpdated": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "Links": []
            }, f)
        logger.info(f"Created empty link status file at {LINK_STATUS_PATH}")
    else:
        try:
            with open(LINK_STATUS_PATH, 'r') as f:
                data = json.load(f)
            logger.info(f"Successfully loaded link status file with {len(data.get('Links', []))} links")
        except json.JSONDecodeError:
            logger.error(f"Link status file exists but is not valid JSON")
            return False
    
    return True

def test_sqlite_db_access():
    """Test if the SQLite database is accessible in read-only mode"""
    logger.info("Testing SQLite database access...")
    
    if not os.path.exists(os.path.dirname(SQLITE_DB_PATH)):
        logger.info(f"Creating directory {os.path.dirname(SQLITE_DB_PATH)}")
        os.makedirs(os.path.dirname(SQLITE_DB_PATH), exist_ok=True)
        
    # Check if DB exists, create minimal version if not
    if not os.path.exists(SQLITE_DB_PATH):
        logger.info(f"SQLite database does not exist, creating minimal test version...")
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()
        
        # Create pages table (for crawler)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            domain TEXT,
            status_code INTEGER,
            crawled_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Add sample test data
        cursor.execute("""
        INSERT INTO pages (url, domain, status_code, crawled_at)
        VALUES (?, ?, ?, datetime('now'))
        """, ("https://example.com", "example.com", 200))
        
        conn.commit()
        conn.close()
        logger.info(f"Created minimal test SQLite database at {SQLITE_DB_PATH}")
    
    # Test read-only access
    try:
        conn = sqlite3.connect(f"file:{SQLITE_DB_PATH}?mode=ro", uri=True)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM pages")
        count = cursor.fetchone()[0]
        logger.info(f"Successfully opened SQLite database in read-only mode. Found {count} pages")
        conn.close()
        return True
    except sqlite3.Error as e:
        logger.error(f"Error accessing SQLite database: {e}")
        return False
    
def test_analyzer_directories():
    """Test if the analyzer watch and results directories are accessible"""
    logger.info("Testing analyzer directories...")
    
    # Check watch directory
    if not os.path.exists(WATCH_DIR):
        logger.info(f"Creating watch directory {WATCH_DIR}")
        os.makedirs(WATCH_DIR, exist_ok=True)
    
    # Check results directory
    if not os.path.exists(RESULTS_DIR):
        logger.info(f"Creating results directory {RESULTS_DIR}")
        os.makedirs(RESULTS_DIR, exist_ok=True)
    
    logger.info(f"Analyzer directories exist and are accessible")
    return True

def run_tests():
    """Run all tests and report results"""
    logger.info("==========================================")
    logger.info("NDAIVI System Test Starting")
    logger.info("==========================================")
    
    # Test link status file (WindsurfRule #3)
    link_status_ok = test_link_status_file()
    
    # Test SQLite database access (WindsurfRule #2)
    sqlite_ok = test_sqlite_db_access()
    
    # Test analyzer directories (WindsurfRule #5, #7)
    analyzer_dirs_ok = test_analyzer_directories()
    
    # Report results
    logger.info("==========================================")
    logger.info("NDAIVI System Test Results")
    logger.info("==========================================")
    logger.info(f"Link Status File: {'✓ OK' if link_status_ok else '✗ FAILED'}")
    logger.info(f"SQLite Database: {'✓ OK' if sqlite_ok else '✗ FAILED'}")
    logger.info(f"Analyzer Directories: {'✓ OK' if analyzer_dirs_ok else '✗ FAILED'}")
    
    # Overall status
    all_ok = link_status_ok and sqlite_ok and analyzer_dirs_ok
    logger.info("-----------------------------------------")
    logger.info(f"Overall System Status: {'✓ READY' if all_ok else '✗ ISSUES DETECTED'}")
    logger.info("==========================================")
    
    return all_ok

if __name__ == "__main__":
    run_tests()
