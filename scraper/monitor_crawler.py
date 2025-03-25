#!/usr/bin/env python3
"""
Crawler monitoring script.
This script connects to the crawler database and provides real-time statistics
about the crawling process.
"""

import os
import sys
import time
import sqlite3
import argparse
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.config_manager import ConfigManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('crawler_monitor')

class CrawlerMonitor:
    """Monitor for the web crawler database."""
    
    def __init__(self, db_path: str):
        """
        Initialize the crawler monitor.
        
        Args:
            db_path: Path to the crawler SQLite database
        """
        self.db_path = db_path
        self.conn = None
        self.start_time = datetime.now()
        self.previous_stats = {}
        
    def connect(self) -> bool:
        """
        Connect to the crawler database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            if not os.path.exists(self.db_path):
                logger.error(f"Database file not found: {self.db_path}")
                return False
                
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            return True
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get current crawler statistics from the database.
        
        Returns:
            Dict containing crawler statistics
        """
        stats = {
            'urls_queued': 0,
            'urls_processed': 0,
            'urls_failed': 0,
            'domains_discovered': 0,
            'elapsed_time': (datetime.now() - self.start_time).total_seconds()
        }
        
        if not self.conn:
            return stats
            
        try:
            # Get queue count
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM url_queue")
            stats['urls_queued'] = cursor.fetchone()[0]
            
            # Get processed count
            cursor.execute("SELECT COUNT(*) FROM visited_urls")
            stats['urls_processed'] = cursor.fetchone()[0]
            
            # Get failed count
            cursor.execute("SELECT COUNT(*) FROM visited_urls WHERE status_code >= 400 OR status_code = 0")
            stats['urls_failed'] = cursor.fetchone()[0]
            
            # Get domains count
            cursor.execute("SELECT COUNT(*) FROM domains")
            stats['domains_discovered'] = cursor.fetchone()[0]
            
            # Get rate calculations
            if self.previous_stats and stats['elapsed_time'] > 0:
                time_diff = stats['elapsed_time'] - self.previous_stats.get('elapsed_time', 0)
                if time_diff > 0:
                    urls_diff = stats['urls_processed'] - self.previous_stats.get('urls_processed', 0)
                    stats['crawl_rate'] = urls_diff / time_diff
            
            # Store current stats for next comparison
            self.previous_stats = stats.copy()
            
            return stats
        except sqlite3.Error as e:
            logger.error(f"Error fetching stats: {e}")
            return stats
    
    def display_stats(self, stats: Dict[str, Any]) -> None:
        """
        Display crawler statistics.
        
        Args:
            stats: Dictionary containing crawler statistics
        """
        os.system('clear' if os.name == 'posix' else 'cls')
        print(f"=== NDAIVI Crawler Monitor ===")
        print(f"Database: {self.db_path}")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Elapsed: {int(stats['elapsed_time'] // 3600):02d}:{int((stats['elapsed_time'] % 3600) // 60):02d}:{int(stats['elapsed_time'] % 60):02d}")
        print(f"")
        print(f"URLs Queued:       {stats['urls_queued']}")
        print(f"URLs Processed:    {stats['urls_processed']}")
        print(f"URLs Failed:       {stats['urls_failed']}")
        print(f"Domains:           {stats['domains_discovered']}")
        
        if 'crawl_rate' in stats:
            print(f"Crawl Rate:        {stats['crawl_rate']:.2f} URLs/second")
        
        print(f"")
        print(f"Press Ctrl+C to exit")
    
    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()

def main():
    """Main function to run the crawler monitor."""
    parser = argparse.ArgumentParser(description='Monitor the web crawler database')
    parser.add_argument('--db-path', type=str, help='Path to the crawler database')
    parser.add_argument('--interval', type=int, default=2, help='Update interval in seconds')
    args = parser.parse_args()
    
    # Get database path from config if not provided
    db_path = args.db_path
    if not db_path:
        config_manager = ConfigManager()
        crawler_config = config_manager.get_config('web_crawler')
        if crawler_config:
            db_path = crawler_config.get('db_path')
    
    if not db_path:
        logger.error("No database path provided or found in configuration")
        return
    
    # Resolve relative path if needed
    if not os.path.isabs(db_path):
        db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', db_path))
    
    # Create monitor and connect to database
    monitor = CrawlerMonitor(db_path)
    
    if not monitor.connect():
        logger.error(f"Failed to connect to database: {db_path}")
        return
    
    try:
        while True:
            stats = monitor.get_stats()
            monitor.display_stats(stats)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Monitor stopped by user")
    except Exception as e:
        logger.exception(f"Error during monitoring: {e}")
    finally:
        monitor.close()

if __name__ == "__main__":
    main()
