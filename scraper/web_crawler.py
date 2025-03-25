#!/usr/bin/env python3
"""
Web Crawler for NDAIVI

This module provides a standalone web crawler implementation that can be used
independently from the competitor scraper. It handles URL management, content
extraction, and crawling logic.

Features:
- Priority-based URL queue
- Domain validation and restrictions
- Configurable crawl depth and limits
- Robust error handling and logging
- Session management
- SQLite database for state persistence
- Stats events for monitoring
"""

import requests
import logging
import time
import os
import heapq
from urllib.parse import urlparse, urljoin, ParseResult, parse_qs, urlencode
import datetime
import signal
import threading
import sqlite3
import json
from typing import Optional, List, Dict, Any, Tuple, Set, Callable
from collections import defaultdict
from bs4 import BeautifulSoup
import re
import traceback
from requests.exceptions import RequestException
import sys
from urllib.robotparser import RobotFileParser

# Add the parent directory to the path to import from utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config_manager import ConfigManager

# Global flags for signal handling
SHUTDOWN_FLAG = False
SUSPEND_FLAG = False

def handle_interrupt(signum, frame):
    """
    Handle keyboard interrupt (Ctrl+C) gracefully.
    
    Args:
        signum: Signal number
        frame: Current stack frame
    """
    global SHUTDOWN_FLAG
    print("\nShutdown signal received. Stopping gracefully...")
    SHUTDOWN_FLAG = True

def handle_suspend(signum, frame):
    """
    Handle suspension signal (SIGUSR1) to pause processing.
    
    Args:
        signum: Signal number
        frame: Current stack frame
    """
    global SUSPEND_FLAG
    SUSPEND_FLAG = not SUSPEND_FLAG
    status = "suspended" if SUSPEND_FLAG else "resumed"
    print(f"\nCrawler {status}.")

# Register global signal handlers
signal.signal(signal.SIGINT, handle_interrupt)  # Ctrl+C
signal.signal(signal.SIGTERM, handle_interrupt)  # kill command
signal.signal(signal.SIGUSR1, handle_suspend)  # SIGUSR1 for suspension


class CrawlDatabase:
    """
    Database manager for crawl state persistence.
    
    This class handles all database operations for storing and retrieving
    crawl state, including visited URLs, queue, and sitemaps.
    """
    
    def __init__(self, db_path: str, logger=None):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to the SQLite database file
            logger: Logger instance for logging database activities
        """
        self.db_path = db_path
        self.logger = logger or logging.getLogger(__name__)
        self.conn = None
        self.cursor = None
        self.initialize_database()
    
    def initialize_database(self):
        """
        Initialize the database connection and create tables if they don't exist.
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            # Connect to database
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            
            # Create tables
            self._create_tables()
            
            self.logger.info(f"Database initialized at {self.db_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise
    
    def _create_tables(self):
        """
        Create the necessary tables for crawl state persistence.
        """
        # Create visited URLs table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS visited_urls (
                url TEXT PRIMARY KEY,
                domain TEXT,
                depth INTEGER,
                status_code INTEGER,
                content_type TEXT,
                content_length INTEGER,
                visited_at TIMESTAMP,
                analyzed BOOLEAN DEFAULT 0,
                analyzed_at TIMESTAMP
            )
        ''')
        
        # Create URL queue table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS url_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                domain TEXT NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                depth INTEGER NOT NULL,
                priority REAL NOT NULL,
                source_url TEXT,
                anchor_text TEXT,
                metadata TEXT
            )
        ''')
        
        # Create sitemaps table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS sitemaps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT UNIQUE NOT NULL,
                sitemap_url TEXT NOT NULL,
                last_fetched TIMESTAMP,
                content TEXT
            )
        ''')
        
        # Create domains table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS domains (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT UNIQUE NOT NULL,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_crawled TIMESTAMP,
                robots_txt TEXT,
                url_count INTEGER DEFAULT 0,
                is_allowed BOOLEAN DEFAULT 1
            )
        ''')
        
        # Create crawl stats table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS crawl_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                urls_processed INTEGER DEFAULT 0,
                urls_queued INTEGER DEFAULT 0,
                urls_failed INTEGER DEFAULT 0,
                domains_visited INTEGER DEFAULT 0,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                stats_data TEXT
            )
        ''')
        
        # Commit changes
        self.conn.commit()
    
    def close(self):
        """
        Close the database connection.
        """
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")
    
    def __del__(self):
        """
        Ensure database connection is closed when object is deleted.
        """
        self.close()


class VisitedUrlManager:
    """
    Manager for visited URLs in the crawler database.
    """
    
    def __init__(self, db: CrawlDatabase, logger=None):
        """
        Initialize the visited URL manager.
        
        Args:
            db: Database manager instance
            logger: Logger instance for logging activities
        """
        self.db = db
        self.logger = logger or logging.getLogger(__name__)
        self._cache = set()  # In-memory cache for faster lookups
    
    def add(self, url: str, domain: str, depth: int, status_code: int, 
            content_type: str = None, content_length: int = 0) -> bool:
        """
        Add a URL to the visited URLs list.
        
        Args:
            url: URL that was visited
            domain: Domain of the URL
            depth: Depth at which the URL was visited
            status_code: HTTP status code from the visit
            content_type: Content type of the response
            content_length: Content length in bytes
            
        Returns:
            True if URL was added, False otherwise
        """
        try:
            # Add to in-memory cache
            self._cache.add(url)
            
            # Add to database
            self.db.cursor.execute('''
                INSERT INTO visited_urls 
                (url, domain, depth, status_code, content_type, content_length, visited_at, analyzed, analyzed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (url, domain, depth, status_code, content_type, content_length, 
                  datetime.datetime.now().isoformat(), False, None))
            self.db.conn.commit()
            
            # Update domain stats
            self.db.cursor.execute('''
                UPDATE domains SET 
                last_crawled = CURRENT_TIMESTAMP,
                url_count = url_count + 1
                WHERE domain = ?
            ''', (domain,))
            self.db.conn.commit()
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to add visited URL {url}: {e}")
            return False
    
    def exists(self, url: str) -> bool:
        """
        Check if a URL exists in the visited URLs list.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL exists, False otherwise
        """
        # Check in-memory cache first for performance
        if url in self._cache:
            return True
        
        try:
            # Check database
            self.db.cursor.execute('SELECT 1 FROM visited_urls WHERE url = ? LIMIT 1', (url,))
            result = self.db.cursor.fetchone()
            
            # If found in database, add to cache
            if result:
                self._cache.add(url)
                return True
            
            return False
        except Exception as e:
            self.logger.error(f"Failed to check if URL {url} exists: {e}")
            return False
    
    def mark_as_analyzed(self, url: str) -> bool:
        """
        Mark a URL as analyzed.
        
        Args:
            url: URL to mark as analyzed
            
        Returns:
            True if URL was marked as analyzed, False otherwise
        """
        try:
            self.db.cursor.execute('''
                UPDATE visited_urls 
                SET analyzed = 1, analyzed_at = ?
                WHERE url = ?
            ''', (datetime.datetime.now().isoformat(), url))
            self.db.conn.commit()
            return self.db.cursor.rowcount > 0
        except Exception as e:
            self.logger.error(f"Failed to mark URL {url} as analyzed: {e}")
            return False
    
    def get_unanalyzed_urls(self, limit: int = 10) -> List[Dict]:
        """
        Get a list of URLs that have been visited but not analyzed.
        
        Args:
            limit: Maximum number of URLs to return
            
        Returns:
            List of dictionaries with URL information
        """
        try:
            self.db.cursor.execute('''
                SELECT url, domain, depth, status_code, content_type, content_length, visited_at
                FROM visited_urls
                WHERE analyzed = 0 AND status_code = 200
                ORDER BY visited_at ASC
                LIMIT ?
            ''', (limit,))
            
            results = []
            for row in self.db.cursor.fetchall():
                results.append({
                    'url': row[0],
                    'domain': row[1],
                    'depth': row[2],
                    'status_code': row[3],
                    'content_type': row[4],
                    'content_length': row[5],
                    'visited_at': row[6]
                })
            return results
        except Exception as e:
            self.logger.error(f"Failed to get unanalyzed URLs: {e}")
            return []
    
    def get_stats(self) -> Dict:
        """
        Get statistics about visited URLs.
        
        Returns:
            Dictionary with statistics
        """
        stats = {
            'total': 0,
            'analyzed': 0,
            'unanalyzed': 0,
            'success': 0,
            'error': 0
        }
        
        try:
            self.db.cursor.execute('SELECT COUNT(*) FROM visited_urls')
            stats['total'] = self.db.cursor.fetchone()[0]
            
            self.db.cursor.execute('SELECT COUNT(*) FROM visited_urls WHERE analyzed = 1')
            stats['analyzed'] = self.db.cursor.fetchone()[0]
            
            self.db.cursor.execute('SELECT COUNT(*) FROM visited_urls WHERE analyzed = 0')
            stats['unanalyzed'] = self.db.cursor.fetchone()[0]
            
            self.db.cursor.execute('SELECT COUNT(*) FROM visited_urls WHERE status_code >= 200 AND status_code < 300')
            stats['success'] = self.db.cursor.fetchone()[0]
            
            self.db.cursor.execute('SELECT COUNT(*) FROM visited_urls WHERE status_code >= 400 OR status_code = 0')
            stats['error'] = self.db.cursor.fetchone()[0]
            
            return stats
        except Exception as e:
            self.logger.error(f"Failed to get visited URL stats: {e}")
            return stats


class UrlQueueManager:
    """
    Manager for URL queue in the crawl database.
    """
    
    def __init__(self, db: CrawlDatabase, logger=None):
        """
        Initialize the URL queue manager.
        
        Args:
            db: Database manager instance
            logger: Logger instance for logging activities
        """
        self.db = db
        self.logger = logger or logging.getLogger(__name__)
        self._url_set = set()  # In-memory set for fast lookups
    
    def add(self, url: str, domain: str, depth: int, priority: float, 
            source_url: str = None, anchor_text: str = None, metadata: Dict = None) -> bool:
        """
        Add a URL to the queue.
        
        Args:
            url: URL to add
            domain: Domain of the URL
            depth: Depth for the URL
            priority: Priority for the URL (lower is higher priority)
            source_url: URL where this URL was found
            anchor_text: Anchor text of the link
            metadata: Additional metadata
            
        Returns:
            True if URL was added, False otherwise
        """
        try:
            # Skip if already in queue
            if url in self._url_set:
                return False
            
            # Add to in-memory set
            self._url_set.add(url)
            
            # Add to database
            metadata_json = json.dumps(metadata) if metadata else None
            self.db.cursor.execute('''
                INSERT OR IGNORE INTO url_queue 
                (url, domain, depth, priority, source_url, anchor_text, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (url, domain, depth, priority, source_url, anchor_text, metadata_json))
            self.db.conn.commit()
            
            # Ensure domain exists
            self.db.cursor.execute('''
                INSERT OR IGNORE INTO domains (domain, first_seen)
                VALUES (?, CURRENT_TIMESTAMP)
            ''', (domain,))
            self.db.conn.commit()
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to add URL {url} to queue: {e}")
            return False
    
    def get_next(self, domain: str = None) -> Dict:
        """
        Get the next URL from the queue with highest priority.
        
        Args:
            domain: Optional domain to filter by
            
        Returns:
            Dictionary with URL data or None if queue is empty
        """
        try:
            query = '''
                SELECT * FROM url_queue 
                WHERE id = (
                    SELECT id FROM url_queue
            '''
            
            params = []
            if domain:
                query += ' WHERE domain = ?'
                params.append(domain)
            
            query += ' ORDER BY priority ASC, added_at ASC LIMIT 1)'
            
            self.db.cursor.execute(query, params)
            row = self.db.cursor.fetchone()
            
            if not row:
                return None
            
            # Convert row to dictionary
            columns = [desc[0] for desc in self.db.cursor.description]
            result = dict(zip(columns, row))
            
            # Parse metadata
            if result.get('metadata'):
                result['metadata'] = json.loads(result['metadata'])
            
            # Remove the URL from the queue
            self.remove(result['url'])
            
            # Remove from in-memory set
            if result['url'] in self._url_set:
                self._url_set.remove(result['url'])
            
            return result
        except Exception as e:
            self.logger.error(f"Failed to get next URL from queue: {e}")
            return None
    
    def remove(self, url: str) -> bool:
        """
        Remove a URL from the queue.
        
        Args:
            url: URL to remove
            
        Returns:
            True if URL was removed, False otherwise
        """
        try:
            # Remove from in-memory set
            if url in self._url_set:
                self._url_set.remove(url)
            
            # Remove from database
            self.db.cursor.execute('DELETE FROM url_queue WHERE url = ?', (url,))
            self.db.conn.commit()
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to remove URL {url} from queue: {e}")
            return False
    
    def is_queued(self, url: str) -> bool:
        """
        Check if a URL is in the queue.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL is in the queue, False otherwise
        """
        # Check in-memory set first for performance
        if url in self._url_set:
            return True
        
        try:
            # Check database
            self.db.cursor.execute('SELECT 1 FROM url_queue WHERE url = ? LIMIT 1', (url,))
            result = self.db.cursor.fetchone()
            
            # If found in database, add to in-memory set
            if result:
                self._url_set.add(url)
                return True
            
            return False
        except Exception as e:
            self.logger.error(f"Failed to check if URL {url} is queued: {e}")
            return False
    
    def get_count(self, domain: str = None) -> int:
        """
        Get the number of URLs in the queue.
        
        Args:
            domain: Optional domain to filter by
            
        Returns:
            Number of URLs in the queue
        """
        try:
            if domain:
                self.db.cursor.execute('SELECT COUNT(*) FROM url_queue WHERE domain = ?', (domain,))
            else:
                self.db.cursor.execute('SELECT COUNT(*) FROM url_queue')
            
            result = self.db.cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            self.logger.error(f"Failed to get queue count: {e}")
            return 0
    
    def clear(self, domain: str = None) -> bool:
        """
        Clear the URL queue, optionally filtered by domain.
        
        Args:
            domain: Optional domain to filter by
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if domain:
                self.db.cursor.execute('DELETE FROM url_queue WHERE domain = ?', (domain,))
            else:
                self.db.cursor.execute('DELETE FROM url_queue')
            
            self.db.conn.commit()
            
            # Clear in-memory set
            self._url_set.clear()
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to clear URL queue: {e}")
            return False
    
    def get_stats(self) -> Dict[str, int]:
        """
        Get statistics about the URL queue.
        
        Returns:
            Dictionary with statistics
        """
        try:
            # Get total count
            self.db.cursor.execute('SELECT COUNT(*) FROM url_queue')
            total = self.db.cursor.fetchone()[0]
            
            # Get domain counts
            self.db.cursor.execute('SELECT domain, COUNT(*) FROM url_queue GROUP BY domain')
            domain_counts = {row[0]: row[1] for row in self.db.cursor.fetchall()}
            
            # Get priority distribution
            self.db.cursor.execute('''
                SELECT 
                    CASE 
                        WHEN priority < 10 THEN 'high'
                        WHEN priority < 50 THEN 'medium'
                        ELSE 'low'
                    END as priority_level,
                    COUNT(*) 
                FROM url_queue 
                GROUP BY priority_level
            ''')
            priority_counts = {row[0]: row[1] for row in self.db.cursor.fetchall()}
            
            # Get depth distribution
            self.db.cursor.execute('''
                SELECT depth, COUNT(*) FROM url_queue GROUP BY depth
            ''')
            depth_counts = {f"depth_{row[0]}": row[1] for row in self.db.cursor.fetchall()}
            
            # Combine all stats
            stats = {
                'total': total,
                'domains': domain_counts,
                'priority': priority_counts,
                'depth': depth_counts
            }
            
            return stats
        except Exception as e:
            self.logger.error(f"Failed to get URL queue stats: {e}")
            return {'total': 0}


class DomainManager:
    """
    Manager for domains in the crawl database.
    """
    
    def __init__(self, db: CrawlDatabase, logger=None):
        """
        Initialize the domain manager.
        
        Args:
            db: Database manager instance
            logger: Logger instance for logging activities
        """
        self.db = db
        self.logger = logger or logging.getLogger(__name__)
    
    def add(self, domain: str, robots_txt: str = None, is_allowed: bool = True) -> bool:
        """
        Add a domain to the database.
        
        Args:
            domain: Domain to add
            robots_txt: Content of robots.txt
            is_allowed: Whether crawling is allowed for this domain
            
        Returns:
            True if domain was added, False otherwise
        """
        try:
            # Check if domain already exists
            if self.exists(domain):
                # Update existing domain
                return self.update_robots_txt(domain, robots_txt) and self.set_allowed(domain, is_allowed)
            
            # Add new domain
            self.db.cursor.execute('''
                INSERT INTO domains (domain, robots_txt, is_allowed, first_seen, last_seen)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ''', (domain, robots_txt, is_allowed))
            self.db.conn.commit()
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to add domain {domain}: {e}")
            return False
    
    def exists(self, domain: str) -> bool:
        """
        Check if a domain exists in the database.
        
        Args:
            domain: Domain to check
            
        Returns:
            True if domain exists, False otherwise
        """
        try:
            self.db.cursor.execute('SELECT 1 FROM domains WHERE domain = ?', (domain,))
            return self.db.cursor.fetchone() is not None
        except Exception as e:
            self.logger.error(f"Failed to check if domain {domain} exists: {e}")
            return False
    
    def is_allowed(self, domain: str) -> bool:
        """
        Check if a domain is allowed for crawling.
        
        Args:
            domain: Domain to check
            
        Returns:
            True if domain is allowed, False otherwise
        """
        try:
            self.db.cursor.execute('SELECT is_allowed FROM domains WHERE domain = ?', (domain,))
            result = self.db.cursor.fetchone()
            
            if result is None:
                # Domain not in database, add it with default allowed status
                self.add(domain)
                return True
            
            return bool(result[0])
        except Exception as e:
            self.logger.error(f"Failed to check if domain {domain} is allowed: {e}")
            return False
    
    def get_all(self) -> List[Dict]:
        """
        Get all domains in the database.
        
        Returns:
            List of dictionaries with domain data
        """
        try:
            self.db.cursor.execute('SELECT * FROM domains ORDER BY first_seen DESC')
            rows = self.db.cursor.fetchall()
            
            # Convert rows to dictionaries
            columns = [desc[0] for desc in self.db.cursor.description]
            result = []
            for row in rows:
                result.append(dict(zip(columns, row)))
            
            return result
        except Exception as e:
            self.logger.error(f"Failed to get all domains: {e}")
            return []
    
    def update_robots_txt(self, domain: str, robots_txt: str) -> bool:
        """
        Update the robots.txt content for a domain.
        
        Args:
            domain: Domain to update
            robots_txt: Content of robots.txt
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.db.cursor.execute('''
                UPDATE domains SET robots_txt = ? WHERE domain = ?
            ''', (robots_txt, domain))
            self.db.conn.commit()
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to update robots.txt for domain {domain}: {e}")
            return False
    
    def set_allowed(self, domain: str, is_allowed: bool) -> bool:
        """
        Set whether a domain is allowed for crawling.
        
        Args:
            domain: Domain to update
            is_allowed: Whether crawling is allowed
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.db.cursor.execute('''
                UPDATE domains SET is_allowed = ? WHERE domain = ?
            ''', (is_allowed, domain))
            self.db.conn.commit()
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to set allowed status for domain {domain}: {e}")
            return False
    
    def get_url_count(self, domain: str) -> int:
        """
        Get the number of URLs processed for a domain.
        
        Args:
            domain: Domain to check
            
        Returns:
            Number of URLs processed for the domain
        """
        try:
            # Count URLs in visited_urls table
            self.db.cursor.execute('''
                SELECT COUNT(*) FROM visited_urls WHERE domain = ?
            ''', (domain,))
            
            count = self.db.cursor.fetchone()[0]
            return count
        except Exception as e:
            self.logger.error(f"Failed to get URL count for domain {domain}: {e}")
            return 0


class StatsManager:
    """
    Manager for crawl statistics in the database.
    """
    
    def __init__(self, db: CrawlDatabase, logger=None):
        """
        Initialize the stats manager.
        
        Args:
            db: Database manager instance
            logger: Logger instance for logging activities
        """
        self.db = db
        self.logger = logger or logging.getLogger(__name__)
        self.current_stats_id = None
        self.stats_callbacks = []
    
    def start_session(self) -> int:
        """
        Start a new crawl session and return its ID.
        
        Returns:
            ID of the new session
        """
        try:
            self.db.cursor.execute('''
                INSERT INTO crawl_stats (start_time, urls_processed, urls_queued, urls_failed, domains_visited)
                VALUES (CURRENT_TIMESTAMP, 0, 0, 0, 0)
            ''')
            self.db.conn.commit()
            
            self.current_stats_id = self.db.cursor.lastrowid
            return self.current_stats_id
        except Exception as e:
            self.logger.error(f"Failed to start stats session: {e}")
            return None
    
    def end_session(self, stats_data: Dict) -> bool:
        """
        End the current crawl session.
        
        Args:
            stats_data: Additional statistics data
            
        Returns:
            True if successful, False otherwise
        """
        if not self.current_stats_id:
            return False
        
        try:
            stats_json = json.dumps(stats_data)
            self.db.cursor.execute('''
                UPDATE crawl_stats SET 
                end_time = CURRENT_TIMESTAMP,
                stats_data = ?
                WHERE id = ?
            ''', (stats_json, self.current_stats_id))
            self.db.conn.commit()
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to end stats session: {e}")
            return False
    
    def update_stats(self, urls_processed: int = None, urls_queued: int = None, 
                    urls_failed: int = None, domains_visited: int = None) -> bool:
        """
        Update the current session statistics.
        
        Args:
            urls_processed: Number of URLs processed
            urls_queued: Number of URLs in the queue
            urls_failed: Number of URLs that failed
            domains_visited: Number of domains visited
            
        Returns:
            True if successful, False otherwise
        """
        if not self.current_stats_id:
            return False
        
        try:
            # Build the update query
            query = 'UPDATE crawl_stats SET '
            params = []
            
            if urls_processed is not None:
                query += 'urls_processed = ?, '
                params.append(urls_processed)
            
            if urls_queued is not None:
                query += 'urls_queued = ?, '
                params.append(urls_queued)
            
            if urls_failed is not None:
                query += 'urls_failed = ?, '
                params.append(urls_failed)
            
            if domains_visited is not None:
                query += 'domains_visited = ?, '
                params.append(domains_visited)
            
            # Remove trailing comma and space
            query = query.rstrip(', ')
            
            # Add WHERE clause
            query += ' WHERE id = ?'
            params.append(self.current_stats_id)
            
            # Execute the query
            self.db.cursor.execute(query, params)
            self.db.conn.commit()
            
            # Get current stats for callbacks
            self.db.cursor.execute('SELECT * FROM crawl_stats WHERE id = ?', (self.current_stats_id,))
            row = self.db.cursor.fetchone()
            
            if row:
                # Convert row to dictionary
                columns = [desc[0] for desc in self.db.cursor.description]
                stats = dict(zip(columns, row))
                
                # Parse stats_data
                if stats.get('stats_data'):
                    stats['stats_data'] = json.loads(stats['stats_data'])
                
                # Call all registered callbacks
                for callback in self.stats_callbacks:
                    try:
                        callback(stats)
                    except Exception as e:
                        self.logger.error(f"Error in stats callback: {e}")
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to update stats: {e}")
            return False
    
    def update(self, stats: Dict[str, Any]) -> bool:
        """
        Update statistics and notify callbacks.
        
        Args:
            stats: Dictionary with current statistics
            
        Returns:
            True if successful, False otherwise
        """
        # Update database stats if we have a session
        if self.current_stats_id:
            self.update_stats(
                urls_processed=stats.get('urls_processed'),
                urls_queued=stats.get('urls_queued'),
                urls_failed=stats.get('urls_failed'),
                domains_visited=stats.get('domains_discovered')
            )
        
        # Notify callbacks
        for callback in self.stats_callbacks:
            try:
                callback(stats)
            except Exception as e:
                self.logger.error(f"Error in stats callback: {e}")
        
        return True
    
    def register_stats_callback(self, callback: Callable[[Dict], None]) -> None:
        """
        Register a callback function for stats updates.
        
        Args:
            callback: Function to call with stats updates
        """
        if callback not in self.stats_callbacks:
            self.stats_callbacks.append(callback)
    
    def unregister_stats_callback(self, callback: Callable[[Dict], None]) -> None:
        """
        Unregister a previously registered callback function.
        
        Args:
            callback: Function to unregister
        """
        if callback in self.stats_callbacks:
            self.stats_callbacks.remove(callback)
    
    def get_latest_stats(self) -> Dict:
        """
        Get the latest crawl statistics.
        
        Returns:
            Dictionary with latest stats
        """
        try:
            self.db.cursor.execute('SELECT * FROM crawl_stats ORDER BY id DESC LIMIT 1')
            row = self.db.cursor.fetchone()
            
            if not row:
                return {}
            
            # Convert row to dictionary
            columns = [desc[0] for desc in self.db.cursor.description]
            stats = dict(zip(columns, row))
            
            # Parse stats_data
            if stats.get('stats_data'):
                stats['stats_data'] = json.loads(stats['stats_data'])
            
            return stats
        except Exception as e:
            self.logger.error(f"Failed to get latest stats: {e}")
            return {}


class UrlPriority:
    """
    Utility class for calculating URL priorities.
    """
    
    # Priority weights
    DEPTH_WEIGHT = 5.0
    KEYWORD_WEIGHT = 10.0
    PATH_LENGTH_WEIGHT = 1.0
    QUERY_PARAMS_WEIGHT = 2.0
    DOMAIN_WEIGHT = 8.0
    ANCHOR_TEXT_WEIGHT = 15.0
    
    # Keywords that might indicate important pages
    IMPORTANT_KEYWORDS = [
        'manufacturer', 'brand', 'brands', 'company', 'companies', 'vendor', 'vendors',
        'supplier', 'suppliers', 'producer', 'producers', 'maker', 'makers',
        'about', 'about-us', 'about_us', 'aboutus', 'about-company', 'about_company',
        'products', 'product-range', 'product_range', 'product-catalog', 'product_catalog',
        'catalog', 'catalogue', 'portfolio', 'range', 'series', 'models',
        'category', 'categories', 'directory', 'listing'
    ]
    
    @classmethod
    def calculate_priority(cls, url: str, depth: int, anchor_text: str = None, 
                          html_structure: Dict = None) -> float:
        """
        Calculate priority score for a URL. Lower scores have higher priority.
        
        Args:
            url: The URL to calculate priority for
            depth: Current crawl depth
            anchor_text: Optional text of the anchor linking to this URL
            html_structure: Optional info about HTML structure
            
        Returns:
            Priority score (lower is higher priority)
        """
        # Start with base priority from depth
        priority = depth * cls.DEPTH_WEIGHT
        
        try:
            # Parse URL
            parsed_url = urlparse(url)
            
            # Analyze path components
            path = parsed_url.path
            path_components = [p for p in path.split('/') if p]
            
            # Path length penalty (longer paths are lower priority)
            priority += len(path_components) * cls.PATH_LENGTH_WEIGHT
            
            # Query parameters penalty
            if parsed_url.query:
                priority += len(parsed_url.query.split('&')) * cls.QUERY_PARAMS_WEIGHT
            
            # Keyword bonus for paths that might contain important info
            path_str = ' '.join(path_components).lower()
            for keyword in cls.IMPORTANT_KEYWORDS:
                if keyword in path_str:
                    priority -= cls.KEYWORD_WEIGHT
                    break
            
            # Anchor text bonus
            if anchor_text:
                anchor_text = anchor_text.lower()
                for keyword in cls.IMPORTANT_KEYWORDS:
                    if keyword in anchor_text:
                        priority -= cls.ANCHOR_TEXT_WEIGHT
                        break
            
            # Domain bonus for certain domains
            domain = parsed_url.netloc.lower()
            if any(term in domain for term in ['manufacturer', 'brand', 'supplier', 'company']):
                priority -= cls.DOMAIN_WEIGHT
            
            # HTML structure bonus (if available)
            if html_structure:
                # Implement additional priority adjustments based on HTML structure
                pass
            
        except Exception:
            # If there's an error in calculation, use a default high (low priority) value
            return 1000.0
        
        return max(0.0, priority)


class PriorityUrlQueue:
    """
    Priority queue for URLs to be crawled with enhanced prioritization.
    """
    
    def __init__(self):
        """Initialize the priority queue."""
        self._queue = []  # heapq priority queue with entries: (priority, counter, url, depth, metadata)
        self._counter = 0  # Unique counter for stable sorting
        self._url_set = set()  # Fast lookup for URLs
        self._url_metadata = {}  # Store metadata about URLs
    
    def __contains__(self, url):
        """Enable 'in' operator support for direct URL checking
        
        Args:
            url: URL to check
            
        Returns:
            True if the URL is in the queue, False otherwise
        """
        if not url or not isinstance(url, str):
            return False
            
        # Normalize URL for consistent comparison
        normalized_url = url.rstrip('/')
        
        # Check if the normalized URL is in the set
        return normalized_url in self._url_set
    
    def push(self, url: str, depth: int, priority: Optional[float] = None, 
             content_hint: Optional[str] = None, html_structure: Optional[Dict] = None,
             anchor_text: Optional[str] = None) -> None:
        """
        Add a URL to the queue with priority.
        
        Args:
            url: URL to add
            depth: Current crawl depth
            priority: Optional priority override (lower is higher priority)
            content_hint: Optional hint about content type
            html_structure: Optional info about HTML structure
            anchor_text: Optional text of the anchor linking to this URL
        """
        if not url or not isinstance(url, str):
            return
            
        # Normalize URL for consistent comparison
        normalized_url = url.rstrip('/')
        
        # Skip if already in queue
        if normalized_url in self._url_set:
            return
            
        # Calculate priority if not provided
        if priority is None:
            priority = UrlPriority.calculate_priority(
                normalized_url, depth, anchor_text
            )
        
        # Add to queue
        self._counter += 1
        metadata = {
            'content_hint': content_hint,
            'anchor_text': anchor_text,
            'html_structure': html_structure
        }
        
        # Add to heap queue
        heapq.heappush(self._queue, (priority, self._counter, normalized_url, depth, metadata))
        
        # Add to set for fast lookup
        self._url_set.add(normalized_url)
        
        # Store metadata
        self._url_metadata[normalized_url] = metadata
    
    def pop(self) -> Optional[Tuple[str, int, Dict]]:
        """
        Get the highest priority URL from the queue.
        
        Returns:
            Tuple of (url, depth, metadata) or None if queue is empty
        """
        if not self._queue:
            return None
            
        # Pop from heap queue
        priority, counter, url, depth, metadata = heapq.heappop(self._queue)
        
        # Remove from set
        self._url_set.discard(url)
        
        # Remove from metadata
        if url in self._url_metadata:
            del self._url_metadata[url]
            
        return url, depth, metadata
    
    def peek(self) -> Optional[Tuple[str, int, Dict]]:
        """
        Peek at the highest priority URL without removing it.
        
        Returns:
            Tuple of (url, depth, metadata) or None if queue is empty
        """
        if not self._queue:
            return None
            
        # Peek at heap queue
        priority, counter, url, depth, metadata = self._queue[0]
            
        return url, depth, metadata
    
    def is_empty(self) -> bool:
        """
        Check if the queue is empty.
        
        Returns:
            True if the queue is empty, False otherwise
        """
        return len(self._queue) == 0
    
    def __len__(self) -> int:
        """
        Get the number of URLs in the queue.
        
        Returns:
            Number of URLs in the queue
        """
        return len(self._queue)
    
    def get_metadata(self, url: str) -> Optional[Dict]:
        """
        Get metadata for a URL.
        
        Args:
            url: URL to get metadata for
            
        Returns:
            Metadata dictionary or None if URL not in queue
        """
        # Normalize URL for consistent comparison
        normalized_url = url.rstrip('/')
        
        return self._url_metadata.get(normalized_url)
    
    def clear(self) -> None:
        """Clear the queue."""
        self._queue = []
        self._url_set.clear()
        self._url_metadata.clear()
        self._counter = 0


class ContentExtractor:
    """
    Extract and process HTML content from web pages.
    
    This class handles all content extraction operations, including text extraction,
    link extraction, and HTML structure analysis.
    """
    
    def __init__(self, user_agent: str, timeout: int, logger):
        """
        Initialize the ContentExtractor.
        
        Args:
            user_agent: User agent string for HTTP requests
            timeout: Timeout for HTTP requests in seconds
            logger: Logger instance for logging extraction activities
        """
        self.user_agent = user_agent
        self.timeout = timeout
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})
    
    def fetch(self, url: str) -> Tuple[str, Dict]:
        """
        Fetch a URL and return its content and metadata.
        
        Args:
            url: URL to fetch
            
        Returns:
            Tuple of (content, metadata) or raises exception if fetch failed
        """
        result = self.fetch_url(url)
        if result is None:
            raise Exception(f"Failed to fetch URL: {url}")
        
        title, content, metadata = result
        return content, metadata
    
    def fetch_url(self, url: str) -> Optional[Tuple[str, str, Dict]]:
        """
        Fetch a URL and return its content.
        
        Args:
            url: URL to fetch
            
        Returns:
            Tuple of (title, content, metadata) or None if fetch failed
        """
        try:
            self.logger.debug(f"Fetching URL: {url}")
            
            # Fetch URL with timeout
            response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            
            # Check response status
            if response.status_code != 200:
                self.logger.warning(f"Failed to fetch URL {url}: HTTP {response.status_code}")
                return None
                
            # Get content type
            content_type = response.headers.get('Content-Type', '').lower()
            
            # Skip non-HTML content
            if 'text/html' not in content_type:
                self.logger.debug(f"Skipping non-HTML content: {content_type}")
                return None
                
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract title
            title = self.extract_title(soup) or url
            
            # Extract text content
            text_content = self.extract_text_content(soup)
            
            # Extract metadata
            metadata = {
                'url': url,
                'content_type': content_type,
                'html_content': response.text,
                'html_structure': self.extract_html_structure(soup),
                'links': self.extract_links(soup, url)
            }
            
            self.logger.debug(f"Successfully fetched URL: {url}")
            return title, text_content, metadata
            
        except RequestException as e:
            self.logger.warning(f"Request error fetching URL {url}: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching URL {url}: {str(e)}")
            self.logger.error(traceback.format_exc())
            return None
    
    def extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Extract title from a BeautifulSoup object.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Title as string or None if not found
        """
        try:
            # Try to get title tag
            if soup.title and soup.title.string:
                return soup.title.string.strip()
                
            # Try to get h1 tag
            if soup.h1 and soup.h1.text:
                return soup.h1.text.strip()
                
            # Try to get first heading
            for heading in soup.find_all(['h1', 'h2', 'h3']):
                if heading.text:
                    return heading.text.strip()
                    
            return None
        except Exception as e:
            self.logger.error(f"Error extracting title: {str(e)}")
            return None
    
    def extract_text_content(self, soup: BeautifulSoup) -> str:
        """
        Extract readable text content from the page.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Cleaned text content as string
        """
        try:
            # Remove script and style elements
            for script in soup(['script', 'style', 'noscript', 'iframe', 'head']):
                script.extract()
                
            # Get text
            text = soup.get_text(separator=' ', strip=True)
            
            # Clean up text
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            text = ' '.join(lines)
            
            # Remove excessive whitespace
            text = re.sub(r'\s+', ' ', text).strip()
            
            return text
        except Exception as e:
            self.logger.error(f"Error extracting text content: {str(e)}")
            return ""
    
    def extract_links(self, soup: BeautifulSoup, base_url: str) -> Dict[str, str]:
        """
        Extract links from a BeautifulSoup object.
        
        Args:
            soup: BeautifulSoup object of the page
            base_url: Base URL for resolving relative links
            
        Returns:
            Dictionary mapping URLs to their anchor text
        """
        links = {}
        try:
            # Find all links
            for a in soup.find_all('a', href=True):
                href = a['href']
                
                # Skip empty links
                if not href:
                    continue
                    
                # Skip javascript links
                if href.startswith('javascript:'):
                    continue
                    
                # Skip mailto links
                if href.startswith('mailto:'):
                    continue
                    
                # Skip tel links
                if href.startswith('tel:'):
                    continue
                    
                # Skip anchor links
                if href.startswith('#'):
                    continue
                    
                # Resolve relative links
                url = urljoin(base_url, href)
                
                # Get anchor text
                anchor_text = a.get_text(strip=True)
                
                # Add to links
                links[url] = anchor_text
                
            return links
        except Exception as e:
            self.logger.error(f"Error extracting links: {str(e)}")
            return {}
    
    def extract_html_structure(self, soup: BeautifulSoup) -> Dict:
        """
        Extract information about HTML structure for priority calculation.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            Dictionary with counts of relevant HTML elements
        """
        try:
            # Count elements
            structure = {
                'headings': len(soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])),
                'paragraphs': len(soup.find_all('p')),
                'links': len(soup.find_all('a', href=True)),
                'images': len(soup.find_all('img')),
                'lists': len(soup.find_all(['ul', 'ol'])),
                'tables': len(soup.find_all('table')),
                'forms': len(soup.find_all('form')),
                'inputs': len(soup.find_all(['input', 'select', 'textarea'])),
                'divs': len(soup.find_all('div')),
                'spans': len(soup.find_all('span'))
            }
            
            # Get main content area if available
            main_content = soup.find(['main', 'article']) or soup.find('div', {'id': 'content'}) or soup.find('div', {'class': 'content'})
            if main_content:
                structure['main_content_length'] = len(main_content.get_text())
                structure['main_content_links'] = len(main_content.find_all('a', href=True))
            else:
                structure['main_content_length'] = 0
                structure['main_content_links'] = 0
                
            return structure
        except Exception as e:
            self.logger.error(f"Error extracting HTML structure: {str(e)}")
            return {}


class WebCrawler:
    """
    Advanced web crawler with configurable behavior and prioritization.
    
    This crawler focuses on discovering links and storing them in a database.
    It does not perform content analysis, which is handled by external components.
    """
    
    def __init__(self, config=None, stats_callback=None):
        """
        Initialize the web crawler.
        
        Args:
            config: Dictionary with configuration parameters
            stats_callback: Optional callback for stats updates
        """
        # Default configuration
        self.default_config = {
            'max_depth': 3,
            'max_urls_per_domain': 100,
            'max_concurrent_requests': 5,
            'request_delay': 1.0,
            'timeout': 30,
            'user_agent': 'Mozilla/5.0 (compatible; NDaiviBot/1.0; +https://ndaivi.com/bot)',
            'respect_robots_txt': True,
            'follow_redirects': True,
            'allowed_domains': [],
            'disallowed_domains': [],
            'allowed_url_patterns': [],
            'disallowed_url_patterns': [],
            'max_content_size': 5 * 1024 * 1024,  # 5MB
            'extract_metadata': True,
            'extract_links': True,
            'extract_text': True,
            'db_path': 'crawler.db',
            'restrict_to_start_domain': True,
            'single_domain_mode': True,
            'stats_update_interval': 10
        }
        
        # Merge provided config with defaults
        self.config = self.default_config.copy()
        if config:
            self.config.update(config)
        
        # Set up logging
        self.logger = logging.getLogger('scraper.web_crawler')
        
        # Initialize database
        db_path = self.config['db_path']
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        
        self.db = CrawlDatabase(db_path)
        
        # Initialize managers
        self.visited_urls = VisitedUrlManager(self.db, self.logger)
        self.url_queue_manager = UrlQueueManager(self.db, self.logger)
        self.domain_manager = DomainManager(self.db, self.logger)
        self.stats_manager = StatsManager(self.db, self.logger)
        
        # Initialize content extractor
        self.content_extractor = ContentExtractor(
            self.config['user_agent'],
            self.config['timeout'],
            self.logger
        )
        
        # In-memory URL queue for faster processing
        self.url_queue = PriorityUrlQueue()
        
        # Statistics
        self.stats = {
            'start_time': None,
            'end_time': None,
            'urls_processed': 0,
            'urls_queued': 0,
            'urls_failed': 0,
            'domains_discovered': 0,
            'processing_times': [],
            'elapsed_time': 0
        }
        
        # Register stats callback
        if stats_callback:
            self.stats_manager.register_stats_callback(stats_callback)
        
        # Start domain for single domain mode
        self.start_domain = None
        
        # Robots.txt parsers
        self.robots_parsers = {}
    
    def add_url(self, url: str, depth: int = 0, priority: float = 0.0) -> bool:
        """
        Add a URL to the crawler queue.
        
        Args:
            url: URL to add
            depth: Depth of the URL in the crawl tree
            priority: Priority of the URL (higher = more important)
            
        Returns:
            bool: True if URL was added, False otherwise
        """
        # Normalize URL
        url = url.rstrip('/')
        
        self.logger.info(f"Attempting to add URL: {url}")
        
        # Skip if already processed or in queue
        if self.visited_urls.exists(url):
            self.logger.info(f"URL {url} already visited, skipping")
            return False
            
        if self.url_queue_manager.is_queued(url):
            self.logger.info(f"URL {url} already in queue, skipping")
            return False
        
        # Parse URL to get domain
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            
            self.logger.info(f"Parsed domain: {domain}")
            
            # Set start domain if this is the first URL
            if self.start_domain is None and self.config['single_domain_mode']:
                self.start_domain = domain
                self.logger.info(f"Set start domain to {domain}")
            
            # Check if domain is allowed
            if not self._is_domain_allowed(domain):
                self.logger.info(f"Skipping URL {url} - domain {domain} not allowed")
                return False
            
            # Check if URL matches allowed patterns
            if not self._is_url_allowed(url):
                self.logger.info(f"Skipping URL {url} - URL pattern not allowed")
                return False
            
            # Check robots.txt if enabled
            if self.config['respect_robots_txt'] and not self._is_allowed_by_robots(url):
                self.logger.info(f"Skipping URL {url} - disallowed by robots.txt")
                return False
            
            # Add domain if new
            if not self.domain_manager.exists(domain):
                self.domain_manager.add(domain)
                self.stats['domains_discovered'] += 1
            
            # Add to queue
            queue_result = self.url_queue_manager.add(url, domain, depth, priority)
            if queue_result:
                self.stats['urls_queued'] += 1
                self.logger.info(f"Successfully added URL {url} to queue")
                return True
            else:
                self.logger.error(f"Failed to add URL {url} to queue")
                return False
        except Exception as e:
            self.logger.error(f"Error adding URL {url}: {str(e)}")
            return False
    
    def process_url(self, url: str, depth: int) -> Dict:
        """
        Process a single URL, extracting links but not analyzing content.
        
        Args:
            url: URL to process
            depth: Depth of the URL in the crawl tree
            
        Returns:
            Dict: Results of processing the URL
        """
        # Check if we should stop
        if self._should_stop():
            self.logger.info("Stopping crawler due to stop condition")
            return {'status': 'shutdown'}
        
        # Skip if already processed
        if self.visited_urls.exists(url):
            return {'status': 'skipped', 'reason': 'already_processed'}
        
        # Parse URL to get domain
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            
            # Check if domain is allowed
            if not self._is_domain_allowed(domain):
                return {'status': 'skipped', 'reason': 'domain_not_allowed'}
            
            # Check if we're restricting to start domain
            if self.config['restrict_to_start_domain'] and domain != self.start_domain:
                return {'status': 'skipped', 'reason': 'different_domain'}
            
            # Check domain limits
            domain_count = self.domain_manager.get_url_count(domain)
            if domain_count >= self.config['max_urls_per_domain']:
                return {'status': 'skipped', 'reason': 'domain_limit_reached'}
            
            # Fetch content
            start_time = time.time()
            try:
                content, metadata = self.content_extractor.fetch(url)
                processing_time = time.time() - start_time
                self.stats['processing_times'].append(processing_time)
            except Exception as e:
                self.logger.error(f"Error fetching URL {url}: {str(e)}")
                
                # Add to visited URLs to avoid retrying
                self.visited_urls.add(url, domain, depth, status_code=None, 
                                     content_type=None, content_length=0)
                
                self.stats['urls_failed'] += 1
                return {'status': 'error', 'reason': 'fetch_failed', 'error': str(e)}
            
            # Add to visited URLs
            self.visited_urls.add(url, domain, depth, 
                                 status_code=metadata.get('status_code'),
                                 content_type=metadata.get('content_type'),
                                 content_length=len(content) if content else 0)
            
            # Update stats
            self.stats['urls_processed'] += 1
            
            # Extract links if enabled and not at max depth
            links = []
            if self.config['extract_links'] and depth < self.config['max_depth']:
                links = self.content_extractor.extract_links(url, content)
                
                # Add extracted links to queue
                for link_url, anchor_text in links:
                    self.add_url(link_url, depth + 1)
            
            # Update stats periodically
            if self.stats['urls_processed'] % self.config['stats_update_interval'] == 0:
                self._update_stats()
            
            return {
                'status': 'success',
                'url': url,
                'domain': domain,
                'depth': depth,
                'links_extracted': len(links),
                'processing_time': processing_time,
                'metadata': metadata
            }
        except Exception as e:
            self.logger.error(f"Error processing URL {url}: {str(e)}")
            self.stats['urls_failed'] += 1
            return {'status': 'error', 'reason': 'processing_failed', 'error': str(e)}
    
    def process_next_url(self) -> Dict:
        """
        Process the next URL in the queue.
        
        Returns:
            Dict: Results of processing the URL, or None if queue is empty
        """
        # Get next URL from queue
        url_data = self.url_queue_manager.get_next()
        if not url_data:
            return None
        
        # Process URL
        url = url_data['url']
        depth = url_data['depth']
        
        # Apply delay if configured
        if self.config['request_delay'] > 0:
            time.sleep(self.config['request_delay'])
        
        # Process URL
        result = self.process_url(url, depth)
        
        return result
    
    def crawl(self, max_urls=None, max_time=None):
        """
        Start the crawling process.
        
        Args:
            max_urls: Maximum number of URLs to process (None for unlimited)
            max_time: Maximum time to run in seconds (None for unlimited)
        """
        self.stats['start_time'] = time.time()
        self.logger.info(f"Starting crawl with max_urls={max_urls}, max_time={max_time}")
        
        try:
            # Process URLs until queue is empty or limits reached
            while True:
                # Check if we've reached the URL limit
                if max_urls is not None and self.stats['urls_processed'] >= max_urls:
                    self.logger.info(f"Reached maximum URLs limit: {max_urls}")
                    break
                
                # Check if we've reached the time limit
                if max_time is not None:
                    elapsed = time.time() - self.stats['start_time']
                    if elapsed >= max_time:
                        self.logger.info(f"Reached maximum time limit: {max_time}s")
                        break
                
                # Process next URL
                result = self.process_next_url()
                if not result:
                    self.logger.info("URL queue is empty")
                    break
                
                # Check if we should stop
                if result.get('status') == 'shutdown':
                    self.logger.info("Received shutdown signal")
                    break
        except KeyboardInterrupt:
            self.logger.info("Crawl interrupted by user")
        except Exception as e:
            self.logger.error(f"Error during crawl: {str(e)}")
        finally:
            # Update final stats
            self.stats['end_time'] = time.time()
            self.stats['elapsed_time'] = self.stats['end_time'] - self.stats['start_time']
            self._update_stats()
            
            self.logger.info(f"Crawl completed in {self.stats['elapsed_time']:.2f}s")
            self.logger.info(f"Processed {self.stats['urls_processed']} URLs")
    
    def get_unanalyzed_urls(self, limit=10) -> List[Dict]:
        """
        Get a list of URLs that have been visited but not analyzed.
        
        Args:
            limit: Maximum number of URLs to return
            
        Returns:
            List of dictionaries with URL information
        """
        return self.visited_urls.get_unanalyzed_urls(limit)
    
    def mark_url_as_analyzed(self, url: str) -> bool:
        """
        Mark a URL as analyzed.
        
        Args:
            url: URL to mark as analyzed
            
        Returns:
            bool: True if URL was marked as analyzed, False otherwise
        """
        return self.visited_urls.mark_as_analyzed(url)
    
    def get_stats(self) -> Dict:
        """
        Get current crawler statistics.
        
        Returns:
            Dict: Current crawler statistics
        """
        # Calculate derived stats
        if self.stats['urls_processed'] > 0:
            avg_time = sum(self.stats['processing_times']) / len(self.stats['processing_times'])
        else:
            avg_time = 0
        
        # Update elapsed time
        if self.stats['start_time']:
            if self.stats['end_time']:
                self.stats['elapsed_time'] = self.stats['end_time'] - self.stats['start_time']
            else:
                self.stats['elapsed_time'] = time.time() - self.stats['start_time']
        
        # Get database stats
        visited_stats = self.visited_urls.get_stats()
        queue_stats = self.url_queue_manager.get_stats()
        
        # Combine stats
        combined_stats = {
            'urls_processed': self.stats['urls_processed'],
            'urls_queued': queue_stats.get('total', 0),
            'urls_failed': self.stats['urls_failed'],
            'domains_discovered': self.stats['domains_discovered'],
            'avg_processing_time': avg_time,
            'elapsed_time': self.stats['elapsed_time'],
            'analyzed_urls': visited_stats.get('analyzed', 0),
            'unanalyzed_urls': visited_stats.get('unanalyzed', 0)
        }
        
        return combined_stats
    
    def close(self):
        """Close the crawler and release resources."""
        if self.db:
            self.db.close()
    
    def _update_stats(self):
        """Update and store current stats."""
        stats = self.get_stats()
        self.stats_manager.update(stats)
    
    def _should_stop(self):
        """Check if the crawler should stop."""
        # This can be extended with more conditions
        return False
    
    def _is_domain_allowed(self, domain: str) -> bool:
        """
        Check if a domain is allowed to be crawled.
        
        Args:
            domain: Domain to check
            
        Returns:
            bool: True if domain is allowed, False otherwise
        """
        # If allowed domains list is empty, all domains are allowed
        if not self.config['allowed_domains']:
            # Unless explicitly disallowed
            return domain not in self.config['disallowed_domains']
        
        # Otherwise, domain must be in allowed domains
        return domain in self.config['allowed_domains']
    
    def _is_url_allowed(self, url: str) -> bool:
        """
        Check if a URL is allowed to be crawled.
        
        Args:
            url: URL to check
            
        Returns:
            bool: True if URL is allowed, False otherwise
        """
        # If allowed patterns list is empty, all URLs are allowed
        if not self.config['allowed_url_patterns']:
            # Unless explicitly disallowed
            if self.config['disallowed_url_patterns']:
                for pattern in self.config['disallowed_url_patterns']:
                    if re.search(pattern, url):
                        return False
            return True
        
        # Otherwise, URL must match at least one allowed pattern
        for pattern in self.config['allowed_url_patterns']:
            if re.search(pattern, url):
                # Unless explicitly disallowed
                for disallowed in self.config['disallowed_url_patterns']:
                    if re.search(disallowed, url):
                        return False
                return True
        
        return False
    
    def _is_allowed_by_robots(self, url: str) -> bool:
        """
        Check if a URL is allowed by robots.txt.
        
        Args:
            url: URL to check
            
        Returns:
            bool: True if URL is allowed, False otherwise
        """
        if not self.config['respect_robots_txt']:
            return True
        
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            
            # Get or create robots parser for this domain
            if domain not in self.robots_parsers:
                robots_url = f"{parsed_url.scheme}://{domain}/robots.txt"
                parser = RobotFileParser()
                parser.set_url(robots_url)
                
                try:
                    parser.read()
                except Exception as e:
                    self.logger.warning(f"Error reading robots.txt for {domain}: {str(e)}")
                    # If we can't read robots.txt, assume everything is allowed
                    return True
                
                self.robots_parsers[domain] = parser
            
            # Check if URL is allowed
            return self.robots_parsers[domain].can_fetch(self.config['user_agent'], url)
        except Exception as e:
            self.logger.error(f"Error checking robots.txt for {url}: {str(e)}")
            # If there's an error, assume it's allowed
            return True

# Example usage
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create crawler
    crawler = WebCrawler()
    
    # Define callbacks
    def on_content_extracted(url, depth, title, content, metadata):
        print(f"Processed: {url} (depth {depth}) - {title}")
        
    # Set callbacks
    crawler.on_content_extracted = on_content_extracted
    
    # Start crawling
    crawler.crawl(start_url="https://example.com", max_urls=10)
    
    # Print stats
    print("Crawl complete!")
    stats = crawler.get_stats()
    print(f"URLs processed: {stats['urls_processed']}")
    print(f"URLs failed: {stats['urls_failed']}")
    print(f"Domains visited: {stats['domains_discovered']}")
    print(f"Duration: {stats['elapsed_time']:.2f} seconds")
