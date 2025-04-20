"""
Database manager for the NDAIVI system.

This module provides classes for managing multiple databases in the system:

CRAWLER DATABASE:
- CrawlDatabase: Core database connection and management for crawler
- UrlQueueManager: Manages the URL queue in the crawler database
- DomainManager: Manages domain information in the crawler database
- VisitedUrlManager: Manages visited URLs in the crawler database
- DBManager: Wrapper for crawler database functionality

MAIN DATABASE (Manual Information):
- MainDBManager: Database manager for the main application (competitors, products, etc.)

IMPORTANT: The system uses completely separate databases for crawler data and manual information.
"""

import logging
import os
import sqlite3
import threading
import time
import json
from typing import List, Dict, Tuple, Optional, Any, Union

class CrawlDatabase:
    """
    Manages the database connection and operations for the crawler.
    
    This class handles database initialization, connection management,
    and provides thread-safe access to the database.
    """
    
    def __init__(self, db_path: str, max_retries: int = 5, timeout: float = 20.0, logger=None):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to the SQLite database file
            max_retries: Maximum number of retries for locked database
            timeout: Timeout for database operations in seconds
            logger: Logger instance
        """
        self.db_path = db_path
        self.max_retries = max_retries
        self.timeout = timeout
        self.logger = logger or logging.getLogger(__name__)
        
        # Thread-local storage for database connections
        self.local = threading.local()
        self.local.conn = None
        self.local.cursor = None
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Initialize database
        self.initialize_database()
        
    def initialize_database(self):
        """
        Initialize the database by creating tables if they don't exist.
        
        This method creates the database file if it doesn't exist and
        initializes the required tables.
        """
        self.logger.info(f"Initializing database at {self.db_path}")
        
        # Create directory if it doesn't exist
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            
        # Connect to database and create tables
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Attempting to connect to database (attempt {attempt+1}/{self.max_retries})")
                self.conn = sqlite3.connect(self.db_path, timeout=self.timeout)
                self.conn.isolation_level = None  # Use autocommit mode
                self.cursor = self.conn.cursor()
                
                # Create tables
                self._create_tables()
                
                self.logger.info("Database initialized successfully")
                break
                
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < self.max_retries - 1:
                    self.logger.warning(f"Database locked, retrying in 1 second (attempt {attempt+1}/{self.max_retries})")
                    time.sleep(1)
                else:
                    self.logger.error(f"Failed to initialize database: {str(e)}")
                    raise
                    
    def _create_tables(self):
        """
        Create the required tables in the database.
        """
        self.logger.info("Creating database tables if they don't exist")
        
        # Create url_queue table
        self.execute("""
        CREATE TABLE IF NOT EXISTS url_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            domain TEXT,
            depth INTEGER,
            priority REAL,
            status TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_url TEXT,
            anchor_text TEXT,
            metadata TEXT
        )
        """)
        
        # Create domains table
        self.execute("""
        CREATE TABLE IF NOT EXISTS domains (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT UNIQUE,
            last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            robots_txt TEXT,
            crawl_delay REAL,
            robots_checked INTEGER DEFAULT 0
        )
        """)
        
        # Create pages table
        self.execute("""
        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            domain TEXT,
            depth INTEGER DEFAULT 0,
            title TEXT,
            content_type TEXT,
            status_code INTEGER,
            content_hash TEXT,
            content_length INTEGER DEFAULT 0,
            content TEXT,
            headers TEXT,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_modified TIMESTAMP,
            analyzed INTEGER DEFAULT 0
        )
        """)
        
        # Create links table
        self.execute("""
        CREATE TABLE IF NOT EXISTS links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_url TEXT,
            target_url TEXT,
            anchor_text TEXT,
            rel TEXT,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_url, target_url)
        )
        """)
        
        # Create crawl_stats table
        self.execute("""
        CREATE TABLE IF NOT EXISTS crawl_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_time TIMESTAMP,
            urls_crawled INTEGER DEFAULT 0,
            urls_queued INTEGER DEFAULT 0,
            urls_failed INTEGER DEFAULT 0,
            bytes_downloaded INTEGER DEFAULT 0,
            status TEXT DEFAULT 'running',
            config TEXT
        )
        """)
        
        # Create indexes
        self.execute("CREATE INDEX IF NOT EXISTS idx_url_queue_status ON url_queue(status)")
        self.execute("CREATE INDEX IF NOT EXISTS idx_url_queue_domain ON url_queue(domain)")
        self.execute("CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages(domain)")
        self.execute("CREATE INDEX IF NOT EXISTS idx_links_source ON links(source_url)")
        self.execute("CREATE INDEX IF NOT EXISTS idx_links_target ON links(target_url)")
        
    @property
    def conn(self):
        """
        Get the database connection for the current thread.
        
        Returns:
            sqlite3.Connection: The database connection
        """
        # Check if connection exists for current thread
        if not hasattr(self.local, 'conn') or self.local.conn is None:
            # Create a new connection for this thread
            for attempt in range(self.max_retries):
                try:
                    self.local.conn = sqlite3.connect(self.db_path, timeout=self.timeout)
                    self.local.conn.row_factory = sqlite3.Row
                    self.logger.debug(f"Created new database connection for thread {threading.current_thread().name}")
                    break
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < self.max_retries - 1:
                        self.logger.warning(f"Database locked, retrying ({attempt+1}/{self.max_retries})...")
                        time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    else:
                        self.logger.error(f"Failed to connect to database: {str(e)}")
                        raise
                        
        return self.local.conn
        
    @conn.setter
    def conn(self, value):
        """
        Set the database connection for the current thread.
        
        Args:
            value: Database connection
        """
        self.local.conn = value
        
    @property
    def cursor(self):
        """
        Get the database cursor for the current thread.
        
        Returns:
            sqlite3.Cursor: Database cursor
        """
        if not hasattr(self.local, 'cursor') or self.local.cursor is None:
            self.local.cursor = self.conn.cursor()
        return self.local.cursor
        
    @cursor.setter
    def cursor(self, value):
        """
        Set the database cursor for the current thread.
        
        Args:
            value: Database cursor
        """
        self.local.cursor = value
        
    def execute(self, query, params=None):
        """
        Execute a SQL query with retry logic for locked database.
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            
        Returns:
            sqlite3.Cursor: Database cursor
        """
        with self.lock:
            for attempt in range(self.max_retries):
                try:
                    if params:
                        return self.cursor.execute(query, params)
                    else:
                        return self.cursor.execute(query)
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < self.max_retries - 1:
                        self.logger.warning(f"Database locked, retrying in 1 second (attempt {attempt+1}/{self.max_retries})")
                        time.sleep(1)
                    else:
                        self.logger.error(f"Database error executing query: {str(e)}")
                        self.logger.error(f"Query: {query}")
                        if params:
                            self.logger.error(f"Params: {params}")
                        raise
                        
    def executemany(self, query, params_list):
        """
        Execute a SQL query with many parameter sets.
        
        Args:
            query: SQL query to execute
            params_list: List of parameter sets
            
        Returns:
            sqlite3.Cursor: Database cursor
        """
        with self.lock:
            for attempt in range(self.max_retries):
                try:
                    return self.cursor.executemany(query, params_list)
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < self.max_retries - 1:
                        self.logger.warning(f"Database locked, retrying in 1 second (attempt {attempt+1}/{self.max_retries})")
                        time.sleep(1)
                    else:
                        self.logger.error(f"Database error executing query: {str(e)}")
                        self.logger.error(f"Query: {query}")
                        raise
                        
    def fetchone(self):
        """
        Fetch one row from the cursor.
        
        Returns:
            tuple: Row data or None
        """
        return self.cursor.fetchone()
        
    def fetchall(self):
        """
        Fetch all rows from the cursor.
        
        Returns:
            list: List of rows
        """
        return self.cursor.fetchall()
        
    def close(self):
        """
        Close the database connection.
        """
        if hasattr(self.local, 'conn') and self.local.conn is not None:
            self.local.conn.close()
            self.local.conn = None
            self.local.cursor = None


class UrlQueueManager:
    """
    Manages the URL queue in the database.
    """
    
    def __init__(self, db: CrawlDatabase, logger=None):
        """
        Initialize the URL queue manager.
        
        Args:
            db: Database manager
            logger: Logger instance
        """
        self.db = db
        self.logger = logger or logging.getLogger(__name__)
        
    def add(self, url: str, domain: str, depth: int = 0, priority: float = 0.0, 
            source_url: str = None, anchor_text: str = None, metadata: str = None) -> bool:
        """
        Add a URL to the queue.
        
        Args:
            url: URL to add
            domain: Domain of the URL
            depth: Depth of the URL in the crawl tree
            priority: Priority of the URL
            source_url: URL of the page containing the link
            anchor_text: Text of the anchor linking to this URL
            metadata: Additional metadata about the URL
            
        Returns:
            bool: True if URL was added, False if it was already in the queue
        """
        try:
            self.db.execute(
                """
                INSERT INTO url_queue (url, domain, depth, priority, status, source_url, anchor_text, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (url, domain, depth, priority, 'pending', source_url, anchor_text, metadata)
            )
            self.logger.debug(f"Added URL to queue: {url}")
            return True
        except sqlite3.IntegrityError:
            self.logger.debug(f"URL already in queue: {url}")
            return False
        except Exception as e:
            self.logger.error(f"Error adding URL to queue: {str(e)}")
            return False
            
    def get_next(self) -> Optional[Dict]:
        """
        Get the next URL from the queue.
        
        Returns:
            dict: URL data or None if queue is empty
        """
        try:
            self.db.execute(
                """
                SELECT id, url, domain, depth, priority, source_url, anchor_text, metadata
                FROM url_queue
                WHERE status = 'pending'
                ORDER BY priority DESC, id ASC
                LIMIT 1
                """
            )
            row = self.db.fetchone()
            
            if not row:
                return None
                
            url_id, url, domain, depth, priority, source_url, anchor_text, metadata = row
            
            # Mark URL as processing
            self.db.execute(
                "UPDATE url_queue SET status = 'processing' WHERE id = ?",
                (url_id,)
            )
            
            return {
                'id': url_id,
                'url': url,
                'domain': domain,
                'depth': depth,
                'priority': priority,
                'source_url': source_url,
                'anchor_text': anchor_text,
                'metadata': metadata
            }
        except Exception as e:
            self.logger.error(f"Error getting next URL from queue: {str(e)}")
            return None
            
    def get_next_url(self) -> dict:
        """
        Get the next URL from the queue.
        
        Returns:
            dict: URL data or None if queue is empty
        """
        try:
            self.db.execute(
                """
                SELECT url, domain, depth, priority, added_at, metadata
                FROM url_queue
                WHERE status = 'pending'
                ORDER BY priority DESC, added_at ASC
                LIMIT 1
                """
            )
            row = self.db.fetchone()
            
            if not row:
                return None
                
            url, domain, depth, priority, added_at, metadata_json = row
            
            # Parse metadata JSON
            try:
                metadata = json.loads(metadata_json) if metadata_json else {}
            except:
                metadata = {}
                
            # Mark URL as processing
            self.db.execute(
                """
                UPDATE url_queue
                SET status = 'processing'
                WHERE url = ?
                """,
                (url,)
            )
            
            return {
                'url': url,
                'domain': domain,
                'depth': depth,
                'priority': priority,
                'added_at': added_at,
                'metadata': metadata
            }
            
        except Exception as e:
            self.logger.error(f"Error getting next URL from queue: {str(e)}")
            return None
            
    def mark_complete(self, url_id: int) -> bool:
        """
        Mark a URL as complete.
        
        Args:
            url_id: ID of the URL
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.db.execute(
                "UPDATE url_queue SET status = 'complete' WHERE id = ?",
                (url_id,)
            )
            return True
        except Exception as e:
            self.logger.error(f"Error marking URL as complete: {str(e)}")
            return False
            
    def mark_failed(self, url_id: int) -> bool:
        """
        Mark a URL as failed.
        
        Args:
            url_id: ID of the URL
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.db.execute(
                "UPDATE url_queue SET status = 'failed' WHERE id = ?",
                (url_id,)
            )
            return True
        except Exception as e:
            self.logger.error(f"Error marking URL as failed: {str(e)}")
            return False
            
    def get_count(self, status: str = None) -> int:
        """
        Get the number of URLs in the queue.
        
        Args:
            status: Optional status filter
            
        Returns:
            int: Number of URLs
        """
        try:
            if status:
                self.db.execute(
                    "SELECT COUNT(*) FROM url_queue WHERE status = ?",
                    (status,)
                )
            else:
                self.db.execute("SELECT COUNT(*) FROM url_queue")
                
            return self.db.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Error getting URL count: {str(e)}")
            return 0
            
    def clear(self) -> bool:
        """
        Clear the URL queue.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.db.execute("DELETE FROM url_queue")
            return True
        except Exception as e:
            self.logger.error(f"Error clearing URL queue: {str(e)}")
            return False
            
    def is_empty(self) -> bool:
        """
        Check if the URL queue is empty.
        
        Returns:
            bool: True if the queue is empty, False otherwise
        """
        try:
            self.db.execute("SELECT COUNT(*) FROM url_queue WHERE status = 'pending'")
            count = self.db.fetchone()[0]
            return count == 0
        except Exception as e:
            self.logger.error(f"Error checking if URL queue is empty: {str(e)}")
            return True  # Assume empty on error
            
    def get_stats(self) -> dict:
        """
        Get statistics about the URL queue.
        
        Returns:
            dict: Statistics about the URL queue
        """
        try:
            stats = {}
            
            # Get total count
            self.db.execute("SELECT COUNT(*) FROM url_queue")
            stats['total'] = self.db.fetchone()[0]
            
            # Get pending count
            self.db.execute("SELECT COUNT(*) FROM url_queue WHERE status = 'pending'")
            stats['pending'] = self.db.fetchone()[0]
            
            # Get completed count
            self.db.execute("SELECT COUNT(*) FROM url_queue WHERE status = 'complete'")
            stats['complete'] = self.db.fetchone()[0]
            
            # Get failed count
            self.db.execute("SELECT COUNT(*) FROM url_queue WHERE status = 'failed'")
            stats['failed'] = self.db.fetchone()[0]
            
            # Get domain count
            self.db.execute("SELECT COUNT(DISTINCT domain) FROM url_queue")
            stats['domains'] = self.db.fetchone()[0]
            
            return stats
        except Exception as e:
            self.logger.error(f"Error getting URL queue stats: {str(e)}")
            return {
                'total': 0,
                'pending': 0,
                'complete': 0,
                'failed': 0,
                'domains': 0
            }
            
    def is_queued(self, url: str) -> bool:
        """
        Check if a URL is already in the queue.
        
        Args:
            url: URL to check
            
        Returns:
            bool: True if URL is in the queue, False otherwise
        """
        try:
            self.db.execute(
                "SELECT COUNT(*) FROM url_queue WHERE url = ? AND status = 'pending'",
                (url,)
            )
            count = self.db.fetchone()[0]
            return count > 0
        except Exception as e:
            self.logger.error(f"Error checking if URL is in queue: {str(e)}")
            return False
            
    def get_all_queued(self) -> List[Dict]:
        """
        Get all URLs in the queue with 'pending' status.
        
        Returns:
            list: List of URL data dictionaries
        """
        try:
            self.db.execute(
                """
                SELECT id, url, domain, depth, priority, source_url, anchor_text, metadata
                FROM url_queue
                WHERE status = 'pending'
                ORDER BY priority DESC, id ASC
                """
            )
            rows = self.db.fetchall()
            
            if not rows:
                return []
                
            result = []
            for row in rows:
                url_id, url, domain, depth, priority, source_url, anchor_text, metadata = row
                result.append({
                    'id': url_id,
                    'url': url,
                    'domain': domain,
                    'depth': depth,
                    'priority': priority,
                    'source_url': source_url,
                    'anchor_text': anchor_text,
                    'metadata': metadata
                })
            
            return result
        except Exception as e:
            self.logger.error(f"Error getting all queued URLs: {str(e)}")
            return []
            
class DomainManager:
    """
    Manages domain information in the database.
    """
    
    def __init__(self, db: CrawlDatabase, logger=None):
        """
        Initialize the domain manager.
        
        Args:
            db: Database manager
            logger: Logger instance
        """
        self.db = db
        self.logger = logger or logging.getLogger(__name__)
        
    def add_domain(self, domain: str, robots_txt: str = None, crawl_delay: float = None) -> bool:
        """
        Add a domain to the database.
        
        Args:
            domain: Domain to add
            robots_txt: Contents of robots.txt file
            crawl_delay: Crawl delay in seconds
            
        Returns:
            bool: True if domain was added or updated, False otherwise
        """
        try:
            # Check if domain exists
            self.db.execute(
                "SELECT id FROM domains WHERE domain = ?",
                (domain,)
            )
            row = self.db.fetchone()
            
            if row:
                # Update existing domain
                self.db.execute(
                    """
                    UPDATE domains
                    SET last_seen = CURRENT_TIMESTAMP,
                        robots_txt = COALESCE(?, robots_txt),
                        crawl_delay = COALESCE(?, crawl_delay)
                    WHERE domain = ?
                    """,
                    (robots_txt, crawl_delay, domain)
                )
            else:
                # Add new domain
                self.db.execute(
                    """
                    INSERT INTO domains (domain, robots_txt, crawl_delay)
                    VALUES (?, ?, ?)
                    """,
                    (domain, robots_txt, crawl_delay)
                )
                
            self.logger.debug(f"Added/updated domain: {domain}")
            return True
        except Exception as e:
            self.logger.error(f"Error adding domain: {str(e)}")
            return False
            
    def get_domain(self, domain: str) -> Optional[Dict]:
        """
        Get domain information.
        
        Args:
            domain: Domain to get
            
        Returns:
            dict: Domain data or None if not found
        """
        try:
            self.db.execute(
                """
                SELECT id, domain, last_seen, robots_txt, crawl_delay
                FROM domains
                WHERE domain = ?
                """,
                (domain,)
            )
            row = self.db.fetchone()
            
            if not row:
                return None
                
            domain_id, domain_name, last_seen, robots_txt, crawl_delay = row
            
            return {
                'id': domain_id,
                'domain': domain_name,
                'last_seen': last_seen,
                'robots_txt': robots_txt,
                'crawl_delay': crawl_delay
            }
        except Exception as e:
            self.logger.error(f"Error getting domain: {str(e)}")
            return None
            
    def update_robots_txt(self, domain: str, robots_txt: str) -> bool:
        """
        Update robots.txt for a domain.
        
        Args:
            domain: Domain to update
            robots_txt: Contents of robots.txt file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.db.execute(
                """
                UPDATE domains
                SET robots_txt = ?,
                    last_seen = CURRENT_TIMESTAMP
                WHERE domain = ?
                """,
                (robots_txt, domain)
            )
            return True
        except Exception as e:
            self.logger.error(f"Error updating robots.txt: {str(e)}")
            return False
            
    def update_crawl_delay(self, domain: str, crawl_delay: float) -> bool:
        """
        Update crawl delay for a domain.
        
        Args:
            domain: Domain to update
            crawl_delay: Crawl delay in seconds
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.db.execute(
                """
                UPDATE domains
                SET crawl_delay = ?,
                    last_seen = CURRENT_TIMESTAMP
                WHERE domain = ?
                """,
                (crawl_delay, domain)
            )
            return True
        except Exception as e:
            self.logger.error(f"Error updating crawl delay: {str(e)}")
            return False
            
    def get_all_domains(self) -> List[Dict]:
        """
        Get all domains.
        
        Returns:
            list: List of domain data dictionaries
        """
        try:
            self.db.execute(
                """
                SELECT id, domain, last_seen, robots_txt, crawl_delay
                FROM domains
                ORDER BY domain
                """
            )
            rows = self.db.fetchall()
            
            domains = []
            for row in rows:
                domain_id, domain_name, last_seen, robots_txt, crawl_delay = row
                domains.append({
                    'id': domain_id,
                    'domain': domain_name,
                    'last_seen': last_seen,
                    'robots_txt': robots_txt,
                    'crawl_delay': crawl_delay
                })
                
            return domains
        except Exception as e:
            self.logger.error(f"Error getting all domains: {str(e)}")
            return []
            
    def exists(self, domain: str) -> bool:
        """
        Check if a domain exists in the database.
        
        Args:
            domain: Domain to check
            
        Returns:
            bool: True if domain exists, False otherwise
        """
        try:
            self.db.execute(
                "SELECT 1 FROM domains WHERE domain = ?",
                (domain,)
            )
            return self.db.fetchone() is not None
        except Exception as e:
            self.logger.error(f"Error checking if domain exists: {str(e)}")
            return False


class VisitedUrlManager:
    """
    Manages visited URLs in the database.
    """
    
    def __init__(self, db: CrawlDatabase, logger=None):
        """
        Initialize the visited URL manager.
        
        Args:
            db: Database manager
            logger: Logger instance
        """
        self.db = db
        self.logger = logger or logging.getLogger(__name__)
        
    def add(self, url: str, domain: str, depth: int, status_code: int = None, 
            content_type: str = None, content_length: int = 0, analyzed: bool = False) -> bool:
        """
        Add a URL to the visited URLs list.
        
        Args:
            url: URL that was visited
            domain: Domain of the URL
            depth: Depth of the URL in the crawl tree
            status_code: HTTP status code
            content_type: Content type of the response
            content_length: Content length in bytes
            analyzed: Whether the URL has been analyzed
            
        Returns:
            bool: True if URL was added, False otherwise
        """
        try:
            self.db.execute(
                """
                INSERT OR REPLACE INTO pages (
                    url, domain, depth, status_code, content_type, content_length, crawled_at, analyzed
                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                """,
                (url, domain, depth, status_code, content_type, content_length, analyzed)
            )
            self.logger.debug(f"Added URL to visited list: {url}")
            return True
        except Exception as e:
            self.logger.error(f"Error adding URL to visited list: {str(e)}")
            return False
            
    def exists(self, url: str) -> bool:
        """
        Check if a URL has been visited.
        
        Args:
            url: URL to check
            
        Returns:
            bool: True if URL has been visited, False otherwise
        """
        try:
            self.db.execute(
                "SELECT 1 FROM pages WHERE url = ?",
                (url,)
            )
            return self.db.fetchone() is not None
        except Exception as e:
            self.logger.error(f"Error checking if URL exists: {str(e)}")
            return False
            
    def mark_as_analyzed(self, url: str) -> bool:
        """
        Mark a URL as analyzed.
        
        Args:
            url: URL to mark as analyzed
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.db.execute(
                "UPDATE pages SET analyzed = 1 WHERE url = ?",
                (url,)
            )
            return True
        except Exception as e:
            self.logger.error(f"Error marking URL as analyzed: {str(e)}")
            return False
            
    def get_unanalyzed_urls(self, limit: int = 10, priority_order: bool = True) -> list:
        """
        Get a list of URLs that have been visited but not analyzed.
        
        Args:
            limit: Maximum number of URLs to return
            priority_order: If True, order URLs by priority (lower depth first, then by timestamp)
            
        Returns:
            list: List of URL dictionaries
        """
        try:
            self.db.execute(
                """
                -- Include URL length in priority calculation (shorter URLs get higher priority)
                SELECT url, domain, depth, status_code, content_type, content_length, crawled_at
                FROM pages
                WHERE analyzed = 0
                ORDER BY depth ASC, length(url) ASC, crawled_at ASC
                LIMIT ?
                """,
                (limit,)
            )
            rows = self.db.fetchall()
            
            urls = []
            for row in rows:
                url, domain, depth, status_code, content_type, content_length, crawled_at = row
                urls.append({
                    'url': url,
                    'domain': domain,
                    'depth': depth,
                    'status_code': status_code,
                    'content_type': content_type,
                    'content_length': content_length,
                    'crawled_at': crawled_at
                })
                
            return urls
        except Exception as e:
            self.logger.error(f"Error getting unanalyzed URLs: {str(e)}")
            return []
            
    def get_stats(self) -> dict:
        """
        Get statistics about visited URLs.
        
        Returns:
            dict: Statistics about visited URLs
        """
        try:
            stats = {}
            
            # Get total count
            self.db.execute("SELECT COUNT(*) FROM pages")
            stats['total'] = self.db.fetchone()[0]
            
            # Get analyzed count
            self.db.execute("SELECT COUNT(*) FROM pages WHERE analyzed = 1")
            stats['analyzed'] = self.db.fetchone()[0]
            
            # Get unanalyzed count
            stats['unanalyzed'] = stats['total'] - stats['analyzed']
            
            # Get domain count
            self.db.execute("SELECT COUNT(DISTINCT domain) FROM pages")
            stats['domains'] = self.db.fetchone()[0]
            
            return stats
        except Exception as e:
            self.logger.error(f"Error getting visited URL stats: {str(e)}")
            return {
                'total': 0,
                'analyzed': 0,
                'unanalyzed': 0,
                'domains': 0
            }

class DBManager:
    """
    Wrapper for crawler database functionality.
    
    This class serves as a simplified interface to the crawler database,
    providing methods for common operations needed by the crawler and analyzer.
    
    IMPORTANT: This manager is ONLY for crawler-related data and uses a separate
    database file from the main application database.
    """
    
    def __init__(self, db_path: str = None, max_retries: int = 5, timeout: float = 20.0, logger=None):
        """
        Initialize the crawler database manager.
        
        Args:
            db_path: Path to the SQLite database file for crawler data
                    If None, defaults to 'data/crawler.db'
            max_retries: Maximum number of retries for locked database
            timeout: Timeout for database operations in seconds
            logger: Logger instance
        """
        # Set default crawler database path if not provided
        if db_path is None:
            db_path = os.path.join('data', 'crawler.db')
            
        # Ensure the directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info(f"Initializing crawler database at: {db_path}")
        self.db = CrawlDatabase(db_path, max_retries, timeout, logger)
        self.url_queue = UrlQueueManager(self.db, logger)
        self.domains = DomainManager(self.db, logger)
        self.visited_urls = VisitedUrlManager(self.db, logger)
    
    def add_url(self, url: str, domain: str, status_code: int = None, 
                content_type: str = None, title: str = None, links: List[str] = None, 
                depth: int = 0):
        """
        Add a URL to the visited URLs list and store its content.
        
        Args:
            url: URL that was visited
            domain: Domain of the URL
            status_code: HTTP status code
            content_type: Content type of the response
            title: Title of the page
            links: List of links found on the page
            depth: Depth of the URL in the crawl tree
            
        Returns:
            bool: True if URL was added, False otherwise
        """
        # Add to visited URLs
        result = self.visited_urls.add(
            url=url,
            domain=domain,
            depth=depth,
            status_code=status_code,
            content_type=content_type
        )
        
        # Add links to the queue
        if links:
            for link in links:
                # Extract domain from link
                try:
                    link_domain = link.split('//')[1].split('/')[0]
                except (IndexError, ValueError):
                    link_domain = domain
                
                # Add to queue
                self.url_queue.add(
                    url=link,
                    domain=link_domain,
                    depth=depth + 1,
                    source_url=url
                )
        
        return result
    
    def get_next_url(self):
        """
        Get the next URL from the queue.
        
        Returns:
            dict: URL data or None if queue is empty
        """
        return self.url_queue.get_next()
    
    def mark_url_complete(self, url_id: int):
        """
        Mark a URL as complete.
        
        Args:
            url_id: ID of the URL
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.url_queue.mark_complete(url_id)
    
    def mark_url_failed(self, url_id: int):
        """
        Mark a URL as failed.
        
        Args:
            url_id: ID of the URL
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.url_queue.mark_failed(url_id)
    
    def get_unanalyzed_urls(self, limit: int = 10, priority_order: bool = True):
        """
        Get a list of URLs that have been visited but not analyzed.
        
        Args:
            limit: Maximum number of URLs to return
            priority_order: If True, order URLs by priority (lower depth first, then by timestamp)
            
        Returns:
            list: List of URL dictionaries
        """
        return self.visited_urls.get_unanalyzed_urls(limit, priority_order)
    
    def mark_url_as_analyzed(self, url: str):
        """
        Mark a URL as analyzed.
        
        Args:
            url: URL to mark as analyzed
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self.visited_urls.mark_as_analyzed(url)
    
    def get_stats(self):
        """
        Get statistics about the crawler database.
        
        Returns:
            dict: Statistics about the database
        """
        return {
            'url_queue': self.url_queue.get_stats(),
            'visited_urls': self.visited_urls.get_stats(),
            'domains': self.domains.get_all_domains()
        }
    
    def close(self):
        """
        Close the database connection.
        """
        self.db.close()


class MainDBManager:
    """
    Database manager for the main application.
    
    This class manages the main application database, which is completely separate
    from the crawler database and handles different data structures and requirements.
    
    IMPORTANT: This manager is for storing manually collected information about
    competitors, products, features, etc. It uses a separate database file from
    the crawler database.
    """
    
    def __init__(self, db_path: str = None, max_retries: int = 5, timeout: float = 20.0, logger=None):
        """
        Initialize the main database manager.
        
        Args:
            db_path: Path to the SQLite database file for main application data
                    If None, defaults to 'data/main.db'
            max_retries: Maximum number of retries for locked database
            timeout: Timeout for database operations in seconds
            logger: Logger instance
        """
        # Set default main database path if not provided
        if db_path is None:
            db_path = os.path.join('data', 'main.db')
            
        # Ensure the directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
        self.db_path = db_path
        self.max_retries = max_retries
        self.timeout = timeout
        self.logger = logger or logging.getLogger(__name__)
        
        self.logger.info(f"Initializing main application database at: {db_path}")
        
        # Thread-local storage for database connections
        self.local = threading.local()
        self.local.conn = None
        self.local.cursor = None
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Initialize database
        self.initialize_database()
    
    def initialize_database(self):
        """
        Initialize the database by creating tables if they don't exist.
        
        This method creates the database file if it doesn't exist and
        initializes the required tables.
        """
        self.logger.info(f"Initializing main database at {self.db_path}")
        
        # Create directory if it doesn't exist
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            
        # Connect to database and create tables
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Attempting to connect to main database (attempt {attempt+1}/{self.max_retries})")
                self.conn = sqlite3.connect(self.db_path, timeout=self.timeout)
                self.conn.isolation_level = None  # Use autocommit mode
                self.cursor = self.conn.cursor()
                
                # Create tables
                self._create_tables()
                
                self.logger.info("Main database initialized successfully")
                break
                
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < self.max_retries - 1:
                    self.logger.warning(f"Main database locked, retrying in 1 second (attempt {attempt+1}/{self.max_retries})")
                    time.sleep(1)
                else:
                    self.logger.error(f"Failed to initialize main database: {str(e)}")
                    raise
                    
    def _create_tables(self):
        """
        Create the required tables in the main database.
        """
        self.logger.info("Creating main database tables if they don't exist")
        
        # Create competitors table
        self.execute("""
        CREATE TABLE IF NOT EXISTS competitors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            website TEXT,
            description TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP,
            status TEXT DEFAULT 'active'
        )
        """)
        
        # Create products table
        self.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            competitor_id INTEGER,
            name TEXT,
            description TEXT,
            url TEXT,
            price REAL,
            currency TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP,
            status TEXT DEFAULT 'active',
            FOREIGN KEY (competitor_id) REFERENCES competitors(id),
            UNIQUE(competitor_id, url)
        )
        """)
        
        # Create features table
        self.execute("""
        CREATE TABLE IF NOT EXISTS features (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            name TEXT,
            description TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id),
            UNIQUE(product_id, name)
        )
        """)
        
        # Create analysis_results table
        self.execute("""
        CREATE TABLE IF NOT EXISTS analysis_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            competitor_id INTEGER,
            analysis_type TEXT,
            result TEXT,
            confidence REAL,
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (competitor_id) REFERENCES competitors(id)
        )
        """)
        
        # Create indexes
        self.execute("CREATE INDEX IF NOT EXISTS idx_products_competitor ON products(competitor_id)")
        self.execute("CREATE INDEX IF NOT EXISTS idx_features_product ON features(product_id)")
        self.execute("CREATE INDEX IF NOT EXISTS idx_analysis_competitor ON analysis_results(competitor_id)")
    
    @property
    def conn(self):
        """
        Get the database connection for the current thread.
        
        Returns:
            sqlite3.Connection: The database connection
        """
        # Check if connection exists for current thread
        if not hasattr(self.local, 'conn') or self.local.conn is None:
            # Create a new connection for this thread
            for attempt in range(self.max_retries):
                try:
                    self.local.conn = sqlite3.connect(self.db_path, timeout=self.timeout)
                    self.local.conn.row_factory = sqlite3.Row
                    self.logger.debug(f"Created new main database connection for thread {threading.current_thread().name}")
                    break
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < self.max_retries - 1:
                        self.logger.warning(f"Main database locked, retrying ({attempt+1}/{self.max_retries})...")
                        time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    else:
                        self.logger.error(f"Failed to connect to main database: {str(e)}")
                        raise
                        
        return self.local.conn
        
    @conn.setter
    def conn(self, connection):
        """
        Set the database connection for the current thread.
        
        Args:
            connection: SQLite connection object
        """
        self.local.conn = connection
    
    @property
    def cursor(self):
        """
        Get the database cursor for the current thread.
        
        Returns:
            sqlite3.Cursor: Database cursor
        """
        if not hasattr(self.local, 'cursor') or self.local.cursor is None:
            self.local.cursor = self.conn.cursor()
        
        return self.local.cursor
        
    @cursor.setter
    def cursor(self, cursor):
        """
        Set the database cursor for the current thread.
        
        Args:
            cursor: SQLite cursor object
        """
        self.local.cursor = cursor
        
    def execute(self, query, params=None):
        """
        Execute a SQL query with retry logic for locked database.
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            
        Returns:
            sqlite3.Cursor: Query result
        """
        with self.lock:
            for attempt in range(self.max_retries):
                try:
                    if params:
                        self.cursor.execute(query, params)
                    else:
                        self.cursor.execute(query)
                    
                    return self.cursor
                    
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < self.max_retries - 1:
                        self.logger.warning(f"Main database locked, retrying ({attempt+1}/{self.max_retries})...")
                        time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    else:
                        self.logger.error(f"Database error: {str(e)}")
                        self.logger.error(f"Query: {query}")
                        self.logger.error(f"Params: {params}")
                        raise
                        
    def close(self):
        """
        Close the database connection.
        """
        if hasattr(self.local, 'conn') and self.local.conn:
            self.local.conn.close()
            self.local.conn = None
            self.local.cursor = None
    
    # Methods for competitor management
    
    def add_competitor(self, name: str, website: str, description: str = None):
        """
        Add a competitor to the database.
        
        Args:
            name: Competitor name
            website: Competitor website
            description: Competitor description
            
        Returns:
            int: Competitor ID or None if failed
        """
        try:
            cursor = self.execute(
                "INSERT INTO competitors (name, website, description, last_updated) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                (name, website, description)
            )
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            self.logger.warning(f"Competitor {name} already exists")
            return None
    
    def get_competitor(self, competitor_id: int):
        """
        Get competitor information.
        
        Args:
            competitor_id: Competitor ID
            
        Returns:
            dict: Competitor data or None if not found
        """
        cursor = self.execute("SELECT * FROM competitors WHERE id = ?", (competitor_id,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        
        return None
    
    def get_competitor_by_name(self, name: str):
        """
        Get competitor information by name.
        
        Args:
            name: Competitor name
            
        Returns:
            dict: Competitor data or None if not found
        """
        cursor = self.execute("SELECT * FROM competitors WHERE name = ?", (name,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        
        return None
    
    def update_competitor(self, competitor_id: int, name: str = None, website: str = None, description: str = None):
        """
        Update competitor information.
        
        Args:
            competitor_id: Competitor ID
            name: New name (optional)
            website: New website (optional)
            description: New description (optional)
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Build update query
        update_fields = []
        params = []
        
        if name:
            update_fields.append("name = ?")
            params.append(name)
        
        if website:
            update_fields.append("website = ?")
            params.append(website)
        
        if description:
            update_fields.append("description = ?")
            params.append(description)
        
        if not update_fields:
            return False
        
        update_fields.append("last_updated = CURRENT_TIMESTAMP")
        
        # Add competitor ID to params
        params.append(competitor_id)
        
        # Execute update
        query = f"UPDATE competitors SET {', '.join(update_fields)} WHERE id = ?"
        cursor = self.execute(query, params)
        
        return cursor.rowcount > 0
    
    # Methods for product management
    
    def add_product(self, competitor_id: int, name: str, url: str, description: str = None, price: float = None, currency: str = None):
        """
        Add a product to the database.
        
        Args:
            competitor_id: Competitor ID
            name: Product name
            url: Product URL
            description: Product description
            price: Product price
            currency: Price currency
            
        Returns:
            int: Product ID or None if failed
        """
        try:
            cursor = self.execute(
                "INSERT INTO products (competitor_id, name, url, description, price, currency, last_updated) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
                (competitor_id, name, url, description, price, currency)
            )
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            self.logger.warning(f"Product with URL {url} already exists for competitor {competitor_id}")
            return None
    
    def get_product(self, product_id: int):
        """
        Get product information.
        
        Args:
            product_id: Product ID
            
        Returns:
            dict: Product data or None if not found
        """
        cursor = self.execute("SELECT * FROM products WHERE id = ?", (product_id,))
        row = cursor.fetchone()
        
        if row:
            return dict(row)
        
        return None
    
    def get_products_by_competitor(self, competitor_id: int):
        """
        Get all products for a competitor.
        
        Args:
            competitor_id: Competitor ID
            
        Returns:
            list: List of product dictionaries
        """
        cursor = self.execute("SELECT * FROM products WHERE competitor_id = ?", (competitor_id,))
        rows = cursor.fetchall()
        
        return [dict(row) for row in rows]
    
    # Methods for analysis results
    
    def add_analysis_result(self, url: str, competitor_id: int, analysis_type: str, result: str, confidence: float = None):
        """
        Add an analysis result to the database.
        
        Args:
            url: URL that was analyzed
            competitor_id: Competitor ID
            analysis_type: Type of analysis (e.g., 'product', 'feature')
            result: Analysis result (JSON string)
            confidence: Confidence score
            
        Returns:
            int: Result ID or None if failed
        """
        try:
            cursor = self.execute(
                "INSERT OR REPLACE INTO analysis_results (url, competitor_id, analysis_type, result, confidence, analyzed_at) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
                (url, competitor_id, analysis_type, result, confidence)
            )
            return cursor.lastrowid
        except Exception as e:
            self.logger.error(f"Error adding analysis result: {str(e)}")
            return None
    
    def get_analysis_results(self, competitor_id: int = None, analysis_type: str = None, limit: int = 100):
        """
        Get analysis results.
        
        Args:
            competitor_id: Filter by competitor ID (optional)
            analysis_type: Filter by analysis type (optional)
            limit: Maximum number of results to return
            
        Returns:
            list: List of result dictionaries
        """
        query = "SELECT * FROM analysis_results"
        params = []
        
        # Add filters
        where_clauses = []
        
        if competitor_id:
            where_clauses.append("competitor_id = ?")
            params.append(competitor_id)
        
        if analysis_type:
            where_clauses.append("analysis_type = ?")
            params.append(analysis_type)
        
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        query += " ORDER BY analyzed_at DESC LIMIT ?"
        params.append(limit)
        
        cursor = self.execute(query, params)
        rows = cursor.fetchall()
        
        return [dict(row) for row in rows]
