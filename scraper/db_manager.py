"""
Database manager for the web crawler.

This module provides classes for managing the crawler database, including:
- CrawlDatabase: Main database connection and management
- UrlQueueManager: Manages the URL queue in the database
- DomainManager: Manages domain information in the database
- VisitedUrlManager: Manages visited URLs in the database
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
            
    def get_unanalyzed_urls(self, limit: int = 10) -> list:
        """
        Get a list of URLs that have been visited but not analyzed.
        
        Args:
            limit: Maximum number of URLs to return
            
        Returns:
            list: List of URL dictionaries
        """
        try:
            self.db.execute(
                """
                SELECT url, domain, status_code, content_type, content_length, crawled_at
                FROM pages
                WHERE analyzed = 0
                ORDER BY crawled_at
                LIMIT ?
                """,
                (limit,)
            )
            rows = self.db.fetchall()
            
            urls = []
            for row in rows:
                url, domain, status_code, content_type, content_length, crawled_at = row
                urls.append({
                    'url': url,
                    'domain': domain,
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
