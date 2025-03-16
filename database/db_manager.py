"""Database connection manager to prevent concurrent access and locks.

Implements a singleton connection pattern for SQLite to prevent database locks.
"""

import logging
import sqlite3
import threading
import time
import random
import os
import shutil
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from typing import Optional, Dict, Any, Union, Tuple

# Global lock for database operations
db_lock = threading.RLock()

class DatabaseManager:
    """Singleton database manager to prevent concurrent access and locks."""
    
    _instance = None
    _engine = None
    _session_factory = None
    
    def __new__(cls, db_path=None):
        """Ensure only one instance of the database manager exists."""
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_path=None):
        """Initialize the database connection if not already initialized."""
        if self._initialized:
            return
            
        self.logger = logging.getLogger('db_manager')
        self.db_path = db_path
        self._initialized = True
        self._stats = {
            'connections_created': 0,
            'sessions_created': 0,
            'slow_queries': 0,
            'lock_timeouts': 0,
            'last_maintenance': None
        }
        
        if db_path:
            self.initialize(db_path)
    
    def initialize(self, db_path):
        """Initialize the database connection with optimal settings for SQLite."""
        self.db_path = db_path
        self.logger.info(f"Initializing database connection to {db_path}")
        
        # Create engine with optimized settings
        self._engine = self._create_engine(db_path)
        
        # Create session factory
        self._session_factory = scoped_session(sessionmaker(
            bind=self._engine,
            expire_on_commit=False,
            autoflush=False
        ))
        
        # Set SQLite pragmas - this is non-critical, so wrap in try/except
        try:
            self._set_pragmas()
        except Exception as e:
            self.logger.warning(f"Non-critical pragma setting failed, continuing anyway: {str(e)}")
        
        # Mark as initialized even if pragmas failed
        self._initialized = True
        self.logger.info("Database connection initialized successfully")
    
    def _create_engine(self, db_path: str):
        """Create a SQLAlchemy engine with optimized settings."""
        engine = create_engine(
            f'sqlite:///{db_path}',
            connect_args={
                'timeout': 60,  # 60 seconds timeout for locks
                'check_same_thread': False,  # Allow cross-thread usage
                'isolation_level': None  # Let SQLAlchemy handle transactions
            },
            # Configure connection pooling
            pool_size=1,  # Use a single connection for all operations
            max_overflow=0,  # No overflow connections
            pool_timeout=60,
            pool_recycle=3600  # Recycle connections after 1 hour
        )
        self._stats['connections_created'] += 1
        return engine
    
    def _set_pragmas(self):
        """Set SQLite pragmas for optimal performance and reliability."""
        try:
            # Add retry logic for database locks
            max_retries = 5
            
            for attempt in range(max_retries):
                try:
                    # Add random jitter to retry delays to prevent synchronized retries
                    retry_delay = 1.0 + random.uniform(0, 0.5)  # 1.0-1.5 seconds
                    
                    # Create a new session for each attempt to prevent locking issues
                    session = self._session_factory()
                    
                    # Set busy_timeout first to handle locks in other statements
                    session.execute(text("PRAGMA busy_timeout=60000"))  # 60 second timeout
                    
                    # Set other pragmas
                    session.execute(text("PRAGMA journal_mode=WAL"))
                    session.execute(text("PRAGMA synchronous=NORMAL"))
                    session.execute(text("PRAGMA temp_store=MEMORY"))
                    session.execute(text("PRAGMA cache_size=-20000"))  # ~20MB cache
                    
                    session.commit()
                    session.close()
                    self.logger.info("SQLite pragmas set successfully")
                    return  # Success, exit method
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempt < max_retries - 1:
                        self.logger.warning(f"Database locked (attempt {attempt+1}/{max_retries}), retrying in {retry_delay:.2f} seconds")
                        session.close()  # Make sure to close the session
                        time.sleep(retry_delay)
                        # Increase delay for next attempt with jitter
                        retry_delay = retry_delay * 1.5 + random.uniform(0, 0.5)
                    else:
                        raise  # Re-raise if max retries reached or different error
                except Exception as e:
                    session.close()  # Make sure to close the session
                    raise  # Re-raise other exceptions
        except Exception as e:
            self.logger.warning(f"Failed to set SQLite pragmas: {str(e)}")
    
    @property
    def engine(self):
        """Get the SQLAlchemy engine."""
        if not self._engine:
            raise ValueError("Database not initialized, call initialize() first")
        return self._engine
    
    @contextmanager
    def session(self):
        """Get a database session with automatic locking and cleanup."""
        if not self._session_factory:
            raise ValueError("Database not initialized, call initialize() first")
        
        session = None
        acquired = False
        session_id = None
        start_time = time.time()
        
        try:
            # Acquire the global database lock with timeout
            timeout = 30  # 30 seconds timeout for acquiring lock
            
            while not acquired and (time.time() - start_time) < timeout:
                acquired = db_lock.acquire(blocking=False)
                if acquired:
                    self.logger.debug("Acquired database lock")
                    break
                time.sleep(0.1)  # Short sleep to prevent CPU spinning
            
            if not acquired:
                self._stats['lock_timeouts'] += 1
                self.logger.warning(f"Failed to acquire database lock after {timeout} seconds")
                raise TimeoutError(f"Could not acquire database lock after {timeout} seconds")
            
            # Create and yield the session
            session = self._session_factory()
            session_id = id(session)
            self._stats['sessions_created'] += 1
            self.logger.debug(f"Created session {session_id}")
            
            yield session
            
            # Log slow queries
            elapsed = time.time() - start_time
            if elapsed > 1.0:  # Log queries taking more than 1 second
                self._stats['slow_queries'] += 1
                self.logger.warning(f"Slow database operation: {elapsed:.2f}s in session {session_id}")
            
            # Commit if no exception occurred
            session.commit()
            
        except Exception as e:
            # Rollback on exception
            if session:
                self.logger.warning(f"Rolling back transaction due to error in session {session_id}: {str(e)}")
                session.rollback()
            raise
        
        finally:
            # Always clean up resources
            if session:
                session.close()
            
            # Always release the lock
            if acquired:
                db_lock.release()
                self.logger.debug(f"Released database lock for session {session_id}")
    
    def check_connection_health(self) -> bool:
        """Verify database connection is healthy and recover if needed."""
        with db_lock:
            try:
                with self._engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    self.logger.debug("Database connection health check: OK")
                    return True
            except Exception as e:
                self.logger.error(f"Database connection unhealthy: {str(e)}")
                # Attempt recovery
                self.logger.info("Attempting connection recovery")
                self._engine.dispose()
                self._engine = self._create_engine(self.db_path)
                self._session_factory = scoped_session(sessionmaker(
                    bind=self._engine,
                    expire_on_commit=False,
                    autoflush=False
                ))
                
                # Verify recovery was successful
                try:
                    with self._engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                        self.logger.info("Database connection recovery: successful")
                        return True
                except Exception as e2:
                    self.logger.error(f"Database connection recovery failed: {str(e2)}")
                    return False
    
    def perform_maintenance(self) -> bool:
        """Run regular database maintenance."""
        with db_lock:
            try:
                self.logger.info("Starting database maintenance")
                with self._engine.connect() as conn:
                    # Check integrity
                    result = conn.execute(text("PRAGMA integrity_check")).fetchone()
                    if result and result[0] != 'ok':
                        self.logger.error(f"Database integrity check failed: {result[0]}")
                        return False
                    self.logger.info("Database integrity check: OK")
                    
                    # Analyze for query optimization
                    conn.execute(text("ANALYZE"))
                    self.logger.info("Database analyze completed")
                    
                    # Only do this if we can safely acquire exclusive lock
                    try:
                        # Quick test to see if we can get exclusive access
                        test_conn = sqlite3.connect(self.db_path, timeout=1)
                        cursor = test_conn.cursor()
                        cursor.execute("PRAGMA locking_mode=EXCLUSIVE")
                        test_conn.commit()
                        cursor.close()
                        test_conn.close()
                        
                        # If we got here, we can get exclusive access, so run vacuum
                        conn.execute(text("VACUUM"))
                        self.logger.info("Database vacuum completed")
                    except sqlite3.OperationalError:
                        self.logger.warning("Skipping vacuum - database in use")
                    
                    self._stats['last_maintenance'] = time.time()
                    return True
            except Exception as e:
                self.logger.error(f"Database maintenance failed: {str(e)}")
                return False
    
    def get_status_metrics(self) -> Dict[str, Any]:
        """Return metrics about database performance."""
        metrics = {
            # Database manager stats
            **self._stats,
            
            # Add pool stats if available
            "pool_size": getattr(self._engine.pool, "size", lambda: 0)() if self._engine else 0,
            "connections_in_use": getattr(self._engine.pool, "checkedin", lambda: 0)() if self._engine else 0,
            "connections_checked_out": getattr(self._engine.pool, "checkedout", lambda: 0)() if self._engine else 0,
        }
        
        # Get sqlite stats
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text("PRAGMA page_count")).fetchone()
                page_count = result[0] if result else 0
                
                result = conn.execute(text("PRAGMA page_size")).fetchone()
                page_size = result[0] if result else 0
                
                metrics["db_size_bytes"] = page_count * page_size
                
                # Get journal mode
                result = conn.execute(text("PRAGMA journal_mode")).fetchone()
                metrics["journal_mode"] = result[0] if result else "unknown"
                
                # Get busy timeout
                result = conn.execute(text("PRAGMA busy_timeout")).fetchone()
                metrics["busy_timeout"] = result[0] if result else 0
                
                # These may not be available in all SQLite versions
                try:
                    result = conn.execute(text("PRAGMA cache_hit")).fetchone()
                    if result:
                        metrics["cache_hits"] = result[0]
                        
                    result = conn.execute(text("PRAGMA cache_miss")).fetchone()  
                    if result:
                        metrics["cache_misses"] = result[0]
                except Exception:
                    pass  # Ignore if these pragmas aren't available
        except Exception as e:
            self.logger.warning(f"Failed to get some database metrics: {e}")
            
        return metrics
    
    def backup_database(self, backup_path: Optional[str] = None) -> Union[str, None]:
        """Create a backup of the database.
        
        Args:
            backup_path: Optional path for the backup file. If None, a timestamped 
                         path will be created based on the original database path.
                         
        Returns:
            Path to the backup file or None if backup failed.
        """
        if backup_path is None:
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            backup_path = f"{self.db_path}.backup-{timestamp}"
            
        try:
            # First try to use SQLite's backup API
            with db_lock:
                self.logger.info(f"Creating database backup at {backup_path}")
                
                # Try to use the specialized backup API
                try:
                    source_conn = sqlite3.connect(self.db_path)
                    dest_conn = sqlite3.connect(backup_path)
                    
                    source_conn.backup(dest_conn)
                    
                    dest_conn.close()
                    source_conn.close()
                    self.logger.info("Database backup completed using SQLite backup API")
                    return backup_path
                except (sqlite3.OperationalError, AttributeError) as e:
                    self.logger.warning(f"SQLite backup API failed, falling back to file copy: {e}")
                    
                    # Fall back to file copy
                    shutil.copy2(self.db_path, backup_path)
                    self.logger.info("Database backup completed using file copy")
                    return backup_path
                    
        except Exception as e:
            self.logger.error(f"Database backup failed: {str(e)}")
            return None
    
    @contextmanager
    def optimized_bulk_session(self):
        """Session optimized for bulk operations.
        
        This creates a session with settings optimized for bulk inserts or 
        other operations that modify a large number of records.
        
        Use with caution and only for batch operations when no other processes
        need to access the database.
        """
        if not self._session_factory:
            raise ValueError("Database not initialized, call initialize() first")
        
        session = None
        acquired = False
        
        try:
            self.logger.info("Acquiring lock for bulk operations")
            acquired = db_lock.acquire(timeout=30)
            if not acquired:
                raise TimeoutError("Could not acquire database lock for bulk operations")
                
            session = self._session_factory()
            session_id = id(session)
            self.logger.info(f"Created bulk operation session {session_id}")
            
            # Disable autoflush for bulk inserts
            session.autoflush = False
            
            # Begin transaction
            session.execute(text("BEGIN"))
            
            # Set pragmas for faster bulk inserts
            session.execute(text("PRAGMA synchronous = OFF"))
            session.execute(text("PRAGMA journal_mode = MEMORY"))
            
            self.logger.info("Database optimized for bulk operations")
            
            yield session
            
            # Commit the transaction
            session.commit()
            
            # Reset pragmas
            session.execute(text("PRAGMA synchronous = NORMAL"))
            session.execute(text("PRAGMA journal_mode = WAL"))
            
            self.logger.info(f"Bulk operation completed in session {session_id}")
            
        except Exception as e:
            if session:
                self.logger.error(f"Error during bulk operation: {str(e)}")
                session.rollback()
            raise
        finally:
            if session:
                session.close()
            if acquired:
                db_lock.release()
                self.logger.info("Released lock after bulk operation")
    
    def shutdown(self):
        """Gracefully close all database connections."""
        if self._engine:
            self.logger.info("Shutting down database connections")
            # Dispose of the engine to close all connections
            self._engine.dispose()
            # Reset member variables
            self._engine = None
            self._session_factory = None
            self.logger.info("Database connections closed")
    
    def check_schema_version(self, expected_version: str) -> bool:
        """Check if database schema matches expected version.
        
        Args:
            expected_version: The expected schema version string.
            
        Returns:
            True if schema version matches expected version, False otherwise.
        """
        try:
            with self.session() as session:
                # Try to get version
                result = session.execute(text("SELECT version FROM schema_version")).fetchone()
                if result and result[0] == expected_version:
                    self.logger.info(f"Schema version check: OK (version {expected_version})")
                    return True
                if result:
                    self.logger.warning(f"Schema version mismatch: found {result[0]}, expected {expected_version}")
                else:
                    self.logger.warning(f"Schema version not found, expected {expected_version}")
                return False
        except Exception as e:
            # Table or column doesn't exist
            self.logger.warning(f"Schema version check failed: {str(e)}")
            return False
    
    def update_schema_version(self, new_version: str) -> bool:
        """Update schema version number.
        
        Args:
            new_version: The new schema version string to set.
            
        Returns:
            True if update successful, False otherwise.
        """
        try:
            with self.session() as session:
                # Check if table exists
                result = session.execute(text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
                )).fetchone()
                
                if not result:
                    # Create version table
                    self.logger.info("Creating schema_version table")
                    session.execute(text(
                        "CREATE TABLE schema_version (version TEXT)"
                    ))
                    session.execute(text(
                        "INSERT INTO schema_version VALUES (:version)"
                    ), {"version": new_version})
                else:
                    # Update version
                    self.logger.info(f"Updating schema version to {new_version}")
                    session.execute(text(
                        "UPDATE schema_version SET version = :version"
                    ), {"version": new_version})
                return True
        except Exception as e:
            self.logger.error(f"Failed to update schema version: {str(e)}")
            return False
    
    def get_schema_version(self) -> Union[str, None]:
        """Get the current schema version.
        
        Returns:
            The current schema version string, or None if not found.
        """
        try:
            with self.session() as session:
                result = session.execute(text("SELECT version FROM schema_version LIMIT 1")).fetchone()
                return result[0] if result else None
        except Exception:
            return None

# Create a singleton instance
db_manager = DatabaseManager()

def get_db_manager(db_path=None) -> DatabaseManager:
    """Get the database manager singleton instance."""
    # Always initialize if path is provided, regardless of _initialized state
    # This ensures we don't rely on the possibly incorrect _initialized flag
    if db_path:
        db_manager.initialize(db_path)
    elif not db_manager._initialized:
        raise ValueError("Database path must be provided for initialization")
    return db_manager
