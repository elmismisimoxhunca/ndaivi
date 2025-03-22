#!/usr/bin/env python3

"""
Statistics Manager for NDAIVI

Handles statistics tracking, display, and database operations for different
components: scraper, manufacturer website finder, and combined operations.
"""

import datetime
import logging
from typing import Dict, Any, Optional, List, Union

from database.db_manager import DatabaseManager, get_db_manager
from database.schema import ScraperSession


class StatisticsManager:
    """
    Central manager for handling statistics across different components of NDAIVI.
    
    This class provides a unified interface for tracking, updating, and displaying
    statistics for the scraper, manufacturer website finder, and combined operations.
    It handles both in-memory statistics and database persistence.
    """
    
    def __init__(self, component_type: str, db_manager: Optional[DatabaseManager] = None, create_session: bool = True):
        """
        Initialize the statistics manager.
        
        Args:
            component_type: Type of component ('scraper', 'finder', or 'combined')
            db_manager: Database manager instance for database operations
            create_session: Whether to automatically create a new session
        """
        self.component_type = component_type
        self.db_manager = db_manager or get_db_manager()
        self.logger = logging.getLogger(f"ndaivi.statistics.{component_type}")
        
        # Initialize session ID
        self.session_id = None
        
        # Initialize statistics based on component type
        self.stats = self._initialize_stats()
        
        # Create session only if requested
        if create_session:
            self.create_session()
    
    def _initialize_stats(self) -> Dict[str, Any]:
        """
        Initialize statistics dictionary based on component type.
        
        Returns:
            Dictionary of initial statistics
        """
        # Common stats for all components
        base_stats = {
            "start_time": datetime.datetime.now(),
            "end_time": None,
            "runtime_seconds": 0,
            "max_pages": None,
            "max_runtime_minutes": None,
            "error": None
        }
        
        if self.component_type == 'scraper':
            return {
                **base_stats,
                "urls_processed": 0,
                "manufacturers_extracted": 0,
                "manufacturers_found": 0,  # Added for tracking manufacturer pages found
                "categories_extracted": 0,  # Added for tracking categories extracted
                "websites_found": 0,
                "pages_crawled": 0,
                "pages_analyzed": 0,  # Added for Claude Haiku analysis
                "manual_links_found": 0,
                "translations": 0,  # Added for tracking translations
                "http_errors": 0,
                "connection_errors": 0,
                "timeout_errors": 0,
                "dns_resolution_errors": 0,
                "errors": 0,  # General error counter
                "status": "running",
            }
        elif self.component_type == 'finder':
            return {
                **base_stats,
                "total_manufacturers": 0,
                "manufacturers_with_website": 0,
                "manufacturers_processed": 0,
                "claude_found": 0,
                "search_engine_found": 0,
                "validation_failed": 0,
                "websites_validated": 0,
                "dns_resolution_errors": 0,
                "connection_errors": 0,
                "timeout_errors": 0,
            }
        elif self.component_type == 'combined':
            # Combined stats include both scraper and finder metrics
            scraper_stats = self._initialize_stats_for_type('scraper')
            finder_stats = self._initialize_stats_for_type('finder')
            # Remove duplicates
            for key in list(finder_stats.keys()):
                if key in base_stats:
                    del finder_stats[key]
            
            return {**base_stats, **scraper_stats, **finder_stats}
        else:
            self.logger.warning(f"Unknown component type: {self.component_type}. Using generic stats.")
            return base_stats
    
    def _initialize_stats_for_type(self, component_type: str) -> Dict[str, Any]:
        """
        Helper method to initialize stats for a specific component type.
        
        Args:
            component_type: Type of component to initialize stats for
            
        Returns:
            Dictionary of initial statistics for the specified component
        """
        # Save current component type
        current_type = self.component_type
        
        # Set temporary component type
        self.component_type = component_type
        
        # Get stats for the component type
        stats = self._initialize_stats()
        
        # Restore original component type
        self.component_type = current_type
        
        return stats
    
    def create_session(self) -> None:
        """
        Create a new statistics session in the database.
        
        This public method provides a clean interface to create a new session
        for tracking statistics during a scraping, finding, or combined operation.
        """
        self._create_session()
    
    def _create_session(self) -> None:
        """
        Create a new session in the database.
        """
        try:
            with self.db_manager.session() as db_session:
                # Map component types to session types
                session_type_map = {
                    'scraper': 'scraper',
                    'finder': 'finder',
                    'combined': 'combined'
                }
                
                # Create new session record with all fields initialized
                new_session = ScraperSession(
                    session_type=session_type_map.get(self.component_type, 'unknown'),
                    start_time=datetime.datetime.now(),
                    status='running',
                    runtime_seconds=0,
                    # Common statistics
                    urls_processed=0,
                    manufacturers_extracted=0,
                    manufacturers_found=0,
                    categories_extracted=0,
                    websites_found=0,
                    translations=0,
                    # Page tracking
                    pages_crawled=0,
                    pages_analyzed=0,
                    manual_links_found=0,
                    # Error tracking
                    http_errors=0,
                    connection_errors=0,
                    timeout_errors=0,
                    dns_resolution_errors=0,
                    errors=0
                )
                
                db_session.add(new_session)
                db_session.commit()
                
                # Store the session ID
                self.session_id = new_session.id
                self.logger.info(f"Created new {self.component_type} session with ID: {self.session_id}")
                
        except Exception as e:
            self.logger.error(f"Error creating database session: {str(e)}")
    
    def update_stat(self, key: str, value: Any, increment: bool = False) -> None:
        """
        Update a specific statistic.
        
        Args:
            key: The statistic key to update
            value: The value to set or increment by
            increment: If True, increment the current value; otherwise, set it
        """
        if key not in self.stats:
            self.logger.warning(f"Unknown statistic key: {key}")
            self.stats[key] = value
            return
            
        # Special handling for datetime fields
        if key in ["start_time", "end_time"] and value is not None:
            # Ensure we always store datetime objects, not strings
            if isinstance(value, str):
                try:
                    value = datetime.datetime.fromisoformat(value)
                except ValueError:
                    self.logger.error(f"Invalid datetime format for {key}: {value}")
                    # Keep existing value
                    return
        
        if increment:
            if isinstance(self.stats[key], (int, float)):
                self.stats[key] += value
            else:
                self.logger.warning(f"Cannot increment non-numeric statistic: {key}")
        else:
            self.stats[key] = value
    
    def update_multiple_stats(self, stats_dict: Dict[str, Any], increment: bool = False) -> None:
        """
        Update multiple statistics at once.
        
        Args:
            stats_dict: Dictionary of statistics to update
            increment: If True, increment current values; otherwise, set them
        """
        for key, value in stats_dict.items():
            self.update_stat(key, value, increment)
    
    def update_stats(self, stats_dict: Dict[str, Any], increment: bool = False) -> None:
        """
        Legacy method that forwards to update_multiple_stats for backward compatibility.
        
        Args:
            stats_dict: Dictionary of statistics to update
            increment: Whether to increment existing stats or replace them
        """
        self.update_multiple_stats(stats_dict, increment)
    
    def get_stat(self, key: str) -> Any:
        """
        Get a specific statistic value.
        
        Args:
            key: The statistic key to retrieve
            
        Returns:
            The value of the statistic or None if not found
        """
        return self.stats.get(key)
    
    def get_all_stats(self) -> Dict[str, Any]:
        """
        Get all statistics.
        
        Returns:
            Dictionary of all statistics
        """
        # Update runtime before returning
        if self.stats["start_time"]:
            end_time = self.stats["end_time"] or datetime.datetime.now()
            
            # Ensure start_time is a datetime object
            start_time = self.stats["start_time"]
            if isinstance(start_time, str):
                try:
                    # Try to parse the string to a datetime object
                    start_time = datetime.datetime.fromisoformat(start_time)
                    # Update the stats with the corrected start_time
                    self.stats["start_time"] = start_time
                except ValueError:
                    self.logger.error(f"Invalid start_time format: {start_time}")
                    # Don't update runtime in this case
                    return self.stats
            
            # Now calculate runtime with the corrected start_time
            self.stats["runtime_seconds"] = (end_time - start_time).total_seconds()
        
        return self.stats
    
    def display_statistics(self, detailed: bool = False) -> None:
        """
        Display statistics in a readable format.
        
        Args:
            detailed: Whether to display detailed statistics
        """
        stats = self.get_all_stats()
        
        self.logger.info(f"=== {self.component_type.upper()} STATISTICS ===")
        
        # Display session information
        self.logger.info(f"Session ID: {self.session_id}")
        self.logger.info(f"Status: {stats.get('status', 'running').upper()}")
        
        # Display runtime statistics
        runtime_seconds = int(stats["runtime_seconds"])
        hours = runtime_seconds // 3600
        minutes = (runtime_seconds % 3600) // 60
        seconds = runtime_seconds % 60
        self.logger.info(f"Runtime: {hours}h {minutes}m {seconds}s")
        
        # Display error statistics if any errors occurred
        total_errors = sum([
            stats.get('http_errors', 0),
            stats.get('connection_errors', 0),
            stats.get('timeout_errors', 0),
            stats.get('dns_resolution_errors', 0)
        ])
        
        if total_errors > 0 or detailed:
            self.logger.info("\n=== ERROR STATISTICS ===")
            self.logger.info(f"HTTP Errors: {stats.get('http_errors', 0)}")
            self.logger.info(f"Connection Errors: {stats.get('connection_errors', 0)}")
            self.logger.info(f"Timeout Errors: {stats.get('timeout_errors', 0)}")
            self.logger.info(f"DNS Resolution Errors: {stats.get('dns_resolution_errors', 0)}")
            self.logger.info(f"Total Errors: {total_errors}")
        
        # Display component-specific statistics
        if self.component_type == 'scraper':
            self._display_scraper_statistics(stats, detailed)
        elif self.component_type == 'finder':
            self._display_finder_statistics(stats, detailed)
        elif self.component_type == 'combined':
            self._display_scraper_statistics(stats, detailed)
            self._display_finder_statistics(stats, detailed)
        
        self.logger.info("=============================")
    
    def _display_scraper_statistics(self, stats: Dict[str, Any], detailed: bool) -> None:
        """
        Display scraper-specific statistics.
        
        Args:
            stats: Statistics dictionary
            detailed: Whether to display detailed statistics
        """
        self.logger.info("\n=== SCRAPING STATISTICS ===")
        self.logger.info(f"URLs processed: {stats.get('urls_processed', 0)}")
        self.logger.info(f"Manufacturers extracted: {stats.get('manufacturers_extracted', 0)}")
        self.logger.info(f"Websites found: {stats.get('websites_found', 0)}")
        self.logger.info(f"Pages crawled: {stats.get('pages_crawled', 0)}")
        self.logger.info(f"Manual links found: {stats.get('manual_links_found', 0)}")
        
        if detailed:
            success_rate = 0
            if stats.get('urls_processed', 0) > 0:
                success_rate = (stats.get('manufacturers_extracted', 0) / stats.get('urls_processed', 0)) * 100
            self.logger.info(f"Success rate: {success_rate:.2f}%")
            self.logger.info(f"Categories extracted: {stats.get('categories_extracted', 0)}")
            if stats.get('manufacturers_extracted', 0) > 0:
                categories_per_manufacturer = stats.get('categories_extracted', 0) / stats.get('manufacturers_extracted', 0)
                self.logger.info(f"Categories per manufacturer: {categories_per_manufacturer:.2f}")
    
    def _display_finder_statistics(self, stats: Dict[str, Any], detailed: bool) -> None:
        """
        Display finder-specific statistics.
        
        Args:
            stats: Statistics dictionary
            detailed: Whether to display detailed statistics
        """
        self.logger.info("\n=== WEBSITE FINDER STATISTICS ===")
        processed = stats.get('manufacturers_processed', 0)
        claude_found = stats.get('claude_found', 0)
        search_found = stats.get('search_engine_found', 0)
        validation_failed = stats.get('validation_failed', 0)
        websites_found = claude_found + search_found - validation_failed
        success_rate = (websites_found / processed * 100) if processed > 0 else 0
        
        self.logger.info(f"Manufacturers processed: {processed}")
        self.logger.info(f"Total websites found: {websites_found}")
        self.logger.info(f"Found via Claude: {claude_found}")
        self.logger.info(f"Found via search engines: {search_found}")
        self.logger.info(f"Validation failures: {validation_failed}")
        self.logger.info(f"Success rate: {success_rate:.2f}%")
        
        if detailed:
            self.logger.info("\n=== DETAILED FINDER STATISTICS ===")
            total_manufacturers = stats.get('total_manufacturers', 0)
            manufacturers_with_website = stats.get('manufacturers_with_website', 0)
            self.logger.info(f"Total manufacturers in database: {total_manufacturers}")
            self.logger.info(f"Manufacturers with website: {manufacturers_with_website}")
            if total_manufacturers > 0:
                coverage = (manufacturers_with_website / total_manufacturers) * 100
                self.logger.info(f"Database coverage: {coverage:.2f}%")
            
            # Search engine usage
            if 'search_engine_usage' in stats:
                self.logger.info("\n=== SEARCH ENGINE USAGE ===")
                for engine, count in stats['search_engine_usage'].items():
                    self.logger.info(f"{engine}: {count} queries")
    
    def update_session_in_database(self, completed: bool = False) -> None:
        """
        Update the session statistics in the database.
        
        Args:
            completed: Whether the session is completed
        """
        if not self.session_id:
            self.logger.warning("Cannot update session in database: No session ID")
            return
        
        try:
            with self.db_manager.session() as db_session:
                # Get the current session record
                current_session = db_session.query(ScraperSession).filter_by(id=self.session_id).first()
                
                if not current_session:
                    self.logger.warning(f"Session with ID {self.session_id} not found in database")
                    return
                
                # Update common statistics
                current_session.http_errors = self.stats.get('http_errors', 0)
                current_session.connection_errors = self.stats.get('connection_errors', 0)
                current_session.timeout_errors = self.stats.get('timeout_errors', 0)
                current_session.dns_resolution_errors = self.stats.get('dns_resolution_errors', 0)
                
                # Update runtime
                if self.stats["start_time"]:
                    end_time = self.stats["end_time"] or datetime.datetime.now()
                    current_session.runtime_seconds = int((end_time - self.stats["start_time"]).total_seconds())
                
                # Update component-specific statistics
                if self.component_type in ['scraper', 'combined']:
                    current_session.urls_processed = self.stats.get('urls_processed', 0)
                    current_session.manufacturers_extracted = self.stats.get('manufacturers_extracted', 0)
                    current_session.manufacturers_found = self.stats.get('manufacturers_found', 0)
                    current_session.categories_extracted = self.stats.get('categories_extracted', 0)
                    current_session.websites_found = self.stats.get('websites_found', 0)
                    current_session.translations = self.stats.get('translations', 0)
                    current_session.pages_crawled = self.stats.get('pages_crawled', 0)
                    current_session.pages_analyzed = self.stats.get('pages_analyzed', 0)
                    current_session.manual_links_found = self.stats.get('manual_links_found', 0)
                    current_session.errors = self.stats.get('errors', 0)
                
                elif self.component_type == 'finder':
                    current_session.manufacturers_extracted = self.stats.get('manufacturers_processed', 0)
                    current_session.urls_processed = (
                        self.stats.get('claude_found', 0) + 
                        self.stats.get('search_engine_found', 0)
                    )
                    current_session.websites_found = (
                        self.stats.get('claude_found', 0) + 
                        self.stats.get('search_engine_found', 0) - 
                        self.stats.get('validation_failed', 0)
                    )
                    current_session.errors = self.stats.get('errors', 0)
                
                # Mark session as completed if requested
                if completed:
                    current_session.status = 'completed'
                    current_session.end_time = datetime.datetime.now()
                
                # Commit the changes to the database
                db_session.commit()
                self.logger.info(f"Updated session statistics in database for session ID: {self.session_id}")
                
        except Exception as e:
            self.logger.error(f"Error updating session statistics in database: {str(e)}")
    
    def complete_session(self) -> None:
        """
        Complete the current session, updating end time and status.
        """
        self.stats["end_time"] = datetime.datetime.now()
        self.update_session_in_database(completed=True)
        
        # Calculate and log final runtime
        runtime_seconds = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        self.stats["runtime_seconds"] = runtime_seconds
    
    def start_batch(self) -> None:
        """
        Start a new batch of operations within the current session.
        This is used for tracking statistics for a subset of operations.
        """
        self.batch_stats = {
            "start_time": datetime.datetime.now(),
            "end_time": None,
            "runtime_seconds": 0,
            "pages_crawled": 0,
            "manufacturers_found": 0,
            "categories_found": 0,
            "status": "running"
        }
        self.logger.info(f"Started new batch in session {self.session_id}")
    
    def end_batch(self) -> None:
        """
        End the current batch of operations, updating end time and runtime.
        """
        if not hasattr(self, 'batch_stats'):
            self.logger.warning("Cannot end batch: No batch was started")
            return
            
        self.batch_stats["end_time"] = datetime.datetime.now()
        
        # Calculate runtime
        if self.batch_stats["start_time"]:
            runtime_seconds = (self.batch_stats["end_time"] - self.batch_stats["start_time"]).total_seconds()
            self.batch_stats["runtime_seconds"] = runtime_seconds
            
        self.logger.info(f"Ended batch in session {self.session_id}. Runtime: {runtime_seconds:.2f} seconds")
    
    def increment_stat(self, key: str, value: int = 1) -> None:
        """
        Increment a specific statistic by the given value.
        This is a convenience method for update_stat with increment=True.
        
        Args:
            key: The statistic key to increment
            value: The value to increment by (default: 1)
        """
        self.update_stat(key, value, increment=True)
        
        # Also update batch stats if we're in a batch
        if hasattr(self, 'batch_stats') and key in self.batch_stats:
            self.batch_stats[key] += value
    
    def get_batch_stats(self) -> Dict[str, Any]:
        """
        Get statistics for the current batch.
        
        Returns:
            Dictionary of batch statistics or None if no batch is active
        """
        if not hasattr(self, 'batch_stats'):
            self.logger.warning("No batch statistics available")
            return {}
            
        return self.batch_stats
        
    def get_session_stats(self, session_type=None, limit=10):
        """
        Get statistics for completed sessions from the database.
        
        Args:
            session_type: Type of sessions to retrieve ('scraper', 'finder', or 'combined')
                          If None, retrieve all session types
            limit: Maximum number of sessions to retrieve
            
        Returns:
            Dictionary with session statistics
        """
        try:
            with self.db_manager.session() as db_session:
                query = db_session.query(ScraperSession)
                
                # Filter by session type if specified
                if session_type and session_type != 'all':
                    query = query.filter(ScraperSession.session_type == session_type)
                
                # Order by start time (descending - newest first)
                query = query.order_by(ScraperSession.start_time.desc())
                
                # Limit results
                if limit:
                    query = query.limit(limit)
                
                sessions = query.all()
                
                # Format session data for display
                session_data = []
                for session in sessions:
                    session_dict = {
                        'id': session.id,
                        'session_type': session.session_type,
                        'start_time': session.start_time,
                        'end_time': session.end_time,
                        'status': session.status,
                        'runtime_seconds': session.runtime_seconds,
                        'pages_crawled': session.pages_crawled,
                        'manufacturers_extracted': session.manufacturers_extracted,
                        'manufacturers_found': session.manufacturers_found,
                        'categories_extracted': session.categories_extracted,
                        'urls_processed': session.urls_processed,
                        'errors': session.errors
                    }
                    session_data.append(session_dict)
                
                return {
                    'sessions': session_data,
                    'count': len(session_data),
                    'type_filter': session_type or 'all'
                }
                
        except Exception as e:
            self.logger.error(f"Error retrieving session stats: {str(e)}")
            return {
                'sessions': [],
                'count': 0,
                'error': str(e)
            }
