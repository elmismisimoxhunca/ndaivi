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
    
    def __init__(self, component_type: str, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize the statistics manager.
        
        Args:
            component_type: Type of component ('scraper', 'finder', or 'combined')
            db_manager: Database manager instance for database operations
        """
        self.component_type = component_type
        self.db_manager = db_manager or get_db_manager()
        self.logger = logging.getLogger(f"ndaivi.statistics.{component_type}")
        
        # Initialize session ID
        self.session_id = None
        
        # Initialize statistics based on component type
        self.stats = self._initialize_stats()
        
        # Create a new session in the database
        self._create_session()
    
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
        }
        
        if self.component_type == 'scraper':
            return {
                **base_stats,
                "urls_processed": 0,
                "manufacturers_extracted": 0,
                "websites_found": 0,
                "pages_crawled": 0,
                "manual_links_found": 0,
                "http_errors": 0,
                "connection_errors": 0,
                "timeout_errors": 0,
                "dns_resolution_errors": 0,
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
                
                # Create new session record
                new_session = ScraperSession(
                    session_type=session_type_map.get(self.component_type, 'unknown'),
                    start_time=datetime.datetime.now(),
                    status='running',
                    urls_processed=0,
                    manufacturers_extracted=0,
                    websites_found=0
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
            self.stats["runtime_seconds"] = (end_time - self.stats["start_time"]).total_seconds()
        
        return self.stats
    
    def display_statistics(self, detailed: bool = False) -> None:
        """
        Display statistics in a readable format.
        
        Args:
            detailed: Whether to display detailed statistics
        """
        stats = self.get_all_stats()
        
        self.logger.info(f"=== {self.component_type.upper()} STATISTICS ===")
        
        # Display runtime statistics
        runtime_minutes = stats["runtime_seconds"] / 60
        self.logger.info(f"Runtime: {runtime_minutes:.2f} minutes")
        
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
        self.logger.info(f"URLs processed: {stats.get('urls_processed', 0)}")
        self.logger.info(f"Manufacturers extracted: {stats.get('manufacturers_extracted', 0)}")
        self.logger.info(f"Websites found: {stats.get('websites_found', 0)}")
        self.logger.info(f"Pages crawled: {stats.get('pages_crawled', 0)}")
        self.logger.info(f"Manual links found: {stats.get('manual_links_found', 0)}")
        
        if detailed:
            self.logger.info(f"HTTP errors: {stats.get('http_errors', 0)}")
            self.logger.info(f"Connection errors: {stats.get('connection_errors', 0)}")
            self.logger.info(f"Timeout errors: {stats.get('timeout_errors', 0)}")
            self.logger.info(f"DNS resolution errors: {stats.get('dns_resolution_errors', 0)}")
    
    def _display_finder_statistics(self, stats: Dict[str, Any], detailed: bool) -> None:
        """
        Display finder-specific statistics.
        
        Args:
            stats: Statistics dictionary
            detailed: Whether to display detailed statistics
        """
        processed = stats.get('manufacturers_processed', 0)
        claude_found = stats.get('claude_found', 0)
        search_found = stats.get('search_engine_found', 0)
        validation_failed = stats.get('validation_failed', 0)
        success_rate = (claude_found + search_found - validation_failed) / processed * 100 if processed > 0 else 0
        
        self.logger.info(f"Manufacturers processed: {processed}")
        self.logger.info(f"Websites found via Claude: {claude_found}")
        self.logger.info(f"Websites found via search engines: {search_found}")
        self.logger.info(f"Validation failures: {validation_failed}")
        self.logger.info(f"Success rate: {success_rate:.2f}%")
        
        if detailed:
            self.logger.info(f"Total manufacturers: {stats.get('total_manufacturers', 0)}")
            self.logger.info(f"Manufacturers with website: {stats.get('manufacturers_with_website', 0)}")
            self.logger.info(f"DNS resolution errors: {stats.get('dns_resolution_errors', 0)}")
            self.logger.info(f"Connection errors: {stats.get('connection_errors', 0)}")
            self.logger.info(f"Timeout errors: {stats.get('timeout_errors', 0)}")
    
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
                
                # Update session statistics based on component type
                if self.component_type in ['scraper', 'combined']:
                    current_session.urls_processed = self.stats.get('urls_processed', 0)
                    current_session.manufacturers_extracted = self.stats.get('manufacturers_extracted', 0)
                    current_session.websites_found = self.stats.get('websites_found', 0)
                
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
        runtime_minutes = runtime_seconds / 60
        
        self.logger.info(f"{self.component_type.capitalize()} session completed in {runtime_minutes:.2f} minutes")
