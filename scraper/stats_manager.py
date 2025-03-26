"""
Stats manager for the web crawler.

This module provides the StatsManager class for tracking and storing crawler statistics.
"""

import json
import logging
import threading
import time
from typing import Dict, Optional, Any

from scraper.db_manager import CrawlDatabase

class StatsManager:
    """
    Manages statistics for the crawler.
    
    This class tracks various statistics about the crawl process and
    periodically saves them to the database.
    """
    
    def __init__(self, db: CrawlDatabase, config: Dict = None, update_interval: int = 10, logger=None):
        """
        Initialize the stats manager.
        
        Args:
            db: Database manager
            config: Crawler configuration
            update_interval: Interval in seconds for updating stats in the database
            logger: Logger instance
        """
        self.db = db
        self.config = config or {}
        self.update_interval = update_interval
        self.logger = logger or logging.getLogger(__name__)
        
        # Initialize stats
        self.stats = {
            'urls_crawled': 0,
            'urls_queued': 0,
            'urls_failed': 0,
            'bytes_downloaded': 0,
            'start_time': time.time(),
            'last_update_time': time.time(),
            'domains_crawled': set(),
            'status_codes': {},
            'content_types': {},
            'errors': []
        }
        
        # Thread safety
        self.lock = threading.Lock()
        
        # Current stats ID in the database
        self.current_stats_id = self._create_stats_entry()
        
        # Start background thread for periodic updates
        self.running = True
        self.update_thread = threading.Thread(target=self._update_thread, daemon=True)
        self.update_thread.start()
        
    def _create_stats_entry(self) -> int:
        """
        Create a new stats entry in the database.
        
        Returns:
            int: ID of the new stats entry
        """
        try:
            # Convert config to JSON string
            config_json = json.dumps(self.config)
            
            # Insert new stats entry
            self.db.execute(
                """
                INSERT INTO crawl_stats (start_time, config)
                VALUES (CURRENT_TIMESTAMP, ?)
                """,
                (config_json,)
            )
            
            # Get the ID of the new entry
            self.db.execute("SELECT last_insert_rowid()")
            stats_id = self.db.fetchone()[0]
            
            self.logger.info(f"Created new stats entry with ID {stats_id}")
            return stats_id
            
        except Exception as e:
            self.logger.error(f"Error creating stats entry: {str(e)}")
            return -1
            
    def _update_thread(self):
        """
        Background thread for periodically updating stats in the database.
        """
        while self.running:
            # Sleep for the update interval
            time.sleep(self.update_interval)
            
            # Update stats in the database
            self.save_stats()
            
    def save_stats(self):
        """
        Save current stats to the database.
        """
        if self.current_stats_id < 0:
            return
            
        try:
            with self.lock:
                # Update stats entry
                self.db.execute(
                    """
                    UPDATE crawl_stats
                    SET urls_crawled = ?,
                        urls_queued = ?,
                        urls_failed = ?,
                        bytes_downloaded = ?,
                        status = ?
                    WHERE id = ?
                    """,
                    (
                        self.stats['urls_crawled'],
                        self.stats['urls_queued'],
                        self.stats['urls_failed'],
                        self.stats['bytes_downloaded'],
                        'running',
                        self.current_stats_id
                    )
                )
                
                self.logger.debug(f"Updated stats in database (ID: {self.current_stats_id})")
                
        except Exception as e:
            self.logger.error(f"Error saving stats: {str(e)}")
            
    def increment(self, stat: str, value: int = 1):
        """
        Increment a numeric stat.
        
        Args:
            stat: Name of the stat to increment
            value: Value to increment by
        """
        with self.lock:
            if stat in self.stats and isinstance(self.stats[stat], (int, float)):
                self.stats[stat] += value
                
    def set(self, stat: str, value: Any):
        """
        Set a stat to a specific value.
        
        Args:
            stat: Name of the stat to set
            value: Value to set
        """
        with self.lock:
            self.stats[stat] = value
            
    def add_to_set(self, stat: str, value: Any):
        """
        Add a value to a set stat.
        
        Args:
            stat: Name of the set stat
            value: Value to add
        """
        with self.lock:
            if stat in self.stats and isinstance(self.stats[stat], set):
                self.stats[stat].add(value)
                
    def increment_dict(self, stat: str, key: Any, value: int = 1):
        """
        Increment a value in a dictionary stat.
        
        Args:
            stat: Name of the dictionary stat
            key: Key in the dictionary
            value: Value to increment by
        """
        with self.lock:
            if stat in self.stats and isinstance(self.stats[stat], dict):
                if key not in self.stats[stat]:
                    self.stats[stat][key] = 0
                self.stats[stat][key] += value
                
    def add_error(self, error: str, url: str = None):
        """
        Add an error to the error list.
        
        Args:
            error: Error message
            url: URL that caused the error
        """
        with self.lock:
            error_entry = {
                'time': time.time(),
                'error': error
            }
            if url:
                error_entry['url'] = url
                
            self.stats['errors'].append(error_entry)
            
            # Limit the number of errors stored
            if len(self.stats['errors']) > 100:
                self.stats['errors'] = self.stats['errors'][-100:]
                
    def get_stats(self) -> Dict:
        """
        Get a copy of the current stats.
        
        Returns:
            dict: Current stats
        """
        with self.lock:
            # Create a copy of the stats
            stats_copy = dict(self.stats)
            
            # Convert sets to lists for JSON serialization
            for key, value in stats_copy.items():
                if isinstance(value, set):
                    stats_copy[key] = list(value)
                    
            # Calculate derived stats
            stats_copy['elapsed_time'] = time.time() - stats_copy['start_time']
            if stats_copy['elapsed_time'] > 0:
                stats_copy['urls_per_second'] = stats_copy['urls_crawled'] / stats_copy['elapsed_time']
            else:
                stats_copy['urls_per_second'] = 0
                
            return stats_copy
    
    def update(self, stats_dict: Dict):
        """
        Update stats with values from a dictionary.
        
        Args:
            stats_dict: Dictionary with stats to update
        """
        with self.lock:
            for key, value in stats_dict.items():
                if key in self.stats:
                    # Handle different types of values
                    if isinstance(self.stats[key], (int, float)) and isinstance(value, (int, float)):
                        self.stats[key] = value
                    elif isinstance(self.stats[key], dict) and isinstance(value, dict):
                        self.stats[key].update(value)
                    elif isinstance(self.stats[key], set) and isinstance(value, (list, set)):
                        self.stats[key].update(value)
                    else:
                        self.stats[key] = value
            
            # Save updated stats to database
            self.save_stats()
            
    def finish(self):
        """
        Mark the crawl as finished and save final stats.
        """
        try:
            with self.lock:
                # Stop the update thread
                self.running = False
                
                # Update stats entry with end time and status
                self.db.execute(
                    """
                    UPDATE crawl_stats
                    SET end_time = CURRENT_TIMESTAMP,
                        urls_crawled = ?,
                        urls_queued = ?,
                        urls_failed = ?,
                        bytes_downloaded = ?,
                        status = ?
                    WHERE id = ?
                    """,
                    (
                        self.stats['urls_crawled'],
                        self.stats['urls_queued'],
                        self.stats['urls_failed'],
                        self.stats['bytes_downloaded'],
                        'completed',
                        self.current_stats_id
                    )
                )
                
                self.logger.info(f"Finalized stats in database (ID: {self.current_stats_id})")
                
        except Exception as e:
            self.logger.error(f"Error finalizing stats: {str(e)}")
            
    def get_all_crawls(self):
        """
        Get information about all crawls.
        
        Returns:
            list: List of crawl information dictionaries
        """
        try:
            self.db.execute(
                """
                SELECT id, start_time, end_time, urls_crawled, urls_queued, urls_failed, 
                       bytes_downloaded, status
                FROM crawl_stats
                ORDER BY start_time DESC
                """
            )
            rows = self.db.fetchall()
            
            crawls = []
            for row in rows:
                crawl_id, start_time, end_time, urls_crawled, urls_queued, urls_failed, bytes_downloaded, status = row
                
                crawls.append({
                    'id': crawl_id,
                    'start_time': start_time,
                    'end_time': end_time,
                    'urls_crawled': urls_crawled,
                    'urls_queued': urls_queued,
                    'urls_failed': urls_failed,
                    'bytes_downloaded': bytes_downloaded,
                    'status': status
                })
                
            return crawls
            
        except Exception as e:
            self.logger.error(f"Error getting all crawls: {str(e)}")
            return []
