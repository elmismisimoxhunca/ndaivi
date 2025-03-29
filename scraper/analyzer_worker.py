#!/usr/bin/env python3
"""
Analyzer Worker for NDAIVI

This module provides a worker implementation for the Claude Analyzer
that integrates with the Redis message system. It subscribes to
analyzer command channels and publishes status updates.
"""

import json
import logging
import threading
import time
from typing import Dict, List, Any, Optional, Union

from scraper.claude_analyzer import ClaudeAnalyzer
from scraper.db_manager import DBManager
from utils.redis_manager import get_redis_manager

# Configure logging
logger = logging.getLogger(__name__)

class AnalyzerWorker:
    """
    Worker implementation for the Claude Analyzer that integrates with Redis.
    
    This class wraps the ClaudeAnalyzer and handles communication with other
    components through the Redis message system.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the analyzer worker with configuration.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.analyzer_config = config.get('claude_analyzer', {})
        self.redis_manager = get_redis_manager(config)
        
        # Initialize the Claude analyzer with the full config
        # Pass None as config_path to use the ConfigManager's default behavior
        self.analyzer = ClaudeAnalyzer(None)
        
        # Initialize the database manager
        db_path = config.get('web_crawler', {}).get('db_path', 'scraper/data/crawler.db')
        self.db_manager = DBManager(db_path)
        
        # Worker state
        self.running = False
        self.worker_thread = None
        self.queue = []
        self.processing = False
        self.batch_size = config.get('application', {}).get('components', {}).get('analyzer', {}).get('batch_size', 10)
        
        # Get channel names from config
        self.command_channel = self.redis_manager.get_channel('analyzer_commands')
        self.status_channel = self.redis_manager.get_channel('analyzer_status')
        self.stats_channel = self.redis_manager.get_channel('stats')
    
    def start(self) -> bool:
        """
        Start the analyzer worker and subscribe to command channel.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        if self.running:
            logger.warning("Analyzer worker already running")
            return True
        
        try:
            # Subscribe to command channel
            self.redis_manager.subscribe(self.command_channel, self._handle_command)
            
            # Start worker thread
            self.running = True
            self.worker_thread = threading.Thread(target=self._worker_loop)
            self.worker_thread.daemon = True
            self.worker_thread.start()
            
            # Publish status update
            self._publish_status('idle', 'Analyzer worker started')
            
            logger.info("Analyzer worker started successfully")
            return True
        except Exception as e:
            logger.error(f"Error starting analyzer worker: {e}")
            self.running = False
            return False
    
    def stop(self) -> None:
        """Stop the analyzer worker."""
        if not self.running:
            return
        
        self.running = False
        
        # Wait for worker thread to terminate
        if self.worker_thread:
            self.worker_thread.join(timeout=1.0)
        
        # Unsubscribe from command channel
        self.redis_manager.unsubscribe(self.command_channel)
        
        # Publish status update
        self._publish_status('stopped', 'Analyzer worker stopped')
        
        logger.info("Analyzer worker stopped")
    
    def _worker_loop(self) -> None:
        """
        Main worker loop that processes analyzer jobs.
        This runs in a separate thread.
        """
        stats_interval = 10  # Update stats every 10 seconds
        last_stats_time = 0
        
        while self.running:
            try:
                # Process items in the queue
                if self.queue and not self.processing:
                    self._process_batch()
                
                # Publish stats periodically
                current_time = time.time()
                if current_time - last_stats_time >= stats_interval:
                    self._publish_stats()
                    last_stats_time = current_time
                
                # Small sleep to prevent CPU hogging
                time.sleep(0.01)
            except Exception as e:
                logger.error(f"Error in analyzer worker loop: {e}")
                time.sleep(0.1)  # Small delay on errors
    
    def _process_batch(self) -> None:
        """Process a batch of items from the queue."""
        self.processing = True
        
        try:
            # Get a batch of items to process
            batch = self.queue[:self.batch_size]
            self.queue = self.queue[self.batch_size:]
            
            # Update status
            self._publish_status('analyzing', f"Processing batch of {len(batch)} items")
            
            # Process each item in the batch
            for item in batch:
                self._process_item(item)
            
            # Update status
            self._publish_status('idle', f"Batch processing complete")
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            self._publish_status('error', f"Error processing batch: {str(e)}")
        finally:
            self.processing = False
    
    def _process_item(self, item: Dict[str, Any]) -> None:
        """
        Process a single item from the queue.
        
        Args:
            item: Item to process
        """
        try:
            url = item.get('url')
            content = item.get('content', '')
            title = item.get('title', '')
            
            if not url or not content:
                logger.warning(f"Skipping item with missing URL or content: {url}")
                return
            
            # Update status
            self._publish_status('analyzing', f"Analyzing URL: {url}")
            
            # Step 1: Keyword Filter
            if not self.analyzer.keyword_filter(title, content):
                logger.info(f"URL failed keyword filter: {url}")
                self._update_analysis_result(url, {'status': 'filtered', 'reason': 'keyword_filter'})
                return
            
            # Step 2: Metadata Analysis
            metadata_result = self.analyzer.analyze_metadata(title, url)
            if metadata_result.get('is_conclusive', False):
                self._update_analysis_result(url, metadata_result)
                return
            
            # Step 3: Link Analysis
            links = item.get('links', [])
            link_result = self.analyzer.analyze_links(links, url)
            if link_result.get('is_conclusive', False):
                self._update_analysis_result(url, link_result)
                return
            
            # Step 4: Content Analysis
            content_result = self.analyzer.analyze_content(content, url)
            self._update_analysis_result(url, content_result)
            
        except Exception as e:
            logger.error(f"Error processing item {item.get('url')}: {e}")
            self._publish_status('error', f"Error processing item {item.get('url')}: {str(e)}")
    
    def _update_analysis_result(self, url: str, result: Dict[str, Any]) -> None:
        """
        Update the analysis result in the database and publish results.
        
        Args:
            url: URL that was analyzed
            result: Analysis result dictionary
        """
        try:
            # Update database
            self.db_manager.update_analysis(
                url=url,
                analysis_type=result.get('type', 'unknown'),
                analysis_data=json.dumps(result),
                is_manufacturer=result.get('is_manufacturer', False),
                is_category=result.get('is_category', False)
            )
            
            # Publish results
            self._publish_analysis_result(url, result)
        except Exception as e:
            logger.error(f"Error updating analysis result for {url}: {e}")
    
    def _handle_command(self, message: Dict[str, Any]) -> None:
        """
        Handle a command message from Redis.
        
        Args:
            message: Command message dictionary
        """
        try:
            command = message.get('command')
            params = message.get('params', {})
            
            if command == 'analyze':
                self._handle_analyze_command(params)
            elif command == 'status':
                self._handle_status_command()
            elif command == 'clear_queue':
                self._handle_clear_queue_command()
            else:
                logger.warning(f"Unknown command: {command}")
        except Exception as e:
            logger.error(f"Error handling command: {e}")
    
    def _handle_analyze_command(self, params: Dict[str, Any]) -> None:
        """
        Handle an analyze command.
        
        Args:
            params: Command parameters
        """
        # Add item to queue
        self.queue.append(params)
        
        # Update status
        queue_size = len(self.queue)
        self._publish_status('queued', f"Item added to queue. Queue size: {queue_size}")
        
        logger.info(f"Added item to analyzer queue: {params.get('url')}. Queue size: {queue_size}")
    
    def _handle_status_command(self) -> None:
        """Handle a status command."""
        status = 'idle'
        message = "Analyzer is idle"
        
        if self.processing:
            status = 'analyzing'
            message = "Analyzer is processing items"
        elif self.queue:
            status = 'queued'
            message = f"Analyzer has {len(self.queue)} items in queue"
        
        self._publish_status(status, message)
    
    def _handle_clear_queue_command(self) -> None:
        """Handle a clear queue command."""
        queue_size = len(self.queue)
        self.queue = []
        self._publish_status('idle', f"Queue cleared. {queue_size} items removed")
        logger.info(f"Analyzer queue cleared. {queue_size} items removed")
    
    def _publish_status(self, status: str, message: str) -> None:
        """
        Publish a status update to the status channel.
        
        Args:
            status: Status string (idle, analyzing, error, etc.)
            message: Status message
        """
        status_data = {
            'component': 'analyzer',
            'status': status,
            'message': message,
            'timestamp': time.time(),
            'queue_size': len(self.queue)
        }
        
        self.redis_manager.publish(self.status_channel, status_data)
    
    def _publish_stats(self) -> None:
        """Publish analyzer statistics to the stats channel."""
        stats = {
            'queue_size': len(self.queue),
            'processing': self.processing,
            'batch_size': self.batch_size
        }
        
        # Add analyzer stats if available
        analyzer_stats = getattr(self.analyzer, 'stats', {})
        if analyzer_stats:
            stats.update(analyzer_stats)
        
        stats_data = {
            'component': 'analyzer',
            'stats': stats,
            'timestamp': time.time()
        }
        
        self.redis_manager.publish(self.stats_channel, stats_data)
    
    def _publish_analysis_result(self, url: str, result: Dict[str, Any]) -> None:
        """
        Publish analysis results to the database channel.
        
        Args:
            url: Analyzed URL
            result: Analysis result dictionary
        """
        database_channel = self.redis_manager.get_channel('database')
        
        message = {
            'command': 'update_analysis',
            'params': {
                'url': url,
                'analysis_type': result.get('type', 'unknown'),
                'analysis_data': result,
                'is_manufacturer': result.get('is_manufacturer', False),
                'is_category': result.get('is_category', False),
                'timestamp': time.time()
            }
        }
        
        self.redis_manager.publish(database_channel, message)
