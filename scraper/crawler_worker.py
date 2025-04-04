#!/usr/bin/env python3
"""
Crawler Worker for NDAIVI

This module provides a worker implementation for the web crawler
that integrates with the Redis message system. It subscribes to
crawler command channels and publishes status updates.
"""

import json
import logging
import threading
import time
from typing import Dict, List, Any, Optional, Union

from scraper.web_crawler import WebCrawler
from scraper.db_manager import DBManager
from utils.redis_manager import get_redis_manager

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set to DEBUG for more detailed logging

# Add a file handler to log crawler output to a separate file
file_handler = logging.FileHandler('/var/ndaivi/logs/crawler_debug.log', mode='w')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class CrawlerWorker:
    """
    Worker implementation for the web crawler that integrates with Redis.
    
    This class wraps the WebCrawler and handles communication with other
    components through the Redis message system.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the crawler worker with configuration.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.crawler_config = config.get('web_crawler', {})
        self.redis_manager = get_redis_manager(config)
        
        # Initialize the web crawler
        self.crawler = WebCrawler(self.crawler_config)
        
        # Initialize the database manager
        db_path = self.crawler_config.get('db_path', 'scraper/data/crawler.db')
        self.db_manager = DBManager(db_path)
        
        # Worker state
        self.running = False
        self.worker_thread = None
        self.current_job = None
        self.paused = False
        self.active = False  # Whether the crawler is actively crawling or waiting for requests
        
        # Backlog management
        self.backlog_min_threshold = self.crawler_config.get('backlog_min_threshold', 128)
        self.backlog_target_size = self.crawler_config.get('backlog_target_size', 1024)
        self.backlog_check_interval = self.crawler_config.get('backlog_check_interval', 30)
        self.last_backlog_check = 0
        
        # Get channel names from config
        channels_config = config.get('application', {}).get('channels', {})
        self.command_channel = channels_config.get('crawler_commands', 'ndaivi:crawler:commands')
        self.status_channel = channels_config.get('crawler_status', 'ndaivi:crawler:status')
        self.stats_channel = channels_config.get('stats', 'ndaivi:stats')
        self.analyzer_results_channel = channels_config.get('analyzer_results', 'ndaivi:analyzer:results')
        self.backlog_status_channel = channels_config.get('backlog_status', 'ndaivi:backlog:status')
        self.backlog_request_channel = channels_config.get('backlog_request', 'ndaivi:backlog:request')
    
    def start(self) -> bool:
        """
        Start the crawler worker and subscribe to command channel.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        if self.running:
            logger.warning("Crawler worker already running")
            return True
        
        try:
            # Log channel names for debugging
            logger.info(f"Command channel: {self.command_channel}")
            logger.info(f"Status channel: {self.status_channel}")
            logger.info(f"Stats channel: {self.stats_channel}")
            logger.info(f"Analyzer results channel: {self.analyzer_results_channel}")
            logger.info(f"Backlog status channel: {self.backlog_status_channel}")
            logger.info(f"Backlog request channel: {self.backlog_request_channel}")
            
            # Subscribe to command channel
            success = self.redis_manager.subscribe(self.command_channel, self._handle_command)
            if not success:
                logger.error(f"Failed to subscribe to command channel: {self.command_channel}")
                return False
            
            # Subscribe to analyzer results channel
            success = self.redis_manager.subscribe(self.analyzer_results_channel, self._handle_analyzer_result)
            if not success:
                logger.error(f"Failed to subscribe to analyzer results channel: {self.analyzer_results_channel}")
                return False
            
            # Subscribe to backlog request channel
            success = self.redis_manager.subscribe(self.backlog_request_channel, self._handle_backlog_request)
            if not success:
                logger.error(f"Failed to subscribe to backlog request channel: {self.backlog_request_channel}")
                return False
                
            # Also subscribe to the direct channel as a fallback
            direct_channel = 'ndaivi:crawler:commands'
            if self.command_channel != direct_channel:
                logger.info(f"Also subscribing to direct channel: {direct_channel}")
                self.redis_manager.subscribe(direct_channel, self._handle_command)
            
            # Start worker thread
            self.running = True
            self.worker_thread = threading.Thread(target=self._worker_loop)
            self.worker_thread.daemon = True
            self.worker_thread.start()
            
            # Check if we should resume from database
            if self.config.get('application', {}).get('components', {}).get('crawler', {}).get('resume_from_db', True):
                self._resume_from_database()
            
            # Publish status update
            self._publish_status('running', 'Crawler worker started and ready')
            
            logger.info("Crawler worker started successfully")
            return True
        except Exception as e:
            logger.error(f"Error starting crawler worker: {e}")
            # Log the full traceback for better debugging
            import traceback
            logger.error(traceback.format_exc())
            self.running = False
            return False
    
    def stop(self) -> None:
        """Stop the crawler worker."""
        if not self.running:
            return
        
        logger.info("Stopping crawler worker...")
        
        # Set flags to stop all processing
        self.running = False
        self.active = False
        self.paused = False
        
        # Wait for worker thread to terminate
        if self.worker_thread and self.worker_thread.is_alive():
            logger.info("Waiting for worker thread to terminate...")
            try:
                self.worker_thread.join(timeout=2.0)
                if self.worker_thread.is_alive():
                    logger.warning("Worker thread did not terminate within timeout, proceeding anyway")
            except Exception as e:
                logger.error(f"Error joining worker thread: {e}")
        
        # Unsubscribe from channels
        try:
            self.redis_manager.unsubscribe(self.command_channel)
            self.redis_manager.unsubscribe(self.analyzer_results_channel)
            self.redis_manager.unsubscribe(self.backlog_request_channel)
            
            # Also unsubscribe from the direct channel if it was different
            direct_channel = 'ndaivi:crawler:commands'
            if self.command_channel != direct_channel:
                self.redis_manager.unsubscribe(direct_channel)
        except Exception as e:
            logger.error(f"Error unsubscribing from channels: {e}")
        
        # Publish status update
        try:
            self._publish_status('stopped', 'Crawler worker stopped')
        except Exception as e:
            logger.error(f"Error publishing stop status: {e}")
        
        logger.info("Crawler worker stopped")
    
    def _resume_from_database(self) -> None:
        """
        Resume crawling from the database state.
        This loads any pending URLs from the database into the crawler queue.
        """
        try:
            # Check if there are pending URLs in the database
            if self.crawler.url_queue_manager:
                pending_urls = self.crawler.url_queue_manager.get_all_queued()
                
                if pending_urls:
                    logger.info(f"Resuming from database with {len(pending_urls)} pending URLs")
                    
                    # Create a new job with default parameters
                    self.current_job = {
                        'id': f"resumed_{int(time.time())}",
                        'start_url': self.crawler_config.get('target_website', 'manualslib.com'),
                        'max_urls': self.crawler_config.get('max_urls'),
                        'max_depth': self.crawler_config.get('max_depth', 3),
                        'start_time': time.time()
                    }
                    
                    # Load URLs into the queue
                    for url_data in pending_urls:
                        self.crawler.add_url(
                            url_data.get('url'),
                            url_data.get('depth', 0),
                            url_data.get('priority', 0.0)
                        )
                    
                    logger.info(f"Loaded {self.crawler.url_queue.size()} URLs into queue")
                    
                    # Set crawler to idle but ready state
                    self.active = False
                    self._publish_status('idle', f"Resumed from database with {len(pending_urls)} pending URLs")
                else:
                    logger.info("No pending URLs found in database")
        except Exception as e:
            logger.error(f"Error resuming from database: {e}")
    
    def _worker_loop(self) -> None:
        """
        Main worker loop that processes crawler jobs.
        This runs in a separate thread.
        """
        stats_interval = self.crawler_config.get('stats_update_interval', 10)
        last_stats_time = 0
        
        while self.running:
            try:
                # Check if we need to update backlog status
                current_time = time.time()
                if current_time - self.last_backlog_check >= self.backlog_check_interval:
                    self._publish_backlog_status()
                    self.last_backlog_check = current_time
                
                # If we have a current job, active flag is set, and not paused, process URLs
                if self.current_job and self.active and not self.paused:
                    # Process one URL at a time
                    if not self.crawler.url_queue.is_empty():
                        url_data = self.crawler.url_queue.pop()
                        if url_data:
                            # Extract URL from the tuple returned by pop()
                            url = url_data[0] if isinstance(url_data, tuple) and len(url_data) > 0 else url_data
                            depth = url_data[1] if isinstance(url_data, tuple) and len(url_data) > 1 else 0
                            metadata = url_data[2] if isinstance(url_data, tuple) and len(url_data) > 2 else None
                            
                            self._process_url(url, depth)
                            logger.info(f"Processing URL: {url} at depth {depth}")
                    else:
                        # No URLs in queue, check if we need to load from database
                        if self.crawler.url_queue_manager and self.crawler.url_queue.size() == 0:
                            # Try to load more URLs from database
                            logger.info("Queue empty, trying to load more URLs from database")
                            next_url = self.crawler.url_queue_manager.get_next()
                            if next_url:
                                url = next_url.get('url')
                                depth = next_url.get('depth', 0)
                                priority = next_url.get('priority', 0.0)
                                self.crawler.add_url(url, depth, priority)
                                logger.info(f"Loaded URL from database: {url}")
                            else:
                                # No more URLs to process, set to idle
                                logger.info("No more URLs to process, setting crawler to idle")
                                self.active = False
                                self._publish_status('idle', "Crawler idle - waiting for backlog requests")
                
                # Publish stats periodically
                if current_time - last_stats_time >= stats_interval:
                    self._publish_stats()
                    last_stats_time = current_time
                
                # Small sleep to prevent CPU hogging
                time.sleep(0.01)
            except Exception as e:
                logger.error(f"Error in crawler worker loop: {e}")
                time.sleep(0.1)  # Small delay on errors
    
    def _handle_command(self, message: Dict[str, Any]) -> None:
        """
        Handle a command message from Redis.
        
        Args:
            message: Command message dictionary
        """
        try:
            # Log the received message for debugging
            logger.info(f"Crawler worker received command: {message}")
            
            command = message.get('command')
            params = message.get('params', {})
            
            if command == 'crawl':
                self._handle_crawl_command(params)
            elif command == 'pause':
                self._handle_pause_command()
            elif command == 'resume':
                self._handle_resume_command()
            elif command == 'stop':
                self._handle_stop_command()
            elif command == 'status':
                self._handle_status_command()
            elif command == 'start':
                self._handle_start_command()
            else:
                logger.warning(f"Unknown command: {command}")
        except Exception as e:
            logger.error(f"Error handling command: {e}")
            # Log the full traceback for better debugging
            import traceback
            logger.error(traceback.format_exc())
    
    def _handle_analyzer_result(self, message: Dict[str, Any]) -> None:
        """
        Handle a message from the analyzer indicating a URL has been analyzed.
        
        Args:
            message: Analyzer result message
        """
        try:
            url = message.get('url')
            if not url:
                logger.warning("Received analyzer result without URL")
                return
                
            logger.info(f"URL analyzed by analyzer: {url}")
            
            # Mark URL as analyzed in the database
            if self.db_manager:
                self.db_manager.mark_url_as_analyzed(url)
                logger.info(f"Marked URL as analyzed in database: {url}")
        except Exception as e:
            logger.error(f"Error handling analyzer result: {e}")
    
    def _handle_backlog_request(self, message: Dict[str, Any]) -> None:
        """
        Handle a request to fill the backlog with more URLs.
        
        Args:
            message: Backlog request message
        """
        try:
            # Log the request
            count = message.get('count', self.backlog_target_size)
            logger.info(f"Received backlog request for {count} URLs")
            
            # If we're not active, start crawling
            if not self.active:
                logger.info("Activating crawler to fulfill backlog request")
                self.active = True
                self._publish_status('active', "Crawler activated to fulfill backlog request")
                
                # If we don't have a current job, create one with default parameters
                if not self.current_job:
                    logger.info("Creating new job to fulfill backlog request")
                    target_website = self.crawler_config.get('target_website')
                    if target_website:
                        # Add the start URL to the crawler
                        success = self.crawler.add_url(target_website, depth=0, priority=1.0)
                        if success:
                            self._publish_status('active', f"Added start URL: {target_website}")
                            
                            # Process multiple URLs to ensure we discover more links - increased from 5 to 10
                            urls_processed = 0
                            for _ in range(min(30, count)):  # Increased from 10 to 30 to discover more URLs
                                result = self.crawler.process_next_url()
                                if result:
                                    urls_processed += 1
                                    logger.info(f"Processed URL #{urls_processed} for backlog request")
                                else:
                                    # If we ran out of URLs, add the start URL again
                                    if urls_processed == 0:
                                        self.crawler.add_url(target_website, depth=0, priority=1.0)
                                        # Try one more time
                                        result = self.crawler.process_next_url()
                                        if result:
                                            urls_processed += 1
                                    break
                            
                            if urls_processed > 0:
                                self._publish_status('active', f"Processed {urls_processed} URLs to discover more links")
                            else:
                                self._publish_status('warning', "No URLs processed for backlog request")
                    else:
                        logger.error("No target website configured, cannot fulfill backlog request")
                        self._publish_status('error', "No target website configured, cannot fulfill backlog request")
                        return
            
            # Get URLs from the crawler
            urls = []
            
            # First check if we have any URLs in the queue
            queue_size = self.crawler.url_queue.size()
            if queue_size > 0:
                logger.info(f"Found {queue_size} URLs in the crawler queue")
                # Make a copy of the URLs instead of removing them from the crawler's queue
                copied_urls = []
                for _ in range(min(count, queue_size)):
                    url_data = self.crawler.url_queue.pop()
                    if url_data:
                        url = url_data[0] if isinstance(url_data, tuple) and len(url_data) > 0 else url_data
                        depth = url_data[1] if isinstance(url_data, tuple) and len(url_data) > 1 else 0
                        metadata = url_data[2] if isinstance(url_data, tuple) and len(url_data) > 2 else None
                        
                        # Add to our list of URLs to return
                        urls.append({
                            'url': url,
                            'depth': depth,
                            'metadata': metadata
                        })
                        
                        # Add back to the crawler's queue with same priority to ensure it's processed
                        self.crawler.add_url(url, depth, priority=1.0, metadata=metadata)
            
            # If we didn't get enough URLs, try to process more
            if len(urls) < count and self.active:
                # Process more URLs to get more links
                urls_to_process = min(50, count - len(urls))  # Increased from 20 to 50
                logger.info(f"Processing {urls_to_process} more URLs to discover links")
                
                urls_processed = 0
                for _ in range(urls_to_process):
                    result = self.crawler.process_next_url()
                    if result:
                        urls_processed += 1
                        # Check if new URLs were discovered (queue size increased)
                        new_queue_size = self.crawler.url_queue.size()
                        if new_queue_size > 0:
                            # Get newly discovered URLs but don't remove them from the crawler's queue
                            # Instead, make copies for the backlog
                            new_urls_to_add = min(count - len(urls), new_queue_size)
                            if new_urls_to_add > 0:
                                logger.info(f"Found {new_urls_to_add} new URLs to add to backlog")
                                # Make a copy instead of removing URLs from the original queue
                                copied_urls = []
                                for _ in range(new_urls_to_add):
                                    url_data = self.crawler.url_queue.pop()
                                    if url_data:
                                        url = url_data[0] if isinstance(url_data, tuple) and len(url_data) > 0 else url_data
                                        depth = url_data[1] if isinstance(url_data, tuple) and len(url_data) > 1 else 0
                                        metadata = url_data[2] if isinstance(url_data, tuple) and len(url_data) > 2 else None
                                        copied_urls.append((url, depth, metadata))
                                        urls.append({
                                            'url': url,
                                            'depth': depth,
                                            'metadata': metadata
                                        })
                                
                                # Add the URLs back to the crawler's queue
                                for url, depth, metadata in copied_urls:
                                    self.crawler.add_url(url, depth, priority=1.0, metadata=metadata)
                    else:
                        logger.warning("No more URLs to process")
                        break
                
                logger.info(f"Processed {urls_processed} URLs to discover more links")
            
            # Prepare and publish response
            response = {
                'status': 'backlog_response',
                'urls': urls,
                'count': len(urls),
                'timestamp': time.time()
            }
            
            logger.info(f"Sending backlog response with {len(urls)} URLs")
            
            # Publish response
            success = self.redis_manager.publish(self.backlog_status_channel, response)
            
            if success:
                self._publish_status('backlog_response', f"Sent {len(urls)} URLs for backlog")
            else:
                logger.error("Failed to publish backlog response")
                
        except Exception as e:
            logger.error(f"Error handling backlog request: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _handle_crawl_command(self, params: Dict[str, Any]) -> None:
        """
        Handle a crawl command.
        
        Args:
            params: Command parameters
        """
        try:
            # Extract parameters
            start_url = params.get('start_url', self.crawler_config.get('target_website'))
            max_urls = params.get('max_urls', self.crawler_config.get('max_urls'))
            max_depth = params.get('max_depth', self.crawler_config.get('max_depth', 3))
            
            if not start_url:
                logger.error("No start URL provided for crawl command")
                self._publish_status('error', "No start URL provided")
                return
            
            # Create a new job
            self.current_job = {
                'id': f"crawl_{int(time.time())}",
                'start_url': start_url,
                'max_urls': max_urls,
                'max_depth': max_depth,
                'start_time': time.time()
            }
            
            # Add the start URL to the crawler
            success = self.crawler.add_url(start_url, depth=0, priority=1.0)
            if success:
                # Set crawler to active state
                self.active = True
                self.paused = False
                self._publish_status('crawling', f"Started crawling from {start_url}")
                logger.info(f"Started new crawl job from {start_url}")
            else:
                logger.error(f"Failed to add start URL: {start_url}")
                self._publish_status('error', f"Failed to add start URL: {start_url}")
                self.current_job = None
        except Exception as e:
            logger.error(f"Error handling crawl command: {e}")
            self._publish_status('error', f"Error starting crawl: {str(e)}")
            self.current_job = None
    
    def _handle_pause_command(self) -> None:
        """Handle a pause command."""
        self.paused = True
        self._publish_status('paused', "Crawler paused")
        logger.info("Crawler paused")
    
    def _handle_resume_command(self) -> None:
        """Handle a resume command."""
        self.paused = False
        status = 'crawling' if self.active else 'idle'
        message = "Crawler resumed" if self.active else "Crawler resumed (waiting for backlog requests)"
        self._publish_status(status, message)
        logger.info(f"Crawler resumed (active: {self.active})")
    
    def _handle_stop_command(self) -> None:
        """Handle a stop command."""
        # Clear current job and queue
        self.current_job = None
        self.active = False
        
        # Don't reset the crawler to preserve the database state
        # self.crawler.reset()
        
        self._publish_status('idle', "Crawler stopped")
        logger.info("Crawler stopped")
    
    def _handle_start_command(self) -> None:
        """Handle the start command."""
        try:
            # Create a default job if none exists
            if not self.current_job:
                target_website = self.crawler_config.get('target_website')
                if not target_website:
                    logger.error("No target website configured")
                    self._publish_status('error', "No target website configured")
                    return
                    
                self.current_job = {
                    'id': f"start_{int(time.time())}",
                    'start_url': target_website,
                    'max_urls': self.crawler_config.get('max_urls'),
                    'max_depth': self.crawler_config.get('max_depth', 3),
                    'start_time': time.time()
                }
                
                # Add the start URL to the crawler
                success = self.crawler.add_url(target_website, depth=0, priority=1.0)
                if success:
                    logger.info(f"Added start URL: {target_website}")
                else:
                    logger.error(f"Failed to add start URL: {target_website}")
                    self._publish_status('error', f"Failed to add start URL: {target_website}")
                    return
            
            # Set crawler to active state
            self.active = True
            self.paused = False
            self._publish_status('crawling', f"Started crawling from {self.current_job['start_url']}")
            
            # Process the start URL immediately
            if not self.crawler.url_queue.is_empty():
                url_data = self.crawler.url_queue.pop()
                if url_data:
                    url = url_data[0] if isinstance(url_data, tuple) and len(url_data) > 0 else url_data
                    depth = url_data[1] if isinstance(url_data, tuple) and len(url_data) > 1 else 0
                    self._process_url(url, depth)
                    
            logger.info("Crawler started successfully")
        except Exception as e:
            logger.error(f"Error handling start command: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _handle_status_command(self) -> None:
        """Handle a status command."""
        status = 'idle'
        message = "Crawler is idle"
        
        if self.current_job:
            if self.paused:
                status = 'paused'
                message = f"Crawler is paused (job: {self.current_job.get('id')})"
            elif self.active:
                status = 'crawling'
                message = f"Crawler is active (job: {self.current_job.get('id')})"
            else:
                status = 'idle'
                message = f"Crawler is idle (job: {self.current_job.get('id')}, waiting for backlog requests)"
        
        self._publish_status(status, message)
    
    def _publish_status(self, status: str, message: str, data: Dict[str, Any] = {}) -> None:
        """
        Publish a status update to the status channel.
        
        Args:
            status: Status string (idle, crawling, paused, error, etc.)
            message: Status message
            data: Additional data to include in the status update
        """
        status_data = {
            'component': 'crawler',
            'status': status,
            'message': message,
            'timestamp': time.time()
        }
        
        if self.current_job:
            status_data['job_id'] = self.current_job.get('id')
        
        status_data.update(data)
        
        self.redis_manager.publish(self.status_channel, status_data)
    
    def _publish_stats(self) -> None:
        """Publish crawler statistics to the stats channel."""
        stats = self.crawler.get_stats()
        
        # Add backlog information
        backlog_stats = {
            'backlog_size': self.crawler.url_queue.size(),
            'backlog_min_threshold': self.backlog_min_threshold,
            'backlog_target_size': self.backlog_target_size
        }
        stats.update(backlog_stats)
        
        stats_data = {
            'component': 'crawler',
            'stats': stats,
            'timestamp': time.time()
        }
        
        if self.current_job:
            stats_data['job_id'] = self.current_job.get('id')
            stats_data['job_runtime'] = time.time() - self.current_job.get('start_time', 0)
        
        self.redis_manager.publish(self.stats_channel, stats_data)
    
    def _publish_backlog_status(self) -> None:
        """Publish backlog status to the backlog status channel."""
        backlog_size = self.crawler.url_queue.size()
        
        # Get additional backlog stats from database if available
        db_stats = {}
        if self.db_manager:
            try:
                db_stats = self.db_manager.get_stats()
            except Exception as e:
                logger.error(f"Error getting database stats: {e}")
        
        status_data = {
            'component': 'crawler',
            'backlog_size': backlog_size,
            'backlog_min_threshold': self.backlog_min_threshold,
            'backlog_target_size': self.backlog_target_size,
            'db_stats': db_stats,
            'timestamp': time.time()
        }
        
        self.redis_manager.publish(self.backlog_status_channel, status_data)
        
        # If backlog is below threshold, publish a notification
        if backlog_size < self.backlog_min_threshold and self.current_job:
            logger.info(f"Backlog size ({backlog_size}) is below threshold ({self.backlog_min_threshold})")
            
            # Only activate crawler if it's not already active
            if not self.active and not self.paused:
                self.active = True
                self._publish_status('crawling', f"Filling backlog (current size: {backlog_size})")
                logger.info("Activated crawler to fill backlog")
    
    def _publish_url_processed(self, url: str, result: Dict[str, Any]) -> None:
        """
        Publish a URL processed message for the analyzer.
        
        Args:
            url: Processed URL
            result: Processing result dictionary
        """
        analyzer_channel = self.redis_manager.get_channel('analyzer_commands')
        
        message = {
            'command': 'analyze',
            'params': {
                'url': url,
                'domain': result.get('domain', ''),
                'title': result.get('title', ''),
                'content': result.get('content', ''),
                'links': result.get('links', []),
                'depth': result.get('depth', 0),
                'job_id': self.current_job.get('id') if self.current_job else None
            }
        }
        
        self.redis_manager.publish(analyzer_channel, message)
    
    def _process_url(self, url: str, depth: int = 0) -> Dict[str, Any]:
        """
        Process a single URL from the crawler queue.
        
        Args:
            url: URL to process
            depth: Depth of the URL in the crawl tree
            
        Returns:
            Dict: Processing result
        """
        try:
            if not url:
                logger.warning("Empty URL provided")
                return {'error': 'Empty URL provided'}
                
            # Update status
            self._publish_status('crawling', f"Processing URL: {url}")
            logger.info(f"Processing URL: {url} at depth {depth}")
            
            # Process the URL
            result = self.crawler.process_url(url, depth)
            
            if result:
                # Update status
                self._publish_status('active', f"Processed URL: {url}")
                logger.info(f"Successfully processed URL: {url}")
                
                # Publish URL processed message
                self._publish_url_processed(url, result)
                
                return result
            else:
                logger.warning(f"Failed to process URL: {url}")
                self._publish_status('warning', f"Failed to process URL: {url}")
                return {'error': 'Failed to process URL', 'url': url}
                
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            self._publish_status('error', f"Error processing URL {url}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {'error': str(e), 'url': url}
