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
        
        # Get channel names from config
        self.command_channel = self.redis_manager.get_channel('crawler_commands')
        self.status_channel = self.redis_manager.get_channel('crawler_status')
        self.stats_channel = self.redis_manager.get_channel('stats')
    
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
            # Subscribe to command channel
            self.redis_manager.subscribe(self.command_channel, self._handle_command)
            
            # Start worker thread
            self.running = True
            self.worker_thread = threading.Thread(target=self._worker_loop)
            self.worker_thread.daemon = True
            self.worker_thread.start()
            
            # Publish status update
            self._publish_status('idle', 'Crawler worker started')
            
            logger.info("Crawler worker started successfully")
            return True
        except Exception as e:
            logger.error(f"Error starting crawler worker: {e}")
            self.running = False
            return False
    
    def stop(self) -> None:
        """Stop the crawler worker."""
        if not self.running:
            return
        
        self.running = False
        
        # Wait for worker thread to terminate
        if self.worker_thread:
            self.worker_thread.join(timeout=1.0)
        
        # Unsubscribe from command channel
        self.redis_manager.unsubscribe(self.command_channel)
        
        # Publish status update
        self._publish_status('stopped', 'Crawler worker stopped')
        
        logger.info("Crawler worker stopped")
    
    def _worker_loop(self) -> None:
        """
        Main worker loop that processes crawler jobs.
        This runs in a separate thread.
        """
        stats_interval = self.crawler_config.get('stats_update_interval', 10)
        last_stats_time = 0
        
        while self.running:
            try:
                # If we have a current job and not paused, process URLs
                if self.current_job and not self.paused:
                    # Process one URL at a time
                    if not self.crawler.url_queue.is_empty():
                        url = self.crawler.url_queue.pop()
                        if url:
                            self._process_url(url)
                    else:
                        # Job complete
                        self._publish_status('idle', f"Crawl job complete: {self.current_job.get('id')}")
                        self.current_job = None
                
                # Publish stats periodically
                current_time = time.time()
                if current_time - last_stats_time >= stats_interval:
                    self._publish_stats()
                    last_stats_time = current_time
                
                # Small sleep to prevent CPU hogging
                time.sleep(0.01)
            except Exception as e:
                logger.error(f"Error in crawler worker loop: {e}")
                time.sleep(0.1)  # Small delay on errors
    
    def _process_url(self, url: str) -> None:
        """
        Process a single URL from the crawler queue.
        
        Args:
            url: URL to process
        """
        try:
            # Update status
            self._publish_status('crawling', f"Processing URL: {url}")
            
            # Process the URL
            result = self.crawler.process_url(url)
            
            if result:
                # Store result in database
                self.db_manager.add_url(
                    url=url,
                    domain=result.get('domain', ''),
                    status_code=result.get('status_code', 0),
                    content_type=result.get('content_type', ''),
                    title=result.get('title', ''),
                    links=result.get('links', []),
                    depth=result.get('depth', 0)
                )
                
                # Publish URL processed message to analyzer
                self._publish_url_processed(url, result)
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            self._publish_status('error', f"Error processing URL {url}: {str(e)}")
    
    def _handle_command(self, message: Dict[str, Any]) -> None:
        """
        Handle a command message from Redis.
        
        Args:
            message: Command message dictionary
        """
        try:
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
            else:
                logger.warning(f"Unknown command: {command}")
        except Exception as e:
            logger.error(f"Error handling command: {e}")
    
    def _handle_crawl_command(self, params: Dict[str, Any]) -> None:
        """
        Handle a crawl command.
        
        Args:
            params: Command parameters
        """
        start_url = params.get('start_url')
        job_id = params.get('job_id', str(int(time.time())))
        max_urls = params.get('max_urls', self.crawler_config.get('max_urls_per_domain', 100))
        max_depth = params.get('max_depth', self.crawler_config.get('max_depth', 3))
        
        if not start_url:
            self._publish_status('error', "No start URL provided for crawl command")
            return
        
        # Reset crawler state
        self.crawler.reset()
        
        # Set current job
        self.current_job = {
            'id': job_id,
            'start_url': start_url,
            'max_urls': max_urls,
            'max_depth': max_depth,
            'start_time': time.time()
        }
        
        # Add start URL to queue
        self.crawler.add_url(start_url)
        
        # Update status
        self._publish_status('crawling', f"Starting crawl job {job_id} from {start_url}")
        
        logger.info(f"Started crawl job {job_id} from {start_url}")
    
    def _handle_pause_command(self) -> None:
        """Handle a pause command."""
        self.paused = True
        self._publish_status('paused', "Crawler paused")
        logger.info("Crawler paused")
    
    def _handle_resume_command(self) -> None:
        """Handle a resume command."""
        self.paused = False
        self._publish_status('crawling', "Crawler resumed")
        logger.info("Crawler resumed")
    
    def _handle_stop_command(self) -> None:
        """Handle a stop command."""
        # Clear current job and queue
        self.current_job = None
        self.crawler.reset()
        self._publish_status('idle', "Crawler stopped")
        logger.info("Crawler stopped")
    
    def _handle_status_command(self) -> None:
        """Handle a status command."""
        status = 'idle'
        message = "Crawler is idle"
        
        if self.current_job:
            if self.paused:
                status = 'paused'
                message = f"Crawler is paused (job: {self.current_job.get('id')})"
            else:
                status = 'crawling'
                message = f"Crawler is active (job: {self.current_job.get('id')})"
        
        self._publish_status(status, message)
    
    def _publish_status(self, status: str, message: str) -> None:
        """
        Publish a status update to the status channel.
        
        Args:
            status: Status string (idle, crawling, paused, error, etc.)
            message: Status message
        """
        status_data = {
            'component': 'crawler',
            'status': status,
            'message': message,
            'timestamp': time.time()
        }
        
        if self.current_job:
            status_data['job_id'] = self.current_job.get('id')
        
        self.redis_manager.publish(self.status_channel, status_data)
    
    def _publish_stats(self) -> None:
        """Publish crawler statistics to the stats channel."""
        stats = self.crawler.get_stats()
        
        stats_data = {
            'component': 'crawler',
            'stats': stats,
            'timestamp': time.time()
        }
        
        if self.current_job:
            stats_data['job_id'] = self.current_job.get('id')
            stats_data['job_runtime'] = time.time() - self.current_job.get('start_time', 0)
        
        self.redis_manager.publish(self.stats_channel, stats_data)
    
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
