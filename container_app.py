#!/usr/bin/env python3
"""
NDAIVI Container Application

This module provides a container application that coordinates the crawler and analyzer
components of the NDAIVI system. It manages the backlog of URLs, requests URLs from
the crawler, and forwards them to the analyzer.

Workflow:
1. The system gets ALL of its config from config.yaml
2. Once started, the crawler runs, resuming from where it left, according to the database
3. The container app requests an URL using Redis and forwards it to the analyzer
4. When an URL is processed, the analyzer messages the crawler that a website has been analyzed
5. The container app reads the amount of links on the backlog and if the amount is less than 128,
   it requests crawler more links until the amount reaches 1024 again
6. This system can run in the background and stats may be checked at any moment
"""

import os
import sys
import time
import json
import signal
import logging
import threading
from typing import Dict, Any, List, Optional, Callable

from utils.redis_manager import get_redis_manager
from utils.config_manager import get_config

logger = logging.getLogger(__name__)

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
    print(f"\nContainer app {status}.")

# Register global signal handlers
signal.signal(signal.SIGINT, handle_interrupt)  # Ctrl+C
signal.signal(signal.SIGTERM, handle_interrupt)  # kill command
signal.signal(signal.SIGUSR1, handle_suspend)  # SIGUSR1 for suspension


class ContainerApp:
    """
    Container application that coordinates the crawler and analyzer components.
    
    This class manages the backlog of URLs, requests URLs from the crawler,
    and forwards them to the analyzer. It also monitors the backlog size and
    requests more URLs from the crawler when needed.
    """
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize the container application.
        
        Args:
            config_path: Path to the configuration file
        """
        # Load configuration
        self.config_path = config_path
        self.config = get_config(config_path)
        
        # Initialize Redis manager
        self.redis_manager = get_redis_manager(self.config)
        
        # Get channel names from config
        channels_config = self.config.get('application', {}).get('channels', {})
        self.crawler_commands_channel = channels_config.get('crawler_commands', 'ndaivi:crawler:commands')
        self.crawler_status_channel = channels_config.get('crawler_status', 'ndaivi:crawler:status')
        self.analyzer_commands_channel = channels_config.get('analyzer_commands', 'ndaivi:analyzer:commands')
        self.analyzer_status_channel = channels_config.get('analyzer_status', 'ndaivi:analyzer:status')
        self.analyzer_results_channel = channels_config.get('analyzer_results', 'ndaivi:analyzer:results')
        self.container_status_channel = channels_config.get('container_status', 'ndaivi:container:status')
        self.backlog_status_channel = channels_config.get('backlog_status', 'ndaivi:backlog:status')
        self.backlog_request_channel = channels_config.get('backlog_request', 'ndaivi:backlog:request')
        self.stats_channel = channels_config.get('stats', 'ndaivi:stats')
        self.system_channel = channels_config.get('system', 'ndaivi:system')
        
        # Initialize component status
        self.crawler_status = {
            'status': 'unknown',
            'message': 'Not started',
            'timestamp': time.time(),
            'component': 'crawler'
        }
        
        self.analyzer_status = {
            'status': 'unknown',
            'message': 'Not started',
            'timestamp': time.time(),
            'component': 'analyzer'
        }
        
        # Backlog management
        self.backlog = []  # List of URLs to be processed
        self.processing = []  # URLs currently being processed
        self.backlog_min_threshold = self.config.get('web_crawler', {}).get('backlog_min_threshold', 128)
        self.backlog_target_size = self.config.get('web_crawler', {}).get('backlog_target_size', 1024)
        self.backlog_check_interval = self.config.get('web_crawler', {}).get('backlog_check_interval', 30)
        self.last_backlog_check = 0
        
        # Statistics
        self.stats = {
            'start_time': None,
            'processed_count': 0,
            'processing_count': 0,
            'backlog_size': 0,
            'backlog_requests': 0,
            'crawler_status': 'unknown',
            'analyzer_status': 'unknown',
            'last_crawler_update': 0,
            'queued_urls': [],
            'processing_urls': []
        }
        
        # Application state
        self.running = False
        self.paused = False
        self.worker_thread = None
        self.lock = threading.Lock()
        
        # Processing configuration
        self.max_concurrent_processing = self.config.get('application', {}).get('components', {}).get('container_app', {}).get('max_concurrent_processing', 10)
        
        # Initialize monitoring attributes
        self.monitor_thread = None
        self.monitor_running = False
        
        logger.info(f"Container app initialized with backlog thresholds: min={self.backlog_min_threshold}, target={self.backlog_target_size}")
    
    def start(self) -> bool:
        """
        Start the container application and its components.
        
        Returns:
            bool: True if started successfully
        """
        if self.running:
            logger.info("Container application already running")
            return True
            
        logger.info("Starting container application...")
        
        try:
            # Initialize Redis manager
            self.redis_manager = get_redis_manager(self.config)
            if not self.redis_manager:
                logger.error("Failed to initialize Redis manager")
                return False
                
            # Subscribe to status channels
            self.redis_manager.subscribe(self.crawler_status_channel, self._handle_crawler_status)
            self.redis_manager.subscribe(self.analyzer_status_channel, self._handle_analyzer_status)
            self.redis_manager.subscribe(self.backlog_status_channel, self._handle_backlog_status)
            self.redis_manager.subscribe(self.analyzer_results_channel, self._handle_analyzer_result)
            
            # Initialize crawler and analyzer status
            self.crawler_status = {
                'status': 'running',
                'message': 'Crawler component started',
                'timestamp': time.time(),
                'component': 'crawler'
            }
            
            self.analyzer_status = {
                'status': 'running',
                'message': 'Analyzer component started',
                'timestamp': time.time(),
                'component': 'analyzer'
            }
            
            # Set running flag
            self.running = True
            
            # Start worker thread
            self.worker_thread = threading.Thread(target=self._worker_loop)
            self.worker_thread.daemon = True
            self.worker_thread.start()
            
            # Start components
            self._start_crawler()
            self._start_analyzer()
            
            logger.info("Container application started successfully")
            return True
        except Exception as e:
            logger.error(f"Error starting container application: {e}")
            return False
    
    def start_monitor(self) -> None:
        """Start monitoring system status."""
        if not self.monitor_thread or not self.monitor_thread.is_alive():
            self.monitor_running = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()
            logger.info("Monitor started")
            
    def stop_monitor(self) -> None:
        """Stop monitoring system status."""
        self.monitor_running = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=1.0)
            logger.info("Monitor stopped")
            
    def _monitor_loop(self) -> None:
        """Monitor loop that displays system status."""
        while self.monitor_running:
            try:
                status = self.get_status()
                
                # Clear screen and print status
                print("\033[2J\033[H", end="")  # Clear screen and move cursor to top
                print("\nNDAIVI System Monitor")
                print("===================")
                print(f"\nCrawler: {status['crawler']['status']} - {status['crawler']['message']}")
                print(f"Analyzer: {status['analyzer']['status']} - {status['analyzer']['message']}")
                print(f"\nBacklog size: {status.get('backlog_size', 0)}")
                print(f"URLs processed: {status.get('processed_count', 0)}")
                print(f"URLs queued: {status.get('backlog_size', 0)}")
                print("\nPress Ctrl+C to stop monitoring...")
                
                time.sleep(1.0)  # Update every second
                
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
                time.sleep(1.0)  # Wait before retrying
    
    def stop(self) -> bool:
        """
        Stop the container application and all components.
        
        Returns:
            bool: True if stopped successfully
        """
        if not self.running:
            logger.info("Container application already stopped")
            return True
            
        logger.info("Stopping container application...")
        
        try:
            # Set flags to stop all processing
            self.running = False
            self.paused = False
            
            # Stop the monitor thread
            if hasattr(self, 'monitor_thread') and self.monitor_thread and self.monitor_thread.is_alive():
                logger.info("Stopping monitor thread...")
                try:
                    self.monitor_thread.join(timeout=2.0)
                except Exception as e:
                    logger.error(f"Error stopping monitor thread: {e}")
            
            # Send stop commands to components
            stop_command = {'command': 'stop'}
            self.redis_manager.publish(self.crawler_commands_channel, stop_command)
            self.redis_manager.publish(self.analyzer_commands_channel, stop_command)
            
            # Wait for components to stop (max 5 seconds)
            wait_start = time.time()
            while time.time() - wait_start < 5:
                if (self.crawler_status.get('status') == 'stopped' and 
                    self.analyzer_status.get('status') == 'stopped'):
                    break
                time.sleep(0.1)
            
            # Unsubscribe from all channels
            logger.info("Unsubscribing from Redis channels...")
            channels = [
                self.crawler_status_channel,
                self.analyzer_status_channel,
                self.backlog_status_channel,
                self.analyzer_results_channel,
                self.container_status_channel
            ]
            for channel in channels:
                try:
                    self.redis_manager.unsubscribe(channel)
                except Exception as e:
                    logger.error(f"Error unsubscribing from {channel}: {e}")
            
            # Close Redis connection
            logger.info("Closing Redis connection...")
            try:
                self.redis_manager.close()
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")
            
            logger.info("Container application stopped successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping container application: {e}")
            return False
    
    def _worker_loop(self) -> None:
        """
        Main worker loop that manages the backlog and coordinates components.
        This runs in a separate thread.
        """
        stats_interval = self.config.get('application', {}).get('components', {}).get('stats', {}).get('update_interval', 5)
        last_stats_time = 0
        
        while self.running and not SHUTDOWN_FLAG:
            try:
                # Skip processing if suspended
                if SUSPEND_FLAG or self.paused:
                    time.sleep(0.1)
                    continue
                
                current_time = time.time()
                
                # Check backlog size periodically
                if current_time - self.last_backlog_check >= self.backlog_check_interval:
                    self._check_backlog_size()
                    self.last_backlog_check = current_time
                
                # Publish stats periodically
                if current_time - last_stats_time >= stats_interval:
                    self._publish_stats()
                    last_stats_time = current_time
                
                # Process next URL in backlog
                self._process_next_url()
                
                # Small sleep to prevent CPU hogging
                time.sleep(0.01)
            except Exception as e:
                logger.error(f"Error in container app worker loop: {e}")
                time.sleep(0.1)  # Small delay on errors
    
    def _check_backlog_size(self) -> None:
        """
        Check if the backlog size is below the threshold and request more URLs if needed.
        """
        try:
            backlog_size = 0
            with self.lock:
                backlog_size = len(self.backlog)
                self.stats['backlog_size'] = backlog_size
            
            logger.info(f"Backlog size: {backlog_size}")
            
            # Publish status update
            self._publish_status('backlog_check', f"Backlog size: {backlog_size}")
            
            # Check if we need more URLs
            if backlog_size < self.backlog_min_threshold:
                logger.info(f"Backlog below threshold ({backlog_size} < {self.backlog_min_threshold}). Requesting {self.backlog_target_size} more URLs.")
                self._request_more_urls(self.backlog_target_size)
        except Exception as e:
            logger.error(f"Error checking backlog size: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _request_more_urls(self, count: int = None) -> None:
        """
        Request more URLs from the crawler to fill the backlog.
        
        Args:
            count: Number of URLs to request (default: target_size - current_size)
        """
        try:
            with self.lock:
                current_size = len(self.backlog)
                
                # Calculate how many URLs to request
                if count is None:
                    count = self.backlog_target_size - current_size
                
                if count <= 0:
                    logger.debug("No need to request more URLs")
                    return
                
                # Update stats
                self.stats['backlog_requests'] += 1
            
            logger.info(f"Requesting {count} more URLs from crawler")
            
            # Prepare request
            request = {
                'command': 'backlog_request',
                'count': count,
                'timestamp': time.time()
            }
            
            # Publish request
            success = self.redis_manager.publish(self.backlog_request_channel, request)
            
            if success:
                self._publish_status('backlog_request', f"Requested {count} URLs from crawler")
            else:
                logger.error("Failed to publish backlog request")
        except Exception as e:
            logger.error(f"Error requesting more URLs: {e}")
    
    def _handle_crawler_status(self, message: Dict[str, Any]) -> None:
        """
        Handle a status update from the crawler.
        
        Args:
            message: Status message dictionary
        """
        try:
            status = message.get('status')
            msg = message.get('message')
            data = message.get('data', {})
            timestamp = message.get('timestamp', time.time())
            
            # Update crawler status
            self.crawler_status = {
                'status': status,
                'message': msg,
                'timestamp': timestamp,
                'data': data,
                'component': 'crawler'
            }
            
            # Log status update
            logger.info(f"Crawler status: {status} - {msg}")
            if data:
                logger.info(f"Crawler data: {json.dumps(data, indent=2)}")
            
            # Publish to container status channel
            self.redis_manager.publish(self.container_status_channel, {
                'type': 'crawler_status',
                'status': status,
                'message': msg,
                'data': data,
                'timestamp': timestamp
            })
        except Exception as e:
            logger.error(f"Error handling crawler status: {e}")

    def _handle_backlog_status(self, message: Dict[str, Any]) -> None:
        """
        Handle backlog status updates.
        
        Args:
            message: Backlog status message
        """
        try:
            # Log backlog status
            logger.info(f"Backlog status: {json.dumps(message, indent=2)}")
            
            # Update stats
            self.stats['backlog_size'] = message.get('size', 0)
            self.stats['queued_urls'] = message.get('queued_urls', [])
            self.stats['processing_urls'] = message.get('processing_urls', [])
            
            # Publish to container status channel
            self.redis_manager.publish(self.container_status_channel, {
                'type': 'backlog_status',
                'data': message,
                'timestamp': time.time()
            })
        except Exception as e:
            logger.error(f"Error handling backlog status: {e}")

    def _handle_analyzer_status(self, message: Dict[str, Any]) -> None:
        """
        Handle status updates from the analyzer.
        
        Args:
            message: Status message
        """
        status = message.get('status')
        msg_text = message.get('message')
        
        # Log the status update
        logger.info(f"Analyzer status: {status} - {msg_text}")
        
        # Update analyzer status
        self.analyzer_status = message
        
        # Handle specific status updates
        if status == 'completed':
            # Analyzer has completed processing a URL
            url = message.get('url', '')
            if url:
                logger.info(f"Analyzer completed processing URL: {url}")
                
                # Remove from processing set if present
                if url in self.processing:
                    self.processing.remove(url)
                    logger.debug(f"Removed {url} from processing set. Processing count: {len(self.processing)}")
                
                # Update stats
                self.stats['processed_count'] += 1
                self._publish_status('url_analyzed', f"URL analyzed: {url}", {
                    'url': url,
                    'processed_count': self.stats['processed_count']
                })
    
    def _handle_analyzer_result(self, message: Dict[str, Any]) -> None:
        """
        Handle results from the analyzer.
        
        Args:
            message: Analyzer result message
        """
        try:
            url = message.get('url')
            if not url:
                logger.warning("Received analyzer result without URL")
                return
            
            logger.info(f"URL analyzed: {url}")
            
            with self.lock:
                self.stats['processed_count'] += 1
                
                # Remove URL from processing list
                for i, processing_item in enumerate(self.processing):
                    if processing_item.get('url') == url:
                        self.processing.pop(i)
                        break
                
                # Update stats
                self.stats['processing_count'] = len(self.processing)
                self.stats['backlog_size'] = len(self.backlog)
                
                # Publish status update
                self._publish_status('url_analyzed', f"URL analyzed: {url}")
        except Exception as e:
            logger.error(f"Error handling analyzer result: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _publish_stats(self) -> None:
        """Publish container app statistics to the stats channel."""
        try:
            with self.lock:
                stats_data = {
                    'component': 'container_app',
                    'stats': {
                        'uptime': time.time() - self.stats['start_time'] if self.stats['start_time'] else 0,
                        'backlog_size': self.stats['backlog_size'],
                        'backlog_requests': self.stats['backlog_requests'],
                        'processed_count': self.stats['processed_count'],
                        'processing_count': self.stats['processing_count'],
                        'crawler_status': self.stats['crawler_status'],
                        'analyzer_status': self.stats['analyzer_status']
                    },
                    'timestamp': time.time()
                }
            
            self.redis_manager.publish(self.stats_channel, stats_data)
        except Exception as e:
            logger.error(f"Error publishing stats: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _publish_status(self, status: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """
        Publish status update to the status channel.
        
        Args:
            status: Status code (e.g., 'starting', 'running', 'error')
            message: Status message
            details: Optional details dictionary
        """
        if details is None:
            details = {}
            
        status_update = {
            'timestamp': time.time(),
            'status': status,
            'message': message,
            'backlog_size': len(self.backlog),
            'backlog_min_threshold': self.backlog_min_threshold,
            'backlog_target_size': self.backlog_target_size,
            'details': details
        }
        
        # Log the status update
        logger.info(f"Status update: {status} - {message}")
        
        # Publish to the status channel
        self.redis_manager.publish(self.backlog_status_channel, status_update)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get current statistics.
        
        Returns:
            Dict[str, Any]: Current statistics
        """
        with self.lock:
            return {
                'uptime': time.time() - self.stats['start_time'] if self.stats['start_time'] else 0,
                'backlog_size': self.stats['backlog_size'],
                'backlog_requests': self.stats['backlog_requests'],
                'processed_count': self.stats['processed_count'],
                'processing_count': self.stats['processing_count'],
                'crawler_status': self.stats['crawler_status'],
                'analyzer_status': self.stats['analyzer_status'],
                'running': self.running,
                'paused': self.paused
            }
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of the container app and its components.
        
        Returns:
            Dict[str, Any]: Status information
        """
        status = {
            'container': {
                'running': self.running,
                'paused': self.paused,
                'stats': self.stats
            },
            'crawler': self.crawler_status,
            'analyzer': self.analyzer_status,
            'backlog': {
                'size': len(self.backlog),
                'min_threshold': self.backlog_min_threshold,
                'target_size': self.backlog_target_size
            }
        }
        
        return status
    
    def pause(self) -> None:
        """Pause the container application."""
        self.paused = True
        self._publish_status('paused', 'Container application paused')
        logger.info("Container application paused")
    
    def resume(self) -> None:
        """Resume the container application."""
        self.paused = False
        self._publish_status('resumed', 'Container application resumed')
        logger.info("Container application resumed")
    
    def start_crawler(self) -> bool:
        """
        Send a command to start the crawler.
        
        Returns:
            bool: True if command was sent successfully, False otherwise
        """
        try:
            # Prepare command
            command = {
                'command': 'start',
                'params': {
                    'active': True  # Ensure the crawler is set to active
                }
            }
            
            # Publish command
            success = self.redis_manager.publish(self.crawler_commands_channel, command)
            
            if success:
                logger.info("Sent start command to crawler")
                return True
            else:
                logger.error("Failed to send start command to crawler")
                return False
        except Exception as e:
            logger.error(f"Error starting crawler: {e}")
            return False
    
    def stop_crawler(self) -> bool:
        """
        Send a command to stop the crawler.
        
        Returns:
            bool: True if command was sent successfully, False otherwise
        """
        try:
            command = {
                'command': 'stop',
                'params': {}
            }
            
            success = self.redis_manager.publish(self.crawler_commands_channel, command)
            
            if success:
                logger.info("Sent stop command to crawler")
                return True
            else:
                logger.error("Failed to send stop command to crawler")
                return False
        except Exception as e:
            logger.error(f"Error stopping crawler: {e}")
            return False
    
    def pause_crawler(self) -> bool:
        """
        Send a command to pause the crawler.
        
        Returns:
            bool: True if command was sent successfully, False otherwise
        """
        try:
            command = {
                'command': 'pause',
                'params': {}
            }
            
            success = self.redis_manager.publish(self.crawler_commands_channel, command)
            
            if success:
                logger.info("Sent pause command to crawler")
                return True
            else:
                logger.error("Failed to send pause command to crawler")
                return False
        except Exception as e:
            logger.error(f"Error pausing crawler: {e}")
            return False
    
    def resume_crawler(self) -> bool:
        """
        Send a command to resume the crawler.
        
        Returns:
            bool: True if command was sent successfully, False otherwise
        """
        try:
            command = {
                'command': 'resume',
                'params': {}
            }
            
            success = self.redis_manager.publish(self.crawler_commands_channel, command)
            
            if success:
                logger.info("Sent resume command to crawler")
                return True
            else:
                logger.error("Failed to send resume command to crawler")
                return False
        except Exception as e:
            logger.error(f"Error resuming crawler: {e}")
            return False
    
    def start_analyzer(self) -> bool:
        """
        Send a command to start the analyzer.
        
        Returns:
            bool: True if command was sent successfully, False otherwise
        """
        try:
            # Prepare command
            command = {
                'command': 'start',
                'params': {}
            }
            
            # Publish command
            success = self.redis_manager.publish(self.analyzer_commands_channel, command)
            
            if success:
                logger.info("Sent start command to analyzer")
                return True
            else:
                logger.error("Failed to send start command to analyzer")
                return False
        except Exception as e:
            logger.error(f"Error starting analyzer: {e}")
            return False
    
    def start_crawl_job(self, start_url: str = None, max_urls: int = None, max_depth: int = None) -> bool:
        """
        Start a new crawl job.
        
        Args:
            start_url: URL to start crawling from
            max_urls: Maximum number of URLs to crawl
            max_depth: Maximum depth to crawl
            
        Returns:
            bool: True if command was sent successfully, False otherwise
        """
        try:
            # Use default values from config if not provided
            if not start_url:
                start_url = self.config.get('web_crawler', {}).get('target_website')
                if not start_url:
                    logger.error("No start URL provided and no default in config")
                    return False
            
            # Prepare command
            command = {
                'command': 'crawl',
                'params': {
                    'start_url': start_url,
                    'job_id': f"job_{int(time.time())}",
                    'active': True  # Ensure the crawler is set to active
                }
            }
            
            # Add optional parameters if provided
            if max_urls is not None:
                command['params']['max_urls'] = max_urls
            if max_depth is not None:
                command['params']['max_depth'] = max_depth
            
            # Publish command
            success = self.redis_manager.publish(self.crawler_commands_channel, command)
            
            if success:
                logger.info(f"Sent crawl job command to crawler for URL: {start_url}")
                return True
            else:
                logger.error(f"Failed to send crawl job command to crawler for URL: {start_url}")
                return False
        except Exception as e:
            logger.error(f"Error starting crawl job: {e}")
            return False
    
    def _start_crawler(self) -> None:
        """Send explicit command to start the crawler worker."""
        try:
            command = {
                'command': 'start',
                'params': {
                    'target_website': self.config.get('web_crawler', {}).get('target_website', 'manualslib.com')
                }
            }
            logger.info(f"Sending start command to crawler: {command}")
            self.redis_manager.publish(self.crawler_commands_channel, command)
        except Exception as e:
            logger.error(f"Error sending start command to crawler: {e}")

    def _start_analyzer(self) -> None:
        """Send explicit command to start the analyzer worker."""
        try:
            command = {
                'command': 'start',
                'params': {}
            }
            logger.info(f"Sending start command to analyzer: {command}")
            self.redis_manager.publish(self.analyzer_commands_channel, command)
        except Exception as e:
            logger.error(f"Error sending start command to analyzer: {e}")

    def _process_next_url(self) -> None:
        """
        Process the next URL in the backlog.
        """
        try:
            # Check if we can process more URLs
            with self.lock:
                if len(self.processing) >= self.max_concurrent_processing:
                    logger.debug(f"Max concurrent processing reached ({len(self.processing)}/{self.max_concurrent_processing}). Waiting...")
                    return
                    
                # Get the next URL from the backlog
                if not self.backlog:
                    logger.debug("Backlog is empty. Waiting for more URLs...")
                    return
                    
                url = self.backlog.pop(0)
                
                # Skip if already processing
                processed_urls = [item.get('url') for item in self.processing]
                if url in processed_urls:
                    logger.debug(f"Skipping already processing URL: {url}")
                    return
                    
                # Add to processing list
                self.processing.append({
                    'url': url,
                    'timestamp': time.time(),
                    'component': 'container'
                })
                
                # Update stats
                self.stats['processing_count'] = len(self.processing)
                self.stats['backlog_size'] = len(self.backlog)
            
            # Log the processing
            logger.info(f"Processing URL: {url}")
            self._publish_status('processing_url', f"Processing URL: {url}")
            
            # Send to analyzer
            self._send_to_analyzer(url)
        except Exception as e:
            logger.error(f"Error processing next URL: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _send_to_analyzer(self, url: str) -> None:
        """
        Send a URL to the analyzer.
        
        Args:
            url: URL to send
        """
        try:
            # Create analyzer command
            command = {
                'command': 'analyze',
                'params': {
                    'url': url
                }
            }
            
            # Publish to analyzer commands channel
            self.redis_manager.publish(self.analyzer_commands_channel, command)
            
            logger.info(f"Sent URL to analyzer: {url}")
        except Exception as e:
            logger.error(f"Error sending URL to analyzer: {e}")


def main():
    """Main entry point for the container application."""
    try:
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        
        # Create container app
        app = ContainerApp()
        
        # Start container app
        success = app.start()
        if not success:
            logger.error("Failed to start container app")
            return 1
        
        logger.info("Container app started. Press Ctrl+C to stop.")
        
        # Main loop
        try:
            while not SHUTDOWN_FLAG:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received. Stopping...")
        
        # Stop container app
        app.stop()
        
        return 0
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
