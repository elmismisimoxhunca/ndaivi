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
    
    def _start_crawler(self) -> None:
        """Start the crawler component."""
        try:
            start_command = {
                'command': 'start',
                'params': {
                    'active': True
                }
            }
            success = self.redis_manager.publish(self.crawler_commands_channel, start_command)
            if success:
                logger.info("Sent start command to crawler")
                self._publish_status('starting_crawler', "Sent start command to crawler")
            else:
                logger.error("Failed to send start command to crawler")
        except Exception as e:
            logger.error(f"Error starting crawler: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _start_analyzer(self) -> None:
        """Start the analyzer component."""
        try:
            start_command = {
                'command': 'start',
                'params': {}
            }
            success = self.redis_manager.publish(self.analyzer_commands_channel, start_command)
            if success:
                logger.info("Sent start command to analyzer")
                self._publish_status('starting_analyzer', "Sent start command to analyzer")
            else:
                logger.error("Failed to send start command to analyzer")
        except Exception as e:
            logger.error(f"Error starting analyzer: {e}")
    
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
        """
        Monitor loop for the container application.
        Periodically checks the status of the container app and its components.
        """
        logger.info("Starting container monitor loop")
        
        while self.running:
            try:
                # Get current status
                status = self.get_status()
                
                # Log status if verbose
                if self.config.get('application', {}).get('verbose', False):
                    logger.info(f"Container status: {json.dumps(status, indent=2)}")
                
                # Publish status update
                self._publish_status('status_update', 'Container status update', status)
                
                # Sleep for a short time
                time.sleep(1.0)
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
                import traceback
                logger.error(traceback.format_exc())
                time.sleep(1.0)  # Sleep longer on error
    
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
        last_ping_time = 0
        ping_interval = 30  # Send ping every 30 seconds
        
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
                
                # Send ping to crawler periodically
                if current_time - last_ping_time >= ping_interval:
                    self._send_ping_to_crawler()
                    last_ping_time = current_time
                
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
            
            # Generate a unique request ID for tracking
            request_id = f"req_{int(time.time())}_{id(self)}"
            
            # Prepare request
            request = {
                'command': 'backlog_request',
                'count': count,
                'request_id': request_id,
                'timestamp': time.time()
            }
            
            # Track this request for potential retry
            self.stats['last_backlog_request'] = {
                'id': request_id,
                'count': count,
                'timestamp': time.time(),
                'retries': 0
            }
            
            # Publish request
            success = self.redis_manager.publish(self.backlog_request_channel, request)
            
            if success:
                self._publish_status('backlog_request', f"Requested {count} URLs from crawler (ID: {request_id})")
            else:
                logger.error(f"Failed to publish backlog request (ID: {request_id})")
                # Schedule a retry after a short delay
                threading.Timer(2.0, self._retry_backlog_request, args=[request_id, count, 1]).start()
        except Exception as e:
            logger.error(f"Error requesting more URLs: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _retry_backlog_request(self, request_id: str, count: int, retry_count: int) -> None:
        """
        Retry a failed backlog request.
        
        Args:
            request_id: Original request ID
            count: Number of URLs to request
            retry_count: Current retry attempt
        """
        max_retries = 3
        if retry_count > max_retries:
            logger.error(f"Maximum retries ({max_retries}) reached for backlog request {request_id}")
            return
            
        logger.info(f"Retrying backlog request {request_id} (attempt {retry_count}/{max_retries})")
        
        # Prepare retry request
        retry_request = {
            'command': 'backlog_request',
            'count': count,
            'request_id': request_id,
            'timestamp': time.time(),
            'retry': retry_count
        }
        
        # Update stats
        with self.lock:
            if 'last_backlog_request' in self.stats and self.stats['last_backlog_request'].get('id') == request_id:
                self.stats['last_backlog_request']['retries'] = retry_count
                self.stats['last_backlog_request']['timestamp'] = time.time()
        
        # Publish retry request
        success = self.redis_manager.publish(self.backlog_request_channel, retry_request)
        
        if success:
            self._publish_status('backlog_request_retry', f"Retry {retry_count}/{max_retries} for backlog request {request_id}")
        else:
            logger.error(f"Failed to publish backlog request retry {retry_count}/{max_retries}")
            # Schedule another retry with exponential backoff
            if retry_count < max_retries:
                backoff_time = 2.0 * (2 ** retry_count)  # Exponential backoff
                threading.Timer(backoff_time, self._retry_backlog_request, args=[request_id, count, retry_count + 1]).start()
    
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
            status = message.get('status')
            
            # Handle different status types
            if status == 'backlog_response':
                # Handle backlog response with URLs
                urls = message.get('urls', [])
                count = message.get('count', 0)
                request_id = message.get('request_id', 'unknown')
                
                logger.info(f"Received backlog response with {count} URLs for request {request_id}")
                
                # Add URLs to backlog
                with self.lock:
                    for url_data in urls:
                        url = url_data.get('url')
                        if url and url not in [item.get('url') for item in self.backlog]:
                            self.backlog.append(url_data)
                    
                    # Update stats
                    self.stats['backlog_size'] = len(self.backlog)
                    self.stats['last_backlog_response'] = {
                        'request_id': request_id,
                        'urls_received': count,
                        'timestamp': time.time()
                    }
                
                # Publish status update
                self._publish_status('backlog_updated', f"Added {count} URLs to backlog from request {request_id}")
                
            elif status == 'backlog_ack':
                # Handle acknowledgment of backlog request
                request_id = message.get('request_id', 'unknown')
                urls_sent = message.get('urls_sent', 0)
                
                logger.info(f"Received acknowledgment for backlog request {request_id} with {urls_sent} URLs sent")
                
                # Update stats
                with self.lock:
                    if 'last_backlog_request' in self.stats and self.stats['last_backlog_request'].get('id') == request_id:
                        self.stats['last_backlog_request']['acknowledged'] = True
                        self.stats['last_backlog_request']['urls_sent'] = urls_sent
                
            elif status == 'pong':
                # Handle pong response from crawler
                timestamp = message.get('timestamp', 0)
                crawler_id = message.get('crawler_id', 'unknown')
                
                # Calculate latency
                latency = time.time() - timestamp
                
                logger.debug(f"Received pong from crawler {crawler_id} with latency {latency:.3f}s")
                
                # Update stats
                with self.lock:
                    self.stats['last_crawler_ping'] = {
                        'crawler_id': crawler_id,
                        'timestamp': time.time(),
                        'latency': latency
                    }
                
            else:
                # General backlog status update
                logger.info(f"Backlog status update: {json.dumps(message, indent=2)}")
                
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
            import traceback
            logger.error(traceback.format_exc())
            
    def _send_ping_to_crawler(self) -> None:
        """
        Send a ping message to the crawler to check if it's still responsive.
        """
        try:
            ping_message = {
                'command': 'ping',
                'timestamp': time.time(),
                'container_id': id(self)
            }
            
            success = self.redis_manager.publish(self.crawler_commands_channel, ping_message)
            
            if success:
                logger.debug("Sent ping to crawler")
                
                # Schedule a check for the pong response
                threading.Timer(5.0, self._check_pong_response).start()
            else:
                logger.error("Failed to send ping to crawler")
        except Exception as e:
            logger.error(f"Error sending ping to crawler: {e}")
            
    def _check_pong_response(self) -> None:
        """
        Check if we received a pong response from the crawler.
        If not, the crawler might be disconnected.
        """
        try:
            current_time = time.time()
            
            # If we haven't received a pong response within 5 seconds, consider the crawler disconnected
            if not hasattr(self, 'last_pong_time') or current_time - self.last_pong_time > 5:
                logger.warning("No pong response received from crawler, it might be disconnected")
                
                # Try to restart the crawler if it's been more than 30 seconds since the last pong
                if not hasattr(self, 'last_pong_time') or current_time - self.last_pong_time > 30:
                    logger.warning("Crawler appears to be disconnected for too long, attempting to restart")
                    self._start_crawler()
        except Exception as e:
            logger.error(f"Error checking pong response: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
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
        try:
            if details is None:
                details = {}
                
            status_data = {
                'component': 'container',
                'status': status,
                'message': message,
                'details': details,
                'timestamp': time.time()
            }
            
            # Add current backlog size to status
            with self.lock:
                status_data['backlog_size'] = len(self.backlog)
                status_data['processing_count'] = len(self.processing)
            
            # Publish status update
            success = self.redis_manager.publish(self.container_status_channel, status_data)
            
            if not success:
                logger.error(f"Failed to publish status update: {status} - {message}")
                # Retry the publication after a short delay
                self._retry_publish(self.container_status_channel, status_data, 1)
        except Exception as e:
            logger.error(f"Error publishing status update: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _retry_publish(self, channel: str, message: Dict[str, Any], retry_count: int, max_retries: int = 3) -> None:
        """
        Retry publishing a message to a Redis channel.
        
        Args:
            channel: Channel to publish to
            message: Message to publish
            retry_count: Current retry count
            max_retries: Maximum number of retries
        """
        if retry_count > max_retries:
            logger.error(f"Maximum retries ({max_retries}) reached for publishing to {channel}")
            return
            
        # Add retry information to the message
        message['retry_count'] = retry_count
        message['retry_timestamp'] = time.time()
        
        # Exponential backoff
        backoff_time = 0.5 * (2 ** retry_count)
        
        # Use threading.Timer to retry after the backoff time
        def retry_task():
            try:
                logger.info(f"Retrying publish to {channel} (attempt {retry_count}/{max_retries})")
                success = self.redis_manager.publish(channel, message)
                
                if not success and retry_count < max_retries:
                    logger.warning(f"Retry {retry_count} failed, scheduling another retry")
                    self._retry_publish(channel, message, retry_count + 1, max_retries)
            except Exception as e:
                logger.error(f"Error in retry_publish task: {e}")
                
        # Schedule the retry
        threading.Timer(backoff_time, retry_task).start()
    
    def _process_next_url(self) -> None:
        """
        Process the next URL in the backlog.
        """
        try:
            # Check if we're at the maximum concurrent processing limit
            with self.lock:
                if len(self.processing) >= self.max_concurrent_processing:
                    return
                
                # Check if we have any URLs in the backlog
                if not self.backlog:
                    return
                
                # Get the next URL from the backlog
                url_data = self.backlog.pop(0)
                
                # Add to processing list
                self.processing.append(url_data)
                
                # Update stats
                self.stats['processing_count'] = len(self.processing)
                self.stats['backlog_size'] = len(self.backlog)
            
            # Get the URL from the data
            url = url_data.get('url')
            if not url:
                logger.warning("URL data missing URL field")
                return
                
            # Send URL to analyzer
            self._send_to_analyzer(url_data)
            
            # Log processing
            logger.info(f"Processing URL: {url}")
            
        except Exception as e:
            logger.error(f"Error processing next URL: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
            # If there was an error, check if we need to request more URLs
            current_time = time.time()
            if current_time - self.last_backlog_check >= 5:  # Check more frequently on error
                self._check_backlog_size()
                self.last_backlog_check = current_time
    
    def _send_to_analyzer(self, url_data: Dict[str, Any]) -> None:
        """
        Send a URL to the analyzer.
        
        Args:
            url_data: URL data dictionary containing url, depth, and metadata
        """
        try:
            # Extract URL from the data
            url = url_data.get('url')
            if not url:
                logger.warning("URL data missing URL field")
                return
                
            # Prepare command
            command = {
                'command': 'analyze',
                'url': url,
                'depth': url_data.get('depth', 0),
                'metadata': url_data.get('metadata', {}),
                'timestamp': time.time(),
                'request_id': f"analyze_{int(time.time())}_{id(self)}"
            }
            
            # Publish command
            success = self.redis_manager.publish(self.analyzer_commands_channel, command)
            
            if success:
                logger.info(f"Sent URL to analyzer: {url}")
                self._publish_status('url_sent', f"Sent URL to analyzer: {url}")
            else:
                logger.error(f"Failed to send URL to analyzer: {url}")
                
                # Remove from processing list if failed
                with self.lock:
                    for i, item in enumerate(self.processing):
                        if item.get('url') == url:
                            self.processing.pop(i)
                            break
                    
                    # Add back to backlog for retry
                    self.backlog.insert(0, url_data)
                    
                    # Update stats
                    self.stats['processing_count'] = len(self.processing)
                    self.stats['backlog_size'] = len(self.backlog)
                
                # Try again after a delay
                threading.Timer(5.0, lambda: self._send_to_analyzer(url_data)).start()
        except Exception as e:
            logger.error(f"Error sending URL to analyzer: {e}")
            import traceback
            logger.error(traceback.format_exc())

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
