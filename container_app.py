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

import json
import logging
import threading
import time
import os
import signal
import sys
from typing import Dict, List, Any, Optional, Union

# Add the parent directory to the path to import from utils
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.redis_manager import get_redis_manager
from utils.config_manager import ConfigManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/container_app.log'),
        logging.StreamHandler()
    ]
)
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
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.get_all()
        
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
            'last_crawler_update': 0
        }
        
        # Application state
        self.running = False
        self.paused = False
        self.worker_thread = None
        self.lock = threading.Lock()
        
        # Processing configuration
        self.max_concurrent_processing = self.config.get('application', {}).get('components', {}).get('container_app', {}).get('max_concurrent_processing', 10)
        
        logger.info(f"Container app initialized with backlog thresholds: min={self.backlog_min_threshold}, target={self.backlog_target_size}")
    
    def start(self) -> bool:
        """
        Start the container application.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        if self.running:
            logger.warning("Container app already running")
            return True
        
        try:
            # Ensure Redis manager is connected
            if not self.redis_manager.is_connected():
                success = self.redis_manager.start()
                if not success:
                    logger.error("Failed to connect to Redis")
                    return False
            
            # Subscribe to channels
            self.redis_manager.subscribe(self.crawler_status_channel, self._handle_crawler_status)
            self.redis_manager.subscribe(self.analyzer_status_channel, self._handle_analyzer_status)
            self.redis_manager.subscribe(self.backlog_status_channel, self._handle_backlog_status)
            self.redis_manager.subscribe(self.analyzer_results_channel, self._handle_analyzer_result)
            
            # Start worker thread
            self.running = True
            self.stats['start_time'] = time.time()
            self.worker_thread = threading.Thread(target=self._worker_loop)
            self.worker_thread.daemon = True
            self.worker_thread.start()
            
            # Publish system status
            self._publish_status('starting', 'Container app starting')
            
            # Start crawler and analyzer components
            logger.info("Starting crawler component...")
            crawler_started = self.start_crawler()
            if not crawler_started:
                logger.warning("Failed to start crawler component")
            
            logger.info("Starting analyzer component...")
            analyzer_started = self.start_analyzer()
            if not analyzer_started:
                logger.warning("Failed to start analyzer component")
            
            # Start a crawl job with the target website from config
            if crawler_started:
                target_website = self.config.get('web_crawler', {}).get('target_website')
                if target_website:
                    logger.info(f"Starting initial crawl job with target website: {target_website}")
                    time.sleep(1)  # Give the crawler a moment to initialize
                    self.start_crawl_job(target_website)
                else:
                    logger.warning("No target website configured in config.yaml")
            
            logger.info("Container app started successfully")
            return True
        except Exception as e:
            logger.error(f"Error starting container app: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.running = False
            return False
    
    def stop(self) -> None:
        """Stop the container application."""
        if not self.running:
            return
        
        self.running = False
        
        # Wait for worker thread to terminate
        if self.worker_thread:
            self.worker_thread.join(timeout=1.0)
        
        # Unsubscribe from channels
        self.redis_manager.unsubscribe(self.crawler_status_channel)
        self.redis_manager.unsubscribe(self.analyzer_status_channel)
        self.redis_manager.unsubscribe(self.backlog_status_channel)
        self.redis_manager.unsubscribe(self.analyzer_results_channel)
        
        # Publish system status
        self._publish_status('stopping', 'Container app stopping')
        
        logger.info("Container app stopped")
    
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
            message: Status message
        """
        try:
            # Log the status update
            status = message.get('status', 'unknown')
            details = message.get('message', '')
            logger.info(f"Crawler status update: {status} - {details}")
            
            # Update crawler status
            with self.lock:
                self.crawler_status = message
                self.stats['last_crawler_update'] = time.time()
            
            # If the crawler is processing a URL, add it to the processing list
            if status == 'crawling' and 'Processing URL:' in details:
                url = details.replace('Processing URL:', '').strip()
                if url:
                    with self.lock:
                        # Add to processing list if not already there
                        if url not in [item.get('url') for item in self.processing]:
                            self.processing.append({
                                'url': url,
                                'timestamp': time.time(),
                                'component': 'crawler'
                            })
                            logger.debug(f"Added URL to processing list: {url}")
            
            # Publish system status update
            self._publish_status('running', f"Crawler: {status} - {details}")
        except Exception as e:
            logger.error(f"Error handling crawler status: {e}")
    
    def _handle_analyzer_status(self, message: Dict[str, Any]) -> None:
        """
        Handle status updates from the analyzer.
        
        Args:
            message: Status message
        """
        status = message.get('status', '')
        msg_text = message.get('message', '')
        
        # Log the status update
        logger.info(f"Analyzer status: {status} - {msg_text}")
        
        # Update analyzer status
        self.analyzer_status = status
        
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
    
    def _handle_backlog_status(self, message: Dict[str, Any]) -> None:
        """
        Handle a backlog status update from the crawler.
        
        Args:
            message: Backlog status message
        """
        try:
            status = message.get('status')
            urls = message.get('urls', [])
            count = message.get('count', 0)
            
            logger.info(f"Received backlog status update: {status}, {count} URLs")
            
            if status == 'backlog_response' and urls:
                with self.lock:
                    # Add URLs to the backlog
                    for url_data in urls:
                        # Handle both string URLs and dictionary URL objects
                        if isinstance(url_data, str):
                            url = url_data
                        elif isinstance(url_data, dict):
                            url = url_data.get('url')
                        else:
                            logger.warning(f"Unexpected URL data format: {type(url_data)}")
                            continue
                            
                        if not url:
                            continue
                            
                        # Check if URL is already in backlog or processing
                        processing_urls = [item.get('url') for item in self.processing]
                        if url not in self.backlog and url not in processing_urls:
                            self.backlog.append(url)
                            logger.info(f"Added URL to backlog: {url}")
                
                    # Update stats
                    self.stats['backlog_size'] = len(self.backlog)
                
                    # Publish status update
                    self._publish_status('backlog_updated', f"Added {len(urls)} URLs to backlog")
                    logger.info(f"Backlog size after update: {len(self.backlog)}")
        except Exception as e:
            logger.error(f"Error handling backlog status: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
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
        self._publish_status('paused', 'Container app paused')
        logger.info("Container app paused")
    
    def resume(self) -> None:
        """Resume the container application."""
        self.paused = False
        self._publish_status('resumed', 'Container app resumed')
        logger.info("Container app resumed")
    
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
