#!/usr/bin/env python3
"""
NDAIVI Main Application

This is the main container application that coordinates all components of the NDAIVI system
using a Redis message-based architecture for effective communication between:
- Web Crawler
- Claude Analyzer
- Stats Manager
- Database Manager
"""

import os
import sys
import time
import json
import logging
import argparse
import signal
import yaml
from typing import Dict, List, Any, Optional, Union
import threading
import cmd
import subprocess
import psutil

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import components
from scraper.crawler_worker import CrawlerWorker
from scraper.analyzer_worker import AnalyzerWorker
from scraper.db_manager import DBManager
from utils.redis_manager import get_redis_manager
from utils.config_manager import ConfigManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/ndaivi.log')
    ]
)

# Set more restrictive log levels for noisy modules
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)

logger = logging.getLogger('ndaivi')

class StatsManager:
    """
    Manages system-wide statistics.
    
    This class collects and aggregates statistics from all components
    and provides methods to retrieve and display them.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the stats manager.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.redis_manager = get_redis_manager(config.get('redis', {}))
        
        # Ensure Redis manager is started
        if not self.redis_manager.is_connected():
            self.redis_manager.start()
            
        self.stats = {
            'crawler': {
                'urls_processed': 0,
                'urls_queued': 0,
                'urls_failed': 0,
                'domains_discovered': set(),
                'content_types': {},
                'status_codes': {},
                'crawl_rate': 0.0  # URLs per second
            },
            'analyzer': {
                'pages_analyzed': 0,
                'manufacturer_pages': 0,
                'category_pages': 0,
                'other_pages': 0,
                'categories_extracted': 0,
                'translations_completed': 0,
                'analysis_rate': 0.0  # Pages per second
            },
            'system': {
                'start_time': time.time(),
                'last_update': time.time(),
                'uptime': 0,
                'status': 'idle'
            }
        }
        
        # Subscribe to stats channel
        self.stats_channel = self.redis_manager.get_channel('stats')
        self.crawler_status_channel = self.redis_manager.get_channel('crawler_status')
        self.analyzer_status_channel = self.redis_manager.get_channel('analyzer_status')
        
        # Subscribe to all channels
        self.redis_manager.subscribe(self.stats_channel, self._handle_stats)
        self.redis_manager.subscribe(self.crawler_status_channel, self._handle_crawler_status)
        self.redis_manager.subscribe(self.analyzer_status_channel, self._handle_analyzer_status)
        
        # Update interval
        self.update_interval = config.get('application', {}).get('components', {}).get('stats', {}).get('update_interval', 5)
        
        # Worker state
        self.running = False
        self.worker_thread = None
        
        # Stats history for time-based metrics
        self.history = {
            'crawler': {
                'timestamps': [],
                'urls_processed': [],
                'max_history': 100
            },
            'analyzer': {
                'timestamps': [],
                'pages_analyzed': [],
                'max_history': 100
            }
        }
        
        # Lock for thread safety
        self.lock = threading.Lock()
    
    # Add method to publish stats to Redis for background process monitoring
    def publish_stats_to_redis(self):
        """Publish current stats to Redis for background process monitoring."""
        try:
            # Ensure Redis is connected
            if not self.redis_manager.is_connected():
                logger.warning("Redis not connected. Attempting to reconnect...")
                if not self.redis_manager.connect():
                    logger.error("Failed to reconnect to Redis")
                    return
                
            with self.lock:
                # Create a copy of stats to avoid thread safety issues
                stats_copy = {
                    'crawler': dict(self.stats['crawler']),
                    'analyzer': dict(self.stats['analyzer']),
                    'system': dict(self.stats['system'])
                }
                
                # Convert sets to lists for JSON serialization
                if isinstance(stats_copy['crawler'].get('domains_discovered'), set):
                    stats_copy['crawler']['domains_discovered'] = list(stats_copy['crawler']['domains_discovered'])
                
                # Publish to Redis
                self.redis_manager.store_data('ndaivi:stats:global', stats_copy)
                self.redis_manager.store_data('ndaivi:crawler:stats', stats_copy['crawler'])
                self.redis_manager.store_data('ndaivi:analyzer:stats', stats_copy['analyzer'])
        except Exception as e:
            logger.error(f"Error publishing stats to Redis: {e}")
    
    def start(self) -> bool:
        """
        Start the stats manager.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        if self.running:
            logger.warning("Stats manager already running")
            return True
        
        try:
            # Start worker thread
            self.running = True
            self.worker_thread = threading.Thread(target=self._worker_loop)
            self.worker_thread.daemon = True
            self.worker_thread.start()
            
            logger.info("Stats manager started successfully")
            return True
        except Exception as e:
            logger.error(f"Error starting stats manager: {e}")
            self.running = False
            return False
    
    def _worker_loop(self) -> None:
        """
        Main worker loop that periodically stores stats.
        This runs in a separate thread.
        """
        last_store_time = 0
        
        while self.running:
            try:
                # Store stats periodically
                current_time = time.time()
                if current_time - last_store_time >= self.update_interval:
                    self._store_stats()
                    self._update_rates()
                    # Publish stats to Redis for background process monitoring
                    self.publish_stats_to_redis()
                    last_store_time = current_time
                
                # Update system uptime
                with self.lock:
                    self.stats['system']['uptime'] = time.time() - self.stats['system']['start_time']
                
                # Small sleep to prevent CPU hogging
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error in stats manager loop: {e}")
                time.sleep(1)  # Back off on error
    
    def _handle_stats(self, stats_message: Dict[str, Any]) -> None:
        """
        Handle stats updates from components.
        
        Args:
            stats_message: Stats message dictionary
        """
        try:
            component = stats_message.get('component')
            stats = stats_message.get('stats', {})
            
            if component == 'crawler':
                self._update_crawler_stats(stats)
            elif component == 'analyzer':
                self._update_analyzer_stats(stats)
        except Exception as e:
            logger.error(f"Error handling stats message: {e}")
    
    def _handle_crawler_status(self, status_message: Dict[str, Any]) -> None:
        """
        Handle crawler status updates.
        
        Args:
            status_message: Status message dictionary
        """
        try:
            status = status_message.get('status')
            message = status_message.get('message', '')
            
            with self.lock:
                if status == 'crawling':
                    self.stats['system']['status'] = 'crawling'
                elif status == 'idle' and self.stats['system']['status'] == 'crawling':
                    self.stats['system']['status'] = 'analyzing'
                
                # Store the last crawler message
                self.stats['crawler']['last_message'] = message
                self.stats['system']['last_update'] = time.time()
        except Exception as e:
            logger.error(f"Error handling crawler status message: {e}")
    
    def _handle_analyzer_status(self, status_message: Dict[str, Any]) -> None:
        """
        Handle analyzer status updates.
        
        Args:
            status_message: Status message dictionary
        """
        try:
            status = status_message.get('status')
            message = status_message.get('message', '')
            
            with self.lock:
                if status == 'analyzing':
                    self.stats['system']['status'] = 'analyzing'
                elif status == 'idle' and self.stats['system']['status'] == 'analyzing':
                    self.stats['system']['status'] = 'idle'
                
                # Store the last analyzer message
                self.stats['analyzer']['last_message'] = message
                self.stats['system']['last_update'] = time.time()
        except Exception as e:
            logger.error(f"Error handling analyzer status message: {e}")
    
    def _update_crawler_stats(self, stats: Dict[str, Any]) -> None:
        """
        Update crawler statistics.
        
        Args:
            stats: Crawler stats dictionary
        """
        with self.lock:
            crawler_stats = self.stats['crawler']
            
            # Update basic stats
            crawler_stats['urls_processed'] = stats.get('urls_processed', crawler_stats['urls_processed'])
            crawler_stats['urls_queued'] = stats.get('urls_queued', crawler_stats['urls_queued'])
            crawler_stats['urls_failed'] = stats.get('urls_failed', crawler_stats['urls_failed'])
            
            # Update domains set
            domains = stats.get('domains', [])
            if domains:
                if isinstance(crawler_stats['domains_discovered'], set):
                    crawler_stats['domains_discovered'].update(domains)
                else:
                    crawler_stats['domains_discovered'] = set(domains)
            
            # Update content types
            content_types = stats.get('content_types', {})
            for content_type, count in content_types.items():
                if content_type in crawler_stats['content_types']:
                    crawler_stats['content_types'][content_type] += count
                else:
                    crawler_stats['content_types'][content_type] = count
            
            # Update status codes
            status_codes = stats.get('status_codes', {})
            for status_code, count in status_codes.items():
                status_code_str = str(status_code)
                if status_code_str in crawler_stats['status_codes']:
                    crawler_stats['status_codes'][status_code_str] += count
                else:
                    crawler_stats['status_codes'][status_code_str] = count
            
            # Update system timestamp
            self.stats['system']['last_update'] = time.time()
            
            # Update history for rate calculation
            self._update_history('crawler', crawler_stats['urls_processed'])
    
    def _update_analyzer_stats(self, stats: Dict[str, Any]) -> None:
        """
        Update analyzer statistics.
        
        Args:
            stats: Analyzer stats dictionary
        """
        with self.lock:
            analyzer_stats = self.stats['analyzer']
            
            # Update basic stats
            analyzer_stats['pages_analyzed'] = stats.get('pages_analyzed', analyzer_stats['pages_analyzed'])
            analyzer_stats['manufacturer_pages'] = stats.get('manufacturer_pages', analyzer_stats['manufacturer_pages'])
            analyzer_stats['category_pages'] = stats.get('category_pages', analyzer_stats['category_pages'])
            analyzer_stats['other_pages'] = stats.get('other_pages', analyzer_stats['other_pages'])
            
            # Update categories and translations
            analyzer_stats['categories_extracted'] = stats.get('categories_extracted', analyzer_stats['categories_extracted'])
            analyzer_stats['translations_completed'] = stats.get('translations_completed', analyzer_stats['translations_completed'])
            
            # Update system timestamp
            self.stats['system']['last_update'] = time.time()
            
            # Update history for rate calculation
            self._update_history('analyzer', analyzer_stats['pages_analyzed'])
    
    def _update_history(self, component: str, value: int) -> None:
        """
        Update history for a component metric.
        
        Args:
            component: Component name
            value: Current value
        """
        history = self.history.get(component)
        if not history:
            return
        
        current_time = time.time()
        
        # Add current values
        history['timestamps'].append(current_time)
        history['urls_processed' if component == 'crawler' else 'pages_analyzed'].append(value)
        
        # Trim history if needed
        if len(history['timestamps']) > history['max_history']:
            history['timestamps'] = history['timestamps'][-history['max_history']:]
            history['urls_processed' if component == 'crawler' else 'pages_analyzed'] = history['urls_processed' if component == 'crawler' else 'pages_analyzed'][-history['max_history']:]
    
    def _update_rates(self) -> None:
        """Update processing rates based on history."""
        with self.lock:
            # Update crawler rate
            crawler_history = self.history['crawler']
            if len(crawler_history['timestamps']) >= 2:
                time_diff = crawler_history['timestamps'][-1] - crawler_history['timestamps'][0]
                if time_diff > 0:
                    value_diff = crawler_history['urls_processed'][-1] - crawler_history['urls_processed'][0]
                    self.stats['crawler']['crawl_rate'] = value_diff / time_diff
            
            # Update analyzer rate
            analyzer_history = self.history['analyzer']
            if len(analyzer_history['timestamps']) >= 2:
                time_diff = analyzer_history['timestamps'][-1] - analyzer_history['timestamps'][0]
                if time_diff > 0:
                    value_diff = analyzer_history['pages_analyzed'][-1] - analyzer_history['pages_analyzed'][0]
                    self.stats['analyzer']['analysis_rate'] = value_diff / time_diff
    
    def _store_stats(self) -> None:
        """Store current stats to database or file."""
        # Publish stats to Redis for background process monitoring
        try:
            # Ensure Redis is connected
            if not self.redis_manager.is_connected():
                logger.warning("Redis not connected. Attempting to reconnect...")
                if not self.redis_manager.connect():
                    logger.error("Failed to reconnect to Redis")
                    return
                
            with self.lock:
                # Create a copy of stats to avoid thread safety issues
                stats_copy = {
                    'crawler': dict(self.stats['crawler']),
                    'analyzer': dict(self.stats['analyzer']),
                    'system': dict(self.stats['system'])
                }
                
                # Convert sets to lists for JSON serialization
                if isinstance(stats_copy['crawler'].get('domains_discovered'), set):
                    stats_copy['crawler']['domains_discovered'] = list(stats_copy['crawler']['domains_discovered'])
                
                # Publish to Redis
                self.redis_manager.store_data('ndaivi:stats:global', stats_copy)
                self.redis_manager.store_data('ndaivi:crawler:stats', stats_copy['crawler'])
                self.redis_manager.store_data('ndaivi:analyzer:stats', stats_copy['analyzer'])
        except Exception as e:
            logger.error(f"Error publishing stats to Redis: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get current statistics.
        
        Returns:
            Dict[str, Any]: Current statistics
        """
        with self.lock:
            # Create a copy of stats to avoid thread safety issues
            stats_copy = {
                'crawler': dict(self.stats['crawler']),
                'analyzer': dict(self.stats['analyzer']),
                'system': dict(self.stats['system'])
            }
            
            # Convert sets to lists for JSON serialization
            if isinstance(stats_copy['crawler'].get('domains_discovered'), set):
                stats_copy['crawler']['domains_discovered'] = list(stats_copy['crawler']['domains_discovered'])
            
            return stats_copy
    
    def format_stats(self) -> str:
        """
        Format statistics for display.
        
        Returns:
            str: Formatted statistics
        """
        stats = self.get_stats()
        
        # Format system stats
        system_stats = stats['system']
        uptime = system_stats['uptime']
        uptime_str = f"{int(uptime // 3600)}h {int((uptime % 3600) // 60)}m {int(uptime % 60)}s"
        
        # Format crawler stats
        crawler_stats = stats['crawler']
        
        # Format analyzer stats
        analyzer_stats = stats['analyzer']
        
        # Build formatted string
        formatted = [
            "=== NDAIVI System Statistics ===",
            f"Status: {system_stats['status']}",
            f"Uptime: {uptime_str}",
            "",
            "--- Crawler Statistics ---",
            f"URLs Processed: {crawler_stats.get('urls_processed', 0)}",
            f"URLs Queued: {crawler_stats.get('urls_queued', 0)}",
            f"URLs Failed: {crawler_stats.get('urls_failed', 0)}",
            f"Domains Discovered: {len(crawler_stats.get('domains_discovered', []))}",
            f"Crawl Rate: {crawler_stats.get('crawl_rate', 0.0):.2f} URLs/second",
            "",
            "--- Analyzer Statistics ---",
            f"Pages Analyzed: {analyzer_stats.get('pages_analyzed', 0)}",
            f"Manufacturer Pages: {analyzer_stats.get('manufacturer_pages', 0)}",
            f"Category Pages: {analyzer_stats.get('category_pages', 0)}",
            f"Other Pages: {analyzer_stats.get('other_pages', 0)}",
            f"Categories Extracted: {analyzer_stats.get('categories_extracted', 0)}",
            f"Translations Completed: {analyzer_stats.get('translations_completed', 0)}",
            f"Analysis Rate: {analyzer_stats.get('analysis_rate', 0.0):.2f} pages/second"
        ]
        
        return "\n".join(formatted)
    
    def print_stats(self) -> None:
        """Print current statistics to console."""
        print(self.format_stats())

class NDaiviCLI(cmd.Cmd):
    """
    Command-line interface for NDAIVI.
    
    This class provides a command-line interface for interacting with the NDAIVI
    system, including commands for starting and stopping the system, checking
    status, and managing the crawler and analyzer components.
    """
    
    intro = "Welcome to NDAIVI CLI. Type help or ? to list commands.\n"
    prompt = "(ndaivi) "
    
    def __init__(self, app: 'NDaiviApp'):
        """
        Initialize the CLI with an NDaiviApp instance.
        
        Args:
            app: NDaiviApp instance
        """
        super().__init__()
        self.app = app
        
    def do_start(self, arg):
        """
        Start the NDAIVI system.
        
        Usage: start [--target URL]
        """
        try:
            # Parse arguments
            parser = argparse.ArgumentParser(prog='start')
            parser.add_argument('--target', help='Target website URL', default=None)
            args = parser.parse_args(arg.split())
            
            if args.target:
                self.app.config['web_crawler']['target_website'] = args.target
            
            # Start the system
            if self.app.start_modular():
                print("NDAIVI system started successfully")
            else:
                print("Failed to start NDAIVI system")
        except Exception as e:
            print(f"Error starting system: {e}")
            
    def do_stop(self, arg):
        """
        Stop the NDAIVI system.
        
        Usage: stop
        """
        try:
            if self.app.stop():
                print("NDAIVI system stopped successfully")
            else:
                print("Failed to stop NDAIVI system")
        except Exception as e:
            print(f"Error stopping system: {e}")
            
    def do_monitor(self, arg):
        """
        Monitor the NDAIVI system in real-time.
        Press Ctrl+C to stop monitoring.
        
        Usage: monitor
        """
        try:
            print("Starting system monitor... Press Ctrl+C to stop.")
            self.app.start_monitor()
        except KeyboardInterrupt:
            print("\nStopping monitor...")
            self.app.stop_monitor()
        except Exception as e:
            print(f"Error in monitor: {e}")
            
    def do_status(self, arg):
        """
        Show current system status.
        
        Usage: status
        """
        try:
            status = self.app.get_status()
            print("\nSystem Status:")
            print(f"Crawler: {status['crawler']['status']} - {status['crawler']['message']}")
            print(f"Analyzer: {status['analyzer']['status']} - {status['analyzer']['message']}")
            print(f"\nBacklog size: {status.get('backlog_size', 0)}")
        except Exception as e:
            print(f"Error getting status: {e}")
            
    def do_exit(self, arg):
        """Exit the CLI."""
        try:
            print("Stopping NDAIVI system...")
            # Stop monitoring if active
            self.app.stop_monitor()
            # Stop the application
            self.app.stop()
            print("Goodbye!")
            return True
        except Exception as e:
            print(f"Error during exit: {e}")
            return True
        
    def do_quit(self, arg):
        """Exit the CLI."""
        return self.do_exit(arg)

class NDaiviApp:
    """
    NDAIVI Application.
    
    This class provides the main application logic for the NDAIVI system,
    including methods for starting and stopping the system, checking status,
    and managing the crawler and analyzer components.
    """
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize the NDAIVI application.
        
        Args:
            config_path: Path to configuration file
        """
        # Load configuration
        self._load_config(config_path)
        
        # Initialize Redis manager
        self.redis_manager = get_redis_manager(self.config)
        
        # Initialize stats manager
        self.stats_manager = StatsManager(self.config)
        
        # Initialize background process
        self.background_process = None
    
    def _load_config(self, config_path: str) -> None:
        """
        Load configuration from file.
        
        Args:
            config_path: Path to configuration file
        """
        # Use ConfigManager to load configuration
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.get_all()
    
    def reload_config(self) -> bool:
        """
        Reload configuration from file.
        
        Returns:
            bool: True if configuration was reloaded successfully
        """
        try:
            # Reload configuration using ConfigManager
            self.config_manager.reload()
            self.config = self.config_manager.get_all()
            return True
        except Exception as e:
            logger.error(f"Error reloading configuration: {e}")
            return False
    
    def start_modular(self) -> bool:
        """
        Start the NDAIVI system in modular mode.
        
        This method starts the container app, which coordinates the crawler and analyzer.
        
        Returns:
            bool: True if started successfully
        """
        try:
            # Initialize container app first (it will coordinate the other components)
            logger.info("Starting container app...")
            from container_app import ContainerApp
            self.container_app = ContainerApp()
            container_started = self.container_app.start()
            
            if not container_started:
                logger.error("Failed to start container app")
                return False
                
            # Start the monitor
            self.container_app.start_monitor()
                
            # Initialize crawler and analyzer workers
            logger.info("Initializing crawler and analyzer workers...")
            from scraper.crawler_worker import CrawlerWorker
            from scraper.analyzer_worker import AnalyzerWorker
            
            # Create and start workers
            self.crawler_worker = CrawlerWorker(self.config)
            self.analyzer_worker = AnalyzerWorker(self.config)
            
            # Start the workers
            crawler_started = self.crawler_worker.start()
            analyzer_started = self.analyzer_worker.start()
            
            if not crawler_started:
                logger.error("Failed to start crawler worker")
                self.container_app.stop()
                return False
            
            if not analyzer_started:
                logger.error("Failed to start analyzer worker")
                self.crawler_worker.stop()
                self.container_app.stop()
                return False
            
            logger.info("NDAIVI started successfully in modular mode")
            
            # Start a crawl job with the target website from config
            target_website = self.config.get('web_crawler', {}).get('target_website')
            if target_website:
                logger.info(f"Starting initial crawl job with target website: {target_website}")
                time.sleep(1)  # Brief pause to ensure everything is ready
                self.container_app.start_crawl_job(target_website)
            else:
                logger.warning("No target website configured, no initial crawl job started")
            
            # Set background process as running
            self.background_process = True  # Just a flag to indicate it's running
            
            return True
        except Exception as e:
            logger.error(f"Error starting NDAIVI in modular mode: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def stop(self) -> bool:
        """
        Stop the NDAIVI system.
        
        Returns:
            bool: True if stopped successfully
        """
        try:
            # Stop container app if running
            if hasattr(self, 'container_app'):
                logger.info("Stopping container app...")
                self.container_app.stop()
            
            # Stop crawler worker if running
            if hasattr(self, 'crawler_worker'):
                logger.info("Stopping crawler worker...")
                self.crawler_worker.stop()
            
            # Stop analyzer worker if running
            if hasattr(self, 'analyzer_worker'):
                logger.info("Stopping analyzer worker...")
                self.analyzer_worker.stop()
            
            # Stop background process if running
            if self.background_process:
                logger.info("Stopping background process...")
                self.stop_background_process()
            
            logger.info("NDAIVI stopped")
            return True
        except Exception as e:
            logger.error(f"Error stopping NDAIVI: {e}")
            return False
    
    def pause(self) -> bool:
        """
        Pause the NDAIVI system.
        
        Returns:
            bool: True if paused successfully
        """
        try:
            # Pause container app if running
            if hasattr(self, 'container_app'):
                logger.info("Pausing container app...")
                self.container_app.pause()
            
            logger.info("NDAIVI paused")
            return True
        except Exception as e:
            logger.error(f"Error pausing NDAIVI: {e}")
            return False
    
    def resume(self) -> bool:
        """
        Resume the NDAIVI system.
        
        Returns:
            bool: True if resumed successfully
        """
        try:
            # Resume container app if running
            if hasattr(self, 'container_app'):
                logger.info("Resuming container app...")
                self.container_app.resume()
            
            logger.info("NDAIVI resumed")
            return True
        except Exception as e:
            logger.error(f"Error resuming NDAIVI: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of the NDAIVI system.
        
        Returns:
            Dict[str, Any]: Status information
        """
        status = {
            'crawler': {'status': 'not_running'},
            'analyzer': {'status': 'not_running'},
            'background': {'status': 'not_running'},
            'backlog': {'size': 0, 'min_threshold': 0, 'target_size': 0},
            'stats': {}
        }
        
        try:
            # Get container app status
            if hasattr(self, 'container_app'):
                container_status = self.container_app.get_status()
                status['container_app'] = {'status': 'running' if container_status else 'error'}
                
                # Get crawler and analyzer status from container app
                if container_status:
                    crawler_data = container_status.get('crawler', {})
                    analyzer_data = container_status.get('analyzer', {})
                    
                    # If we have the components initialized but no status yet, show as running
                    if hasattr(self, 'crawler_worker') and self.crawler_worker.running:
                        status['crawler'] = {
                            'status': crawler_data.get('status', 'running'),
                            'message': crawler_data.get('message', 'Running')
                        }
                    else:
                        status['crawler'] = {
                            'status': crawler_data.get('status', 'unknown'),
                            'message': crawler_data.get('message', '')
                        }
                    
                    if hasattr(self, 'analyzer_worker') and self.analyzer_worker.running:
                        status['analyzer'] = {
                            'status': analyzer_data.get('status', 'running'),
                            'message': analyzer_data.get('message', 'Running')
                        }
                    else:
                        status['analyzer'] = {
                            'status': analyzer_data.get('status', 'unknown'),
                            'message': analyzer_data.get('message', '')
                        }
                    
                    # Get backlog status
                    backlog_data = container_status.get('backlog', {})
                    status['backlog'] = {
                        'size': backlog_data.get('size', 0),
                        'min_threshold': backlog_data.get('min_threshold', 0),
                        'target_size': backlog_data.get('target_size', 0)
                    }
                    
                    # Get stats
                    status['stats'] = container_status.get('stats', {})
            
            # Get background process status
            if hasattr(self, 'background_process') and self.background_process:
                status['background'] = {
                    'status': 'running',
                    'pid': 'N/A'  # We're not using an actual process, so no PID
                }
            
            return status
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return status
    
    def request_more_urls(self, count: int) -> bool:
        """
        Request more URLs from the crawler.
        
        Args:
            count: Number of URLs to request
            
        Returns:
            bool: True if request was sent successfully
        """
        try:
            # Check if container app is running
            if not hasattr(self, 'container_app'):
                logger.warning("No container app running to request URLs from")
                return False
            
            # Send request to container app
            self.container_app.request_more_urls(count)
            logger.info(f"Requested {count} more URLs from crawler")
            return True
        except Exception as e:
            logger.error(f"Error requesting more URLs: {e}")
            return False

    def stop_background_process(self) -> bool:
        """
        Stop the background process.
        
        Returns:
            bool: True if stopped successfully
        """
        try:
            # Just set the flag to False since we're not actually spawning a separate process
            self.background_process = False
            logger.info("Background process stopped")
            return True
        except Exception as e:
            logger.error(f"Error stopping background process: {e}")
            return False

def main():
    """Main entry point for the NDAIVI application."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='NDAIVI Crawler System')
    parser.add_argument('--interactive', '-i', action='store_true', help='Run in interactive mode')
    parser.add_argument('--crawl', '-c', metavar='URL', help='Start a crawl from the given URL')
    parser.add_argument('--max-urls', '--max_urls', '-m', type=int, default=100, help='Maximum URLs to crawl')
    parser.add_argument('--max-depth', '--max_depth', '-d', type=int, default=3, help='Maximum crawl depth')
    parser.add_argument('--background', '-b', action='store_true', help='Run in background mode')
    parser.add_argument('--target', '-t', metavar='URL', help='Target website to crawl in background mode')
    parser.add_argument('--start-detach', '--start_detach', '-s', action='store_true', help='Start the application in detached mode')
    parser.add_argument('--status', action='store_true', help='Get the status of the background process')
    parser.add_argument('--stop', action='store_true', help='Stop the background process')
    parser.add_argument('--config', metavar='FILE', help='Path to configuration file')
    parser.add_argument('--container', action='store_true', help='Start the container app for coordinating crawler and analyzer')
    parser.add_argument('--full-system', '--full_system', action='store_true', help='Start the full system including container app, crawler, and analyzer')

    args = parser.parse_args()
    
    # Create and start the application
    app = NDaiviApp()
    
    # Handle full system mode
    if args.full_system:
        logger.info("Starting full NDAIVI system with container app")
        
        # Start the application
        if not app.start_modular():
            logger.error("Failed to start NDAIVI application")
            return
            
        # Get target website from config if not specified
        target_website = args.target
        if not target_website:
            target_website = app.config.get('web_crawler', {}).get('target_website')
            if not target_website:
                logger.error("No target website specified and none found in config")
                app.stop()
                return
                
        # Start the crawler
        app.request_more_urls(100)
        
        # Keep running until stopped
        try:
            logger.info(f"Full system running, crawling {target_website}")
            print("NDAIVI system is running. Press Ctrl+C to stop.")
            print("Use 'python main.py status' to check system status.")
            
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, shutting down...")
        finally:
            app.stop()
    
    # Handle container app only mode
    elif args.container:
        logger.info("Starting container app")
        
        # Start the application
        if not app.start_modular():
            logger.error("Failed to start NDAIVI application")
            return
            
        # Keep running until stopped
        try:
            logger.info("Container app running")
            print("Container app is running. Press Ctrl+C to stop.")
            print("Use 'python main.py status' to check system status.")
            
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, shutting down...")
        finally:
            app.stop()
    
    # Handle background mode
    elif args.background:
        # Run in background mode
        if not args.target:
            logger.error("Target website is required for background mode")
            return
        
        # Start the application
        if not app.start_modular():
            logger.error("Failed to start NDAIVI application")
            return
        
        # Start the crawler
        app.request_more_urls(100)
        
        # Keep running until stopped
        try:
            logger.info(f"Running in background mode, crawling {args.target}")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, shutting down...")
        finally:
            app.stop()
    
    # Handle start-detach mode
    elif args.start_detach:
        if not app.start_modular():
            logger.error("Failed to start NDAIVI application")
            return
        
        logger.info("NDAIVI application started in detached mode")
    
    # Handle status check
    elif args.status:
        status = app.get_status()
        if status['background']['status'] == 'running':
            print(f"Background process is running with PID {status['background']['pid']}")
            print(f"Uptime: {status['background']['uptime']:.2f} seconds")
            print(f"Memory usage: {status['background']['memory_usage']:.2f} MB")
            print(f"CPU usage: {status['background']['cpu_usage']:.2f}%")
            print(f"Log file: {status['background']['log_file']}")
        else:
            print("No background process is running")
    
    # Handle stop command
    elif args.stop:
        if app.stop():
            print("Background process stopped")
        else:
            print("No background process to stop")
    
    # Handle interactive mode
    elif args.interactive:
        # Start the application in interactive mode
        cli = NDaiviCLI(app)
        try:
            cli.cmdloop()
        except KeyboardInterrupt:
            print("\nExiting NDAIVI...")
        finally:
            app.stop()
    
    # Handle direct crawl command
    elif args.crawl:
        # Start the application
        if not app.start_modular():
            logger.error("Failed to start NDAIVI application")
            return
        
        # Start the crawler
        app.request_more_urls(100)
        
        # Keep running until stopped
        try:
            logger.info(f"Crawling {args.crawl}")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received, shutting down...")
        finally:
            app.stop()
    
    # Default to interactive mode if no arguments provided
    else:
        cli = NDaiviCLI(app)
        try:
            cli.cmdloop()
        except KeyboardInterrupt:
            print("\nExiting NDAIVI...")
        finally:
            app.stop()

if __name__ == "__main__":
    main()
