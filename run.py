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
        self.redis_manager = get_redis_manager(config)
        self.stats = {
            'crawler': {
                'urls_processed': 0,
                'urls_queued': 0,
                'domains_discovered': set()
            },
            'analyzer': {
                'pages_analyzed': 0,
                'manufacturer_pages': 0,
                'category_pages': 0,
                'other_pages': 0
            },
            'system': {
                'start_time': time.time(),
                'last_update': time.time()
            }
        }
        
        # Subscribe to stats channel
        self.stats_channel = self.redis_manager.get_channel('stats')
        self.redis_manager.subscribe(self.stats_channel, self._handle_stats)
        
        # Update interval
        self.update_interval = config.get('application', {}).get('components', {}).get('stats', {}).get('update_interval', 5)
        
        # Worker state
        self.running = False
        self.worker_thread = None
    
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
    
    def stop(self) -> None:
        """Stop the stats manager."""
        if not self.running:
            return
        
        self.running = False
        
        # Wait for worker thread to terminate
        if self.worker_thread:
            self.worker_thread.join(timeout=1.0)
        
        logger.info("Stats manager stopped")
    
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
                    last_store_time = current_time
                
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
    
    def _update_crawler_stats(self, stats: Dict[str, Any]) -> None:
        """
        Update crawler statistics.
        
        Args:
            stats: Crawler stats dictionary
        """
        crawler_stats = self.stats['crawler']
        
        # Update basic stats
        crawler_stats['urls_processed'] = stats.get('urls_processed', crawler_stats['urls_processed'])
        crawler_stats['urls_queued'] = stats.get('urls_queued', crawler_stats['urls_queued'])
        
        # Update domains set
        domains = stats.get('domains', [])
        if domains:
            crawler_stats['domains_discovered'].update(domains)
        
        # Update system timestamp
        self.stats['system']['last_update'] = time.time()
    
    def _update_analyzer_stats(self, stats: Dict[str, Any]) -> None:
        """
        Update analyzer statistics.
        
        Args:
            stats: Analyzer stats dictionary
        """
        analyzer_stats = self.stats['analyzer']
        
        # Update basic stats
        analyzer_stats['pages_analyzed'] = stats.get('pages_analyzed', analyzer_stats['pages_analyzed'])
        analyzer_stats['manufacturer_pages'] = stats.get('manufacturer_pages', analyzer_stats['manufacturer_pages'])
        analyzer_stats['category_pages'] = stats.get('category_pages', analyzer_stats['category_pages'])
        analyzer_stats['other_pages'] = stats.get('other_pages', analyzer_stats['other_pages'])
        
        # Update system timestamp
        self.stats['system']['last_update'] = time.time()
    
    def _store_stats(self) -> None:
        """Store statistics in Redis."""
        try:
            # Make a copy of stats with serializable values
            serializable_stats = self._make_serializable(self.stats)
            
            # Store in Redis
            self.redis_manager.store_data('ndaivi:stats', serializable_stats)
        except Exception as e:
            logger.error(f"Error storing stats: {e}")
    
    def _make_serializable(self, obj: Any) -> Any:
        """
        Convert an object to a JSON-serializable format.
        
        Args:
            obj: Object to convert
            
        Returns:
            Any: JSON-serializable object
        """
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(v) for v in obj]
        elif isinstance(obj, set):
            return list(obj)
        else:
            return obj
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get current statistics.
        
        Returns:
            Dict[str, Any]: Statistics dictionary
        """
        return self._make_serializable(self.stats)
    
    def format_stats(self) -> str:
        """
        Format statistics for display.
        
        Returns:
            str: Formatted statistics string
        """
        stats = self.get_stats()
        
        # Calculate runtime
        runtime = time.time() - stats['system']['start_time']
        hours, remainder = divmod(runtime, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Format the stats
        lines = [
            "=== NDAIVI System Statistics ===",
            f"Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s",
            "",
            "Crawler:",
            f"  URLs Processed: {stats['crawler']['urls_processed']}",
            f"  URLs Queued: {stats['crawler']['urls_queued']}",
            f"  Domains Discovered: {len(stats['crawler']['domains_discovered'])}",
            "",
            "Analyzer:",
            f"  Pages Analyzed: {stats['analyzer']['pages_analyzed']}",
            f"  Manufacturer Pages: {stats['analyzer']['manufacturer_pages']}",
            f"  Category Pages: {stats['analyzer']['category_pages']}",
            f"  Other Pages: {stats['analyzer']['other_pages']}",
        ]
        
        return "\n".join(lines)

class NDaiviCLI(cmd.Cmd):
    """
    Interactive command-line interface for the NDAIVI system.
    
    This class provides a simple command-line interface for controlling
    the NDAIVI system, including commands for crawling, analyzing,
    viewing statistics, and managing the system.
    """
    
    intro = """
    ███╗   ██╗██████╗  █████╗ ██╗██╗   ██╗██╗
    ████╗  ██║██╔══██╗██╔══██╗██║██║   ██║██║
    ██╔██╗ ██║██║  ██║███████║██║██║   ██║██║
    ██║╚██╗██║██║  ██║██╔══██║██║╚██╗ ██╔╝██║
    ██║ ╚████║██████╔╝██║  ██║██║ ╚████╔╝ ██║
    ╚═╝  ╚═══╝╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═══╝  ╚═╝
    
    NDAIVI Crawler System - Interactive Mode
    Type 'help' or '?' to list commands.
    """
    
    prompt = 'ndaivi> '
    
    def __init__(self, app):
        """
        Initialize the CLI with a reference to the NDAIVI app.
        
        Args:
            app: NDAIVI application instance
        """
        super().__init__()
        self.app = app
    
    def do_crawl(self, arg):
        """
        Start a crawl job.
        Usage: crawl <start_url> [max_urls] [max_depth]
        """
        args = arg.split()
        
        if not args:
            print("Error: Start URL is required")
            print("Usage: crawl <start_url> [max_urls] [max_depth]")
            return
        
        start_url = args[0]
        max_urls = int(args[1]) if len(args) > 1 else 100
        max_depth = int(args[2]) if len(args) > 2 else 3
        
        # Create command message
        command = {
            'command': 'crawl',
            'params': {
                'start_url': start_url,
                'max_urls': max_urls,
                'max_depth': max_depth,
                'job_id': f"job_{int(time.time())}"
            }
        }
        
        # Publish command
        channel = self.app.redis_manager.get_channel('crawler_commands')
        self.app.redis_manager.publish(channel, command)
        
        print(f"Started crawl job from {start_url} (max_urls={max_urls}, max_depth={max_depth})")
    
    def do_pause(self, arg):
        """Pause the crawler."""
        channel = self.app.redis_manager.get_channel('crawler_commands')
        self.app.redis_manager.publish(channel, {'command': 'pause'})
        print("Crawler paused")
    
    def do_resume(self, arg):
        """Resume the crawler."""
        channel = self.app.redis_manager.get_channel('crawler_commands')
        self.app.redis_manager.publish(channel, {'command': 'resume'})
        print("Crawler resumed")
    
    def do_stop(self, arg):
        """Stop the current crawl job."""
        channel = self.app.redis_manager.get_channel('crawler_commands')
        self.app.redis_manager.publish(channel, {'command': 'stop'})
        print("Crawler stopped")
    
    def do_analyze(self, arg):
        """
        Analyze a URL manually.
        Usage: analyze <url>
        """
        if not arg:
            print("Error: URL is required")
            print("Usage: analyze <url>")
            return
        
        url = arg.strip()
        
        # Create command message
        command = {
            'command': 'analyze',
            'params': {
                'url': url,
                'manual': True
            }
        }
        
        # Publish command
        channel = self.app.redis_manager.get_channel('analyzer_commands')
        self.app.redis_manager.publish(channel, command)
        
        print(f"Submitted URL for analysis: {url}")
    
    def do_stats(self, arg):
        """Display current system statistics."""
        stats = self.app.stats_manager.format_stats()
        print(stats)
    
    def do_status(self, arg):
        """Display current status of all components."""
        # Request status from components
        crawler_channel = self.app.redis_manager.get_channel('crawler_commands')
        analyzer_channel = self.app.redis_manager.get_channel('analyzer_commands')
        
        self.app.redis_manager.publish(crawler_channel, {'command': 'status'})
        self.app.redis_manager.publish(analyzer_channel, {'command': 'status'})
        
        print("Status request sent to all components")
        print("Check the logs for status updates")
    
    def do_exit(self, arg):
        """Exit the program."""
        print("Shutting down NDAIVI system...")
        self.app.stop()
        return True
    
    def do_quit(self, arg):
        """Exit the program."""
        return self.do_exit(arg)
    
    def do_help(self, arg):
        """List available commands with their descriptions."""
        if arg:
            # Show help for a specific command
            super().do_help(arg)
        else:
            # Show all commands
            print("\nAvailable commands:")
            print("  crawl <url> [max_urls] [max_depth] - Start a crawl job")
            print("  pause                              - Pause the crawler")
            print("  resume                             - Resume the crawler")
            print("  stop                               - Stop the current crawl job")
            print("  analyze <url>                      - Analyze a URL manually")
            print("  stats                              - Display system statistics")
            print("  status                             - Display component status")
            print("  exit                               - Exit the program")
            print("  help                               - Show this help message")
            print("\nType 'help <command>' for more information on a specific command.")

class NDaiviApp:
    """
    Main NDAIVI application that coordinates all components.
    
    This class initializes and manages all components of the NDAIVI system,
    including the crawler, analyzer, and stats manager.
    """
    
    def __init__(self):
        """Initialize the NDAIVI application."""
        # Ensure logs directory exists
        os.makedirs('logs', exist_ok=True)
        
        # Load configuration
        self.config = self._load_config()
        
        # Initialize Redis manager
        self.redis_manager = get_redis_manager(self.config)
        
        # Initialize components
        self.crawler_worker = CrawlerWorker(self.config)
        self.analyzer_worker = AnalyzerWorker(self.config)
        self.stats_manager = StatsManager(self.config)
        
        # Flag for running state
        self.running = False
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from config.yaml.
        
        Returns:
            Dict[str, Any]: Configuration dictionary
        """
        try:
            with open('config.yaml', 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return {}
    
    def _handle_signal(self, signum, frame):
        """
        Handle termination signals.
        
        Args:
            signum: Signal number
            frame: Current stack frame
        """
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
    
    def start(self) -> bool:
        """
        Start the NDAIVI application and all components.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        if self.running:
            logger.warning("NDAIVI application already running")
            return True
        
        try:
            # Start Redis manager
            if not self.redis_manager.start():
                logger.error("Failed to start Redis manager")
                return False
            
            # Start components
            if not self.crawler_worker.start():
                logger.error("Failed to start crawler worker")
                return False
            
            if not self.analyzer_worker.start():
                logger.error("Failed to start analyzer worker")
                return False
            
            if not self.stats_manager.start():
                logger.error("Failed to start stats manager")
                return False
            
            self.running = True
            logger.info("NDAIVI application started successfully")
            return True
        except Exception as e:
            logger.error(f"Error starting NDAIVI application: {e}")
            self.stop()
            return False
    
    def stop(self) -> None:
        """Stop the NDAIVI application and all components."""
        if not self.running:
            return
        
        # Stop components in reverse order
        try:
            self.stats_manager.stop()
        except Exception as e:
            logger.error(f"Error stopping stats manager: {e}")
        
        try:
            self.analyzer_worker.stop()
        except Exception as e:
            logger.error(f"Error stopping analyzer worker: {e}")
        
        try:
            self.crawler_worker.stop()
        except Exception as e:
            logger.error(f"Error stopping crawler worker: {e}")
        
        try:
            self.redis_manager.stop()
        except Exception as e:
            logger.error(f"Error stopping Redis manager: {e}")
        
        self.running = False
        logger.info("NDAIVI application stopped")
    
    def interactive_mode(self) -> None:
        """Run the application in interactive mode."""
        try:
            # Start the application
            if not self.start():
                logger.error("Failed to start NDAIVI application")
                return
            
            # Start the CLI
            cli = NDaiviCLI(self)
            cli.cmdloop()
        except Exception as e:
            logger.error(f"Error in interactive mode: {e}")
        finally:
            # Ensure application is stopped
            self.stop()

def main():
    """Main entry point for the NDAIVI application."""
    parser = argparse.ArgumentParser(description='NDAIVI Crawler System')
    parser.add_argument('--interactive', '-i', action='store_true', help='Run in interactive mode')
    parser.add_argument('--crawl', '-c', metavar='URL', help='Start a crawl from the given URL')
    parser.add_argument('--max-urls', '-m', type=int, default=100, help='Maximum URLs to crawl')
    parser.add_argument('--max-depth', '-d', type=int, default=3, help='Maximum crawl depth')
    
    args = parser.parse_args()
    
    # Create and start the application
    app = NDaiviApp()
    
    if args.interactive:
        # Run in interactive mode
        app.interactive_mode()
    elif args.crawl:
        # Run a crawl job
        if app.start():
            # Create command message
            command = {
                'command': 'crawl',
                'params': {
                    'start_url': args.crawl,
                    'max_urls': args.max_urls,
                    'max_depth': args.max_depth,
                    'job_id': f"job_{int(time.time())}"
                }
            }
            
            # Publish command
            channel = app.redis_manager.get_channel('crawler_commands')
            app.redis_manager.publish(channel, command)
            
            logger.info(f"Started crawl job from {args.crawl}")
            
            # Keep the application running
            try:
                while app.running:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Interrupted by user")
            finally:
                app.stop()
        else:
            logger.error("Failed to start NDAIVI application")
    else:
        # No action specified, show help
        parser.print_help()

if __name__ == "__main__":
    main()
