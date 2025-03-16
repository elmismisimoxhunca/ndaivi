#!/usr/bin/env python3
import os
import sys
import argparse
import logging
import json
import yaml
import signal
import atexit
from dotenv import load_dotenv
from scraper.competitor_scraper import CompetitorScraper

# Remove dependency on separate batch processing and translator modules

# Load environment variables from .env file
load_dotenv()

# Global scraper instance for signal handlers
scraper_instance = None

def cleanup_resources():
    """Clean up resources before exiting"""
    logger = logging.getLogger('ndaivi')
    logger.info("Cleaning up resources before exit")
    
    global scraper_instance
    if scraper_instance is not None:
        try:
            logger.info("Closing scraper and database connections")
            scraper_instance.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

def signal_handler(signum, frame):
    """Handle termination signals"""
    signal_name = {
        signal.SIGINT: "SIGINT",
        signal.SIGTERM: "SIGTERM",
        signal.SIGTSTP: "SIGTSTP"
    }.get(signum, f"Signal {signum}")
    
    logger = logging.getLogger('ndaivi')
    logger.info(f"Received {signal_name} signal, shutting down gracefully")
    
    # Clean up resources
    cleanup_resources()
    
    # Exit with appropriate code
    if signum == signal.SIGTSTP:
        # For SIGTSTP, we need to re-raise it after cleanup
        # to actually suspend the process
        os.kill(os.getpid(), signal.SIGSTOP)
    else:
        sys.exit(0)

def setup_logging():
    """Set up logging configuration"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, 'ndaivi.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger('ndaivi')

def validate_config(config_path):
    """Validate that the config file exists and has required fields"""
    if not os.path.exists(config_path):
        return False, f"Config file not found: {config_path}"
    
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        # Check for required fields
        required_fields = [
            'target_url',
            'database.path',
            'ai_apis.anthropic.api_key',
            'ai_apis.anthropic.model',
            'ai_apis.anthropic.sonnet_model'
        ]
        
        for field in required_fields:
            parts = field.split('.')
            current = config
            for part in parts:
                if part not in current:
                    return False, f"Missing required config field: {field}"
                current = current[part]
        
        return True, "Config validation successful"
        
    except Exception as e:
        return False, f"Error validating config: {str(e)}"

def display_statistics(scraper):
    """Display statistics about the scraping process"""
    stats = scraper.get_statistics()
    
    print("\n" + "=" * 60)
    print("COMPETITOR SCRAPER STATISTICS")
    print("=" * 60)
    
    # Crawling statistics
    print("\nCRAWLING STATISTICS:")
    print(f"Total URLs in database: {stats['total_urls']}")
    print(f"Visited URLs: {stats['visited_urls']}")
    print(f"URLs remaining in queue: {stats['urls_in_queue']}")
    
    if 'start_time' in stats and 'end_time' in stats:
        duration = stats['end_time'] - stats['start_time']
        hours, remainder = divmod(duration, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"Crawling duration: {int(hours)}h {int(minutes)}m {int(seconds)}s")
    
    print(f"Pages crawled in last run: {stats.get('pages_crawled', 0)}")
    
    # Sitemap statistics from database
    try:
        # Get sitemap statistics directly from database
        from database.schema import CrawlStatus
        with scraper.db_manager.session() as session:
            # Count total pages with sitemap info
            total_pages = session.query(CrawlStatus).filter(CrawlStatus.visited == True).count()
            pages_with_title = session.query(CrawlStatus).filter(CrawlStatus.title != None).count()
            
            print(f"\nSITEMAP STATISTICS:")
            print(f"Total pages tracked: {total_pages}")
            print(f"Pages with title information: {pages_with_title}")
            
            # Count pages by depth
            from sqlalchemy import func
            depth_counts = session.query(
                CrawlStatus.depth, func.count(CrawlStatus.id)
            ).filter(CrawlStatus.visited == True).group_by(CrawlStatus.depth).all()
            
            if depth_counts:
                print("Pages by depth:")
                for depth, count in sorted(depth_counts):
                    print(f"  Depth {depth}: {count} pages")
                    
            # Option to generate sitemap files
            print("\nTo generate sitemap files, use: python cli.py generate-sitemap")
    except Exception as e:
        print(f"Error getting sitemap statistics: {str(e)}")
    
    # Data extraction statistics
    print("\nDATA EXTRACTION STATISTICS:")
    print(f"Manufacturer pages identified: {stats['manufacturer_pages']}")
    print(f"Manufacturers extracted: {stats['manufacturers']}")
    print(f"Categories extracted: {stats['categories']}")
    print(f"Manufacturers with website: {stats['manufacturers_with_website']}")
    
    # Category distribution
    if stats['manufacturers'] > 0:
        print(f"\nCATEGORY DISTRIBUTION:")
        print(f"Average categories per manufacturer: {stats['categories'] / stats['manufacturers']:.2f}")
        
        # Get top manufacturers by category count
        top_manufacturers = scraper.get_top_manufacturers_by_category_count(5)
        if top_manufacturers:
            print("\nTop manufacturers by category count:")
            for manufacturer, count in top_manufacturers:
                print(f"  {manufacturer}: {count} categories")
    
    print("=" * 60 + "\n")

def main():
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # kill command
    signal.signal(signal.SIGTSTP, signal_handler)  # Ctrl+Z
    
    # Register cleanup function to run at exit
    atexit.register(cleanup_resources)
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='NDAIVI Competitor Scraping Engine')
    parser.add_argument('--config', default='config.yaml', help='Path to configuration file')
    parser.add_argument('--validate-only', action='store_true', help='Only validate the config file and exit')
    parser.add_argument('--stats-only', action='store_true', help='Only display statistics and exit')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    # Validate configuration
    valid, message = validate_config(args.config)
    if not valid:
        logger.error(message)
        sys.exit(1)
    
    logger.info(message)
    
    if args.validate_only:
        print("Configuration validated successfully.")
        sys.exit(0)
    
    # Initialize the scraper
    try:
        global scraper_instance
        scraper_instance = CompetitorScraper(config_path=args.config)
        
        if args.stats_only:
            display_statistics(scraper_instance)
            scraper_instance.close()
            scraper_instance = None
            sys.exit(0)
        
        # Start the crawling process
        scraper_instance.start_crawling()
        
        # Display statistics
        display_statistics(scraper_instance)
        
        # Close the scraper
        scraper_instance.close()
        scraper_instance = None
        
    except Exception as e:
        logger.error(f"Error running scraper: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
