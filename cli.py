#!/usr/bin/env python3
import os
import sys
import argparse
import time
import yaml
import json
import datetime
import logging
import signal
import traceback
from scraper.competitor_scraper_new import CompetitorScraper, ConfigManager
from manufacturer_website_finder import ManufacturerWebsiteFinder
from utils import load_config, validate_database_consistency, export_database_to_json
from database.schema import init_db, Category, Manufacturer, CrawlStatus, ScraperLog, ScraperSession
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database.db_manager import get_db_manager
from database.statistics_manager import StatisticsManager

def setup_logging():
    """Set up logging configuration"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, 'cli.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger('cli')

def get_db_session(config):
    """Get a database session"""
    db_path = config['database']['path']
    engine = create_engine(f'sqlite:///{db_path}')
    Session = sessionmaker(bind=engine)
    return Session()

def list_sessions(config: dict, session_type: str = None, limit: int = 10) -> None:
    """
    List past scraper sessions with detailed information
    
    Args:
        config: Application configuration dictionary
        session_type: Type of sessions to list ('scraper' or 'finder')
        limit: Maximum number of sessions to display
    """
    session = get_db_session(config)
    
    try:
        # Build query
        query = session.query(ScraperSession)
        
        # Filter by type if specified
        if session_type in ['scraper', 'finder']:
            query = query.filter_by(session_type=session_type)
        
        # Get the most recent sessions
        sessions = query.order_by(ScraperSession.start_time.desc()).limit(limit).all()
        
        if not sessions:
            print(f"No {'scraper ' if session_type == 'scraper' else ''}{'finder ' if session_type == 'finder' else ''}sessions found in database.")
            return
        
        print("\n" + "=" * 80)
        print(f"RECENT {'SCRAPER ' if session_type == 'scraper' else ''}{'FINDER ' if session_type == 'finder' else ''}SESSIONS")
        print("=" * 80)
        
        for i, s in enumerate(sessions, 1):
            # Calculate duration if session has ended
            duration_str = "N/A"
            if s.end_time:
                duration = (s.end_time - s.start_time).total_seconds()
                hours = int(duration // 3600)
                minutes = int((duration % 3600) // 60)
                seconds = int(duration % 60)
                duration_str = f"{hours}h {minutes}m {seconds}s"
            
            # Determine what statistics to show based on session type
            stats = []
            if s.session_type == 'scraper':
                stats.append(f"URLs: {s.urls_processed}")
                stats.append(f"Manufacturers: {s.manufacturers_extracted}")
                stats.append(f"Categories: {s.categories_extracted}")
            elif s.session_type == 'finder':
                stats.append(f"Websites found: {s.websites_found}")
            stats_str = ", ".join(stats)
            
            # Format start time
            start_time = s.start_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Format error message (truncated)
            error_msg = f"Error: {s.error[:50]}..." if s.error and len(s.error) > 50 else f"Error: {s.error}" if s.error else ""
            
            # Print session details with color formatting based on status
            status_color = "\033[92m" if s.status == 'completed' else "\033[93m" if s.status == 'running' else "\033[91m"  # Green for completed, yellow for running, red for interrupted
            reset_color = "\033[0m"
            
            print(f"{i}. [Session #{s.id}] {start_time} - {status_color}{s.status.upper()}{reset_color}")
            print(f"   Type: {s.session_type.capitalize()}, Duration: {duration_str}")
            print(f"   Stats: {stats_str}")
            if error_msg:
                print(f"   {error_msg}")
            print()
    
    finally:
        session.close()

def display_statistics(config, report_type=None):
    """Display statistics about scraper and finder sessions"""
    # Get database session
    db_manager = get_db_manager(config['database']['path'])
    
    # Create a StatisticsManager instance for displaying statistics
    stats_manager = StatisticsManager('combined', db_manager)
    
    # Determine which reports to show based on report_type
    show_scraper = report_type in [None, 'scraper', 'all']
    show_finder = report_type in [None, 'finder', 'all']
    
    logger = logging.getLogger('cli')
    
    try:
        with db_manager.session() as session:
            # Get basic database statistics
            manufacturers = session.query(Manufacturer).count()
            categories = session.query(Category).count()
            manufacturers_with_website = session.query(Manufacturer).filter(Manufacturer.website != None).count()
            
            # Get latest sessions
            latest_scraper = session.query(ScraperSession).filter_by(session_type='scraper').order_by(ScraperSession.start_time.desc()).first()
            latest_finder = session.query(ScraperSession).filter_by(session_type='finder').order_by(ScraperSession.start_time.desc()).first()
            latest_session = session.query(ScraperSession).order_by(ScraperSession.start_time.desc()).first()
            
            # Get URL statistics if needed
            if show_scraper:
                total_urls = session.query(CrawlStatus).count()
                visited_urls = session.query(CrawlStatus).filter_by(visited=True).count()
                urls_in_queue = total_urls - visited_urls
            
            # Display scraper stats if requested
            if show_scraper and latest_scraper:
                print("\n" + "=" * 60)
                print("COMPETITOR SCRAPER STATISTICS")
                print("=" * 60)
                
                # Session information
                print("\n--- LATEST SCRAPER SESSION ---")
                print(f"Session ID: {latest_scraper.id}")
                print(f"Start time: {latest_scraper.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Status: {latest_scraper.status.upper()}")
                
                if latest_scraper.end_time:
                    duration = (latest_scraper.end_time - latest_scraper.start_time).total_seconds()
                    print(f"Duration: {int(duration//3600)}h {int((duration%3600)//60)}m {int(duration%60)}s")
                
                # URL statistics
                print("\n--- URL STATISTICS ---")
                print(f"Total URLs in database: {total_urls}")
                print(f"Visited URLs: {visited_urls}")
                print(f"URLs remaining in queue: {urls_in_queue}")
                
                # Extraction statistics
                print("\n--- EXTRACTION STATISTICS ---")
                print(f"Manufacturers extracted: {manufacturers}")
                print(f"Categories extracted: {categories}")
                print(f"Manufacturers with website: {manufacturers_with_website}")
                
                if manufacturers > 0:
                    website_percentage = (manufacturers_with_website / manufacturers) * 100
                    print(f"Percentage with website: {website_percentage:.2f}%")
                    print(f"Average categories per manufacturer: {categories / manufacturers:.2f}")
                
                # Display manufacturers with websites with clear separation
                print("\n--- TOP 10 MANUFACTURERS WITH WEBSITES ---")
                manufacturers_with_sites = session.query(Manufacturer).filter(Manufacturer.website != None).limit(10).all()
                for i, mfr in enumerate(manufacturers_with_sites, 1):
                    print(f"{i}. {mfr.name}")
                    print(f"   Website: {mfr.website}")
                    print(f"   Categories: {len(mfr.categories)}")
                
                print("=" * 60)
            
            # Display finder stats if requested
            if show_finder and latest_finder:
                print("\n" + "=" * 60 if show_scraper else "\n" + "=" * 60)
                print("MANUFACTURER WEBSITE FINDER STATISTICS")
                print("=" * 60)
                
                # Session information
                print("\n--- LATEST FINDER SESSION ---")
                print(f"Session ID: {latest_finder.id}")
                print(f"Start time: {latest_finder.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Status: {latest_finder.status.upper()}")
                
                if latest_finder.end_time:
                    duration = (latest_finder.end_time - latest_finder.start_time).total_seconds()
                    print(f"Duration: {int(duration//3600)}h {int((duration%3600)//60)}m {int(duration%60)}s")
                
                # Finder statistics
                print("\n--- FINDER STATISTICS ---")
                print(f"Total manufacturers: {manufacturers}")
                print(f"Manufacturers with website: {manufacturers_with_website}")
                
                if manufacturers > 0:
                    website_percentage = (manufacturers_with_website / manufacturers) * 100
                    print(f"Percentage with website: {website_percentage:.2f}%")
                
                # Display manufacturers with websites with clear separation
                print("\n--- TOP 10 MANUFACTURERS WITH WEBSITES ---")
                manufacturers_with_sites = session.query(Manufacturer).filter(Manufacturer.website != None).limit(10).all()
                for i, mfr in enumerate(manufacturers_with_sites, 1):
                    print(f"{i}. {mfr.name}")
                    print(f"   Website: {mfr.website}")
                    print(f"   Categories: {len(mfr.categories)}")
                
                print("=" * 60)
            
            # If no reports could be shown, display basic database stats
            if not (show_scraper and latest_scraper) and not (show_finder and latest_finder):
                print("\n" + "=" * 60)
                print("DATABASE STATISTICS")
                print("=" * 60)
                
                # Show latest session information if available
                if latest_session:
                    print("\n--- LATEST SESSION INFORMATION ---")
                    print(f"Session type: {latest_session.session_type.capitalize()}")
                    print(f"Start time: {latest_session.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"Status: {latest_session.status.upper()}")
                    if latest_session.end_time:
                        duration = (latest_session.end_time - latest_session.start_time).total_seconds()
                        print(f"Duration: {int(duration//3600)}h {int((duration%3600)//60)}m {int(duration%60)}s")
                
                print("\n--- GLOBAL DATABASE STATISTICS ---")
                print(f"Total manufacturers: {manufacturers}")
                print(f"Total categories: {categories}")
                print(f"Manufacturers with website: {manufacturers_with_website}")
                
                # Display top manufacturers with clear separation
                print("\n--- TOP 10 MANUFACTURERS ---")
                top_manufacturers = session.query(Manufacturer).limit(10).all()
                for i, mfr in enumerate(top_manufacturers, 1):
                    print(f"{i}. {mfr.name}")
                    print(f"   Categories: {len(mfr.categories)}")
                    print(f"   Website: {mfr.website or 'N/A'}")
                
                print("=" * 60 + "\n")
    
    except Exception as e:
        print(f"Error getting statistics: {str(e)}")
        logger.error(f"Error in display_statistics: {str(e)}")

def list_sessions(config, session_type=None, limit=10):
    """
    List past scraper sessions with their status and statistics
    
    Args:
        config: Configuration dictionary
        session_type: Optional filter by session type ('scraper' or 'finder')
        limit: Maximum number of sessions to display
    """
    session = get_db_session(config)
    
    try:
        # Build query
        query = session.query(ScraperSession)
        
        # Filter by type if specified
        if session_type in ['scraper', 'finder']:
            query = query.filter_by(session_type=session_type)
        
        # Get the most recent sessions (newest first, limited to 10 by default)
        sessions = query.order_by(ScraperSession.start_time.desc()).limit(limit).all()
        
        if not sessions:
            print(f"No {'scraper ' if session_type else ''}{'finder ' if session_type == 'finder' else ''}sessions found in database.")
            return
        
        print("\n" + "=" * 80)
        print(f"RECENT {'SCRAPER ' if session_type == 'scraper' else ''}{'FINDER ' if session_type == 'finder' else ''}SESSIONS")
        print("=" * 80)
        
        for i, s in enumerate(sessions, 1):
            # Calculate duration if session has ended
            duration_str = "N/A"
            if s.end_time and s.start_time:
                try:
                    # Ensure both timestamps are in the same timezone format
                    # If the duration is negative, it means there's a timezone issue - use absolute value
                    duration = abs((s.end_time - s.start_time).total_seconds())
                    hours = int(duration // 3600)
                    minutes = int((duration % 3600) // 60)
                    seconds = int(duration % 60)
                    duration_str = f"{hours}h {minutes}m {seconds}s"
                    logging.info(f"Calculated duration for session {s.id}: {duration_str} (start: {s.start_time}, end: {s.end_time})")
                except Exception as e:
                    duration_str = "Error calculating duration"
                    logging.error(f"Error calculating session duration: {str(e)}")
            
            # Format start time
            start_time = s.start_time.strftime('%Y-%m-%d %H:%M:%S') if s.start_time else "N/A"
            
            # Print session details with clear section header
            print(f"{i}. [ID: {s.id}] {start_time} - {s.status.upper()} - Duration: {duration_str}")
            
            # Display session-specific statistics with clear separation
            if s.session_type == 'scraper':
                print(f"   Type: Scraper")
                print(f"   URLs processed: {s.urls_processed or 0}")
                print(f"   Manufacturers extracted: {s.manufacturers_extracted or 0}")
                print(f"   Categories extracted: {s.categories_extracted or 0}")
            elif s.session_type == 'finder':
                print(f"   Type: Website Finder")
                print(f"   Searches performed: {s.urls_processed or 0}")
                print(f"   Websites visited: {s.manufacturers_extracted or 0}")
                print(f"   Websites validated: {s.websites_found or 0}")
            
            # Format error message (truncated)
            if s.error:
                print(f"   Error: {s.error[:100]}...") if len(s.error) > 100 else print(f"   Error: {s.error}")
            
            print()
        
        print("=" * 80 + "\n")
    
    except Exception as e:
        print(f"Error listing sessions: {str(e)}")
        logging.error(f"Error in list_sessions: {str(e)}")
    
    finally:
        session.close()

def list_manufacturers(config, limit=None, with_categories=False):
    """List manufacturers in the database"""
    session = get_db_session(config)
    
    try:
        query = session.query(Manufacturer)
        
        if limit:
            manufacturers = query.limit(limit).all()
        else:
            manufacturers = query.all()
        
        print("\n" + "=" * 50)
        print(f"MANUFACTURERS ({len(manufacturers)})")
        print("=" * 50)
        
        for i, mfr in enumerate(manufacturers, 1):
            print(f"{i}. {mfr.name} - Website: {mfr.website or 'N/A'}")
            
            if with_categories and mfr.categories:
                print("   Categories:")
                for cat in mfr.categories:
                    print(f"   - {cat.name}")
        
        print("=" * 50 + "\n")
        
    except Exception as e:
        print(f"Error listing manufacturers: {str(e)}")
    
    finally:
        session.close()

def display_category_translations(config, limit=None):
    """
    Display categories with their translations in all available languages.
    
    Args:
        config: Configuration dictionary
        limit: Maximum number of categories to display
    """
    db_manager = get_db_manager(config['database']['path'])
    logger = logging.getLogger('cli')
    
    try:
        with db_manager.session() as session:
            # Get categories with optional limit
            query = session.query(Category)
            if limit:
                query = query.limit(limit)
            categories = query.all()
            
            if not categories:
                print("No categories found in database.")
                return
            
            print("\n" + "=" * 80)
            print("CATEGORIES WITH TRANSLATIONS")
            print("=" * 80 + "\n")
            
            # Get available language tables
            from database.schema import _created_category_tables
            languages = list(_created_category_tables.keys())
            
            # Display each category with its translations
            for i, category in enumerate(categories, 1):
                print(f"{i}. {category.name}")
                
                # Get translations for each language
                for lang in languages:
                    table = _created_category_tables[lang]
                    translation = session.query(table).filter_by(category_id=category.id).first()
                    if translation:
                        print(f"   {lang}: {translation.category_name}")
                    else:
                        print(f"   {lang}: No translation available")
                
                # Display associated manufacturers
                if category.manufacturers:
                    print("   Manufacturers:", ", ".join(m.name for m in category.manufacturers))
                print()
                
    except Exception as e:
        logger.error(f"Error displaying category translations: {str(e)}")

def list_categories(config, limit=None, with_manufacturers=False):
    """List categories in the database"""
    session = get_db_session(config)
    
    try:
        query = session.query(Category)
        
        if limit:
            categories = query.limit(limit).all()
        else:
            categories = query.all()
        
        print("\n" + "=" * 50)
        print(f"CATEGORIES ({len(categories)})")
        print("=" * 50)
        
        for i, cat in enumerate(categories, 1):
            print(f"{i}. {cat.name} - Manufacturers: {len(cat.manufacturers)}")
            
            if with_manufacturers and cat.manufacturers:
                print("   Manufacturers:")
                for mfr in cat.manufacturers:
                    print(f"   - {mfr.name}")
        
        print("=" * 50 + "\n")
        
    except Exception as e:
        print(f"Error listing categories: {str(e)}")
    
    finally:
        session.close()

def search_database(config, term, entity_type=None):
    """Search for manufacturers or categories"""
    session = get_db_session(config)
    
    try:
        results = []
        
        if entity_type is None or entity_type.lower() == 'manufacturer':
            # Search manufacturers
            manufacturers = session.query(Manufacturer).filter(
                Manufacturer.name.ilike(f'%{term}%')
            ).all()
            
            if manufacturers:
                print("\n" + "=" * 50)
                print(f"MANUFACTURERS MATCHING '{term}' ({len(manufacturers)})")
                print("=" * 50)
                
                for i, mfr in enumerate(manufacturers, 1):
                    print(f"{i}. {mfr.name} - Website: {mfr.website or 'N/A'}")
                    print("   Categories:")
                    for cat in mfr.categories:
                        print(f"   - {cat.name}")
                
                print("=" * 50 + "\n")
        
        if entity_type is None or entity_type.lower() == 'category':
            # Search categories
            categories = session.query(Category).filter(
                Category.name.ilike(f'%{term}%')
            ).all()
            
            if categories:
                print("\n" + "=" * 50)
                print(f"CATEGORIES MATCHING '{term}' ({len(categories)})")
                print("=" * 50)
                
                for i, cat in enumerate(categories, 1):
                    print(f"{i}. {cat.name} - Manufacturers: {len(cat.manufacturers)}")
                    print("   Manufacturers:")
                    for mfr in cat.manufacturers:
                        print(f"   - {mfr.name}")
                
                print("=" * 50 + "\n")
        
        if not manufacturers and not categories:
            print(f"No results found for '{term}'")
        
    except Exception as e:
        print(f"Error searching database: {str(e)}")
    
    finally:
        session.close()

def initialize_database(config_path: str, db_path: str = None) -> str:
    """
    Initialize the database if it doesn't exist.
    
    Args:
        config_path: Path to the configuration file
        db_path: Optional custom database path
        
    Returns:
        Path to the initialized database
    """
    logger = logging.getLogger('cli')
    
    # Load config
    config = load_config(config_path)
    
    # If db_path is provided, update config
    if db_path:
        config['database']['path'] = db_path
    
    db_path = config['database']['path']
    
    # Check if database directory exists
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        logger.info(f"Creating database directory: {db_dir}")
        try:
            os.makedirs(db_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create database directory: {str(e)}")
            raise
    
    # Check if database exists
    if not os.path.exists(db_path):
        logger.info(f"Database not found at {db_path}. Initializing new database.")
        try:
            # Initialize the database
            init_db(db_path, config_path)
            logger.info(f"Database initialized successfully at {db_path}")
            print(f"✅ Database initialized successfully at {db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise
    else:
        logger.info(f"Database already exists at {db_path}")
    
    return db_path

def check_database_exists(config: dict) -> bool:
    """
    Check if the database exists.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        True if database exists, False otherwise
    """
    db_path = config['database']['path']
    return os.path.exists(db_path)

def save_process_status(config, status_data):
    """
    Save process status to a file for background tracking.
    
    Args:
        config: Configuration dictionary
        status_data: Status data to save
    """
    status_file = config['scheduling']['background']['status_file']
    status_dir = os.path.dirname(status_file)
    os.makedirs(status_dir, exist_ok=True)
    
    try:
        with open(status_file, 'w') as f:
            json.dump(status_data, f, indent=2)
    except Exception as e:
        logging.error(f"Failed to save process status: {str(e)}")

def load_process_status(config):
    """
    Load process status from file.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Status data dictionary or None if file doesn't exist
    """
    status_file = config['scheduling']['background']['status_file']
    
    if not os.path.exists(status_file):
        return None
    
    try:
        with open(status_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load process status: {str(e)}")
        return None

def is_process_running(pid):
    """
    Check if a process with the given PID is running.
    
    Args:
        pid: Process ID to check
        
    Returns:
        True if process is running, False otherwise
    """
    try:
        os.kill(pid, 0)  # Signal 0 doesn't kill the process, just checks if it exists
        return True
    except OSError:
        return False

def save_pid(config, pid):
    """
    Save process ID to file.
    
    Args:
        config: Configuration dictionary
        pid: Process ID to save
    """
    pid_file = config['scheduling']['background']['pid_file']
    pid_dir = os.path.dirname(pid_file)
    os.makedirs(pid_dir, exist_ok=True)
    
    try:
        with open(pid_file, 'w') as f:
            f.write(str(pid))
    except Exception as e:
        logging.error(f"Failed to save PID: {str(e)}")

def load_pid(config):
    """
    Load process ID from file.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Process ID or None if file doesn't exist
    """
    pid_file = config['scheduling']['background']['pid_file']
    
    if not os.path.exists(pid_file):
        return None
    
    try:
        with open(pid_file, 'r') as f:
            return int(f.read().strip())
    except Exception as e:
        logging.error(f"Failed to load PID: {str(e)}")
        return None

def run_scraper_and_finder(config, max_runtime=None, time_allocation=None, background=False):
    """
    Run the scraper and manufacturer website finder sequentially based on time allocation.
    
    Args:
        config: Configuration dictionary
        max_runtime: Maximum runtime in minutes (overrides config value)
        time_allocation: Time allocation between scraper and finder (0-100) (overrides config value)
        background: Whether to run in background mode
        
    Returns:
        Dictionary with status information
    """
    logger = logging.getLogger('cli')
    
    # Get configuration values or use overrides
    if max_runtime is None:
        max_runtime = config['scheduling']['max_runtime_minutes']
    
    if time_allocation is None:
        time_allocation = config['scheduling']['time_allocation']
    
    # Calculate time allocation
    if max_runtime > 0:
        scraper_time = (time_allocation / 100) * max_runtime
        finder_time = max_runtime - scraper_time
    else:
        scraper_time = 0  # Run without time limit
        finder_time = 0   # Run without time limit
    
    start_time = time.time()
    checkpoint_interval = config['scheduling']['checkpoint_interval'] * 60  # Convert to seconds
    last_checkpoint = start_time
    
    # Initialize status data
    status = {
        "start_time": start_time,
        "last_update": start_time,
        "max_runtime_minutes": max_runtime,
        "time_allocation": time_allocation,
        "current_stage": "competitor_scraper",
        "stages_completed": [],
        "scraper_stats": {},
        "finder_stats": {},
        "is_complete": False
    }
    
    if background:
        save_process_status(config, status)
    
    try:
        # Stage 1: Run competitor scraper
        logger.info(f"Starting competitor scraper (time allocation: {time_allocation}%)")
        status["current_stage"] = "competitor_scraper"
        
        scraper = CompetitorScraper(config_path=config['config_path'])
        
        # Set up timing for scraper
        scraper_start = time.time()
        scraper_end_time = None
        if scraper_time > 0:
            scraper_end_time = scraper_start + (scraper_time * 60)  # Convert to seconds
        
        # Run the competitor scraper with time limit
        logger.info(f"Running competitor scraper with time limit: {int(scraper_time)} minutes")
        scraper_stats = scraper.start(max_pages=config['scraper'].get('batch_size', 100))
        
        # If time limit is set, check if we need to stop early
        if scraper_time > 0 and time.time() >= scraper_end_time:
            logger.info(f"Reached maximum runtime for scraper: {int(scraper_time)} minutes")
            scraper.shutdown_requested = True
        
        # Update status with scraper statistics
        status["scraper_stats"] = scraper_stats
        status["last_update"] = time.time()
        
        # Save checkpoint
        if background:
            save_process_status(config, status)
        
        # Update status after scraper completes
        status["stages_completed"].append("competitor_scraper")
        status["scraper_stats"] = scraper.get_statistics()
        status["last_update"] = time.time()
        
        if background:
            save_process_status(config, status)
        
        # Stage 2: Run manufacturer website finder if time permits
        if max_runtime == 0 or (time.time() - start_time) / 60 < max_runtime:
            logger.info("Starting manufacturer website finder")
            status["current_stage"] = "manufacturer_website_finder"
            
            finder = ManufacturerWebsiteFinder(config_path=config['config_path'])
            
            # Set up timing for finder
            finder_start = time.time()
            finder_end_time = None
            if finder_time > 0:
                finder_end_time = finder_start + (finder_time * 60)  # Convert to seconds
            
            # Run the manufacturer website finder with time limit
            logger.info(f"Running manufacturer website finder with time limit: {int(finder_time)} minutes")
            finder_stats = finder.find_websites()
            
            # If time limit is set, check if we need to stop early
            if finder_time > 0 and time.time() >= finder_end_time:
                logger.info(f"Reached maximum runtime for finder: {int(finder_time)} minutes")
                finder.shutdown_requested = True
            
            # Update status with finder statistics
            status["finder_stats"] = finder_stats
            status["last_update"] = time.time()
            
            # Save checkpoint
            if background:
                save_process_status(config, status)
            
            # Update status after finder completes
            status["stages_completed"].append("manufacturer_website_finder")
            status["finder_stats"] = finder.stats
        
        # Mark process as complete
        status["is_complete"] = True
        status["last_update"] = time.time()
        status["total_runtime_minutes"] = (time.time() - start_time) / 60
        
        if background:
            save_process_status(config, status)
            
        return status
        
    except Exception as e:
        logger.error(f"Error in sequential run: {str(e)}")
        status["error"] = str(e)
        status["last_update"] = time.time()
        
        if background:
            save_process_status(config, status)
            
        raise

def format_time_delta(seconds):
    """
    Format time delta in a human-readable format.
    
    Args:
        seconds: Time in seconds
        
    Returns:
        Formatted time string
    """
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    
    if days > 0:
        return f"{days}d {hours}h {minutes}m {seconds}s"
    elif hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def get_last_log_entries(log_file, num_lines=10):
    """
    Get the last N lines from a log file.
    
    Args:
        log_file: Path to the log file
        num_lines: Number of lines to read from the end of the file
        
    Returns:
        List of the last N lines from the log file
    """
    if not os.path.exists(log_file):
        return []
        
    try:
        with open(log_file, 'r') as f:
            # Use deque with maxlen to efficiently get the last N lines
            from collections import deque
            last_lines = deque(maxlen=num_lines)
            for line in f:
                last_lines.append(line.strip())
            return list(last_lines)
    except Exception as e:
        logging.error(f"Error reading log file {log_file}: {str(e)}")
        return []

def update_status_for_terminated_process(config, pid, status):
    """
    Update status file for a terminated process.
    
    Args:
        config: Configuration dictionary
        pid: Process ID
        status: Status data
        
    Returns:
        Updated status data
    """
    if not status or status.get('is_complete', False):
        return status
        
    logging.info(f"Process {pid} is not running but status shows in progress. Updating status.")
    status['is_complete'] = True
    status['end_time'] = time.time()
    status['termination_reason'] = "Process terminated unexpectedly"
    save_process_status(config, status)
    logging.info("Status updated for terminated process")
    return status

def ensure_database_exists(config):
    """
    Ensure the database exists with all required tables.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        True if database exists and has all required tables, False otherwise
    """
    db_path = config['database']['path']
    logger = logging.getLogger('cli')
    
    # Create database directory if it doesn't exist
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
            logger.info(f"Created database directory {db_dir}")
        except Exception as e:
            logger.error(f"Failed to create database directory: {str(e)}")
            print(f"Error: Failed to create database directory: {str(e)}")
            return False
    
    # Check if database file exists
    db_exists = os.path.exists(db_path)
    
    # Initialize database if it doesn't exist
    if not db_exists:
        try:
            logger.info(f"Database not found. Initializing database at {db_path}")
            from database.schema import init_db
            init_db(db_path, config_path=os.path.join(os.path.dirname(__file__), 'config.yaml'))
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            print(f"Error: Failed to initialize database: {str(e)}")
            return False
    
    # Verify that the database has all required tables
    try:
        # Connect to the database
        engine = create_engine(f'sqlite:///{db_path}')
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Check if the crawl_status table exists by querying it
        try:
            # Try to query the CrawlStatus table
            from database.schema import CrawlStatus
            session.query(CrawlStatus).limit(1).all()
            logger.info("Database verification successful: crawl_status table exists")
        except Exception as e:
            logger.warning(f"Database verification failed: crawl_status table may not exist: {str(e)}")
            
            # If the table doesn't exist, reinitialize the database
            logger.info("Reinitializing database to ensure all tables exist")
            from database.schema import init_db
            init_db(db_path, config_path=os.path.join(os.path.dirname(__file__), 'config.yaml'))
            logger.info("Database reinitialized successfully")
            
        session.close()
        return True
    except Exception as e:
        logger.error(f"Failed to verify database tables: {str(e)}")
        print(f"Error: Failed to verify database tables: {str(e)}")
        return False

def display_process_status(config):
    """
    Display status of a background process.
    
    Args:
        config: Configuration dictionary
    """
    # Ensure database exists before any database operations
    ensure_database_exists(config)
    
    # Check if there's a running process
    pid = load_pid(config)
    if pid is None:
        print("No background process found.")
        return
    
    # Check if the process is still running
    is_running = is_process_running(pid)
    
    # Double-check process status using ps command
    if is_running:
        try:
            import subprocess
            result = subprocess.run(["ps", "-p", str(pid), "-o", "stat="], capture_output=True, text=True)
            if result.returncode != 0 or not result.stdout.strip() or result.stdout.strip()[0] == 'Z':
                is_running = False
                logging.debug(f"Process {pid} not running according to ps command")
        except Exception as e:
            logging.debug(f"Error checking process status with ps: {str(e)}")
    
    # Load status data
    status = load_process_status(config)
    
    # Update status for terminated processes
    if not is_running and status and not status.get('is_complete', False):
        status = update_status_for_terminated_process(config, pid, status)
    
    if status is None:
        print(f"Process ID: {pid} ({'Running' if is_running else 'Not running'})")
        print("No status data available.")
        return
    
    # Calculate runtime
    current_time = time.time()
    start_time = status.get("start_time", current_time)
    last_update = status.get("last_update", current_time)
    runtime = current_time - start_time
    time_since_update = current_time - last_update
    
    # Display status
    print("\n" + "=" * 50)
    print("NDAIVI BACKGROUND PROCESS STATUS")
    print("=" * 50)
    print(f"Process ID: {pid} ({'Running' if is_running else 'Not running'})")
    print(f"Started: {datetime.datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Last update: {datetime.datetime.fromtimestamp(last_update).strftime('%Y-%m-%d %H:%M:%S')} ({format_time_delta(time_since_update)} ago)")
    print(f"Total runtime: {format_time_delta(runtime)}")
    
    if status.get("max_runtime_minutes", 0) > 0:
        max_runtime_seconds = status["max_runtime_minutes"] * 60
        remaining = max_runtime_seconds - runtime
        if remaining > 0:
            print(f"Time remaining: {format_time_delta(remaining)}")
        else:
            print("Time limit reached")
    
    print(f"Current stage: {status.get('current_stage', 'Unknown')}")
    print(f"Stages completed: {', '.join(status.get('stages_completed', []))}")
    print(f"Status: {'Complete' if status.get('is_complete', False) else 'In progress'}")
    
    # Display scraper stats if available
    scraper_stats = status.get("scraper_stats", {})
    if scraper_stats:
        print("\nCompetitor Scraper Statistics:")
        print(f"  URLs crawled: {scraper_stats.get('urls_crawled', 0)}")
        print(f"  Manufacturer pages: {scraper_stats.get('manufacturer_pages', 0)}")
        print(f"  Manufacturers extracted: {scraper_stats.get('manufacturers_extracted', 0)}")
        print(f"  Categories extracted: {scraper_stats.get('categories_extracted', 0)}")
    
    # Display finder stats if available
    finder_stats = status.get("finder_stats", {})
    if finder_stats:
        print("\nManufacturer Website Finder Statistics:")
        print(f"  Total manufacturers: {finder_stats.get('total_manufacturers', 0)}")
        print(f"  Manufacturers with website: {finder_stats.get('manufacturers_with_website', 0)}")
        print(f"  Claude found: {finder_stats.get('claude_found', 0)}")
        print(f"  Search engine found: {finder_stats.get('search_engine_found', 0)}")
    
    # Display last log entries if available
    current_stage = status.get('current_stage', 'Unknown')
    log_file = None
    
    # Determine which log file to read based on the current stage
    if current_stage == 'scraping':
        log_file = os.path.join('logs', 'competitor_scraper.log')
    elif current_stage == 'finding_websites':
        log_file = os.path.join('logs', 'ndaivi.log')
    else:
        log_file = os.path.join('logs', 'ndaivi.log')
    
    if log_file and os.path.exists(log_file):
        print("\nRecent Log Entries:")
        log_entries = get_last_log_entries(log_file, 5)
        if log_entries:
            for entry in log_entries:
                # Extract and display only the message part of the log entry
                parts = entry.split(' - ', 2)
                if len(parts) >= 3:
                    print(f"  {parts[0]} - {parts[2]}")
                else:
                    print(f"  {entry}")
        else:
            print("  No recent log entries found")
    
    print("=" * 50 + "\n")

def interactive_mode(config):
    """
    Run the CLI in interactive mode, prompting the user for commands.
    
    Args:
        config: Configuration dictionary
    """
    logger = logging.getLogger('cli')
    logger.info("Starting interactive mode")
    
    # Ensure database exists before proceeding
    try:
        if not check_database_exists(config):
            print("Database does not exist. Initializing...")
            db_path = initialize_database(config['config_path'])
            print(f"Database initialized at {db_path}")
    except Exception as e:
        print(f"Warning: {str(e)}")
        print("Continuing with interactive mode. Some commands may not work without a valid database.")
    
    print("\n" + "=" * 60)
    print("NDAIVI INTERACTIVE MODE")
    print("=" * 60)
    print("Type 'help' for available commands or 'exit' to quit.")
    
    while True:
        try:
            command = input("\nndaivi> ").strip().lower()
            
            if command == 'exit' or command == 'quit':
                print("Exiting NDAIVI CLI...")
                break
                
            elif command == 'help':
                print("\nAvailable commands:")
                print("  scrape [--max-pages N] [--background]\t\tStart the competitor scraper")
                print("  find-websites [--limit N]\t\t\tFind manufacturer websites")
                print("  run-all [--time-allocation N] [--max-runtime N] [--background]\tRun scraper and finder sequentially")
                print("  status\t\t\t\t\tCheck status of background process")
                print("  stop\t\t\t\t\tStop background process")
                print("  stats\t\t\t\t\tDisplay global database statistics")
                print("  sessions [--limit N]\t\t\t\tList detailed statistics for past sessions")
                print("  manufacturers [--limit N] [--with-categories]\tList manufacturers")
                print("  categories [--limit N] [--with-manufacturers]\tList categories")
                print("  translations [--limit N]\t\t\tShow categories with translations")
                print("  search <term> [--type manufacturer|category]\t\tSearch the database")
                print("  export [--output PATH]\t\t\tExport database to JSON")
                print("  validate\t\t\t\t\tValidate database consistency")
                print("  help\t\t\t\t\tShow this help message")
                print("  exit\t\t\t\t\tExit the program")
            
            elif command.startswith('stats') or command.startswith('global-stats'):
                args = command.split()[1:] if len(command.split()) > 1 else []
                report_type = None
                
                # Parse arguments
                i = 0
                while i < len(args):
                    if args[i] == '--type' and i + 1 < len(args):
                        if args[i + 1] in ['scraper', 'finder']:
                            report_type = args[i + 1]
                            i += 2
                        else:
                            print(f"Error: Invalid type '{args[i + 1]}'. Use 'scraper' or 'finder'.")
                            break
                    else:
                        print(f"Unrecognized argument: {args[i]}")
                        i += 1
                
                display_statistics(config, report_type=report_type)
                
            elif command.startswith('sessions') or command.startswith('session-stats'):
                args = command.split()[1:] if len(command.split()) > 1 else []
                session_type = None
                limit = 10  # Default limit
                
                # Parse arguments
                i = 0
                while i < len(args):
                    if args[i] == '--type' and i + 1 < len(args):
                        if args[i + 1] in ['scraper', 'finder']:
                            session_type = args[i + 1]
                            i += 2
                        else:
                            print(f"Error: Invalid type '{args[i + 1]}'. Use 'scraper' or 'finder'.")
                            break
                    elif args[i] == '--limit' and i + 1 < len(args):
                        try:
                            limit = int(args[i + 1])
                            i += 2
                        except ValueError:
                            print("Error: --limit requires a number")
                            break
                    else:
                        print(f"Unrecognized argument: {args[i]}")
                        i += 1
                
                list_sessions(config, session_type, limit)
                
            elif command.startswith('manufacturers') or command.startswith('list-manufacturers'):
                args = command.split()[1:] if len(command.split()) > 1 else []
                limit = None
                with_categories = False
                
                # Parse arguments
                i = 0
                while i < len(args):
                    if args[i] == '--limit' and i + 1 < len(args):
                        try:
                            limit = int(args[i + 1])
                            i += 2
                        except ValueError:
                            print("Error: --limit requires a number")
                            break
                    elif args[i] == '--with-categories':
                        with_categories = True
                        i += 1
                    else:
                        print(f"Unrecognized argument: {args[i]}")
                        i += 1
                
                list_manufacturers(config, limit, with_categories)
                
            elif command.startswith('categories') or command.startswith('list-categories'):
                args = command.split()[1:] if len(command.split()) > 1 else []
                limit = None
                with_manufacturers = False
                
                # Parse arguments
                i = 0
                while i < len(args):
                    if args[i] == '--limit' and i + 1 < len(args):
                        try:
                            limit = int(args[i + 1])
                            i += 2
                        except ValueError:
                            print("Error: --limit requires a number")
                            break
                    elif args[i] == '--with-manufacturers':
                        with_manufacturers = True
                        i += 1
                    else:
                        print(f"Unrecognized argument: {args[i]}")
                        i += 1
                
                list_categories(config, limit, with_manufacturers)
                
            elif command.startswith('translations') or command.startswith('show-translations'):
                args = command.split()[1:] if len(command.split()) > 1 else []
                limit = None
                
                # Parse arguments
                i = 0
                while i < len(args):
                    if args[i] == '--limit' and i + 1 < len(args):
                        try:
                            limit = int(args[i + 1])
                            i += 2
                        except ValueError:
                            print("Error: --limit requires a number")
                            break
                    else:
                        print(f"Unrecognized argument: {args[i]}")
                        i += 1
                
                display_category_translations(config, limit)
                
            elif command.startswith('scrape'):
                args = command.split()[1:] if len(command.split()) > 1 else []
                max_pages = None
                background = False
                debug = False
                
                # Parse arguments
                i = 0
                while i < len(args):
                    if args[i] in ['--max-pages', '--max_pages'] and i + 1 < len(args):
                        try:
                            max_pages = int(args[i + 1])
                            i += 2
                        except ValueError:
                            print("Error: --max-pages requires a number")
                            break
                    elif args[i] == '--background':
                        background = True
                        i += 1
                    elif args[i] == '--debug':
                        debug = True
                        i += 1
                    else:
                        print(f"Unrecognized argument: {args[i]}")
                        i += 1
                
                # Start the scraper
                try:
                    print(f"Starting competitor scraper{' in background' if background else ''}...")
                    
                    # Initialize the scraper with just the config path
                    scraper = CompetitorScraper(config_path=config['config_path'])
                    
                    # Enable debug logging if requested
                    if debug:
                        scraper.logger.setLevel(logging.DEBUG)
                        print("Debug mode enabled - detailed logs will be shown")
                    
                    if background:
                        # Fork a new process for background operation using double-fork pattern
                        try:
                            # First fork to create child process
                            pid = os.fork()
                            if pid > 0:
                                # Parent process
                                print(f"Scraper started in background with PID {pid}")
                                save_pid(config, pid)
                            else:
                                # Child process - create a second fork to fully detach
                                pid2 = os.fork()
                                if pid2 > 0:
                                    # Exit first child immediately
                                    os._exit(0)
                                else:
                                    # Grandchild process (now fully detached)
                                    try:
                                        # Detach from terminal
                                        os.setsid()
                                        
                                        # Close file descriptors
                                        os.close(0)
                                        os.close(1)
                                        os.close(2)
                                        
                                        # Redirect standard file descriptors
                                        sys.stdin = open('/dev/null', 'r')
                                        sys.stdout = open(os.path.join('logs', 'scraper_background.log'), 'a+')
                                        sys.stderr = open(os.path.join('logs', 'scraper_background_error.log'), 'a+')
                                        
                                        # Start scraper
                                        status_data = {
                                            'command': 'scrape',
                                            'start_time': datetime.datetime.now().isoformat(),
                                            'status': 'running',
                                            'pid': os.getpid(),
                                            'max_pages': max_pages
                                        }
                                        save_process_status(config, status_data)
                                        
                                        # Run scraper
                                        if max_pages:
                                            scraper.start(max_pages=max_pages)
                                        else:
                                            scraper.start()
                                        
                                        # Update status
                                        status_data['status'] = 'completed'
                                        status_data['end_time'] = datetime.datetime.now().isoformat()
                                        save_process_status(config, status_data)
                                        
                                    except Exception as e:
                                        # Log error
                                        with open(os.path.join('logs', 'scraper_background_error.log'), 'a+') as f:
                                            f.write(f"{datetime.datetime.now().isoformat()} - Error: {str(e)}\n")
                                        
                                        # Update status
                                        status_data = load_process_status(config)
                                        if status_data:
                                            status_data['status'] = 'error'
                                            status_data['error'] = str(e)
                                            status_data['end_time'] = datetime.datetime.now().isoformat()
                                            save_process_status(config, status_data)
                                    
                                    # Exit grandchild
                                    os._exit(0)
                        except OSError as e:
                            print(f"Error starting background process: {str(e)}")
                    else:
                        # Run in foreground
                        print("Starting scraper in foreground mode...")
                        if max_pages:
                            print(f"Maximum pages to process: {max_pages}")
                            scraper.start(max_pages=max_pages)
                        else:
                            scraper.start()
                        print("Scraper completed successfully")
                except Exception as e:
                    print(f"Error starting scraper: {str(e)}")
                    logger.error(f"Error starting scraper: {str(e)}")
                    logger.error(traceback.format_exc())
                
            elif command.startswith('find-websites'):
                args = command.split()[1:] if len(command.split()) > 1 else []
                limit = None
                background = False
                
                # Parse arguments
                i = 0
                while i < len(args):
                    if args[i] == '--limit' and i + 1 < len(args):
                        try:
                            limit = int(args[i + 1])
                            i += 2
                        except ValueError:
                            print("Error: --limit requires a number")
                            break
                    elif args[i] == '--background':
                        background = True
                        i += 1
                    else:
                        print(f"Unrecognized argument: {args[i]}")
                        i += 1
                
                try:
                    print(f"Starting manufacturer website finder{' in background' if background else ''}...")
                    finder = ManufacturerWebsiteFinder(config_path=config['config_path'])
                    
                    if background:
                        # Fork a new process for background operation using double-fork pattern
                        try:
                            # First fork to create child process
                            pid = os.fork()
                            if pid > 0:
                                # Parent process
                                print(f"Website finder started in background with PID {pid}")
                                save_pid(config, pid)
                            else:
                                # Child process - create a second fork to fully detach
                                pid2 = os.fork()
                                if pid2 > 0:
                                    # Exit first child immediately
                                    os._exit(0)
                                else:
                                    # Grandchild process (now fully detached)
                                    try:
                                        # Detach from terminal
                                        os.setsid()
                                        
                                        # Close file descriptors
                                        os.close(0)
                                        os.close(1)
                                        os.close(2)
                                        
                                        # Redirect standard file descriptors
                                        sys.stdin = open('/dev/null', 'r')
                                        sys.stdout = open(os.path.join('logs', 'finder_background.log'), 'a+')
                                        sys.stderr = open(os.path.join('logs', 'finder_background_error.log'), 'a+')
                                        
                                        # Start finder
                                        status_data = {
                                            'command': 'find-websites',
                                            'start_time': datetime.datetime.now().isoformat(),
                                            'status': 'running',
                                            'pid': os.getpid(),
                                            'limit': limit
                                        }
                                        save_process_status(config, status_data)
                                        
                                        # Run finder
                                        finder.find_websites(limit=limit)
                                        
                                        # Update status
                                        status_data['status'] = 'completed'
                                        status_data['end_time'] = datetime.datetime.now().isoformat()
                                        save_process_status(config, status_data)
                                        
                                    except Exception as e:
                                        # Log error
                                        with open(os.path.join('logs', 'finder_background_error.log'), 'a+') as f:
                                            f.write(f"{datetime.datetime.now().isoformat()} - Error: {str(e)}\n")
                                        
                                        # Update status
                                        status_data = load_process_status(config)
                                        if status_data:
                                            status_data['status'] = 'error'
                                            status_data['error'] = str(e)
                                            status_data['end_time'] = datetime.datetime.now().isoformat()
                                            save_process_status(config, status_data)
                                    
                                    # Exit grandchild
                                    os._exit(0)
                        except OSError as e:
                            print(f"Error starting background process: {str(e)}")
                    else:
                        # Run in foreground
                        finder.find_websites(limit=limit)
                        print("Website finder completed successfully")
                except Exception as e:
                    print(f"Error starting website finder: {str(e)}")
                    logger.error(f"Error in website finder: {str(e)}")
                
            elif command.startswith('run-all'):
                args = command.split()[1:] if len(command.split()) > 1 else []
                time_allocation = None
                max_runtime = None
                background = False
                
                # Parse arguments
                i = 0
                while i < len(args):
                    if args[i] == '--time-allocation' and i + 1 < len(args):
                        try:
                            time_allocation = int(args[i + 1])
                            if time_allocation < 0 or time_allocation > 100:
                                print("Error: --time-allocation must be between 0 and 100")
                                break
                            i += 2
                        except ValueError:
                            print("Error: --time-allocation requires a number")
                            break
                    elif args[i] == '--max-runtime' and i + 1 < len(args):
                        try:
                            max_runtime = int(args[i + 1])
                            i += 2
                        except ValueError:
                            print("Error: --max-runtime requires a number")
                            break
                    elif args[i] == '--background':
                        background = True
                        i += 1
                    else:
                        print(f"Unrecognized argument: {args[i]}")
                        i += 1
                
                try:
                    print(f"Starting sequential run of scraper and website finder{' in background' if background else ''}...")
                    
                    if background:
                        # Fork a new process for background operation using double-fork pattern
                        try:
                            # First fork to create child process
                            pid = os.fork()
                            if pid > 0:
                                # Parent process
                                print(f"Sequential run started in background with PID {pid}")
                                save_pid(config, pid)
                            else:
                                # Child process - create a second fork to fully detach
                                pid2 = os.fork()
                                if pid2 > 0:
                                    # Exit first child immediately
                                    os._exit(0)
                                else:
                                    # Grandchild process (now fully detached)
                                    try:
                                        # Detach from terminal
                                        os.setsid()
                                        
                                        # Close file descriptors
                                        os.close(0)
                                        os.close(1)
                                        os.close(2)
                                        
                                        # Redirect standard file descriptors
                                        sys.stdin = open('/dev/null', 'r')
                                        sys.stdout = open(os.path.join('logs', 'run_all_background.log'), 'a+')
                                        sys.stderr = open(os.path.join('logs', 'run_all_background_error.log'), 'a+')
                                        
                                        # Start sequential run
                                        status_data = {
                                            'command': 'run-all',
                                            'start_time': datetime.datetime.now().isoformat(),
                                            'status': 'running',
                                            'pid': os.getpid(),
                                            'max_runtime': max_runtime,
                                            'time_allocation': time_allocation
                                        }
                                        save_process_status(config, status_data)
                                        
                                        # Run scraper and finder sequentially
                                        run_scraper_and_finder(
                                            config,
                                            max_runtime=max_runtime,
                                            time_allocation=time_allocation,
                                            background=True
                                        )
                                        
                                        # Update status
                                        status_data['status'] = 'completed'
                                        status_data['end_time'] = datetime.datetime.now().isoformat()
                                        save_process_status(config, status_data)
                                        
                                    except Exception as e:
                                        # Log error
                                        with open(os.path.join('logs', 'run_all_background_error.log'), 'a+') as f:
                                            f.write(f"{datetime.datetime.now().isoformat()} - Error: {str(e)}\n")
                                        
                                        # Update status
                                        status_data = load_process_status(config)
                                        if status_data:
                                            status_data['status'] = 'error'
                                            status_data['error'] = str(e)
                                            status_data['end_time'] = datetime.datetime.now().isoformat()
                                            save_process_status(config, status_data)
                                    
                                    # Exit grandchild
                                    os._exit(0)
                        except OSError as e:
                            print(f"Error starting background process: {str(e)}")
                    else:
                        # Run in foreground
                        run_scraper_and_finder(
                            config,
                            max_runtime=max_runtime,
                            time_allocation=time_allocation,
                            background=False
                        )
                        print("Sequential run completed successfully")
                except Exception as e:
                    print(f"Error in sequential run: {str(e)}")
                    logger.error(f"Error in sequential run: {str(e)}")
                
            elif command == 'status':
                try:
                    display_process_status(config)
                except Exception as e:
                    print(f"Error checking process status: {str(e)}")
                    logger.error(f"Error checking process status: {str(e)}")
            
            elif command == 'stop':
                try:
                    pid = load_pid(config)
                    if pid and is_process_running(pid):
                        try:
                            os.kill(pid, signal.SIGTERM)
                            print(f"Sent termination signal to process {pid}")
                            
                            # Update status file
                            status_data = load_process_status(config)
                            if status_data:
                                status_data['status'] = 'stopped'
                                status_data['end_time'] = datetime.datetime.now().isoformat()
                                save_process_status(config, status_data)
                        except Exception as e:
                            print(f"Error stopping process: {str(e)}")
                            logger.error(f"Error stopping process: {str(e)}")
                    else:
                        print("No running background process found")
                except Exception as e:
                    print(f"Error stopping process: {str(e)}")
                    logger.error(f"Error stopping process: {str(e)}")
            
            else:
                print(f"Unrecognized command: {command}")
        
        except Exception as e:
            print(f"Error in interactive mode: {str(e)}")
            logger.error(f"Error in interactive_mode: {str(e)}")

def main():
    print("Starting CLI main function...")
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='NDAIVI Competitor Scraper CLI')
    parser.add_argument('--config', default='config.yaml', help='Path to configuration file')
    parser.add_argument('--non-interactive', '-n', action='store_true', help='Run in non-interactive mode (command line only)')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Add subparsers for each command
    print("Setting up subparsers...")
    
    # Scraper command
    scraper_parser = subparsers.add_parser('scrape', help='Run the competitor scraper')
    scraper_parser.add_argument('--max-pages', type=int, help='Maximum number of pages to scrape')
    scraper_parser.add_argument('--seed-urls', nargs='+', help='Seed URLs to start scraping from')
    scraper_parser.add_argument('--background', action='store_true', help='Run in background mode')
    
    # Website finder command
    finder_parser = subparsers.add_parser('find-websites', help='Run the manufacturer website finder')
    finder_parser.add_argument('--limit', type=int, help='Limit the number of manufacturers to process')
    finder_parser.add_argument('--background', action='store_true', help='Run in background mode')
    
    # Run all command
    run_all_parser = subparsers.add_parser('run-all', help='Run scraper and website finder sequentially')
    run_all_parser.add_argument('--time-allocation', type=int, help='Time allocation between scraper and finder (0-100)')
    run_all_parser.add_argument('--max-runtime', type=int, help='Maximum runtime in minutes')
    run_all_parser.add_argument('--background', action='store_true', help='Run in background mode')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Display global statistics')
    stats_parser.add_argument('--type', choices=['scraper', 'finder'], help='Type of statistics to display')
    
    # Sessions command
    sessions_parser = subparsers.add_parser('sessions', help='List past scraper sessions')
    sessions_parser.add_argument('--type', choices=['scraper', 'finder'], help='Type of sessions to list')
    sessions_parser.add_argument('--limit', type=int, default=10, help='Maximum number of sessions to display')
    
    # Manufacturers command
    manufacturers_parser = subparsers.add_parser('manufacturers', help='List manufacturers in the database')
    manufacturers_parser.add_argument('--limit', type=int, help='Maximum number of manufacturers to display')
    manufacturers_parser.add_argument('--with-categories', action='store_true', help='Include categories for each manufacturer')
    
    # Categories command
    categories_parser = subparsers.add_parser('categories', help='List categories in the database')
    categories_parser.add_argument('--limit', type=int, help='Maximum number of categories to display')
    categories_parser.add_argument('--with-manufacturers', action='store_true', help='Include manufacturers for each category')
    
    # Translations command
    translations_parser = subparsers.add_parser('translations', help='Display category translations')
    translations_parser.add_argument('--limit', type=int, help='Maximum number of categories to display')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search for manufacturers or categories')
    search_parser.add_argument('term', help='Search term')
    search_parser.add_argument('--type', choices=['manufacturer', 'category'], help='Type of entity to search for')
    
    # Status command
    subparsers.add_parser('status', help='Display status of background processes')
    
    # Stop command
    subparsers.add_parser('stop', help='Stop a running background process')
    
    # Interactive mode command
    subparsers.add_parser('interactive', help='Run in interactive mode')
    
    print("Parsing arguments...")
    args = parser.parse_args()
    
    # Setup logging
    print("Setting up logging...")
    logger = setup_logging()
    
    try:
        # Load configuration
        print("Loading configuration...")
        config = load_config(args.config)
        
        # Store config path in config dict for use in other functions
        config['config_path'] = args.config
        print("Configuration loaded successfully")
        
        # Check for non-interactive mode, otherwise default to interactive mode
        if args.non_interactive:
            print("Running in non-interactive mode")
            if args.command == 'scrape':
                # Run the scraper
                max_pages = args.max_pages
                seed_urls = args.seed_urls
                background = args.background if hasattr(args, 'background') else False
                
                try:
                    # Ensure database exists
                    if not ensure_database_exists(config):
                        print("Error: Database initialization failed")
                        return
                    
                    # Run the scraper
                    scraper = CompetitorScraper(config_path=config['config_path'])
                    
                    if background:
                        # Run in background
                        pid = os.fork()
                        if pid > 0:
                            # Parent process
                            print(f"Scraper started in background with PID {pid}")
                            save_pid(config, pid)
                        else:
                            # Child process
                            try:
                                # Start scraper
                                scraper.start(seed_urls=seed_urls, max_pages=max_pages)
                                os._exit(0)
                            except Exception as e:
                                print(f"Error in background scraper: {str(e)}")
                                os._exit(1)
                    else:
                        # Run in foreground
                        scraper.start(seed_urls=seed_urls, max_pages=max_pages)
                        print("Scraper completed successfully")
                except Exception as e:
                    print(f"Error starting scraper: {str(e)}")
                    logger.error(f"Error starting scraper: {str(e)}")
            
            elif args.command == 'find-websites':
                # Run the website finder
                limit = args.limit
                background = args.background if hasattr(args, 'background') else False
                
                try:
                    # Ensure database exists
                    if not ensure_database_exists(config):
                        print("Error: Database initialization failed")
                        return
                    
                    # Run the finder
                    finder = ManufacturerWebsiteFinder(config_path=config['config_path'])
                    
                    if background:
                        # Run in background
                        pid = os.fork()
                        if pid > 0:
                            # Parent process
                            print(f"Website finder started in background with PID {pid}")
                            save_pid(config, pid)
                        else:
                            # Child process
                            try:
                                # Start finder
                                finder.find_websites(limit=limit)
                                os._exit(0)
                            except Exception as e:
                                print(f"Error in background finder: {str(e)}")
                                os._exit(1)
                    else:
                        # Run in foreground
                        finder.find_websites(limit=limit)
                        print("Website finder completed successfully")
                except Exception as e:
                    print(f"Error starting website finder: {str(e)}")
                    logger.error(f"Error starting website finder: {str(e)}")
            
            elif args.command == 'run-all':
                # Run scraper and finder sequentially
                max_runtime = args.max_runtime
                time_allocation = args.time_allocation
                background = args.background if hasattr(args, 'background') else False
                
                try:
                    # Ensure database exists
                    if not ensure_database_exists(config):
                        print("Error: Database initialization failed")
                        return
                    
                    if background:
                        # Run in background
                        pid = os.fork()
                        if pid > 0:
                            # Parent process
                            print(f"Sequential run started in background with PID {pid}")
                            save_pid(config, pid)
                        else:
                            # Child process
                            try:
                                # Start sequential run
                                run_scraper_and_finder(
                                    config,
                                    max_runtime=max_runtime,
                                    time_allocation=time_allocation,
                                    background=True
                                )
                                os._exit(0)
                            except Exception as e:
                                print(f"Error in background sequential run: {str(e)}")
                                os._exit(1)
                    else:
                        # Run in foreground
                        run_scraper_and_finder(
                            config,
                            max_runtime=max_runtime,
                            time_allocation=time_allocation,
                            background=False
                        )
                        print("Sequential run completed successfully")
                except Exception as e:
                    print(f"Error in sequential run: {str(e)}")
                    logger.error(f"Error in sequential run: {str(e)}")
            
            elif args.command == 'stats':
                # Display statistics
                report_type = args.type if hasattr(args, 'type') else None
                display_statistics(config, report_type=report_type)
            
            elif args.command == 'sessions':
                # List sessions
                session_type = args.type if hasattr(args, 'type') else None
                limit = args.limit if hasattr(args, 'limit') else 10
                list_sessions(config, session_type=session_type, limit=limit)
            
            elif args.command == 'manufacturers':
                # List manufacturers
                limit = args.limit if hasattr(args, 'limit') else None
                with_categories = args.with_categories if hasattr(args, 'with_categories') else False
                list_manufacturers(config, limit=limit, with_categories=with_categories)
            
            elif args.command == 'categories':
                # List categories
                limit = args.limit if hasattr(args, 'limit') else None
                with_manufacturers = args.with_manufacturers if hasattr(args, 'with_manufacturers') else False
                list_categories(config, limit=limit, with_manufacturers=with_manufacturers)
            
            elif args.command == 'translations':
                # Display translations
                limit = args.limit if hasattr(args, 'limit') else None
                display_category_translations(config, limit=limit)
            
            elif args.command == 'search':
                # Search database
                term = args.term
                entity_type = args.type if hasattr(args, 'type') else None
                search_database(config, term, entity_type=entity_type)
            
            elif args.command == 'status':
                # Display process status
                display_process_status(config)
            
            elif args.command == 'stop':
                # Stop background process
                pid = load_pid(config)
                if pid and is_process_running(pid):
                    try:
                        os.kill(pid, signal.SIGTERM)
                        print(f"Sent termination signal to process {pid}")
                        
                        # Update status file
                        status_data = load_process_status(config)
                        if status_data:
                            status_data['status'] = 'stopped'
                            status_data['end_time'] = datetime.datetime.now().isoformat()
                            save_process_status(config, status_data)
                    except Exception as e:
                        print(f"Error stopping process: {str(e)}")
                        logger.error(f"Error stopping process: {str(e)}")
                else:
                    print("No running background process found")
            
            elif args.command == 'interactive':
                # Run in interactive mode
                print("Starting interactive mode...")
                interactive_mode(config)
            
            else:
                if args.command:
                    print(f"Unknown command: {args.command}")
                else:
                    print("No command specified")
                parser.print_help()
        
        elif not args.command:
            # If no command is specified, go to interactive mode by default
            print("Starting interactive mode...")
            interactive_mode(config)
            return
        else:
            # If a command is specified but we're not in non-interactive mode, still use interactive
            print("Command specified but running in interactive mode...")
            interactive_mode(config)
            return
    except Exception as e:
        print(f"Error in main function: {str(e)}")
        if logger:
            logger.error(f"Error in main function: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
