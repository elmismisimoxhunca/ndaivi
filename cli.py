#!/usr/bin/env python3
import os
import sys
import argparse
import time
import yaml
import json
import datetime
import logging
from scraper.competitor_scraper import CompetitorScraper
from manufacturer_website_finder import ManufacturerWebsiteFinder
from utils import load_config, validate_database_consistency, export_database_to_json
from database.schema import init_db, Category, Manufacturer, CrawlStatus, ScraperLog, ScraperSession
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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
    """
    Display statistics about the database
    
    Args:
        config: Configuration dictionary
        report_type: Optional, type of report to display ('scraper', 'finder', or None for combined)
    """
    session = get_db_session(config)
    logger = logging.getLogger('cli')
    
    try:
        # Common statistics
        manufacturers = session.query(Manufacturer).count()
        categories = session.query(Category).count()
        manufacturers_with_website = session.query(Manufacturer).filter(Manufacturer.website != None).count()
        
        # Get most recent session of each type if available
        latest_scraper = session.query(ScraperSession).filter_by(session_type='scraper').order_by(ScraperSession.start_time.desc()).first()
        latest_finder = session.query(ScraperSession).filter_by(session_type='finder').order_by(ScraperSession.start_time.desc()).first()
        
        # Determine which report(s) to show
        show_scraper = report_type in [None, 'scraper'] and latest_scraper is not None
        show_finder = report_type in [None, 'finder'] and latest_finder is not None
        
        # Display scraper stats if requested
        if show_scraper:
            total_urls = session.query(CrawlStatus).count()
            visited_urls = session.query(CrawlStatus).filter_by(visited=True).count()
            manufacturer_pages = session.query(CrawlStatus).filter_by(is_manufacturer_page=True).count()
            
            print("\n" + "=" * 60)
            print("COMPETITOR SCRAPER STATISTICS")
            print("=" * 60)
            
            # SESSION STATISTICS SECTION
            print("\n--- SESSION STATISTICS ---")
            print(f"Last session: {latest_scraper.start_time.strftime('%Y-%m-%d %H:%M:%S')} ({latest_scraper.status})")
            if latest_scraper.end_time and latest_scraper.start_time:
                try:
                    duration = (latest_scraper.end_time - latest_scraper.start_time).total_seconds()
                    print(f"Duration: {int(duration//3600)}h {int((duration%3600)//60)}m {int(duration%60)}s")
                except Exception as e:
                    print("Duration: Error calculating duration")
                    logger.error(f"Error calculating session duration: {str(e)}")
            
            print(f"URLs processed in session: {latest_scraper.urls_processed}")
            print(f"Manufacturers extracted in session: {latest_scraper.manufacturers_extracted}")
            print(f"Categories extracted in session: {latest_scraper.categories_extracted}")
            
            # GLOBAL STATISTICS SECTION
            print("\n--- GLOBAL DATABASE STATISTICS ---")
            print(f"Total URLs in database: {total_urls}")
            print(f"Visited URLs: {visited_urls}")
            print(f"Manufacturer pages identified: {manufacturer_pages}")
            print(f"Total manufacturers in database: {manufacturers}")
            print(f"Total categories in database: {categories}")
            print(f"Manufacturers with website: {manufacturers_with_website}")
            
            # Display top manufacturers with clear separation between data types
            print("\n--- TOP 10 MANUFACTURERS ---")
            top_manufacturers = session.query(Manufacturer).limit(10).all()
            for i, mfr in enumerate(top_manufacturers, 1):
                print(f"{i}. {mfr.name}")
                print(f"   Categories: {len(mfr.categories)}")
                print(f"   Website: {mfr.website or 'N/A'}")
            
            # Display top categories with clear separation
            print("\n--- TOP 10 CATEGORIES ---")
            top_categories = session.query(Category).limit(10).all()
            for i, cat in enumerate(top_categories, 1):
                # Each category should belong to exactly one manufacturer
                manufacturer_name = cat.manufacturers[0].name if cat.manufacturers else "Unknown"
                print(f"{i}. {cat.name}")
                print(f"   Manufacturer: {manufacturer_name}")
            
            print("=" * 60)
        
        # Display finder stats if requested
        if show_finder:
            print("\n" + "=" * 60 if show_scraper else "\n" + "=" * 60)
            print("MANUFACTURER WEBSITE FINDER STATISTICS")
            print("=" * 60)
            
            # SESSION STATISTICS SECTION
            print("\n--- SESSION STATISTICS ---")
            print(f"Last session: {latest_finder.start_time.strftime('%Y-%m-%d %H:%M:%S')} ({latest_finder.status})")
            if latest_finder.end_time and latest_finder.start_time:
                try:
                    duration = (latest_finder.end_time - latest_finder.start_time).total_seconds()
                    print(f"Duration: {int(duration//3600)}h {int((duration%3600)//60)}m {int(duration%60)}s")
                except Exception as e:
                    print("Duration: Error calculating duration")
                    logger.error(f"Error calculating session duration: {str(e)}")
            
            print(f"Websites found in session: {latest_finder.websites_found}")
            
            # GLOBAL STATISTICS SECTION
            print("\n--- GLOBAL DATABASE STATISTICS ---")
            print(f"Total manufacturers in database: {manufacturers}")
            print(f"Manufacturers with website: {manufacturers_with_website}")
            
            # Display manufacturers with websites with clear separation
            print("\n--- TOP 10 MANUFACTURERS WITH WEBSITES ---")
            manufacturers_with_sites = session.query(Manufacturer).filter(Manufacturer.website != None).limit(10).all()
            for i, mfr in enumerate(manufacturers_with_sites, 1):
                print(f"{i}. {mfr.name}")
                print(f"   Website: {mfr.website}")
                print(f"   Categories: {len(mfr.categories)}")
            
            print("=" * 60)
        
        # If no reports could be shown, display basic database stats
        if not (show_scraper or show_finder):
            print("\n" + "=" * 60)
            print("DATABASE STATISTICS")
            print("=" * 60)
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
    
    finally:
        session.close()

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
        
        # Get the most recent sessions
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
                    duration = (s.end_time - s.start_time).total_seconds()
                    hours = int(duration // 3600)
                    minutes = int((duration % 3600) // 60)
                    seconds = int(duration % 60)
                    duration_str = f"{hours}h {minutes}m {seconds}s"
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
                print(f"   URLs processed: {s.urls_processed}")
                print(f"   Manufacturers extracted: {s.manufacturers_extracted}")
                print(f"   Categories extracted: {s.categories_extracted}")
            elif s.session_type == 'finder':
                print(f"   Type: Website Finder")
                print(f"   Websites found: {s.websites_found}")
            
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
        scraper_stats = scraper.process_batch(
            max_pages=config['scraper'].get('batch_size', 100),
            max_runtime_minutes=int(scraper_time) if scraper_time > 0 else None
        )
        
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
            finder_stats = finder.process_batch(
                max_pages=config['manufacturer_finder'].get('batch_size', 20),
                max_runtime_minutes=int(finder_time) if finder_time > 0 else None
            )
            
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
                print("  resume\t\t\t\t\tResume background process")
                print("  stats [--type scraper|finder]\t\t\tDisplay database statistics")
                print("  sessions [--type scraper|finder] [--limit N]\tList past scraper sessions")
                print("  list-manufacturers [--limit N] [--with-categories]\tList manufacturers")
                print("  list-categories [--limit N] [--with-manufacturers]\tList categories")
                print("  search <term> [--type manufacturer|category]\t\tSearch the database")
                print("  export [--output PATH]\t\t\tExport database to JSON")
                print("  validate\t\t\t\t\tValidate database consistency")
                print("  generate-sitemap [--format json|xml|both]\t\tGenerate sitemap files")
                print("  help\t\t\t\t\tShow this help message")
                print("  exit\t\t\t\t\tExit the program")
            
            elif command.startswith('scrape'):
                args = command.split()[1:] if len(command.split()) > 1 else []
                max_pages = None
                background = False
                
                # Parse arguments
                i = 0
                while i < len(args):
                    if args[i] == '--max-pages' and i + 1 < len(args):
                        try:
                            max_pages = int(args[i + 1])
                            i += 2
                        except ValueError:
                            print("Error: --max-pages requires a number")
                            break
                    elif args[i] == '--background':
                        background = True
                        i += 1
                    else:
                        print(f"Unrecognized argument: {args[i]}")
                        i += 1
                
                # Start the scraper
                print(f"Starting competitor scraper{' in background' if background else ''}...")
                scraper = CompetitorScraper(config_path=config['config_path'])
                
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
                                    # Close standard file descriptors
                                    os.close(0)
                                    os.close(1)
                                    os.close(2)
                                    # Redirect to /dev/null and log files
                                    sys.stdin = open(os.devnull, 'r')
                                    sys.stdout = open(os.path.join('logs', 'scraper_background.log'), 'a')
                                    sys.stderr = open(os.path.join('logs', 'scraper_background_error.log'), 'a')
                                    
                                    # Run the actual process
                                    scraper.start_crawling(max_pages=max_pages)
                                    sys.exit(0)
                                except Exception as e:
                                    logger.error(f"Error in background scraper: {str(e)}")
                                    sys.exit(1)
                        # Wait for the first child to exit
                        if pid == 0:
                            os._exit(0)
                        os.waitpid(pid, 0)
                    except OSError as e:
                        logger.error(f"Failed to fork process: {str(e)}")
                        print(f"Error starting background process: {str(e)}")
                else:
                    # Run in foreground
                    try:
                        scraper.start_crawling(max_pages=max_pages)
                        print("Scraper completed.")
                        display_statistics(config)
                    except Exception as e:
                        logger.error(f"Error in scraper: {str(e)}")
                        print(f"Error: {str(e)}")
            
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
                
                # Start the manufacturer website finder
                print(f"Starting manufacturer website finder{' in background' if background else ''}...")
                finder = ManufacturerWebsiteFinder(config_path=config['config_path'])
                
                if background:
                    # Fork a new process for background operation using double-fork pattern
                    try:
                        # First fork to create child process
                        pid = os.fork()
                        if pid > 0:
                            # Parent process
                            print(f"Manufacturer website finder started in background with PID {pid}")
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
                                    # Close standard file descriptors
                                    os.close(0)
                                    os.close(1)
                                    os.close(2)
                                    # Redirect to /dev/null and log files
                                    sys.stdin = open(os.devnull, 'r')
                                    sys.stdout = open(os.path.join('logs', 'finder_background.log'), 'a')
                                    sys.stderr = open(os.path.join('logs', 'finder_background_error.log'), 'a')
                                    
                                    # Run the actual process
                                    finder.find_websites(limit=limit)
                                    sys.exit(0)
                                except Exception as e:
                                    logger.error(f"Error in background finder: {str(e)}")
                                    sys.exit(1)
                        # Wait for the first child to exit
                        if pid == 0:
                            os._exit(0)
                        os.waitpid(pid, 0)
                    except OSError as e:
                        logger.error(f"Failed to fork process: {str(e)}")
                        print(f"Error starting background process: {str(e)}")
                else:
                    # Run in foreground
                    try:
                        finder.find_websites(limit=limit)
                        print("Manufacturer website finder completed.")
                        display_statistics(config)
                    except Exception as e:
                        logger.error(f"Error in finder: {str(e)}")
                        print(f"Error: {str(e)}")
            
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
                
                # Run scraper and finder sequentially
                print(f"Starting sequential run{' in background' if background else ''}...")
                
                if background:
                    # Fork a new process for background operation
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
                                    # Close standard file descriptors
                                    os.close(0)
                                    os.close(1)
                                    os.close(2)
                                    # Redirect to /dev/null
                                    sys.stdin = open(os.devnull, 'r')
                                    sys.stdout = open(os.path.join('logs', 'background_process.log'), 'a')
                                    sys.stderr = open(os.path.join('logs', 'background_process_error.log'), 'a')
                                    
                                    # Run the actual process
                                    run_scraper_and_finder(
                                        config,
                                        max_runtime=max_runtime,
                                        time_allocation=time_allocation,
                                        background=True
                                    )
                                    sys.exit(0)
                                except Exception as e:
                                    logger.error(f"Error in background process: {str(e)}")
                                    sys.exit(1)
                        # Wait for the first child to exit
                        if pid == 0:
                            os._exit(0)
                        os.waitpid(pid, 0)
                    except OSError as e:
                        logger.error(f"Failed to fork process: {str(e)}")
                        print(f"Error starting background process: {str(e)}")
                else:
                    # Run in foreground
                    try:
                        status = run_scraper_and_finder(
                            config,
                            max_runtime=max_runtime,
                            time_allocation=time_allocation,
                            background=False
                        )
                        print("Sequential run completed.")
                        display_statistics(config)
                    except Exception as e:
                        logger.error(f"Error in sequential run: {str(e)}")
                        print(f"Error: {str(e)}")
            
            elif command == 'status':
                display_process_status(config)
                
            elif command == 'stop':
                pid = load_pid(config)
                if pid and is_process_running(pid):
                    try:
                        os.kill(pid, signal.SIGTERM)
                        print(f"Sent termination signal to process {pid}")
                    except Exception as e:
                        logger.error(f"Error stopping process: {str(e)}")
                        print(f"Error stopping process: {str(e)}")
                else:
                    print("No running process found")
                    
            elif command == 'resume':
                status_data = load_process_status(config)
                if not status_data:
                    print("No saved process status found")
                    continue
                    
                print("Resuming previous process...")
                # TODO: Implement resume functionality
                print("Resume functionality not yet implemented")
                
            elif command.startswith('stats'):
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
                
            elif command.startswith('sessions'):
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
                
                list_sessions(config, session_type=session_type, limit=limit)
                
            elif command.startswith('list-manufacturers'):
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
                
            elif command.startswith('list-categories'):
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
                
            elif command.startswith('search '):
                parts = command.split()
                if len(parts) < 2:
                    print("Error: search requires a term")
                    continue
                    
                term = parts[1]
                entity_type = None
                
                if len(parts) > 2 and parts[2] == '--type' and len(parts) > 3:
                    if parts[3] in ['manufacturer', 'category']:
                        entity_type = parts[3]
                    else:
                        print(f"Error: Invalid type '{parts[3]}'. Use 'manufacturer' or 'category'")
                        continue
                
                search_database(config, term, entity_type)
                
            elif command.startswith('export'):
                args = command.split()[1:] if len(command.split()) > 1 else []
                output_path = 'output/database_export.json'  # Default path
                
                # Parse arguments
                i = 0
                while i < len(args):
                    if args[i] == '--output' and i + 1 < len(args):
                        output_path = args[i + 1]
                        i += 2
                    else:
                        print(f"Unrecognized argument: {args[i]}")
                        i += 1
                
                success, message = export_database_to_json(config['database']['path'], output_path)
                print(message)
                
            elif command == 'validate':
                issues = validate_database_consistency(config['database']['path'])
                if issues:
                    print("Found database consistency issues:")
                    for issue in issues:
                        print(f"  - {issue}")
                else:
                    print("No database consistency issues found")
                    
            elif command.startswith('generate-sitemap'):
                args = command.split()[1:] if len(command.split()) > 1 else []
                format_type = 'both'  # Default format
                
                # Parse arguments
                i = 0
                while i < len(args):
                    if args[i] == '--format' and i + 1 < len(args):
                        if args[i + 1] in ['json', 'xml', 'both']:
                            format_type = args[i + 1]
                            i += 2
                        else:
                            print(f"Error: Invalid format '{args[i + 1]}'. Use 'json', 'xml', or 'both'")
                            break
                    else:
                        print(f"Unrecognized argument: {args[i]}")
                        i += 1
                
                print("Generating sitemap files from database...")
                scraper = CompetitorScraper(config_path=config['config_path'])
                scraper.generate_sitemap_files(format_type=format_type)
                print("Sitemap generation completed.")
                
            else:
                print(f"Unknown command: {command}")
                print("Type 'help' for available commands.")
                
        except KeyboardInterrupt:
            print("\nOperation interrupted. Type 'exit' to quit.")
        except Exception as e:
            logger.error(f"Error in interactive mode: {str(e)}")
            print(f"Error: {str(e)}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='NDAIVI Competitor Scraper CLI')
    parser.add_argument('--config', default='config.yaml', help='Path to configuration file')
    parser.add_argument('--interactive', '-i', action='store_true', help='Run in interactive mode')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Scrape command (replaces 'start')
    scrape_parser = subparsers.add_parser('scrape', help='Start the competitor scraper')
    scrape_parser.add_argument('--max-pages', type=int, help='Maximum number of pages to crawl')
    scrape_parser.add_argument('--background', action='store_true', help='Run in background mode')
    
    # Find websites command
    find_websites_parser = subparsers.add_parser('find-websites', help='Find manufacturer websites')
    find_websites_parser.add_argument('--limit', type=int, help='Limit the number of manufacturers to process')
    find_websites_parser.add_argument('--background', action='store_true', help='Run in background mode')
    
    # Run-all command (replaces 'run')
    run_all_parser = subparsers.add_parser('run-all', help='Run scraper and manufacturer website finder sequentially')
    run_all_parser.add_argument('--time-allocation', type=int, choices=range(0, 101), metavar='[0-100]',
                           help='Time allocation between scraper and finder (0-100, higher values favor scraper)')
    run_all_parser.add_argument('--max-runtime', type=int, help='Maximum runtime in minutes')
    run_all_parser.add_argument('--background', action='store_true', help='Run in background mode')
    
    # Background commands
    background_parser = subparsers.add_parser('background', help='Background process commands')
    background_subparsers = background_parser.add_subparsers(dest='bg_command', help='Background command')
    
    # Status command
    status_parser = background_subparsers.add_parser('status', help='Check status of background process')
    
    # Stop command
    stop_parser = background_subparsers.add_parser('stop', help='Stop background process')
    
    # Resume command
    resume_parser = background_subparsers.add_parser('resume', help='Resume background process')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Display statistics')
    stats_parser.add_argument('--type', choices=['scraper', 'finder'], help='Type of statistics to display')
    
    # Sessions command
    sessions_parser = subparsers.add_parser('sessions', help='List past scraper sessions')
    sessions_parser.add_argument('--type', choices=['scraper', 'finder'], help='Type of sessions to list')
    sessions_parser.add_argument('--limit', type=int, default=10, help='Maximum number of sessions to display')
    
    # Initialize database command
    init_db_parser = subparsers.add_parser('init-db', help='Initialize the database')
    init_db_parser.add_argument('--path', help='Custom database file path')
    
    # List manufacturers command
    list_mfr_parser = subparsers.add_parser('list-manufacturers', help='List manufacturers')
    list_mfr_parser.add_argument('--limit', type=int, help='Limit the number of results')
    list_mfr_parser.add_argument('--with-categories', action='store_true', help='Show categories for each manufacturer')
    
    # List categories command
    list_cat_parser = subparsers.add_parser('list-categories', help='List categories')
    list_cat_parser.add_argument('--limit', type=int, help='Limit the number of results')
    list_cat_parser.add_argument('--with-manufacturers', action='store_true', help='Show manufacturers for each category')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search the database')
    search_parser.add_argument('term', help='Search term')
    search_parser.add_argument('--type', choices=['manufacturer', 'category'], help='Entity type to search')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export database to JSON')
    export_parser.add_argument('--output', default='output/database_export.json', help='Output file path')
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate database consistency')
    
    # Manufacturer websites command (Stage 2)
    mfr_websites_parser = subparsers.add_parser('manufacturer-websites', help='Find websites for manufacturers (Stage 2)')
    mfr_websites_parser.add_argument('--limit', type=int, help='Limit the number of manufacturers to process')
    
    # Generate sitemap command
    sitemap_parser = subparsers.add_parser('generate-sitemap', help='Generate sitemap files from database')
    sitemap_parser.add_argument('--format', choices=['json', 'xml', 'both'], default='both', help='Sitemap format to generate')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Store config path in config dict for use in other functions
        config['config_path'] = args.config
        
        # Ensure database exists with all required tables before any command that requires it
        # Exclude init-db command which handles its own database check
        if args.command not in [None, 'init-db']:
            logger.info("Ensuring database exists with all required tables...")
            if not ensure_database_exists(config):
                print("⚠️ Database initialization failed! Please check the logs for details.")
                user_input = input("Do you want to try initializing the database again? (y/n): ").lower()
                
                if user_input == 'y':
                    db_path = input(f"Enter database path (default: {config['database']['path']}): ")
                    if not db_path.strip():
                        db_path = config['database']['path']
                    
                    initialize_database(args.config, db_path)
                    # Reload config in case the path was changed
                    config = load_config(args.config)
                    config['config_path'] = args.config
                    
                    # Verify database initialization again
                    if not ensure_database_exists(config):
                        print("⚠️ Database initialization failed again. Please check the logs for details.")
                        return
                else:
                    print("Database initialization canceled. Use 'init-db' command to initialize the database later.")
                    return
            else:
                logger.info("Database verification successful. All required tables exist.")
        
        # Check for interactive mode first
        if args.interactive:
            interactive_mode(config)
            return
            
        if args.command == 'init-db':
            # Initialize database with custom path if provided
            try:
                db_path = args.path
                initialize_database(args.config, db_path)
            except Exception as e:
                logger.error(f"Database initialization failed: {str(e)}")
                print(f"Error initializing database: {str(e)}")
                sys.exit(1)
        
        elif args.command == 'scrape':
            # Start the competitor scraper (replaces 'start')
            max_pages = None
            if args.max_pages:
                config['crawling']['max_pages_per_run'] = args.max_pages
                max_pages = args.max_pages
            
            try:
                # Initialize and start the scraper
                scraper = CompetitorScraper(config_path=args.config)
                # Pass max_pages directly to start_crawling
                scraper.start_crawling(max_pages=max_pages)
                display_statistics(config)
                scraper.close()
            except Exception as e:
                logger.error(f"Error in scraper: {str(e)}")
                print(f"Error: {str(e)}")
        
        elif args.command == 'find-websites':
            # Find manufacturer websites
            print(f"Starting manufacturer website finder{' in background' if args.background else ''}...")
            
            # Check if we should run in background mode
            if args.background:
                # Check if there's already a background process running
                pid = load_pid(config)
                if pid and is_process_running(pid):
                    print(f"⚠️ Background process already running with PID {pid}")
                    print("Use 'background status' to check its status or 'background stop' to stop it.")
                    return
                
                # Fork a new process using double-fork pattern
                try:
                    # First fork to create child process
                    pid = os.fork()
                    if pid > 0:
                        # Parent process
                        print(f"Started background process with PID {pid}")
                        print("Use 'background status' to check its status.")
                        # Save PID to file
                        save_pid(config, pid)
                        return
                    else:
                        # Child process - create a second fork to fully detach
                        pid2 = os.fork()
                        if pid2 > 0:
                            # Exit first child immediately
                            os._exit(0)
                        else:
                            # Grandchild process (now fully detached)
                            # Detach from terminal
                            os.setsid()
                            os.umask(0)
                            # Close file descriptors
                            os.close(0)
                            os.close(1)
                            os.close(2)
                            # Redirect to log files
                            sys.stdin = open(os.devnull, 'r')
                            sys.stdout = open(os.path.join('logs', 'finder_background.log'), 'a')
                            sys.stderr = open(os.path.join('logs', 'finder_background_error.log'), 'a')
                except OSError as e:
                    logger.error(f"Failed to fork process: {str(e)}")
                    print(f"Error starting background process: {str(e)}")
                    return
            
            try:
                # Initialize the finder
                finder = ManufacturerWebsiteFinder(config_path=args.config)
                # Find websites
                finder.find_websites(limit=args.limit)
                
                if not args.background:
                    print("\nManufacturer website finder completed successfully.")
                    display_statistics(config)
                    
                # If we're in the child process, exit
                if args.background and os.getpid() != os.getppid():
                    sys.exit(0)
                    
            except Exception as e:
                logger.error(f"Error in manufacturer website finder: {str(e)}")
                print(f"Error: {str(e)}")
                if args.background and os.getpid() != os.getppid():
                    sys.exit(1)
        
        elif args.command == 'run-all':
            # Run scraper and manufacturer website finder sequentially
            print(f"Starting sequential run of competitor scraper and manufacturer website finder{' in background' if args.background else ''}...")
            
            # Check if we should run in background mode
            if args.background:
                # Check if there's already a background process running
                pid = load_pid(config)
                if pid and is_process_running(pid):
                    print(f"⚠️ Background process already running with PID {pid}")
                    print("Use 'background status' to check its status or 'background stop' to stop it.")
                    return
                
                # Fork a new process using double-fork pattern
                try:
                    # First fork to create child process
                    pid = os.fork()
                    if pid > 0:
                        # Parent process
                        print(f"Started background process with PID {pid}")
                        print("Use 'background status' to check its status.")
                        # Save PID to file
                        save_pid(config, pid)
                        return
                    else:
                        # Child process - create a second fork to fully detach
                        pid2 = os.fork()
                        if pid2 > 0:
                            # Exit first child immediately
                            os._exit(0)
                        else:
                            # Grandchild process (now fully detached)
                            # Detach from terminal
                            os.setsid()
                            os.umask(0)
                            # Close file descriptors
                            os.close(0)
                            os.close(1)
                            os.close(2)
                            # Redirect to log files
                            sys.stdin = open(os.devnull, 'r')
                            sys.stdout = open(os.path.join('logs', 'finder_background.log'), 'a')
                            sys.stderr = open(os.path.join('logs', 'finder_background_error.log'), 'a')
                except OSError as e:
                    logger.error(f"Failed to fork process: {str(e)}")
                    print(f"Error starting background process: {str(e)}")
                    return
            
            try:
                # Run the sequential process
                run_scraper_and_finder(
                    config,
                    max_runtime=args.max_runtime,
                    time_allocation=args.time_allocation,
                    background=args.background
                )
                
                if not args.background:
                    print("\nSequential run completed successfully.")
                    display_statistics(config)
                    
                # If we're in the child process, exit
                if args.background and os.getpid() != os.getppid():
                    sys.exit(0)
                    
            except Exception as e:
                logger.error(f"Error in sequential run: {str(e)}")
                print(f"Error in sequential run: {str(e)}")
                if args.background and os.getpid() != os.getppid():
                    sys.exit(1)
        
        elif args.command == 'background':
            if args.bg_command == 'status':
                # Display status of background process
                display_process_status(config)
                
            elif args.bg_command == 'stop':
                # Stop background process
                pid = load_pid(config)
                if pid is None:
                    print("No background process found.")
                    return
                
                if not is_process_running(pid):
                    print(f"Process with PID {pid} is not running.")
                    # Clean up PID file
                    os.remove(config['scheduling']['background']['pid_file'])
                    return
                
                # Try to terminate the process gracefully
                try:
                    os.kill(pid, 15)  # SIGTERM
                    print(f"Sent termination signal to process {pid}.")
                    
                    # Wait for process to terminate
                    for _ in range(5):
                        time.sleep(1)
                        if not is_process_running(pid):
                            print(f"Process {pid} terminated successfully.")
                            # Clean up PID file
                            os.remove(config['scheduling']['background']['pid_file'])
                            return
                    
                    # If process didn't terminate, ask to force kill
                    print(f"Process {pid} is still running. Force kill? (y/n): ", end='')
                    if input().lower() == 'y':
                        os.kill(pid, 9)  # SIGKILL
                        print(f"Sent kill signal to process {pid}.")
                        # Clean up PID file
                        os.remove(config['scheduling']['background']['pid_file'])
                    else:
                        print("Process not killed.")
                        
                except OSError as e:
                    logger.error(f"Failed to terminate process: {str(e)}")
                    print(f"Error terminating process: {str(e)}")
                    
            elif args.bg_command == 'resume':
                # Resume background process
                status = load_process_status(config)
                if status is None:
                    print("No saved process state found to resume.")
                    return
                
                # Check if there's already a background process running
                pid = load_pid(config)
                if pid and is_process_running(pid):
                    print(f"⚠️ Background process already running with PID {pid}")
                    print("Use 'background status' to check its status or 'background stop' to stop it.")
                    return
                
                print("Resuming background process...")
                
                # Fork a new process using double-fork pattern
                try:
                    # First fork to create child process
                    pid = os.fork()
                    if pid > 0:
                        # Parent process
                        print(f"Started background process with PID {pid}")
                        print("Use 'background status' to check its status.")
                        # Save PID to file
                        save_pid(config, pid)
                        return
                    else:
                        # Child process - create a second fork to fully detach
                        pid2 = os.fork()
                        if pid2 > 0:
                            # Exit first child immediately
                            os._exit(0)
                        else:
                            # Grandchild process (now fully detached)
                            # Detach from terminal
                            os.setsid()
                            os.umask(0)
                            # Close file descriptors
                            os.close(0)
                            os.close(1)
                            os.close(2)
                            # Redirect to log files
                            sys.stdin = open(os.devnull, 'r')
                            sys.stdout = open(os.path.join('logs', 'finder_background.log'), 'a')
                            sys.stderr = open(os.path.join('logs', 'finder_background_error.log'), 'a')
                except OSError as e:
                    logger.error(f"Failed to fork process: {str(e)}")
                    print(f"Error starting background process: {str(e)}")
                    return
                
                try:
                    # Resume the sequential process based on saved state
                    current_stage = status.get('current_stage', 'competitor_scraper')
                    stages_completed = status.get('stages_completed', [])
                    
                    # Determine what to run based on saved state
                    if 'competitor_scraper' not in stages_completed and current_stage == 'competitor_scraper':
                        # Resume from competitor scraper
                        run_scraper_and_finder(
                            config,
                            max_runtime=status.get('max_runtime_minutes'),
                            time_allocation=status.get('time_allocation'),
                            background=True
                        )
                    elif 'competitor_scraper' in stages_completed and current_stage == 'manufacturer_website_finder':
                        # Resume from manufacturer website finder
                        finder = ManufacturerWebsiteFinder(config_path=config['config_path'])
                        finder.find_websites()
                        
                        # Update status
                        status["stages_completed"].append("manufacturer_website_finder")
                        status["finder_stats"] = finder.stats
                        status["is_complete"] = True
                        status["last_update"] = time.time()
                        save_process_status(config, status)
                    else:
                        # Both stages completed or unknown state
                        logger.error(f"Cannot resume from unknown state: {current_stage}")
                        sys.exit(1)
                    
                    # If we're in the child process, exit
                    if os.getpid() != os.getppid():
                        sys.exit(0)
                        
                except Exception as e:
                    logger.error(f"Error resuming process: {str(e)}")
                    if os.getpid() != os.getppid():
                        sys.exit(1)
            else:
                print("Unknown background command. Use 'status', 'stop', or 'resume'.")
        
        elif args.command == 'stats':
            display_statistics(config, report_type=args.type)
            
        elif args.command == 'sessions':
            list_sessions(config, session_type=args.type, limit=args.limit)
            
        elif args.command == 'list-manufacturers':
            list_manufacturers(config, args.limit, args.with_categories)
            
        elif args.command == 'list-categories':
            list_categories(config, args.limit, args.with_manufacturers)
            
        elif args.command == 'search':
            search_database(config, args.term, args.type)
            
        elif args.command == 'export':
            success, message = export_database_to_json(config['database']['path'], args.output)
            print(message)
            
        elif args.command == 'validate':
            issues = validate_database_consistency(config['database']['path'])
            if issues:
                print("Found database consistency issues:")
                for issue in issues:
                    print(f"  - {issue}")
            else:
                print("No database consistency issues found")
                
        elif args.command == 'manufacturer-websites':
            print("Starting manufacturer website finder (Stage 2)...")
            finder = ManufacturerWebsiteFinder(config_path=args.config)
            limit = args.limit if args.limit else None
            finder.find_websites(limit=limit)
            print("Manufacturer website finder completed.")
            display_statistics(config)
            
        elif args.command == 'generate-sitemap':
            print("Generating sitemap files from database...")
            # Initialize the scraper to access its methods
            scraper = CompetitorScraper(config_path=args.config)
            # Generate the sitemap files
            scraper.generate_sitemap_files()
            print("Sitemap generation completed.")
            # Display statistics
            display_statistics(config)
                
        else:
            parser.print_help()
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
