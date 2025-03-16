#!/usr/bin/env python3
import os
import sys
import argparse
import time
import yaml
import logging
from scraper.competitor_scraper import CompetitorScraper
from manufacturer_website_finder import ManufacturerWebsiteFinder
from utils import load_config, validate_database_consistency, export_database_to_json
from database.schema import init_db, Category, Manufacturer, CrawlStatus, ScraperLog
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

def display_statistics(config):
    """Display statistics about the database"""
    session = get_db_session(config)
    
    try:
        # Get counts
        total_urls = session.query(CrawlStatus).count()
        visited_urls = session.query(CrawlStatus).filter_by(visited=True).count()
        manufacturer_pages = session.query(CrawlStatus).filter_by(is_manufacturer_page=True).count()
        manufacturers = session.query(Manufacturer).count()
        categories = session.query(Category).count()
        manufacturers_with_website = session.query(Manufacturer).filter(Manufacturer.website != None).count()
        
        # Display stats
        print("\n" + "=" * 50)
        print("COMPETITOR SCRAPER STATISTICS")
        print("=" * 50)
        print(f"Total URLs in database: {total_urls}")
        print(f"Visited URLs: {visited_urls}")
        print(f"Manufacturer pages identified: {manufacturer_pages}")
        print(f"Manufacturers extracted: {manufacturers}")
        print(f"Categories extracted: {categories}")
        print(f"Manufacturers with website: {manufacturers_with_website}")
        
        # Display top manufacturers
        print("\nTop 10 Manufacturers:")
        top_manufacturers = session.query(Manufacturer).limit(10).all()
        for i, mfr in enumerate(top_manufacturers, 1):
            print(f"{i}. {mfr.name} - Categories: {len(mfr.categories)} - Website: {mfr.website or 'N/A'}")
        
        # Display top categories
        print("\nTop 10 Categories:")
        top_categories = session.query(Category).limit(10).all()
        for i, cat in enumerate(top_categories, 1):
            print(f"{i}. {cat.name} - Manufacturers: {len(cat.manufacturers)}")
        
        print("=" * 50 + "\n")
        
    except Exception as e:
        print(f"Error getting statistics: {str(e)}")
    
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

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='NDAIVI Competitor Scraper CLI')
    parser.add_argument('--config', default='config.yaml', help='Path to configuration file')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Start command
    start_parser = subparsers.add_parser('start', help='Start the scraper')
    start_parser.add_argument('--max-pages', type=int, help='Maximum number of pages to crawl')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Display statistics')
    
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
        
        # Check if database exists before any command that requires it
        # Exclude init-db command which handles its own database check
        if args.command not in [None, 'init-db'] and not check_database_exists(config):
            print("⚠️ Database not found! You need to initialize it first.")
            user_input = input("Do you want to initialize the database now? (y/n): ").lower()
            
            if user_input == 'y':
                db_path = input(f"Enter database path (default: {config['database']['path']}): ")
                if not db_path.strip():
                    db_path = config['database']['path']
                
                initialize_database(args.config, db_path)
                # Reload config in case the path was changed
                config = load_config(args.config)
            else:
                print("Database initialization canceled. Use 'init-db' command to initialize the database later.")
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
        
        elif args.command == 'start':
            # Override max pages if specified
            if args.max_pages:
                config['crawling']['max_pages_per_run'] = args.max_pages
            
            # Initialize and start the scraper
            scraper = CompetitorScraper(config_path=args.config)
            scraper.start_crawling()
            display_statistics(config)
            scraper.close()
            
        elif args.command == 'stats':
            display_statistics(config)
            
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
