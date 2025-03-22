from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Table, MetaData, Boolean, DateTime, Text, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import os
import datetime
import yaml

Base = declarative_base()

def get_supported_languages(config_path=None):
    """
    Get list of supported languages from config.
    
    Args:
        config_path: Optional path to the config file. If None, uses the default path.
        
    Returns:
        List of language codes including source and target languages.
    """
    if config_path is None:
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config.yaml')
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
        source = config['languages']['source']
        targets = list(config['languages']['targets'].keys())
        return [source] + targets
        
# Association table for many-to-many relationship between manufacturers and categories
manufacturer_category = Table(
    'manufacturer_category', 
    Base.metadata,
    Column('manufacturer_id', Integer, ForeignKey('manufacturers.id'), primary_key=True),
    Column('category_id', Integer, ForeignKey('categories.id'), primary_key=True)
)

# Dynamic creation of category tables for each language
# Keep track of already created table classes
_created_category_tables = {}

def create_category_table(lang_code):
    """Create a category table for a specific language"""
    # Return existing class if already created to avoid warnings
    if lang_code in _created_category_tables:
        return _created_category_tables[lang_code]
        
    # Create new table class
    table_class = type(
        f'Category_{lang_code}',
        (Base,),
        {
            '__tablename__': f'categories_{lang_code}',
            # Add extend_existing to handle tables that already exist
            '__table_args__': {'extend_existing': True},
            'id': Column(Integer, primary_key=True),
            'category_id': Column(Integer, ForeignKey('categories.id'), nullable=False),
            'category_name': Column(Text, nullable=False),
            'created_at': Column(DateTime, default=datetime.datetime.utcnow),
            'updated_at': Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow),
            '__repr__': lambda self: f"<Category_{lang_code}(product_id='{self.product_id}', category_name='{self.category_name}')>"
        }
    )
    
    # Store in our registry
    _created_category_tables[lang_code] = table_class
    return table_class

# Create category tables for all supported languages
# Note: This will be called with the config_path when init_db is called
def create_language_tables(config_path=None):
    """Create tables for all languages specified in the config"""
    # Get supported languages from config
    languages = get_supported_languages(config_path)
    
    # Create tables for each language
    for lang in languages:
        # Create table class if it doesn't exist
        if lang not in _created_category_tables:
            create_category_table(lang)
    
    return _created_category_tables

# Tables will be created when init_db is called with the specific config path
# No default creation here to avoid creating tables for languages not in config

class Product(Base):
    __tablename__ = 'products'
    
    id = Column(Integer, primary_key=True)
    manufacturer = Column(String(100), nullable=False)
    product_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    download_link = Column(String(255), nullable=True)
    manufacturer_website = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    def __repr__(self):
        return f"<Product(manufacturer='{self.manufacturer}', product_name='{self.product_name}')>"

class Manufacturer(Base):
    __tablename__ = 'manufacturers'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    __table_args__ = (
        # Create a unique index that uses the lower-case version of the name
        # This ensures case-insensitive uniqueness
        UniqueConstraint('name', name='uix_manufacturer_name_case_insensitive',
                        info={'case_sensitive': False}),
    )
    website = Column(String(255), nullable=True)
    website_validated = Column(Boolean, default=False)  # Indicates if the website has been validated
    description = Column(Text, nullable=True)
    logo_url = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    
    # Relationship with categories
    categories = relationship('Category', secondary=manufacturer_category, back_populates='manufacturers')
    
    def __repr__(self):
        return f"<Manufacturer(name='{self.name}', website='{self.website}')>"

class Category(Base):
    __tablename__ = 'categories'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    
    # Define the relationship back to manufacturers
    manufacturers = relationship('Manufacturer', secondary=manufacturer_category, back_populates='categories')
    
    def __repr__(self):
        return f"<Category(name='{self.name}')>"

class CrawlStatus(Base):
    __tablename__ = 'crawl_status'
    
    id = Column(Integer, primary_key=True)
    url = Column(String(255), unique=True, nullable=False)
    visited = Column(Boolean, default=False)
    analyzed = Column(Boolean, default=False)
    is_manufacturer_page = Column(Boolean, nullable=True)  # Retained for backward compatibility
    page_type = Column(String(50), nullable=True)  # 'brand_page', 'brand_category_page', 'inconclusive', or 'other'
    last_visited = Column(DateTime, nullable=True)
    depth = Column(Integer, default=0)
    parent_url = Column(String(255), nullable=True)
    # Sitemap-related fields
    title = Column(String(255), nullable=True)
    outgoing_links = Column(Integer, nullable=True)
    priority = Column(String(10), nullable=True)  # For XML sitemap priority (0.1-1.0)
    last_modified = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<CrawlStatus(url='{self.url}', visited={self.visited}, analyzed={self.analyzed}, page_type='{self.page_type}')>"

class ScraperLog(Base):
    __tablename__ = 'scraper_logs'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    level = Column(String(10), nullable=False)  # INFO, WARNING, ERROR
    message = Column(Text, nullable=False)
    
    def __repr__(self):
        return f"<ScraperLog(timestamp='{self.timestamp}', level='{self.level}', message='{self.message[:50]}...')>"


class ScraperSession(Base):
    __tablename__ = 'scraper_sessions'
    
    id = Column(Integer, primary_key=True)
    session_type = Column(String(20), nullable=False)  # 'scraper', 'finder', or 'combined'
    start_time = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)
    end_time = Column(DateTime, nullable=True)
    runtime_seconds = Column(Integer, default=0)  # Added runtime_seconds field
    status = Column(String(20), default='running')  # 'running', 'completed', 'interrupted'
    
    # Common statistics
    urls_processed = Column(Integer, default=0)
    manufacturers_extracted = Column(Integer, default=0)
    manufacturers_found = Column(Integer, default=0)  # Added for tracking manufacturer pages found
    categories_extracted = Column(Integer, default=0)
    websites_found = Column(Integer, default=0)
    translations = Column(Integer, default=0)  # Added for tracking translations
    
    # Page tracking
    pages_crawled = Column(Integer, default=0)
    pages_analyzed = Column(Integer, default=0)
    manual_links_found = Column(Integer, default=0)  # Added for tracking manual links
    
    # Error tracking
    http_errors = Column(Integer, default=0)
    connection_errors = Column(Integer, default=0)
    timeout_errors = Column(Integer, default=0)
    dns_resolution_errors = Column(Integer, default=0)
    errors = Column(Integer, default=0)  # General error counter
    
    # Batch configuration
    max_pages = Column(Integer, nullable=True)
    max_runtime_minutes = Column(Integer, nullable=True)
    
    # Error information
    error = Column(Text, nullable=True)
    
    def __repr__(self):
        return f"<ScraperSession(id={self.id}, type='{self.session_type}', status='{self.status}')>"


def get_db_engine(db_path):
    """
    Create a SQLAlchemy engine with optimized settings for SQLite.
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        SQLAlchemy engine instance
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Try setting basic SQLite configuration that doesn't require locks
    # We'll avoid trying to set WAL mode entirely to prevent locking warnings
    import sqlite3
    import logging
    logger = logging.getLogger('ndaivi')
    
    try:
        # Check if database file exists - if not, we can set all pragmas freely
        db_exists = os.path.exists(db_path)
        
        # Connect with a very short timeout to avoid waiting if locked
        direct_conn = sqlite3.connect(db_path, timeout=1)
        
        if not db_exists:
            # If this is a new database, we can set all pragmas including WAL mode
            logger.info("Setting up new database with optimal settings")
            direct_conn.execute("PRAGMA journal_mode=WAL")
            direct_conn.execute("PRAGMA synchronous=NORMAL")
        
        # These pragmas are safe to set even if the database is in use
        direct_conn.execute("PRAGMA busy_timeout=60000")  # 60 second timeout 
        direct_conn.execute("PRAGMA temp_store=MEMORY")
        direct_conn.execute("PRAGMA cache_size=-20000")  # ~20MB cache
        
        # Get current journal mode for logging
        cursor = direct_conn.execute("PRAGMA journal_mode")
        journal_mode = cursor.fetchone()[0]
        logger.info(f"SQLite journal mode: {journal_mode}")
        
        # Close the direct connection
        direct_conn.close()
        logger.info("Basic SQLite pragmas set successfully")
    except Exception as e:
        # If direct connection fails, we'll continue and let SQLAlchemy handle it
        import logging
        logger = logging.getLogger('ndaivi')
        logger.warning(f"Direct database connection failed: {str(e)}")
    
    # Create engine with more robust settings to handle locking issues
    engine = create_engine(
        f'sqlite:///{db_path}',
        connect_args={
            'timeout': 60,  # 60 seconds timeout for locks
            'check_same_thread': False,  # Allow cross-thread usage
            'isolation_level': None  # Let SQLAlchemy handle transactions
        },
        # Configure connection pooling
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=3600  # Recycle connections after 1 hour
    )
    
    return engine

def init_db(db_path, config_path=None):
    """
    Initialize database with tables for all supported languages.
    
    Args:
        db_path: Path to the SQLite database file
        config_path: Optional path to the config file with language settings
    
    Returns:
        SQLAlchemy engine instance
    """
    # Get database engine with optimized settings
    engine = get_db_engine(db_path)
    
    # Create language-specific tables based on config
    create_language_tables(config_path)
    
    # Create all tables including language-specific ones
    Base.metadata.create_all(engine)
    
    return engine

# Create language tables when module is imported
