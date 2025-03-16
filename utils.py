import os
import yaml
import json
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database.schema import Base, Category, Manufacturer, CrawlStatus, ScraperLog

def load_config(config_path='config.yaml'):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def get_db_session(db_path):
    """Get a database session"""
    # Ensure the directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Create the engine and session
    engine = create_engine(f'sqlite:///{db_path}')
    Session = sessionmaker(bind=engine)
    
    return Session()

def initialize_database(db_path):
    """Initialize the database with tables"""
    # Ensure the directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Create the engine and tables
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    
    return engine

def validate_manufacturer_data(data):
    """Validate manufacturer data for consistency"""
    issues = []
    
    # Check for required fields
    if 'name' not in data or not data['name']:
        issues.append("Missing manufacturer name")
    
    # Check for valid website format if present
    if 'website' in data and data['website']:
        if not data['website'].startswith(('http://', 'https://')):
            issues.append(f"Invalid website URL format: {data['website']}")
    
    # Check for categories
    if 'categories' not in data or not isinstance(data['categories'], list):
        issues.append("Missing or invalid categories list")
    
    return issues

def export_database_to_json(db_path, output_path):
    """Export database contents to JSON for inspection"""
    session = get_db_session(db_path)
    
    try:
        # Get all manufacturers with their categories
        manufacturers = session.query(Manufacturer).all()
        categories = session.query(Category).all()
        
        # Prepare data structure
        data = {
            "manufacturers": [],
            "categories": []
        }
        
        # Add manufacturers
        for mfr in manufacturers:
            mfr_data = {
                "id": mfr.id,
                "name": mfr.name,
                "website": mfr.website,
                "categories": [cat.name for cat in mfr.categories]
            }
            data["manufacturers"].append(mfr_data)
        
        # Add categories
        for cat in categories:
            cat_data = {
                "id": cat.id,
                "name": cat.name,
                "manufacturers": [mfr.name for mfr in cat.manufacturers]
            }
            data["categories"].append(cat_data)
        
        # Write to file
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        return True, f"Database exported to {output_path}"
    
    except Exception as e:
        return False, f"Error exporting database: {str(e)}"
    
    finally:
        session.close()

def validate_database_consistency(db_path):
    """Validate database for consistency issues"""
    session = get_db_session(db_path)
    issues = []
    
    try:
        # Check for manufacturers without categories
        mfrs_without_cats = session.query(Manufacturer).filter(~Manufacturer.categories.any()).all()
        if mfrs_without_cats:
            issues.append(f"Found {len(mfrs_without_cats)} manufacturers without categories")
            for mfr in mfrs_without_cats:
                issues.append(f"  - {mfr.name}")
        
        # Check for categories without manufacturers
        cats_without_mfrs = session.query(Category).filter(~Category.manufacturers.any()).all()
        if cats_without_mfrs:
            issues.append(f"Found {len(cats_without_mfrs)} categories without manufacturers")
            for cat in cats_without_mfrs:
                issues.append(f"  - {cat.name}")
        
        # Check for duplicate manufacturer names (case insensitive)
        from sqlalchemy import func
        duplicate_mfrs = session.query(func.lower(Manufacturer.name), func.count(Manufacturer.id))\
                               .group_by(func.lower(Manufacturer.name))\
                               .having(func.count(Manufacturer.id) > 1).all()
        
        if duplicate_mfrs:
            issues.append(f"Found {len(duplicate_mfrs)} duplicate manufacturer names (case-insensitive)")
            for name, count in duplicate_mfrs:
                issues.append(f"  - {name}: {count} occurrences")
        
        # Check for duplicate category names (case insensitive)
        duplicate_cats = session.query(func.lower(Category.name), func.count(Category.id))\
                              .group_by(func.lower(Category.name))\
                              .having(func.count(Category.id) > 1).all()
        
        if duplicate_cats:
            issues.append(f"Found {len(duplicate_cats)} duplicate category names (case-insensitive)")
            for name, count in duplicate_cats:
                issues.append(f"  - {name}: {count} occurrences")
        
        return issues
    
    except Exception as e:
        issues.append(f"Error validating database: {str(e)}")
        return issues
    
    finally:
        session.close()
