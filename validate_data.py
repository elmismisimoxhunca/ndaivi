#!/usr/bin/env python3
import os
import sys
import argparse
import yaml
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import openai
from database.schema import Base, Category, Manufacturer
from utils import load_config, validate_database_consistency, export_database_to_json

def setup_logging():
    """Set up logging configuration"""
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, 'validation.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger('validation')

def get_db_session(config):
    """Get a database session"""
    db_path = config['database']['path']
    engine = create_engine(f'sqlite:///{db_path}')
    Session = sessionmaker(bind=engine)
    return Session()

def validate_with_ai(config, session, logger):
    """Use OpenAI to validate the extracted data"""
    # Initialize OpenAI
    openai.api_key = config['ai_apis']['openai']['api_key']
    model = config['ai_apis']['openai']['model']
    
    # Get all manufacturers with their categories
    manufacturers = session.query(Manufacturer).all()
    
    validation_results = []
    
    for manufacturer in manufacturers:
        logger.info(f"Validating manufacturer: {manufacturer.name}")
        
        # Prepare data for validation
        data = {
            "name": manufacturer.name,
            "website": manufacturer.website,
            "categories": [cat.name for cat in manufacturer.categories]
        }
        
        # Create prompt for validation
        prompt = f"""Please validate the following manufacturer data for accuracy and consistency:

Manufacturer Name: {data['name']}
Website: {data['website'] or 'Not provided'}
Product Categories: {', '.join(data['categories']) if data['categories'] else 'None'}

Please check for the following issues:
1. Is the manufacturer name correct and properly formatted?
2. Are the product categories appropriate for this manufacturer?
3. Are there any obvious errors or inconsistencies?

Provide your assessment as JSON with the following structure:
{{
  "name_valid": true/false,
  "categories_valid": true/false,
  "issues": ["issue1", "issue2"],
  "suggested_corrections": {{
    "name": "corrected name" (if needed),
    "categories": ["category1", "category2"] (if needed)
  }}
}}
"""
        
        try:
            response = openai.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a data validation assistant. Validate manufacturer and product category data for accuracy and consistency."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"}
            )
            
            result = response.choices[0].message.content
            
            import json
            validation = json.loads(result)
            
            # Add to validation results
            validation['manufacturer_id'] = manufacturer.id
            validation['manufacturer_name'] = manufacturer.name
            validation_results.append(validation)
            
            # Log issues if any
            if not validation.get('name_valid', True) or not validation.get('categories_valid', True):
                issues = validation.get('issues', [])
                for issue in issues:
                    logger.warning(f"Issue with {manufacturer.name}: {issue}")
            
        except Exception as e:
            logger.error(f"Error validating {manufacturer.name}: {str(e)}")
            validation_results.append({
                "manufacturer_id": manufacturer.id,
                "manufacturer_name": manufacturer.name,
                "error": str(e)
            })
    
    return validation_results

def apply_corrections(session, validation_results, logger):
    """Apply corrections based on validation results"""
    corrections_applied = 0
    
    for result in validation_results:
        # Skip if no corrections suggested or if there was an error
        if 'error' in result or not result.get('suggested_corrections'):
            continue
        
        manufacturer_id = result['manufacturer_id']
        corrections = result['suggested_corrections']
        
        # Get the manufacturer
        manufacturer = session.query(Manufacturer).filter_by(id=manufacturer_id).first()
        if not manufacturer:
            logger.warning(f"Manufacturer with ID {manufacturer_id} not found")
            continue
        
        # Apply name correction if provided
        if 'name' in corrections and corrections['name'] and corrections['name'] != manufacturer.name:
            old_name = manufacturer.name
            manufacturer.name = corrections['name']
            logger.info(f"Corrected manufacturer name: {old_name} -> {corrections['name']}")
            corrections_applied += 1
        
        # Apply category corrections if provided
        if 'categories' in corrections and corrections['categories']:
            # Get current categories
            current_categories = [cat.name for cat in manufacturer.categories]
            
            # Add new categories
            for cat_name in corrections['categories']:
                if cat_name not in current_categories:
                    # Check if category exists
                    category = session.query(Category).filter_by(name=cat_name).first()
                    if not category:
                        category = Category(name=cat_name)
                        session.add(category)
                        session.flush()
                    
                    # Add to manufacturer
                    if category not in manufacturer.categories:
                        manufacturer.categories.append(category)
                        logger.info(f"Added category {cat_name} to {manufacturer.name}")
                        corrections_applied += 1
    
    # Commit changes
    if corrections_applied > 0:
        session.commit()
        logger.info(f"Applied {corrections_applied} corrections")
    else:
        logger.info("No corrections needed")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='NDAIVI Data Validation Tool')
    parser.add_argument('--config', default='config.yaml', help='Path to configuration file')
    parser.add_argument('--export', action='store_true', help='Export database to JSON after validation')
    parser.add_argument('--apply-corrections', action='store_true', help='Apply suggested corrections')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Get database session
        session = get_db_session(config)
        
        # Check database consistency
        logger.info("Checking database consistency...")
        issues = validate_database_consistency(config['database']['path'])
        
        if issues:
            logger.warning("Found database consistency issues:")
            for issue in issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info("No database consistency issues found")
        
        # Validate with AI
        logger.info("Starting AI validation...")
        validation_results = validate_with_ai(config, session, logger)
        
        # Save validation results
        import json
        output_dir = os.path.join(os.path.dirname(__file__), 'output')
        os.makedirs(output_dir, exist_ok=True)
        
        validation_file = os.path.join(output_dir, 'validation_results.json')
        with open(validation_file, 'w') as f:
            json.dump(validation_results, f, indent=2)
        
        logger.info(f"Validation results saved to {validation_file}")
        
        # Apply corrections if requested
        if args.apply_corrections:
            logger.info("Applying corrections...")
            apply_corrections(session, validation_results, logger)
        
        # Export database if requested
        if args.export:
            logger.info("Exporting database to JSON...")
            export_file = os.path.join(output_dir, 'database_export.json')
            success, message = export_database_to_json(config['database']['path'], export_file)
            logger.info(message)
        
        # Close session
        session.close()
        
        logger.info("Validation completed successfully")
        
    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
