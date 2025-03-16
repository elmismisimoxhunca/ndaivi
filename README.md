# NDAIVI - Python Scraping Engine for Manuals

## Project Overview
NDAIVI is an automated scraping engine designed to extract user manuals from competitor and manufacturer websites. The system populates an SQLite database that can be used by a React frontend and Flask admin system. The primary goal is to self-populate and maintain a manuals database with minimal manual intervention.

## Environment
- Runs as root on a Debian VM
- Clonable to DigitalOcean
- Uses GitHub for version control
- Configured with cron for auto-pulling updates

## Tech Stack
- **Language**: Python 3.10+
- **Libraries**:
  - requests, beautifulsoup4: HTTP requests and HTML parsing
  - selenium: Dynamic sites and captcha handling (with ChromeDriver)
  - sqlalchemy: SQLite database management
  - python-dotenv: Load API keys from .env
  - anthropic: AI analysis with Claude API
  - schedule: Periodic updates
  - yaml: Configuration management
- **Database**: SQLite (manuals.db)
- **Virtual Environment**: `/var/ndaivimanuales/ndaivi`

## Project Structure
```
/var/ndaivimanuales/
├── config.yaml           # Main configuration file
├── main.py               # Entry point for the scraper
├── run_claude_scraper.py # Script to run Claude-based scraping
├── cli.py                # Command-line interface
├── utils.py              # Utility functions
├── validate_data.py      # Data validation tools
├── database/             # Database components
│   ├── __init__.py
│   ├── db_manager.py     # Database connection manager
│   └── schema.py         # Database schema definitions
├── scraper/              # Scraping components
│   ├── __init__.py
│   ├── competitor_scraper.py # Main scraper for competitor sites
│   ├── claude_analyzer.py    # Claude AI integration
│   └── prompt_templates.py   # Templates for Claude prompts
├── logs/                 # Log files directory
│   └── ndaivi.log        # Main log file
└── docs/                 # Documentation
```

## Implemented Functionality

### 1. Competitor Scraping Engine
- **Input**: Competitor URL (e.g., https://www.manualslib.com)
- **Task**: Crawls site, extracts manufacturers/products, stores in SQLite
- **Features**:
  - Respects robots.txt (configurable)
  - Configurable crawl depth and delay
  - Sitemap generation
  - Keyword filtering for relevant pages

### 2. Claude AI Integration
- Uses Anthropic's Claude API for intelligent data extraction
- Specialized prompt templates for consistent data formatting
- Two main functions:
  - Manufacturer page detection
  - Manufacturer data extraction (name, categories, website)
- Strict delimited response format for reliable parsing

### 3. Database Management
- SQLite database with optimized configuration
- Schema includes:
  - Products
  - Manufacturers
  - Categories (with language support)
  - CrawlStatus for tracking visited URLs
  - ScraperLog for logging events
- Thread-safe database manager to prevent locks

### 4. Monitoring System
- Comprehensive logging to file and database
- Statistics tracking and reporting
- Health monitoring

## Configuration
The system is configured via `config.yaml` which includes:
- Target website URL
- Database path
- AI API settings (Anthropic Claude)
- Crawling parameters (depth, delay, user agent)
- Language settings for translations
- Keyword filters

## Usage
```bash
# Basic usage
python main.py

# With custom config
python main.py --config custom_config.yaml

# Validate config only
python main.py --validate-only

# Display statistics only
python main.py --stats-only
```

## Logging
Logs are stored in the `logs/` directory:
- `ndaivi.log`: Main application log
- `competitor_scraper.log`: Detailed scraping logs

## Database Schema
- **products**: Stores product information
- **manufacturers**: Stores manufacturer information
- **categories**: Stores category information with language support
- **crawl_status**: Tracks URL crawling status
- **scraper_logs**: Stores application logs

## AI Response Format
The system uses a strict delimited format for Claude API responses:

1. Manufacturer Detection Format:
```
# RESPONSE_START
IS_MANUFACTURER: yes  # or 'no'
# RESPONSE_END
```

2. Manufacturer Data Format:
```
# RESPONSE_START
# MANUFACTURER_START
NAME: ManufacturerName
CATEGORY: ManufacturerName Category1
WEBSITE: https://example.com
# MANUFACTURER_END
# RESPONSE_END
```

## License
Proprietary - All rights reserved

## Future Plans
-Stage 2 of the Scrapping Engine: Takes the extracted manufacturers and finds their websites, completing the database.