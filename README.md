# NDAIVI - Python Scraping Engine for Manuals

*Updated on March 22, 2025*

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
  - redis: Message-based communication between components
- **Database**: SQLite (manuals.db)
- **Message Broker**: Redis
- **Virtual Environment**: `/var/ndaivimanuales/ndaivi`

## Project Structure
```
/var/ndaivimanuales/
├── config.yaml           # Main configuration file
├── run.py                # Main application entry point with interactive CLI
├── utils/                # Utility components
│   ├── __init__.py
│   └── redis_manager.py  # Redis communication manager
├── database/             # Database components
│   ├── __init__.py
│   ├── db_manager.py     # Database connection manager
│   └── schema.py         # Database schema definitions
├── scraper/              # Scraping components
│   ├── __init__.py
│   ├── web_crawler.py    # Web crawler implementation
│   ├── crawler_worker.py # Worker for the web crawler
│   ├── claude_analyzer.py    # Claude AI integration
│   ├── analyzer_worker.py    # Worker for the Claude analyzer
│   ├── stats_manager.py      # Statistics tracking
│   └── prompt_templates.py   # Templates for Claude prompts
├── logs/                 # Log files directory
│   ├── ndaivi.log        # Main log file
│   └── crawler.log       # Crawler-specific log file
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
- Four-step analysis process:
  1. **Keyword Filter**: Quick filtering without API calls
  2. **Metadata Analysis**: Analyzes page metadata using Claude
  3. **Link Analysis**: Analyzes page links if metadata is inconclusive
  4. **Content Analysis**: Analyzes page content as a final step
- Strict delimited response format for reliable parsing
- Caching for API responses to reduce costs

### 3. Database Management
- SQLite database with optimized configuration
- Schema includes:
  - Products
  - Manufacturers
  - Categories (with language support)
  - CrawlStatus for tracking visited URLs
  - ScraperLog for logging events
- Thread-safe database manager to prevent locks

### 4. Redis-Based Messaging Architecture
- Decoupled components that communicate via Redis pub/sub channels
- Each component runs in its own thread for concurrent operation
- Key Redis channels:
  - `ndaivi:crawler:commands` - Commands for the crawler
  - `ndaivi:crawler:status` - Status updates from the crawler
  - `ndaivi:analyzer:commands` - Commands for the analyzer
  - `ndaivi:analyzer:status` - Status updates from the analyzer
  - `ndaivi:stats` - Statistics from all components
  - `ndaivi:system` - System-wide messages

### 5. Interactive CLI
- Simple command-line interface for controlling the system
- Commands: crawl, analyze, stats, status, help, exit
- Real-time status updates and statistics

### 6. Monitoring System
- Comprehensive logging to file and database
- Statistics tracking and reporting via Redis
- Health monitoring and status updates

## Configuration
The system is configured via `config.yaml` which includes:
- Target website URL
- Database path
- AI API settings (Anthropic Claude)
- Crawling parameters (depth, delay, user agent)
- Language settings for translations
- Keyword filters
- Redis connection settings

## Usage
```bash
# Start the application with interactive CLI
python run.py

# With custom config
python run.py --config custom_config.yaml
```

### Interactive CLI Commands
```
crawl <url> [max_urls] [max_depth] - Start crawling from a URL
analyze <url> - Analyze a specific URL with Claude
stats - Display current statistics
status - Show system status
help - Display available commands
exit - Exit the application
```

## Logging
Logs are stored in the `logs/` directory:
- `ndaivi.log`: Main application log
- `crawler.log`: Detailed crawler logs

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

-Stage 3 of the Scrapping Engine: Allows for optimized, faster operation, more advanced configuration and refinement, headless mode, start-stop-resume, supports HTTP and SOCKS proxy connection. And allows for remote operation, as well as switching between modes.

-Stage 4 of the Scrapping Engine: Scans all manufacturers and their websites for manuals, populating the database with the extracted information. It not only extracts the manual, but creates a new product entity for each manual, complete with:
-Natural product description
-Relevant info about the product and the company.
-SEO tags and keywords
-Image, derived from the internet or the manufacturers website, using Claude Haiku to validate. 
This can come out from the manuals themselves. Each manual has a separate entry for English and Spanish / Other specified target languages. It uses the already coded translation methods to render a separate translation, this time with CLaude Sonnet, which is more advanced. 

This is the most complex implementation, as each manufacturer website may vary and it has to adapt to all cases. It includes a robust 2captcha solver. 

-Stage 5: Wrapper / Motor: This provides an elegant, easy to use solution that executes all steps:
1: Scan competitors website for manufacturers and cateogries, translating them to spanish and other specified languages
2: Search and register for their official websites
3: Scan the websites for manuals and populate the database with the actual content that will be displayed to the user. 
All three steps are doing sequentially and in order, by cycles, in a configurable time allocation. User can configure how much time it will spend in each step. For example, of 1000 seconds, 100 dedicated to category extraction, 100 to website finding, and 800 to manuals extraction.

This will be the nucleus of the FLASK app, and will include an API for the Flask web app to work on.

-Stage 6: Flask-based web app for administrating the scrapping engine.
A functional, not so pretty system, for handling the scrapping and content population. 
-Controlling the scrapping system by the flask app, starting, stopping, resuming scrapping.
-Handle settings
-Read the scrapping statistics and logs
-Real time logs
-Accessing and reading info from the database. 
-Access file database (Stored PDFs) and their download links

-Stage 7: Postgresql implementation
AT this point, we will code a system to periodically populate the Postgresql implementation with the latest updates from the scrapping engine, which can be run from a different server than the frontend. It will query daily, updating its contents with the info gathered by the scrapping system.

The PDF files will be stored in the same directory in both frontend and backend and will be served as static content. At least for a while, as the websites need to increase in popularity.

-Stage 8: API that connects to Postgresql to build React-based website displaying the extracted manuals. 
