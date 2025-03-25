# NDAIVI Web Crawler

## Overview

The NDAIVI Web Crawler is a powerful, configurable web crawling solution with database integration for state persistence. It's designed to efficiently crawl websites while respecting robots.txt rules and domain restrictions.

## Features

- **Database Integration**: SQLite database for persistent storage of crawl state
- **Domain Restriction**: Option to restrict crawling to specified domains
- **Single Link Propagation**: Ensures crawling starts from a single link
- **Stats Monitoring**: Regular stats events for monitoring crawl progress
- **Configuration Management**: Centralized configuration via config.yaml
- **Priority Queue**: Intelligent URL prioritization for efficient crawling
- **Robust Error Handling**: Comprehensive error management and logging

## Usage

### Basic Usage

To run the crawler with default settings:

```bash
./scraper/run_crawler.py https://example.com
```

### Advanced Usage

```bash
./scraper/run_crawler.py https://example.com \
  --max-urls 100 \
  --max-time 300 \
  --max-depth 3 \
  --single-domain \
  --delay 2.0 \
  --allowed-domains example.com sub.example.com
```

### Command-Line Options

- `start_url`: URL to start crawling from (required)
- `--max-urls`: Maximum number of URLs to process
- `--max-time`: Maximum time to run in seconds
- `--max-depth`: Maximum crawl depth
- `--single-domain`: Restrict to the start domain only
- `--db-path`: Path to the crawler database
- `--delay`: Delay between requests in seconds
- `--user-agent`: User agent string to use
- `--allowed-domains`: List of allowed domains
- `--disallowed-domains`: List of disallowed domains

## Monitoring

You can monitor the crawler's progress in real-time using the monitor script:

```bash
./scraper/monitor_crawler.py
```

This will display live statistics about the crawling process, including:
- URLs queued
- URLs processed
- URLs failed
- Domains discovered
- Crawl rate

## Testing

To run a quick test of the crawler:

```bash
./scraper/test_crawler.py
```

This will crawl a few pages from example.com to verify that the crawler is working correctly.

## Configuration

The crawler is configured via the `web_crawler` section in `config.yaml`. Key configuration options include:

```yaml
web_crawler:
  # Crawler behavior
  max_depth: 3
  max_urls_per_domain: 100
  request_delay: 1.0
  timeout: 30
  
  # Domain restrictions
  allowed_domains: []
  disallowed_domains: []
  
  # Database and state
  db_path: 'scraper/data/crawler.db'
  
  # Crawl mode
  restrict_to_start_domain: true
  single_domain_mode: true
  
  # Stats
  stats_update_interval: 10
```

## Database Schema

The crawler uses a SQLite database with the following tables:

- `visited_urls`: Stores information about processed URLs
- `url_queue`: Manages the queue of URLs to be processed
- `domains`: Tracks discovered domains
- `stats`: Stores crawl statistics

## Integration

The web crawler can be integrated with other components of the NDAIVI system by:

1. Importing the WebCrawler class
2. Configuring it with appropriate settings
3. Adding callbacks for processing extracted content

Example:

```python
from scraper.web_crawler import WebCrawler
from utils.config_manager import ConfigManager

config_manager = ConfigManager()
crawler_config = config_manager.get_config('web_crawler')

crawler = WebCrawler(
    config=crawler_config,
    stats_callback=my_stats_callback
)

crawler.add_url("https://example.com")
crawler.crawl(max_urls=100)
```
