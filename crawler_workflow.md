# NDAIVI Web Crawler Workflow

## Overview

The NDAIVI Web Crawler is designed to operate as a modular component within the larger NDAIVI system. The crawler communicates with other components via Redis, allowing for a decoupled architecture where components can be started, stopped, and monitored independently.

## Workflow

1. **Configuration**: The system gets ALL of its configuration from `config.yaml`
2. **Crawler Operation**: Once started, the crawler runs, resuming from where it left off according to the database
3. **URL Processing**: The container app requests URLs using Redis and forwards them to the analyzer
4. **Analysis Feedback**: When a URL is processed, the analyzer messages the crawler that a website has been analyzed
5. **Backlog Management**: The container app monitors the backlog size and requests more URLs when needed:
   - If the backlog falls below the minimum threshold (default: 128), more URLs are requested
   - The crawler fills the backlog until it reaches the target size (default: 1024)
6. **Background Operation**: This system can run in the background with status checks available at any time

## Components

### Container App (`container_app.py`)

The container app is the central coordinator that:
- Manages communication between the crawler and analyzer
- Monitors the backlog size and requests more URLs when needed
- Provides status updates and statistics
- Handles start/stop commands for the crawler

### Crawler Worker (`crawler_worker.py`)

The crawler worker:
- Listens for commands via Redis
- Manages the URL queue and visited URLs
- Crawls websites according to configuration settings
- Responds to backlog requests
- Publishes status updates

### Web Crawler (`web_crawler.py`)

The core crawler implementation that:
- Handles the actual crawling logic
- Manages URL priorities
- Extracts content from websites
- Maintains crawling statistics

## Redis Communication Channels

The system uses the following Redis channels for communication:

- `ndaivi:crawler:commands` - Commands for the crawler (start, stop, pause, resume)
- `ndaivi:crawler:status` - Status updates from the crawler
- `ndaivi:analyzer:commands` - Commands for the analyzer
- `ndaivi:analyzer:status` - Status updates from the analyzer
- `ndaivi:analyzer:results` - Results from the analyzer
- `ndaivi:backlog:status` - Status updates about the backlog size
- `ndaivi:backlog:request` - Requests for more URLs to fill the backlog
- `ndaivi:stats` - System-wide statistics
- `ndaivi:system` - System-wide status updates

## Usage

### Starting the System

To start the entire system:

```bash
python main.py start --all
```

To start individual components:

```bash
python main.py start --container  # Start the container app
python main.py start --crawler    # Start the crawler
python main.py start --analyzer   # Start the analyzer
```

### Starting a Crawl Job

To start a crawl job with custom parameters:

```bash
python main.py crawl --url https://example.com --max-urls 1000 --max-depth 3
```

Or use the defaults from `config.yaml`:

```bash
python main.py crawl
```

### Checking Status

To check the system status:

```bash
python main.py status
```

### Stopping the System

To stop the entire system:

```bash
python main.py stop --all
```

Or stop individual components:

```bash
python main.py stop --container  # Stop the container app
python main.py stop --crawler    # Stop the crawler
python main.py stop --analyzer   # Stop the analyzer
```

## Configuration

All configuration is stored in `config.yaml`. Key settings for the crawler workflow include:

```yaml
web_crawler:
  target_website: https://example.com
  max_urls: 10000
  max_depth: 3
  backlog_min_threshold: 128
  backlog_target_size: 1024
  backlog_check_interval: 30

application:
  channels:
    crawler_commands: ndaivi:crawler:commands
    crawler_status: ndaivi:crawler:status
    analyzer_commands: ndaivi:analyzer:commands
    analyzer_status: ndaivi:analyzer:status
    analyzer_results: ndaivi:analyzer:results
    backlog_status: ndaivi:backlog:status
    backlog_request: ndaivi:backlog:request
    stats: ndaivi:stats
    system: ndaivi:system
```

## Benefits of This Workflow

1. **Modularity**: Components can be developed, deployed, and scaled independently
2. **Resilience**: The system can recover from failures and resume where it left off
3. **Observability**: Status updates and statistics are available at any time
4. **Flexibility**: Configuration can be changed without modifying code
5. **Efficiency**: The backlog management ensures optimal resource utilization
