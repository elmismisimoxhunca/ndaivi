# NDAIVI Modular Web Intelligence Daemon

## Overview
NDAIVI is a modular, daemonized system for large-scale web crawling and analysis, designed for reliability, extensibility, and data integrity.

## Architecture
- **main.go**: Orchestrates the system, manages logs, coordinates crawler/analyzer, and handles state.
- **scraper/web_crawler.py**: Standalone crawler, python-based microservice that are ran by mai.go
- **Analyzer**: Receives URLs from Go, analyzes, results sent to PostgreSQL and JSON state, by using main.go (Analyser just outputs its log and main.go does the work)

## Workflow
1. Start the daemon: `go run main.go --config config.yaml`
2. Daemon launches crawler (Python), monitors logs.
3. Crawler writes URLs to SQLite (no “ANALYSED” flag).
4. Daemon reads SQLite (read-only), and populates a JSON with all discovered links ordered by priority and the analysed or not flag.
5. Daemon sends links to analyzer, updates JSON and PostgreSQL.
6. When all links are analyzed, re-read SQLite. If none, sleep and retry.

## Logs
- `logs/ndaivi.log`: Daemon status
- `logs/crawler.log`: Crawler output
- `logs/analyzer.log`: Analyzer output

## Configuration
All settings in `config.yaml`.
