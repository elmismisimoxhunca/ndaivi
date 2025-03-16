# Claude Integration Guide

## Overview

The NDAIVI scraping engine now exclusively uses Anthropic's Claude AI models for analyzing web pages and extracting manufacturer information. This document explains how the Claude integration works and how to interpret the YAML output format.

## Claude Models Used

The system uses two Claude models for different purposes:

1. **Claude 3 Haiku** (claude-3-haiku-20240307): A faster, more efficient model used for initial classification of pages to determine if they contain manufacturer information.

2. **Claude 3.7 Sonnet** (claude-3-7-sonnet-20250219): A more powerful model used for detailed extraction of manufacturer names, categories, and website URLs.

## YAML Output Format

All AI analysis results are now returned in YAML format for better readability and easier parsing. Here's an example of the output format:

```yaml
manufacturers:
  - name: Samsung Electronics
    categories:
      - Samsung mobile phones
      - Samsung smartphones
      - Samsung TVs
      - Samsung home electronics
    website: https://www.samsung.com
  - name: LG Electronics
    categories:
      - LG washing machines
      - LG refrigerators
      - LG air conditioners
    website: https://www.lg.com
```

### Format Details

- The top-level key is always `manufacturers`, which contains a list of manufacturer objects.
- Each manufacturer object has the following properties:
  - `name`: The name of the manufacturer (required)
  - `categories`: A list of product categories associated with the manufacturer (optional)
  - `website`: The manufacturer's website URL if available (optional)

## Integration Components

### ClaudeAnalyzer Class

The `ClaudeAnalyzer` class in `scraper/claude_analyzer.py` handles all interactions with the Claude API. It provides two main methods:

1. `is_manufacturer_page(url, title, content)`: Uses Claude Haiku to determine if a page contains manufacturer information.

2. `extract_manufacturers(url, title, content)`: Uses Claude Sonnet to extract detailed manufacturer information in YAML format.

### YAML Parsing

The system includes robust YAML parsing capabilities that can handle various response formats:

- YAML content between triple backticks (```yaml...```)
- YAML content with document separators (---...---)
- Plain YAML content

## Configuration

The Claude API configuration is stored in `config.yaml` under the `ai_apis.anthropic` section:

```yaml
ai_apis:
  anthropic:
    api_key: ${ANTHROPIC_API_KEY}  # Will be loaded from environment variable
    model: claude-3-haiku-20240307
    sonnet_model: claude-3-7-sonnet-20250219
```

## Running the Scraper

To run the Claude-powered scraper:

```bash
python run_claude_scraper.py
```

This will use the Claude models to analyze web pages and extract manufacturer information in YAML format.

## Troubleshooting

If you encounter issues with the Claude integration:

1. Check that your Anthropic API key is correctly set in the `.env` file or as an environment variable.
2. Verify that the model names in `config.yaml` are correct and up-to-date.
3. Check the logs for any error messages from the Claude API.
4. If you're getting parsing errors, examine the raw response from Claude to see if it's not following the expected YAML format.

## Benefits of YAML Format

- **Human-readable**: YAML is easy to read and understand.
- **Structured data**: YAML provides a clear structure for the extracted information.
- **Compatibility**: YAML is widely supported by many programming languages and tools.
- **Flexibility**: The format can be easily extended to include additional information in the future.
