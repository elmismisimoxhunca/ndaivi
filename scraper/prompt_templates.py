# Claude AI Prompt Templates

# Manufacturer detection prompt template
MANUFACTURER_DETECTION_PROMPT = """
URL: {url}
Title: {title}

Content: {truncated_content}

Question: Is this webpage about a manufacturer or does it contain a list of manufacturers or brands?

Respond using this exact format without any explanations:

# RESPONSE_START
IS_MANUFACTURER: yes  # Replace with 'no' if not a manufacturer page
# RESPONSE_END
"""

# Manufacturer extraction prompt template
MANUFACTURER_EXTRACTION_PROMPT = """
URL: {url}
Title: {title}

Content: {truncated_content}

Task: Extract manufacturer names and product categories from this webpage.

Rules:
1. Each category MUST include the manufacturer name (e.g., "Samsung TVs" not just "TVs")
2. Each manufacturer must have at least one category
3. Website URL is optional
4. Do not include any text outside the delimited format

Respond using this exact format:

# RESPONSE_START
# MANUFACTURER_START
NAME: ManufacturerName
CATEGORY: ManufacturerName Category1
CATEGORY: ManufacturerName Category2
WEBSITE: https://manufacturer-website.com
# MANUFACTURER_END
# RESPONSE_END

If no manufacturers found, respond with empty markers:
# RESPONSE_START
# RESPONSE_END

Examples of good and bad categories:
- GOOD: "LG Smart Refrigerators", "Samsung QLED TVs", "Sony Wireless Headphones"
- BAD: "Refrigerators", "TVs", "Headphones"

Rules:
1. Each category MUST include the manufacturer name (e.g., "Samsung TVs" not just "TVs")
2. Make categories as specific as possible (e.g., "Samsung 4K Smart TVs" instead of just "Samsung TVs")
3. Each manufacturer must have at least one category
4. Website URL is optional
5. Do not include any text outside the delimited format
6. If no manufacturers found, respond with empty markers
7. Repeat the MANUFACTURER_START/END block for each manufacturer found
"""

# System prompt for all Claude API calls
SYSTEM_PROMPT = """You are a data extraction assistant that ONLY responds in a strict delimited format.
Never include explanations or text outside the delimited format.
Always wrap responses between # RESPONSE_START and # RESPONSE_END markers.

For manufacturer detection:
# RESPONSE_START
IS_MANUFACTURER: yes  # or 'no'
# RESPONSE_END

For manufacturer data extraction:
# RESPONSE_START
# MANUFACTURER_START
NAME: ManufacturerName
CATEGORY: ManufacturerName Category1
WEBSITE: https://example.com
# MANUFACTURER_END
# RESPONSE_END
"""
