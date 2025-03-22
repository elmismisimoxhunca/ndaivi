"""Prompt templates for the Claude API interactions"""

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
MANUFACTURER_EXTRACTION_PROMPT = """Analyze this structured web page content to identify manufacturers/brands ONLY.

Page Information:
{content}

Important Instructions:
1. Focus on links in navigation menus, breadcrumbs, and manufacturer sections
2. Use URL patterns (e.g., /brands/, /manufacturer/) to validate manufacturers
3. Look for manufacturer names in the page title and headings
4. Only include actual manufacturers/brands, not retailers or generic categories

Provide your analysis in the specified delimited format:
# RESPONSE_START
# MANUFACTURER_START
NAME: Manufacturer Name
URL: URL1
URL: URL2
# MANUFACTURER_END
# RESPONSE_END

If no manufacturers found, respond with empty delimiters:
# RESPONSE_START
# RESPONSE_END
"""

# Category validation prompt template
CATEGORY_VALIDATION_PROMPT = """Validate and normalize these potential product categories for {manufacturer}:

Categories to validate:
{categories}

Rules:
1. Remove generic terms not specific to {manufacturer}
2. Standardize format (e.g., "{manufacturer} TVs" not "TVs by {manufacturer}")
3. Remove duplicates and similar categories
4. Ensure categories are actual product lines, not marketing terms

Respond with only the validated categories in the delimited format:
# RESPONSE_START
CATEGORY: Validated Category 1
CATEGORY: Validated Category 2
# RESPONSE_END
"""

# Translation prompt template
TRANSLATION_PROMPT = """
Translate this product category to {target_language}:
{category}

Rules:
1. Keep the manufacturer name EXACTLY as is - do not translate it
2. Format appropriately for the target language:
   - Spanish: "[Manufacturer] de [Product]" or "[Product] de [Manufacturer]"
   - Portuguese: "[Product] da [Manufacturer]"
   - French: "[Product] de [Manufacturer]"
   - Italian: "[Product] di [Manufacturer]"
3. Use simple, clear product terms
4. No commentary or descriptions

Examples for Spanish:
- "AEG Refrigerators" -> "Refrigeradores de AEG"
- "Samsung TVs" -> "Televisores de Samsung"

Examples for Portuguese:
- "AEG Refrigerators" -> "Refrigeradores da AEG"
- "Samsung TVs" -> "Televisores da Samsung"

Respond using this exact format:

# RESPONSE_START
TRANSLATED: [your translation]
# RESPONSE_END
"""

# Category extraction prompt template
CATEGORY_EXTRACTION_PROMPT = """Analyze this structured web page content to identify product categories for manufacturer: {manufacturer_name}

Page Information:
{content}

Important Instructions:
1. Focus on links in navigation menus, breadcrumbs, and category sections
2. Look for product categories that belong to {manufacturer_name}
3. Ignore individual products, model numbers, or accessories
4. Only include categories, not other types of content

Provide your analysis in the specified delimited format:
# RESPONSE_START
CATEGORY: {manufacturer_name} Category 1
CATEGORY: {manufacturer_name} Category 2
# RESPONSE_END

If no categories found, respond with empty delimiters:
# RESPONSE_START
# RESPONSE_END
"""

# System prompts
MANUFACTURER_SYSTEM_PROMPT = """You are a specialized AI trained to analyze web content and identify manufacturers and their product categories.
Your task is to analyze structured navigation data from web pages and identify manufacturers/brands ONLY.

ALWAYS respond in this EXACT delimited format:

# RESPONSE_START
# MANUFACTURER_START
NAME: Manufacturer Name
URL: URL1
URL: URL2
# MANUFACTURER_END
# RESPONSE_END

If no manufacturers found, respond with empty delimiters:
# RESPONSE_START
# RESPONSE_END

Rules:
1. Only include ACTUAL manufacturers, not generic categories or other entities
2. Look for patterns in URLs that indicate manufacturer pages (e.g., /brands/, /manufacturer/)
3. Consider breadcrumb paths to understand the site hierarchy
4. Normalize manufacturer names (remove "Inc.", "Corp.", etc.)"""

CATEGORY_SYSTEM_PROMPT = """You are a specialized AI trained to analyze web content and identify product categories for a specific manufacturer.
Your task is to analyze structured navigation data from web pages and identify product categories belonging to the manufacturer.

ALWAYS respond in this EXACT delimited format:

# RESPONSE_START
CATEGORY: Category 1
CATEGORY: Category 2
# RESPONSE_END

If no categories found, respond with empty delimiters:
# RESPONSE_START
# RESPONSE_END

Rules:
1. Only include ACTUAL product categories, not individual products or model numbers
2. Categories should be specific to the manufacturer (e.g., "Sony TVs", not just "TVs")
3. Always prefix categories with the manufacturer name
4. Focus on general product lines, not marketing terms or collections"""

# General system prompt used for other tasks
SYSTEM_PROMPT = """You are Claude, an AI assistant specialized in analyzing web content to extract structured information about manufacturers and their product categories.

Your task is to carefully analyze the provided content and extract the requested information following the exact format specified in each instruction.

Respond ONLY with the requested information in the delimited format. Do not add any explanations, comments, or extra text outside the delimiters.

Be precise, accurate, and follow instructions exactly."""
