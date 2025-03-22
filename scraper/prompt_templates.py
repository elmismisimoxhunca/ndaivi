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

# Translation prompt templates
TRANSLATION_BATCH_PROMPT = """
Translate these product categories to {target_language}. Keep manufacturer names unchanged but position them naturally.

# CATEGORIES
{categories}

VERY IMPORTANT RULES:
1. Keep manufacturer names untranslated but position them naturally in the target language:
   - Spanish: 'Laptops de {manufacturer}' or '{manufacturer} Laptops'
   - Portuguese: 'Laptops da {manufacturer}' or '{manufacturer} Laptops'
   - French: 'Ordinateurs portables {manufacturer}' or 'Laptops {manufacturer}'
   - Italian: 'Laptop {manufacturer}' or 'Laptop della {manufacturer}'
2. Maintain product type accuracy
3. Use standard industry terminology
4. Keep translations concise
5. ALWAYS include the manufacturer name in each translation
6. Position the manufacturer name according to what sounds most natural in the target language

Examples:
- 'Acer Laptops' → Spanish: 'Laptops de Acer' or 'Portátiles de Acer'
- 'Samsung TVs' → French: 'Téléviseurs Samsung'
- 'LG Monitors' → Portuguese: 'Monitores da LG'
- 'Dell Servers' → Italian: 'Server Dell'

Respond with translations in this EXACT format:
# RESPONSE_START
Translated Category 1 with manufacturer
Translated Category 2 with manufacturer
# RESPONSE_END
"""

# Legacy single translation prompt (for fallback)
TRANSLATION_PROMPT = """
Translate this product category to {target_language}, positioning the manufacturer name naturally.

Category: {category}

Rules for manufacturer name positioning:
- Spanish: 'Laptops de {manufacturer}' or '{manufacturer} Laptops'
- Portuguese: 'Laptops da {manufacturer}' or '{manufacturer} Laptops'
- French: 'Ordinateurs portables {manufacturer}' or 'Laptops {manufacturer}'
- Italian: 'Laptop {manufacturer}' or 'Laptop della {manufacturer}'

ALWAYS keep manufacturer names untranslated but position them according to target language norms.

# RESPONSE_START
TRANSLATED: <translation with manufacturer name positioned naturally>
# RESPONSE_END
"""

# Category extraction prompt template
CATEGORY_EXTRACTION_PROMPT = """Analyze this structured web page content to identify ONLY legitimate product categories for manufacturer: {manufacturer_name}

Page Information:
{content}

Important Instructions:
1. Focus ONLY on actual product categories (like 'Refrigerators', 'TVs', 'Washing Machines')
2. ONLY extract product categories that belong to {manufacturer_name} - DO NOT extract any other manufacturers
3. STRICTLY IGNORE the following:
   - UI elements (Home, Menu, Search, Login, Contact Us, etc.)
   - Navigation controls (Next, Previous, View All, etc.)
   - Individual products or model numbers
   - Accessories or parts
   - Footer links (Privacy Policy, Terms of Service, etc.)
   - Social media links
   - Website sections that aren't product categories
4. Only include REAL PRODUCT CATEGORIES, not website navigation elements
5. Ensure each category represents an actual product line or type
6. DO NOT extract other manufacturers or brands - ONLY extract categories for {manufacturer_name}

Provide your analysis in the specified delimited format:
# RESPONSE_START
CATEGORY: {manufacturer_name} Category 1
CATEGORY: {manufacturer_name} Category 2
# RESPONSE_END

If no categories found, respond with empty delimiters:
# RESPONSE_START
# RESPONSE_END
"""

# Category validation prompt template
CATEGORY_VALIDATION_PROMPT = """You are a specialized AI trained to validate product categories for manufacturers.

I have a list of potential product categories for {manufacturer} that need validation.
Your task is to review this list and identify ONLY the legitimate product categories.

VERY IMPORTANT RULES:
1. ONLY include actual product categories (like 'Refrigerators', 'TVs', 'Washing Machines')
2. ALWAYS include the manufacturer name in each category (e.g., 'Acer Laptops', 'Samsung TVs')
3. Position the manufacturer name naturally:
   - English: '{manufacturer} Laptops' or 'Laptops by {manufacturer}'
   - Spanish: 'Laptops de {manufacturer}' or '{manufacturer} Laptops'
4. REMOVE any website UI elements such as:
   - Navigation buttons (Home, Menu, Search, Login, Contact Us)
   - Control elements (Next, Previous, View All)
   - Footer links (Privacy Policy, Terms of Service)
   - Social media links
5. REMOVE any individual products or model numbers
6. REMOVE accessories or parts unless they form a distinct product category
7. REMOVE marketing terms, slogans, or campaign names
8. NORMALIZE similar categories (e.g., combine 'TV' and 'Television')

Here is the list of potential categories to validate:
{categories}

ALWAYS respond in this EXACT delimited format:

# RESPONSE_START
CATEGORY: {manufacturer} Valid Category 1
CATEGORY: Valid Category 2 by {manufacturer}
# RESPONSE_END

If no valid categories found, respond with empty delimiters.
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

CATEGORY_SYSTEM_PROMPT = """You are a specialized AI trained to analyze web content and identify ONLY legitimate product categories for a specific manufacturer.
Your task is to analyze structured navigation data from web pages and identify REAL product categories belonging to the manufacturer.

VERY IMPORTANT RULES:
1. ONLY extract actual product categories (like 'Refrigerators', 'TVs', 'Washing Machines', 'Home Automation Systems')
2. NEVER include website UI elements such as:
   - Navigation buttons (Home, Menu, Search, Login, Contact Us)
   - Control elements (Next, Previous, View All)
   - Footer links (Privacy Policy, Terms of Service)
   - Social media links
3. NEVER include individual products or model numbers
4. NEVER include accessories or parts unless they form a distinct product category
5. ALWAYS ensure each category represents an actual product line or type
6. DO NOT extract other manufacturers or brands - ONLY extract categories for the specified manufacturer
7. DO NOT filter out legitimate product categories that contain words like 'home' if they are actual product lines (e.g., 'Home Automation Systems' is valid)

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
