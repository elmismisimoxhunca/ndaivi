"""
Prompt templates for the NDAIVI competitor scraper.
"""

# Prompt for analyzing page type
PAGE_TYPE_ANALYSIS_PROMPT = """
Analyze this content and determine if it's:
1. A brand/manufacturer page (contains multiple product categories)
2. A brand category page (specific product category within a brand)
3. Inconclusive (not enough information to make a determination)
4. Other (not related to product categories)

Content:
{content}

Rules:
- ONLY respond with one of: "brand_page", "brand_category_page", "inconclusive", or "other"
- "brand_page" = Main manufacturer page listing multiple product categories
- "brand_category_page" = Page for a specific product category within a brand
- "inconclusive" = Not enough information to determine the page type
- "other" = Any other type of page (about, contact, etc.)
- Use "inconclusive" when you need more context to make a determination
- Use "other" when you are confident it's not brand-related
"""

# Prompt for extracting manufacturer data from a brand page
BRAND_PAGE_EXTRACTION_PROMPT = """
Extract manufacturer name and product categories from this brand page.

Content:
{content}

Rules:
1. ONLY extract categories that are clearly product lines or model types
2. IGNORE:
   - Individual product names/models
   - Support/service categories
   - Accessory categories
3. Format: Exactly as shown below, nothing else

# RESPONSE_START
MANUFACTURER: [name]
CATEGORIES:
- [category 1]
- [category 2]
WEBSITE: [official website if found]
# RESPONSE_END
"""

# Prompt for extracting category data from a category page
CATEGORY_PAGE_EXTRACTION_PROMPT = """
Extract manufacturer name and product category from this category page.

Content:
{content}

Rules:
1. ONLY extract the main category name
2. IGNORE subcategories and individual products
3. Format: Exactly as shown below, nothing else

# RESPONSE_START
MANUFACTURER: [name]
CATEGORY: [category name]
WEBSITE: [official website if found]
# RESPONSE_END
"""

# Batch translation prompt
TRANSLATION_BATCH_PROMPT = """
Translate these product categories to {target_language}. 
Manufacturer name: {manufacturer_name}

Categories:
{categories}

Rules:
1. Keep "{manufacturer_name}" untranslated
2. Format each line: "[Original] → [Translation]"
3. Preserve technical terms and model numbers
4. Use industry-standard translations
"""

# System prompts
MANUFACTURER_SYSTEM_PROMPT = """You are a specialized AI that analyzes web content to identify manufacturers and their product categories."""

PAGE_TYPE_SYSTEM_PROMPT = """You are a specialized AI that classifies pages as: brand pages, brand category pages, or other pages."""

TRANSLATION_SYSTEM_PROMPT = """You are a specialized AI that translates product categories while preserving manufacturer names."""

# General system prompt
SYSTEM_PROMPT = """You are an AI specialized in extracting structured information about manufacturers and their product categories from web content."""
