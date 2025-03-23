#!/usr/bin/env python3
"""
Translation Manager for NDAIVI.

This module provides translation functionality for product categories and other text
using Claude API. It includes caching to avoid redundant API calls.
"""

import json
import logging
import os
import requests
from typing import Dict, List, Optional

from utils.config_manager import ConfigManager


class TranslationManager:
    """
    Translation Manager for NDAIVI.
    
    This class provides translation functionality using Claude API with caching
    to avoid redundant API calls.
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize the Translation Manager.
        
        Args:
            config_path: Optional path to configuration file
        """
        self.logger = logging.getLogger(__name__)
        self.config_manager = ConfigManager(config_path)
        
        # Translation settings
        self.enabled = self.config_manager.get('translation.enabled', True)
        self.default_source_lang = self.config_manager.get('translation.default_source_lang', 'en')
        self.default_target_lang = self.config_manager.get('translation.default_target_lang', 'es')
        self.batch_size = self.config_manager.get('translation.batch_size', 50)
        
        # Cache for translations
        self.cache_enabled = self.config_manager.get('translation.cache_enabled', True)
        self.cache = {}
        
        # Claude API configuration
        try:
            # Get API key from environment or config, using the same approach as ClaudeAnalyzer
            self.api_key = os.environ.get('CLAUDE_API_KEY')
            if not self.api_key:
                self.api_key = self.config_manager.get('claude_analyzer.api_key', '')
                
            if not self.api_key and not self.api_key.strip():
                self.logger.warning("No Claude API key found. Translation will not work without a valid API key.")
                self.enabled = False
                
            # Get target languages
            self.target_languages = self.config_manager.get('languages.targets', {})
            if not self.target_languages:
                self.logger.warning("No target languages configured. Translation will be limited.")
                
            # Get translation prompt template
            self.translation_prompt = self.config_manager.get('prompt_templates.translate_categories', '')
            if not self.translation_prompt:
                self.logger.warning("No translation prompt template found. Using default template.")
                self.translation_prompt = """
                Translate the following product categories from {source_language} to {target_language}.
                Preserve any brand names or proper nouns without translation.
                
                Categories:
                {categories}
                
                Respond in JSON format:
                {
                  "translations": [
                    "Translated Category 1",
                    "Translated Category 2",
                    ...
                  ]
                }
                """
                
            self.logger.info(f"Translation manager initialized. Enabled: {self.enabled}")
            self.logger.info(f"Target languages: {', '.join(self.target_languages.keys()) if self.target_languages else 'None'}")
            
        except Exception as e:
            self.logger.error(f"Error initializing translation manager: {str(e)}")
            self.enabled = False

    def _test_api_connection(self):
        """Test the connection to the Claude API to ensure it's working."""
        try:
            # Call translate with a simple test string
            test_result = self.translate("Hello", "en", "es")
            self.logger.info(f"API test successful: 'Hello' -> '{test_result}'")
        except Exception as e:
            self.logger.error(f"API test failed: {str(e)}")

    def translate(self, text: str, source_lang: Optional[str] = None, target_lang: Optional[str] = None) -> str:
        """
        Translate text from source language to target language.
        
        Args:
            text: Text to translate
            source_lang: Source language code (default from config)
            target_lang: Target language code (default from config)
            
        Returns:
            Translated text
        """
        if not self.api_key:
            self.logger.error("Translation failed: No API key available")
            return text
            
        if not self.enabled:
            self.logger.warning("Translation is disabled, returning original text")
            return text
        
        # Use default languages if not specified
        source_lang = source_lang or self.default_source_lang
        target_lang = target_lang or self.default_target_lang
        
        # No need to translate if source and target are the same
        if source_lang == target_lang:
            return text
        
        # Log text to translate
        self.logger.info(f"Translating text: '{text}' from {source_lang} to {target_lang}")
        
        # Check cache first
        cache_key = f"{text}_{source_lang}_{target_lang}"
        if self.cache_enabled and cache_key in self.cache:
            self.logger.debug(f"Translation cache hit for: {text}")
            return self.cache[cache_key]
        
        # Prepare prompt
        system_prompt = """You are a translation assistant. Your ONLY job is to translate the exact text provided.
Respond with ONLY the translated text. No explanations, no comments, no extra text.
"""
        
        user_prompt = f"""Translate the following text from {source_lang} to {target_lang}:

{text}

IMPORTANT: Your response must contain ONLY the translated text, nothing else.
"""
        
        # Call Claude API directly
        try:
            self.logger.info(f"Calling Claude API for translation of: '{text}'")
            
            headers = {
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            }
            
            payload = {
                "model": self.config_manager.get('claude_analyzer.model', 'claude-3-haiku-20240307'),
                "max_tokens": 1000,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}]
            }
            
            self.logger.debug(f"API URL: {self.config_manager.get('claude_analyzer.api_base_url', 'https://api.anthropic.com/v1/messages')}")
            
            response = requests.post(
                self.config_manager.get('claude_analyzer.api_base_url', 'https://api.anthropic.com/v1/messages'),
                headers=headers,
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            response_data = response.json()
            
            # Extract the translated text
            if 'content' in response_data and len(response_data['content']) > 0:
                translated = response_data['content'][0]['text'].strip()
                
                # Cache the result
                if self.cache_enabled:
                    self.cache[cache_key] = translated
                
                self.logger.info(f"Successfully translated: '{text}' -> '{translated}'")
                return translated
            else:
                self.logger.error(f"Invalid response format from Claude API: {response_data}")
                return text
                
        except Exception as e:
            self.logger.error(f"Error translating text: {str(e)}")
            # Print the full exception traceback for debugging
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return text

    def translate_list(self, items: list, source_lang: Optional[str] = None, target_lang: Optional[str] = None) -> list:
        """
        Translate a list of items from source language to target language.
        
        Args:
            items: List of items to translate
            source_lang: Source language code (default from config)
            target_lang: Target language code (default from config)
            
        Returns:
            List of translated items
        """
        if not self.api_key:
            self.logger.error("Translation failed: No API key available for batch translation")
            return items
            
        if not self.enabled:
            self.logger.warning("Translation is disabled, returning original items")
            return items
            
        if not items:
            return items
        
        # Use default languages if not specified
        source_lang = source_lang or self.default_source_lang
        target_lang = target_lang or self.default_target_lang
        
        self.logger.info(f"Batch translating {len(items)} items from {source_lang} to {target_lang}")
        
        # Translate each item individually for reliability
        translated_items = []
        for item in items:
            translated = self.translate(item, source_lang, target_lang)
            translated_items.append(translated)
            
        self.logger.info(f"Batch translation complete. Translated {len(items)} items.")
        
        return translated_items
