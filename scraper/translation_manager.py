#!/usr/bin/env python3
"""
Translation Manager for NDAIVI.

This module provides translation functionality for product categories and other text
using Claude API. It includes caching to avoid redundant API calls.
"""

import json
import logging
from typing import Dict, Optional

from utils.config_manager import ConfigManager


class TranslationManager:
    """
    Translation Manager for NDAIVI.
    
    This class provides translation functionality using Claude API with caching
    to avoid redundant API calls.
    """
    
    def __init__(self):
        """Initialize the Translation Manager."""
        self.logger = logging.getLogger(__name__)
        self.config_manager = ConfigManager()
        
        # Translation settings
        self.enabled = self.config_manager.get('translation.enabled', False)
        self.default_source_lang = self.config_manager.get('translation.default_source_lang', 'en')
        self.default_target_lang = self.config_manager.get('translation.default_target_lang', 'en')
        
        # Cache for translations
        self.cache = {}
        
        # Claude API client
        try:
            from anthropic import Anthropic
            self.client = Anthropic(api_key=self.config_manager.get('claude.api_key'))
            self.model = self.config_manager.get('claude.translation_model', 'claude-3-haiku-20240307')
            self.logger.info(f"Translation Manager initialized with model: {self.model}")
        except ImportError:
            self.logger.error("Anthropic package not installed. Translation will not work.")
            self.client = None
        except Exception as e:
            self.logger.error(f"Error initializing Claude client: {str(e)}")
            self.client = None
    
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
        if not self.enabled or not self.client:
            return text
        
        # Use default languages if not specified
        source_lang = source_lang or self.default_source_lang
        target_lang = target_lang or self.default_target_lang
        
        # No need to translate if source and target are the same
        if source_lang == target_lang:
            return text
        
        # Check cache first
        cache_key = f"{text}_{source_lang}_{target_lang}"
        if cache_key in self.cache:
            self.logger.debug(f"Translation cache hit for: {text}")
            return self.cache[cache_key]
        
        self.logger.info(f"Translating from {source_lang} to {target_lang}: {text}")
        
        # Prepare prompt
        system_prompt = """You are a translation assistant. Your task is to translate text from one language to another.
Respond ONLY with the translated text, no other text or explanation.
"""
        
        user_prompt = f"""Translate the following text from {source_lang} to {target_lang}:
{text}

Respond ONLY with the translated text.
"""
        
        # Call Claude API
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1000,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}]
            )
            
            # Clean up response
            translated = response.content[0].text.strip()
            
            # Cache the result
            self.cache[cache_key] = translated
            
            self.logger.debug(f"Translated '{text}' to '{translated}'")
            return translated
        except Exception as e:
            self.logger.error(f"Error translating text: {str(e)}")
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
        if not self.enabled or not self.client:
            return items
        
        translated_items = []
        for item in items:
            if isinstance(item, str) and item:
                translated = self.translate(item, source_lang, target_lang)
                translated_items.append(translated)
            else:
                translated_items.append(item)
        
        return translated_items
