"""
Configuration manager for NDAIVI project.

This module provides a centralized way to load, validate, and access configuration settings.
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional

class ConfigManager:
    """
    Configuration manager for NDAIVI project.
    
    This class handles loading configuration from YAML files, validating settings,
    and providing a clean interface to access configuration values.
    """
    
    _instance = None
    
    def __new__(cls, config_path: str = None):
        """
        Implement singleton pattern to ensure only one config manager exists.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            ConfigManager instance
        """
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_path: str = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to configuration file
        """
        # Only initialize once
        if getattr(self, '_initialized', False):
            return
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        
        # Load configuration
        self._config_path = config_path
        self._config = self._load_config()
        
        # Mark as initialized
        self._initialized = True
        
        self.logger.info("Configuration manager initialized")
    
    def _load_config(self) -> Dict:
        """
        Load configuration from YAML file.
        
        Returns:
            Configuration dictionary
        """
        try:
            # Handle case where config_path is already a dictionary (direct config)
            if isinstance(self._config_path, dict):
                self.logger.info(f"Loading configuration from provided dictionary")
                config = self._config_path
                self._config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')
                return config
                
            # Use default config path if none provided
            if not self._config_path:
                self._config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')
            
            self.logger.info(f"Loading configuration from {self._config_path}")
            
            # Check if config file exists
            if not os.path.exists(self._config_path):
                self.logger.warning(f"Configuration file not found at {self._config_path}. Creating default configuration.")
                config = self._create_default_config()
                self._save_config(config)
                return config
            
            with open(self._config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Validate configuration
            self._validate_config(config)
            
            return config
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            # If we can't load the config, create a default one
            config = self._create_default_config()
            try:
                self._save_config(config)
            except Exception as save_error:
                self.logger.error(f"Failed to save default configuration: {save_error}")
            return config
    
    def _create_default_config(self) -> Dict:
        """
        Create default configuration.
        
        Returns:
            Default configuration dictionary
        """
        self.logger.info("Creating default configuration")
        
        # Default configuration focused on claude_analyzer and translation_manager
        default_config = {
            # Language configuration
            "languages": {
                "source": "en",
                "targets": {
                    "es": "Spanish"
                },
                "translation_batch_size": 50
            },
            
            # Translation configuration
            "translation": {
                "enabled": True,
                "default_source_lang": "en",
                "default_target_lang": "es",
                "batch_size": 50,
                "cache_enabled": True,
                "cache_ttl": 86400  # 24 hours
            },
            
            # Claude analyzer configuration
            "claude_analyzer": {
                "api_key": "",  # Should be set via environment variable
                "api_base_url": "https://api.anthropic.com/v1/messages",
                "claude_model": "claude-3-5-haiku-20241022",
                "enable_cache": True,
                "cache_ttl": 86400,
                "cache_dir": ".cache",
                "max_tokens": 4000,
                "temperature": 0.1,
                "min_confidence_threshold": 0.7
            },
            
            # Keywords for filtering
            "keywords": {
                "positive": [
                    "manufacturer",
                    "brand",
                    "companies",
                    "products",
                    "category",
                    "catalog",
                    "brands"
                ],
                "negative": [
                    "cart",
                    "checkout",
                    "login",
                    "register",
                    "account",
                    "privacy",
                    "terms"
                ]
            },
            
            # Prompt templates for Claude AI
            "prompt_templates": {
                "system": {
                    "manufacturer": "You are a specialized AI that analyzes web content to identify manufacturers and their product categories.",
                    "page_type": "You are a specialized AI that classifies pages as: brand pages, brand category pages, or other pages.",
                    "translation": "You are a specialized AI that translates product categories while preserving manufacturer names.",
                    "general": "You are an AI specialized in extracting structured information about manufacturers and their product categories from web content."
                },
                "page_type_analysis": "Analyze this content and determine if it's:\n1. A brand/manufacturer page (contains multiple product categories)\n2. A brand category page (specific product category within a brand)"
            }
        }
        
        return default_config
    
    def _save_config(self, config: Dict) -> None:
        """
        Save configuration to YAML file.
        
        Args:
            config: Configuration dictionary to save
        """
        try:
            # Ensure config_path is a valid string path
            if not isinstance(self._config_path, str):
                self._config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')
                self.logger.warning(f"Config path was not a string, using default path: {self._config_path}")
                
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self._config_path), exist_ok=True)
            
            with open(self._config_path, 'w') as f:
                yaml.dump(config, f, default_flow_style=False, sort_keys=False)
            
            self.logger.info(f"Configuration saved to {self._config_path}")
        except Exception as e:
            self.logger.error(f"Failed to save configuration: {e}")
    
    def _validate_config(self, config: Dict) -> None:
        """
        Validate configuration settings.
        
        Args:
            config: Configuration dictionary to validate
        """
        # Check for required sections
        required_sections = ['keywords', 'prompt_templates', 'claude_analyzer', 'translation', 'languages']
        for section in required_sections:
            if section not in config:
                self.logger.warning(f"Missing required configuration section: {section}")
                # Add default section if missing
                config[section] = self._create_default_config()[section]
        
        # Validate claude_analyzer section
        if 'claude_analyzer' in config:
            required_claude_keys = ['claude_model', 'enable_cache', 'cache_dir']
            for key in required_claude_keys:
                if key not in config['claude_analyzer']:
                    self.logger.warning(f"Missing required claude_analyzer configuration key: {key}")
                    # Add default key if missing
                    config['claude_analyzer'][key] = self._create_default_config()['claude_analyzer'][key]
        
        # Validate translation section
        if 'translation' in config:
            required_translation_keys = ['enabled', 'default_source_lang', 'default_target_lang', 'batch_size']
            for key in required_translation_keys:
                if key not in config['translation']:
                    self.logger.warning(f"Missing required translation configuration key: {key}")
                    # Add default key if missing
                    config['translation'][key] = self._create_default_config()['translation'][key]
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key.
        
        Args:
            key: Configuration key (dot notation supported for nested keys)
            default: Default value to return if key not found
            
        Returns:
            Configuration value or default
        """
        # Handle nested keys with dot notation
        if '.' in key:
            keys = key.split('.')
            value = self._config
            for k in keys:
                if isinstance(value, dict) and k in value:
                    value = value[k]
                else:
                    return default
            return value
        
        # Handle simple keys
        return self._config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value by key.
        
        Args:
            key: Configuration key (dot notation supported for nested keys)
            value: Value to set
        """
        # Handle nested keys with dot notation
        if '.' in key:
            keys = key.split('.')
            config = self._config
            
            # Navigate to the deepest level
            for k in keys[:-1]:
                if k not in config:
                    config[k] = {}
                config = config[k]
            
            # Set the value
            config[keys[-1]] = value
        else:
            # Handle simple keys
            self._config[key] = value
    
    def save(self) -> None:
        """Save the current configuration to file."""
        self._save_config(self._config)
    
    def get_all(self) -> Dict:
        """
        Get the entire configuration dictionary.
        
        Returns:
            Configuration dictionary
        """
        return self._config
    
    def reload(self) -> None:
        """Reload configuration from file."""
        self._config = self._load_config()
        self.logger.info("Configuration reloaded")
