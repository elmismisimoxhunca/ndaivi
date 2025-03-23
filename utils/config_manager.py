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
        if self._initialized:
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
            # Use default config path if none provided
            if not self._config_path:
                self._config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.yaml')
            
            self.logger.info(f"Loading configuration from {self._config_path}")
            
            with open(self._config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Validate configuration
            self._validate_config(config)
            
            return config
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _validate_config(self, config: Dict) -> None:
        """
        Validate configuration settings.
        
        Args:
            config: Configuration dictionary to validate
        """
        # Check for required sections
        required_sections = ['keywords', 'prompt_templates']
        for section in required_sections:
            if section not in config:
                self.logger.warning(f"Missing required configuration section: {section}")
    
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
