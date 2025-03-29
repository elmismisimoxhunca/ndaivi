#!/usr/bin/env python3
"""
Redis Manager for NDAIVI

This module provides a Redis-based message broker for component communication
in the NDAIVI system. It handles pub/sub messaging, command distribution,
and status reporting between the crawler, analyzer, and other components.
"""

import json
import logging
import threading
import time
import redis
from typing import Dict, List, Any, Optional, Union, Callable

# Configure logging
logger = logging.getLogger(__name__)

class RedisManager:
    """
    Redis-based message broker for NDAIVI components.
    
    This class provides a centralized interface for component communication
    using Redis pub/sub channels. It handles message serialization,
    subscription management, and callback routing.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Redis manager with configuration.
        
        Args:
            config: Redis configuration dictionary with host, port, etc.
        """
        self.config = config
        self.redis_client = self._create_redis_client()
        self.pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
        self.subscribers = {}  # Channel -> list of callbacks
        self.running = False
        self.listener_thread = None
        
        # Default channels from config or use defaults
        channels_config = config.get('application', {}).get('channels', {})
        self.channels = {
            'crawler_commands': channels_config.get('crawler_commands', 'ndaivi:crawler:commands'),
            'crawler_status': channels_config.get('crawler_status', 'ndaivi:crawler:status'),
            'analyzer_commands': channels_config.get('analyzer_commands', 'ndaivi:analyzer:commands'),
            'analyzer_status': channels_config.get('analyzer_status', 'ndaivi:analyzer:status'),
            'stats': channels_config.get('stats', 'ndaivi:stats'),
            'system': channels_config.get('system', 'ndaivi:system'),
            'database': channels_config.get('database', 'ndaivi:database')
        }
    
    def _create_redis_client(self) -> redis.Redis:
        """
        Create a Redis client with the provided configuration.
        
        Returns:
            redis.Redis: Configured Redis client
        """
        redis_config = self.config.get('redis', {})
        
        return redis.Redis(
            host=redis_config.get('host', 'localhost'),
            port=redis_config.get('port', 6379),
            db=redis_config.get('db', 0),
            password=redis_config.get('password'),
            socket_timeout=redis_config.get('socket_timeout', 5),
            socket_connect_timeout=redis_config.get('socket_connect_timeout', 5),
            health_check_interval=redis_config.get('health_check_interval', 30),
            retry_on_timeout=redis_config.get('retry_on_timeout', True),
            decode_responses=False
        )
    
    def start(self) -> bool:
        """
        Start the Redis manager and listener thread.
        
        Returns:
            bool: True if started successfully, False otherwise
        """
        if self.running:
            logger.warning("Redis manager already running")
            return True
        
        try:
            # Test Redis connection
            self.redis_client.ping()
            
            # Start listener thread
            self.running = True
            self.listener_thread = threading.Thread(target=self._listener_loop)
            self.listener_thread.daemon = True
            self.listener_thread.start()
            
            logger.info("Redis manager started successfully")
            return True
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.running = False
            return False
        except Exception as e:
            logger.error(f"Error starting Redis manager: {e}")
            self.running = False
            return False
    
    def stop(self) -> None:
        """Stop the Redis manager and listener thread."""
        if not self.running:
            return
        
        self.running = False
        
        # Wait for listener thread to terminate
        if self.listener_thread:
            self.listener_thread.join(timeout=1.0)
        
        # Unsubscribe from all channels
        try:
            self.pubsub.unsubscribe()
            self.pubsub.close()
        except Exception as e:
            logger.error(f"Error closing Redis pubsub: {e}")
        
        logger.info("Redis manager stopped")
    
    def _listener_loop(self) -> None:
        """
        Main listener loop that processes incoming messages.
        This runs in a separate thread.
        """
        while self.running:
            try:
                # Get message with a timeout to allow for clean shutdown
                message = self.pubsub.get_message(timeout=0.1)
                
                if message:
                    self._process_message(message)
                
                # Small sleep to prevent CPU hogging
                time.sleep(0.001)
            except redis.exceptions.ConnectionError as e:
                logger.error(f"Redis connection error in listener: {e}")
                time.sleep(1)  # Back off on connection error
            except Exception as e:
                logger.error(f"Error in Redis listener loop: {e}")
                time.sleep(0.1)  # Small delay on other errors
    
    def _process_message(self, message: Dict[str, Any]) -> None:
        """
        Process a message from Redis pubsub.
        
        Args:
            message: Redis pubsub message
        """
        try:
            # Extract channel and data
            channel = message.get('channel', b'').decode('utf-8')
            data = message.get('data', b'')
            
            # Skip if not a proper message
            if not channel or not data:
                return
            
            # Deserialize JSON data
            try:
                payload = json.loads(data.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.warning(f"Received non-JSON message on channel {channel}")
                return
            
            # Call subscribers for this channel
            if channel in self.subscribers:
                for callback in self.subscribers[channel]:
                    try:
                        callback(payload)
                    except Exception as e:
                        logger.error(f"Error in subscriber callback for channel {channel}: {e}")
        except Exception as e:
            logger.error(f"Error processing Redis message: {e}")
    
    def subscribe(self, channel: str, callback: Callable[[Dict[str, Any]], None]) -> bool:
        """
        Subscribe to a Redis channel with a callback function.
        
        Args:
            channel: Channel name to subscribe to
            callback: Function to call when a message is received
            
        Returns:
            bool: True if subscribed successfully, False otherwise
        """
        try:
            # Add to subscribers dict
            if channel not in self.subscribers:
                self.subscribers[channel] = []
            
            self.subscribers[channel].append(callback)
            
            # Subscribe to the channel
            self.pubsub.subscribe(channel)
            
            logger.info(f"Subscribed to channel: {channel}")
            return True
        except Exception as e:
            logger.error(f"Error subscribing to channel {channel}: {e}")
            return False
    
    def unsubscribe(self, channel: str, callback: Optional[Callable] = None) -> bool:
        """
        Unsubscribe from a Redis channel.
        
        Args:
            channel: Channel name to unsubscribe from
            callback: Specific callback to remove (or all if None)
            
        Returns:
            bool: True if unsubscribed successfully, False otherwise
        """
        try:
            # Remove specific callback or all callbacks
            if channel in self.subscribers:
                if callback:
                    self.subscribers[channel] = [cb for cb in self.subscribers[channel] if cb != callback]
                else:
                    self.subscribers[channel] = []
            
            # If no more subscribers, unsubscribe from the channel
            if channel not in self.subscribers or not self.subscribers[channel]:
                self.pubsub.unsubscribe(channel)
            
            logger.info(f"Unsubscribed from channel: {channel}")
            return True
        except Exception as e:
            logger.error(f"Error unsubscribing from channel {channel}: {e}")
            return False
    
    def publish(self, channel: str, message: Dict[str, Any]) -> bool:
        """
        Publish a message to a Redis channel.
        
        Args:
            channel: Channel name to publish to
            message: Message dictionary to publish
            
        Returns:
            bool: True if published successfully, False otherwise
        """
        try:
            # Serialize message to JSON with custom handling for sets
            message_json = json.dumps(message, default=self._json_serializer)
            
            # Publish to channel
            self.redis_client.publish(channel, message_json)
            
            return True
        except Exception as e:
            logger.error(f"Error publishing to channel {channel}: {e}")
            return False
    
    def get_channel(self, channel_key: str) -> str:
        """
        Get a channel name by its key.
        
        Args:
            channel_key: Key of the channel (e.g., 'crawler_commands')
            
        Returns:
            str: Channel name
        """
        return self.channels.get(channel_key, f"ndaivi:{channel_key}")
    
    def store_data(self, key: str, data: Dict[str, Any], expiry: Optional[int] = None) -> bool:
        """
        Store data in Redis.
        
        Args:
            key: Redis key
            data: Data dictionary to store
            expiry: Optional expiry time in seconds
            
        Returns:
            bool: True if stored successfully, False otherwise
        """
        try:
            # Serialize data to JSON with custom handling for sets
            data_json = json.dumps(data, default=self._json_serializer)
            
            # Store in Redis
            if expiry:
                self.redis_client.setex(key, expiry, data_json)
            else:
                self.redis_client.set(key, data_json)
            
            return True
        except Exception as e:
            logger.error(f"Error storing data with key {key}: {e}")
            return False
    
    def get_data(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get data from Redis.
        
        Args:
            key: Redis key
            
        Returns:
            Optional[Dict[str, Any]]: Data dictionary or None if not found
        """
        try:
            # Get data from Redis
            data = self.redis_client.get(key)
            
            if not data:
                return None
            
            # Deserialize JSON data
            return json.loads(data.decode('utf-8'))
        except Exception as e:
            logger.error(f"Error getting data with key {key}: {e}")
            return None
    
    def delete_data(self, key: str) -> bool:
        """
        Delete data from Redis.
        
        Args:
            key: Redis key
            
        Returns:
            bool: True if deleted successfully, False otherwise
        """
        try:
            # Delete data from Redis
            self.redis_client.delete(key)
            
            return True
        except Exception as e:
            logger.error(f"Error deleting data with key {key}: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get Redis server statistics.
        
        Returns:
            Dict[str, Any]: Redis server statistics
        """
        try:
            # Get Redis info
            info = self.redis_client.info()
            
            # Extract relevant stats
            stats = {
                'connected_clients': info.get('connected_clients', 0),
                'used_memory_human': info.get('used_memory_human', '0B'),
                'total_commands_processed': info.get('total_commands_processed', 0),
                'uptime_in_seconds': info.get('uptime_in_seconds', 0),
                'uptime_in_days': info.get('uptime_in_days', 0),
                'redis_version': info.get('redis_version', 'unknown')
            }
            
            return stats
        except Exception as e:
            logger.error(f"Error getting Redis stats: {e}")
            return {
                'error': str(e),
                'status': 'disconnected'
            }
    
    def is_connected(self) -> bool:
        """
        Check if Redis is connected.
        
        Returns:
            bool: True if connected, False otherwise
        """
        try:
            # Try to ping Redis
            return self.redis_client.ping()
        except Exception as e:
            logger.error(f"Redis connection check failed: {e}")
            return False
    
    def connect(self) -> bool:
        """
        Attempt to connect or reconnect to Redis.
        
        Returns:
            bool: True if connected successfully, False otherwise
        """
        try:
            # Create a new Redis client
            self.redis_client = self._create_redis_client()
            
            # Test the connection
            if self.redis_client.ping():
                logger.info("Successfully connected to Redis")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False
            
    def _json_serializer(self, obj):
        """
        Custom JSON serializer to handle non-serializable types.
        
        Args:
            obj: Object to serialize
            
        Returns:
            Serializable representation of the object
        """
        # Handle sets by converting to lists
        if isinstance(obj, set):
            return list(obj)
        # Handle other non-serializable types as needed
        try:
            return str(obj)
        except:
            return None

# Singleton instance
_redis_manager_instance = None

def get_redis_manager(config: Optional[Dict[str, Any]] = None) -> RedisManager:
    """
    Get the singleton Redis manager instance.
    
    Args:
        config: Optional configuration dictionary
        
    Returns:
        RedisManager: Redis manager instance
    """
    global _redis_manager_instance
    
    if _redis_manager_instance is None and config is not None:
        _redis_manager_instance = RedisManager(config)
    
    return _redis_manager_instance
