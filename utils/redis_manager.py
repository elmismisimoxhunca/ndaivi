import logging
import json
import threading
import time
import redis
from typing import Dict, Any, Callable, Optional, List

logger = logging.getLogger(__name__)

# Singleton Redis manager instance to ensure shared connection
_REDIS_INSTANCE = None

def get_redis_manager(config=None):
    """
    Get a Redis manager instance (singleton).
    
    Args:
        config: Redis configuration
        
    Returns:
        RedisManager: Redis manager instance
    """
    global _REDIS_INSTANCE
    if _REDIS_INSTANCE is None:
        _REDIS_INSTANCE = RedisManager(config)
    return _REDIS_INSTANCE

class RedisManager:
    """
    Redis manager for pub/sub communication.
    
    This class handles Redis connections and provides methods for
    publishing and subscribing to channels.
    """
    
    def __init__(self, config=None):
        """
        Initialize the Redis manager.
        
        Args:
            config: Redis configuration
        """
        self.config = config or {}
        self.redis_config = self.config.get('redis', {})
        self.channels_config = self.config.get('application', {}).get('channels', {})
        
        # Redis connection
        self.redis = None
        self.pubsub = None
        self.connected = False
        
        # Subscription handlers
        self.handlers = {}
        self.subscribed_channels = set()
        
        # Background thread for handling subscription messages
        self.subscription_thread = None
        self.running = False
        
        # Connect to Redis
        self.connect()
    
    def connect(self) -> bool:
        """
        Connect to Redis.
        
        Returns:
            bool: True if connected successfully
        """
        try:
            # Close existing connection if exists
            if self.redis:
                try:
                    self.redis.close()
                except:
                    pass
            
            # Redis connection parameters
            host = self.redis_config.get('host', 'localhost')
            port = self.redis_config.get('port', 6379)
            db = self.redis_config.get('db', 0)
            password = self.redis_config.get('password')
            socket_timeout = self.redis_config.get('socket_timeout', 5)
            socket_connect_timeout = self.redis_config.get('socket_connect_timeout', 5)
            retry_on_timeout = self.redis_config.get('retry_on_timeout', True)
            
            # Log connection details
            logger.info(f"Connecting to Redis at {host}:{port} (db: {db})")
            
            # Create Redis connection
            self.redis = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_connect_timeout,
                retry_on_timeout=retry_on_timeout,
                decode_responses=True
            )
            
            # Create PubSub object
            self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
            
            # Test connection
            self.redis.ping()
            
            # Set connected flag
            self.connected = True
            
            # Start subscription thread if necessary
            if not self.running and self.handlers:
                self._start_subscription_thread()
            
            logger.info("Connected to Redis successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to Redis: {str(e)}")
            self.connected = False
            return False
    
    def is_connected(self) -> bool:
        """
        Check if connected to Redis.
        
        Returns:
            bool: True if connected
        """
        if not self.connected or not self.redis:
            return False
            
        try:
            self.redis.ping()
            return True
        except:
            self.connected = False
            return False
    
    def get_channel(self, channel_key: str) -> str:
        """
        Get the channel name from the configuration.
        
        Args:
            channel_key: Channel key in the configuration
            
        Returns:
            str: Channel name
        """
        if not self.channels_config:
            # Default channel names if not configured
            default_channels = {
                'crawler_commands': 'ndaivi:crawler:commands',
                'crawler_status': 'ndaivi:crawler:status',
                'analyzer_commands': 'ndaivi:analyzer:commands',
                'analyzer_status': 'ndaivi:analyzer:status',
                'analyzer_results': 'ndaivi:analyzer:results',
                'backlog_status': 'ndaivi:backlog:status',
                'backlog_request': 'ndaivi:backlog:request',
                'container_status': 'ndaivi:container:status',
                'stats': 'ndaivi:stats',
                'system': 'ndaivi:system'
            }
            
            if channel_key in default_channels:
                return default_channels[channel_key]
            else:
                raise ValueError(f"Unknown channel key: {channel_key}")
        
        if channel_key in self.channels_config:
            return self.channels_config[channel_key]
        else:
            raise ValueError(f"Channel key '{channel_key}' not found in configuration")
    
    def publish(self, channel: str, message: Any) -> bool:
        """
        Publish a message to a channel.
        
        Args:
            channel: Channel name
            message: Message to publish
            
        Returns:
            bool: True if published successfully
        """
        # Ensure we're connected
        if not self.is_connected():
            if not self.connect():
                logger.error(f"Failed to publish to {channel}: not connected to Redis")
                return False
        
        try:
            # Serialize message to JSON
            if isinstance(message, (dict, list)):
                message_str = json.dumps(message)
            else:
                message_str = str(message)
            
            # Publish message
            result = self.redis.publish(channel, message_str)
            
            # Log successful publication
            logger.debug(f"Published message to {channel}: {message_str[:100]}...")
            
            return result > 0
            
        except Exception as e:
            logger.error(f"Error publishing to {channel}: {str(e)}")
            # Try to reconnect
            self.connect()
            return False
    
    def subscribe(self, channel: str, handler: Callable[[Dict[str, Any]], None]) -> bool:
        """
        Subscribe to a channel.
        
        Args:
            channel: Channel name
            handler: Handler function to call when a message is received
            
        Returns:
            bool: True if subscribed successfully
        """
        # Ensure we're connected
        if not self.is_connected():
            if not self.connect():
                logger.error(f"Failed to subscribe to {channel}: not connected to Redis")
                return False
        
        try:
            # Store the handler
            self.handlers[channel] = handler
            
            # Subscribe to the channel
            self.pubsub.subscribe(channel)
            self.subscribed_channels.add(channel)
            
            # Start the subscription thread if it's not already running
            if not self.running:
                self._start_subscription_thread()
            
            logger.info(f"Subscribed to channel: {channel}")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to {channel}: {str(e)}")
            return False
    
    def unsubscribe(self, channel: str) -> bool:
        """
        Unsubscribe from a channel.
        
        Args:
            channel: Channel name
            
        Returns:
            bool: True if unsubscribed successfully
        """
        if not self.is_connected():
            return False
        
        try:
            # Unsubscribe from the channel
            self.pubsub.unsubscribe(channel)
            
            # Remove the handler
            if channel in self.handlers:
                del self.handlers[channel]
            
            # Remove from subscribed channels
            if channel in self.subscribed_channels:
                self.subscribed_channels.remove(channel)
            
            logger.info(f"Unsubscribed from channel: {channel}")
            return True
            
        except Exception as e:
            logger.error(f"Error unsubscribing from {channel}: {str(e)}")
            return False
    
    def _start_subscription_thread(self) -> None:
        """
        Start the subscription thread.
        """
        if self.running:
            return
            
        self.running = True
        self.subscription_thread = threading.Thread(target=self._subscription_loop, daemon=True)
        self.subscription_thread.start()
        
        logger.info("Started Redis subscription thread")
    
    def _subscription_loop(self) -> None:
        """
        Main loop for handling subscription messages.
        """
        while self.running:
            try:
                # Check if we're still connected
                if not self.is_connected():
                    logger.warning("Redis connection lost in subscription thread, reconnecting...")
                    if self.connect():
                        # Resubscribe to channels
                        for channel in list(self.subscribed_channels):
                            self.pubsub.subscribe(channel)
                    else:
                        # If reconnection failed, sleep and try again
                        time.sleep(1)
                        continue
                
                # Get a message with timeout
                message = self.pubsub.get_message(timeout=0.1)
                
                if message:
                    channel = message.get('channel')
                    data = message.get('data')
                    
                    # Skip control messages
                    if isinstance(channel, (int, type(None))):
                        continue
                    
                    # Handle the message
                    if channel in self.handlers:
                        try:
                            # Parse the data
                            if isinstance(data, str):
                                try:
                                    parsed_data = json.loads(data)
                                except json.JSONDecodeError:
                                    parsed_data = data
                            else:
                                parsed_data = data
                            
                            # Call the handler
                            handler = self.handlers[channel]
                            handler(parsed_data)
                            
                        except Exception as e:
                            logger.error(f"Error handling message on {channel}: {str(e)}")
                
                # Short sleep to prevent CPU hogging
                time.sleep(0.01)
                
            except Exception as e:
                logger.error(f"Error in subscription loop: {str(e)}")
                time.sleep(1)  # Longer sleep on error
    
    def close(self) -> None:
        """
        Close the Redis connection.
        """
        self.running = False
        
        # Wait for subscription thread to terminate
        if self.subscription_thread and self.subscription_thread.is_alive():
            try:
                self.subscription_thread.join(timeout=2.0)
            except:
                pass
        
        # Close pubsub connection
        if self.pubsub:
            try:
                self.pubsub.close()
            except:
                pass
        
        # Close Redis connection
        if self.redis:
            try:
                self.redis.close()
            except:
                pass
        
        self.connected = False
        logger.info("Redis connection closed")
    
    def store_data(self, key: str, data: Any, expire: int = None) -> bool:
        """
        Store data in Redis.
        
        Args:
            key: Redis key
            data: Data to store
            expire: Time to live in seconds
            
        Returns:
            bool: True if stored successfully
        """
        if not self.is_connected():
            if not self.connect():
                return False
        
        try:
            # Serialize data to JSON
            if isinstance(data, (dict, list)):
                data_str = json.dumps(data)
            else:
                data_str = str(data)
            
            # Store data
            self.redis.set(key, data_str)
            
            # Set expiration if specified
            if expire:
                self.redis.expire(key, expire)
            
            return True
            
        except Exception as e:
            logger.error(f"Error storing data for key {key}: {str(e)}")
            return False
    
    def get_data(self, key: str, default: Any = None) -> Any:
        """
        Get data from Redis.
        
        Args:
            key: Redis key
            default: Default value if key doesn't exist
            
        Returns:
            Any: Retrieved data or default
        """
        if not self.is_connected():
            if not self.connect():
                return default
        
        try:
            # Get data
            data = self.redis.get(key)
            
            if data is None:
                return default
            
            # Try to parse as JSON
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return data
                
        except Exception as e:
            logger.error(f"Error retrieving data for key {key}: {str(e)}")
            return default