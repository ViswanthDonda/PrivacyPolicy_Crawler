"""Redis cache service"""
import redis
import json
from typing import Optional, Any
from app.config import settings
import logging

logger = logging.getLogger(__name__)

# Redis client instance
_redis_client = None


def get_redis_client() -> redis.Redis:
    """Get Redis client instance"""
    global _redis_client
    
    if _redis_client is not None:
        return _redis_client
    
    try:
        _redis_client = redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        # Test connection
        _redis_client.ping()
        logger.info("Redis connection established")
        return _redis_client
    except Exception as e:
        logger.error(f"Error connecting to Redis: {e}")
        # Return None if Redis is not available (for development)
        return None


def get_cache(key: str) -> Optional[Any]:
    """Get value from cache"""
    try:
        client = get_redis_client()
        if client is None:
            return None
        
        value = client.get(key)
        if value:
            parsed_value = json.loads(value)
            return parsed_value
        return None
    except Exception as e:
        logger.error(f"Error getting cache key {key}: {e}")
        return None


def set_cache(key: str, value: Any, ttl: int) -> bool:
    """Set value in cache with TTL (in seconds)"""
    try:
        client = get_redis_client()
        if client is None:
            return False
        
        serialized = json.dumps(value, default=str)
        client.setex(key, ttl, serialized)
        return True
    except Exception as e:
        logger.error(f"Error setting cache key {key}: {e}")
        return False


def delete_cache(key: str) -> bool:
    """Delete key from cache"""
    try:
        client = get_redis_client()
        if client is None:
            return False
        
        client.delete(key)
        return True
    except Exception as e:
        logger.error(f"Error deleting cache key {key}: {e}")
        return False


def increment_counter(key: str, ttl: int) -> int:
    """Increment counter and set TTL if key doesn't exist"""
    try:
        client = get_redis_client()
        if client is None:
            return 0
        
        count = client.incr(key)
        if count == 1:
            client.expire(key, ttl)
        return count
    except Exception as e:
        logger.error(f"Error incrementing counter {key}: {e}")
        return 0
