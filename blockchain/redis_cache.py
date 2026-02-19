"""
Redis caching layer for blockchain indexes to speed up startup
"""

import json
import logging
from typing import Optional, Dict, Any, List
from decimal import Decimal
import threading

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)

class BlockchainRedisCache:
    """Redis cache for blockchain indexes and computed values"""
    
    def __init__(self, redis_url: Optional[str] = None):
        self.redis_client = None
        self.enabled = False
        self._lock = threading.Lock()
        
        if redis_url and REDIS_AVAILABLE:
            try:
                # Create Redis client with connection pool settings
                # Simplified configuration without socket keepalive options
                # to avoid platform-specific issues
                pool = redis.ConnectionPool.from_url(
                    redis_url, 
                    decode_responses=True,
                    max_connections=5,  # Limit connections per client
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                self.redis_client = redis.Redis(connection_pool=pool)
                # Test connection
                self.redis_client.ping()
                self.enabled = True
                logger.info("Blockchain Redis cache initialized with connection pool")
            except Exception as e:
                logger.warning(f"Failed to initialize Redis cache: {e}")
                self.enabled = False
    
    def _execute(self, func, *args, **kwargs):
        """Execute a Redis operation with error handling"""
        if not self.enabled:
            logger.debug("Redis not enabled in _execute")
            return None

        try:
            with self._lock:
                logger.debug(f"Executing Redis operation: {func.__name__} with args: {args[:2] if args else 'none'}...")
                result = func(*args, **kwargs)
                logger.debug(f"Redis operation {func.__name__} completed with result: {result}")
                return result
        except Exception as e:
            logger.error(f"Redis operation failed: {e}")
            return None
    
    def close(self):
        """Close Redis connection"""
        if self.redis_client:
            try:
                self.redis_client.close()
            except:
                pass
    
    def ping(self) -> bool:
        """Check if Redis connection is alive"""
        if not self.enabled:
            return False
            
        try:
            return self._execute(self.redis_client.ping)
        except:
            return False
    
    # Chain Index Cache
    def get_chain_index(self) -> Optional[Dict[str, Any]]:
        """Get cached chain index"""
        if not self.enabled:
            return None
            
        try:
            cached_data = self._execute(self.redis_client.get, "blockchain:chain_index")
            if cached_data:
                # Deserialize the JSON data
                data = json.loads(cached_data)
                # Convert cumulative difficulties back to Decimal
                if "block_index" in data:
                    for block_hash, block_info in data["block_index"].items():
                        if "cumulative_difficulty" in block_info:
                            block_info["cumulative_difficulty"] = Decimal(block_info["cumulative_difficulty"])
                return data
        except Exception as e:
            logger.error(f"Failed to get chain index from cache: {e}")
        return None
    
    def set_chain_index(self, index: Dict[str, Any], ttl: int = 300):  # 5 minutes default
        """Cache the chain index"""
        if not self.enabled:
            logger.debug("Redis cache not enabled, skipping set_chain_index")
            return

        try:
            # Convert Decimals to strings for JSON serialization
            serializable_index = {
                "chain_tips": index.get("chain_tips", []),
                "block_index": {}
            }

            for block_hash, block_info in index.get("block_index", {}).items():
                info_copy = block_info.copy()
                if "cumulative_difficulty" in info_copy:
                    info_copy["cumulative_difficulty"] = str(info_copy["cumulative_difficulty"])
                serializable_index["block_index"][block_hash] = info_copy

            logger.debug(f"Attempting to cache chain index with {len(serializable_index['block_index'])} blocks")
            result = self._execute(
                self.redis_client.setex,
                "blockchain:chain_index",
                ttl,
                json.dumps(serializable_index)
            )
            if result:
                logger.info(f"Successfully cached chain index with {len(serializable_index['block_index'])} blocks")
            else:
                logger.warning("set_chain_index returned None/False")
        except Exception as e:
            logger.error(f"Failed to cache chain index: {e}")
    
    # Cumulative Difficulty Cache
    def get_cumulative_difficulty(self, block_hash: str) -> Optional[Decimal]:
        """Get cached cumulative difficulty for a block"""
        if not self.enabled:
            return None
            
        try:
            cached_value = self._execute(self.redis_client.get, f"blockchain:difficulty:{block_hash}")
            if cached_value:
                return Decimal(cached_value)
        except Exception as e:
            logger.error(f"Failed to get cumulative difficulty: {e}")
        return None
    
    def set_cumulative_difficulty(self, block_hash: str, difficulty: Decimal, ttl: int = 3600):  # 1 hour
        """Cache cumulative difficulty for a block"""
        if not self.enabled:
            return
            
        try:
            self._execute(
                self.redis_client.setex,
                f"blockchain:difficulty:{block_hash}",
                ttl,
                str(difficulty)
            )
        except Exception as e:
            logger.error(f"Failed to cache cumulative difficulty: {e}")
    
    # Height Index Cache
    def get_height_index(self) -> Optional[Dict[int, str]]:
        """Get cached height-to-block-hash index"""
        if not self.enabled:
            return None
            
        try:
            cached_data = self._execute(self.redis_client.get, "blockchain:height_index")
            if cached_data:
                # Convert string keys back to integers
                str_index = json.loads(cached_data)
                return {int(k): v for k, v in str_index.items()}
        except Exception as e:
            logger.error(f"Failed to get height index: {e}")
        return None
    
    def set_height_index(self, index: Dict[int, str], ttl: int = 300):  # 5 minutes
        """Cache the height index"""
        if not self.enabled:
            return
            
        try:
            # Convert integer keys to strings for JSON
            str_index = {str(k): v for k, v in index.items()}
            self._execute(
                self.redis_client.setex,
                "blockchain:height_index",
                ttl,
                json.dumps(str_index)
            )
        except Exception as e:
            logger.error(f"Failed to cache height index: {e}")
    
    # Best Chain Cache
    def get_best_chain_info(self) -> Optional[Dict[str, Any]]:
        """Get cached best chain information"""
        if not self.enabled:
            return None
            
        try:
            data = self._execute(self.redis_client.get, "blockchain:best_chain")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Failed to get best chain info: {e}")
        return None
    
    def set_best_chain_info(self, info: Dict[str, Any], ttl: int = 3600):  # 1 hour - changes less frequently
        """Cache best chain information"""
        if not self.enabled:
            return
            
        try:
            self._execute(
                self.redis_client.setex,
                "blockchain:best_chain",
                ttl,
                json.dumps(info)
            )
        except Exception as e:
            logger.error(f"Failed to cache best chain info: {e}")
    
    # Chain Statistics Cache
    def get_chain_stats(self) -> Optional[Dict[str, Any]]:
        """Get cached chain statistics"""
        if not self.enabled:
            return None
            
        try:
            data = self._execute(self.redis_client.get, "blockchain:chain_stats")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Failed to get chain stats: {e}")
        return None
    
    def set_chain_stats(self, stats: Dict[str, Any], ttl: int = 3600):  # 1 hour
        """Cache chain statistics"""
        if not self.enabled:
            return
            
        try:
            self._execute(
                self.redis_client.setex,
                "blockchain:chain_stats",
                ttl,
                json.dumps(stats)
            )
        except Exception as e:
            logger.error(f"Failed to cache chain stats: {e}")
    
    # Incremental Cache Updates
    def update_chain_index_entry(self, block_hash: str, block_info: Dict[str, Any]):
        """Update a single entry in the cached chain index"""
        if not self.enabled:
            return
            
        try:
            # Get existing index
            cached_data = self.get_chain_index()
            if cached_data:
                cached_data["block_index"][block_hash] = block_info
                # Update chain tips if necessary
                if block_info.get("is_tip", False):
                    if isinstance(cached_data["chain_tips"], list):
                        if block_hash not in cached_data["chain_tips"]:
                            cached_data["chain_tips"].append(block_hash)
                    else:
                        cached_data["chain_tips"] = [block_hash]
                self.set_chain_index(cached_data)
            else:
                logger.debug("No cached chain index to update")
        except Exception as e:
            logger.error(f"Failed to update chain index entry: {e}")
    
    def update_height_index_entry(self, height: int, block_hash: str):
        """Update a single entry in the cached height index"""
        if not self.enabled:
            return
            
        try:
            # Get existing index
            cached_index = self.get_height_index()
            if cached_index:
                cached_index[height] = block_hash
                self.set_height_index(cached_index)
            else:
                logger.debug("No cached height index to update")
        except Exception as e:
            logger.error(f"Failed to update height index entry: {e}")
    
    # Batch Operations
    def invalidate_all(self):
        """Invalidate all cached data"""
        if not self.enabled:
            return
            
        try:
            keys = self._execute(self.redis_client.keys, "blockchain:*")
            if keys:
                self._execute(self.redis_client.delete, *keys)
                logger.info(f"Invalidated {len(keys)} cache entries")
        except Exception as e:
            logger.error(f"Failed to invalidate cache: {e}")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        if not self.enabled:
            return {"enabled": False}
            
        try:
            info = self._execute(self.redis_client.info, "stats")
            return {
                "enabled": True,
                "hits": info.get("keyspace_hits", 0),
                "misses": info.get("keyspace_misses", 0),
                "hit_rate": info.get("keyspace_hits", 0) / max(1, info.get("keyspace_hits", 0) + info.get("keyspace_misses", 0)),
                "total_keys": self._execute(self.redis_client.dbsize) or 0
            }
        except Exception as e:
            logger.error(f"Failed to get cache stats: {e}")
            return {"enabled": True, "error": str(e)}