"""
Redis caching layer for blockchain indexes to speed up startup
"""

import json
import logging
import asyncio
from typing import Optional, Dict, Any, List
from decimal import Decimal

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)

class BlockchainRedisCache:
    """Redis cache for blockchain indexes and computed values"""
    
    def __init__(self, redis_url: Optional[str] = None):
        self.redis_client = None
        self.enabled = False
        
        if redis_url and REDIS_AVAILABLE:
            try:
                self.redis_client = redis.from_url(redis_url, decode_responses=True)
                self.enabled = True
                logger.info("Blockchain Redis cache initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize Redis cache: {e}")
    
    async def close(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()
    
    # Chain Index Cache
    async def get_chain_index(self) -> Optional[Dict[str, Any]]:
        """Get cached chain index"""
        if not self.enabled:
            return None
            
        try:
            data = await self.redis_client.get("blockchain:chain_index")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Failed to get chain index from Redis: {e}")
        return None
    
    async def set_chain_index(self, index: Dict[str, Any], ttl: int = 3600):
        """Cache chain index"""
        if not self.enabled:
            return
            
        try:
            await self.redis_client.setex(
                "blockchain:chain_index",
                ttl,
                json.dumps(index)
            )
        except Exception as e:
            logger.error(f"Failed to cache chain index: {e}")
    
    # Cumulative Difficulty Cache
    async def get_cumulative_difficulty(self, block_hash: str) -> Optional[str]:
        """Get cached cumulative difficulty for a block"""
        if not self.enabled:
            return None
            
        try:
            return await self.redis_client.get(f"blockchain:difficulty:{block_hash}")
        except Exception as e:
            logger.error(f"Failed to get cumulative difficulty: {e}")
        return None
    
    async def set_cumulative_difficulty(self, block_hash: str, difficulty: Decimal, ttl: int = 86400):
        """Cache cumulative difficulty for a block"""
        if not self.enabled:
            return
            
        try:
            await self.redis_client.setex(
                f"blockchain:difficulty:{block_hash}",
                ttl,
                str(difficulty)
            )
        except Exception as e:
            logger.error(f"Failed to cache cumulative difficulty: {e}")
    
    async def batch_set_cumulative_difficulties(self, difficulties: Dict[str, Decimal], ttl: int = 86400):
        """Batch cache multiple cumulative difficulties"""
        if not self.enabled:
            return
            
        try:
            pipe = self.redis_client.pipeline()
            for block_hash, difficulty in difficulties.items():
                pipe.setex(f"blockchain:difficulty:{block_hash}", ttl, str(difficulty))
            await pipe.execute()
        except Exception as e:
            logger.error(f"Failed to batch cache difficulties: {e}")
    
    # Height Index Cache
    async def get_height_index(self) -> Optional[Dict[int, str]]:
        """Get cached height index"""
        if not self.enabled:
            return None
            
        try:
            data = await self.redis_client.get("blockchain:height_index")
            if data:
                # Convert string keys back to int
                index = json.loads(data)
                return {int(k): v for k, v in index.items()}
        except Exception as e:
            logger.error(f"Failed to get height index: {e}")
        return None
    
    async def set_height_index(self, index: Dict[int, str], ttl: int = 3600):
        """Cache height index"""
        if not self.enabled:
            return
            
        try:
            # Convert int keys to strings for JSON
            str_index = {str(k): v for k, v in index.items()}
            await self.redis_client.setex(
                "blockchain:height_index",
                ttl,
                json.dumps(str_index)
            )
        except Exception as e:
            logger.error(f"Failed to cache height index: {e}")
    
    # Best Chain Cache
    async def get_best_chain_info(self) -> Optional[Dict[str, Any]]:
        """Get cached best chain information"""
        if not self.enabled:
            return None
            
        try:
            data = await self.redis_client.get("blockchain:best_chain")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Failed to get best chain info: {e}")
        return None
    
    async def set_best_chain_info(self, info: Dict[str, Any], ttl: int = 300):
        """Cache best chain information"""
        if not self.enabled:
            return
            
        try:
            await self.redis_client.setex(
                "blockchain:best_chain",
                ttl,
                json.dumps(info)
            )
        except Exception as e:
            logger.error(f"Failed to cache best chain info: {e}")
    
    # Chain Statistics Cache
    async def get_chain_stats(self) -> Optional[Dict[str, Any]]:
        """Get cached chain statistics"""
        if not self.enabled:
            return None
            
        try:
            data = await self.redis_client.get("blockchain:chain_stats")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Failed to get chain stats: {e}")
        return None
    
    async def set_chain_stats(self, stats: Dict[str, Any], ttl: int = 600):
        """Cache chain statistics"""
        if not self.enabled:
            return
            
        try:
            await self.redis_client.setex(
                "blockchain:chain_stats",
                ttl,
                json.dumps(stats)
            )
        except Exception as e:
            logger.error(f"Failed to cache chain stats: {e}")
    
    # Cache Invalidation
    async def invalidate_chain_caches(self):
        """Invalidate all chain-related caches"""
        if not self.enabled:
            return
            
        try:
            keys_to_delete = [
                "blockchain:chain_index",
                "blockchain:height_index",
                "blockchain:best_chain",
                "blockchain:chain_stats"
            ]
            
            # Also delete all difficulty caches
            difficulty_keys = await self.redis_client.keys("blockchain:difficulty:*")
            keys_to_delete.extend(difficulty_keys)
            
            if keys_to_delete:
                await self.redis_client.delete(*keys_to_delete)
                logger.info(f"Invalidated {len(keys_to_delete)} cache entries")
        except Exception as e:
            logger.error(f"Failed to invalidate caches: {e}")
    
    # Health Check
    async def is_healthy(self) -> bool:
        """Check if Redis connection is healthy"""
        if not self.enabled:
            return False
            
        try:
            await self.redis_client.ping()
            return True
        except Exception:
            return False
    
    # Synchronous wrapper methods for use in non-async contexts
    def get_chain_index_sync(self) -> Optional[Dict[str, Any]]:
        """Synchronous wrapper for get_chain_index"""
        if not self.enabled:
            return None
            
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self.get_chain_index())
            loop.close()
            return result
        except Exception as e:
            logger.error(f"Failed to get chain index (sync): {e}")
            return None
    
    def set_chain_index_sync(self, index: Dict[str, Any], ttl: int = 3600):
        """Synchronous wrapper for set_chain_index"""
        if not self.enabled:
            return
            
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.set_chain_index(index, ttl))
            loop.close()
        except Exception as e:
            logger.error(f"Failed to set chain index (sync): {e}")
    
    def get_cumulative_difficulty_sync(self, block_hash: str) -> Optional[str]:
        """Synchronous wrapper for get_cumulative_difficulty"""
        if not self.enabled:
            return None
            
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self.get_cumulative_difficulty(block_hash))
            loop.close()
            return result
        except Exception as e:
            logger.error(f"Failed to get cumulative difficulty (sync): {e}")
            return None
    
    def batch_set_cumulative_difficulties_sync(self, difficulties: Dict[str, Decimal], ttl: int = 86400):
        """Synchronous wrapper for batch_set_cumulative_difficulties"""
        if not self.enabled:
            return
            
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.batch_set_cumulative_difficulties(difficulties, ttl))
            loop.close()
        except Exception as e:
            logger.error(f"Failed to batch set difficulties (sync): {e}")