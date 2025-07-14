"""
Redis caching layer for blockchain indexes to speed up startup
"""

import json
import logging
import asyncio
from typing import Optional, Dict, Any, List
from decimal import Decimal
import threading
from concurrent.futures import ThreadPoolExecutor

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
        self._thread_pool = ThreadPoolExecutor(max_workers=1)
        self._event_loop = None
        self._loop_thread = None
        
        if redis_url and REDIS_AVAILABLE:
            try:
                self.redis_client = redis.from_url(redis_url, decode_responses=True)
                self.enabled = True
                # Create a dedicated event loop for Redis operations
                self._start_event_loop()
                logger.info("Blockchain Redis cache initialized")
            except Exception as e:
                logger.warning(f"Failed to initialize Redis cache: {e}")
    
    def _start_event_loop(self):
        """Start a dedicated event loop in a background thread"""
        def run_loop():
            self._event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._event_loop)
            self._event_loop.run_forever()
        
        self._loop_thread = threading.Thread(target=run_loop, daemon=True)
        self._loop_thread.start()
        # Wait for loop to be ready
        while self._event_loop is None:
            threading.Event().wait(0.01)
    
    def _run_async(self, coro):
        """Run an async coroutine in the dedicated event loop"""
        if not self.enabled:
            return None
            
        if not self._event_loop or not self._loop_thread.is_alive():
            logger.warning("Event loop not running, falling back to sync Redis")
            # Fallback to creating a new event loop
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(coro)
                loop.close()
                return result
            except Exception as e:
                logger.error(f"Failed to run async operation: {e}")
                return None
        
        try:
            future = asyncio.run_coroutine_threadsafe(coro, self._event_loop)
            return future.result(timeout=5.0)  # 5 second timeout
        except Exception as e:
            logger.error(f"Async operation failed: {e}")
            return None
    
    async def close(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()
        if self._event_loop:
            self._event_loop.call_soon_threadsafe(self._event_loop.stop)
    
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
    
    async def set_chain_index(self, index: Dict[str, Any], ttl: int = 86400 * 7):  # 7 days
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
    
    async def set_cumulative_difficulty(self, block_hash: str, difficulty: Decimal, ttl: int = 86400 * 30):  # 30 days - immutable data
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
    
    async def batch_set_cumulative_difficulties(self, difficulties: Dict[str, Decimal], ttl: int = 86400 * 30):  # 30 days
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
    
    async def set_height_index(self, index: Dict[int, str], ttl: int = 86400 * 7):  # 7 days
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
    
    async def set_best_chain_info(self, info: Dict[str, Any], ttl: int = 3600):  # 1 hour - changes less frequently
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
    
    async def set_chain_stats(self, stats: Dict[str, Any], ttl: int = 3600):  # 1 hour
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
    
    # Incremental Cache Updates
    async def update_chain_index_entry(self, block_hash: str, block_info: Dict[str, Any]):
        """Update a single entry in the cached chain index"""
        if not self.enabled:
            return
            
        try:
            # Get existing index
            cached_data = await self.get_chain_index()
            if cached_data:
                cached_data["block_index"][block_hash] = block_info
                # Update chain tips if necessary
                if block_info.get("is_tip", False):
                    if isinstance(cached_data["chain_tips"], list):
                        if block_hash not in cached_data["chain_tips"]:
                            cached_data["chain_tips"].append(block_hash)
                    else:
                        cached_data["chain_tips"] = [block_hash]
                await self.set_chain_index(cached_data)
            else:
                logger.debug("No cached chain index to update")
        except Exception as e:
            logger.error(f"Failed to update chain index entry: {e}")
    
    async def update_height_index_entry(self, height: int, block_hash: str):
        """Update a single entry in the cached height index"""
        if not self.enabled:
            return
            
        try:
            # Get existing index
            cached_index = await self.get_height_index()
            if cached_index:
                cached_index[height] = block_hash
                await self.set_height_index(cached_index)
            else:
                logger.debug("No cached height index to update")
        except Exception as e:
            logger.error(f"Failed to update height index entry: {e}")
    
    async def remove_chain_index_entry(self, block_hash: str):
        """Remove a single entry from the cached chain index (for reorgs)"""
        if not self.enabled:
            return
            
        try:
            cached_data = await self.get_chain_index()
            if cached_data and block_hash in cached_data.get("block_index", {}):
                del cached_data["block_index"][block_hash]
                if isinstance(cached_data.get("chain_tips"), list) and block_hash in cached_data["chain_tips"]:
                    cached_data["chain_tips"].remove(block_hash)
                await self.set_chain_index(cached_data)
        except Exception as e:
            logger.error(f"Failed to remove chain index entry: {e}")
    
    async def remove_height_index_entry(self, height: int):
        """Remove a single entry from the cached height index (for reorgs)"""
        if not self.enabled:
            return
            
        try:
            cached_index = await self.get_height_index()
            if cached_index and height in cached_index:
                del cached_index[height]
                await self.set_height_index(cached_index)
        except Exception as e:
            logger.error(f"Failed to remove height index entry: {e}")
    
    # Cache Invalidation (kept for major issues)
    async def invalidate_chain_caches(self):
        """Invalidate all chain-related caches - use only when incremental updates won't work"""
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
            return self._run_async(self.get_chain_index())
        except Exception as e:
            logger.error(f"Failed to get chain index (sync): {e}")
            return None
    
    def set_chain_index_sync(self, index: Dict[str, Any], ttl: int = 86400 * 7):
        """Synchronous wrapper for set_chain_index"""
        if not self.enabled:
            return
            
        try:
            self._run_async(self.set_chain_index(index, ttl))
        except Exception as e:
            logger.error(f"Failed to set chain index (sync): {e}")
    
    def get_cumulative_difficulty_sync(self, block_hash: str) -> Optional[str]:
        """Synchronous wrapper for get_cumulative_difficulty"""
        if not self.enabled:
            return None
            
        try:
            return self._run_async(self.get_cumulative_difficulty(block_hash))
        except Exception as e:
            logger.error(f"Failed to get cumulative difficulty (sync): {e}")
            return None
    
    def batch_set_cumulative_difficulties_sync(self, difficulties: Dict[str, Decimal], ttl: int = 86400 * 30):
        """Synchronous wrapper for batch_set_cumulative_difficulties"""
        if not self.enabled:
            return
            
        try:
            self._run_async(self.batch_set_cumulative_difficulties(difficulties, ttl))
        except Exception as e:
            logger.error(f"Failed to batch set difficulties (sync): {e}")
    
    def update_chain_index_entry_sync(self, block_hash: str, block_info: Dict[str, Any]):
        """Synchronous wrapper for update_chain_index_entry"""
        if not self.enabled:
            return
            
        try:
            self._run_async(self.update_chain_index_entry(block_hash, block_info))
        except Exception as e:
            logger.error(f"Failed to update chain index entry (sync): {e}")
    
    def update_height_index_entry_sync(self, height: int, block_hash: str):
        """Synchronous wrapper for update_height_index_entry"""
        if not self.enabled:
            return
            
        try:
            self._run_async(self.update_height_index_entry(height, block_hash))
        except Exception as e:
            logger.error(f"Failed to update height index entry (sync): {e}")
    
    def remove_chain_index_entry_sync(self, block_hash: str):
        """Synchronous wrapper for remove_chain_index_entry"""
        if not self.enabled:
            return
            
        try:
            self._run_async(self.remove_chain_index_entry(block_hash))
        except Exception as e:
            logger.error(f"Failed to remove chain index entry (sync): {e}")
    
    def remove_height_index_entry_sync(self, height: int):
        """Synchronous wrapper for remove_height_index_entry"""
        if not self.enabled:
            return
            
        try:
            self._run_async(self.remove_height_index_entry(height))
        except Exception as e:
            logger.error(f"Failed to remove height index entry (sync): {e}")