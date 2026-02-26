"""
Redis caching layer for blockchain indexes to speed up startup.
Uses redis.asyncio to avoid blocking the event loop.
"""

import json
import logging
from typing import Optional, Dict, Any, List
from decimal import Decimal

try:
    import redis
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)

# M4: Cache version for staleness detection across restarts
_CACHE_VERSION = "qbtc-cache-v2"
_CACHE_VERSION_KEY = "blockchain:cache_version"

# Default TTLs (in seconds)
_TTL_CHAIN_INDEX = 3600       # 1 hour
_TTL_HEIGHT_INDEX = 1800      # 30 minutes
_TTL_DIFFICULTY = 3600        # 1 hour
_TTL_BEST_CHAIN = 3600        # 1 hour
_TTL_CHAIN_STATS = 3600       # 1 hour

class BlockchainRedisCache:
    """Redis cache for blockchain indexes and computed values.
    All runtime methods are async to avoid blocking the event loop."""

    def __init__(self, redis_url: Optional[str] = None):
        self.redis_client = None
        self.enabled = False

        if redis_url and REDIS_AVAILABLE:
            try:
                # One-time sync init: ping + version check
                sync_client = redis.from_url(
                    redis_url, decode_responses=True,
                    socket_connect_timeout=5, socket_timeout=5
                )
                sync_client.ping()

                # M4: Validate cache version — invalidate stale caches on mismatch
                stored_version = sync_client.get(_CACHE_VERSION_KEY)
                if stored_version != _CACHE_VERSION:
                    logger.warning(f"Cache version mismatch (stored={stored_version}, expected={_CACHE_VERSION}) — invalidating all")
                    cursor = 0
                    while True:
                        cursor, keys = sync_client.scan(cursor, match="blockchain:*", count=100)
                        if keys:
                            sync_client.delete(*keys)
                        if cursor == 0:
                            break
                    sync_client.set(_CACHE_VERSION_KEY, _CACHE_VERSION)

                sync_client.close()

                # Create async client for all runtime operations
                pool = aioredis.ConnectionPool.from_url(
                    redis_url,
                    decode_responses=True,
                    max_connections=5,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                self.redis_client = aioredis.Redis(connection_pool=pool)
                self.enabled = True

                logger.info("Blockchain Redis cache initialized (async)")
            except Exception as e:
                logger.warning(f"Failed to initialize Redis cache: {e}")
                self.enabled = False

    async def _execute(self, coro):
        """Await a Redis coroutine with error handling"""
        if not self.enabled:
            return None
        try:
            return await coro
        except Exception as e:
            logger.error(f"Redis operation failed: {e}")
            return None

    async def close(self):
        """Close Redis connection"""
        if self.redis_client:
            try:
                await self.redis_client.aclose()
            except:
                pass

    async def ping(self) -> bool:
        """Check if Redis connection is alive"""
        if not self.enabled:
            return False
        try:
            return await self._execute(self.redis_client.ping())
        except:
            return False

    # Chain Index Cache
    async def get_chain_index(self) -> Optional[Dict[str, Any]]:
        """Get cached chain index"""
        if not self.enabled:
            return None

        try:
            cached_data = await self._execute(self.redis_client.get("blockchain:chain_index"))
            if cached_data:
                data = json.loads(cached_data)
                if "block_index" in data:
                    for block_hash, block_info in data["block_index"].items():
                        if "cumulative_difficulty" in block_info:
                            block_info["cumulative_difficulty"] = Decimal(block_info["cumulative_difficulty"])
                return data
        except Exception as e:
            logger.error(f"Failed to get chain index from cache: {e}")
        return None

    async def set_chain_index(self, index: Dict[str, Any], ttl: int = _TTL_CHAIN_INDEX):
        """Cache the chain index"""
        if not self.enabled:
            return

        try:
            serializable_index = {
                "chain_tips": index.get("chain_tips", []),
                "block_index": {}
            }

            for block_hash, block_info in index.get("block_index", {}).items():
                info_copy = block_info.copy()
                if "cumulative_difficulty" in info_copy:
                    info_copy["cumulative_difficulty"] = str(info_copy["cumulative_difficulty"])
                serializable_index["block_index"][block_hash] = info_copy

            result = await self._execute(
                self.redis_client.setex(
                    "blockchain:chain_index",
                    ttl,
                    json.dumps(serializable_index)
                )
            )
            if result:
                logger.info(f"Successfully cached chain index with {len(serializable_index['block_index'])} blocks")
        except Exception as e:
            logger.error(f"Failed to cache chain index: {e}")

    # Cumulative Difficulty Cache
    async def get_cumulative_difficulty(self, block_hash: str) -> Optional[Decimal]:
        """Get cached cumulative difficulty for a block"""
        if not self.enabled:
            return None

        try:
            cached_value = await self._execute(self.redis_client.get(f"blockchain:difficulty:{block_hash}"))
            if cached_value:
                return Decimal(cached_value)
        except Exception as e:
            logger.error(f"Failed to get cumulative difficulty: {e}")
        return None

    async def set_cumulative_difficulty(self, block_hash: str, difficulty: Decimal, ttl: int = _TTL_DIFFICULTY):
        """Cache cumulative difficulty for a block"""
        if not self.enabled:
            return

        try:
            await self._execute(
                self.redis_client.setex(
                    f"blockchain:difficulty:{block_hash}",
                    ttl,
                    str(difficulty)
                )
            )
        except Exception as e:
            logger.error(f"Failed to cache cumulative difficulty: {e}")

    # Height Index Cache
    async def get_height_index(self) -> Optional[Dict[int, str]]:
        """Get cached height-to-block-hash index"""
        if not self.enabled:
            return None

        try:
            cached_data = await self._execute(self.redis_client.get("blockchain:height_index"))
            if cached_data:
                str_index = json.loads(cached_data)
                return {int(k): v for k, v in str_index.items()}
        except Exception as e:
            logger.error(f"Failed to get height index: {e}")
        return None

    async def set_height_index(self, index: Dict[int, str], ttl: int = _TTL_HEIGHT_INDEX):
        """Cache the height index"""
        if not self.enabled:
            return

        try:
            str_index = {str(k): v for k, v in index.items()}
            await self._execute(
                self.redis_client.setex(
                    "blockchain:height_index",
                    ttl,
                    json.dumps(str_index)
                )
            )
        except Exception as e:
            logger.error(f"Failed to cache height index: {e}")

    # Best Chain Cache
    async def get_best_chain_info(self) -> Optional[Dict[str, Any]]:
        """Get cached best chain information"""
        if not self.enabled:
            return None

        try:
            data = await self._execute(self.redis_client.get("blockchain:best_chain"))
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Failed to get best chain info: {e}")
        return None

    async def set_best_chain_info(self, info: Dict[str, Any], ttl: int = _TTL_BEST_CHAIN):
        """Cache best chain information"""
        if not self.enabled:
            return

        try:
            await self._execute(
                self.redis_client.setex(
                    "blockchain:best_chain",
                    ttl,
                    json.dumps(info)
                )
            )
        except Exception as e:
            logger.error(f"Failed to cache best chain info: {e}")

    # Chain Statistics Cache
    async def get_chain_stats(self) -> Optional[Dict[str, Any]]:
        """Get cached chain statistics"""
        if not self.enabled:
            return None

        try:
            data = await self._execute(self.redis_client.get("blockchain:chain_stats"))
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Failed to get chain stats: {e}")
        return None

    async def set_chain_stats(self, stats: Dict[str, Any], ttl: int = _TTL_CHAIN_STATS):
        """Cache chain statistics"""
        if not self.enabled:
            return

        try:
            await self._execute(
                self.redis_client.setex(
                    "blockchain:chain_stats",
                    ttl,
                    json.dumps(stats)
                )
            )
        except Exception as e:
            logger.error(f"Failed to cache chain stats: {e}")

    # Incremental Cache Updates
    async def update_chain_index_entry(self, block_hash: str, block_info: Dict[str, Any]):
        """Update a single entry in the cached chain index"""
        if not self.enabled:
            return

        try:
            cached_data = await self.get_chain_index()
            if cached_data:
                cached_data["block_index"][block_hash] = block_info
                if block_info.get("is_tip", False):
                    if isinstance(cached_data["chain_tips"], list):
                        if block_hash not in cached_data["chain_tips"]:
                            cached_data["chain_tips"].append(block_hash)
                    else:
                        cached_data["chain_tips"] = [block_hash]
                await self.set_chain_index(cached_data)
        except Exception as e:
            logger.error(f"Failed to update chain index entry: {e}")

    async def update_height_index_entry(self, height: int, block_hash: str):
        """Update a single entry in the cached height index"""
        if not self.enabled:
            return

        try:
            cached_index = await self.get_height_index()
            if cached_index:
                cached_index[height] = block_hash
                await self.set_height_index(cached_index)
        except Exception as e:
            logger.error(f"Failed to update height index entry: {e}")

    async def delete_best_chain_info(self):
        """Delete cached best chain information"""
        if not self.enabled:
            return
        try:
            await self._execute(self.redis_client.delete("blockchain:best_chain"))
        except Exception as e:
            logger.error(f"Failed to delete best chain info: {e}")

    async def remove_chain_index_entry(self, block_hash: str):
        """Remove a single entry from the cached chain index"""
        if not self.enabled:
            return
        try:
            cached_data = await self.get_chain_index()
            if cached_data and block_hash in cached_data.get("block_index", {}):
                del cached_data["block_index"][block_hash]
                tips = cached_data.get("chain_tips", [])
                if block_hash in tips:
                    tips.remove(block_hash)
                await self.set_chain_index(cached_data)
        except Exception as e:
            logger.error(f"Failed to remove chain index entry: {e}")

    async def remove_height_index_entry(self, height: int):
        """Remove a single entry from the cached height index"""
        if not self.enabled:
            return
        try:
            cached_index = await self.get_height_index()
            if cached_index and height in cached_index:
                del cached_index[height]
                await self.set_height_index(cached_index)
        except Exception as e:
            logger.error(f"Failed to remove height index entry: {e}")

    # Batch Operations
    async def invalidate_all(self):
        """Invalidate all cached data"""
        if not self.enabled:
            return

        try:
            deleted = 0
            cursor = 0
            while True:
                cursor, keys = await self.redis_client.scan(cursor, match="blockchain:*", count=100)
                if keys:
                    await self._execute(self.redis_client.delete(*keys))
                    deleted += len(keys)
                if cursor == 0:
                    break
            if deleted:
                logger.info(f"Invalidated {deleted} cache entries")
        except Exception as e:
            logger.error(f"Failed to invalidate cache: {e}")

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        if not self.enabled:
            return {"enabled": False}

        try:
            info = await self._execute(self.redis_client.info("stats"))
            dbsize = await self._execute(self.redis_client.dbsize())
            return {
                "enabled": True,
                "hits": info.get("keyspace_hits", 0),
                "misses": info.get("keyspace_misses", 0),
                "hit_rate": info.get("keyspace_hits", 0) / max(1, info.get("keyspace_hits", 0) + info.get("keyspace_misses", 0)),
                "total_keys": dbsize or 0
            }
        except Exception as e:
            logger.error(f"Failed to get cache stats: {e}")
            return {"enabled": True, "error": str(e)}
