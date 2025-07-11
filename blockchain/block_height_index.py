"""
Block Height Index - Provides efficient block lookups by height
"""
import json
import logging
from typing import Optional, Dict
from database.database import get_db

logger = logging.getLogger(__name__)

class BlockHeightIndex:
    """
    Maintains a height-to-hash index for fast block lookups.
    Uses RocksDB keys with format: height:XXXXXXXX -> block_hash
    """
    
    def __init__(self, db=None):
        self.db = db if db is not None else get_db()
        self._memory_cache: Dict[int, str] = {}  # height -> block_hash cache
        self._cache_size_limit = 1000  # Keep last 1000 blocks in memory
        
    def get_block_hash_by_height(self, height: int) -> Optional[str]:
        """Get block hash for a given height"""
        # Check memory cache first
        if height in self._memory_cache:
            return self._memory_cache[height]
            
        # Check database index
        height_key = f"height:{height:08d}".encode()
        hash_data = self.db.get(height_key)
        
        if hash_data:
            block_hash = hash_data.decode()
            # Add to cache
            self._add_to_cache(height, block_hash)
            return block_hash
            
        return None
    
    def get_block_by_height(self, height: int) -> Optional[dict]:
        """Get full block data for a given height"""
        block_hash = self.get_block_hash_by_height(height)
        if not block_hash:
            return None
            
        block_key = f"block:{block_hash}".encode()
        block_data = self.db.get(block_key)
        
        if block_data:
            return json.loads(block_data.decode())
            
        return None
    
    def add_block_to_index(self, height: int, block_hash: str):
        """Add a block to the height index"""
        height_key = f"height:{height:08d}".encode()
        self.db.put(height_key, block_hash.encode())
        
        # Add to cache
        self._add_to_cache(height, block_hash)
        
        logger.debug(f"Added block {block_hash} at height {height} to index")
    
    def remove_block_from_index(self, height: int):
        """Remove a block from the height index (used during reorgs)"""
        height_key = f"height:{height:08d}".encode()
        if height_key in self.db:
            self.db.delete(height_key)
            
        # Remove from cache
        if height in self._memory_cache:
            del self._memory_cache[height]
            
        logger.debug(f"Removed block at height {height} from index")
    
    def _add_to_cache(self, height: int, block_hash: str):
        """Add entry to memory cache with size limit"""
        self._memory_cache[height] = block_hash
        
        # Remove oldest entries if cache is too large
        if len(self._memory_cache) > self._cache_size_limit:
            # Remove the lowest height (oldest) entries
            heights_to_remove = sorted(self._memory_cache.keys())[:100]
            for h in heights_to_remove:
                del self._memory_cache[h]
    
    def rebuild_index(self):
        """Rebuild the entire height index from existing blocks"""
        logger.info("Rebuilding block height index...")
        
        count = 0
        for key, value in self.db.items():
            if key.startswith(b"block:"):
                block_data = json.loads(value.decode())
                height = block_data.get("height")
                block_hash = block_data.get("block_hash")
                
                if height is not None and block_hash:
                    self.add_block_to_index(height, block_hash)
                    count += 1
                    
                    if count % 1000 == 0:
                        logger.info(f"Indexed {count} blocks...")
        
        logger.info(f"Block height index rebuilt with {count} blocks")
    
    def get_highest_indexed_height(self) -> int:
        """Get the highest block height in the index"""
        highest = -1
        
        # Check database for height keys
        for key in self.db.keys():
            if key.startswith(b"height:"):
                try:
                    height_str = key[7:].decode()  # Skip "height:" prefix
                    height = int(height_str)
                    if height > highest:
                        highest = height
                except:
                    continue
                    
        return highest

# Global singleton instance
_height_index_instance = None

def get_height_index() -> BlockHeightIndex:
    """Get the singleton BlockHeightIndex instance"""
    global _height_index_instance
    if _height_index_instance is None:
        _height_index_instance = BlockHeightIndex()
    return _height_index_instance