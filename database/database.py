import logging
import json
import rocksdict
import time
import threading
from typing import Tuple, Optional


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

db = None
GENESIS_PREVHASH = "00" * 32

# Height cache with thread safety
class HeightCache:
    def __init__(self, ttl_seconds: float = 1.0):
        self.cache = None
        self.cache_time = 0
        self.ttl = ttl_seconds
        self.lock = threading.RLock()
        self.last_known_good = None  # Fallback for when all methods fail
        
    def get(self) -> Optional[Tuple[int, str]]:
        with self.lock:
            now = time.time()
            if self.cache and (now - self.cache_time) < self.ttl:
                return self.cache
            return None
    
    def set(self, height: int, block_hash: str):
        with self.lock:
            self.cache = (height, block_hash)
            self.cache_time = time.time()
            # Only update last_known_good if height is valid (>= 0 includes genesis)
            # -1 is a valid "no blocks" state but not a "known good" state
            if height >= 0:
                self.last_known_good = (height, block_hash)
    
    def get_last_known_good(self) -> Optional[Tuple[int, str]]:
        with self.lock:
            return self.last_known_good
    
    def invalidate(self):
        with self.lock:
            self.cache = None
            self.cache_time = 0

# Global height cache instance
height_cache = HeightCache(ttl_seconds=1.0)

def get_current_height(db, max_retries: int = 3) -> Tuple[int, str]:
    """
    Return (height, block_hash) of the chain tip with robust error handling.
    Returns (-1, GENESIS_PREVHASH) if the DB has no blocks yet.
    Uses caching and retry logic to handle transient failures.
    """
    # Check cache first
    cached = height_cache.get()
    if cached:
        return cached
    
    # Try multiple times to get the height
    last_error = None
    
    for retry in range(max_retries):
        if retry > 0:
            # Brief delay between retries with exponential backoff
            time.sleep(0.1 * (2 ** (retry - 1)))
            
        try:
            # Method 1: Try ChainManager (preferred)
            try:
                from blockchain.chain_singleton import get_chain_manager
                cm = get_chain_manager()
                best_hash, best_height = cm.get_best_chain_tip()
                
                # Validate the result
                if best_hash and best_hash != "00" * 32 and best_height >= 0:
                    logging.debug(f"ChainManager returned: hash={best_hash}, height={best_height}")
                    height_cache.set(best_height, best_hash)
                    return best_height, best_hash
                elif best_height == -1 and best_hash == "00" * 32:
                    # This is a valid "no blocks" response, not an error
                    logging.info("ChainManager indicates empty blockchain")
                    # Cache this as it's a valid state
                    height_cache.set(-1, GENESIS_PREVHASH)
                    return -1, GENESIS_PREVHASH
                    
            except ImportError:
                # ChainManager module not available, this is ok
                logging.debug("ChainManager not available, trying height index")
            except Exception as e:
                last_error = e
                logging.debug(f"ChainManager attempt {retry + 1} failed: {e}")
            
            # Method 2: Try height index
            try:
                from blockchain.block_height_index import get_height_index
                height_index = get_height_index()
                
                highest_height = height_index.get_highest_indexed_height()
                
                if highest_height >= 0:
                    block_hash = height_index.get_block_hash_by_height(highest_height)
                    if block_hash:
                        logging.debug(f"Height index method: Best block height={highest_height}")
                        height_cache.set(highest_height, block_hash)
                        return highest_height, block_hash
                
                # If height is -1, check if this is genuinely empty or an error
                if highest_height == -1:
                    # Quick check: are there any blocks in the DB?
                    has_blocks = any(k.startswith(b"block:") for k in db.keys())
                    if not has_blocks:
                        logging.info("Height index confirms empty blockchain")
                        height_cache.set(-1, GENESIS_PREVHASH)
                        return -1, GENESIS_PREVHASH
                    else:
                        # This is suspicious - we have blocks but index says -1
                        # This indicates a corrupted or uninitialized index
                        raise ValueError("Height index returned -1 but blocks exist in DB")
                        
            except ImportError:
                logging.debug("Height index not available, trying scan method")
            except Exception as e:
                last_error = e
                logging.debug(f"Height index attempt {retry + 1} failed: {e}")
            
            # Method 3: Final fallback - scan all blocks
            try:
                # First check if we have any blocks at all
                block_count = 0
                tip_block = None
                
                for k, v in db.items():
                    if k.startswith(b"block:"):
                        block_count += 1
                        if block_count == 1:
                            # Initialize with first block found
                            tip_block = json.loads(v.decode())
                        else:
                            # Compare with current tip
                            block = json.loads(v.decode())
                            if block.get("height", -1) > tip_block.get("height", -1):
                                tip_block = block
                
                if tip_block and tip_block.get("height") is not None:
                    height = tip_block["height"]
                    block_hash = tip_block.get("block_hash", "")
                    if height >= 0 and block_hash:
                        logging.info(f"Scan method found tip at height {height}")
                        height_cache.set(height, block_hash)
                        return height, block_hash
                
                if block_count == 0:
                    logging.info("Scan method confirms empty blockchain")
                    height_cache.set(-1, GENESIS_PREVHASH)
                    return -1, GENESIS_PREVHASH
                else:
                    raise ValueError(f"Found {block_count} blocks but couldn't determine tip")
                    
            except Exception as e:
                last_error = e
                logging.warning(f"Scan method attempt {retry + 1} failed: {e}")
                
        except Exception as e:
            last_error = e
            logging.error(f"Unexpected error in height retrieval attempt {retry + 1}: {e}")
    
    # All methods failed - check if we have a last known good height
    last_known = height_cache.get_last_known_good()
    if last_known:
        height, block_hash = last_known
        logging.warning(f"All height retrieval methods failed after {max_retries} attempts. "
                       f"Using last known good height: {height}")
        # Don't update cache time, but return the last known value
        return height, block_hash
    
    # No last known good height - check if DB is accessible
    logging.error(f"Failed to get current height after {max_retries} attempts. "
                 f"Last error: {last_error}")
    
    # One final check - if DB is actually inaccessible, raise the error
    try:
        # Try a simple DB operation
        _ = db.get(b"test")
    except Exception as db_error:
        logging.error(f"Database appears to be inaccessible: {db_error}")
        raise RuntimeError(f"Database error prevented height retrieval: {db_error}")
    
    # DB is accessible but we couldn't determine height
    # Only return -1 if we're absolutely sure there are no blocks
    # Do one more careful check
    try:
        has_any_blocks = False
        for key in db.keys():
            if key.startswith(b"block:"):
                has_any_blocks = True
                break
        
        if not has_any_blocks:
            # Confirmed: database is empty
            logging.info("Final check confirms empty blockchain")
            height_cache.set(-1, GENESIS_PREVHASH)
            return -1, GENESIS_PREVHASH
        else:
            # We have blocks but can't determine height - this is an error condition
            # Don't return -1 as that would cause sync issues
            raise RuntimeError(f"Database contains blocks but height cannot be determined. "
                             f"This may indicate index corruption. Last error: {last_error}")
    except Exception as e:
        # If we can't even iterate keys, the DB has issues
        raise RuntimeError(f"Database error during final verification: {e}")

def set_db(db_path):
    global db
    if db is None:
        try:
            db = rocksdict.Rdict(db_path)
            logging.info(f"Database initialized at {db_path}")
        except Exception as e:
            logging.error(f"Failed to initialize RocksDB at {db_path}: {e}")
            raise
    else:
        logging.info(f"Database already initialized at {db_path}")
    return db

def get_db():
    if db is None:
        raise RuntimeError("Database not initialized yet")
    return db

def invalidate_height_cache():
    """Invalidate the height cache. Should be called when new blocks are added."""
    height_cache.invalidate()
    logging.debug("Height cache invalidated")

def close_db():
    global db
    if db is not None:
        db.close()
        logging.info("Database closed")
        db = None
