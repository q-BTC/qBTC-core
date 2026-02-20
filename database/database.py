import logging
import json
import rocksdict
from rocksdict import Rdict, Options, BlockBasedOptions, Cache, DBCompressionType, SliceTransform
import time
import threading
import asyncio
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

async def get_current_height(db, max_retries: int = 3) -> Tuple[int, str]:
    """
    Return (height, block_hash) of the chain tip with robust error handling.
    Returns (-1, GENESIS_PREVHASH) if the DB has no blocks yet.
    Uses retry logic to handle transient failures.
    """
    # Check cache first for performance
    cached = height_cache.get()
    if cached:
        return cached

    # Try multiple times to get the height
    last_error = None
    
    for retry in range(max_retries):
        if retry > 0:
            # Brief delay between retries with exponential backoff
            await asyncio.sleep(0.1 * (2 ** (retry - 1)))
            
        try:
            # Method 1: Try ChainManager (preferred)
            try:
                from blockchain.chain_singleton import get_chain_manager
                cm = await get_chain_manager()
                # Use the async get_best_chain_tip method
                # Get the best chain tip using async method
                tip_info = await cm.get_best_chain_tip()
                if tip_info:
                    # get_best_chain_tip returns a tuple (block_hash, height)
                    best_hash, best_height = tip_info
                else:
                    best_hash = "00" * 32
                    best_height = -1
                
                # Validate the result
                if best_hash and best_hash != "00" * 32 and best_height >= 0:
                    logging.debug(f"ChainManager returned: hash={best_hash}, height={best_height}")
                    height_cache.set(best_height, best_hash)
                    return best_height, best_hash
                elif best_height == -1 and best_hash == "00" * 32:
                    # This is a valid "no blocks" response, not an error
                    logging.info("ChainManager indicates empty blockchain")
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
                
                highest_height = await height_index.get_highest_indexed_height()
                
                if highest_height >= 0:
                    block_hash = height_index.get_block_hash_by_height(highest_height)
                    if block_hash:
                        logging.debug(f"Height index method: Best block height={highest_height}")
                        height_cache.set(highest_height, block_hash)
                        return highest_height, block_hash
                
                # If height is -1, check if this is genuinely empty or an error
                if highest_height == -1:
                    # O(1) check: see if chain:best_tip exists (not a full DB scan)
                    tip_data = db.get(b"chain:best_tip")
                    if not tip_data:
                        logging.info("Height index confirms empty blockchain")
                        return -1, GENESIS_PREVHASH
                    else:
                        # We have a tip but index says -1 — corrupted/uninitialized index
                        raise ValueError("Height index returned -1 but blocks exist in DB")
                        
            except ImportError:
                logging.debug("Height index not available, trying scan method")
            except Exception as e:
                last_error = e
                logging.debug(f"Height index attempt {retry + 1} failed: {e}")
            
            # Method 3: Final fallback - read chain:best_tip directly (O(1))
            try:
                tip_data = db.get(b"chain:best_tip")
                if tip_data:
                    tip_info = json.loads(tip_data.decode())
                    tip_hash = tip_info.get("hash", "")
                    tip_height = tip_info.get("height", -1)
                    if tip_hash and tip_height >= 0:
                        logging.info(f"chain:best_tip fallback found tip at height {tip_height}")
                        height_cache.set(tip_height, tip_hash)
                        return tip_height, tip_hash

                # No chain:best_tip means empty blockchain
                logging.info("chain:best_tip fallback confirms empty blockchain")
                return -1, GENESIS_PREVHASH
                    
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
        # O(1) final check: see if chain:best_tip exists
        tip_data = db.get(b"chain:best_tip")
        if not tip_data:
            logging.info("Final check confirms empty blockchain (no chain:best_tip)")
            return -1, GENESIS_PREVHASH
        else:
            # We have a tip but all methods failed to parse it
            raise RuntimeError(f"chain:best_tip exists but height cannot be determined. "
                             f"This may indicate data corruption. Last error: {last_error}")
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Database error during final verification: {e}")

def set_db(db_path):
    global db
    if db is None:
        try:
            # Create optimized RocksDB options for blockchain workload
            opts = Options()

            # Write buffer configuration
            # Larger write buffers reduce write amplification and improve write throughput
            opts.set_write_buffer_size(256 * 1024 * 1024)  # 256MB per memtable
            opts.set_max_write_buffer_number(3)  # Allow up to 3 memtables
            opts.set_min_write_buffer_number_to_merge(2)  # Merge 2 memtables before flushing

            # SST file configuration
            opts.set_target_file_size_base(64 * 1024 * 1024)  # 64MB base file size
            opts.set_max_bytes_for_level_base(512 * 1024 * 1024)  # 512MB for level 1

            # Block cache for reads - critical for performance
            block_cache = Cache(512 * 1024 * 1024)  # 512MB block cache
            block_opts = BlockBasedOptions()
            block_opts.set_block_cache(block_cache)
            block_opts.set_block_size(16 * 1024)  # 16KB blocks
            block_opts.set_cache_index_and_filter_blocks(True)  # Cache index/filter blocks
            block_opts.set_pin_l0_filter_and_index_blocks_in_cache(True)  # Keep L0 in cache
            opts.set_block_based_table_factory(block_opts)

            # Compression - LZ4 for good balance of speed/ratio
            opts.set_compression_type(DBCompressionType.lz4())

            # Parallelism - use multiple threads for compaction
            opts.increase_parallelism(4)
            opts.set_max_background_jobs(4)

            # Optimize for point lookups and sequential scans
            opts.set_level_compaction_dynamic_level_bytes(True)

            # Prefix bloom filter for efficient prefix scans (block:, utxo:, etc.)
            prefix_transform = SliceTransform.create_fixed_prefix(6)  # "block:", "utxo:", etc. are 5-6 chars
            opts.set_prefix_extractor(prefix_transform)
            block_opts.set_bloom_filter(10, True)  # 10 bits per key, block-based

            db = Rdict(db_path, options=opts)
            logging.info(f"Database initialized at {db_path} with optimized RocksDB configuration")
            logging.info(f"RocksDB config: 256MB write buffer, 512MB block cache, LZ4 compression, 4 threads")
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
