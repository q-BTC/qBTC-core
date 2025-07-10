import logging
import json
import rocksdict
from typing import Tuple


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

db = None
GENESIS_PREVHASH = "00" * 32

def get_current_height(db) -> Tuple[int, str]:
    """
    Return (height, block_hash) of the chain tip.
    Returns (-1, GENESIS_PREVHASH) if the DB has no blocks yet.
    """
    # Try to use ChainManager if available
    try:
        from blockchain.chain_singleton import get_chain_manager
        cm = get_chain_manager()
        best_hash, best_height = cm.get_best_chain_tip()
        logging.info(f"ChainManager returned: hash={best_hash}, height={best_height}")
        if best_hash != "00" * 32:  # Not genesis
            return best_height, best_hash
    except Exception as e:
        # ChainManager not available or failed, fall back to old method
        logging.info(f"ChainManager failed, using legacy method: {e}")
    
    # Legacy method - use height index for efficiency
    try:
        from blockchain.block_height_index import get_height_index
        height_index = get_height_index()
        
        # Get the highest indexed height
        highest_height = height_index.get_highest_indexed_height()
        
        if highest_height >= 0:
            # Get the block hash at the highest height
            block_hash = height_index.get_block_hash_by_height(highest_height)
            if block_hash:
                logging.info(f"Height index method: Best block height={highest_height}")
                return highest_height, block_hash
        
        logging.info("Height index method: No blocks found, returning -1")
        return -1, GENESIS_PREVHASH
        
    except Exception as e:
        logging.warning(f"Height index failed: {e}, falling back to scan method")
        
        # Final fallback - scan all blocks (inefficient but works)
        try:
            # Count blocks first
            block_count = sum(1 for k, _ in db.items() if k.startswith(b"block:"))
            logging.info(f"Fallback scan method: Found {block_count} blocks in database")
            
            tip_block = max(
                (json.loads(v.decode())             # each decoded block dict
                 for k, v in db.items()
                 if k.startswith(b"block:")),
                key=lambda blk: blk["height"]       # pick the one with max height
            )
            logging.info(f"Fallback scan method: Best block height={tip_block['height']}")
            return tip_block["height"], tip_block["block_hash"]

        except ValueError:                          # raised if the generator is empty
            logging.info("Fallback scan method: No blocks found, returning -1")
            return -1, GENESIS_PREVHASH  # Return -1 for empty database

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

def close_db():
    global db
    if db is not None:
        db.close()
        logging.info("Database closed")
        db = None
