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
    
    # Legacy method - find highest block
    try:
        # Count blocks first
        block_count = sum(1 for k, _ in db.items() if k.startswith(b"block:"))
        logging.info(f"Legacy method: Found {block_count} blocks in database")
        
        tip_block = max(
            (json.loads(v.decode())             # each decoded block dict
             for k, v in db.items()
             if k.startswith(b"block:")),
            key=lambda blk: blk["height"]       # pick the one with max height
        )
        logging.info(f"Legacy method: Best block height={tip_block['height']}")
        return tip_block["height"], tip_block["block_hash"]

    except ValueError:                          # raised if the generator is empty
        logging.info("Legacy method: No blocks found, returning -1")
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
