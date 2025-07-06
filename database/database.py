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
        from blockchain.chain_manager import ChainManager
        cm = ChainManager()
        best_hash, best_height = cm.get_best_chain_tip()
        if best_hash != "00" * 32:  # Not genesis
            return best_height, best_hash
    except Exception as e:
        # ChainManager not available or failed, fall back to old method
        logging.debug(f"ChainManager not available, using legacy method: {e}")
    
    # Legacy method - find highest block
    try:
        tip_block = max(
            (json.loads(v.decode())             # each decoded block dict
             for k, v in db.items()
             if k.startswith(b"block:")),
            key=lambda blk: blk["height"]       # pick the one with max height
        )
        return tip_block["height"], tip_block["block_hash"]

    except ValueError:                          # raised if the generator is empty
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
