#!/usr/bin/env python3
"""
Script to rebuild the block height index for existing blocks.
This is needed when upgrading to the new indexed system.
"""

import sys
import os
import time
import logging

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.database import set_db, get_db
from blockchain.block_height_index import get_height_index
from blockchain.chain_singleton import get_chain_manager_sync
from blockchain.chain_init import init_chain_manager_sync

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def rebuild_height_index():
    """Rebuild the height index from existing blocks"""
    logger.info("Starting height index rebuild...")
    
    # Initialize database
    db_path = os.environ.get('ROCKSDB_PATH', './ledger.rocksdb')
    set_db(db_path)
    db = get_db()
    
    # Get the height index
    height_index = get_height_index()
    
    # Initialize ChainManager
    init_chain_manager_sync()
    
    # Get current state
    cm = get_chain_manager_sync()
    # Get best chain tip using direct access to block index
    best_tip = None
    best_difficulty = 0
    best_height = -1
    
    for tip_hash in cm.chain_tips:
        tip_info = cm.block_index.get(tip_hash)
        if tip_info and tip_info.get("cumulative_difficulty", 0) > best_difficulty:
            best_difficulty = tip_info["cumulative_difficulty"]
            best_tip = tip_hash
            best_height = tip_info["height"]
    
    best_hash = best_tip if best_tip else "00" * 32
    
    logger.info(f"Current blockchain height: {best_height}")
    
    # Check current index state
    highest_indexed = height_index.get_highest_indexed_height_sync()
    logger.info(f"Highest indexed block: {highest_indexed}")
    
    if highest_indexed >= best_height:
        logger.info("Height index is already up to date!")
        return
    
    # Count total blocks
    block_count = 0
    for key in db.keys():
        if key.startswith(b"block:"):
            block_count += 1
    
    logger.info(f"Found {block_count} total blocks in database")
    
    # Rebuild the index
    start_time = time.time()
    height_index.rebuild_index()
    
    # Verify the rebuild
    new_highest = height_index.get_highest_indexed_height_sync()
    elapsed = time.time() - start_time
    
    logger.info(f"Index rebuild complete in {elapsed:.2f} seconds")
    logger.info(f"New highest indexed block: {new_highest}")
    
    # Test the index with a few lookups
    logger.info("\nTesting index with sample lookups:")
    test_heights = [0, 100, 500, 1000, 2000, 3000, best_height]
    
    for height in test_heights:
        if height <= best_height:
            start = time.time()
            block_hash = height_index.get_block_hash_by_height(height)
            lookup_time = (time.time() - start) * 1000  # Convert to ms
            
            if block_hash:
                logger.info(f"  Height {height}: {block_hash[:16]}... (lookup: {lookup_time:.3f}ms)")
            else:
                logger.info(f"  Height {height}: Not found")
    
    logger.info("\nHeight index rebuild successful!")

if __name__ == "__main__":
    try:
        rebuild_height_index()
    except Exception as e:
        logger.error(f"Failed to rebuild height index: {e}", exc_info=True)
        sys.exit(1)