#!/usr/bin/env python3
"""
Migration script to add cumulative difficulty to existing blocks in the database.
This is a one-time migration to improve startup performance.
"""

import sys
import os
import json
import logging
import time
from decimal import Decimal
from typing import Dict

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.database import get_db
from blockchain.blockchain import bits_to_target

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def calculate_cumulative_difficulty(db, block_hash: str, cache: Dict[str, Decimal]) -> Decimal:
    """Calculate cumulative difficulty for a block, using cache for efficiency"""
    if block_hash in cache:
        return cache[block_hash]
    
    if not block_hash or block_hash == "00" * 32:
        return Decimal(0)
    
    # Get block data
    block_key = f"block:{block_hash}".encode()
    block_data = db.get(block_key)
    if not block_data:
        logger.warning(f"Block {block_hash} not found")
        return Decimal(0)
    
    block_info = json.loads(block_data.decode())
    
    # Check if already has cumulative difficulty
    if "cumulative_difficulty" in block_info:
        difficulty = Decimal(block_info["cumulative_difficulty"])
        cache[block_hash] = difficulty
        return difficulty
    
    # Calculate parent's cumulative difficulty
    parent_hash = block_info.get("previous_hash")
    parent_cumulative = calculate_cumulative_difficulty(db, parent_hash, cache)
    
    # Calculate this block's difficulty
    bits = block_info.get("bits", 0x1d00ffff)
    target = bits_to_target(bits)
    block_difficulty = Decimal(2**256) / Decimal(target)
    
    cumulative = parent_cumulative + block_difficulty
    cache[block_hash] = cumulative
    
    return cumulative


def migrate_blocks():
    """Add cumulative difficulty to all blocks that don't have it"""
    db = get_db()
    
    logger.info("Starting cumulative difficulty migration...")
    start_time = time.time()
    
    # First pass: count blocks needing migration
    blocks_to_migrate = []
    total_blocks = 0
    
    for key, value in db.items():
        if key.startswith(b"block:"):
            total_blocks += 1
            block_data = json.loads(value.decode())
            if "cumulative_difficulty" not in block_data:
                block_hash = block_data["block_hash"]
                blocks_to_migrate.append(block_hash)
    
    if not blocks_to_migrate:
        logger.info("No blocks need migration. All blocks already have cumulative difficulty.")
        return
    
    logger.info(f"Found {len(blocks_to_migrate)} blocks (out of {total_blocks}) needing migration")
    
    # Build a cache to avoid recalculating
    difficulty_cache = {}
    
    # Second pass: calculate and update cumulative difficulties
    updated_count = 0
    
    for i, block_hash in enumerate(blocks_to_migrate):
        if i % 1000 == 0 and i > 0:
            logger.info(f"Progress: {i}/{len(blocks_to_migrate)} blocks migrated...")
        
        # Get block data
        block_key = f"block:{block_hash}".encode()
        block_data = db.get(block_key)
        if not block_data:
            logger.warning(f"Block {block_hash} disappeared during migration")
            continue
        
        block_info = json.loads(block_data.decode())
        
        # Calculate cumulative difficulty
        cumulative_difficulty = calculate_cumulative_difficulty(db, block_hash, difficulty_cache)
        
        # Update block with cumulative difficulty
        block_info["cumulative_difficulty"] = str(cumulative_difficulty)
        
        # Save updated block
        db.put(block_key, json.dumps(block_info).encode())
        updated_count += 1
    
    elapsed = time.time() - start_time
    logger.info(f"Migration complete! Updated {updated_count} blocks in {elapsed:.2f} seconds")
    logger.info(f"Average time per block: {elapsed/updated_count*1000:.2f}ms")


if __name__ == "__main__":
    try:
        migrate_blocks()
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)