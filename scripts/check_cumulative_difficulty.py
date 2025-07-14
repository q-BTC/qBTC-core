#!/usr/bin/env python3
"""
Check blocks for cumulative difficulty field without modifying the database.
Works with read-only access while the validator is running.
"""

import sys
import os
import json
import logging
from decimal import Decimal

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import rocksdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def check_blocks(db_path="/app/db"):
    """Check blocks for cumulative difficulty"""
    try:
        # Open database in read-only mode
        db = rocksdict.Rdict(db_path, read_only=True)
        
        total_blocks = 0
        blocks_with_cd = 0
        blocks_without_cd = 0
        
        # Iterate through all blocks
        for key_bytes in db.keys():
            try:
                key = key_bytes.decode('utf-8', errors='ignore')
                if key.startswith("block:"):
                    total_blocks += 1
                    block_data = db.get(key_bytes)
                    if block_data:
                        block_info = json.loads(block_data.decode())
                        if "cumulative_difficulty" in block_info:
                            blocks_with_cd += 1
                        else:
                            blocks_without_cd += 1
                            if blocks_without_cd <= 5:  # Show first 5 blocks without CD
                                logger.info(f"Block without cumulative_difficulty: {key[6:]} at height {block_info.get('height', 'unknown')}")
            except Exception as e:
                logger.debug(f"Error processing key {key_bytes}: {e}")
                continue
        
        logger.info(f"Total blocks: {total_blocks}")
        logger.info(f"Blocks with cumulative_difficulty: {blocks_with_cd}")
        logger.info(f"Blocks without cumulative_difficulty: {blocks_without_cd}")
        
        if blocks_without_cd > 0:
            logger.info(f"Migration needed for {blocks_without_cd} blocks")
            return False
        else:
            logger.info("All blocks have cumulative_difficulty - no migration needed")
            return True
            
    except Exception as e:
        logger.error(f"Failed to check blocks: {e}")
        return False


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Check blocks for cumulative difficulty")
    parser.add_argument("--db-path", default="/app/db", help="Path to RocksDB database")
    args = parser.parse_args()
    
    success = check_blocks(args.db_path)
    sys.exit(0 if success else 1)