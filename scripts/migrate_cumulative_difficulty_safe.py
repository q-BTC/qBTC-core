#!/usr/bin/env python3
"""
Safe migration script to add cumulative difficulty to existing blocks.
Includes dry-run mode and verification.
"""

import sys
import os
import json
import logging
import time
from decimal import Decimal
from typing import Dict
import argparse

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.database import get_db, set_db
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
    
    # Calculate this block's difficulty with safety check
    bits = block_info.get("bits", 0x1d00ffff)
    try:
        target = bits_to_target(bits)
        if target == 0:
            logger.error(f"Invalid target (0) for block {block_hash} with bits {bits:#x}")
            return parent_cumulative
        block_difficulty = Decimal(2**256) / Decimal(target)
    except Exception as e:
        logger.error(f"Error calculating difficulty for block {block_hash}: {e}")
        return parent_cumulative
    
    cumulative = parent_cumulative + block_difficulty
    cache[block_hash] = cumulative
    
    return cumulative


def verify_migration(db, sample_size=100):
    """Verify a sample of migrated blocks have reasonable cumulative difficulties"""
    logger.info(f"Verifying migration with sample size {sample_size}...")
    
    blocks_checked = 0
    issues_found = 0
    
    for key, value in db.items():
        if key.startswith(b"block:") and blocks_checked < sample_size:
            block_data = json.loads(value.decode())
            
            if "cumulative_difficulty" in block_data:
                blocks_checked += 1
                
                # Verify it's a valid number
                try:
                    diff = Decimal(block_data["cumulative_difficulty"])
                    
                    # Basic sanity checks
                    if diff < 0:
                        logger.error(f"Negative cumulative difficulty in block {block_data['block_hash']}")
                        issues_found += 1
                    elif block_data["height"] > 0 and diff == 0:
                        logger.error(f"Zero cumulative difficulty for non-genesis block {block_data['block_hash']}")
                        issues_found += 1
                        
                except Exception as e:
                    logger.error(f"Invalid cumulative difficulty format in block {block_data['block_hash']}: {e}")
                    issues_found += 1
    
    logger.info(f"Verification complete. Checked {blocks_checked} blocks, found {issues_found} issues")
    return issues_found == 0


def migrate_blocks(dry_run=False):
    """Add cumulative difficulty to all blocks that don't have it"""
    db = get_db()
    
    mode = "DRY RUN" if dry_run else "LIVE"
    logger.info(f"Starting cumulative difficulty migration in {mode} mode...")
    start_time = time.time()
    
    # First pass: count blocks needing migration
    blocks_to_migrate = []
    total_blocks = 0
    
    logger.info("Scanning blockchain...")
    for key, value in db.items():
        if key.startswith(b"block:"):
            total_blocks += 1
            block_data = json.loads(value.decode())
            if "cumulative_difficulty" not in block_data:
                block_hash = block_data["block_hash"]
                blocks_to_migrate.append(block_hash)
    
    if not blocks_to_migrate:
        logger.info("No blocks need migration. All blocks already have cumulative difficulty.")
        return True
    
    logger.info(f"Found {len(blocks_to_migrate)} blocks (out of {total_blocks}) needing migration")
    
    if dry_run:
        logger.info("DRY RUN: Would migrate the following blocks:")
        for i, block_hash in enumerate(blocks_to_migrate[:10]):  # Show first 10
            logger.info(f"  {i+1}. {block_hash}")
        if len(blocks_to_migrate) > 10:
            logger.info(f"  ... and {len(blocks_to_migrate) - 10} more blocks")
    
    # Build a cache to avoid recalculating
    difficulty_cache = {}
    
    # Second pass: calculate and update cumulative difficulties
    updated_count = 0
    errors = 0
    
    for i, block_hash in enumerate(blocks_to_migrate):
        if i % 1000 == 0 and i > 0:
            logger.info(f"Progress: {i}/{len(blocks_to_migrate)} blocks processed...")
        
        try:
            # Get block data
            block_key = f"block:{block_hash}".encode()
            block_data = db.get(block_key)
            if not block_data:
                logger.warning(f"Block {block_hash} disappeared during migration")
                errors += 1
                continue
            
            block_info = json.loads(block_data.decode())
            
            # Calculate cumulative difficulty
            cumulative_difficulty = calculate_cumulative_difficulty(db, block_hash, difficulty_cache)
            
            if dry_run:
                if i < 5:  # Show first 5 calculations in dry run
                    logger.info(f"Would set block {block_hash} cumulative_difficulty to {cumulative_difficulty}")
            else:
                # Update block with cumulative difficulty
                block_info["cumulative_difficulty"] = str(cumulative_difficulty)
                
                # Save updated block
                db.put(block_key, json.dumps(block_info).encode())
            
            updated_count += 1
            
        except Exception as e:
            logger.error(f"Error processing block {block_hash}: {e}")
            errors += 1
    
    elapsed = time.time() - start_time
    
    if dry_run:
        logger.info(f"DRY RUN complete! Would update {updated_count} blocks in approximately {elapsed:.2f} seconds")
        logger.info(f"Estimated time for actual migration: {elapsed:.2f} seconds")
    else:
        logger.info(f"Migration complete! Updated {updated_count} blocks in {elapsed:.2f} seconds")
        if updated_count > 0:
            logger.info(f"Average time per block: {elapsed/updated_count*1000:.2f}ms")
        
        if errors > 0:
            logger.warning(f"Encountered {errors} errors during migration")
        
        # Verify the migration
        if verify_migration(db):
            logger.info("Migration verification passed!")
        else:
            logger.error("Migration verification failed! Check the logs above.")
            return False
    
    return errors == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate blocks to include cumulative difficulty")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode (no changes)")
    parser.add_argument("--verify-only", action="store_true", help="Only verify existing migration")
    parser.add_argument("--db-path", default="/app/db", help="Path to RocksDB database")
    args = parser.parse_args()
    
    try:
        # Initialize the database
        set_db(args.db_path)
        
        if args.verify_only:
            db = get_db()
            if verify_migration(db, sample_size=1000):
                logger.info("Verification passed!")
                sys.exit(0)
            else:
                logger.error("Verification failed!")
                sys.exit(1)
        else:
            success = migrate_blocks(dry_run=args.dry_run)
            sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)