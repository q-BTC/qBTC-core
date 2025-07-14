#!/usr/bin/env python3
"""
Rollback blockchain to a specific height
This script removes all blocks after the specified height
"""
import json
import sys
import os
import argparse
from typing import Set

# Add the project root to Python path
sys.path.append('/app')
os.environ['ROCKSDB_PATH'] = '/app/db'

from database.database import set_db, get_db
from rocksdict import WriteBatch
from blockchain.block_height_index import BlockHeightIndex

def rollback_to_height(target_height: int, dry_run: bool = True):
    """Rollback blockchain to specified height"""
    
    # Initialize database
    set_db('/app/db')
    db = get_db()
    height_index = BlockHeightIndex(db)
    
    # Get current chain tip
    tip_key = b"chain:best_tip"
    tip_data = db.get(tip_key)
    if not tip_data:
        print("ERROR: No chain tip found")
        return False
    
    current_tip = json.loads(tip_data.decode())
    current_height = current_tip["height"]
    
    if current_height <= target_height:
        print(f"Current height {current_height} is already at or below target {target_height}")
        return True
    
    print(f"Current chain height: {current_height}")
    print(f"Rolling back to height: {target_height}")
    print(f"This will remove {current_height - target_height} blocks")
    
    if dry_run:
        print("\nDRY RUN MODE - No changes will be made")
    else:
        response = input("\nAre you sure you want to proceed? (yes/no): ")
        if response.lower() != 'yes':
            print("Rollback cancelled")
            return False
    
    # Collect all blocks to be removed
    blocks_to_remove = []
    utxos_to_remove = set()
    
    for height in range(current_height, target_height, -1):
        block = height_index.get_block_by_height(height)
        if block:
            block_hash = block.get('hash')
            if block_hash:
                blocks_to_remove.append((height, block_hash))
                
                # Collect UTXOs created by this block
                tx_ids = block.get('tx_ids', [])
                for tx_id in tx_ids:
                    # Find all UTXOs from this transaction
                    # Format: utxo:txid:index
                    prefix = f"utxo:{tx_id}:".encode()
                    for key in db.keys():
                        if key.startswith(prefix):
                            utxos_to_remove.add(key)
                
                print(f"  Block {height}: {block_hash} (has {len(tx_ids)} transactions)")
    
    # Find the new chain tip
    new_tip_block = height_index.get_block_by_height(target_height)
    if not new_tip_block:
        print(f"ERROR: Could not find block at height {target_height}")
        return False
    
    new_tip_hash = new_tip_block.get('hash')
    print(f"\nNew chain tip will be: {new_tip_hash} at height {target_height}")
    
    if not dry_run:
        # Create batch for atomic operation
        batch = WriteBatch()
        
        # Remove blocks and their references
        for height, block_hash in blocks_to_remove:
            # Remove block
            block_key = f"block:{block_hash}".encode()
            batch.delete(block_key)
            
            # Remove height index
            height_key = f"height:{height}".encode()
            batch.delete(height_key)
            
            # Remove transactions
            block_data = db.get(block_key)
            if block_data:
                block = json.loads(block_data.decode())
                for tx_id in block.get('tx_ids', []):
                    tx_key = f"tx:{tx_id}".encode()
                    batch.delete(tx_key)
        
        # Remove UTXOs
        for utxo_key in utxos_to_remove:
            batch.delete(utxo_key)
        
        # Update chain tip
        new_tip_data = {
            "hash": new_tip_hash,
            "height": target_height
        }
        batch.put(tip_key, json.dumps(new_tip_data).encode())
        
        # Write all changes atomically
        print("\nApplying changes...")
        db.write(batch)
        print("Rollback completed successfully")
        
        # Verify new state
        new_tip = db.get(tip_key)
        if new_tip:
            verified_tip = json.loads(new_tip.decode())
            print(f"\nVerified new chain tip: height={verified_tip['height']}, hash={verified_tip['hash']}")
    
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Rollback blockchain to specific height')
    parser.add_argument('height', type=int, help='Target height to rollback to')
    parser.add_argument('--confirm', action='store_true', help='Actually perform the rollback (default is dry run)')
    
    args = parser.parse_args()
    
    success = rollback_to_height(args.height, dry_run=not args.confirm)
    sys.exit(0 if success else 1)