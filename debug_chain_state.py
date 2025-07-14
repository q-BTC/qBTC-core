#!/usr/bin/env python3
"""Debug script to check the current blockchain state"""
import asyncio
import json
import logging
from database.database import get_db, get_current_height
from blockchain.chain_singleton import get_chain_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    db = get_db()
    
    # Check current height
    height, tip = await get_current_height(db)
    print(f"\nCurrent height from get_current_height: {height}")
    print(f"Current tip: {tip}")
    
    # Check chain:best_tip in database
    tip_key = b"chain:best_tip"
    tip_data = db.get(tip_key)
    if tip_data:
        tip_info = json.loads(tip_data.decode())
        print(f"\nchain:best_tip in database: {tip_info}")
    else:
        print("\nNo chain:best_tip found in database")
    
    # List all blocks
    print("\nBlocks in database:")
    blocks = []
    for key, value in db.items():
        if key.startswith(b"block:"):
            block = json.loads(value.decode())
            blocks.append({
                'hash': block.get('block_hash', 'unknown')[:16] + '...',
                'height': block.get('height', -1),
                'prev_hash': block.get('previous_hash', 'unknown')[:16] + '...',
                'difficulty': block.get('cumulative_difficulty', 'unknown')
            })
    
    # Sort by height
    blocks.sort(key=lambda x: x['height'])
    
    print(f"\nTotal blocks: {len(blocks)}")
    for block in blocks:
        print(f"  Height {block['height']}: {block['hash']} <- {block['prev_hash']} (diff: {block['difficulty']})")
    
    # Check ChainManager state
    try:
        cm = await get_chain_manager()
        best_hash, best_height = await cm.get_best_chain_tip()
        print(f"\nChainManager best tip: {best_hash[:16]}... at height {best_height}")
        
        print(f"\nChain tips ({len(cm.chain_tips)}):")
        for tip in cm.chain_tips:
            if tip in cm.block_index:
                info = cm.block_index[tip]
                print(f"  {tip[:16]}... height={info['height']} difficulty={info.get('cumulative_difficulty', 'unknown')}")
    except Exception as e:
        print(f"\nError accessing ChainManager: {e}")

if __name__ == "__main__":
    asyncio.run(main())