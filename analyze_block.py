#!/usr/bin/env python3
import json
import sys
import os
sys.path.append('/app')

# Set environment
os.environ['ROCKSDB_PATH'] = '/app/db'

from blockchain.chain_manager import ChainManager
from blockchain.block_height_index import BlockHeightIndex
from blockchain.difficulty import get_next_bits, MAX_TARGET_BITS
from database.database import get_db_for_read

# Use read-only access to avoid lock issues
db = get_db_for_read('/app/db')
height_index = BlockHeightIndex(db)

print("Analyzing block 19444 issue")
print("=" * 60)

# Get block 19444 from height index
block_19444 = height_index.get_block_by_height(19444)
if block_19444:
    print(f"\nBlock 19444 found in height index:")
    print(f"  Hash: {block_19444.get('hash', 'N/A')}")
    print(f"  Header bits: 0x{block_19444['header']['bits']:08x}")
    print(f"  Previous hash: {block_19444['header'].get('previous_hash', 'N/A')[:16]}...")
    print(f"  Timestamp: {block_19444['header']['timestamp']}")
    print(f"  Transactions: {len(block_19444.get('tx_ids', []))}")
    
    # Check if transaction exists
    if 'tx_ids' in block_19444:
        for txid in block_19444['tx_ids']:
            tx_key = f"tx:{txid}".encode()
            tx_data = db.get(tx_key)
            if tx_data:
                print(f"  TX {txid[:16]}... exists")
            else:
                print(f"  TX {txid[:16]}... MISSING!")
    
    # Get block 19443 to compare
    block_19443 = height_index.get_block_by_height(19443)
    if block_19443:
        print(f"\nBlock 19443:")
        print(f"  Hash: {block_19443.get('hash', 'N/A')}")
        print(f"  Header bits: 0x{block_19443['header']['bits']:08x}")
        
        # Check if 19444's previous_hash matches 19443's hash
        if block_19444['header'].get('previous_hash') == block_19443.get('hash'):
            print("  ✓ Block 19444 correctly references block 19443")
        else:
            print("  ✗ Block 19444 does NOT reference block 19443!")
            print(f"    Expected: {block_19443.get('hash')}")
            print(f"    Actual: {block_19444['header'].get('previous_hash')}")
    
    # Calculate what bits should be for 19444
    print(f"\nDifficulty calculation for block 19444:")
    print(f"  19444 % 10 = {19444 % 10} (not an adjustment block)")
    print(f"  Should inherit difficulty from block 19443")
    
    # Check what get_next_bits would return
    try:
        # Since we're at height 19443, calculate bits for the next block (19444)
        expected_bits = get_next_bits(db, 19443)
        print(f"  Expected bits: 0x{expected_bits:08x}")
        print(f"  Actual bits: 0x{block_19444['header']['bits']:08x}")
        
        if expected_bits == block_19444['header']['bits']:
            print("  ✓ Difficulty bits are correct")
        else:
            print("  ✗ Difficulty bits MISMATCH!")
            
            # Check if it's using MAX_TARGET_BITS
            if block_19444['header']['bits'] == MAX_TARGET_BITS:
                print("  ! Block is using MAX_TARGET_BITS (minimum difficulty)")
    except Exception as e:
        print(f"  Error calculating expected bits: {e}")
else:
    print("Block 19444 NOT FOUND in height index!")

# Check if there's a chain reorganization issue
print("\n" + "=" * 60)
print("Checking for potential chain issues...")

# Look for blocks with same height
blocks_at_19444 = []
for key, value in db.items():
    if key.startswith(b'block:'):
        try:
            block = json.loads(value.decode())
            if block['header']['height'] == 19444:
                blocks_at_19444.append({
                    'hash': key[6:].decode(),
                    'bits': block['header']['bits'],
                    'previous': block['header'].get('previous_hash', 'N/A')
                })
        except:
            pass

if len(blocks_at_19444) > 1:
    print(f"WARNING: Found {len(blocks_at_19444)} blocks at height 19444!")
    for b in blocks_at_19444:
        print(f"  - Hash: {b['hash'][:16]}... bits: 0x{b['bits']:08x}")
elif len(blocks_at_19444) == 1:
    print(f"Found exactly 1 block at height 19444 (good)")
else:
    print(f"No blocks found at height 19444 in block storage")