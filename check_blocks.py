#!/usr/bin/env python3
import json
import sys
sys.path.append('/home/qBTC-core')

from database.database import get_db, set_db
from blockchain.difficulty import get_next_bits, validate_block_bits

# Initialize database
set_db('/home/qBTC-core/data/blockchain')
db = get_db()

print("Checking blocks around height 19444...")
print("=" * 60)

# Check blocks 19442 to 19445
for height in range(19442, 19446):
    # Get block hash for this height
    height_key = f"height:{height}".encode()
    block_hash = db.get(height_key)
    
    if block_hash:
        block_hash = block_hash.decode()
        # Get block data
        block_key = f"block:{block_hash}".encode()
        block_data = db.get(block_key)
        
        if block_data:
            block = json.loads(block_data.decode())
            header = block['header']
            print(f"\nBlock {height}:")
            print(f"  Hash: {block_hash}")
            print(f"  Bits: 0x{header['bits']:08x}")
            print(f"  Timestamp: {header['timestamp']}")
            print(f"  Previous: {header['previous_hash']}")
            
            # Calculate what bits should be for the next block
            if height < 19445:
                try:
                    expected_bits = get_next_bits(db, block_hash, height + 1)
                    print(f"  Expected bits for block {height + 1}: 0x{expected_bits:08x}")
                except Exception as e:
                    print(f"  Error calculating next bits: {e}")
        else:
            print(f"\nBlock {height}: Hash found but no block data")
    else:
        print(f"\nBlock {height}: NOT FOUND in height index")

print("\n" + "=" * 60)
print("Difficulty adjustment info:")
print(f"Block 19444 % 10 = {19444 % 10} (not an adjustment block)")
print(f"Block 19440 % 10 = {19440 % 10} (is an adjustment block)")