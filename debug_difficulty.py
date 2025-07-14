#!/usr/bin/env python3
import json
import sys
sys.path.append('/app')

from database.database import get_db, set_db
from blockchain.block_height_index import BlockHeightIndex
from blockchain.difficulty import get_next_bits, MAX_TARGET_BITS, DIFFICULTY_ADJUSTMENT_INTERVAL

# Initialize database
set_db('/app/data/blockchain')
db = get_db()
height_index = BlockHeightIndex(db)

print("Debugging difficulty issue for block 19444")
print("=" * 60)

# Check if we can find blocks around 19444
for height in range(19440, 19450):
    block = height_index.get_block_by_height(height)
    if block:
        print(f"\nBlock {height}:")
        print(f"  Hash: {block.get('hash', 'N/A')}")
        print(f"  Bits: 0x{block['header']['bits']:08x}")
        print(f"  Previous: {block['header'].get('previous_hash', 'N/A')[:16]}...")
        
        # Try to calculate expected bits for next block
        if height < 19449:
            try:
                # Calculate what the next block's bits should be
                expected_next_bits = get_next_bits(db, height)
                print(f"  Expected bits for block {height + 1}: 0x{expected_next_bits:08x}")
                
                # Check if it's an adjustment block
                if (height + 1) % DIFFICULTY_ADJUSTMENT_INTERVAL == 0:
                    print(f"  Block {height + 1} is a difficulty adjustment block")
            except Exception as e:
                print(f"  Error calculating next bits: {e}")
    else:
        print(f"\nBlock {height}: NOT FOUND")

print("\n" + "=" * 60)
print("Configuration:")
print(f"DIFFICULTY_ADJUSTMENT_INTERVAL: {DIFFICULTY_ADJUSTMENT_INTERVAL}")
print(f"MAX_TARGET_BITS: 0x{MAX_TARGET_BITS:08x}")

# Check if chain tip exists
best_tip = db.get(b'chain:best_tip')
if best_tip:
    tip_data = json.loads(best_tip.decode())
    print(f"\nChain tip found: height={tip_data['height']}, hash={tip_data['hash'][:16]}...")
else:
    print("\nNo chain:best_tip found in database")

# Try alternative methods to find the chain state
print("\nScanning for highest block...")
max_height = 0
max_block = None
for key, value in db.items():
    if key.startswith(b'block:'):
        try:
            block = json.loads(value.decode())
            height = block['header']['height']
            if height > max_height:
                max_height = height
                max_block = block
        except:
            pass

if max_block:
    print(f"Highest block found: height={max_height}, bits=0x{max_block['header']['bits']:08x}")