#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rocksdict import Rdict
import json

db = Rdict('./db')

# The transaction we're looking for
txid = '1a5b4908e5dfbf5847a48ebe7620403bc29b3fb1aa9932a5b261eaac73628ce8'

print(f"Diagnosing transaction: {txid}")
print("=" * 80)

# 1. Check if transaction is in blockchain
tx_key = f'tx:{txid}'.encode()
tx_data = db.get(tx_key)
if tx_data:
    print(f"✓ Transaction IS IN BLOCKCHAIN (key: tx:{txid})")
    
    # Find which block contains it
    current_height = int(db.get(b'chain:height', 0))
    print(f"  Current blockchain height: {current_height}")
    
    found_in_block = None
    for h in range(max(0, current_height - 100), current_height + 1):
        block_key = f'block:{h}'.encode()
        block_data = db.get(block_key)
        if block_data:
            block = json.loads(block_data.decode() if isinstance(block_data, bytes) else block_data)
            if 'tx_ids' in block and txid in block['tx_ids']:
                found_in_block = h
                print(f"  Found in block {h}")
                print(f"  Block hash: {block['block_hash']}")
                print(f"  Block has {len(block['tx_ids'])} transactions")
                print(f"  Transaction index: {block['tx_ids'].index(txid)}")
                break
    
    if not found_in_block:
        print("  WARNING: Transaction in DB but not found in any recent block!")
else:
    print(f"✗ Transaction NOT in blockchain")

print()

# 2. Check mempool persistence in database
mempool_key = b'mempool'
mempool_data = db.get(mempool_key)
if mempool_data:
    mempool = json.loads(mempool_data.decode() if isinstance(mempool_data, bytes) else mempool_data)
    if txid in mempool:
        print(f"✗ PROBLEM: Transaction IS IN PERSISTED MEMPOOL!")
        print(f"  This means it wasn't removed when the block was processed")
        print(f"  Total persisted mempool size: {len(mempool)} transactions")
    else:
        print(f"✓ Transaction NOT in persisted mempool (good)")
else:
    print("✓ No persisted mempool found")

print()

# 3. Check mempool manager (if running)
try:
    from mempool.mempool_manager import mempool_manager
    print(f"In-memory mempool size: {mempool_manager.size()} transactions")
    if txid in mempool_manager.transactions:
        print(f"✗ PROBLEM: Transaction IS IN MEMORY MEMPOOL!")
        tx = mempool_manager.transactions[txid]
        print(f"  Transaction details: {json.dumps(tx, indent=2)[:200]}...")
    else:
        print(f"✓ Transaction NOT in memory mempool")
except:
    print("Cannot check in-memory mempool (node not running in this process)")

print()

# 4. Check best chain tip
best_tip = db.get(b'chain:best_tip')
if best_tip:
    print(f"Best chain tip: {best_tip.decode() if isinstance(best_tip, bytes) else best_tip}")

print()
print("DIAGNOSIS:")
if tx_data and found_in_block and txid in mempool:
    print("The transaction was mined in a block but NOT removed from the persisted mempool.")
    print("This indicates the mempool removal logic didn't execute when the block was processed.")