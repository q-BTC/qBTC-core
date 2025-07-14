#!/usr/bin/env python3
"""
Reset blockchain by removing all blocks and chain state
"""
import rocksdict
import os
import sys

def reset_blockchain():
    db_path = os.environ.get('ROCKSDB_PATH', './ledger.rocksdb')
    print(f"Resetting blockchain database at: {db_path}")
    
    try:
        db = rocksdict.Rdict(db_path)
        
        # Count items to be deleted
        block_count = 0
        tx_count = 0
        utxo_count = 0
        chain_count = 0
        
        keys_to_delete = []
        
        # Collect all keys that need to be deleted
        for key, _ in db.items():
            if key.startswith(b"block:"):
                block_count += 1
                keys_to_delete.append(key)
            elif key.startswith(b"tx:"):
                tx_count += 1
                keys_to_delete.append(key)
            elif key.startswith(b"utxo:"):
                utxo_count += 1
                keys_to_delete.append(key)
            elif key.startswith(b"chain:"):
                chain_count += 1
                keys_to_delete.append(key)
            elif key == b"validators_list":
                # Keep validator list
                continue
            elif key.startswith(b"height:"):
                keys_to_delete.append(key)
        
        print(f"Found {block_count} blocks, {tx_count} transactions, {utxo_count} UTXOs, {chain_count} chain entries")
        
        if not keys_to_delete:
            print("No blockchain data found to reset")
            return
        
        # Delete all blockchain data
        print(f"Deleting {len(keys_to_delete)} keys...")
        for key in keys_to_delete:
            del db[key]
        
        db.close()
        print("Blockchain reset complete!")
        print("The genesis block will be recreated on next startup.")
        
    except Exception as e:
        print(f"Error resetting blockchain: {e}")
        sys.exit(1)

if __name__ == "__main__":
    response = input("Are you sure you want to reset the blockchain? This will delete all blocks! (yes/no): ")
    if response.lower() == "yes":
        reset_blockchain()
    else:
        print("Reset cancelled.")