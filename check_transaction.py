#!/usr/bin/env python3
"""Check transaction status for qBTC transactions"""

import sys
import json
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.database import get_db
from mempool.mempool_manager import MempoolManager

def check_transaction_by_address(address, amount=None):
    """Check transactions for a given address"""
    db = get_db()
    mempool = MempoolManager(db)
    
    print(f"\nChecking transactions for address: {address}")
    if amount:
        print(f"Looking for amount: {amount} qBTC")
    print("-" * 60)
    
    # Check mempool
    print("\n1. Checking mempool (pending transactions)...")
    pending_found = False
    mempool_txs = mempool.get_all_transactions()
    
    for tx in mempool_txs:
        # Check if address is in outputs
        for output in tx.get('outputs', []):
            if output.get('address') == address:
                if amount is None or output.get('amount') == amount:
                    pending_found = True
                    print(f"\nFound pending transaction!")
                    print(f"  TXID: {tx.get('txid', 'N/A')}")
                    print(f"  Amount: {output.get('amount')} qBTC")
                    print(f"  Status: PENDING (in mempool)")
                    print(f"  Timestamp: {tx.get('timestamp', 'N/A')}")
    
    if not pending_found:
        print("  No pending transactions found in mempool")
    
    # Check confirmed transactions
    print("\n2. Checking confirmed transactions...")
    confirmed_found = False
    
    # Get all transactions for the address
    cursor = db.iterate_prefix(b'tx:')
    tx_count = 0
    
    for key, value in cursor:
        try:
            tx_data = json.loads(value.decode('utf-8'))
            tx_count += 1
            
            # Check outputs
            for output in tx_data.get('outputs', []):
                if output.get('address') == address:
                    if amount is None or output.get('amount') == amount:
                        confirmed_found = True
                        txid = key.decode('utf-8').replace('tx:', '')
                        print(f"\nFound confirmed transaction!")
                        print(f"  TXID: {txid}")
                        print(f"  Amount: {output.get('amount')} qBTC")
                        print(f"  Status: CONFIRMED (mined)")
                        print(f"  Timestamp: {tx_data.get('timestamp', 'N/A')}")
                        
                        # Check which block it's in
                        block_cursor = db.iterate_prefix(b'block:')
                        for block_key, block_value in block_cursor:
                            try:
                                block_data = json.loads(block_value.decode('utf-8'))
                                if txid in block_data.get('transactions', []):
                                    print(f"  Block Height: {block_data.get('index', 'N/A')}")
                                    print(f"  Block Hash: {block_data.get('hash', 'N/A')}")
                                    break
                            except:
                                continue
                        
        except Exception as e:
            continue
    
    print(f"\n  Checked {tx_count} total confirmed transactions")
    
    if not confirmed_found:
        print("  No matching confirmed transactions found")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY:")
    if pending_found:
        print("✓ Transaction found in mempool - waiting to be mined")
        print("  Your transaction should be included in the next block")
    elif confirmed_found:
        print("✓ Transaction found and confirmed on blockchain")
    else:
        print("✗ Transaction not found")
        print("\nPossible reasons:")
        print("  1. Transaction was not successfully submitted")
        print("  2. Transaction was rejected (invalid signature, insufficient balance, etc.)")
        print("  3. Transaction expired before being mined")
        print("  4. Wrong address or amount specified")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check_transaction.py <address> [amount]")
        print("Example: python check_transaction.py bqs189pxUzJr1mMuRe8DfViksPwYHKNSJqFwE 100")
        sys.exit(1)
    
    address = sys.argv[1]
    amount = float(sys.argv[2]) if len(sys.argv) > 2 else None
    
    try:
        check_transaction_by_address(address, amount)
    except Exception as e:
        print(f"Error: {str(e)}")
        print("\nMake sure the qBTC node is running and database is accessible")