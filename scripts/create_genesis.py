#!/usr/bin/env python3
"""
Create a genesis block for the validator node
"""
import json
import time
import hashlib
import rocksdict

def sha256d(data):
    """Double SHA256 hash"""
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()

def calculate_merkle_root(tx_ids):
    """Calculate merkle root from transaction IDs"""
    if not tx_ids:
        return "0" * 64
    
    # Convert to bytes
    hashes = [bytes.fromhex(txid) for txid in tx_ids]
    
    while len(hashes) > 1:
        next_level = []
        for i in range(0, len(hashes), 2):
            if i + 1 < len(hashes):
                combined = hashes[i] + hashes[i + 1]
            else:
                combined = hashes[i] + hashes[i]
            next_level.append(sha256d(combined))
        hashes = next_level
    
    return hashes[0].hex()

def main():
    # Configuration
    GENESIS_ADDRESS = "bqs1genesis00000000000000000000000000000000"
    ADMIN_ADDRESS = "bqs1HpmbeSd8nhRpq5zX5df91D3Xy8pSUovmV"
    
    # Create genesis transaction
    genesis_tx = {
        "version": 1,
        "inputs": [{
            "txid": "00" * 32,
            "utxo_index": 0,
            "signature": "",
            "pubkey": ""
        }],
        "outputs": [{
            "sender": GENESIS_ADDRESS,
            "receiver": ADMIN_ADDRESS,
            "amount": "21000000"  # 21 million coins
        }],
        "body": {
            "transaction_data": "initial_distribution",
            "msg_str": "",  # No message for genesis
            "pubkey": "",   # No pubkey for genesis
            "signature": "" # No signature for genesis
        },
        "txid": "2a827ec7858bd690949677ca8dc6302988163bc773c3494107eb090a5131a1d6"  # Same as bootstrap
    }
    
    # Create genesis block
    genesis_block = {
        "version": 1,
        "previous_hash": "00" * 32,
        "merkle_root": calculate_merkle_root([genesis_tx["txid"]]),
        "timestamp": 1730000000,  # Fixed timestamp for consistency
        "bits": 0x1d00ffff,
        "nonce": 0,
        "height": 0,
        "tx_ids": [genesis_tx["txid"]],
        "full_transactions": [genesis_tx]
    }
    
    # Calculate block hash
    block_content = f"{genesis_block['version']}{genesis_block['previous_hash']}{genesis_block['merkle_root']}{genesis_block['timestamp']}{genesis_block['bits']}{genesis_block['nonce']}"
    genesis_block["block_hash"] = sha256d(block_content.encode()).hex()
    
    print(f"Genesis block hash: {genesis_block['block_hash']}")
    print(f"Genesis transaction ID: {genesis_tx['txid']}")
    
    # Open database
    db = rocksdict.Rdict('/app/db')
    
    # Store genesis block
    block_key = f"block:{genesis_block['block_hash']}".encode()
    db[block_key] = json.dumps(genesis_block).encode()
    
    # Store genesis transaction
    tx_key = f"tx:{genesis_tx['txid']}".encode()
    db[tx_key] = json.dumps(genesis_tx).encode()
    
    # Legacy coinbase storage removed - transaction already stored with its txid above
    
    # Create UTXO
    utxo_key = f"utxo:{genesis_tx['txid']}:0".encode()
    utxo_data = {
        "txid": genesis_tx["txid"],
        "sender": GENESIS_ADDRESS,
        "receiver": ADMIN_ADDRESS,
        "amount": "21000000",
        "spent": False
    }
    db[utxo_key] = json.dumps(utxo_data).encode()
    
    db.close()
    print("Genesis block created successfully!")

if __name__ == "__main__":
    main()