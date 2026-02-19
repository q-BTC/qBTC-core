"""
Simple test to verify replay protection is working
"""
import pytest
import time
import asyncio
from blockchain.chain_singleton import get_chain_manager
from unittest.mock import patch, MagicMock

def test_legacy_transaction_rejected():
    """Test that transactions without chain ID are rejected"""
    
    # Mock all dependencies
    with patch('blockchain.chain_manager.get_db') as mock_db, \
         patch('blockchain.blockchain.calculate_merkle_root', return_value="00"*32), \
         patch('blockchain.transaction_validator.verify_transaction', return_value=True), \
         patch('config.config.CHAIN_ID', 1), \
         patch('config.config.ADMIN_ADDRESS', "admin_addr"):
        
        # Setup mock database
        mock_db.return_value = MagicMock()
        
        # Create a legacy transaction (old format without chain ID)
        block = {
            "height": 100,
            "block_hash": "test_hash",
            "previous_hash": "prev_hash",
            "tx_ids": ["tx1"],
            "nonce": 12345,
            "timestamp": 1234567890,
            "miner_address": "miner_addr",
            "merkle_root": "00"*32,
            "version": 1,
            "bits": 0x1f00ffff,
            "full_transactions": [{
                "txid": "tx1",
                "inputs": [{"txid": "prev_tx", "utxo_index": 0}],
                "outputs": [{"receiver": "bob", "amount": "100", "utxo_index": 0}],
                "body": {
                    "msg_str": f"alice:bob:100:{int(time.time()*1000)}",  # Old format
                    "signature": "dummy_sig",
                    "pubkey": "dummy_pubkey",
                    "transaction_data": ""
                }
            }]
        }
        
        # Should raise error about invalid format (missing chain ID)
        async def test_legacy():
            cm = await get_chain_manager()
            success, error = await cm.add_block(block)
            assert not success
            assert "invalid format" in error.lower() or "chain" in error.lower()
        
        asyncio.run(test_legacy())


def test_wrong_chain_id_rejected():
    """Test that transactions with wrong chain ID are rejected"""
    
    with patch('blockchain.chain_manager.get_db') as mock_db, \
         patch('blockchain.blockchain.calculate_merkle_root', return_value="00"*32), \
         patch('blockchain.transaction_validator.verify_transaction', return_value=True), \
         patch('config.config.CHAIN_ID', 1), \
         patch('config.config.ADMIN_ADDRESS', "admin_addr"):
        
        # Setup mock database
        mock_db.return_value = MagicMock()
        
        # Create transaction with wrong chain ID
        block = {
            "height": 100,
            "block_hash": "test_hash",
            "previous_hash": "prev_hash",
            "tx_ids": ["tx1"],
            "nonce": 12345,
            "timestamp": 1234567890,
            "miner_address": "miner_addr",
            "merkle_root": "00"*32,
            "version": 1,
            "bits": 0x1f00ffff,
            "full_transactions": [{
                "txid": "tx1",
                "inputs": [{"txid": "prev_tx", "utxo_index": 0}],
                "outputs": [{"receiver": "bob", "amount": "100", "utxo_index": 0}],
                "body": {
                    "msg_str": f"alice:bob:100:{int(time.time()*1000)}:999",  # Wrong chain ID
                    "signature": "dummy_sig",
                    "pubkey": "dummy_pubkey",
                    "transaction_data": ""
                }
            }]
        }
        
        # Should raise error about wrong chain ID
        async def test_wrong_chain():
            cm = await get_chain_manager()
            success, error = await cm.add_block(block)
            assert not success
            assert "chain" in error.lower() or "999" in error
        
        asyncio.run(test_wrong_chain())