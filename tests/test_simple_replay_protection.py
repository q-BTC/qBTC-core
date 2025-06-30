"""
Simple test to verify replay protection is working
"""
import pytest
import time
from sync.sync import _process_block_in_chain
from unittest.mock import patch, MagicMock

def test_legacy_transaction_rejected():
    """Test that transactions without chain ID are rejected"""
    
    # Mock all dependencies
    with patch('sync.sync.get_db') as mock_db, \
         patch('sync.sync.WriteBatch'), \
         patch('sync.sync.calculate_merkle_root', return_value="00"*32), \
         patch('sync.sync.emit_database_event'), \
         patch('sync.sync.event_bus'), \
         patch('sync.sync.CHAIN_ID', 1), \
         patch('sync.sync.ADMIN_ADDRESS', "admin_addr"), \
         patch('sync.sync.verify_transaction', return_value=True):
        
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
        with pytest.raises(ValueError, match="invalid format"):
            _process_block_in_chain(block)


def test_wrong_chain_id_rejected():
    """Test that transactions with wrong chain ID are rejected"""
    
    with patch('sync.sync.get_db') as mock_db, \
         patch('sync.sync.WriteBatch'), \
         patch('sync.sync.calculate_merkle_root', return_value="00"*32), \
         patch('sync.sync.emit_database_event'), \
         patch('sync.sync.event_bus'), \
         patch('sync.sync.CHAIN_ID', 1), \
         patch('sync.sync.ADMIN_ADDRESS', "admin_addr"), \
         patch('sync.sync.verify_transaction', return_value=True):
        
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
        with pytest.raises(ValueError, match="Invalid chain ID.*expected 1, got 999"):
            _process_block_in_chain(block)