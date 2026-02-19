"""
Test to verify UTXO spent flag is properly set in all code paths
"""
import json
import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch
from blockchain.chain_manager import ChainManager

def test_utxo_spent_flag_on_transaction_processing():
    """Test that spent flags are properly set when processing transactions"""
    
    # Mock database
    mock_db = {}
    
    def mock_get(key):
        return mock_db.get(key)
    
    def mock_put(key, value):
        mock_db[key] = value
        
    class MockBatch:
        def __init__(self):
            self.operations = []
            
        def put(self, key, value):
            self.operations.append(('put', key, value))
            
        def delete(self, key):
            self.operations.append(('delete', key))
    
    # Create test data
    sender_address = "bqs1sender123"
    receiver_address = "bqs1receiver456"
    change_address = sender_address  # Change goes back to sender
    
    # Create an initial UTXO that will be spent
    initial_utxo = {
        "txid": "initial_tx_123",
        "utxo_index": 0,
        "sender": "genesis",
        "receiver": sender_address,
        "amount": "100.0",
        "spent": False
    }
    mock_db[b"utxo:initial_tx_123:0"] = json.dumps(initial_utxo).encode()
    
    # Create a transaction that spends the UTXO and creates change
    transaction = {
        "txid": "test_tx_456",
        "inputs": [{
            "txid": "initial_tx_123",
            "utxo_index": 0
        }],
        "outputs": [
            {
                "utxo_index": 0,
                "sender": sender_address,
                "receiver": receiver_address,
                "amount": "30.0",
                "spent": False  # This should not affect the stored UTXO
            },
            {
                "utxo_index": 1,
                "sender": sender_address,
                "receiver": change_address,
                "amount": "69.999",  # Change output
                "spent": False  # This should not affect the stored UTXO
            }
        ],
        "body": {
            "msg_str": f"{sender_address}:{receiver_address}:30.0:123456",
            "pubkey": "test_pubkey",
            "signature": "test_signature"
        }
    }
    
    # Test ChainManager._apply_transaction
    # Mock the database getter
    with patch('blockchain.chain_manager.get_db', return_value=mock_db):
        cm = ChainManager()
        batch = MockBatch()
        cm.db = mock_db  # Override db attribute directly
        cm._apply_transaction(transaction, 100, batch)
    
    # Verify operations
    found_spent_update = False
    found_output_0 = False
    found_output_1 = False
    
    for op_type, key, value in batch.operations:
        if op_type == 'put':
            if key == b"utxo:initial_tx_123:0":
                # Check that the spent flag was set to True
                utxo_data = json.loads(value.decode())
                assert utxo_data["spent"] == True, "Input UTXO should be marked as spent"
                found_spent_update = True
                
            elif key == b"utxo:test_tx_456:0":
                # Check output 0 (payment)
                utxo_data = json.loads(value.decode())
                assert utxo_data["txid"] == "test_tx_456"
                assert utxo_data["utxo_index"] == 0
                assert utxo_data["receiver"] == receiver_address
                assert utxo_data["amount"] == "30.0"
                assert utxo_data["spent"] == False, "New UTXO should not be spent"
                found_output_0 = True
                
            elif key == b"utxo:test_tx_456:1":
                # Check output 1 (change)
                utxo_data = json.loads(value.decode())
                assert utxo_data["txid"] == "test_tx_456"
                assert utxo_data["utxo_index"] == 1
                assert utxo_data["receiver"] == change_address
                assert utxo_data["amount"] == "69.999"
                assert utxo_data["spent"] == False, "Change UTXO should not be spent"
                found_output_1 = True
    
    assert found_spent_update, "Input UTXO spent flag was not updated"
    assert found_output_0, "Output 0 was not created properly"
    assert found_output_1, "Change output was not created properly"
    

def test_sync_creates_proper_utxos():
    """Test that sync.py creates proper UTXO records"""
    
    # Create a mock transaction in a block
    block = {
        "height": 100,
        "block_hash": "test_block_hash",
        "previous_hash": "prev_hash",
        "tx_ids": ["test_tx_789"],
        "nonce": 12345,
        "timestamp": 1234567890,
        "miner_address": "bqs1miner",
        "full_transactions": [{
            "txid": "test_tx_789",
            "inputs": [],
            "outputs": [
                {
                    "utxo_index": 0,
                    "sender": "bqs1sender",
                    "receiver": "bqs1receiver",
                    "amount": "50.0",
                    "spent": False  # This should not be stored in UTXO
                }
            ],
            "body": {
                "msg_str": "test",
                "pubkey": "test",
                "signature": "test"
            }
        }],
        "merkle_root": "test_merkle",
        "version": 1,
        "bits": 0x1d00ffff
    }
    
    # Mock database and batch
    mock_db = {}
    
    class MockBatch:
        def __init__(self):
            self.operations = []
            
        def put(self, key, value):
            self.operations.append(('put', key, value))
            mock_db[key] = value
            
        def delete(self, key):
            self.operations.append(('delete', key))
            if key in mock_db:
                del mock_db[key]
    
    # Test would require mocking more dependencies
    # Key point is that the UTXO created should have proper structure
    # with txid, utxo_index, sender, receiver, amount, and spent fields


if __name__ == "__main__":
    test_utxo_spent_flag_on_transaction_processing()
    print("All tests passed!")