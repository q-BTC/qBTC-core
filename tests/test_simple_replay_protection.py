"""
Simple test to verify replay protection is working.

Tests the TransactionValidator directly to verify that:
1. Legacy transactions (without chain ID) are rejected
2. Transactions with wrong chain ID are rejected
"""
import pytest
import json
import time
from unittest.mock import patch
from blockchain.transaction_validator import TransactionValidator


class FakeDB(dict):
    """Dict-based stub with rocksdict-compatible API."""
    def put(self, key, value):
        self[key] = value

    def write(self, batch):
        pass

    def delete(self, key):
        self.pop(key, None)

    def get(self, key):
        return super().get(key, None)


@patch('config.config.CHAIN_ID', 1)
def test_legacy_transaction_rejected():
    """Test that transactions without chain ID are rejected"""
    db = FakeDB()

    # Store the UTXO that the transaction references
    db[b"utxo:prev_tx:0"] = json.dumps({
        "txid": "prev_tx", "utxo_index": 0,
        "receiver": "alice", "amount": "100", "spent": False
    }).encode()

    validator = TransactionValidator(db=db)

    # Legacy transaction: msg_str has only 4 parts (no chain ID)
    block_data = {
        "height": 100,
        "full_transactions": [{
            "txid": "tx1",
            "inputs": [{"txid": "prev_tx", "utxo_index": 0}],
            "outputs": [{"receiver": "bob", "amount": "100", "utxo_index": 0}],
            "body": {
                "msg_str": f"alice:bob:100:{int(time.time()*1000)}",  # Old format — no chain ID
                "signature": "dummy_sig",
                "pubkey": "dummy_pubkey",
                "transaction_data": ""
            }
        }]
    }

    is_valid, error, fees = validator.validate_block_transactions(block_data)
    assert not is_valid
    assert "invalid format" in error.lower() or "chain" in error.lower()


@patch('config.config.CHAIN_ID', 1)
def test_wrong_chain_id_rejected():
    """Test that transactions with wrong chain ID are rejected"""
    db = FakeDB()

    # Store the UTXO that the transaction references
    db[b"utxo:prev_tx:0"] = json.dumps({
        "txid": "prev_tx", "utxo_index": 0,
        "receiver": "alice", "amount": "100", "spent": False
    }).encode()

    validator = TransactionValidator(db=db)

    # Transaction with wrong chain ID (999 instead of 1)
    block_data = {
        "height": 100,
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

    is_valid, error, fees = validator.validate_block_transactions(block_data)
    assert not is_valid
    assert "chain" in error.lower() or "999" in error
