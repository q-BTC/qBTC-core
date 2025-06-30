"""
Test cases for critical security fixes:
1. Coinbase validation
2. Double-spending prevention within blocks
3. Exact output amount validation
"""
import pytest
import json
import time
import asyncio
from decimal import Decimal
from unittest.mock import MagicMock, patch, AsyncMock
from sync.sync import _process_block_in_chain
from rocksdict import WriteBatch

class DummyWriteBatch:
    """Mimics RocksDB WriteBatch: just collects .put() calls."""
    def __init__(self):
        self.ops = []
        self.deletes = []

    def put(self, key, val):
        self.ops.append((key, val))
    
    def delete(self, key):
        self.deletes.append(key)


class FakeDB(dict):
    """dict with a .write(batch) method that commits DummyWriteBatch ops."""
    def write(self, batch):
        for k, v in batch.ops:
            self[k] = v
        for k in batch.deletes:
            self.pop(k, None)
    
    def get(self, key):
        return super().get(key, None)
    
    def items(self):
        return super().items()


class TestCoinbaseValidation:
    """Test Fix 1: Validate coinbase amounts against block subsidy + fees"""
    
    @patch('sync.sync.get_db')
    @patch('sync.sync.WriteBatch', DummyWriteBatch)
    @patch('sync.sync.calculate_merkle_root', return_value="00"*32)
    @patch('sync.sync.emit_database_event')
    @patch('sync.sync.event_bus')
    def test_coinbase_exceeds_allowed_amount(self, mock_event_bus, mock_emit, mock_merkle, mock_get_db):
        mock_event_bus.emit = AsyncMock()
        """Test that blocks with excessive coinbase rewards are rejected"""
        db = FakeDB()
        mock_get_db.return_value = db
        
        # Create a transaction that pays 10 qBTC fee
        # Store UTXO that will be spent
        utxo_key = b"utxo:prev_tx:0"
        utxo_data = {
            "txid": "prev_tx",
            "utxo_index": 0,
            "sender": "alice",
            "receiver": "alice",
            "amount": "100",
            "spent": False
        }
        db[utxo_key] = json.dumps(utxo_data).encode()
        
        # Create block with coinbase claiming 50 qBTC (when only fees are allowed)
        block = {
            "height": 100,
            "block_hash": "test_block_hash",
            "previous_hash": "prev_hash",
            "tx_ids": ["coinbase_100", "tx1"],
            "nonce": 12345,
            "timestamp": int(time.time()),
            "miner_address": "miner_addr",
            "merkle_root": "00"*32,
            "version": 1,
            "bits": 0x1f00ffff,
            "full_transactions": [
                {
                    # Coinbase transaction claiming way too much (60 billion satoshis = 600 BTC)
                    # At height 100, subsidy is 50 BTC = 5 billion satoshis, fees are 10
                    # So max allowed is 5,000,000,010 but we claim 6,000,000,000
                    "version": 1,
                    "inputs": [{"coinbase": "00"*32}],
                    "outputs": [{"value": "6000000000", "receiver": "miner_addr"}]
                },
                {
                    # Regular transaction paying fee
                    "txid": "tx1",
                    "inputs": [{"txid": "prev_tx", "utxo_index": 0}],
                    "outputs": [{"receiver": "bob", "amount": "90", "utxo_index": 0}],
                    "body": {
                        "msg_str": f"alice:bob:90:{int(time.time()*1000)}:1",  # Added chain ID
                        "signature": "dummy_sig",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                }
            ]
        }
        
        # Mock signature verification and chain ID
        with patch('sync.sync.verify_transaction', return_value=True), \
             patch('sync.sync.CHAIN_ID', 1):
            # Should raise error for excessive coinbase
            with pytest.raises(ValueError, match="Invalid coinbase amount"):
                _process_block_in_chain(block)
    
    @patch('sync.sync.get_db')
    @patch('sync.sync.WriteBatch', DummyWriteBatch)
    @patch('sync.sync.calculate_merkle_root', return_value="00"*32)
    @patch('sync.sync.emit_database_event')
    @patch('sync.sync.event_bus.emit')
    def test_coinbase_valid_amount(self, mock_event_emit, mock_emit, mock_merkle, mock_get_db):
        """Test that blocks with correct coinbase amounts are accepted"""
        db = FakeDB()
        mock_get_db.return_value = db
        
        # Store UTXO that will be spent
        utxo_key = b"utxo:prev_tx:0"
        utxo_data = {
            "txid": "prev_tx",
            "utxo_index": 0,
            "sender": "alice",
            "receiver": "alice",
            "amount": "100",
            "spent": False
        }
        db[utxo_key] = json.dumps(utxo_data).encode()
        
        # Create block with coinbase claiming exactly the fee amount (0.09 qBTC)
        block = {
            "height": 100,
            "block_hash": "test_block_hash",
            "previous_hash": "prev_hash",
            "tx_ids": ["coinbase_100", "tx1"],
            "nonce": 12345,
            "timestamp": int(time.time()),
            "miner_address": "miner_addr",
            "merkle_root": "00"*32,
            "version": 1,
            "bits": 0x1f00ffff,
            "full_transactions": [
                {
                    # Coinbase transaction claiming only fees
                    "version": 1,
                    "inputs": [{"coinbase": "00"*32}],
                    "outputs": [{"value": "0.09", "receiver": "miner_addr"}]
                },
                {
                    # Regular transaction: 100 -> 90 to bob + 9.91 change (fee = 0.09)
                    "txid": "tx1",
                    "inputs": [{"txid": "prev_tx", "utxo_index": 0}],
                    "outputs": [
                        {"receiver": "bob", "amount": "90", "utxo_index": 0},
                        {"receiver": "alice", "amount": "9.91", "utxo_index": 1}
                    ],
                    "body": {
                        "msg_str": f"alice:bob:90:{int(time.time()*1000)}:1",  # Added chain ID
                        "signature": "dummy_sig",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                }
            ]
        }
        
        # Mock signature verification and chain ID
        with patch('sync.sync.verify_transaction', return_value=True), \
             patch('sync.sync.CHAIN_ID', 1):
            # Should not raise error
            _process_block_in_chain(block)


class TestDoubleSpendingPrevention:
    """Test Fix 2: Prevent double-spending within a single block"""
    
    @patch('sync.sync.get_db')
    @patch('sync.sync.WriteBatch', DummyWriteBatch)
    @patch('sync.sync.calculate_merkle_root', return_value="00"*32)
    @patch('sync.sync.emit_database_event')
    @patch('sync.sync.event_bus')
    def test_double_spend_in_block_rejected(self, mock_event_bus, mock_emit, mock_merkle, mock_get_db):
        mock_event_bus.emit = AsyncMock()
        """Test that blocks with double-spends are rejected"""
        db = FakeDB()
        mock_get_db.return_value = db
        
        # Store UTXO that will be double-spent
        utxo_key = b"utxo:prev_tx:0"
        utxo_data = {
            "txid": "prev_tx",
            "utxo_index": 0,
            "sender": "alice",
            "receiver": "alice",
            "amount": "100.1",  # Include fee amount
            "spent": False
        }
        db[utxo_key] = json.dumps(utxo_data).encode()
        
        # Create block with two transactions spending the same UTXO
        block = {
            "height": 100,
            "block_hash": "test_block_hash",
            "previous_hash": "prev_hash",
            "tx_ids": ["tx1", "tx2"],
            "nonce": 12345,
            "timestamp": int(time.time()),
            "miner_address": "miner_addr",
            "merkle_root": "00"*32,
            "version": 1,
            "bits": 0x1f00ffff,
            "full_transactions": [
                {
                    # First spend of UTXO
                    "txid": "tx1",
                    "inputs": [{"txid": "prev_tx", "utxo_index": 0}],
                    "outputs": [{"receiver": "bob", "amount": "100", "utxo_index": 0}],
                    "body": {
                        "msg_str": f"alice:bob:100:{int(time.time()*1000)}:1",  # Added chain ID
                        "signature": "dummy_sig1",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                },
                {
                    # Second spend of same UTXO (double-spend)
                    "txid": "tx2",
                    "inputs": [{"txid": "prev_tx", "utxo_index": 0}],
                    "outputs": [{"receiver": "charlie", "amount": "100", "utxo_index": 0}],
                    "body": {
                        "msg_str": f"alice:charlie:100:{int(time.time()*1000)}:1",  # Added chain ID
                        "signature": "dummy_sig2",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                }
            ]
        }
        
        # Mock signature verification and chain ID
        with patch('sync.sync.verify_transaction', return_value=True), \
             patch('sync.sync.CHAIN_ID', 1):
            # Should raise error for double-spend
            with pytest.raises(ValueError, match="Double spend detected"):
                _process_block_in_chain(block)
    
    @patch('sync.sync.get_db')
    @patch('sync.sync.WriteBatch', DummyWriteBatch)
    @patch('sync.sync.calculate_merkle_root', return_value="00"*32)
    @patch('sync.sync.emit_database_event')
    @patch('sync.sync.event_bus')
    def test_valid_multiple_spends(self, mock_event_bus, mock_emit, mock_merkle, mock_get_db):
        mock_event_bus.emit = AsyncMock()
        """Test that blocks with valid multiple transactions are accepted"""
        db = FakeDB()
        mock_get_db.return_value = db
        
        # Store two different UTXOs
        utxo1_key = b"utxo:prev_tx1:0"
        utxo1_data = {
            "txid": "prev_tx1",
            "utxo_index": 0,
            "sender": "alice",
            "receiver": "alice",
            "amount": "100.1",  # Include fee
            "spent": False
        }
        db[utxo1_key] = json.dumps(utxo1_data).encode()
        
        utxo2_key = b"utxo:prev_tx2:0"
        utxo2_data = {
            "txid": "prev_tx2",
            "utxo_index": 0,
            "sender": "alice",
            "receiver": "alice",
            "amount": "50.05",  # Include fee
            "spent": False
        }
        db[utxo2_key] = json.dumps(utxo2_data).encode()
        
        # Create block with two valid transactions
        block = {
            "height": 100,
            "block_hash": "test_block_hash",
            "previous_hash": "prev_hash",
            "tx_ids": ["tx1", "tx2"],
            "nonce": 12345,
            "timestamp": int(time.time()),
            "miner_address": "miner_addr",
            "merkle_root": "00"*32,
            "version": 1,
            "bits": 0x1f00ffff,
            "full_transactions": [
                {
                    # First transaction spending UTXO1
                    "txid": "tx1",
                    "inputs": [{"txid": "prev_tx1", "utxo_index": 0}],
                    "outputs": [{"receiver": "bob", "amount": "100", "utxo_index": 0}],
                    "body": {
                        "msg_str": f"alice:bob:100:{int(time.time()*1000)}:1",  # Added chain ID
                        "signature": "dummy_sig1",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                },
                {
                    # Second transaction spending UTXO2 (different UTXO)
                    "txid": "tx2",
                    "inputs": [{"txid": "prev_tx2", "utxo_index": 0}],
                    "outputs": [{"receiver": "charlie", "amount": "50", "utxo_index": 0}],
                    "body": {
                        "msg_str": f"alice:charlie:50:{int(time.time()*1000)}:1",  # Added chain ID
                        "signature": "dummy_sig2",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                }
            ]
        }
        
        # Mock signature verification and chain ID
        with patch('sync.sync.verify_transaction', return_value=True), \
             patch('sync.sync.CHAIN_ID', 1):
            # Should not raise error
            _process_block_in_chain(block)


class TestExactOutputValidation:
    """Test Fix 3: Ensure exact authorized amounts are sent to recipients"""
    
    @patch('sync.sync.get_db')
    @patch('sync.sync.WriteBatch', DummyWriteBatch)
    @patch('sync.sync.calculate_merkle_root', return_value="00"*32)
    @patch('sync.sync.ADMIN_ADDRESS', "admin_addr")
    @patch('sync.sync.emit_database_event')
    @patch('sync.sync.event_bus')
    def test_insufficient_payment_rejected(self, mock_event_bus, mock_emit, mock_merkle, mock_get_db):
        mock_event_bus.emit = AsyncMock()
        """Test that transactions sending less than authorized are rejected"""
        db = FakeDB()
        mock_get_db.return_value = db
        
        # Store UTXO
        utxo_key = b"utxo:prev_tx:0"
        utxo_data = {
            "txid": "prev_tx",
            "utxo_index": 0,
            "sender": "alice",
            "receiver": "alice",
            "amount": "100.1",  # Include fee
            "spent": False
        }
        db[utxo_key] = json.dumps(utxo_data).encode()
        
        # Create transaction that signs for 100 but only sends 90
        block = {
            "height": 100,
            "block_hash": "test_block_hash",
            "previous_hash": "prev_hash",
            "tx_ids": ["tx1"],
            "nonce": 12345,
            "timestamp": int(time.time()),
            "miner_address": "miner_addr",
            "merkle_root": "00"*32,
            "version": 1,
            "bits": 0x1f00ffff,
            "full_transactions": [
                {
                    "txid": "tx1",
                    "inputs": [{"txid": "prev_tx", "utxo_index": 0}],
                    "outputs": [
                        {"receiver": "bob", "amount": "90", "utxo_index": 0},  # Less than authorized
                        {"receiver": "alice", "amount": "9.9", "utxo_index": 1}  # Extra change
                    ],
                    "body": {
                        "msg_str": f"alice:bob:100:{int(time.time()*1000)}:1",  # Added chain ID  # Authorized 100
                        "signature": "dummy_sig",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                }
            ]
        }
        
        # Mock signature verification and chain ID
        with patch('sync.sync.verify_transaction', return_value=True), \
             patch('sync.sync.CHAIN_ID', 1):
            # Should raise error for incorrect amount
            with pytest.raises(ValueError, match="authorized amount 100 != amount sent to recipient 90"):
                _process_block_in_chain(block)
    
    @patch('sync.sync.get_db')
    @patch('sync.sync.WriteBatch', DummyWriteBatch)
    @patch('sync.sync.calculate_merkle_root', return_value="00"*32)
    @patch('sync.sync.ADMIN_ADDRESS', "admin_addr")
    @patch('sync.sync.emit_database_event')
    @patch('sync.sync.event_bus.emit')
    def test_exact_payment_accepted(self, mock_event_emit, mock_emit, mock_merkle, mock_get_db):
        """Test that transactions sending exact authorized amounts are accepted"""
        db = FakeDB()
        mock_get_db.return_value = db
        
        # Store UTXO
        utxo_key = b"utxo:prev_tx:0"
        utxo_data = {
            "txid": "prev_tx",
            "utxo_index": 0,
            "sender": "alice",
            "receiver": "alice",
            "amount": "100.1",
            "spent": False
        }
        db[utxo_key] = json.dumps(utxo_data).encode()
        
        # Create transaction that sends exact authorized amount
        block = {
            "height": 100,
            "block_hash": "test_block_hash",
            "previous_hash": "prev_hash",
            "tx_ids": ["tx1"],
            "nonce": 12345,
            "timestamp": int(time.time()),
            "miner_address": "miner_addr",
            "merkle_root": "00"*32,
            "version": 1,
            "bits": 0x1f00ffff,
            "full_transactions": [
                {
                    "txid": "tx1",
                    "inputs": [{"txid": "prev_tx", "utxo_index": 0}],
                    "outputs": [
                        {"receiver": "bob", "amount": "100", "utxo_index": 0},  # Exact amount
                        {"receiver": "admin_addr", "amount": "0.1", "utxo_index": 1}  # Fee
                    ],
                    "body": {
                        "msg_str": f"alice:bob:100:{int(time.time()*1000)}:1",  # Added chain ID  # Authorized 100
                        "signature": "dummy_sig",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                }
            ]
        }
        
        # Mock signature verification and chain ID
        with patch('sync.sync.verify_transaction', return_value=True), \
             patch('sync.sync.CHAIN_ID', 1):
            # Should not raise error
            _process_block_in_chain(block)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])