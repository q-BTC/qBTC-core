"""
Test cases for critical security fixes:
1. Coinbase validation
2. Double-spending prevention within blocks
3. Exact output amount validation

These tests exercise the TransactionValidator directly to verify
consensus-level security checks without requiring full block-level
validation (hash, PoW, difficulty, parent lookup).
"""
import pytest
import json
import time
from decimal import Decimal
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


class TestCoinbaseValidation:
    """Test Fix 1: Validate coinbase amounts against block subsidy + fees"""

    def test_coinbase_exceeds_allowed_amount(self):
        """Test that coinbase transactions with excessive rewards are rejected"""
        db = FakeDB()
        validator = TransactionValidator(db=db)

        # Coinbase claiming way too much (6 billion qBTC)
        coinbase_tx = {
            "txid": "coinbase_100",
            "inputs": [{"txid": "0" * 64, "utxo_index": 0}],
            "outputs": [{"amount": "6000000000", "receiver": "miner_addr"}]
        }

        # At height 100, subsidy is ~0.4167 qBTC, fees are 10 qBTC
        # max allowed = 10.4167, we claim 6 billion -> rejected
        is_valid, error = validator.validate_coinbase_transaction(
            coinbase_tx, 100, Decimal("10")
        )
        assert not is_valid
        assert "coinbase" in error.lower() or "exceeds" in error.lower()

    def test_coinbase_valid_amount(self):
        """Test that coinbase transactions with correct amounts are accepted"""
        db = FakeDB()
        validator = TransactionValidator(db=db)

        # Coinbase claiming a small, valid amount (0.09 qBTC)
        coinbase_tx = {
            "txid": "coinbase_100",
            "inputs": [{"txid": "0" * 64, "utxo_index": 0}],
            "outputs": [{"amount": "0.09", "receiver": "miner_addr"}]
        }

        # At height 100, subsidy is ~0.4167, fees are 0.09
        # max allowed = ~0.5067, we claim 0.09 -> accepted
        is_valid, error = validator.validate_coinbase_transaction(
            coinbase_tx, 100, Decimal("0.09")
        )
        assert is_valid
        assert error is None


class TestDoubleSpendingPrevention:
    """Test Fix 2: Prevent double-spending within a single block"""

    @patch('config.config.CHAIN_ID', 1)
    @patch('blockchain.transaction_validator.derive_qsafe_address', return_value="alice")
    @patch('blockchain.transaction_validator.verify_transaction', return_value=True)
    def test_double_spend_in_block_rejected(self, _mock_verify, _mock_derive):
        """Test that blocks with intra-block double-spends are rejected"""
        db = FakeDB()

        # Store UTXO that will be double-spent
        utxo_key = b"utxo:prev_tx:0"
        utxo_data = {
            "txid": "prev_tx",
            "utxo_index": 0,
            "receiver": "alice",
            "amount": "100.1",
            "spent": False
        }
        db[utxo_key] = json.dumps(utxo_data).encode()

        validator = TransactionValidator(db=db)

        ts = str(int(time.time() * 1000))
        block_data = {
            "height": 100,
            "full_transactions": [
                {
                    # Coinbase (skipped by validator)
                    "txid": "coinbase_100",
                    "inputs": [{"txid": "0" * 64, "utxo_index": 0}],
                    "outputs": [{"amount": "0.1", "receiver": "miner_addr"}]
                },
                {
                    # First spend of UTXO
                    "txid": "tx1",
                    "inputs": [{"txid": "prev_tx", "utxo_index": 0}],
                    "outputs": [{"receiver": "bob", "amount": "100", "utxo_index": 0}],
                    "body": {
                        "msg_str": f"alice:bob:100:{ts}:1",
                        "signature": "dummy_sig1",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                },
                {
                    # Second spend of SAME UTXO (double-spend)
                    "txid": "tx2",
                    "inputs": [{"txid": "prev_tx", "utxo_index": 0}],
                    "outputs": [{"receiver": "charlie", "amount": "100", "utxo_index": 0}],
                    "body": {
                        "msg_str": f"alice:charlie:100:{ts}:1",
                        "signature": "dummy_sig2",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                }
            ]
        }

        is_valid, error, fees = validator.validate_block_transactions(block_data)
        assert not is_valid
        assert "double spend" in error.lower() or "already spent" in error.lower()

    @patch('config.config.CHAIN_ID', 1)
    @patch('blockchain.transaction_validator.derive_qsafe_address', return_value="alice")
    @patch('blockchain.transaction_validator.verify_transaction', return_value=True)
    def test_valid_multiple_spends(self, _mock_verify, _mock_derive):
        """Test that blocks with valid multiple transactions (different UTXOs) are accepted"""
        db = FakeDB()

        # Store two different UTXOs
        db[b"utxo:prev_tx1:0"] = json.dumps({
            "txid": "prev_tx1", "utxo_index": 0,
            "receiver": "alice", "amount": "100.1", "spent": False
        }).encode()
        db[b"utxo:prev_tx2:0"] = json.dumps({
            "txid": "prev_tx2", "utxo_index": 0,
            "receiver": "alice", "amount": "50.05", "spent": False
        }).encode()

        validator = TransactionValidator(db=db)

        ts = str(int(time.time() * 1000))
        block_data = {
            "height": 100,
            "full_transactions": [
                {
                    "txid": "coinbase_100",
                    "inputs": [{"txid": "0" * 64, "utxo_index": 0}],
                    "outputs": [{"amount": "0.1", "receiver": "miner_addr"}]
                },
                {
                    # Spending UTXO1
                    "txid": "tx1",
                    "inputs": [{"txid": "prev_tx1", "utxo_index": 0}],
                    "outputs": [{"receiver": "bob", "amount": "100", "utxo_index": 0}],
                    "body": {
                        "msg_str": f"alice:bob:100:{ts}:1",
                        "signature": "dummy_sig1",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                },
                {
                    # Spending UTXO2 (different UTXO — not a double-spend)
                    "txid": "tx2",
                    "inputs": [{"txid": "prev_tx2", "utxo_index": 0}],
                    "outputs": [{"receiver": "charlie", "amount": "50", "utxo_index": 0}],
                    "body": {
                        "msg_str": f"alice:charlie:50:{ts}:1",
                        "signature": "dummy_sig2",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                }
            ]
        }

        is_valid, error, fees = validator.validate_block_transactions(block_data)
        assert is_valid
        assert error is None


class TestExactOutputValidation:
    """Test Fix 3: Ensure exact authorized amounts are sent to recipients"""

    @patch('config.config.CHAIN_ID', 1)
    @patch('config.config.ADMIN_ADDRESS', "admin_addr")
    @patch('blockchain.transaction_validator.derive_qsafe_address', return_value="alice")
    @patch('blockchain.transaction_validator.verify_transaction', return_value=True)
    def test_insufficient_payment_rejected(self, _mock_verify, _mock_derive):
        """Test that transactions sending less than authorized are rejected"""
        db = FakeDB()

        # Store UTXO with enough balance
        db[b"utxo:prev_tx:0"] = json.dumps({
            "txid": "prev_tx", "utxo_index": 0,
            "receiver": "alice", "amount": "100.1", "spent": False
        }).encode()

        validator = TransactionValidator(db=db)

        ts = str(int(time.time() * 1000))
        block_data = {
            "height": 100,
            "full_transactions": [
                {
                    # Transaction authorizes 100 to bob but only sends 90
                    "txid": "tx1",
                    "inputs": [{"txid": "prev_tx", "utxo_index": 0}],
                    "outputs": [
                        {"receiver": "bob", "amount": "90", "utxo_index": 0},
                        {"receiver": "alice", "amount": "9.9", "utxo_index": 1}
                    ],
                    "body": {
                        "msg_str": f"alice:bob:100:{ts}:1",
                        "signature": "dummy_sig",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                }
            ]
        }

        is_valid, error, fees = validator.validate_block_transactions(block_data)
        assert not is_valid
        assert "amount" in error.lower() or "authorized" in error.lower()

    @patch('config.config.CHAIN_ID', 1)
    @patch('config.config.ADMIN_ADDRESS', "admin_addr")
    @patch('blockchain.transaction_validator.derive_qsafe_address', return_value="alice")
    @patch('blockchain.transaction_validator.verify_transaction', return_value=True)
    def test_exact_payment_accepted(self, _mock_verify, _mock_derive):
        """Test that transactions sending exact authorized amounts are accepted"""
        db = FakeDB()

        # Store UTXO with enough balance
        db[b"utxo:prev_tx:0"] = json.dumps({
            "txid": "prev_tx", "utxo_index": 0,
            "receiver": "alice", "amount": "100.1", "spent": False
        }).encode()

        validator = TransactionValidator(db=db)

        ts = str(int(time.time() * 1000))
        block_data = {
            "height": 100,
            "full_transactions": [
                {
                    "txid": "tx1",
                    "inputs": [{"txid": "prev_tx", "utxo_index": 0}],
                    "outputs": [{"receiver": "bob", "amount": "100", "utxo_index": 0}],
                    "body": {
                        "msg_str": f"alice:bob:100:{ts}:1",
                        "signature": "dummy_sig",
                        "pubkey": "dummy_pubkey",
                        "transaction_data": ""
                    }
                }
            ]
        }

        is_valid, error, fees = validator.validate_block_transactions(block_data)
        assert is_valid
        assert error is None
