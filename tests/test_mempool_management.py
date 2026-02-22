import pytest
import json
import time
from decimal import Decimal
from unittest.mock import patch
from mempool.mempool_manager import MempoolManager
from blockchain.blockchain import serialize_transaction, sha256d


class TestMempoolManagement:
    """Test suite for advanced mempool management features."""
    
    @staticmethod
    def _store_utxos(inputs):
        """Pre-populate UTXOs in the stub database so UTXO validation passes."""
        from database.database import get_db
        db = get_db()
        for inp in inputs:
            if inp.get("txid") == "0" * 64:
                continue
            utxo_key = f"utxo:{inp['txid']}:{inp.get('utxo_index', 0)}".encode()
            db[utxo_key] = json.dumps({
                "spent": False,
                "amount": str(inp.get("amount", "10000")),
                "version": 0
            }).encode()

    def create_test_transaction(self, sender="sender1", receiver="receiver1",
                               amount="100", fee_override=None, txid=None):
        """Helper to create a test transaction with configurable fee."""
        inputs = [{
            "txid": f"prev_tx_{sender}_{amount}",
            "utxo_index": 0,
            "sender": "genesis",
            "receiver": sender,
            "amount": "10000",
            "spent": False
        }]

        # Calculate fee (0.1% unless overridden)
        if fee_override is not None:
            fee = Decimal(str(fee_override))
        else:
            fee = (Decimal(amount) * Decimal("0.001")).quantize(Decimal("0.00000001"))

        remaining = Decimal("10000") - Decimal(amount) - fee

        outputs = [
            {"utxo_index": 0, "sender": sender, "receiver": receiver,
             "amount": str(amount), "spent": False},
            {"utxo_index": 1, "sender": sender, "receiver": sender,
             "amount": str(remaining), "spent": False}
        ]

        timestamp = int(time.time() * 1000)
        msg_str = f"{sender}:{receiver}:{amount}:{timestamp}:1"

        transaction = {
            "type": "transaction",
            "inputs": inputs,
            "outputs": outputs,
            "body": {
                "msg_str": msg_str,
                "pubkey": "test_pubkey",
                "signature": "test_signature"
            },
            "timestamp": timestamp
        }

        if txid is None:
            raw_tx = serialize_transaction(transaction)
            txid = sha256d(bytes.fromhex(raw_tx))[::-1].hex()

        transaction["txid"] = txid
        # Auto-populate UTXOs in stub database
        self._store_utxos(inputs)
        return transaction
    
    def test_minimum_relay_fee_rejection(self):
        """Test that transactions below minimum relay fee are rejected."""
        manager = MempoolManager()
        
        # Create transaction with very low amount (fee will be below minimum)
        # MIN_RELAY_FEE is 0.00001, so we need amount < 0.01 for fee < MIN_RELAY_FEE
        tx = self.create_test_transaction(amount="0.001")  # Fee will be 0.000001
        
        success, error = manager.add_transaction(tx)
        assert success is False
        assert "below minimum relay fee" in error
        assert manager.size() == 0
    
    def test_fee_based_eviction(self):
        """Test that lower fee transactions are evicted when mempool is full."""
        # Create manager with small limit
        manager = MempoolManager(max_size=3)
        
        # Add 3 transactions with different fees
        tx1 = self.create_test_transaction(sender="alice", amount="100")  # Fee: 0.1
        tx2 = self.create_test_transaction(sender="bob", amount="500")    # Fee: 0.5
        tx3 = self.create_test_transaction(sender="charlie", amount="200") # Fee: 0.2
        
        manager.add_transaction(tx1)
        manager.add_transaction(tx2)
        manager.add_transaction(tx3)
        
        assert manager.size() == 3
        
        # Add a 4th transaction with higher fee than lowest
        tx4 = self.create_test_transaction(sender="david", amount="300")  # Fee: 0.3
        success, error = manager.add_transaction(tx4)
        
        assert success is True
        assert manager.size() == 3  # Still 3 (one was evicted)
        
        # Verify lowest fee transaction (tx1) was evicted
        assert manager.get_transaction(tx1["txid"]) is None
        assert manager.get_transaction(tx2["txid"]) is not None
        assert manager.get_transaction(tx3["txid"]) is not None
        assert manager.get_transaction(tx4["txid"]) is not None
    
    def test_reject_low_fee_when_full(self):
        """Test that low fee transactions are rejected when mempool is full."""
        manager = MempoolManager(max_size=2)
        
        # Fill mempool with high-fee transactions
        tx1 = self.create_test_transaction(sender="alice", amount="1000")  # Fee: 1.0
        tx2 = self.create_test_transaction(sender="bob", amount="2000")    # Fee: 2.0
        
        manager.add_transaction(tx1)
        manager.add_transaction(tx2)
        
        # Try to add low-fee transaction
        tx3 = self.create_test_transaction(sender="charlie", amount="50")  # Fee: 0.05
        success, error = manager.add_transaction(tx3)
        
        assert success is False
        assert "too low for full mempool" in error  # Check for partial message
        assert manager.size() == 2
    
    def test_memory_based_eviction(self):
        """Test eviction based on memory limits."""
        # Create manager with 10KB memory limit
        manager = MempoolManager(max_memory_mb=0.01)  # 10KB
        
        # Add transactions until memory limit approached
        added_txs = []
        for i in range(20):
            tx = self.create_test_transaction(
                sender=f"sender{i}", 
                amount=str(100 * (i + 1))  # Increasing fees
            )
            success, _ = manager.add_transaction(tx)
            if success:
                added_txs.append(tx)
        
        # Verify memory limit is respected
        stats = manager.get_stats()
        assert stats["memory_usage_mb"] <= 0.01
        
        # At least some low-fee transactions should have been evicted
        # (but we filled up to memory limit, so some might remain)
        evicted_count = sum(1 for tx in added_txs[:5] 
                          if manager.get_transaction(tx["txid"]) is None)
        assert evicted_count > 0  # At least some low-fee txs were evicted
    
    def test_fee_rate_calculation(self):
        """Test that fee rate (fee per byte) is calculated correctly."""
        manager = MempoolManager()
        
        # Create two transactions with same fee but different sizes
        tx1 = self.create_test_transaction(sender="alice", amount="1000")  # Standard size
        
        # Create larger transaction with more inputs/outputs
        tx2 = self.create_test_transaction(sender="bob", amount="1000")
        extra_input = {
            "txid": "extra_input",
            "utxo_index": 0,
            "sender": "genesis",
            "receiver": "bob",
            "amount": "5000",
            "spent": False
        }
        tx2["inputs"].append(extra_input)
        self._store_utxos([extra_input])
        # Recalculate txid
        del tx2["txid"]
        raw_tx = serialize_transaction(tx2)
        tx2["txid"] = sha256d(bytes.fromhex(raw_tx))[::-1].hex()
        
        manager.add_transaction(tx1)
        manager.add_transaction(tx2)
        
        # Both should be added
        assert manager.size() == 2
        
        # tx2 should have lower fee rate (same fee, larger size)
        assert manager.tx_fee_rates[tx1["txid"]] > manager.tx_fee_rates[tx2["txid"]]
    
    def test_stats_reporting(self):
        """Test mempool statistics reporting."""
        manager = MempoolManager(max_size=100, max_memory_mb=10)
        
        # Add some transactions
        total_expected_fee = Decimal(0)
        for i in range(5):
            amount = Decimal(str(100 * (i + 1)))
            tx = self.create_test_transaction(
                sender=f"sender{i}",
                amount=str(amount)
            )
            manager.add_transaction(tx)
            fee = (amount * Decimal("0.001")).quantize(Decimal("0.00000001"))
            total_expected_fee += fee
        
        stats = manager.get_stats()
        
        assert stats["size"] == 5
        assert Decimal(stats["total_fees"]) == total_expected_fee
        assert stats["in_use_utxos"] == 5
        assert stats["max_size"] == 100
        assert stats["max_memory_mb"] == 10
        assert "average_fee_rate" in stats
    
    def test_concurrent_eviction(self):
        """Test that eviction works correctly with concurrent operations."""
        manager = MempoolManager(max_size=5)
        
        # Fill mempool
        for i in range(5):
            tx = self.create_test_transaction(
                sender=f"sender{i}",
                amount=str(50 + i * 10)  # 50, 60, 70, 80, 90
            )
            manager.add_transaction(tx)
        
        # Add multiple higher-fee transactions
        new_txs = []
        for i in range(3):
            tx = self.create_test_transaction(
                sender=f"new_sender{i}",
                amount=str(200 + i * 100)  # 200, 300, 400
            )
            success, _ = manager.add_transaction(tx)
            assert success is True
            new_txs.append(tx)
        
        # Verify mempool still has 5 transactions
        assert manager.size() == 5
        
        # Verify high-fee transactions are present
        for tx in new_txs:
            assert manager.get_transaction(tx["txid"]) is not None
    
    def test_get_transactions_for_block_respects_limits(self):
        """Test that block template respects transaction limits."""
        manager = MempoolManager()
        
        # Add many transactions
        for i in range(50):
            tx = self.create_test_transaction(
                sender=f"sender{i}",
                amount=str(100 + i)
            )
            manager.add_transaction(tx)
        
        # Get transactions for block with limit
        block_txs = manager.get_transactions_for_block(max_count=10)
        assert len(block_txs) <= 10
        
        # Verify they're sorted by fee (highest first)
        amounts = []
        for tx in block_txs:
            msg_str = tx["body"]["msg_str"]
            amount = Decimal(msg_str.split(":")[2])
            amounts.append(amount)
        
        # Should be sorted descending (higher amounts = higher fees)
        assert amounts == sorted(amounts, reverse=True)
    
    def test_eviction_cleans_up_properly(self):
        """Test that eviction properly cleans up all data structures."""
        manager = MempoolManager(max_size=2)
        
        # Add two transactions
        tx1 = self.create_test_transaction(sender="alice", amount="100")
        tx2 = self.create_test_transaction(sender="bob", amount="500")
        
        manager.add_transaction(tx1)
        manager.add_transaction(tx2)
        
        # Track initial UTXO count
        initial_utxos = len(manager.in_use_utxos)
        
        # Add third transaction to trigger eviction
        tx3 = self.create_test_transaction(sender="charlie", amount="1000")
        manager.add_transaction(tx3)
        
        # Verify tx1 was evicted and cleaned up
        assert manager.get_transaction(tx1["txid"]) is None
        assert tx1["txid"] not in manager.tx_fees
        assert tx1["txid"] not in manager.tx_sizes
        assert tx1["txid"] not in manager.tx_fee_rates
        
        # Verify UTXOs were cleaned up
        for inp in tx1["inputs"]:
            utxo_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
            assert manager.in_use_utxos.get(utxo_key) != tx1["txid"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])