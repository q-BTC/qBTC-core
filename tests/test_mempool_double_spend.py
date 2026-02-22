import pytest
import json
import time
import asyncio
from decimal import Decimal
from unittest.mock import MagicMock, patch

from mempool import MempoolManager
from state.state import mempool_manager
from blockchain.blockchain import serialize_transaction, sha256d


class TestMempoolDoubleSpendPrevention:
    """Test suite for mempool double-spend prevention functionality."""
    
    def setup_method(self):
        """Reset mempool before each test."""
        # Clear the global mempool
        from state.state import mempool_manager
        # Reset mempool manager
        mempool_manager.transactions.clear()
        mempool_manager.in_use_utxos.clear()
        mempool_manager.tx_fees.clear()
        mempool_manager.tx_sizes.clear()
        mempool_manager.current_memory_usage = 0
        
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
                "amount": str(inp.get("amount", "1000")),
                "version": 0
            }).encode()

    def create_test_transaction(self, sender="sender1", receiver="receiver1", amount="100",
                              inputs=None, txid=None):
        """Helper to create a test transaction."""
        if inputs is None:
            inputs = [{
                "txid": "prev_tx_123",
                "utxo_index": 0,
                "sender": "genesis",
                "receiver": sender,
                "amount": "1000",
                "spent": False
            }]

        outputs = [
            {"utxo_index": 0, "sender": sender, "receiver": receiver,
             "amount": str(amount), "spent": False},
            {"utxo_index": 1, "sender": sender, "receiver": sender,
             "amount": str(Decimal("1000") - Decimal(amount) - Decimal("0.1")), "spent": False}
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
            # Calculate txid
            raw_tx = serialize_transaction(transaction)
            txid = sha256d(bytes.fromhex(raw_tx))[::-1].hex()

        transaction["txid"] = txid
        # Auto-populate UTXOs in stub database
        self._store_utxos(inputs)
        return transaction
        
    def test_mempool_manager_basic_functionality(self):
        """Test basic mempool manager functionality."""
        manager = MempoolManager()
        
        # Create a test transaction
        tx = self.create_test_transaction()
        txid = tx["txid"]
        
        # Add transaction
        success, error = manager.add_transaction(tx)
        assert success is True
        assert error is None
        assert manager.size() == 1
        
        # Try to add same transaction again
        success, error = manager.add_transaction(tx)
        assert success is False
        assert "already in mempool" in error
        
        # Remove transaction
        removed = manager.remove_transaction(txid)
        assert removed is True
        assert manager.size() == 0
        
    def test_double_spend_detection_same_utxo(self):
        """Test that double-spending the same UTXO is detected."""
        manager = MempoolManager()
        
        # Create first transaction spending a UTXO
        tx1 = self.create_test_transaction(sender="alice", receiver="bob", amount="50")
        success, error = manager.add_transaction(tx1)
        assert success is True
        
        # Create second transaction trying to spend the same UTXO
        tx2 = self.create_test_transaction(sender="alice", receiver="charlie", amount="30")
        # Use same inputs as tx1
        tx2["inputs"] = tx1["inputs"].copy()
        
        # Recalculate txid for tx2
        del tx2["txid"]
        raw_tx = serialize_transaction(tx2)
        tx2["txid"] = sha256d(bytes.fromhex(raw_tx))[::-1].hex()
        
        # Try to add conflicting transaction
        success, error = manager.add_transaction(tx2)
        assert success is False
        assert "conflicts with existing mempool transactions" in error
        
    def test_multiple_inputs_partial_conflict(self):
        """Test detection when only some inputs conflict."""
        manager = MempoolManager()
        
        # First transaction with one input
        tx1 = self.create_test_transaction()
        manager.add_transaction(tx1)
        
        # Second transaction with multiple inputs, one conflicting
        tx2 = self.create_test_transaction()
        extra_input = {
            "txid": "other_tx_456",
            "utxo_index": 0,
            "sender": "genesis",
            "receiver": "alice",
            "amount": "500",
            "spent": False
        }
        tx2["inputs"] = [
            tx1["inputs"][0],  # Conflicting input
            extra_input
        ]
        self._store_utxos([extra_input])
        
        # Recalculate txid
        del tx2["txid"]
        raw_tx = serialize_transaction(tx2)
        tx2["txid"] = sha256d(bytes.fromhex(raw_tx))[::-1].hex()
        
        success, error = manager.add_transaction(tx2)
        assert success is False
        assert "conflicts" in error
        
    def test_mempool_manager_state_integration(self):
        """Test MempoolManager integration with global state."""
        from state.state import mempool_manager
        
        # Create transaction
        tx = self.create_test_transaction()
        txid = tx["txid"]
        
        # Add through mempool_manager
        success, error = mempool_manager.add_transaction(tx)
        assert success is True
        
        # Verify it's in mempool
        assert mempool_manager.get_transaction(txid) is not None
        assert mempool_manager.size() == 1
        
        # Try to add conflicting transaction
        tx2 = self.create_test_transaction(receiver="charlie")
        tx2["inputs"] = tx["inputs"].copy()
        del tx2["txid"]
        raw_tx = serialize_transaction(tx2)
        tx2["txid"] = sha256d(bytes.fromhex(raw_tx))[::-1].hex()
        
        # Should fail with conflict error
        success, error = mempool_manager.add_transaction(tx2)
        assert success is False
        assert "conflicts" in error.lower()
        
    def test_block_template_no_conflicts(self):
        """Test that block template doesn't include conflicting transactions."""
        manager = MempoolManager()
        
        # Add multiple non-conflicting transactions
        txs = []
        for i in range(5):
            tx = self.create_test_transaction(
                sender=f"sender{i}",
                receiver=f"receiver{i}",
                inputs=[{
                    "txid": f"prev_tx_{i}",
                    "utxo_index": 0,
                    "sender": "genesis",
                    "receiver": f"sender{i}",
                    "amount": "1000",
                    "spent": False
                }]
            )
            manager.add_transaction(tx)
            txs.append(tx)
            
        # Get transactions for block
        block_txs = manager.get_transactions_for_block()
        assert len(block_txs) == 5
        
        # Verify no conflicts in block
        used_utxos = set()
        for tx in block_txs:
            for inp in tx["inputs"]:
                utxo_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
                assert utxo_key not in used_utxos
                used_utxos.add(utxo_key)
                
    def test_fee_based_prioritization(self):
        """Test that transactions are prioritized by fee rate."""
        manager = MempoolManager()
        
        # Add transactions with different amounts (and thus different fees)
        # qBTC uses 0.1% fee, so higher amounts = higher fees
        amounts = ["1000", "500", "2000", "100", "1500"]
        txs = []
        
        for i, amount in enumerate(amounts):
            tx = self.create_test_transaction(
                sender=f"sender{i}",
                amount=amount,
                inputs=[{
                    "txid": f"prev_tx_{i}",
                    "utxo_index": 0,
                    "sender": "genesis",
                    "receiver": f"sender{i}",
                    "amount": "10000",
                    "spent": False
                }]
            )
            manager.add_transaction(tx)
            txs.append((amount, tx))
            
        # Get transactions for block
        block_txs = manager.get_transactions_for_block()
        
        # Extract amounts from block transactions
        block_amounts = []
        for tx in block_txs:
            msg_str = tx["body"]["msg_str"]
            amount = msg_str.split(":")[2]
            block_amounts.append(amount)
            
        # Verify they're sorted by amount (fee) descending
        expected_order = ["2000", "1500", "1000", "500", "100"]
        assert block_amounts == expected_order
        
    def test_mempool_size_limits(self):
        """Test mempool size limits are enforced."""
        # Create manager with small limits
        manager = MempoolManager(max_size=3, max_memory_mb=1)  # 1MB limit
        
        # Add transactions up to count limit
        for i in range(3):
            tx = self.create_test_transaction(
                sender=f"sender{i}",
                inputs=[{
                    "txid": f"prev_tx_{i}",
                    "utxo_index": 0,
                    "sender": "genesis",
                    "receiver": f"sender{i}",
                    "amount": "1000",
                    "spent": False
                }]
            )
            success, error = manager.add_transaction(tx)
            assert success is True
            
        # Try to add one more (should fail due to count limit)
        tx4 = self.create_test_transaction(
            sender="sender4",
            inputs=[{
                "txid": "prev_tx_4",
                "utxo_index": 0,
                "sender": "genesis",
                "receiver": "sender4",
                "amount": "1000",
                "spent": False
            }]
        )
        success, error = manager.add_transaction(tx4)
        assert success is False
        assert "full mempool" in error.lower() or "limit" in error.lower()
        
    def test_remove_confirmed_transactions(self):
        """Test bulk removal of confirmed transactions."""
        manager = MempoolManager()
        
        # Add several transactions
        txids = []
        for i in range(5):
            tx = self.create_test_transaction(
                sender=f"sender{i}",
                inputs=[{
                    "txid": f"prev_tx_{i}",
                    "utxo_index": 0,
                    "sender": "genesis",
                    "receiver": f"sender{i}",
                    "amount": "1000",
                    "spent": False
                }]
            )
            manager.add_transaction(tx)
            txids.append(tx["txid"])
            
        # Remove first 3 as confirmed
        manager.remove_confirmed_transactions(txids[:3])
        
        # Verify correct transactions remain
        assert manager.size() == 2
        assert manager.get_transaction(txids[0]) is None
        assert manager.get_transaction(txids[1]) is None
        assert manager.get_transaction(txids[2]) is None
        assert manager.get_transaction(txids[3]) is not None
        assert manager.get_transaction(txids[4]) is not None
        
    def test_mempool_stats(self):
        """Test mempool statistics calculation."""
        manager = MempoolManager()
        
        # Add transactions with known fees
        total_fee = Decimal(0)
        for i in range(3):
            amount = Decimal(str(100 * (i + 1)))
            fee = (amount * Decimal("0.001")).quantize(Decimal("0.00000001"))
            total_fee += fee
            
            tx = self.create_test_transaction(
                sender=f"sender{i}",
                amount=str(amount),
                inputs=[{
                    "txid": f"prev_tx_{i}",
                    "utxo_index": 0,
                    "sender": "genesis",
                    "receiver": f"sender{i}",
                    "amount": "1000",
                    "spent": False
                }]
            )
            manager.add_transaction(tx)
            
        stats = manager.get_stats()
        assert stats["size"] == 3
        assert Decimal(stats["total_fees"]) == total_fee
        assert Decimal(stats["average_fee"]) == total_fee / 3
        assert stats["in_use_utxos"] == 3
        
    @pytest.mark.asyncio
    async def test_gossip_double_spend_rejection(self):
        """Test that gossip rejects double-spend transactions."""
        from gossip.gossip import GossipNode
        from state.state import mempool_manager

        # Create gossip node
        node = GossipNode("test_node")

        # Create first transaction (UTXOs auto-stored in conftest _StubDB)
        tx1 = self.create_test_transaction()
        tx1_msg = {
            "type": "transaction",
            "txid": tx1["txid"],
            "timestamp": int(time.time() * 1000),
            **tx1
        }

        # Use conftest's _StubDB (already patched into gossip.gossip.get_db)
        # instead of MagicMock which returns truthy for all .get() calls
        with patch('gossip.gossip.verify_transaction', return_value=True):
            # Process first transaction
            await node.handle_gossip_message(tx1_msg, ("127.0.0.1", 8000), None)

        # Verify transaction was added
        assert mempool_manager.get_transaction(tx1["txid"]) is not None

        # Create conflicting transaction
        tx2 = self.create_test_transaction(receiver="charlie")
        tx2["inputs"] = tx1["inputs"].copy()
        del tx2["txid"]
        raw_tx = serialize_transaction(tx2)
        tx2["txid"] = sha256d(bytes.fromhex(raw_tx))[::-1].hex()

        tx2_msg = {
            "type": "transaction",
            "txid": tx2["txid"],
            "timestamp": int(time.time() * 1000),
            **tx2
        }
        
        # Try to process conflicting transaction
        with patch('gossip.gossip.verify_transaction', return_value=True):
            await node.handle_gossip_message(tx2_msg, ("127.0.0.1", 8000), None)
                
        # Verify second transaction was rejected
        assert mempool_manager.get_transaction(tx2["txid"]) is None
        assert mempool_manager.get_transaction(tx1["txid"]) is not None  # First one still there


if __name__ == "__main__":
    pytest.main([__file__, "-v"])