"""
Test suite for double-spend protection and UTXO locking mechanism.

This tests the race condition fixes and ensures atomic transaction validation.
"""

import pytest
import threading
import time
import json
from decimal import Decimal
from unittest.mock import Mock, patch
from mempool.mempool_manager import MempoolManager
from blockchain.utxo_lock import UTXOLockManager, get_utxo_lock_manager


class TestDoubleSpendProtection:
    """Test double-spend protection mechanisms."""
    
    def test_utxo_locking_prevents_double_spend(self):
        """Test that UTXO locking prevents concurrent double-spend attempts."""
        lock_manager = UTXOLockManager()
        
        # Simulate two threads trying to spend the same UTXO
        results = []
        
        def try_spend_utxo(thread_id):
            utxos = [("tx123", 0)]
            acquired = lock_manager.acquire_locks(utxos, timeout=2.0)
            if acquired:
                results.append((thread_id, "acquired"))
                time.sleep(0.1)  # Simulate transaction processing
                lock_manager.release_locks(utxos)
                results.append((thread_id, "released"))
            else:
                results.append((thread_id, "failed"))
        
        # Start two threads trying to acquire the same UTXO lock
        thread1 = threading.Thread(target=try_spend_utxo, args=(1,))
        thread2 = threading.Thread(target=try_spend_utxo, args=(2,))
        
        thread1.start()
        thread2.start()
        
        thread1.join()
        thread2.join()
        
        # Check that only one thread acquired the lock first
        acquired_count = sum(1 for r in results if r[1] == "acquired")
        assert acquired_count == 2, "Both threads should eventually acquire the lock"
        
        # Verify the ordering - one must complete before the other starts
        thread1_acquired = next((i for i, r in enumerate(results) if r == (1, "acquired")), None)
        thread1_released = next((i for i, r in enumerate(results) if r == (1, "released")), None)
        thread2_acquired = next((i for i, r in enumerate(results) if r == (2, "acquired")), None)
        thread2_released = next((i for i, r in enumerate(results) if r == (2, "released")), None)
        
        # Either thread1 completes before thread2 starts, or vice versa
        if thread1_acquired < thread2_acquired:
            assert thread1_released < thread2_acquired, "Thread 1 must release before thread 2 acquires"
        else:
            assert thread2_released < thread1_acquired, "Thread 2 must release before thread 1 acquires"
    
    def test_mempool_rejects_spent_utxos(self):
        """Test that mempool correctly rejects transactions spending already-spent UTXOs."""
        mempool = MempoolManager()
        
        # Create a mock transaction
        tx = {
            "txid": "test_tx_1",
            "inputs": [
                {"txid": "prev_tx", "utxo_index": 0}
            ],
            "outputs": [
                {"receiver": "bqs123", "amount": "10"}
            ],
            "body": {"msg_str": "sender:receiver:10"}
        }
        
        # Mock the database to simulate a spent UTXO
        with patch('database.database.get_db') as mock_get_db:
            mock_db = Mock()
            mock_get_db.return_value = mock_db
            
            # Simulate UTXO that is already spent
            utxo_data = json.dumps({"spent": True, "amount": "100"}).encode()
            mock_db.get.return_value = utxo_data
            
            # Try to add transaction - should fail
            success, error = mempool.add_transaction(tx)
            
            assert not success, "Transaction should be rejected"
            assert "already spent" in error, f"Error should mention UTXO is spent: {error}"
    
    def test_mempool_rejects_nonexistent_utxos(self):
        """Test that mempool correctly rejects transactions spending non-existent UTXOs."""
        mempool = MempoolManager()
        
        # Create a mock transaction
        tx = {
            "txid": "test_tx_2",
            "inputs": [
                {"txid": "nonexistent_tx", "utxo_index": 0}
            ],
            "outputs": [
                {"receiver": "bqs456", "amount": "5"}
            ],
            "body": {"msg_str": "sender:receiver:5"}
        }
        
        # Mock the database to simulate non-existent UTXO
        with patch('database.database.get_db') as mock_get_db:
            mock_db = Mock()
            mock_get_db.return_value = mock_db
            
            # Simulate UTXO that doesn't exist
            mock_db.get.return_value = None
            
            # Try to add transaction - should fail
            success, error = mempool.add_transaction(tx)
            
            assert not success, "Transaction should be rejected"
            assert "does not exist" in error, f"Error should mention UTXO doesn't exist: {error}"
    
    def test_mempool_accepts_valid_transaction(self):
        """Test that mempool correctly accepts valid transactions with unspent UTXOs."""
        mempool = MempoolManager()
        
        # Create a mock transaction
        tx = {
            "txid": "test_tx_3",
            "inputs": [
                {"txid": "valid_tx", "utxo_index": 0}
            ],
            "outputs": [
                {"receiver": "bqs789", "amount": "50"}
            ],
            "body": {"msg_str": "sender:receiver:50"}
        }
        
        # Mock the database to simulate valid unspent UTXO
        with patch('database.database.get_db') as mock_get_db:
            mock_db = Mock()
            mock_get_db.return_value = mock_db
            
            # Simulate valid unspent UTXO
            utxo_data = json.dumps({"spent": False, "amount": "100"}).encode()
            mock_db.get.return_value = utxo_data
            
            # Try to add transaction - should succeed
            success, error = mempool.add_transaction(tx)
            
            assert success, f"Transaction should be accepted: {error}"
            assert mempool.get_transaction("test_tx_3") is not None, "Transaction should be in mempool"
    
    def test_lock_timeout_and_cleanup(self):
        """Test that stale locks are cleaned up after timeout."""
        lock_manager = UTXOLockManager(lock_timeout=0.5)  # 500ms timeout
        
        # Acquire a lock
        utxos = [("tx_timeout", 0)]
        acquired = lock_manager.acquire_locks(utxos, timeout=1.0)
        assert acquired, "Should acquire lock initially"
        
        # Wait for lock to become stale
        time.sleep(0.6)
        
        # Clean up stale locks
        lock_manager.cleanup_stale_locks()
        
        # Another thread should now be able to acquire the same lock
        acquired2 = lock_manager.acquire_locks(utxos, timeout=1.0)
        assert acquired2, "Should be able to acquire lock after cleanup"
        lock_manager.release_locks(utxos)
    
    def test_deadlock_prevention_with_ordered_locking(self):
        """Test that ordered locking prevents deadlocks."""
        lock_manager = UTXOLockManager()
        
        results = []
        
        def thread1_work():
            # Try to lock UTXOs in one order
            utxos = [("tx_a", 0), ("tx_b", 0)]
            acquired = lock_manager.acquire_locks(utxos, timeout=2.0)
            if acquired:
                results.append("thread1_acquired")
                time.sleep(0.1)
                lock_manager.release_locks(utxos)
                results.append("thread1_released")
        
        def thread2_work():
            # Try to lock UTXOs in reverse order - should still work due to sorting
            utxos = [("tx_b", 0), ("tx_a", 0)]
            acquired = lock_manager.acquire_locks(utxos, timeout=2.0)
            if acquired:
                results.append("thread2_acquired")
                time.sleep(0.1)
                lock_manager.release_locks(utxos)
                results.append("thread2_released")
        
        # Start both threads
        t1 = threading.Thread(target=thread1_work)
        t2 = threading.Thread(target=thread2_work)
        
        t1.start()
        t2.start()
        
        t1.join(timeout=3.0)
        t2.join(timeout=3.0)
        
        # Both threads should complete without deadlock
        assert "thread1_acquired" in results, "Thread 1 should acquire locks"
        assert "thread2_acquired" in results, "Thread 2 should acquire locks"
        assert len(results) == 4, "Both threads should acquire and release"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])