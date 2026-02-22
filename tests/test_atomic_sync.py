"""
Test suite for atomic sync state management.

This tests that sync operations are atomic and can be rolled back on failure.
"""

import pytest
import asyncio
import json
import time
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from sync.sync_state_manager import SyncStateManager, SyncStatus, get_sync_state_manager
from sync.sync import process_blocks_from_peer


class TestAtomicSyncStateManager:
    """Test the atomic sync state manager functionality."""
    
    def test_sync_transaction_lifecycle(self):
        """Test that sync transactions follow proper lifecycle."""
        mock_db = Mock()
        manager = SyncStateManager(db=mock_db)
        
        # Begin a sync transaction
        block_hashes = ["hash1", "hash2", "hash3"]
        sync_id = manager.begin_sync_transaction(block_hashes)
        
        # Verify transaction is created
        assert sync_id is not None
        sync_tx = manager.get_sync_status(sync_id)
        assert sync_tx is not None
        assert sync_tx.status == SyncStatus.PENDING
        assert sync_tx.blocks_to_process == block_hashes
        
        # Record processing of first block
        state_changes = {"block": "hash1", "height": 100}
        manager.record_block_processed(sync_id, "hash1", state_changes)
        
        sync_tx = manager.get_sync_status(sync_id)
        assert sync_tx.status == SyncStatus.IN_PROGRESS
        assert "hash1" in sync_tx.blocks_processed
        
        # Record failure of second block
        manager.record_block_failed(sync_id, "hash2", "Invalid block")
        
        sync_tx = manager.get_sync_status(sync_id)
        assert "hash2" in sync_tx.blocks_failed
        assert sync_tx.error == "Invalid block"
        
        # Rollback due to failure
        success = manager.rollback_sync_transaction(sync_id)
        assert success
        
        # Transaction should be removed after rollback
        sync_tx = manager.get_sync_status(sync_id)
        assert sync_tx is None
    
    def test_sync_commit_succeeds_when_no_failures(self):
        """Test that sync commits successfully when all blocks are processed."""
        mock_db = Mock()
        manager = SyncStateManager(db=mock_db)
        
        # Begin transaction
        block_hashes = ["hash1", "hash2"]
        sync_id = manager.begin_sync_transaction(block_hashes)
        
        # Process all blocks successfully
        manager.record_block_processed(sync_id, "hash1", {"height": 100})
        manager.record_block_processed(sync_id, "hash2", {"height": 101})
        
        # Commit should succeed
        success = manager.commit_sync_transaction(sync_id)
        assert success
        
        # Transaction should be removed after commit
        sync_tx = manager.get_sync_status(sync_id)
        assert sync_tx is None
    
    def test_sync_rollback_on_failures(self):
        """Test that sync automatically suggests rollback when there are failures."""
        mock_db = Mock()
        manager = SyncStateManager(db=mock_db)
        
        # Begin transaction
        block_hashes = ["hash1", "hash2"]
        sync_id = manager.begin_sync_transaction(block_hashes)
        
        # One success, one failure
        manager.record_block_processed(sync_id, "hash1", {"height": 100})
        manager.record_block_failed(sync_id, "hash2", "Invalid PoW")
        
        # Commit detects failures and triggers rollback (returns True on successful rollback)
        success = manager.commit_sync_transaction(sync_id)
        assert success  # Rollback succeeded
    
    def test_sync_state_persistence(self):
        """Test that sync state is persisted to database for recovery."""
        mock_db = Mock()
        manager = SyncStateManager(db=mock_db)

        # Begin a sync transaction
        block_hashes = ["hash1", "hash2"]
        sync_id = manager.begin_sync_transaction(block_hashes)

        # Verify state was persisted via batch write (uses WriteBatch internally)
        mock_db.write.assert_called()

        # Verify the sync is tracked in memory
        sync_tx = manager.get_sync_status(sync_id)
        assert sync_tx is not None
        assert sync_tx.status == SyncStatus.PENDING
        assert sync_tx.blocks_to_process == block_hashes
    
    def test_sync_recovery_on_startup(self):
        """Test that incomplete syncs are recovered on startup."""
        mock_db = Mock()

        # Simulate incomplete sync in database
        incomplete_sync = {
            "sync_123": {
                "sync_id": "sync_123",
                "status": "in_progress",
                "start_time": time.time() - 100,
                "blocks_to_process": ["hash1", "hash2"],
                "blocks_processed": ["hash1"],
                "blocks_failed": [],
                "mempool_txs_removed": [],
                "state_changes": {"hash1": {"height": 100}}
            }
        }

        mock_db.get.return_value = json.dumps(incomplete_sync).encode()

        # Create manager - should trigger recovery
        manager = SyncStateManager(db=mock_db)

        # Verify recovery was performed (uses WriteBatch internally)
        mock_db.write.assert_called()

        # Verify no incomplete syncs remain
        assert not manager.has_active_syncs()
    
    @pytest.mark.asyncio
    async def test_process_blocks_with_atomic_sync(self):
        """Test that process_blocks_from_peer uses atomic sync correctly."""

        mock_cm = AsyncMock()
        mock_cm.add_block = AsyncMock(return_value=(True, None))
        mock_cm.is_block_in_main_chain = AsyncMock(return_value=True)
        mock_cm.get_best_chain_tip = AsyncMock(return_value=("tip_hash", 100))
        mock_cm.try_connect_orphan_chain = AsyncMock(return_value=False)
        mock_cm.set_sync_mode = MagicMock()

        mock_db = Mock()
        mock_db.get = Mock(return_value=None)
        mock_db.write = Mock()

        sync_mgr = SyncStateManager(db=mock_db)

        with patch('sync.sync.get_chain_manager', new_callable=AsyncMock, return_value=mock_cm), \
             patch('sync.sync.get_db', return_value=mock_db), \
             patch('sync.sync.get_sync_state_manager', return_value=sync_mgr), \
             patch('sync.sync.normalize_block', side_effect=lambda b, **kw: b), \
             patch('web.web.get_gossip_node', return_value=None):

            # Test blocks — must use valid 64-char hex hashes
            blocks = [
                {
                    "height": 100,
                    "block_hash": "aa" * 32,
                    "previous_hash": "00" * 32,
                    "tx_ids": ["coinbase1", "tx1"],
                    "full_transactions": []
                },
                {
                    "height": 101,
                    "block_hash": "bb" * 32,
                    "previous_hash": "aa" * 32,
                    "tx_ids": ["coinbase2", "tx2"],
                    "full_transactions": []
                }
            ]

            result = await process_blocks_from_peer(blocks)

            assert result == True
            assert mock_cm.add_block.call_count == 2
    
    @pytest.mark.asyncio
    async def test_process_blocks_rollback_on_failure(self):
        """Test that process_blocks_from_peer rolls back on failures."""

        mock_cm = AsyncMock()
        mock_cm.add_block = AsyncMock(side_effect=[
            (True, None),           # First block succeeds
            (False, "Invalid PoW")  # Second block fails
        ])
        mock_cm.is_block_in_main_chain = AsyncMock(return_value=True)
        mock_cm.get_best_chain_tip = AsyncMock(return_value=("tip_hash", 100))
        mock_cm.try_connect_orphan_chain = AsyncMock(return_value=False)
        mock_cm.set_sync_mode = MagicMock()

        mock_db = Mock()
        mock_db.get = Mock(return_value=None)
        mock_db.write = Mock()

        sync_mgr = SyncStateManager(db=mock_db)

        with patch('sync.sync.get_chain_manager', new_callable=AsyncMock, return_value=mock_cm), \
             patch('sync.sync.get_db', return_value=mock_db), \
             patch('sync.sync.get_sync_state_manager', return_value=sync_mgr), \
             patch('sync.sync.normalize_block', side_effect=lambda b, **kw: b), \
             patch('web.web.get_gossip_node', return_value=None):

            # Test blocks — must use valid 64-char hex hashes
            blocks = [
                {
                    "height": 100,
                    "block_hash": "aa" * 32,
                    "previous_hash": "00" * 32,
                    "tx_ids": ["coinbase1"],
                    "full_transactions": []
                },
                {
                    "height": 101,
                    "block_hash": "bb" * 32,
                    "previous_hash": "aa" * 32,
                    "tx_ids": ["coinbase2"],
                    "full_transactions": []
                }
            ]

            result = await process_blocks_from_peer(blocks)

            # Should still return True if at least one block succeeded
            assert result == True
            assert mock_cm.add_block.call_count == 2
    
    def test_mempool_restoration_on_rollback(self):
        """Test that mempool transactions are restored on rollback."""
        mock_db = Mock()
        manager = SyncStateManager(db=mock_db)
        
        with patch('database.database.get_db') as mock_get_db:
            mock_get_db.return_value = mock_db
            
            # Mock mempool manager
            with patch('state.state.mempool_manager') as mock_mempool:
                # Begin transaction
                sync_id = manager.begin_sync_transaction(["hash1"])
                
                # Record mempool removals
                manager.record_mempool_removal(sync_id, "tx1")
                manager.record_mempool_removal(sync_id, "tx2")
                
                # Mock transaction data in database
                tx1_data = json.dumps({"txid": "tx1", "amount": "100"}).encode()
                tx2_data = json.dumps({"txid": "tx2", "amount": "200"}).encode()
                mock_db.get.side_effect = lambda key: {
                    b"tx:tx1": tx1_data,
                    b"tx:tx2": tx2_data
                }.get(key)
                
                # Rollback transaction
                success = manager.rollback_sync_transaction(sync_id)
                assert success
                
                # Verify mempool transactions were re-added
                assert mock_mempool.add_transaction.call_count == 2
    
    def test_concurrent_sync_protection(self):
        """Test that multiple concurrent syncs are handled properly."""
        mock_db = Mock()
        manager = SyncStateManager(db=mock_db)
        
        # Start first sync
        sync_id1 = manager.begin_sync_transaction(["hash1", "hash2"])
        assert manager.has_active_syncs()
        
        # Start second sync (should be allowed - different blocks)
        sync_id2 = manager.begin_sync_transaction(["hash3", "hash4"])
        
        # Both should be active
        assert manager.get_sync_status(sync_id1) is not None
        assert manager.get_sync_status(sync_id2) is not None
        
        # Complete first sync
        manager.commit_sync_transaction(sync_id1)
        
        # Second should still be active
        assert manager.get_sync_status(sync_id2) is not None
        
        # Complete second sync
        manager.commit_sync_transaction(sync_id2)
        
        # No active syncs
        assert not manager.has_active_syncs()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])