"""
Sync State Manager for Atomic Block Processing

This module ensures atomic state updates during blockchain synchronization,
preventing corruption if the process crashes mid-sync.
"""

import json
import logging
import time
import threading
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from rocksdict import WriteBatch
from database.database import get_db

logger = logging.getLogger(__name__)


class SyncStatus(Enum):
    """Sync operation status"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class SyncTransaction:
    """Represents an atomic sync transaction"""
    sync_id: str
    status: SyncStatus
    start_time: float
    blocks_to_process: List[str]  # Block hashes
    blocks_processed: List[str]
    blocks_failed: List[str]
    mempool_txs_removed: List[str]
    state_changes: Dict[str, any]  # Track all state changes for rollback
    error: Optional[str] = None
    end_time: Optional[float] = None


class SyncStateManager:
    """
    Manages atomic sync operations with rollback capability.
    
    Ensures that sync operations are atomic - either all blocks in a sync
    batch are processed successfully, or the entire operation is rolled back.
    """
    
    def __init__(self, db=None):
        self.db = db if db is not None else get_db()
        self.active_syncs: Dict[str, SyncTransaction] = {}
        self.sync_lock = threading.RLock()
        if self.db is not None:
            self._recover_from_incomplete_syncs()
        
    def _recover_from_incomplete_syncs(self):
        """Check for and recover from incomplete sync operations on startup"""
        try:
            # Look for sync state in database
            sync_state_key = b"sync:active_transactions"
            sync_data = self.db.get(sync_state_key)
            
            if sync_data:
                incomplete_syncs = json.loads(sync_data.decode())
                logger.warning(f"Found {len(incomplete_syncs)} incomplete sync operations")
                
                for sync_id, sync_info in incomplete_syncs.items():
                    if sync_info['status'] == SyncStatus.IN_PROGRESS.value:
                        logger.warning(f"Rolling back incomplete sync {sync_id}")
                        self._rollback_sync_internal(sync_id, sync_info)
                        
                # Clear the sync state after recovery atomically
                from rocksdict import WriteBatch
                batch = WriteBatch()
                batch.delete(sync_state_key)
                self.db.write(batch)
                logger.info("Sync recovery completed")
                
        except Exception as e:
            logger.error(f"Error during sync recovery: {e}")
    
    def begin_sync_transaction(self, block_hashes: List[str]) -> str:
        """
        Begin an atomic sync transaction.
        
        Args:
            block_hashes: List of block hashes to process
            
        Returns:
            Sync transaction ID
        """
        import uuid
        
        with self.sync_lock:
            sync_id = str(uuid.uuid4())
            
            sync_tx = SyncTransaction(
                sync_id=sync_id,
                status=SyncStatus.PENDING,
                start_time=time.time(),
                blocks_to_process=block_hashes,
                blocks_processed=[],
                blocks_failed=[],
                mempool_txs_removed=[],
                state_changes={}
            )
            
            self.active_syncs[sync_id] = sync_tx
            
            # Persist sync state to database for crash recovery
            self._persist_sync_state()
            
            logger.info(f"Started sync transaction {sync_id} for {len(block_hashes)} blocks")
            return sync_id
    
    def record_block_processed(self, sync_id: str, block_hash: str, state_changes: Dict):
        """
        Record that a block was successfully processed.
        
        Args:
            sync_id: Sync transaction ID
            block_hash: Hash of processed block
            state_changes: State changes made (for potential rollback)
        """
        with self.sync_lock:
            if sync_id not in self.active_syncs:
                raise ValueError(f"Unknown sync transaction {sync_id}")
                
            sync_tx = self.active_syncs[sync_id]
            sync_tx.status = SyncStatus.IN_PROGRESS
            sync_tx.blocks_processed.append(block_hash)
            
            # Record state changes for potential rollback
            if block_hash not in sync_tx.state_changes:
                sync_tx.state_changes[block_hash] = state_changes
            
            self._persist_sync_state()
            logger.debug(f"Recorded block {block_hash} processed in sync {sync_id}")
    
    def record_block_failed(self, sync_id: str, block_hash: str, error: str):
        """
        Record that a block failed to process.
        
        Args:
            sync_id: Sync transaction ID
            block_hash: Hash of failed block
            error: Error message
        """
        with self.sync_lock:
            if sync_id not in self.active_syncs:
                raise ValueError(f"Unknown sync transaction {sync_id}")
                
            sync_tx = self.active_syncs[sync_id]
            sync_tx.blocks_failed.append(block_hash)
            sync_tx.error = error
            
            self._persist_sync_state()
            logger.warning(f"Recorded block {block_hash} failed in sync {sync_id}: {error}")
    
    def record_mempool_removal(self, sync_id: str, txid: str):
        """
        Record that a transaction was removed from mempool.
        
        Args:
            sync_id: Sync transaction ID
            txid: Transaction ID removed from mempool
        """
        with self.sync_lock:
            if sync_id not in self.active_syncs:
                raise ValueError(f"Unknown sync transaction {sync_id}")
                
            sync_tx = self.active_syncs[sync_id]
            sync_tx.mempool_txs_removed.append(txid)
            
            self._persist_sync_state()
    
    def commit_sync_transaction(self, sync_id: str) -> bool:
        """
        Commit a sync transaction if all blocks were processed successfully.
        
        Args:
            sync_id: Sync transaction ID
            
        Returns:
            True if committed, False if rolled back due to failures
        """
        with self.sync_lock:
            if sync_id not in self.active_syncs:
                raise ValueError(f"Unknown sync transaction {sync_id}")
                
            sync_tx = self.active_syncs[sync_id]
            
            # Check if all blocks were processed successfully
            if sync_tx.blocks_failed:
                logger.warning(f"Sync {sync_id} has failures, rolling back")
                return self.rollback_sync_transaction(sync_id)
            
            # Mark as completed
            sync_tx.status = SyncStatus.COMPLETED
            sync_tx.end_time = time.time()
            
            # Remove from active syncs
            del self.active_syncs[sync_id]
            
            # Clear persisted state
            self._persist_sync_state()
            
            duration = sync_tx.end_time - sync_tx.start_time
            logger.info(f"Committed sync transaction {sync_id}: "
                       f"{len(sync_tx.blocks_processed)} blocks in {duration:.2f}s")
            return True
    
    def rollback_sync_transaction(self, sync_id: str) -> bool:
        """
        Rollback a sync transaction, undoing all changes.
        
        Args:
            sync_id: Sync transaction ID
            
        Returns:
            True if rollback successful
        """
        with self.sync_lock:
            if sync_id not in self.active_syncs:
                logger.warning(f"Sync transaction {sync_id} not found for rollback")
                return False
                
            sync_tx = self.active_syncs[sync_id]
            logger.warning(f"Rolling back sync transaction {sync_id}")
            
            try:
                if self.db is not None:
                    # Create a write batch for atomic rollback
                    batch = WriteBatch()
                    
                    # Rollback blocks in reverse order
                    for block_hash in reversed(sync_tx.blocks_processed):
                        if block_hash in sync_tx.state_changes:
                            changes = sync_tx.state_changes[block_hash]
                            self._rollback_block_changes(block_hash, changes, batch)
                    
                    # Re-add removed mempool transactions
                    from state.state import mempool_manager
                    for txid in sync_tx.mempool_txs_removed:
                        # Note: We can't perfectly restore mempool state, but we can try
                        # to re-add transactions if they still exist in the database
                        tx_key = f"tx:{txid}".encode()
                        tx_data = self.db.get(tx_key)
                        if tx_data:
                            tx = json.loads(tx_data.decode())
                            try:
                                mempool_manager.add_transaction(tx)
                                logger.info(f"Re-added transaction {txid} to mempool during rollback")
                            except Exception as e:
                                logger.warning(f"Could not re-add transaction {txid} to mempool: {e}")
                    
                    # Apply the rollback atomically
                    self.db.write(batch)
                    
                    # Update chain index after rollback
                    # The chain index is automatically maintained by ChainManager
                    # No explicit persistence needed as it's handled internally
                    logger.debug("Chain index updated after rollback")
                
                # Mark as rolled back
                sync_tx.status = SyncStatus.ROLLED_BACK
                sync_tx.end_time = time.time()
                
                # Remove from active syncs
                del self.active_syncs[sync_id]
                
                # Clear persisted state
                self._persist_sync_state()
                
                logger.info(f"Successfully rolled back sync transaction {sync_id}")
                return True
                
            except Exception as e:
                logger.error(f"Error during rollback of sync {sync_id}: {e}")
                return False
    
    def _rollback_block_changes(self, block_hash: str, changes: Dict, batch: WriteBatch):
        """
        Rollback changes made by a specific block.
        
        Args:
            block_hash: Block to rollback
            changes: State changes to undo
            batch: Write batch for atomic operations
        """
        # Only remove blocks that were actually added during this sync transaction
        # Check if we have recorded state changes for this block
        if not changes:
            logger.warning(f"No state changes recorded for block {block_hash}, skipping rollback")
            return
            
        # Remove the block itself only if it was added in this transaction
        if changes.get('block_added', False):
            block_key = f"block:{block_hash}".encode()
            batch.delete(block_key)
            logger.info(f"Rolling back block {block_hash} from database")
            
            # Also remove from chain index to maintain consistency
            try:
                from blockchain.chain_singleton import get_chain_manager_sync
                cm = get_chain_manager_sync()
                if cm and block_hash in cm.block_index:
                    # Remove from in-memory index
                    del cm.block_index[block_hash]
                    logger.debug(f"Removed block {block_hash} from chain manager index")
            except Exception as e:
                logger.debug(f"Could not update chain index during rollback: {e}")
        
        # Restore UTXOs that were spent
        for utxo_key, original_state in changes.get('spent_utxos', {}).items():
            if original_state:
                batch.put(utxo_key.encode(), json.dumps(original_state).encode())
        
        # Remove UTXOs that were created
        for utxo_key in changes.get('created_utxos', []):
            batch.delete(utxo_key.encode())
        
        # Restore any other state changes
        for key, original_value in changes.get('other_changes', {}).items():
            if original_value is None:
                batch.delete(key.encode())
            else:
                batch.put(key.encode(), json.dumps(original_value).encode())
        
        logger.debug(f"Prepared rollback for block {block_hash}")
    
    def _rollback_sync_internal(self, sync_id: str, sync_info: Dict):
        """Internal method to rollback a sync from persisted state"""
        # Convert dict back to SyncTransaction
        sync_tx = SyncTransaction(
            sync_id=sync_id,
            status=SyncStatus(sync_info['status']),
            start_time=sync_info['start_time'],
            blocks_to_process=sync_info['blocks_to_process'],
            blocks_processed=sync_info['blocks_processed'],
            blocks_failed=sync_info['blocks_failed'],
            mempool_txs_removed=sync_info['mempool_txs_removed'],
            state_changes=sync_info['state_changes'],
            error=sync_info.get('error'),
            end_time=sync_info.get('end_time')
        )
        
        self.active_syncs[sync_id] = sync_tx
        self.rollback_sync_transaction(sync_id)
    
    def _persist_sync_state(self):
        """Persist active sync transactions to database for crash recovery"""
        if self.db is None:
            return  # Skip persistence if no database
            
        sync_state_key = b"sync:active_transactions"

        if not self.active_syncs:
            # No active syncs, remove the key atomically
            batch = WriteBatch()
            batch.delete(sync_state_key)
            self.db.write(batch)
        else:
            # Convert to JSON-serializable format
            sync_data = {}
            for sync_id, sync_tx in self.active_syncs.items():
                sync_dict = asdict(sync_tx)
                sync_dict['status'] = sync_tx.status.value
                sync_data[sync_id] = sync_dict

            batch = WriteBatch()
            batch.put(sync_state_key, json.dumps(sync_data).encode())
            self.db.write(batch)
    
    def get_sync_status(self, sync_id: str) -> Optional[SyncTransaction]:
        """Get the status of a sync transaction"""
        return self.active_syncs.get(sync_id)
    
    def has_active_syncs(self) -> bool:
        """Check if there are any active sync operations"""
        return len(self.active_syncs) > 0


# Global sync state manager instance
_sync_state_manager = None


def get_sync_state_manager() -> SyncStateManager:
    """Get the global sync state manager instance"""
    global _sync_state_manager
    if _sync_state_manager is None:
        _sync_state_manager = SyncStateManager()
    return _sync_state_manager