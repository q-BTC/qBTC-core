"""
Chain Manager - Handles blockchain consensus, reorganizations, and fork resolution
Implements the longest chain rule (actually highest cumulative difficulty)
"""
import json
import logging
import time
from typing import Dict, List, Optional, Tuple, Set
from decimal import Decimal
from collections import defaultdict
from database.database import get_db
from blockchain.blockchain import Block, validate_pow, bits_to_target, sha256d
from blockchain.difficulty import get_next_bits, validate_block_bits, validate_block_timestamp, MAX_FUTURE_TIME
from blockchain.transaction_validator import TransactionValidator
from rocksdict import WriteBatch
from state.state import mempool_manager
from blockchain.block_height_index import get_height_index

logger = logging.getLogger(__name__)


class ChainManager:
    """
    Manages blockchain state including:
    - Active chain tracking
    - Fork detection and resolution
    - Chain reorganization
    - Orphan block management
    """
    
    def __init__(self, db=None):
        self.db = db if db is not None else get_db()
        self.orphan_blocks: Dict[str, dict] = {}  # hash -> block data
        self.validator = TransactionValidator(self.db)
        self.orphan_timestamps: Dict[str, int] = {}  # hash -> timestamp when added
        self.block_index: Dict[str, dict] = {}  # hash -> block metadata
        self.chain_tips: Set[str] = set()  # Set of chain tip hashes
        self.MAX_ORPHAN_BLOCKS = 100  # Maximum number of orphan blocks to keep
        self.MAX_ORPHAN_AGE = 3600  # Maximum age of orphan blocks in seconds (1 hour)
        self.is_syncing = False  # Flag to indicate if we're in initial sync
        self._initialize_index()
    
    def _initialize_index(self):
        """Build in-memory index of all blocks in database"""
        logger.info("Initializing chain index...")
        
        # Load all blocks into index
        for key, value in self.db.items():
            if key.startswith(b"block:"):
                block_data = json.loads(value.decode())
                block_hash = block_data["block_hash"]
                self.block_index[block_hash] = {
                    "height": block_data["height"],
                    "previous_hash": block_data["previous_hash"],
                    "timestamp": block_data["timestamp"],
                    "bits": block_data["bits"],
                    "cumulative_difficulty": self._get_cumulative_difficulty(block_hash)
                }
        
        # Find all chain tips (blocks with no children)
        self._update_chain_tips()
        logger.info(f"Chain index initialized with {len(self.block_index)} blocks and {len(self.chain_tips)} tips")
    
    def _update_chain_tips(self):
        """Update the set of chain tips"""
        # Start with all blocks as potential tips
        potential_tips = set(self.block_index.keys())
        
        # Remove any block that is a parent of another block
        for block_hash, block_info in self.block_index.items():
            prev_hash = block_info["previous_hash"]
            # Don't remove genesis (all zeros) from tips
            if prev_hash in potential_tips and prev_hash != "0" * 64:
                potential_tips.discard(prev_hash)
        
        self.chain_tips = potential_tips
    
    def _get_cumulative_difficulty(self, block_hash: str) -> Decimal:
        """Calculate cumulative difficulty from genesis to this block"""
        cumulative = Decimal(0)
        current_hash = block_hash
        
        while current_hash and current_hash != "00" * 32:
            if current_hash not in self.block_index:
                # Block not in index, try to load from DB
                block_key = f"block:{current_hash}".encode()
                block_data = self.db.get(block_key)
                if not block_data:
                    logger.warning(f"Block {current_hash} not found in chain")
                    break
                block_info = json.loads(block_data.decode())
            else:
                block_info = self.block_index[current_hash]
            
            # Add this block's difficulty
            bits = block_info.get("bits", 0x1d00ffff)  # Default to min difficulty
            target = bits_to_target(bits)
            difficulty = Decimal(2**256) / Decimal(target)
            cumulative += difficulty
            
            current_hash = block_info.get("previous_hash")
        
        return cumulative
    
    def get_best_chain_tip(self) -> Tuple[str, int]:
        """
        Get the best chain tip (highest cumulative difficulty)
        Returns (block_hash, height)
        """
        best_tip = None
        best_difficulty = Decimal(0)
        best_height = 0
        
        for tip_hash in self.chain_tips:
            tip_info = self.block_index.get(tip_hash)
            if not tip_info:
                continue
                
            difficulty = self._get_cumulative_difficulty(tip_hash)
            if difficulty > best_difficulty:
                best_difficulty = difficulty
                best_tip = tip_hash
                best_height = tip_info["height"]
        
        if not best_tip:
            # No tips found, return -1 for empty blockchain
            return "00" * 32, -1
            
        return best_tip, best_height
    
    def set_sync_mode(self, syncing: bool):
        """Set whether we're in initial sync mode"""
        self.is_syncing = syncing
        logger.info(f"Sync mode set to: {syncing}")
    
    def add_block(self, block_data: dict) -> Tuple[bool, Optional[str]]:
        """
        Add a new block to the chain
        Returns (success, error_message)
        """
        # Validate required fields
        required_fields = ["block_hash", "previous_hash", "height", "version", "merkle_root", "timestamp", "bits", "nonce"]
        missing_fields = [field for field in required_fields if field not in block_data]
        if missing_fields:
            logger.error(f"Missing required fields in block_data: {missing_fields}")
            logger.error(f"Received block_data keys: {list(block_data.keys())}")
            return False, f"Missing required fields: {missing_fields}"
        
        block_hash = block_data["block_hash"]
        prev_hash = block_data["previous_hash"]
        height = block_data["height"]
        
        # Check if block already exists
        if block_hash in self.block_index:
            return True, None  # Already have this block
        
        # Validate PoW
        try:
            block_obj = Block(
                block_data["version"],
                prev_hash,
                block_data["merkle_root"],
                block_data["timestamp"],
                block_data["bits"],
                block_data["nonce"]
            )
        except KeyError as e:
            logger.error(f"Missing required field in block_data: {e}")
            logger.error(f"Block data keys: {list(block_data.keys())}")
            raise
        
        # Special handling for genesis block
        is_genesis = block_hash == "0" * 64 and height == 0
        
        # Always validate PoW (except for genesis)
        if not is_genesis and not validate_pow(block_obj):
            return False, "Invalid proof of work"
        
        # Validate difficulty adjustment (skip for genesis)
        if not is_genesis and height > 0:
            # For difficulty validation, we need to check against the parent block
            # The parent must already be in our database for proper validation
            parent_block_key = f"block:{prev_hash}".encode()
            parent_block_data = self.db.get(parent_block_key)
            
            if parent_block_data:
                # Parent exists, we can validate difficulty
                parent_block = json.loads(parent_block_data.decode())
                parent_height = parent_block["height"]
                expected_bits = get_next_bits(self.db, parent_height)
                
                if not validate_block_bits(block_data["bits"], expected_bits):
                    return False, f"Invalid difficulty bits at height {height}: expected {expected_bits:#x}, got {block_data['bits']:#x}"
            else:
                # Parent doesn't exist yet - this is an orphan block
                # Store as orphan and process later
                logger.warning(f"Parent block {prev_hash} not found for block {block_hash} at height {height}")
                self.orphan_blocks[block_hash] = block_data
                return False, "Parent block not found - stored as orphan"
        
        # Validate timestamp (skip only for genesis)
        if not is_genesis and prev_hash in self.block_index:
            parent_info = self.block_index[prev_hash]
            
            # During sync, we can't use current time for historical blocks
            # Instead, we only validate that blocks are sequential in time
            if self.is_syncing:
                # During sync, only validate against parent timestamp
                if block_data["timestamp"] <= parent_info["timestamp"]:
                    # Allow equal timestamps for rapid mining scenarios
                    if block_data["timestamp"] < parent_info["timestamp"]:
                        return False, f"Invalid block timestamp during sync - must be >= parent (block: {block_data['timestamp']}, parent: {parent_info['timestamp']})"
                logger.info(f"Sync mode timestamp validation: block_ts={block_data['timestamp']}, parent_ts={parent_info['timestamp']}")
            else:
                # Not syncing - validate against current time too
                current_time = int(time.time())
                logger.info(f"Timestamp validation: block_ts={block_data['timestamp']}, parent_ts={parent_info['timestamp']}, current={current_time}")
                
                # Additional validation when not syncing
                # Special case: If parent is genesis (height 0), be more lenient with timestamp
                parent_height = parent_info.get("height", 0)
                if parent_height == 0:
                    # For block 1 (child of genesis), only check that it's not too far in future
                    if block_data["timestamp"] > current_time + MAX_FUTURE_TIME:
                        return False, f"Block timestamp too far in future"
                    logger.info("Allowing block 1 timestamp despite being before genesis (special case)")
                elif block_data["timestamp"] <= parent_info["timestamp"]:
                    # Special handling for rapid mining (cpuminer compatibility)
                    # If the block timestamp equals or is less than parent timestamp, check if we're mining rapidly
                    # Check if parent block was mined very recently (within last 10 seconds)
                    time_since_parent = current_time - parent_info["timestamp"]
                    logger.info(f"Block timestamp <= parent. Time since parent: {time_since_parent}s")
                    
                    if time_since_parent <= 10:  # Increased window to 10 seconds
                        logger.warning(f"Allowing timestamp {block_data['timestamp']} <= parent {parent_info['timestamp']} for rapid mining (parent mined {time_since_parent}s ago)")
                        # Skip the normal timestamp validation for rapid mining
                    else:
                        return False, f"Invalid block timestamp - must be greater than parent (block: {block_data['timestamp']}, parent: {parent_info['timestamp']})"
                else:
                    # Normal timestamp validation
                    if not validate_block_timestamp(
                        block_data["timestamp"],
                        parent_info["timestamp"],
                        current_time
                    ):
                        return False, "Invalid block timestamp"
        
        # Check if we have the parent block
        if prev_hash not in self.block_index and prev_hash != "00" * 32:
            # Parent not found - this is an orphan
            self._add_orphan(block_data)
            return True, None
        
        # CRITICAL: Validate all transactions before accepting the block
        # This prevents invalid transactions from entering the chain
        if "full_transactions" in block_data and block_data["full_transactions"]:
            logger.info(f"Validating {len(block_data['full_transactions'])} transactions in block {block_hash}")
            
            # Debug: Log transactions in the block
            if height == 1:
                for i, tx in enumerate(block_data['full_transactions']):
                    logger.info(f"Block 1 transaction {i}: has_txid={('txid' in tx) if tx else False}, keys={(list(tx.keys()) if tx else 'None')}")
            
            # During sync mode, skip time validation for historical blocks
            if self.is_syncing:
                self.validator.skip_time_validation = True
            
            # Validate all non-coinbase transactions
            is_valid, error_msg, total_fees = self.validator.validate_block_transactions(block_data)
            
            # Reset skip_time_validation after validation
            if self.is_syncing:
                self.validator.skip_time_validation = False
            if not is_valid:
                logger.error(f"Block {block_hash} rejected: {error_msg}")
                
                # Import mempool manager to clean up invalid transactions
                from state.state import mempool_manager
                
                # Check if any transactions in this rejected block are in our mempool
                # If they are, they're likely invalid and should be removed
                if "tx_ids" in block_data and len(block_data.get("tx_ids", [])) > 1:
                    tx_ids_to_check = block_data["tx_ids"][1:]  # Skip coinbase
                    removed_txids = []
                    
                    for txid in tx_ids_to_check:
                        if mempool_manager.get_transaction(txid) is not None:
                            mempool_manager.remove_transaction(txid)
                            removed_txids.append(txid)
                            logger.info(f"[ChainManager] Removed invalid transaction {txid} from mempool")
                    
                    if removed_txids:
                        logger.info(f"[ChainManager] Removed {len(removed_txids)} invalid transactions from mempool after block validation failure")
                
                return False, error_msg
            
            # Find and validate coinbase transaction
            coinbase_tx = None
            for tx in block_data["full_transactions"]:
                if tx and self.validator._is_coinbase_transaction(tx):
                    coinbase_tx = tx
                    break
            
            if coinbase_tx and height > 0:  # Skip coinbase validation for genesis
                is_valid, error_msg = self.validator.validate_coinbase_transaction(
                    coinbase_tx, height, total_fees
                )
                if not is_valid:
                    logger.error(f"Block {block_hash} rejected: invalid coinbase - {error_msg}")
                    return False, f"Invalid coinbase transaction: {error_msg}"
        
        # Now that validation has passed and we have the parent, store the block
        block_key = f"block:{block_hash}".encode()
        if block_key not in self.db:
            logger.info(f"Storing new block {block_hash} at height {height}")
            self.db.put(block_key, json.dumps(block_data).encode())
            
            # Also store transactions separately for fork blocks
            # This ensures they're available during reorganization
            if "full_transactions" in block_data:
                for tx in block_data["full_transactions"]:
                    if tx and "txid" in tx:
                        tx_key = f"tx:{tx['txid']}".encode()
                        if tx_key not in self.db:
                            self.db.put(tx_key, json.dumps(tx).encode())
                            logger.debug(f"Stored transaction {tx['txid']} from block {block_hash}")
        
        # Add block to index
        self.block_index[block_hash] = {
            "height": height,
            "previous_hash": prev_hash,
            "timestamp": block_data["timestamp"],
            "bits": block_data["bits"],
            "cumulative_difficulty": self._get_cumulative_difficulty(block_hash)
        }
        
        # Check if this creates a new chain tip or extends existing one
        self._update_chain_tips()
        
        # Check if we need to reorganize
        current_tip, current_height = self.get_best_chain_tip()
        
        if block_hash == current_tip:
            # This block became the new best tip
            logger.info(f"New best chain tip: {block_hash} at height {height}")
            
            # Connect the block to process its transactions and create UTXOs
            # Need to create a WriteBatch for the transaction
            batch = WriteBatch()
            self._connect_block(block_data, batch)
            self.db.write(batch)
            
            # Process any orphans that can now be connected
            self._process_orphans_for_block(block_hash)
            
            return True, None
        
        # Check if this block creates a better chain
        if self._should_reorganize(block_hash):
            logger.warning(f"Chain reorganization needed! New tip: {block_hash}")
            success = self._reorganize_to_block(block_hash)
            if not success:
                return False, "Reorganization failed"
        
        return True, None
    
    def _add_orphan(self, block_data: dict):
        """Add a block to the orphan pool"""
        block_hash = block_data["block_hash"]
        logger.info(f"Adding orphan block {block_hash}")
        
        # Clean up old orphans before adding new one
        self._cleanup_orphans()
        
        # Add the new orphan
        self.orphan_blocks[block_hash] = block_data
        self.orphan_timestamps[block_hash] = int(time.time())
        
        # Enforce size limit (remove oldest if over limit)
        if len(self.orphan_blocks) > self.MAX_ORPHAN_BLOCKS:
            # Find oldest orphan
            oldest_hash = min(self.orphan_timestamps.items(), key=lambda x: x[1])[0]
            logger.info(f"Removing oldest orphan {oldest_hash} due to size limit")
            del self.orphan_blocks[oldest_hash]
            del self.orphan_timestamps[oldest_hash]
    
    def _process_orphans_for_block(self, parent_hash: str):
        """Try to connect any orphans that have this block as parent"""
        connected = []
        
        for orphan_hash, orphan_data in self.orphan_blocks.items():
            if orphan_data["previous_hash"] == parent_hash:
                # This orphan can now be connected
                logger.info(f"Connecting orphan {orphan_hash} to parent {parent_hash}")
                success, _ = self.add_block(orphan_data)
                if success:
                    connected.append(orphan_hash)
        
        # Remove connected orphans
        for orphan_hash in connected:
            del self.orphan_blocks[orphan_hash]
            if orphan_hash in self.orphan_timestamps:
                del self.orphan_timestamps[orphan_hash]
    
    def _should_reorganize(self, new_tip_hash: str) -> bool:
        """Check if a new block creates a better chain than current"""
        current_tip, _ = self.get_best_chain_tip()
        
        current_difficulty = self._get_cumulative_difficulty(current_tip)
        new_difficulty = self._get_cumulative_difficulty(new_tip_hash)
        
        return new_difficulty > current_difficulty
    
    def _reorganize_to_block(self, new_tip_hash: str) -> bool:
        """
        Perform chain reorganization to make new_tip_hash the best chain
        This is the critical function for consensus
        """
        logger.warning(f"Starting chain reorganization to {new_tip_hash}")
        
        current_tip, _ = self.get_best_chain_tip()
        
        # Find common ancestor
        common_ancestor = self._find_common_ancestor(current_tip, new_tip_hash)
        if not common_ancestor:
            logger.error("No common ancestor found - cannot reorganize")
            return False
        
        logger.info(f"Common ancestor: {common_ancestor}")
        
        # Get blocks to disconnect (from current chain)
        blocks_to_disconnect = self._get_chain_between(current_tip, common_ancestor)
        
        # Get blocks to connect (from new chain)
        blocks_to_connect = self._get_chain_between(new_tip_hash, common_ancestor)
        blocks_to_connect.reverse()  # Need to apply in forward order
        
        logger.info(f"Disconnecting {len(blocks_to_disconnect)} blocks, connecting {len(blocks_to_connect)} blocks")
        
        # Create backup of current state for rollback
        backup_state = {
            "best_tip": current_tip,
            "height": self.block_index[current_tip]["height"],
            "utxo_backups": {},
            "block_states": {}
        }
        
        # Start database transaction
        batch = WriteBatch()
        
        try:
            # Phase 1: Disconnect blocks from current chain
            for block_hash in blocks_to_disconnect:
                # Backup block state before disconnecting
                block_key = f"block:{block_hash}".encode()
                backup_state["block_states"][block_hash] = self.db.get(block_key)
                
                self._disconnect_block(block_hash, batch, backup_state["utxo_backups"])
            
            # Phase 2: Validate and connect blocks from new chain
            # First, collect all UTXOs that will be spent in new chain
            new_chain_spent_utxos = set()
            for block_hash in blocks_to_connect:
                block_key = f"block:{block_hash}".encode()
                block_data = self.db.get(block_key)
                if not block_data:
                    raise ValueError(f"Block {block_hash} not found during reorg")
                
                block_dict = json.loads(block_data.decode())
                
                # Collect spent UTXOs from this block's transactions
                for tx in self._get_block_transactions(block_dict):
                    for inp in tx.get("inputs", []):
                        if "txid" in inp and inp["txid"] != "00" * 32:  # Skip coinbase
                            utxo_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
                            new_chain_spent_utxos.add(utxo_key)
            
            # Now connect blocks with UTXO tracking
            for block_hash in blocks_to_connect:
                block_key = f"block:{block_hash}".encode()
                block_data = self.db.get(block_key)
                if not block_data:
                    raise ValueError(f"Block {block_hash} not found during reorg")
                
                block_dict = json.loads(block_data.decode())
                
                # Extra safety: Re-validate PoW during reorg (except genesis)
                if block_hash != "0" * 64:
                    try:
                        block_obj = Block(
                            block_dict["version"],
                            block_dict["previous_hash"],
                            block_dict["merkle_root"],
                            block_dict["timestamp"],
                            block_dict["bits"],
                            block_dict["nonce"]
                        )
                        if not validate_pow(block_obj):
                            raise ValueError(f"Block {block_hash} failed PoW validation during reorg!")
                    except Exception as e:
                        raise ValueError(f"Failed to validate block {block_hash} during reorg: {e}")
                
                # Connect with new chain UTXO tracking
                self._connect_block_safe(block_dict, batch, new_chain_spent_utxos)
            
            # Phase 3: Commit the reorganization atomically
            self.db.write(batch)
            
            # Update chain state
            key = b"chain:best_tip"
            self.db.put(key, json.dumps({
                "hash": new_tip_hash,
                "height": self.block_index[new_tip_hash]["height"]
            }).encode())
            
            logger.info(f"Chain reorganization complete. New tip: {new_tip_hash}")
            return True
            
        except Exception as e:
            logger.error(f"Reorganization failed: {e}")
            # Rollback is automatic since we haven't committed the batch
            logger.info("Reorganization rolled back due to error")
            return False
    
    def _find_common_ancestor(self, hash1: str, hash2: str) -> Optional[str]:
        """Find the common ancestor of two blocks"""
        # Get ancestors of both blocks
        ancestors1 = set()
        current = hash1
        while current and current != "00" * 32:
            ancestors1.add(current)
            if current in self.block_index:
                current = self.block_index[current]["previous_hash"]
            else:
                break
        
        # Walk up hash2's chain until we find common ancestor
        current = hash2
        while current and current != "00" * 32:
            if current in ancestors1:
                return current
            if current in self.block_index:
                current = self.block_index[current]["previous_hash"]
            else:
                break
        
        return None
    
    def _get_chain_between(self, tip_hash: str, ancestor_hash: str) -> List[str]:
        """Get all blocks between tip and ancestor (not including ancestor)"""
        blocks = []
        current = tip_hash
        
        while current and current != ancestor_hash and current != "00" * 32:
            blocks.append(current)
            if current in self.block_index:
                current = self.block_index[current]["previous_hash"]
            else:
                break
        
        return blocks
    
    def _get_block_transactions(self, block_dict: dict) -> List[dict]:
        """Get all transactions from a block, loading from DB if necessary"""
        # Use full_transactions if available
        if "full_transactions" in block_dict and block_dict["full_transactions"]:
            return block_dict["full_transactions"]
        
        # Otherwise load from tx_ids
        transactions = []
        for txid in block_dict.get("tx_ids", []):
            tx_key = f"tx:{txid}".encode()
            tx_data = self.db.get(tx_key)
            if tx_data:
                tx = json.loads(tx_data.decode())
                transactions.append(tx)
            else:
                logger.warning(f"Transaction {txid} not found when loading block transactions")
        
        return transactions
    
    def _disconnect_block(self, block_hash: str, batch: WriteBatch, utxo_backups: Dict[str, bytes]):
        """Disconnect a block from the active chain (revert its effects)"""
        logger.info(f"Disconnecting block {block_hash}")
        
        # Load block data
        block_key = f"block:{block_hash}".encode()
        block_data = json.loads(self.db.get(block_key).decode())
        
        # Get full transactions to re-add to mempool
        full_transactions = self._get_block_transactions(block_data)
        
        # Revert all transactions in this block
        for txid in block_data.get("tx_ids", []):
            self._revert_transaction(txid, batch, utxo_backups)
        
        # Re-add non-coinbase transactions back to mempool
        # (they might be valid again after reorg)
        readded_count = 0
        for tx in full_transactions:
            if tx and "txid" in tx and not self.validator._is_coinbase_transaction(tx):
                try:
                    # Try to add back to mempool - it might fail if invalid
                    success, _ = mempool_manager.add_transaction(tx)
                    if success:
                        readded_count += 1
                except Exception as e:
                    logger.debug(f"Could not re-add transaction {tx.get('txid')} to mempool: {e}")
        
        if readded_count > 0:
            logger.info(f"Re-added {readded_count} transactions to mempool after disconnecting block {block_hash}")
        
        # Mark block as disconnected (don't delete - might reconnect later)
        block_data["connected"] = False
        batch.put(block_key, json.dumps(block_data).encode())
        
        # Remove from height index during disconnection
        height_index = get_height_index()
        height_index.remove_block_from_index(block_data["height"])
    
    def _connect_block(self, block_data: dict, batch: WriteBatch):
        """Connect a block to the active chain (apply its effects)"""
        logger.info(f"Connecting block {block_data['block_hash']} at height {block_data['height']}")
        
        # Get full transactions - either from block_data or by loading from DB
        full_transactions = block_data.get("full_transactions", [])
        
        # If full_transactions is empty but we have tx_ids, load the transactions
        if not full_transactions and "tx_ids" in block_data:
            logger.info(f"Loading {len(block_data['tx_ids'])} transactions for block {block_data['block_hash']}")
            full_transactions = []
            for txid in block_data["tx_ids"]:
                tx_key = f"tx:{txid}".encode()
                tx_data = self.db.get(tx_key)
                if tx_data:
                    tx = json.loads(tx_data.decode())
                    full_transactions.append(tx)
                else:
                    logger.warning(f"Transaction {txid} not found in database during block connection")
        
        # Process all transactions in the block
        for tx in full_transactions:
            self._apply_transaction(tx, block_data["height"], batch)
        
        # Remove mined transactions from mempool
        # Skip coinbase (first transaction) as it's not in mempool
        removed_count = 0
        for tx in full_transactions:
            if tx and "txid" in tx and not self.validator._is_coinbase_transaction(tx):
                if mempool_manager.get_transaction(tx["txid"]) is not None:
                    mempool_manager.remove_transaction(tx["txid"])
                    removed_count += 1
        
        if removed_count > 0:
            logger.info(f"Removed {removed_count} mined transactions from mempool after connecting block {block_data['block_hash']}")
        
        # Mark block as connected
        block_data["connected"] = True
        block_key = f"block:{block_data['block_hash']}".encode()
        batch.put(block_key, json.dumps(block_data).encode())
        
        # Update the height index
        height_index = get_height_index()
        height_index.add_block_to_index(block_data["height"], block_data["block_hash"])
    
    def _connect_block_safe(self, block_data: dict, batch: WriteBatch, new_chain_spent_utxos: Set[str]):
        """
        Connect a block during reorganization with double-spend protection
        Ensures UTXOs aren't restored if they're spent elsewhere in new chain
        """
        logger.info(f"Safely connecting block {block_data['block_hash']} at height {block_data['height']}")
        
        # Get full transactions
        full_transactions = self._get_block_transactions(block_data)
        
        # Track UTXOs spent within this block to prevent double-spending within same block
        block_spent_utxos = set()
        
        # Track fees for coinbase validation
        total_fees = Decimal("0")
        coinbase_tx = None
        
        # Process all transactions in the block with validation
        for tx in full_transactions:
            if tx is None:
                continue
            
            # Check if this is coinbase
            if self.validator._is_coinbase_transaction(tx):
                coinbase_tx = tx
                continue  # Validate coinbase after we know total fees
                
            # Validate transaction before applying
            if not self._validate_transaction_for_reorg(tx, block_spent_utxos, new_chain_spent_utxos):
                raise ValueError(f"Invalid transaction {tx.get('txid')} during reorganization")
            
            # Calculate transaction fee
            total_input = Decimal("0")
            total_output = Decimal("0")
            
            for inp in tx.get("inputs", []):
                if "txid" in inp and inp["txid"] != "00" * 32:
                    utxo_key = f"utxo:{inp['txid']}:{inp.get('utxo_index', 0)}".encode()
                    utxo_data = self.db.get(utxo_key)
                    if utxo_data:
                        utxo = json.loads(utxo_data.decode())
                        total_input += Decimal(utxo.get("amount", "0"))
            
            for out in tx.get("outputs", []):
                total_output += Decimal(out.get("amount", "0"))
            
            if total_input > total_output:
                total_fees += (total_input - total_output)
        
        # Validate coinbase transaction if present
        if coinbase_tx and block_data["height"] > 0:
            is_valid, error_msg = self.validator.validate_coinbase_transaction(
                coinbase_tx, block_data["height"], total_fees
            )
            if not is_valid:
                raise ValueError(f"Invalid coinbase during reorganization: {error_msg}")
        
        # Now apply all transactions (including coinbase)
        for tx in full_transactions:
            if tx is None:
                continue
                
            # Skip re-validation for non-coinbase (already validated above)
            if not self.validator._is_coinbase_transaction(tx):
                # Validate transaction before applying (redundant but safe)
                if not self._validate_transaction_for_reorg(tx, block_spent_utxos, new_chain_spent_utxos):
                    raise ValueError(f"Invalid transaction {tx.get('txid')} during reorganization")
            
            # Apply transaction
            self._apply_transaction_safe(tx, block_data["height"], batch, new_chain_spent_utxos)
            
            # Track spent UTXOs from this transaction
            for inp in tx.get("inputs", []):
                if "txid" in inp and inp["txid"] != "00" * 32:
                    utxo_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
                    block_spent_utxos.add(utxo_key)
        
        # Remove mined transactions from mempool during reorganization
        removed_count = 0
        for tx in full_transactions:
            if tx and "txid" in tx and not self.validator._is_coinbase_transaction(tx):
                if mempool_manager.get_transaction(tx["txid"]) is not None:
                    mempool_manager.remove_transaction(tx["txid"])
                    removed_count += 1
        
        if removed_count > 0:
            logger.info(f"Removed {removed_count} mined transactions from mempool during reorg for block {block_data['block_hash']}")
        
        # Mark block as connected
        block_data["connected"] = True
        block_key = f"block:{block_data['block_hash']}".encode()
        batch.put(block_key, json.dumps(block_data).encode())
        
        # Update the height index
        height_index = get_height_index()
        height_index.add_block_to_index(block_data["height"], block_data["block_hash"])
    
    def _revert_transaction(self, txid: str, batch: WriteBatch, utxo_backups: Dict[str, bytes] = None):
        """Revert a transaction's effects on the UTXO set"""
        logger.debug(f"Reverting transaction {txid}")
        
        if utxo_backups is None:
            utxo_backups = {}
        
        # Mark all outputs from this transaction as invalid
        tx_key = f"tx:{txid}".encode()
        tx_data = self.db.get(tx_key)
        if not tx_data:
            return
        
        tx = json.loads(tx_data.decode())
        
        # Restore spent inputs - but backup current state first
        for inp in tx.get("inputs", []):
            if "txid" in inp and inp["txid"] != "00" * 32:  # Skip coinbase
                utxo_key = f"utxo:{inp['txid']}:{inp.get('utxo_index', 0)}".encode()
                
                # Backup current state before modifying
                current_utxo_data = self.db.get(utxo_key)
                if current_utxo_data:
                    backup_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
                    utxo_backups[backup_key] = current_utxo_data
                    
                    utxo = json.loads(current_utxo_data.decode())
                    utxo["spent"] = False
                    batch.put(utxo_key, json.dumps(utxo).encode())
        
        # Remove outputs created by this transaction
        for idx, out in enumerate(tx.get("outputs", [])):
            utxo_key = f"utxo:{txid}:{idx}".encode()
            
            # Backup before deleting
            current_data = self.db.get(utxo_key)
            if current_data:
                backup_key = f"{txid}:{idx}"
                utxo_backups[backup_key] = current_data
            
            batch.delete(utxo_key)
    
    def _apply_transaction(self, tx: dict, height: int, batch: WriteBatch):
        """Apply a transaction's effects on the UTXO set"""
        if tx is None:
            return
            
        
        # Check if this is a coinbase transaction
        is_coinbase = self.validator._is_coinbase_transaction(tx)
        
        # Get transaction ID
        txid = tx.get("txid")
        if not txid:
            logger.error(f"Transaction without txid at height {height}")
            return
        
        logger.debug(f"Applying transaction {txid}")
        
        # Mark inputs as spent (skip for coinbase)
        if not is_coinbase:
            for inp in tx.get("inputs", []):
                if "txid" in inp and inp["txid"] != "00" * 32:
                    utxo_key = f"utxo:{inp['txid']}:{inp.get('utxo_index', 0)}".encode()
                    utxo_data = self.db.get(utxo_key)
                    if utxo_data:
                        utxo = json.loads(utxo_data.decode())
                        utxo["spent"] = True
                        batch.put(utxo_key, json.dumps(utxo).encode())
        
        # Create new UTXOs (including for coinbase!)
        for idx, out in enumerate(tx.get("outputs", [])):
            # Create proper UTXO record with all necessary fields
            utxo_record = {
                "txid": txid,
                "utxo_index": idx,
                "sender": "coinbase" if is_coinbase else out.get('sender', ''),
                "receiver": out.get('receiver', ''),
                "amount": str(out.get('amount', '0')),  # Ensure string to avoid scientific notation
                "spent": False  # New UTXOs are always unspent
            }
            utxo_key = f"utxo:{txid}:{idx}".encode()
            batch.put(utxo_key, json.dumps(utxo_record).encode())
            
            if is_coinbase:
                logger.info(f"Created coinbase UTXO: {utxo_key.decode()} for {out.get('receiver')} amount: {out.get('amount')}")
        
        # Store transaction
        batch.put(f"tx:{txid}".encode(), json.dumps(tx).encode())
    
    def _validate_transaction_for_reorg(self, tx: dict, block_spent_utxos: Set[str], 
                                       new_chain_spent_utxos: Set[str]) -> bool:
        """
        Validate a transaction during reorganization
        Checks signatures, balances, and double-spending
        """
        if not tx or "txid" not in tx:
            logger.error("Invalid transaction format - missing txid")
            return False
        
        txid = tx["txid"]
        
        # Skip coinbase transactions (they have special rules)
        if len(tx.get("inputs", [])) == 1 and tx["inputs"][0].get("txid") == "00" * 32:
            logger.debug(f"Skipping validation for coinbase transaction {txid}")
            return True
        
        # Validate all inputs exist and aren't double-spent
        total_input = Decimal(0)
        for inp in tx.get("inputs", []):
            if "txid" not in inp:
                logger.error(f"Transaction {txid} has invalid input - missing txid")
                return False
            
            # Check if this UTXO is already spent in this block
            utxo_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
            if utxo_key in block_spent_utxos:
                logger.error(f"Double-spend detected: UTXO {utxo_key} already spent in block")
                return False
            
            # Check if this UTXO is spent elsewhere in the new chain
            if utxo_key in new_chain_spent_utxos:
                logger.error(f"Double-spend detected: UTXO {utxo_key} spent in new chain")
                return False
            
            # Verify UTXO exists and get amount
            utxo_db_key = f"utxo:{utxo_key}".encode()
            utxo_data = self.db.get(utxo_db_key)
            if not utxo_data:
                logger.error(f"Transaction {txid} references non-existent UTXO {utxo_key}")
                return False
            
            utxo = json.loads(utxo_data.decode())
            if utxo.get("spent", False):
                logger.error(f"Transaction {txid} tries to spend already spent UTXO {utxo_key}")
                return False
            
            total_input += Decimal(utxo.get("amount", "0"))
        
        # Validate outputs sum to inputs (allowing for fees)
        total_output = Decimal(0)
        for out in tx.get("outputs", []):
            if "amount" not in out:
                logger.error(f"Transaction {txid} has output without amount")
                return False
            total_output += Decimal(out["amount"])
        
        if total_output > total_input:
            logger.error(f"Transaction {txid} outputs ({total_output}) exceed inputs ({total_input})")
            return False
        
        # CRITICAL: Verify transaction signature during reorg
        # This prevents invalid transactions from being accepted during chain reorganization
        
        # Get transaction body for signature verification
        body = tx.get("body")
        if not body:
            logger.error(f"Transaction {txid} missing body")
            return False
        
        msg_str = body.get("msg_str", "")
        signature = body.get("signature", "")
        pubkey = body.get("pubkey", "")
        
        # Parse message string to validate chain ID and timestamp
        if msg_str:  # Skip for coinbase which has no msg_str
            parts = msg_str.split(":")
            if len(parts) == 5:
                from_, to_, amount_str, time_str, tx_chain_id = parts
                
                # Validate chain ID (replay protection)
                try:
                    from config.config import CHAIN_ID
                    if int(tx_chain_id) != CHAIN_ID:
                        logger.error(f"Invalid chain ID in tx {txid} during reorg: expected {CHAIN_ID}, got {tx_chain_id}")
                        return False
                except (ValueError, ImportError) as e:
                    logger.error(f"Chain ID validation error in tx {txid}: {e}")
                    return False
                
                # Validate timestamp
                try:
                    from config.config import TX_EXPIRATION_TIME
                    tx_timestamp = int(time_str)
                    current_time = int(time.time() * 1000)
                    tx_age = (current_time - tx_timestamp) / 1000
                    
                    if tx_age > TX_EXPIRATION_TIME:
                        logger.error(f"Transaction {txid} expired during reorg: age {tx_age}s > max {TX_EXPIRATION_TIME}s")
                        return False
                except (ValueError, ImportError) as e:
                    logger.error(f"Timestamp validation error in tx {txid}: {e}")
                    return False
                
                # Verify signature
                from wallet.wallet import verify_transaction
                if not verify_transaction(msg_str, signature, pubkey):
                    logger.error(f"Signature verification failed for tx {txid} during reorg")
                    return False
        
        return True
    
    def _apply_transaction_safe(self, tx: dict, height: int, batch: WriteBatch, 
                               new_chain_spent_utxos: Set[str]):
        """
        Apply a transaction during reorganization with double-spend protection
        """
        if tx is None:
            return
            
        
        # Check if this is a coinbase transaction
        is_coinbase = self.validator._is_coinbase_transaction(tx)
        
        # Get transaction ID
        txid = tx.get("txid")
        if not txid:
            logger.error(f"Transaction without txid at height {height}")
            return
        
        logger.debug(f"Safely applying transaction {txid}")
        
        # Mark inputs as spent (skip if already spent in new chain)
        for inp in tx.get("inputs", []):
            if "txid" in inp and inp["txid"] != "00" * 32:
                utxo_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
                
                # Skip if this UTXO is already marked as spent in new chain
                if utxo_key not in new_chain_spent_utxos:
                    utxo_db_key = f"utxo:{utxo_key}".encode()
                    utxo_data = self.db.get(utxo_db_key)
                    if utxo_data:
                        utxo = json.loads(utxo_data.decode())
                        utxo["spent"] = True
                        batch.put(utxo_db_key, json.dumps(utxo).encode())
        
        # Create new UTXOs (including for coinbase!)
        for idx, out in enumerate(tx.get("outputs", [])):
            # Create proper UTXO record with all necessary fields
            utxo_record = {
                "txid": txid,
                "utxo_index": idx,
                "sender": "coinbase" if is_coinbase else out.get('sender', ''),
                "receiver": out.get('receiver', ''),
                "amount": str(out.get('amount', '0')),  # Ensure string to avoid scientific notation
                "spent": False  # New UTXOs are always unspent
            }
            utxo_key = f"utxo:{txid}:{idx}".encode()
            batch.put(utxo_key, json.dumps(utxo_record).encode())
            
            if is_coinbase:
                logger.info(f"Created coinbase UTXO during reorg: {utxo_key.decode()} for {out.get('receiver')} amount: {out.get('amount')}")
        
        # Store transaction
        batch.put(f"tx:{txid}".encode(), json.dumps(tx).encode())
    
    def get_block_by_hash(self, block_hash: str) -> Optional[dict]:
        """Get a block by its hash"""
        block_key = f"block:{block_hash}".encode()
        block_data = self.db.get(block_key)
        if block_data:
            return json.loads(block_data.decode())
        return None
    
    def is_block_in_main_chain(self, block_hash: str) -> bool:
        """Check if a block is in the main chain"""
        current_tip, _ = self.get_best_chain_tip()
        
        # Walk back from tip to see if we find this block
        current = current_tip
        while current and current != "00" * 32:
            if current == block_hash:
                return True
            if current in self.block_index:
                current = self.block_index[current]["previous_hash"]
            else:
                break
        
        return False
    
    def _cleanup_orphans(self):
        """Remove orphans that are too old"""
        current_time = int(time.time())
        to_remove = []
        
        for orphan_hash, timestamp in self.orphan_timestamps.items():
            age = current_time - timestamp
            if age > self.MAX_ORPHAN_AGE:
                logger.info(f"Removing orphan {orphan_hash} due to age ({age}s)")
                to_remove.append(orphan_hash)
        
        for orphan_hash in to_remove:
            del self.orphan_blocks[orphan_hash]
            del self.orphan_timestamps[orphan_hash]
    
    def get_orphan_info(self) -> dict:
        """Get information about current orphan blocks"""
        current_time = int(time.time())
        orphans = []
        
        for orphan_hash, orphan_data in self.orphan_blocks.items():
            timestamp = self.orphan_timestamps.get(orphan_hash, 0)
            age = current_time - timestamp
            
            orphans.append({
                "hash": orphan_hash,
                "height": orphan_data.get("height", 0),
                "parent": orphan_data.get("previous_hash", ""),
                "age_seconds": age
            })
        
        # Sort by height (ascending) for better readability
        orphans.sort(key=lambda x: x["height"])
        
        return {
            "count": len(self.orphan_blocks),
            "max_orphans": self.MAX_ORPHAN_BLOCKS,
            "max_age_seconds": self.MAX_ORPHAN_AGE,
            "orphans": orphans
        }