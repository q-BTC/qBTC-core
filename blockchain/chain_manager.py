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
from database.database import get_db, invalidate_height_cache
from blockchain.blockchain import Block, validate_pow, bits_to_target, sha256d
from blockchain.difficulty import get_next_bits, validate_block_bits, validate_block_timestamp, get_median_time_past, MAX_FUTURE_TIME, MAX_TARGET_BITS
from config.config import MAX_REORG_DEPTH
from blockchain.transaction_validator import TransactionValidator
from blockchain.block_factory import normalize_block, validate_block_structure
from rocksdict import WriteBatch
from state.state import mempool_manager
from blockchain.block_height_index import get_height_index
from blockchain.redis_cache import BlockchainRedisCache
import os
import asyncio
import threading

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
        self._check_and_recover_from_incomplete_reorg()
        self.orphan_chains: Dict[str, List[str]] = {}  # tip_hash -> list of block hashes in chain
        self.orphan_roots: Dict[str, str] = {}  # block_hash -> root block hash of orphan chain
        self.MAX_ORPHAN_BLOCKS = 100  # Maximum number of orphan blocks to keep
        self.MAX_ORPHAN_AGE = 3600  # Maximum age of orphan blocks in seconds (1 hour)
        self.is_syncing = False  # Flag to indicate if we're in initial sync
        
        # Initialize Redis cache if available
        redis_url = os.getenv("REDIS_URL", None)
        self.redis_cache = BlockchainRedisCache(redis_url) if redis_url and os.getenv("USE_REDIS", "true").lower() == "true" else None
        
        # Note: _initialize_index() must be called separately after initialization
        # as it's now an async method. Call await chain_manager.initialize() after creating instance.
    
    def _check_and_recover_from_incomplete_reorg(self):
        """Check for and recover from incomplete reorganization on startup"""
        try:
            # Check for reorganization marker
            reorg_marker = self.db.get(b"chain:last_reorg")
            if reorg_marker:
                reorg_data = json.loads(reorg_marker.decode())
                logger.warning(f"Found incomplete reorganization marker: {reorg_data}")
                
                # Check if current tip matches expected result
                current_tip_data = self.db.get(b"chain:best_tip")
                if current_tip_data:
                    current_tip = json.loads(current_tip_data.decode())
                    
                    # If tip doesn't match the expected reorg result, we may have crashed mid-reorg
                    if current_tip["hash"] != reorg_data["to_tip"]:
                        logger.critical(f"CRITICAL: Detected incomplete reorganization! Current tip: {current_tip['hash']}, Expected: {reorg_data['to_tip']}")
                        
                        # Verify blockchain consistency
                        # This is a simplified check - in production, you'd want more comprehensive validation
                        try:
                            # Try to validate the current tip
                            block_key = f"block:{current_tip['hash']}".encode()
                            block_data = self.db.get(block_key)
                            if not block_data:
                                logger.critical(f"Current tip block {current_tip['hash']} not found in database!")
                                # Attempt to revert to the original tip
                                if reorg_data.get("from_tip"):
                                    logger.info(f"Attempting to revert to original tip: {reorg_data['from_tip']}")
                                    batch = WriteBatch()
                                    batch.put(b"chain:best_tip", json.dumps({
                                        "hash": reorg_data["from_tip"],
                                        "height": reorg_data.get("from_height", 0)
                                    }).encode())
                                    self.db.write(batch)
                        except Exception as e:
                            logger.error(f"Failed to validate current chain state: {e}")
                    else:
                        logger.info("Reorganization appears to have completed successfully")
                        # Clear the marker atomically
                        cleanup_batch = WriteBatch()
                        cleanup_batch.delete(b"chain:last_reorg")
                        self.db.write(cleanup_batch)
        except Exception as e:
            logger.error(f"Error checking for incomplete reorganization: {e}")
    
    async def initialize(self):
        """Initialize the chain manager - must be called after __init__"""
        await self._initialize_index()
    
    async def _initialize_index(self):
        """Build in-memory index of all blocks in database"""
        logger.info("Initializing chain index...")
        start_time = time.time()
        
        # Try to load from Redis cache first
        if self.redis_cache:
            try:
                cached_index = self.redis_cache.get_chain_index()
                if cached_index and isinstance(cached_index, dict):
                    # Validate cache structure
                    block_index = cached_index.get("block_index", {})
                    chain_tips = cached_index.get("chain_tips", [])
                    
                    if isinstance(block_index, dict) and isinstance(chain_tips, list):
                        # Clean the cached index - remove blocks that don't exist in database
                        cleaned_index = {}
                        for block_hash, block_info in block_index.items():
                            block_key = f"block:{block_hash}".encode()
                            if self.db.get(block_key):
                                cleaned_index[block_hash] = block_info
                            else:
                                logger.warning(f"Removing orphaned block {block_hash[:16]}... at height {block_info.get('height', '?')} from index (not in database)")

                        self.block_index = cleaned_index
                        # Update chain tips based on cleaned index
                        self._update_chain_tips()
                        elapsed = time.time() - start_time
                        logger.info(f"Chain index loaded from Redis cache in {elapsed:.2f}s with {len(self.block_index)} blocks and {len(self.chain_tips)} tips")

                        # Re-cache the cleaned index
                        if len(cleaned_index) != len(block_index):
                            logger.info(f"Re-caching cleaned index (removed {len(block_index) - len(cleaned_index)} orphaned blocks)")
                            cache_data = {
                                "block_index": self.block_index,
                                "chain_tips": list(self.chain_tips)
                            }
                            try:
                                self.redis_cache.set_chain_index(cache_data)
                            except Exception as e:
                                logger.warning(f"Failed to re-cache cleaned index: {e}")
                        return
                    else:
                        logger.warning("Invalid cache structure, rebuilding from database")
            except Exception as e:
                logger.warning(f"Failed to load chain index from Redis: {e}")
        
        # Build index from database
        logger.info("Building chain index from database (this may take a while)...")
        block_count = 0
        cumulative_difficulties = {}

        # Load all blocks into index
        for key, value in self.db.items():
            if key.startswith(b"block:"):
                block_data = json.loads(value.decode())
                block_hash = block_data["block_hash"]

                # Try to get cumulative difficulty from block data first
                if "cumulative_difficulty" in block_data:
                    cumulative_difficulty = Decimal(block_data["cumulative_difficulty"])
                    # Always add to cumulative_difficulties for caching
                    cumulative_difficulties[block_hash] = cumulative_difficulty
                else:
                    # Try Redis cache
                    cached_difficulty = None
                    if self.redis_cache:
                        try:
                            cached_difficulty = self.redis_cache.get_cumulative_difficulty(block_hash)
                        except:
                            pass

                    if cached_difficulty:
                        cumulative_difficulty = Decimal(cached_difficulty)
                        # Add to cumulative_difficulties for caching
                        cumulative_difficulties[block_hash] = cumulative_difficulty
                    else:
                        # Fall back to calculating (for old blocks without stored difficulty)
                        cumulative_difficulty = await self._get_cumulative_difficulty(block_hash)
                        cumulative_difficulties[block_hash] = cumulative_difficulty

                self.block_index[block_hash] = {
                    "height": block_data["height"],
                    "previous_hash": block_data["previous_hash"],
                    "timestamp": block_data["timestamp"],
                    "bits": block_data["bits"],
                    "cumulative_difficulty": cumulative_difficulty
                }

                block_count += 1
                if block_count % 1000 == 0:
                    logger.info(f"Processed {block_count} blocks...")
        
        # Find all chain tips (blocks with no children)
        self._update_chain_tips()
        
        # Cache the results
        if self.redis_cache:
            try:
                # Cache the chain index
                cache_data = {
                    "block_index": self.block_index,
                    "chain_tips": list(self.chain_tips)
                }
                self.redis_cache.set_chain_index(cache_data)
                
                # Cache cumulative difficulties individually
                if cumulative_difficulties:
                    logger.info(f"Caching {len(cumulative_difficulties)} cumulative difficulties to Redis")
                    for block_hash, difficulty in cumulative_difficulties.items():
                        self.redis_cache.set_cumulative_difficulty(block_hash, difficulty)
                else:
                    logger.info("No cumulative difficulties to cache")

                logger.info("Chain index cached to Redis")
            except Exception as e:
                logger.warning(f"Failed to cache chain index: {e}")
        
        elapsed = time.time() - start_time
        logger.info(f"Chain index initialized in {elapsed:.2f}s with {len(self.block_index)} blocks and {len(self.chain_tips)} tips")
        
        # Initialize chain:best_tip if not already set
        tip_key = b"chain:best_tip"
        if not self.db.get(tip_key):
            # Find and set the best chain tip
            best_tip, best_height = await self.get_best_chain_tip()
            if best_tip:
                batch = WriteBatch()
                batch.put(tip_key, json.dumps({
                    "hash": best_tip,
                    "height": best_height
                }).encode())
                self.db.write(batch)
                logger.info(f"Initialized chain:best_tip to {best_tip} at height {best_height}")
    
    async def _is_block_connected(self, block_hash: str) -> bool:
        """
        Check if a block is connected to the genesis block.
        Returns True if the block has a valid chain back to genesis.
        """
        if not block_hash or block_hash == "00" * 32:
            return False

        # Genesis block itself is always connected
        if block_hash == "0" * 64:
            return True

        current = block_hash
        visited = set()

        while current and current != "00" * 32:
            # Avoid infinite loops
            if current in visited:
                logger.warning(f"Cycle detected in chain at block {current[:16]}...")
                return False
            visited.add(current)

            # Check if block exists in index
            if current not in self.block_index:
                # Check if it's in the database
                block_key = f"block:{current}".encode()
                if not self.db.get(block_key):
                    logger.debug(f"Block {current[:16]}... not found in chain")
                    return False
                # Block exists in DB but not in index - this shouldn't happen
                logger.warning(f"Block {current[:16]}... in DB but not in index")
                return False

            # Get parent
            block_info = self.block_index[current]
            prev_hash = block_info.get("previous_hash")

            # Check if we reached genesis
            if prev_hash == "00" * 32:
                # This block's parent is genesis, so it's connected
                return True

            # Move to parent
            current = prev_hash

        # If we exit the loop without finding genesis, it's not connected
        return False

    def _update_chain_tips(self, pending_block_hash=None):
        """Update the set of chain tips - only includes blocks that exist in database

        Args:
            pending_block_hash: Optional hash of a block that's about to be written to DB
                               This block should be considered as existing for tip calculation
        """
        # Build a set of all blocks that have children (these cannot be tips)
        # This is O(n) instead of O(n²)
        blocks_with_children = set()

        # First pass: find all blocks that are parents of other blocks
        for block_hash, block_info in self.block_index.items():
            # Check if this block exists in database or is pending
            if block_hash == pending_block_hash:
                # This block is about to be written, consider it as existing
                prev_hash = block_info.get("previous_hash")
                if prev_hash and prev_hash != "0" * 64:
                    blocks_with_children.add(prev_hash)
            else:
                block_key = f"block:{block_hash}".encode()
                if self.db.get(block_key):
                    # This block exists, so its parent has a child
                    prev_hash = block_info.get("previous_hash")
                    if prev_hash and prev_hash != "0" * 64:
                        blocks_with_children.add(prev_hash)

        # Second pass: identify tips (blocks that exist but have no children)
        potential_tips = set()

        for block_hash in self.block_index.keys():
            # Skip genesis block (it's never a chain tip)
            if block_hash == "0" * 64:
                continue

            # Check if block exists in database
            if block_hash == pending_block_hash or self.db.get(f"block:{block_hash}".encode()):
                # Block exists and has no children = it's a tip
                if block_hash not in blocks_with_children:
                    potential_tips.add(block_hash)

        old_tips = self.chain_tips.copy() if hasattr(self, 'chain_tips') else set()
        self.chain_tips = potential_tips

        # Log changes in chain tips
        if old_tips != self.chain_tips:
            logger.info(f"Chain tips updated: {len(self.chain_tips)} tips")
            for tip in self.chain_tips:
                if tip in self.block_index:
                    info = self.block_index[tip]
                    logger.info(f"  Tip: {tip[:16]}... height={info['height']} difficulty={info.get('cumulative_difficulty', 'unknown')}")

    def _update_chain_tips_incremental(self, new_block_hash, parent_hash):
        """Incrementally update chain tips when adding a single new block.
        This is O(1) instead of O(n) for the full recalculation.

        Args:
            new_block_hash: The hash of the newly added block
            parent_hash: The parent hash of the new block
        """
        if not hasattr(self, 'chain_tips'):
            self.chain_tips = set()

        # The new block becomes a tip
        self.chain_tips.add(new_block_hash)

        # Its parent is no longer a tip (if it was one)
        if parent_hash in self.chain_tips and parent_hash != "0" * 64:
            self.chain_tips.discard(parent_hash)
            logger.debug(f"Removed {parent_hash[:16]}... from tips (now has child {new_block_hash[:16]}...)")

        logger.info(f"Chain tips incrementally updated: {len(self.chain_tips)} tips")
        if new_block_hash in self.block_index:
            info = self.block_index[new_block_hash]
            logger.info(f"  New tip: {new_block_hash[:16]}... height={info['height']}")

    async def _get_cumulative_difficulty(self, block_hash: str) -> Decimal:
        """Calculate cumulative difficulty from genesis to this block"""
        # First check if the block itself has stored cumulative difficulty
        block_key = f"block:{block_hash}".encode()
        block_data = self.db.get(block_key)
        if block_data:
            block_info = json.loads(block_data.decode())
            if "cumulative_difficulty" in block_info:
                return Decimal(block_info["cumulative_difficulty"])
        
        # Fall back to calculating it by traversing the chain
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
            
            # Check if this block has stored cumulative difficulty
            if "cumulative_difficulty" in block_info:
                # We found a block with stored difficulty, use it and stop
                return Decimal(block_info["cumulative_difficulty"])
            
            # Add this block's difficulty
            bits = block_info.get("bits", MAX_TARGET_BITS)  # Default to min difficulty
            target = bits_to_target(bits)
            difficulty = Decimal(2**256) / Decimal(target)
            cumulative += difficulty
            
            current_hash = block_info.get("previous_hash")
        
        return cumulative
    
    async def _get_cumulative_difficulty_for_new_block(self, parent_hash: str, bits: int) -> Decimal:
        """Calculate cumulative difficulty for a new block based on its parent"""
        # Get parent's cumulative difficulty
        parent_cumulative = Decimal(0)
        
        if parent_hash:  # If there's a parent (even if it's genesis)
            # First check if parent is in our index
            if parent_hash in self.block_index:
                parent_cumulative = Decimal(str(self.block_index[parent_hash]["cumulative_difficulty"]))
                logger.debug(f"Found parent {parent_hash[:16]} in index with cumulative difficulty {parent_cumulative}")
            else:
                # Load parent from database
                parent_key = f"block:{parent_hash}".encode()
                parent_data = self.db.get(parent_key)
                if not parent_data:
                    # This can happen in race conditions where parent is in index but not yet in DB
                    # Fall back to calculating from scratch
                    logger.warning(f"Parent block {parent_hash} not in database, calculating cumulative difficulty from scratch")
                    # Calculate parent's cumulative difficulty first
                    parent_cumulative = await self._get_cumulative_difficulty(parent_hash)
                    # Then add this block's difficulty
                    target = bits_to_target(bits)
                    block_difficulty = Decimal(2**256) / Decimal(target)
                    return parent_cumulative + block_difficulty
                
                parent_block = json.loads(parent_data.decode())
                # Check if parent has stored cumulative difficulty
                if "cumulative_difficulty" in parent_block:
                    parent_cumulative = Decimal(parent_block["cumulative_difficulty"])
                else:
                    # Fall back to calculating it (for old blocks)
                    parent_cumulative = await self._get_cumulative_difficulty(parent_hash)
        
        # Calculate this block's difficulty contribution
        target = bits_to_target(bits)
        block_difficulty = Decimal(2**256) / Decimal(target)
        
        return parent_cumulative + block_difficulty
    
    async def get_best_chain_tip(self) -> Tuple[str, int]:
        """
        Get the best chain tip (highest cumulative difficulty)
        Returns (block_hash, height)
        """
        # Try Redis cache first
        if self.redis_cache:
            try:
                cached_best = self.redis_cache.get_best_chain_info()
                if cached_best:
                    # Verify the cached tip is actually connected
                    if await self._is_block_connected(cached_best["block_hash"]):
                        return cached_best["block_hash"], cached_best["height"]
                    else:
                        logger.warning(f"Cached best chain tip {cached_best['block_hash'][:16]}... is not connected!")
            except Exception as e:
                logger.debug(f"Failed to get best chain from Redis: {e}")

        # Calculate best chain tip - only from CONNECTED blocks
        best_tip = None
        best_difficulty = Decimal(0)
        best_height = 0

        logger.debug(f"Calculating best chain tip from {len(self.chain_tips)} tips")

        for tip_hash in self.chain_tips:
            tip_info = self.block_index.get(tip_hash)
            if not tip_info:
                logger.warning(f"Tip {tip_hash} not found in block index!")
                continue

            # Check if this tip is actually connected to genesis
            if not await self._is_block_connected(tip_hash):
                logger.debug(f"Tip {tip_hash[:16]}... at height {tip_info['height']} is orphaned, skipping")
                continue

            difficulty = tip_info.get("cumulative_difficulty")
            logger.debug(f"Connected tip {tip_hash[:16]}... height={tip_info['height']} difficulty={difficulty}")

            # Convert difficulty to Decimal if it's a string
            if difficulty:
                if isinstance(difficulty, str):
                    difficulty = Decimal(difficulty)

            if difficulty and difficulty > best_difficulty:
                best_difficulty = difficulty
                best_tip = tip_hash
                best_height = tip_info["height"]

        if not best_tip:
            # No connected tips found, return -1 for empty blockchain
            # Return empty string to avoid confusion with genesis hash "00" * 32
            logger.warning("No valid connected chain tips found, returning empty blockchain state")
            return "", -1
        
        logger.debug(f"Best chain tip: {best_tip[:16]}... at height {best_height} with difficulty {best_difficulty}")
        
        # Cache the result
        if self.redis_cache:
            try:
                self.redis_cache.set_best_chain_info({
                    "block_hash": best_tip,
                    "height": best_height,
                    "difficulty": str(best_difficulty)
                })
            except Exception as e:
                logger.debug(f"Failed to cache best chain: {e}")
            
        return best_tip, best_height
    
    # Removed get_best_chain_tip_sync - everyone should use the async version
    
    def set_sync_mode(self, syncing: bool):
        """Set whether we're in initial sync mode"""
        self.is_syncing = syncing
        logger.info(f"Sync mode set to: {syncing}")
    
    def get_best_chain_tip_sync(self) -> Tuple[str, int]:
        """
        Synchronous version of get_best_chain_tip for testing
        Returns (block_hash, height)
        """
        # Calculate best chain tip
        best_tip = None
        best_difficulty = Decimal(0)
        best_height = 0
        
        for tip_hash in self.chain_tips:
            tip_info = self.block_index.get(tip_hash)
            if not tip_info:
                continue
                
            difficulty = tip_info.get("cumulative_difficulty", Decimal(0))
            if isinstance(difficulty, str):
                difficulty = Decimal(difficulty)
            
            if difficulty > best_difficulty:
                best_difficulty = difficulty
                best_tip = tip_hash
                best_height = tip_info["height"]
        
        return best_tip, best_height
    
    async def add_block(self, block_data: dict, pre_validated_batch: WriteBatch = None) -> Tuple[bool, Optional[str]]:
        """
        Add a new block to the chain
        Returns (success, error_message)
        
        Args:
            block_data: The block data including full_transactions
            pre_validated_batch: Optional batch with pre-computed UTXOs and transactions
                               (used by RPC to make block acceptance atomic)
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
        
        # Check block size limit (transaction count)
        from config.config import MAX_TRANSACTIONS_PER_BLOCK
        tx_count = len(block_data.get("tx_ids", []))
        if tx_count > 1:  # Only check if block has more than just coinbase
            # Subtract 1 for coinbase transaction (which is always allowed)
            non_coinbase_tx_count = tx_count - 1
            if non_coinbase_tx_count > MAX_TRANSACTIONS_PER_BLOCK:
                logger.error(f"Block {block_hash} exceeds maximum transaction limit")
                logger.error(f"Block has {non_coinbase_tx_count} non-coinbase transactions, max allowed: {MAX_TRANSACTIONS_PER_BLOCK}")
                return False, f"Block exceeds maximum transaction limit: {non_coinbase_tx_count} > {MAX_TRANSACTIONS_PER_BLOCK}"
        
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
        
        # Validate block hash matches the actual hash (except for genesis)
        if not is_genesis:
            # Calculate the actual block hash using the Block class for consistency
            block_obj = Block(
                block_data['version'],
                block_data['previous_hash'],
                block_data['merkle_root'],
                block_data['timestamp'],
                block_data['bits'],
                block_data['nonce']
            )
            calculated_hash = block_obj.hash()
            
            if calculated_hash != block_hash:
                logger.error(f"[CHAIN_MANAGER] Block hash mismatch!")
                logger.error(f"[CHAIN_MANAGER] Claimed hash: {block_hash}")
                logger.error(f"[CHAIN_MANAGER] Calculated hash: {calculated_hash}")
                return False, f"Invalid block hash: claimed {block_hash}, actual {calculated_hash}"
        
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
                
                try:
                    expected_bits = get_next_bits(self.db, parent_height)
                except ValueError as e:
                    # Can't determine expected difficulty - reject block
                    logger.error(f"Cannot validate block {block_hash}: {str(e)}")
                    return False, f"Cannot determine expected difficulty: {str(e)}"
                
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

            # Compute Median Time Past (MTP) for enhanced timestamp security
            mtp = get_median_time_past(height, self.block_index, self.db)

            # During sync, we can't use current time for historical blocks
            # But we still enforce MTP to prevent timestamp manipulation
            if self.is_syncing:
                # During sync, validate against parent timestamp
                if block_data["timestamp"] <= parent_info["timestamp"]:
                    # Allow equal timestamps for rapid mining scenarios
                    if block_data["timestamp"] < parent_info["timestamp"]:
                        return False, f"Invalid block timestamp during sync - must be >= parent (block: {block_data['timestamp']}, parent: {parent_info['timestamp']})"
                # Enforce MTP even during sync
                if mtp is not None and block_data["timestamp"] <= mtp:
                    return False, f"Block timestamp {block_data['timestamp']} must be greater than median time past {mtp}"
                logger.info(f"Sync mode timestamp validation: block_ts={block_data['timestamp']}, parent_ts={parent_info['timestamp']}, mtp={mtp}")
            else:
                # Not syncing - validate against current time too
                current_time = int(time.time())
                logger.info(f"Timestamp validation: block_ts={block_data['timestamp']}, parent_ts={parent_info['timestamp']}, current={current_time}, mtp={mtp}")

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
                    time_since_parent = current_time - parent_info["timestamp"]
                    logger.info(f"Block timestamp <= parent. Time since parent: {time_since_parent}s")

                    if time_since_parent <= 10:
                        logger.warning(f"Allowing timestamp {block_data['timestamp']} <= parent {parent_info['timestamp']} for rapid mining (parent mined {time_since_parent}s ago)")
                    else:
                        return False, f"Invalid block timestamp - must be greater than parent (block: {block_data['timestamp']}, parent: {parent_info['timestamp']})"
                else:
                    # Normal timestamp validation with MTP
                    if not validate_block_timestamp(
                        block_data["timestamp"],
                        parent_info["timestamp"],
                        current_time,
                        median_time_past=mtp
                    ):
                        return False, "Invalid block timestamp"
        
        # Check if we have the parent block
        if prev_hash not in self.block_index and prev_hash != "00" * 32:
            # Parent not found - this is an orphan
            # Return False to indicate block is NOT accepted into chain yet
            # It will be fully re-validated via add_block() when parent arrives
            await self._add_orphan(block_data)
            return False, "Parent block not found - stored as orphan for later processing"
        
        # CRITICAL: Validate all transactions before accepting the block
        # This prevents invalid transactions from entering the chain
        if "full_transactions" in block_data and block_data["full_transactions"]:
            logger.info(f"Validating {len(block_data['full_transactions'])} transactions in block {block_hash}")
            
            # Debug: Log transactions in the block
            if height == 1:
                for i, tx in enumerate(block_data['full_transactions']):
                    logger.info(f"Block 1 transaction {i}: has_txid={('txid' in tx) if tx else False}, keys={(list(tx.keys()) if tx else 'None')}")
            
            # During sync mode, tell validator we're syncing so it skips timestamp freshness
            # but still enforces chain_id and other non-time-dependent checks
            if self.is_syncing:
                self.validator.is_syncing = True
            
            # Validate all non-coinbase transactions
            logger.info(f"About to validate block with {len(block_data.get('full_transactions', []))} full_transactions")
            
            # First, validate that all transaction hashes match their claimed txids
            from blockchain.blockchain import serialize_transaction
            tx_ids = block_data.get("tx_ids", [])
            for i, tx in enumerate(block_data.get('full_transactions', [])):
                if tx:
                    if 'txid' not in tx:
                        logger.error(f"[CHAIN_MANAGER] Transaction at index {i} missing txid!")
                        logger.error(f"[CHAIN_MANAGER] Transaction keys: {list(tx.keys())}")
                        return False, "Transaction missing txid"
                    
                    claimed_txid = tx['txid']  # Get txid for all transactions
                    
                    # Skip hash validation for coinbase transactions 
                    # Coinbase comes from cpuminer with different hash calculation
                    if self.validator._is_coinbase_transaction(tx):
                        logger.info(f"[CHAIN_MANAGER] Skipping hash validation for coinbase transaction")
                    else:
                        # Validate the transaction hash matches the claimed txid for regular transactions
                        try:
                            tx_hex = serialize_transaction(tx)  # Returns hex string
                            tx_bytes = bytes.fromhex(tx_hex)  # Convert hex to bytes
                            calculated_txid = sha256d(tx_bytes)[::-1].hex()  # Hash, reverse bytes, and convert to hex
                            if calculated_txid != claimed_txid:
                                logger.error(f"[CHAIN_MANAGER] Transaction hash mismatch! Index: {i}")
                                logger.error(f"[CHAIN_MANAGER] Claimed: {claimed_txid}, Calculated: {calculated_txid}")
                                logger.error(f"[CHAIN_MANAGER] Block {block_hash} rejected due to invalid transaction hash")
                                return False, f"Transaction {i} has invalid hash: claimed {claimed_txid}, actual {calculated_txid}"
                        except Exception as e:
                            logger.error(f"[CHAIN_MANAGER] Failed to validate transaction hash: {e}")
                            return False, f"Failed to validate transaction {i} hash: {e}"
                    
                    # Also verify it matches the tx_ids array if present
                    if i < len(tx_ids) and tx_ids[i] != claimed_txid:
                        logger.error(f"[CHAIN_MANAGER] Transaction txid doesn't match tx_ids array!")
                        logger.error(f"[CHAIN_MANAGER] tx.txid: {claimed_txid}, tx_ids[{i}]: {tx_ids[i]}")
                        return False, f"Transaction {i} txid mismatch with tx_ids array"
            
            # Validate merkle root matches the transactions
            if "merkle_root" in block_data and "tx_ids" in block_data:
                from blockchain.blockchain import calculate_merkle_root
                
                # If we have a pre-calculated merkle root (from submitblock), use it
                # Otherwise calculate it from tx_ids
                if "calculated_merkle_root" in block_data:
                    calculated_merkle = block_data["calculated_merkle_root"]
                else:
                    calculated_merkle = calculate_merkle_root(block_data["tx_ids"])
                    
                claimed_merkle = block_data["merkle_root"]
                
                if calculated_merkle != claimed_merkle:
                    logger.error(f"[CHAIN_MANAGER] Merkle root mismatch in block {block_hash}!")
                    logger.error(f"[CHAIN_MANAGER] Claimed merkle: {claimed_merkle}")
                    logger.error(f"[CHAIN_MANAGER] Calculated merkle: {calculated_merkle}")
                    logger.error(f"[CHAIN_MANAGER] Transaction IDs: {block_data['tx_ids']}")
                    return False, f"Invalid merkle root: claimed {claimed_merkle}, actual {calculated_merkle}"
            else:
                logger.error(f"[CHAIN_MANAGER] Block {block_hash} missing merkle_root or tx_ids!")
                return False, "Block missing required merkle_root or tx_ids field"
            
            is_valid, error_msg, total_fees = self.validator.validate_block_transactions(block_data)
            
            # Reset is_syncing flag on validator after validation
            if self.is_syncing:
                self.validator.is_syncing = False
            if not is_valid:
                logger.error(f"Block {block_hash} rejected: {str(error_msg)}")
                
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
                
                return False, str(error_msg)
            
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
                    logger.error(f"Block {block_hash} rejected: invalid coinbase - {str(error_msg)}")
                    return False, f"Invalid coinbase transaction: {str(error_msg)}"
        
        # Now that validation has passed and we have the parent, calculate cumulative difficulty
        # We know parent exists because orphan blocks are handled above
        cumulative_difficulty = await self._get_cumulative_difficulty_for_new_block(prev_hash, block_data["bits"])
        
        # Add cumulative difficulty and connected status to block data before storing
        block_data["cumulative_difficulty"] = str(cumulative_difficulty)
        block_data["connected"] = False  # All new blocks start as disconnected
        
        # Normalize block structure for consistent field ordering
        block_data = normalize_block(block_data, add_defaults=False)
        
        # Store the block with cumulative difficulty and connected status using WriteBatch for atomicity
        block_key = f"block:{block_hash}".encode()

        # Special handling for genesis block - always store it
        if block_hash == "0" * 64 and height == 0:
            logger.info(f"Storing genesis block with hash {block_hash}")
            # Always store genesis, don't check if it exists
            store_block = True
        else:
            # For non-genesis blocks, check if they already exist
            store_block = block_key not in self.db

        logger.info(f"[DEBUG] store_block={store_block} for block {block_hash}")

        # For genesis block and blocks that will become the new tip immediately,
        # we'll store and connect in one atomic operation
        is_genesis_becoming_tip = (store_block and block_hash == "0" * 64 and height == 0)

        if store_block and not is_genesis_becoming_tip:
            # Normal block storage - just store, don't connect yet
            logger.info(f"Storing new block {block_hash} at height {height} with cumulative difficulty {cumulative_difficulty}, connected=False")

            # Use WriteBatch for atomic storage of block and its transactions
            batch = WriteBatch()
            batch.put(block_key, json.dumps(block_data).encode())

            # Also store transactions separately for fork blocks
            # This ensures they're available during reorganization
            if "full_transactions" in block_data:
                for tx in block_data["full_transactions"]:
                    if tx and "txid" in tx:
                        tx_key = f"tx:{tx['txid']}".encode()
                        # Always add to batch - the batch write is atomic
                        # Don't check if it exists, as that would check the old state
                        batch.put(tx_key, json.dumps(tx).encode())
                        logger.debug(f"Storing transaction {tx['txid']} from block {block_hash}")

                        # Update explorer index for non-coinbase transactions
                        inputs = tx.get("inputs", [])
                        is_coinbase = len(inputs) == 1 and inputs[0].get("txid", "") == "0" * 64
                        if not is_coinbase:
                            from blockchain.explorer_index import get_explorer_index
                            explorer_index = get_explorer_index()
                            explorer_index.add_transaction(tx['txid'], tx.get('timestamp', 0), is_coinbase)

            # Write the batch atomically
            logger.info(f"[DEBUG] About to write batch for block {block_hash}")
            try:
                self.db.write(batch)
                logger.info(f"[DEBUG] Batch write completed for block {block_hash}")
            except Exception as e:
                logger.error(f"[DEBUG] Batch write failed for block {block_hash}: {e}")
                raise
            logger.debug(f"Block {block_hash} and its transactions stored atomically")
        elif is_genesis_becoming_tip:
            # For genesis, we'll store it when we connect it below
            logger.info(f"Genesis block will be stored and connected atomically")
        
        # Use the cumulative difficulty we already calculated and stored
        # cumulative_difficulty is already calculated above
        
        # Add block to index
        self.block_index[block_hash] = {
            "height": height,
            "previous_hash": prev_hash,
            "timestamp": block_data["timestamp"],
            "bits": block_data["bits"],
            "cumulative_difficulty": cumulative_difficulty
        }
        
        logger.info(f"Added block {block_hash[:16]}... to index at height {height} with cumulative difficulty {cumulative_difficulty}")
        
        # Cache cumulative difficulty to Redis
        if self.redis_cache:
            try:
                self.redis_cache.set_cumulative_difficulty(block_hash, cumulative_difficulty)
            except Exception as e:
                logger.debug(f"Failed to cache cumulative difficulty: {e}")
        
        # Check if this creates a new chain tip or extends existing one
        # Incrementally update chain tips for the new block
        # This is much more efficient than recalculating all tips
        self._update_chain_tips_incremental(block_hash, prev_hash)
        
        # Force recalculation of best chain tip after adding new block
        # Clear any cached best tip to ensure fresh calculation
        if self.redis_cache:
            try:
                self.redis_cache.delete_best_chain_info()
            except:
                pass
        
        # Check if we need to reorganize
        current_tip, current_height = await self.get_best_chain_tip()
        
        # Special case for genesis block: if blockchain is empty (height -1) and this is height 0
        is_genesis_becoming_tip = (current_height == -1 and height == 0)
        
        if block_hash == current_tip or is_genesis_becoming_tip:
            # This block became the new best tip
            logger.info(f"New best chain tip: {block_hash} at height {height}")
            
            # Update caches incrementally for the new tip
            if self.redis_cache:
                try:
                    # Update chain index entry for new block
                    self.redis_cache.update_chain_index_entry(block_hash, {
                        "height": height,
                        "previous_hash": block_data["previous_hash"],
                        "timestamp": block_data["timestamp"],
                        "bits": block_data["bits"],
                        "cumulative_difficulty": self.block_index[block_hash]["cumulative_difficulty"],
                        "is_tip": True
                    })
                    
                    # Update height index
                    self.redis_cache.update_height_index_entry(height, block_hash)
                    
                    # Update best chain info
                    self.redis_cache.set_best_chain_info({
                        "block_hash": block_hash,
                        "height": height,
                        "timestamp": block_data["timestamp"],
                        "cumulative_difficulty": str(self.block_index[block_hash]["cumulative_difficulty"])
                    })
                except Exception as e:
                    logger.debug(f"Failed to update caches: {e}")
            
            # Connect the block to process its transactions and create UTXOs
            if pre_validated_batch:
                # Use the pre-validated batch from RPC (contains UTXOs and transactions)
                # Still need to apply block-specific changes
                batch = pre_validated_batch
                self._connect_block(block_data, batch)
                self.db.write(batch)
            else:
                # Normal path: create our own batch
                batch = WriteBatch()

                # If this is genesis becoming tip and we haven't stored it yet,
                # store it in the same batch as connection
                if is_genesis_becoming_tip:
                    logger.info(f"Storing genesis block {block_hash} atomically with connection")
                    batch.put(block_key, json.dumps(block_data).encode())

                    # Also store genesis transaction
                    if "full_transactions" in block_data:
                        for tx in block_data["full_transactions"]:
                            if tx and "txid" in tx:
                                tx_key = f"tx:{tx['txid']}".encode()
                                batch.put(tx_key, json.dumps(tx).encode())
                                logger.info(f"Storing genesis transaction {tx['txid']}")

                self._connect_block(block_data, batch)
                self.db.write(batch)
            
            # Update chain:best_tip in database atomically
            tip_key = b"chain:best_tip"
            tip_batch = WriteBatch()
            tip_batch.put(tip_key, json.dumps({
                "hash": block_hash,
                "height": height
            }).encode())
            self.db.write(tip_batch)
            
            # CRITICAL: Invalidate height cache so next call gets fresh data
            invalidate_height_cache()
            logger.info(f"Height cache invalidated after new block at height {height}")
            
            # Process any orphans that can now be connected
            await self._process_orphans_for_block(block_hash)
            
            return True, None
        
        # Check if this block creates a better chain
        if await self._should_reorganize(block_hash):
            logger.warning(f"Chain reorganization needed! New tip: {block_hash}")
            success = await self._reorganize_to_block(block_hash)
            if not success:
                return False, "Reorganization failed"
        
        return True, None
    
    async def _add_orphan(self, block_data: dict):
        """Add a block to the orphan pool"""
        block_hash = block_data["block_hash"]
        logger.info(f"Adding orphan block {block_hash} at height {block_data.get('height')}")
        
        # Clean up old orphans before adding new one
        self._cleanup_orphans()
        
        # Add the new orphan
        self.orphan_blocks[block_hash] = block_data
        self.orphan_timestamps[block_hash] = int(time.time())
        
        # Track orphan chains
        self._update_orphan_chains(block_data)
        
        # Check if this orphan completes a chain that should trigger reorganization
        await self._evaluate_orphan_chains()
        
        # Enforce size limit (remove oldest if over limit)
        if len(self.orphan_blocks) > self.MAX_ORPHAN_BLOCKS:
            # Find oldest orphan
            oldest_hash = min(self.orphan_timestamps.items(), key=lambda x: x[1])[0]
            logger.info(f"Removing oldest orphan {oldest_hash} due to size limit")
            self._remove_orphan(oldest_hash)
    
    async def _process_orphans_for_block(self, parent_hash: str):
        """Try to connect any orphans that have this block as parent"""
        connected = []
        
        for orphan_hash, orphan_data in self.orphan_blocks.items():
            if orphan_data["previous_hash"] == parent_hash:
                # This orphan can now be connected
                logger.info(f"Connecting orphan {orphan_hash} to parent {parent_hash}")
                success, _ = await self.add_block(orphan_data)
                if success:
                    connected.append(orphan_hash)
        
        # Remove connected orphans
        for orphan_hash in connected:
            del self.orphan_blocks[orphan_hash]
            if orphan_hash in self.orphan_timestamps:
                del self.orphan_timestamps[orphan_hash]
    
    def _remove_orphan(self, orphan_hash: str):
        """Remove an orphan and update chain tracking"""
        if orphan_hash in self.orphan_blocks:
            del self.orphan_blocks[orphan_hash]
        if orphan_hash in self.orphan_timestamps:
            del self.orphan_timestamps[orphan_hash]
        if orphan_hash in self.orphan_roots:
            del self.orphan_roots[orphan_hash]
        
        # Remove from orphan chains
        chains_to_remove = []
        for tip_hash, chain in self.orphan_chains.items():
            if orphan_hash in chain:
                chain.remove(orphan_hash)
                if not chain or tip_hash == orphan_hash:
                    chains_to_remove.append(tip_hash)
        
        for tip_hash in chains_to_remove:
            del self.orphan_chains[tip_hash]
    
    def _update_orphan_chains(self, block_data: dict):
        """Update orphan chain tracking when a new orphan is added"""
        block_hash = block_data["block_hash"]
        prev_hash = block_data["previous_hash"]
        height = block_data.get("height", 0)
        
        # Check if this orphan extends an existing orphan chain
        extended_chain = False
        for tip_hash, chain in list(self.orphan_chains.items()):
            if chain and chain[-1] == prev_hash:
                # This orphan extends an existing chain
                logger.info(f"Orphan {block_hash} extends existing orphan chain ending at {tip_hash}")
                # Remove old tip from chains
                del self.orphan_chains[tip_hash]
                # Add extended chain with new tip
                self.orphan_chains[block_hash] = chain + [block_hash]
                # Update root tracking
                root = self.orphan_roots.get(chain[0], chain[0])
                self.orphan_roots[block_hash] = root
                extended_chain = True
                break
        
        if not extended_chain:
            # Check if this orphan's parent is another orphan
            if prev_hash in self.orphan_blocks:
                # Find the chain containing the parent
                parent_chain = None
                for tip_hash, chain in self.orphan_chains.items():
                    if prev_hash in chain:
                        parent_chain = chain[:chain.index(prev_hash) + 1]
                        break
                
                if parent_chain:
                    # Create new chain branch
                    self.orphan_chains[block_hash] = parent_chain + [block_hash]
                    root = self.orphan_roots.get(parent_chain[0], parent_chain[0])
                    self.orphan_roots[block_hash] = root
                else:
                    # Parent is orphan but not in a chain (shouldn't happen)
                    self.orphan_chains[block_hash] = [prev_hash, block_hash]
                    self.orphan_roots[block_hash] = prev_hash
            else:
                # This is a new orphan chain root
                self.orphan_chains[block_hash] = [block_hash]
                self.orphan_roots[block_hash] = block_hash
        
        logger.info(f"Orphan chains: {len(self.orphan_chains)} chains tracking {len(self.orphan_blocks)} orphans")
    
    async def try_connect_orphan_chain(self):
        """Actively try to connect orphan blocks starting from current chain tip"""
        current_tip, current_height = await self.get_best_chain_tip()
        logger.info(f"[ORPHAN_CONNECT] Trying to connect orphans from height {current_height}")
        
        # First, check if we should reorganize to a better orphan chain
        await self._check_orphan_chains_for_reorg()
        
        blocks_connected = 0
        next_height = current_height + 1
        
        # Keep trying to connect blocks as long as we find matches
        while True:
            found_block = False
            
            # Look for an orphan block at the next height that connects to our chain
            for orphan_hash, orphan_data in list(self.orphan_blocks.items()):
                if orphan_data.get("height") == next_height:
                    # Check if this orphan connects to our current tip
                    if orphan_data.get("previous_hash") == current_tip:
                        logger.info(f"[ORPHAN_CONNECT] Found matching orphan {orphan_hash} at height {next_height}")
                        
                        # Try to add this block
                        success, error = await self.add_block(orphan_data)
                        if success:
                            logger.info(f"[ORPHAN_CONNECT] Successfully connected orphan {orphan_hash} at height {next_height}")
                            blocks_connected += 1
                            
                            # Update for next iteration
                            current_tip = orphan_hash
                            next_height += 1
                            found_block = True
                            
                            # Remove from orphan tracking
                            if orphan_hash in self.orphan_blocks:
                                del self.orphan_blocks[orphan_hash]
                            if orphan_hash in self.orphan_timestamps:
                                del self.orphan_timestamps[orphan_hash]
                            # Remove from orphan chains
                            chains_to_remove = []
                            for tip_hash, chain in self.orphan_chains.items():
                                if orphan_hash in chain:
                                    chain.remove(orphan_hash)
                                    if not chain or tip_hash == orphan_hash:
                                        chains_to_remove.append(tip_hash)
                            
                            for tip_hash in chains_to_remove:
                                del self.orphan_chains[tip_hash]
                            
                            break
                        else:
                            logger.warning(f"[ORPHAN_CONNECT] Failed to connect orphan {orphan_hash}: {error}")
            
            if not found_block:
                # No more blocks to connect
                break
        
        if blocks_connected > 0:
            logger.info(f"[ORPHAN_CONNECT] Connected {blocks_connected} orphan blocks")
            return True
        else:
            logger.debug(f"[ORPHAN_CONNECT] No orphan blocks could be connected")
            return False
    
    async def _check_orphan_chains_for_reorg(self):
        """Check if any orphan chain represents a better chain we should reorganize to"""
        current_tip, current_height = await self.get_best_chain_tip()
        logger.info(f"[REORG_CHECK] Checking orphan chains for potential reorganization")
        
        # Look for orphan blocks at or near our current height that might be on a better chain
        for height in range(max(0, current_height - 10), current_height + 1):
            for orphan_hash, orphan_data in self.orphan_blocks.items():
                if orphan_data.get("height") == height:
                    # This orphan is at a height we care about
                    # Check if it's part of a longer chain
                    chain_length = self._get_orphan_chain_length(orphan_hash)
                    if chain_length > 0:
                        logger.info(f"[REORG_CHECK] Found orphan chain starting at height {height} with {chain_length} blocks")
                        
                        # Check if this chain would give us a higher height
                        potential_new_height = height + chain_length - 1
                        if potential_new_height > current_height:
                            logger.warning(f"[REORG_CHECK] Orphan chain would reach height {potential_new_height} vs current {current_height}")
                            
                            # We need to reorganize! 
                            logger.warning(f"[REORG_CHECK] Need to reorganize to orphan chain!")
                            
                            # Find the fork point - where this orphan chain diverges from our main chain
                            fork_height = self._find_fork_height_for_orphan(orphan_hash)
                            if fork_height is not None:
                                logger.warning(f"[REORG_CHECK] Fork detected at height {fork_height}")
                                
                                # Rewind to just before the fork
                                if await self._rewind_to_height(fork_height - 1):
                                    logger.warning(f"[REORG_CHECK] Successfully rewound to height {fork_height - 1}")
                                    # The orphan chain should now be able to connect
                                    return True
                                else:
                                    logger.error(f"[REORG_CHECK] Failed to rewind to height {fork_height - 1}")
                            else:
                                # Can't find fork point, request missing parent blocks
                                self._request_missing_parents_for_reorg(orphan_hash)
                            
                            return
    
    def _get_orphan_chain_length(self, start_hash: str) -> int:
        """Get the length of an orphan chain starting from a given block"""
        length = 1
        current_hash = start_hash
        
        # Follow the chain forward
        while True:
            found_next = False
            for orphan_hash, orphan_data in self.orphan_blocks.items():
                if orphan_data.get("previous_hash") == current_hash:
                    length += 1
                    current_hash = orphan_hash
                    found_next = True
                    break
            
            if not found_next:
                break
        
        return length
    
    def _request_missing_parents_for_reorg(self, orphan_hash: str):
        """Log that we need parent blocks for reorganization"""
        orphan_data = self.orphan_blocks.get(orphan_hash)
        if orphan_data:
            parent_hash = orphan_data.get("previous_hash")
            height = orphan_data.get("height", 0)
            logger.warning(f"[REORG_CHECK] Need parent block {parent_hash} at height {height - 1} for reorganization")
    
    def _find_fork_height_for_orphan(self, orphan_hash: str) -> Optional[int]:
        """Find where an orphan chain diverges from our main chain"""
        orphan_data = self.orphan_blocks.get(orphan_hash)
        if not orphan_data:
            return None
        
        # Start from the orphan and work backwards to find where it connects to main chain
        current_hash = orphan_hash
        min_height = orphan_data.get("height", 0)
        
        # Traverse backwards through orphan chain
        while current_hash:
            block_data = self.orphan_blocks.get(current_hash)
            if not block_data:
                # Not in orphans, check if it's in main chain
                if current_hash in self.block_index:
                    # Found connection point! Get height from main chain
                    main_block_key = f"block:{current_hash}".encode()
                    main_block_data = self.db.get(main_block_key)
                    if main_block_data:
                        main_block = json.loads(main_block_data.decode())
                        return main_block.get("height", min_height)
                break
            
            # Update minimum height seen
            min_height = min(min_height, block_data.get("height", min_height))
            
            # Check if the parent is in our main chain
            parent_hash = block_data.get("previous_hash")
            if parent_hash and parent_hash in self.block_index:
                # Fork point is at the parent's height
                parent_key = f"block:{parent_hash}".encode()
                parent_data = self.db.get(parent_key)
                if parent_data:
                    parent_block = json.loads(parent_data.decode())
                    return parent_block.get("height", min_height)
            
            # Move to parent
            current_hash = parent_hash
        
        # Couldn't find connection point
        return None
    
    async def _rewind_to_height(self, target_height: int) -> bool:
        """Rewind the chain to a specific height by disconnecting blocks"""
        current_tip, current_height = await self.get_best_chain_tip()
        
        if target_height >= current_height:
            logger.warning(f"Cannot rewind to height {target_height} - already at {current_height}")
            return False
        
        logger.warning(f"[REWIND] Rewinding chain from height {current_height} to {target_height}")
        
        # Disconnect blocks one by one
        blocks_to_disconnect = []
        height = current_height
        block_hash = current_tip
        
        while height > target_height:
            block_key = f"block:{block_hash}".encode()
            block_data_raw = self.db.get(block_key)
            if not block_data_raw:
                logger.error(f"[REWIND] Cannot find block {block_hash} at height {height}")
                return False
            
            block_data = json.loads(block_data_raw.decode())
            
            blocks_to_disconnect.append((block_hash, block_data))
            block_hash = block_data.get("previous_hash")
            height -= 1
        
        # Actually disconnect the blocks
        batch = WriteBatch()
        utxo_backups = {}  # Track UTXO states for potential rollback
        for block_hash, block_data in blocks_to_disconnect:
            logger.info(f"[REWIND] Disconnecting block {block_hash} at height {block_data.get('height')}")
            self._disconnect_block(block_hash, batch, utxo_backups)
        
        # Update best block to the new tip
        new_tip_hash = blocks_to_disconnect[-1][1].get("previous_hash")
        if new_tip_hash:
            batch.put(b"best_block_hash", new_tip_hash.encode())
            self.current_tip = new_tip_hash
            logger.warning(f"[REWIND] New chain tip: {new_tip_hash} at height {target_height}")
        
        # Write the batch atomically
        self.db.write(batch)
        
        # Invalidate height cache
        invalidate_height_cache()
        
        return True
    
    async def _evaluate_orphan_chains(self):
        """Check if any orphan chain should trigger a reorganization"""
        current_tip, current_height = await self.get_best_chain_tip()
        current_difficulty = await self._get_cumulative_difficulty(current_tip)
        
        for tip_hash, chain in self.orphan_chains.items():
            # Get the root of this orphan chain
            root_hash = chain[0]
            root_block = self.orphan_blocks.get(root_hash)
            if not root_block:
                continue
            
            # Check if the parent of the root exists in our main chain
            root_parent = root_block.get("previous_hash")
            if root_parent in self.block_index or root_parent == "00" * 32:
                # This orphan chain can potentially connect to our chain
                # Calculate total difficulty of the orphan chain
                orphan_chain_difficulty = self._calculate_orphan_chain_difficulty(chain)
                
                # Get difficulty up to the connection point
                if root_parent == "00" * 32:
                    base_difficulty = Decimal(0)
                else:
                    base_difficulty = await self._get_cumulative_difficulty(root_parent)
                
                total_orphan_difficulty = base_difficulty + orphan_chain_difficulty
                
                # Log detailed information about the potential reorganization
                tip_block = self.orphan_blocks.get(tip_hash)
                if tip_block:
                    logger.warning(f"Evaluating orphan chain: tip={tip_hash}, height={tip_block.get('height')}, "
                                 f"chain_length={len(chain)}, total_difficulty={total_orphan_difficulty}, "
                                 f"current_difficulty={current_difficulty}")
                
                # Check if this orphan chain has more work
                if total_orphan_difficulty > current_difficulty:
                    logger.warning(f"Orphan chain has more work! Attempting reorganization to {tip_hash}")
                    logger.warning(f"Orphan chain: {len(chain)} blocks, root={root_hash}, tip={tip_hash}")
                    logger.warning(f"Current chain: height={current_height}, tip={current_tip}")
                    
                    # First, we need to connect the orphan blocks to the main chain
                    # This requires processing them in order
                    success = await self._connect_orphan_chain(chain)
                    if success:
                        # Now attempt reorganization to the new tip
                        if await self._should_reorganize(tip_hash):
                            success = await self._reorganize_to_block(tip_hash)
                            if success:
                                logger.warning(f"Successfully reorganized to orphan chain tip {tip_hash}")
                                # Clean up orphan data for connected blocks
                                for block_hash in chain:
                                    self._remove_orphan(block_hash)
                            else:
                                logger.error(f"Failed to reorganize to orphan chain tip {tip_hash}")
                        else:
                            logger.warning(f"Connected orphan chain but it's not the best chain")
                    else:
                        logger.error(f"Failed to connect orphan chain starting at {root_hash}")
    
    def _calculate_orphan_chain_difficulty(self, chain: List[str]) -> Decimal:
        """Calculate the total difficulty of an orphan chain"""
        total_difficulty = Decimal(0)
        
        for block_hash in chain:
            block_data = self.orphan_blocks.get(block_hash)
            if not block_data:
                logger.warning(f"Orphan block {block_hash} not found while calculating difficulty")
                continue
            
            bits = block_data.get("bits", MAX_TARGET_BITS)
            target = bits_to_target(bits)
            difficulty = Decimal(2**256) / Decimal(target)
            total_difficulty += difficulty
        
        return total_difficulty
    
    async def _connect_orphan_chain(self, chain: List[str]) -> bool:
        """Attempt to connect an orphan chain to the main chain"""
        logger.info(f"Attempting to connect orphan chain of {len(chain)} blocks")
        
        # Process blocks in order
        for block_hash in chain:
            block_data = self.orphan_blocks.get(block_hash)
            if not block_data:
                logger.error(f"Orphan block {block_hash} not found during connection")
                return False
            
            # Remove from orphan pool temporarily
            self.orphan_blocks.pop(block_hash, None)
            self.orphan_timestamps.pop(block_hash, None)
            
            # Try to add the block normally
            success, error = await self.add_block(block_data)
            if not success:
                # Re-add to orphan pool if failed
                self.orphan_blocks[block_hash] = block_data
                self.orphan_timestamps[block_hash] = int(time.time())
                logger.error(f"Failed to connect orphan block {block_hash}: {error}")
                return False
            
            logger.info(f"Successfully connected orphan block {block_hash}")
        
        return True
    
    async def _should_reorganize(self, new_tip_hash: str) -> bool:
        """Check if a new block creates a better chain than current"""
        current_tip, _ = await self.get_best_chain_tip()
        
        current_difficulty = await self._get_cumulative_difficulty(current_tip)
        new_difficulty = await self._get_cumulative_difficulty(new_tip_hash)
        
        return new_difficulty > current_difficulty
    
    async def _reorganize_to_block(self, new_tip_hash: str) -> bool:
        """
        Perform chain reorganization to make new_tip_hash the best chain
        This is the critical function for consensus
        """
        logger.warning(f"Starting chain reorganization to {new_tip_hash}")
        
        current_tip, _ = await self.get_best_chain_tip()
        
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

        # Reject reorgs deeper than MAX_REORG_DEPTH
        if len(blocks_to_disconnect) > MAX_REORG_DEPTH:
            logger.error(f"Rejecting reorganization: disconnect depth {len(blocks_to_disconnect)} exceeds MAX_REORG_DEPTH ({MAX_REORG_DEPTH})")
            return False
        if len(blocks_to_connect) > MAX_REORG_DEPTH:
            logger.error(f"Rejecting reorganization: connect depth {len(blocks_to_connect)} exceeds MAX_REORG_DEPTH ({MAX_REORG_DEPTH})")
            return False

        # Create backup of current state for rollback
        backup_state = {
            "best_tip": current_tip,
            "height": self.block_index[current_tip]["height"],
            "utxo_backups": {},
            "block_states": {}
        }
        
        # Write a reorganization marker BEFORE starting (separate write for recovery)
        reorg_marker_key = b"chain:last_reorg"
        marker_batch = WriteBatch()
        marker_batch.put(reorg_marker_key, json.dumps({
            "from_tip": current_tip,
            "to_tip": new_tip_hash,
            "timestamp": time.time(),
            "blocks_to_disconnect": len(blocks_to_disconnect),
            "blocks_to_connect": len(blocks_to_connect),
            "status": "in_progress"
        }).encode())
        self.db.write(marker_batch)
        
        # Start database transaction
        batch = WriteBatch()
        
        try:
            # Phase 1: Disconnect blocks from current chain
            # Track UTXOs restored (unspent) by disconnection for cross-checking
            restored_utxos = set()
            for block_hash in blocks_to_disconnect:
                # Backup block state before disconnecting
                block_key = f"block:{block_hash}".encode()
                backup_state["block_states"][block_hash] = self.db.get(block_key)

                self._disconnect_block(block_hash, batch, backup_state["utxo_backups"])

            # Collect the set of UTXOs that were restored (marked unspent) during disconnect
            for utxo_key_str, _ in backup_state["utxo_backups"].items():
                restored_utxos.add(utxo_key_str)

            logger.info(f"Phase 1 complete: {len(restored_utxos)} UTXOs restored during disconnect")

            # Phase 2: Validate and connect blocks from new chain
            # Each block's validation will handle its own spent UTXO tracking
            new_chain_spent_utxos = set()
            
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
                
                # Connect with new chain UTXO tracking and restored UTXO cross-check
                self._connect_block_safe(block_dict, batch, new_chain_spent_utxos, restored_utxos)
            
            # Phase 3: Update chain state in the same batch for atomicity
            key = b"chain:best_tip"
            batch.put(key, json.dumps({
                "hash": new_tip_hash,
                "height": self.block_index[new_tip_hash]["height"]
            }).encode())
            
            # Commit the reorganization atomically - ALL changes in one write
            self.db.write(batch)
            
            # Update the reorganization marker to completed status (separate write is OK here)
            # If we crash after batch write but before this, recovery will see the reorg
            # completed successfully by checking if tip matches expected result
            completion_batch = WriteBatch()
            completion_batch.put(reorg_marker_key, json.dumps({
                "from_tip": current_tip,
                "to_tip": new_tip_hash,
                "timestamp": time.time(),
                "blocks_disconnected": len(blocks_to_disconnect),
                "blocks_connected": len(blocks_to_connect),
                "status": "completed"
            }).encode())
            self.db.write(completion_batch)
            
            logger.info(f"Chain reorganization complete. New tip: {new_tip_hash}")
            
            # Update caches after reorganization
            if self.redis_cache:
                try:
                    # Update cache for disconnected blocks
                    for block_hash in blocks_to_disconnect:
                        self.redis_cache.remove_chain_index_entry(block_hash)
                        if block_hash in self.block_index:
                            height = self.block_index[block_hash]["height"]
                            self.redis_cache.remove_height_index_entry(height)
                    
                    # Update cache for connected blocks
                    for block_hash in blocks_to_connect:
                        if block_hash in self.block_index:
                            block_info = self.block_index[block_hash]
                            self.redis_cache.update_chain_index_entry(block_hash, block_info)
                            self.redis_cache.update_height_index_entry(block_info["height"], block_hash)
                    
                    # Update best chain info
                    self.redis_cache.set_best_chain_info({
                        "block_hash": new_tip_hash,
                        "height": self.block_index[new_tip_hash]["height"],
                        "timestamp": self.block_index[new_tip_hash]["timestamp"],
                        "cumulative_difficulty": str(self.block_index[new_tip_hash]["cumulative_difficulty"])
                    })
                except Exception as e:
                    logger.debug(f"Failed to update caches after reorg: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"Reorganization failed: {e}")
            # Rollback is automatic since we haven't committed the batch
            logger.info("Reorganization rolled back due to error")
            
            # Verify chain state is still consistent
            try:
                current_best = self.db.get(b"chain:best_tip")
                if current_best:
                    best_data = json.loads(current_best.decode())
                    if best_data["hash"] != current_tip:
                        logger.critical(f"CRITICAL: Chain tip inconsistent after failed reorg! Expected {current_tip}, got {best_data['hash']}")
                        # Attempt to restore original tip
                        restore_batch = WriteBatch()
                        restore_batch.put(b"chain:best_tip", json.dumps({
                            "hash": current_tip,
                            "height": backup_state["height"]
                        }).encode())
                        self.db.write(restore_batch)
                        logger.info(f"Restored original chain tip: {current_tip}")
            except Exception as verify_error:
                logger.critical(f"Failed to verify chain state after reorg failure: {verify_error}")
            
            return False
    
    def _find_common_ancestor(self, hash1: str, hash2: str, max_depth: int = MAX_REORG_DEPTH) -> Optional[str]:
        """Find the common ancestor of two blocks, bailing out if deeper than max_depth"""
        # Get ancestors of both blocks (limited by max_depth)
        ancestors1 = set()
        current = hash1
        depth = 0
        while current and depth <= max_depth:
            ancestors1.add(current)
            if current in self.block_index:
                current = self.block_index[current]["previous_hash"]
                # Include genesis block as a potential ancestor
                if current == "00" * 32:
                    ancestors1.add(current)
                    break
            else:
                break
            depth += 1

        # Walk up hash2's chain until we find common ancestor (limited by max_depth)
        current = hash2
        depth = 0
        while current and depth <= max_depth:
            if current in ancestors1:
                return current
            if current in self.block_index:
                current = self.block_index[current]["previous_hash"]
                # Check genesis as common ancestor
                if current == "00" * 32 and current in ancestors1:
                    return current
            else:
                break
            depth += 1

        if depth > max_depth:
            logger.error(f"Common ancestor search exceeded max_depth ({max_depth})")
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
            # Remove tx_block index entry
            tx_block_key = f"tx_block:{txid}".encode()
            batch.delete(tx_block_key)
        
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
        # Normalize before storing
        block_data = normalize_block(block_data, add_defaults=False)
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

        # Create tx_block index for O(1) lookups
        block_height = block_data.get('height', 0)
        block_hash = block_data.get('block_hash', '')
        for txid in block_data.get("tx_ids", []):
            tx_block_key = f"tx_block:{txid}".encode()
            batch.put(tx_block_key, json.dumps({"height": block_height, "hash": block_hash}).encode())
        
        # Process all transactions in the block
        for tx in full_transactions:
            self._apply_transaction(tx, block_data["height"], batch)
        
        # Remove mined transactions from mempool
        # Skip coinbase (first transaction) as it's not in mempool
        logger.info(f"[ChainManager] Starting mempool cleanup for block {block_data['block_hash']}")
        logger.info(f"[ChainManager] Full transactions in block: {len(full_transactions)}")
        logger.info(f"[ChainManager] Current mempool size: {mempool_manager.size()}")
        
        removed_count = 0
        not_in_mempool = 0
        for tx in full_transactions:
            if tx and "txid" in tx and not self.validator._is_coinbase_transaction(tx):
                txid = tx["txid"]
                logger.debug(f"[ChainManager] Checking if txid {txid} is in mempool")
                if mempool_manager.get_transaction(txid) is not None:
                    logger.info(f"[ChainManager] Removing mined transaction {txid} from mempool")
                    mempool_manager.remove_transaction(txid)
                    removed_count += 1
                else:
                    logger.debug(f"[ChainManager] Transaction {txid} not in mempool")
                    not_in_mempool += 1
        
        logger.info(f"[ChainManager] Mempool cleanup complete: removed={removed_count}, not_in_mempool={not_in_mempool}")
        logger.info(f"[ChainManager] Mempool size after cleanup: {mempool_manager.size()}")
        
        # Mark block as connected
        block_data["connected"] = True
        # Normalize before storing
        block_data = normalize_block(block_data, add_defaults=False)
        block_key = f"block:{block_data['block_hash']}".encode()
        batch.put(block_key, json.dumps(block_data).encode())
        
        # Update the height index
        height_index = get_height_index()
        height_index.add_block_to_index(block_data["height"], block_data["block_hash"])
    
    def _connect_block_safe(self, block_data: dict, batch: WriteBatch, new_chain_spent_utxos: Set[str],
                            restored_utxos: Optional[Set[str]] = None):
        """
        Connect a block during reorganization with double-spend protection.
        Ensures UTXOs aren't restored if they're spent elsewhere in new chain.
        Validates each input UTXO actually exists and is unspent before allowing the spend.
        """
        logger.info(f"Safely connecting block {block_data['block_hash']} at height {block_data['height']}")

        # Get full transactions
        full_transactions = self._get_block_transactions(block_data)

        # Create tx_block index for O(1) lookups
        block_height = block_data.get('height', 0)
        block_hash = block_data.get('block_hash', '')
        for txid in block_data.get("tx_ids", []):
            tx_block_key = f"tx_block:{txid}".encode()
            batch.put(tx_block_key, json.dumps({"height": block_height, "hash": block_hash}).encode())
        
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
            if not self._validate_transaction_for_reorg(tx, block_spent_utxos, new_chain_spent_utxos, restored_utxos):
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
                if not self._validate_transaction_for_reorg(tx, block_spent_utxos, new_chain_spent_utxos, restored_utxos):
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
        # Normalize before storing
        block_data = normalize_block(block_data, add_defaults=False)
        block_key = f"block:{block_data['block_hash']}".encode()
        batch.put(block_key, json.dumps(block_data).encode())
        
        # Update the height index
        height_index = get_height_index()
        height_index.add_block_to_index(block_data["height"], block_data["block_hash"])
        
        # Update new_chain_spent_utxos with UTXOs spent in this block
        # This tracks all UTXOs spent across the entire new chain
        new_chain_spent_utxos.update(block_spent_utxos)
    
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
                    # Increment version when reverting
                    utxo["version"] = utxo.get("version", 0) + 1
                    # Clear spending info
                    utxo.pop("spent_at_height", None)
                    utxo.pop("spent_by_tx", None)
                    batch.put(utxo_key, json.dumps(utxo).encode())
                    
                    # Wallet indexes handled by wallet_index.update_for_new_transaction()
        
        # Remove outputs created by this transaction
        for idx, out in enumerate(tx.get("outputs", [])):
            utxo_key = f"utxo:{txid}:{idx}".encode()
            
            # Backup before deleting
            current_data = self.db.get(utxo_key)
            if current_data:
                backup_key = f"{txid}:{idx}"
                utxo_backups[backup_key] = current_data
                
                # Wallet indexes handled by wallet_index.update_for_new_transaction()
                utxo = json.loads(current_data.decode())
            
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
        
        # Mark inputs as spent (skip for coinbase and genesis)
        # Genesis transaction has a special input that shouldn't be marked as spent
        if not is_coinbase and height > 0:  # Skip input processing for genesis block (height 0)
            for inp in tx.get("inputs", []):
                if "txid" in inp and inp["txid"] != "00" * 32:
                    utxo_key = f"utxo:{inp['txid']}:{inp.get('utxo_index', 0)}".encode()
                    utxo_data = self.db.get(utxo_key)
                    if utxo_data:
                        utxo = json.loads(utxo_data.decode())
                        utxo["spent"] = True
                        # Increment version for optimistic locking
                        utxo["version"] = utxo.get("version", 0) + 1
                        utxo["spent_at_height"] = height
                        utxo["spent_by_tx"] = txid
                        batch.put(utxo_key, json.dumps(utxo).encode())
                        
                        # Wallet indexes handled by wallet_index.update_for_new_transaction()
        
        # Create new UTXOs (including for coinbase!)
        for idx, out in enumerate(tx.get("outputs", [])):
            # Create proper UTXO record with all necessary fields
            utxo_record = {
                "txid": txid,
                "utxo_index": idx,
                "sender": "coinbase" if is_coinbase else out.get('sender', ''),
                "receiver": out.get('receiver', ''),
                "amount": str(out.get('amount', '0')),  # Ensure string to avoid scientific notation
                "spent": False,  # New UTXOs are always unspent
                "version": 0,  # Initial version for optimistic locking
                "created_at_height": height,
                "created_by_block": None  # Will be set by the block processor
            }
            utxo_key = f"utxo:{txid}:{idx}".encode()
            batch.put(utxo_key, json.dumps(utxo_record).encode())
            
            # Wallet indexes are updated after transaction is stored (see below)
            
            if is_coinbase:
                logger.info(f"Created coinbase UTXO: {utxo_key.decode()} for {out.get('receiver')} amount: {out.get('amount')}")
        
        # Store transaction
        batch.put(f"tx:{txid}".encode(), json.dumps(tx).encode())
        
        # Update wallet indexes for efficient lookups
        from blockchain.wallet_index import get_wallet_index
        wallet_index = get_wallet_index()
        wallet_index.update_for_new_transaction(tx, batch)
    
    def _validate_transaction_for_reorg(self, tx: dict, block_spent_utxos: Set[str],
                                       new_chain_spent_utxos: Set[str],
                                       restored_utxos: Optional[Set[str]] = None) -> bool:
        """
        Validate a transaction during reorganization.
        Checks signatures, balances, double-spending, and UTXO existence.
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

            # Verify UTXO exists in DB and is unspent
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
        
        # Mark inputs as spent (skip if already spent in new chain, skip for coinbase/genesis)
        # Genesis transaction (height 0) has special inputs that shouldn't be marked as spent
        if not is_coinbase and height > 0:  # Skip input processing for genesis block
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
                        
                        # Wallet indexes handled by wallet_index.update_for_new_transaction()
        
        # Create new UTXOs (including for coinbase!)
        for idx, out in enumerate(tx.get("outputs", [])):
            # Create proper UTXO record with all necessary fields
            utxo_record = {
                "txid": txid,
                "utxo_index": idx,
                "sender": "coinbase" if is_coinbase else out.get('sender', ''),
                "receiver": out.get('receiver', ''),
                "amount": str(out.get('amount', '0')),  # Ensure string to avoid scientific notation
                "spent": False,  # New UTXOs are always unspent
                "version": 0,  # Initial version for optimistic locking
                "created_at_height": height,
                "created_by_block": None  # Will be set by the block processor
            }
            utxo_key = f"utxo:{txid}:{idx}".encode()
            batch.put(utxo_key, json.dumps(utxo_record).encode())
            
            # Wallet indexes are updated after transaction is stored (see below)
            
            if is_coinbase:
                logger.info(f"Created coinbase UTXO during reorg: {utxo_key.decode()} for {out.get('receiver')} amount: {out.get('amount')}")
        
        # Store transaction
        batch.put(f"tx:{txid}".encode(), json.dumps(tx).encode())
        
        # Update wallet indexes for efficient lookups
        from blockchain.wallet_index import get_wallet_index
        wallet_index = get_wallet_index()
        wallet_index.update_for_new_transaction(tx, batch)
    
    async def get_block_by_hash(self, block_hash: str) -> Optional[dict]:
        """Get a block by its hash"""
        block_key = f"block:{block_hash}".encode()
        block_data = self.db.get(block_key)
        if block_data:
            return json.loads(block_data.decode())
        return None
    
    async def is_block_in_main_chain(self, block_hash: str) -> bool:
        """Check if a block is in the main chain"""
        current_tip, _ = await self.get_best_chain_tip()
        
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
    
    async def request_missing_ancestor(self, orphan_hash: str) -> Optional[Tuple[str, int]]:
        """
        For an orphan block, determine what ancestor block we need to request.
        Returns (block_hash, height) of the block we should request, or None.
        """
        if orphan_hash not in self.orphan_blocks:
            return None
        
        orphan_data = self.orphan_blocks[orphan_hash]
        orphan_height = orphan_data.get("height", 0)
        
        # Find the root of this orphan's chain
        root_hash = self.orphan_roots.get(orphan_hash, orphan_hash)
        root_block = self.orphan_blocks.get(root_hash)
        
        if not root_block:
            # Single orphan, request its parent
            return orphan_data.get("previous_hash"), orphan_height - 1
        
        # For an orphan chain, we need the parent of the root
        root_parent = root_block.get("previous_hash")
        root_height = root_block.get("height", 0)
        
        # Check if we already have this block
        if root_parent in self.block_index or root_parent == "00" * 32:
            # We have the connection point, no need to request
            return None
        
        return root_parent, root_height - 1
    
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
            self._remove_orphan(orphan_hash)
    
    async def get_orphan_info(self) -> dict:
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