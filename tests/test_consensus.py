"""
Test consensus mechanisms including chain reorganization, fork resolution, and orphan blocks
"""
import pytest
import json
import time
import asyncio
from unittest.mock import MagicMock, patch
from blockchain.chain_manager import ChainManager
from blockchain.blockchain import Block, sha256d
from blockchain.difficulty import MAX_TARGET_BITS
from database.database import set_db


class TestConsensus:
    """Test suite for blockchain consensus mechanisms"""

    @pytest.fixture
    def setup_db(self, tmp_path):
        """Setup test database"""
        from database.database import close_db
        db_path = str(tmp_path / "test_ledger.rocksdb")
        db = set_db(db_path)
        yield db
        close_db()

    @pytest.fixture(autouse=True)
    def _stub_database(self, setup_db, monkeypatch):
        """Override conftest _stub_database to use real RocksDB for consensus tests.
        Also reset singletons that cache db references between tests.
        """
        import blockchain.block_height_index as bhi
        import blockchain.chain_singleton as bcs

        monkeypatch.setattr("database.database.get_db", lambda: setup_db, raising=True)
        monkeypatch.setattr("gossip.gossip.get_db", lambda: setup_db, raising=True)
        monkeypatch.setattr("web.web.get_db", lambda: setup_db, raising=True)
        monkeypatch.setattr("rpc.rpc.get_db", lambda: setup_db, raising=True)

        # Reset singletons that cache db references
        monkeypatch.setattr(bhi, "_height_index_instance", None)
        bcs.reset_chain_manager()

        yield setup_db

    @pytest.fixture(autouse=True)
    def mock_difficulty_validation(self):
        """Mock difficulty validation for all consensus tests"""
        with patch('blockchain.chain_manager.get_next_bits', return_value=MAX_TARGET_BITS), \
             patch('blockchain.chain_manager.validate_block_bits', return_value=True), \
             patch('blockchain.chain_manager.validate_block_timestamp', return_value=True), \
             patch('blockchain.chain_manager.validate_pow', return_value=True):
            yield

    @pytest.fixture(autouse=True)
    def _reset_ts_counter(self):
        """Reset timestamp counter for each test"""
        TestConsensus._ts_counter = 0
        yield

    @pytest.fixture
    def chain_manager(self, setup_db):
        """Create ChainManager instance with test database.
        Replace _chain_lock with a reentrant-safe wrapper to avoid deadlock
        when _process_orphans_for_block calls add_block within add_block.
        """
        import contextlib

        class _ReentrantAsyncLock:
            """asyncio.Lock is not reentrant; this wrapper skips nested acquires."""
            def __init__(self):
                self._held = False

            async def __aenter__(self):
                if self._held:
                    return self
                self._held = True
                return self

            async def __aexit__(self, *exc):
                self._held = False

        cm = ChainManager(db=setup_db)
        cm._chain_lock = _ReentrantAsyncLock()
        yield cm

    _ts_counter = 0

    def create_test_block(self, height, prev_hash, nonce=0, timestamp=None):
        """Create a test block with valid structure.
        Each call auto-increments timestamp to ensure strict ordering.
        """
        TestConsensus._ts_counter += 1
        if timestamp is None:
            timestamp = int(time.time()) + TestConsensus._ts_counter
        else:
            # Ensure explicit timestamps also respect the counter
            timestamp = max(timestamp, int(time.time()) + TestConsensus._ts_counter)
        
        block = {
            "version": 1,
            "height": height,
            "previous_hash": prev_hash,
            "merkle_root": sha256d(b"test").hex(),
            "timestamp": timestamp,
            "bits": MAX_TARGET_BITS,  # Use standard minimum difficulty
            "nonce": nonce,
            "tx_ids": [],
            "full_transactions": [],
            "miner_address": "test_miner"
        }
        
        # Just create the block object and calculate hash
        block_obj = Block(
            block["version"],
            block["previous_hash"],
            block["merkle_root"],
            block["timestamp"],
            block["bits"],
            nonce
        )
        block["block_hash"] = block_obj.hash()
        
        return block
    
    def test_simple_chain_extension(self, chain_manager):
        """Test adding blocks to extend the chain"""
        async def run_test():
            # Genesis
            genesis = self.create_test_block(0, "00" * 32, nonce=1)
            success, error = await chain_manager.add_block(genesis)
            assert success, f"Failed to add genesis block: {error}"
            
            # Add block 1
            block1 = self.create_test_block(1, genesis["block_hash"], nonce=2)
            success, error = await chain_manager.add_block(block1)
            assert success
            
            # Verify chain state
            best_hash, best_height = chain_manager.get_best_chain_tip_sync()
            assert best_height == 1
            assert best_hash == block1["block_hash"]
        
        asyncio.run(run_test())
    
    def test_orphan_block_handling(self, chain_manager):
        """Test orphan blocks are properly queued and connected"""
        async def run_test():
            # Genesis
            genesis = self.create_test_block(0, "00" * 32, nonce=1)
            await chain_manager.add_block(genesis)
            
            # Add block 2 before block 1 (orphan)
            block1 = self.create_test_block(1, genesis["block_hash"], nonce=2)
            block2 = self.create_test_block(2, block1["block_hash"], nonce=3)
            
            # Block 2 should be orphaned (returns False with orphan message)
            success, error = await chain_manager.add_block(block2)
            assert not success  # Orphan blocks return False
            assert "orphan" in error.lower()
            assert block2["block_hash"] in chain_manager.orphan_blocks
            
            # Add block 1 - should connect block 2
            success, error = await chain_manager.add_block(block1)
            assert success
            
            # Check final state
            best_hash, best_height = chain_manager.get_best_chain_tip_sync()
            assert best_height == 2
            assert best_hash == block2["block_hash"]
            assert block2["block_hash"] not in chain_manager.orphan_blocks
        
        asyncio.run(run_test())
    
    def test_simple_fork_resolution(self, chain_manager):
        """Test that longer chain wins in fork resolution"""
        async def run_test():
            # Build main chain: genesis -> block1 -> block2
            genesis = self.create_test_block(0, "00" * 32, nonce=1)
            await chain_manager.add_block(genesis)
            block1 = self.create_test_block(1, genesis["block_hash"], nonce=2)
            await chain_manager.add_block(block1)
            block2 = self.create_test_block(2, block1["block_hash"], nonce=3)
            await chain_manager.add_block(block2)
            
            # Current best should be at height 2
            best_hash, best_height = chain_manager.get_best_chain_tip_sync()
            assert best_height == 2
            original_tip = best_hash

            # Create competing fork from block1 (same height, same difficulty)
            block2_alt = self.create_test_block(2, block1["block_hash"], nonce=400)
            success, error = await chain_manager.add_block(block2_alt)
            assert success

            # With equal difficulty, best tip is deterministic (lower hash wins)
            best_hash, best_height = chain_manager.get_best_chain_tip_sync()
            assert best_height == 2

            # Extend alternative chain to make it longer
            block3_alt = self.create_test_block(3, block2_alt["block_hash"], nonce=500)
            success, error = await chain_manager.add_block(block3_alt)
            assert success

            # Should switch to alternative chain (longer)
            best_hash, best_height = chain_manager.get_best_chain_tip_sync()
            assert best_height == 3
            assert best_hash == block3_alt["block_hash"]
        
        asyncio.run(run_test())
    
    def test_deep_reorganization(self, chain_manager):
        """Test reorganization with deeper chains"""
        async def run_test():
            # Build main chain up to height 5
            genesis = self.create_test_block(0, "00" * 32, nonce=1)
            await chain_manager.add_block(genesis)
            
            main_blocks = [genesis]
            for i in range(1, 6):
                block = self.create_test_block(i, main_blocks[-1]["block_hash"], nonce=i+1)
                await chain_manager.add_block(block)
                main_blocks.append(block)
            
            # Verify we're at height 5
            best_hash, best_height = chain_manager.get_best_chain_tip_sync()
            assert best_height == 5
            
            # Create longer alternative chain from height 2
            alt_blocks = main_blocks[:2]  # Keep genesis and block 1
            for i in range(2, 8):  # Build to height 7
                block = self.create_test_block(i, alt_blocks[-1]["block_hash"], nonce=100+i, timestamp=int(time.time()) + i)
                success, error = await chain_manager.add_block(block)
                assert success, f"Failed to add alternative block at height {i}: {error}"
                alt_blocks.append(block)
            
            # Should have reorganized to longer chain
            best_hash, best_height = chain_manager.get_best_chain_tip_sync()
            assert best_height == 7
            assert best_hash == alt_blocks[-1]["block_hash"]
        
        asyncio.run(run_test())
    
    def test_invalid_pow_rejection(self, chain_manager):
        """Test that blocks with invalid PoW are rejected"""
        async def run_test():
            with patch('blockchain.chain_manager.validate_pow', return_value=False):
                genesis = self.create_test_block(0, "00" * 32, nonce=1)
                success, error = await chain_manager.add_block(genesis)
                assert not success
                assert "proof of work" in error.lower()
        
        asyncio.run(run_test())
    
    def test_multiple_chain_tips(self, chain_manager):
        """Test handling of multiple competing chain tips"""
        async def run_test():
            # Genesis
            genesis = self.create_test_block(0, "00" * 32, nonce=1)
            await chain_manager.add_block(genesis)
            
            # Create 3 competing blocks at height 1
            blocks = []
            for i in range(3):
                block = self.create_test_block(1, genesis["block_hash"], nonce=10+i, timestamp=int(time.time()) + i)
                success, error = await chain_manager.add_block(block)
                assert success
                blocks.append(block)
            
            # Should have 3 chain tips
            assert len(chain_manager.chain_tips) == 3
            
            # Extend one chain
            block2 = self.create_test_block(2, blocks[0]["block_hash"], nonce=100)
            await chain_manager.add_block(block2)
            
            # Best tip should be the extended chain
            best_hash, best_height = chain_manager.get_best_chain_tip_sync()
            assert best_height == 2
            assert best_hash == block2["block_hash"]
        
        asyncio.run(run_test())
    
    def test_block_already_exists(self, chain_manager):
        """Test adding duplicate blocks"""
        async def run_test():
            genesis = self.create_test_block(0, "00" * 32, nonce=1)
            success, error = await chain_manager.add_block(genesis)
            assert success
            
            # Try adding same block again — silently succeeds (idempotent)
            success, error = await chain_manager.add_block(genesis)
            assert success
            assert error is None
        
        asyncio.run(run_test())
    
    def test_common_ancestor_finding(self, chain_manager):
        """Test finding common ancestor between chains"""
        async def run_test():
            # Build two forked chains
            genesis = self.create_test_block(0, "00" * 32, nonce=1)
            await chain_manager.add_block(genesis)
            
            # Common chain
            block1 = self.create_test_block(1, genesis["block_hash"], nonce=2)
            await chain_manager.add_block(block1)
            
            # Fork 1
            block2a = self.create_test_block(2, block1["block_hash"], nonce=3)
            s, e = await chain_manager.add_block(block2a)
            assert s, f"Failed to add block2a: {e}"
            block3a = self.create_test_block(3, block2a["block_hash"], nonce=4)
            s, e = await chain_manager.add_block(block3a)
            assert s, f"Failed to add block3a: {e}"

            # Fork 2
            block2b = self.create_test_block(2, block1["block_hash"], nonce=103)
            s, e = await chain_manager.add_block(block2b)
            assert s, f"Failed to add block2b: {e}"
            block3b = self.create_test_block(3, block2b["block_hash"], nonce=104)
            s, e = await chain_manager.add_block(block3b)
            assert s, f"Failed to add block3b: {e}"
            
            # Find common ancestor
            ancestor = chain_manager._find_common_ancestor(block3a["block_hash"], block3b["block_hash"])
            assert ancestor == block1["block_hash"]
        
        asyncio.run(run_test())