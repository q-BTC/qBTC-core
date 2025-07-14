"""
Test consensus mechanisms including chain reorganization, fork resolution, and orphan blocks
"""
import pytest
import json
import time
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
    def mock_difficulty_validation(self):
        """Mock difficulty validation for all consensus tests"""
        with patch('blockchain.chain_manager.get_next_bits', return_value=MAX_TARGET_BITS), \
             patch('blockchain.chain_manager.validate_block_bits', return_value=True), \
             patch('blockchain.chain_manager.validate_block_timestamp', return_value=True), \
             patch('blockchain.chain_manager.validate_pow', return_value=True):
            yield
    
    @pytest.fixture
    def chain_manager(self, setup_db):
        """Create ChainManager instance with test database"""
        with patch('blockchain.chain_manager.get_db', return_value=setup_db):
            cm = ChainManager()
            return cm
    
    def create_test_block(self, height, prev_hash, nonce=0, timestamp=None):
        """Create a test block with valid structure"""
        if timestamp is None:
            timestamp = int(time.time())
        
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
        # Genesis
        genesis = self.create_test_block(0, "00" * 32, nonce=1)
        success, error = chain_manager.add_block(genesis)
        assert success, f"Failed to add genesis block: {error}"
        
        # Add block 1
        block1 = self.create_test_block(1, genesis["block_hash"], nonce=2)
        success, error = chain_manager.add_block(block1)
        assert success
        
        # Verify chain state
        best_hash, best_height = chain_manager.get_best_chain_tip_sync()
        assert best_height == 1
        assert best_hash == block1["block_hash"]
    
    def test_orphan_block_handling(self, chain_manager):
        """Test orphan blocks are properly queued and connected"""
        # Genesis
        genesis = self.create_test_block(0, "00" * 32, nonce=1)
        chain_manager.add_block(genesis)
        
        # Add block 2 before block 1 (orphan)
        block1 = self.create_test_block(1, genesis["block_hash"], nonce=2)
        block2 = self.create_test_block(2, block1["block_hash"], nonce=3)
        
        # Block 2 should be orphaned
        success, error = chain_manager.add_block(block2)
        assert success
        assert block2["block_hash"] in chain_manager.orphan_blocks
        
        # Add block 1 - should connect block 2
        success, error = chain_manager.add_block(block1)
        assert success
        assert block2["block_hash"] not in chain_manager.orphan_blocks
        
        # Verify final chain
        best_hash, best_height = chain_manager.get_best_chain_tip_sync()
        assert best_height == 2
        assert best_hash == block2["block_hash"]
    
    def test_simple_fork_resolution(self, chain_manager):
        """Test that longer chain wins in a fork"""
        # Build initial chain: genesis -> block1 -> block2
        genesis = self.create_test_block(0, "00" * 32, nonce=1)
        block1 = self.create_test_block(1, genesis["block_hash"], nonce=2)
        block2 = self.create_test_block(2, block1["block_hash"], nonce=3)
        
        chain_manager.add_block(genesis)
        chain_manager.add_block(block1)
        chain_manager.add_block(block2)
        
        # Create competing chain from block1
        # Chain A: genesis -> block1 -> block2
        # Chain B: genesis -> block1 -> block2_alt -> block3_alt
        block2_alt = self.create_test_block(2, block1["block_hash"], nonce=100, timestamp=block2["timestamp"]+1)
        block3_alt = self.create_test_block(3, block2_alt["block_hash"], nonce=101)
        
        # Add alternative chain
        success, error = chain_manager.add_block(block2_alt)
        assert success
        
        # At this point, both chains have equal height
        best_hash, best_height = chain_manager.get_best_chain_tip_sync()
        assert best_height == 2
        # Either chain could be active (tie-breaking behavior)
        
        # Add block3_alt - should trigger reorganization
        success, error = chain_manager.add_block(block3_alt)
        assert success
        
        # Verify new chain is active
        best_hash, best_height = chain_manager.get_best_chain_tip_sync()
        assert best_height == 3
        assert best_hash == block3_alt["block_hash"]
        
        # Verify the reorganization happened
        assert chain_manager.is_block_in_main_chain(block3_alt["block_hash"])
        assert chain_manager.is_block_in_main_chain(block2_alt["block_hash"])
        assert not chain_manager.is_block_in_main_chain(block2["block_hash"])
    
    def test_deep_reorganization(self, chain_manager):
        """Test reorganization with deeper fork"""
        # Build initial chain: genesis -> b1 -> b2 -> b3
        genesis = self.create_test_block(0, "00" * 32, nonce=1)
        chain_manager.add_block(genesis)
        
        blocks_main = []
        prev_hash = genesis["block_hash"]
        for i in range(1, 4):
            block = self.create_test_block(i, prev_hash, nonce=i+1)
            chain_manager.add_block(block)
            blocks_main.append(block)
            prev_hash = block["block_hash"]
        
        # Create alternative chain from block 1: b1 -> b2_alt -> b3_alt -> b4_alt
        blocks_alt = []
        prev_hash = blocks_main[0]["block_hash"]  # Fork from block 1
        for i in range(2, 5):
            block = self.create_test_block(i, prev_hash, nonce=100+i, timestamp=blocks_main[0]["timestamp"]+i)
            blocks_alt.append(block)
            prev_hash = block["block_hash"]
        
        # Add alternative chain blocks
        for block in blocks_alt:
            success, error = chain_manager.add_block(block)
            assert success
        
        # Verify reorganization happened
        best_hash, best_height = chain_manager.get_best_chain_tip_sync()
        assert best_height == 4
        assert best_hash == blocks_alt[-1]["block_hash"]
        
        # Verify the alternative chain is active
        assert chain_manager.is_block_in_main_chain(blocks_alt[-1]["block_hash"])
        assert not chain_manager.is_block_in_main_chain(blocks_main[-1]["block_hash"])
    
    def test_invalid_pow_rejection(self, chain_manager):
        """Test that blocks with invalid PoW are rejected"""
        genesis = self.create_test_block(0, "00" * 32, nonce=1)
        chain_manager.add_block(genesis)
        
        # Create block with invalid nonce (won't meet difficulty)
        invalid_block = self.create_test_block(1, genesis["block_hash"], nonce=0)
        
        # Mock validate_pow to return False
        with patch('blockchain.chain_manager.validate_pow', return_value=False):
            success, error = chain_manager.add_block(invalid_block)
            assert not success
            assert "Invalid proof of work" in error
    
    def test_multiple_chain_tips(self, chain_manager):
        """Test handling of multiple chain tips"""
        # Create a fork situation with multiple tips
        genesis = self.create_test_block(0, "00" * 32, nonce=1)
        chain_manager.add_block(genesis)
        
        # Create two competing chains of equal length
        block1a = self.create_test_block(1, genesis["block_hash"], nonce=2)
        block1b = self.create_test_block(1, genesis["block_hash"], nonce=3, timestamp=block1a["timestamp"]+1)
        
        chain_manager.add_block(block1a)
        chain_manager.add_block(block1b)
        
        # Should have 2 chain tips
        assert len(chain_manager.chain_tips) == 2
        assert block1a["block_hash"] in chain_manager.chain_tips
        assert block1b["block_hash"] in chain_manager.chain_tips
    
    def test_block_already_exists(self, chain_manager):
        """Test adding a block that already exists"""
        genesis = self.create_test_block(0, "00" * 32, nonce=1)
        
        # Add block first time
        success, error = chain_manager.add_block(genesis)
        assert success
        
        # Add same block again
        success, error = chain_manager.add_block(genesis)
        assert success  # Should return success but not process again
        assert error is None
    
    def test_common_ancestor_finding(self, chain_manager):
        """Test finding common ancestor between two chains"""
        # Build chain: genesis -> b1 -> b2
        #                     \-> b1_alt -> b2_alt
        genesis = self.create_test_block(0, "00" * 32, nonce=1)
        block1 = self.create_test_block(1, genesis["block_hash"], nonce=2)
        block2 = self.create_test_block(2, block1["block_hash"], nonce=3)
        block1_alt = self.create_test_block(1, genesis["block_hash"], nonce=4, timestamp=block1["timestamp"]+1)
        block2_alt = self.create_test_block(2, block1_alt["block_hash"], nonce=5)
        
        # Add all blocks
        for block in [genesis, block1, block2, block1_alt, block2_alt]:
            chain_manager.add_block(block)
        
        # Find common ancestor
        ancestor = chain_manager._find_common_ancestor(block2["block_hash"], block2_alt["block_hash"])
        assert ancestor == genesis["block_hash"]