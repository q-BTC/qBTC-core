"""
Test cases for PoW validation fix in ChainManager:
1. Blocks with invalid PoW should not be stored in database
2. Valid orphan blocks should be cached with size/age limits
3. Chain reorganization should handle orphan blocks correctly
"""
import pytest
import json
import time
from unittest.mock import MagicMock, patch
from blockchain.chain_manager import ChainManager
from blockchain.blockchain import Block, sha256d
from rocksdict import WriteBatch


class DummyWriteBatch:
    """Mimics RocksDB WriteBatch"""
    def __init__(self):
        self.ops = []
    
    def put(self, key, val):
        self.ops.append((key, val))


class FakeDB(dict):
    """Simple mock database"""
    def get(self, key):
        return super().get(key, None)
    
    def put(self, key, value):
        self[key] = value
    
    def items(self):
        return super().items()
    
    def write(self, batch):
        for op in batch.ops:
            self.put(op[0], op[1])


class TestPoWValidationFix:
    """Test that blocks with invalid PoW are not stored in database"""
    
    @patch('blockchain.chain_manager.get_db')
    def test_invalid_pow_not_stored(self, mock_get_db):
        """Test that blocks with invalid PoW are rejected and not stored"""
        db = FakeDB()
        mock_get_db.return_value = db
        
        manager = ChainManager()
        
        # Create a block with invalid PoW (high hash value)
        invalid_block = {
            "block_hash": "f" * 64,  # Very high hash (invalid PoW)
            "previous_hash": "0" * 64,
            "height": 1,
            "version": 1,
            "merkle_root": "00" * 32,
            "timestamp": int(time.time()),
            "bits": 0x1d00ffff,  # Standard difficulty
            "nonce": 12345,
            "tx_ids": [],
            "full_transactions": []
        }
        
        # Try to add block with invalid PoW
        success, error = manager.add_block(invalid_block)
        
        # Should fail with PoW error
        assert not success
        assert "Invalid proof of work" in error
        
        # Block should NOT be in database
        block_key = f"block:{invalid_block['block_hash']}".encode()
        assert block_key not in db
        
        # Block should NOT be in block index
        assert invalid_block["block_hash"] not in manager.block_index
    
    @patch('blockchain.chain_manager.get_db')
    @patch('blockchain.chain_manager.WriteBatch', DummyWriteBatch)
    @patch('blockchain.chain_manager.validate_pow', return_value=True)
    @patch('blockchain.chain_manager.get_next_bits', return_value=0x1d00ffff)
    @patch('blockchain.chain_manager.validate_block_bits', return_value=True)
    @patch('blockchain.chain_manager.validate_block_timestamp', return_value=True)
    def test_valid_pow_is_stored(self, mock_timestamp, mock_bits, mock_next_bits, mock_validate_pow, mock_get_db):
        """Test that blocks with valid PoW are stored"""
        db = FakeDB()
        mock_get_db.return_value = db
        
        # Initialize with genesis block in the index
        genesis_hash = "0" * 64
        genesis = {
            "block_hash": genesis_hash,
            "previous_hash": "00" * 32,
            "height": 0,
            "timestamp": 0,
            "bits": 0x1d00ffff
        }
        db[f"block:{genesis_hash}".encode()] = json.dumps(genesis).encode()
        
        manager = ChainManager()
        
        # Create a valid block
        valid_block = {
            "block_hash": "abc123" + "0" * 58,
            "previous_hash": "0" * 64,
            "height": 1,
            "version": 1,
            "merkle_root": "00" * 32,
            "timestamp": int(time.time()),
            "bits": 0x1d00ffff,
            "nonce": 12345,
            "tx_ids": [],
            "full_transactions": []
        }
        
        # Add block with valid PoW
        success, error = manager.add_block(valid_block)
        
        # Should succeed
        assert success
        assert error is None
        
        # Block SHOULD be in database
        block_key = f"block:{valid_block['block_hash']}".encode()
        assert block_key in db
        
        # Block SHOULD be in block index
        assert valid_block["block_hash"] in manager.block_index
    
    @patch('blockchain.chain_manager.get_db')
    @patch('blockchain.chain_manager.WriteBatch', DummyWriteBatch)
    @patch('blockchain.chain_manager.validate_pow', return_value=True)
    @patch('blockchain.chain_manager.get_next_bits', return_value=0x1d00ffff)
    @patch('blockchain.chain_manager.validate_block_bits', return_value=True)
    @patch('blockchain.chain_manager.validate_block_timestamp', return_value=True)
    def test_orphan_block_cached_not_stored(self, mock_timestamp, mock_bits, mock_next_bits, mock_validate_pow, mock_get_db):
        """Test that orphan blocks are cached but not stored in database"""
        db = FakeDB()
        mock_get_db.return_value = db
        
        manager = ChainManager()
        
        # Create an orphan block (parent doesn't exist)
        orphan_block = {
            "block_hash": "orphan123" + "0" * 55,
            "previous_hash": "parent_doesnt_exist" + "0" * 44,
            "height": 100,
            "version": 1,
            "merkle_root": "00" * 32,
            "timestamp": int(time.time()),
            "bits": 0x1d00ffff,
            "nonce": 12345,
            "tx_ids": [],
            "full_transactions": []
        }
        
        # Add orphan block
        success, error = manager.add_block(orphan_block)
        
        # Should succeed (orphans are valid)
        assert success
        assert error is None
        
        # Block should NOT be in database
        block_key = f"block:{orphan_block['block_hash']}".encode()
        assert block_key not in db
        
        # Block should NOT be in block index
        assert orphan_block["block_hash"] not in manager.block_index
        
        # Block SHOULD be in orphan cache
        assert orphan_block["block_hash"] in manager.orphan_blocks
        assert orphan_block["block_hash"] in manager.orphan_timestamps
    
    @patch('blockchain.chain_manager.get_db')
    @patch('blockchain.chain_manager.WriteBatch', DummyWriteBatch)
    @patch('blockchain.chain_manager.validate_pow', return_value=True)
    @patch('blockchain.chain_manager.get_next_bits', return_value=0x1d00ffff)
    @patch('blockchain.chain_manager.validate_block_bits', return_value=True)
    @patch('blockchain.chain_manager.validate_block_timestamp', return_value=True)
    def test_orphan_cache_size_limit(self, mock_timestamp, mock_bits, mock_next_bits, mock_validate_pow, mock_get_db):
        """Test that orphan cache respects size limits"""
        db = FakeDB()
        mock_get_db.return_value = db
        
        manager = ChainManager()
        # Set a small limit for testing
        manager.MAX_ORPHAN_BLOCKS = 5
        
        # Add more orphans than the limit
        for i in range(10):
            orphan_block = {
                "block_hash": f"orphan{i}" + "0" * (62 - len(str(i))),
                "previous_hash": "parent_doesnt_exist" + "0" * 44,
                "height": 100 + i,
                "version": 1,
                "merkle_root": "00" * 32,
                "timestamp": int(time.time()) + i,  # Different timestamps
                "bits": 0x1d00ffff,
                "nonce": 12345 + i,
                "tx_ids": [],
                "full_transactions": []
            }
            manager.add_block(orphan_block)
        
        # Should only have MAX_ORPHAN_BLOCKS in cache
        assert len(manager.orphan_blocks) == 5
        assert len(manager.orphan_timestamps) == 5
        
        # Oldest orphans should have been removed (0-4)
        for i in range(5):
            assert f"orphan{i}" + "0" * (62 - len(str(i))) not in manager.orphan_blocks
        
        # Newest orphans should still be there (5-9)
        for i in range(5, 10):
            assert f"orphan{i}" + "0" * (62 - len(str(i))) in manager.orphan_blocks
    
    @patch('blockchain.chain_manager.get_db')
    @patch('blockchain.chain_manager.WriteBatch', DummyWriteBatch)
    @patch('blockchain.chain_manager.validate_pow', return_value=True)
    @patch('blockchain.chain_manager.get_next_bits', return_value=0x1d00ffff)
    @patch('blockchain.chain_manager.validate_block_bits', return_value=True)
    @patch('blockchain.chain_manager.validate_block_timestamp', return_value=True)
    def test_orphan_age_cleanup(self, mock_timestamp, mock_bits, mock_next_bits, mock_validate_pow, mock_get_db):
        """Test that old orphans are cleaned up"""
        db = FakeDB()
        mock_get_db.return_value = db
        
        manager = ChainManager()
        # Set short age limit for testing
        manager.MAX_ORPHAN_AGE = 10  # 10 seconds
        
        # Add an orphan with old timestamp
        old_time = int(time.time()) - 20  # 20 seconds ago
        manager.orphan_blocks["old_orphan"] = {
            "block_hash": "old_orphan",
            "previous_hash": "parent",
            "height": 100
        }
        manager.orphan_timestamps["old_orphan"] = old_time
        
        # Add a new orphan (triggers cleanup)
        new_orphan = {
            "block_hash": "new_orphan" + "0" * 54,
            "previous_hash": "parent_doesnt_exist" + "0" * 44,
            "height": 101,
            "version": 1,
            "merkle_root": "00" * 32,
            "timestamp": int(time.time()),
            "bits": 0x1d00ffff,
            "nonce": 12345,
            "tx_ids": [],
            "full_transactions": []
        }
        manager.add_block(new_orphan)
        
        # Old orphan should be cleaned up
        assert "old_orphan" not in manager.orphan_blocks
        assert "old_orphan" not in manager.orphan_timestamps
        
        # New orphan should still be there
        assert new_orphan["block_hash"] in manager.orphan_blocks
    
    @patch('blockchain.chain_manager.get_db')
    @patch('blockchain.chain_manager.WriteBatch', DummyWriteBatch)
    @patch('blockchain.chain_manager.validate_pow', return_value=True)
    @patch('blockchain.chain_manager.get_next_bits', return_value=0x1d00ffff)
    @patch('blockchain.chain_manager.validate_block_bits', return_value=True)
    @patch('blockchain.chain_manager.validate_block_timestamp', return_value=True)
    def test_get_orphan_info(self, mock_timestamp, mock_bits, mock_next_bits, mock_validate_pow, mock_get_db):
        """Test get_orphan_info method"""
        db = FakeDB()
        mock_get_db.return_value = db
        
        manager = ChainManager()
        
        # Add some orphans
        for i in range(3):
            orphan = {
                "block_hash": f"orphan{i}" + "0" * (62 - len(str(i))),
                "previous_hash": f"parent{i}" + "0" * (62 - len(str(i))),
                "height": 100 + i,
                "version": 1,
                "merkle_root": "00" * 32,
                "timestamp": int(time.time()),
                "bits": 0x1d00ffff,
                "nonce": 12345,
                "tx_ids": [],
                "full_transactions": []
            }
            manager.add_block(orphan)
            time.sleep(0.1)  # Small delay to ensure different timestamps
        
        # Get orphan info
        info = manager.get_orphan_info()
        
        assert info["count"] == 3
        assert info["max_orphans"] == manager.MAX_ORPHAN_BLOCKS
        assert info["max_age_seconds"] == manager.MAX_ORPHAN_AGE
        assert len(info["orphans"]) == 3
        
        # Check orphan details
        for i, orphan_detail in enumerate(info["orphans"]):
            assert orphan_detail["height"] == 100 + i
            assert orphan_detail["parent"] == f"parent{i}" + "0" * (62 - len(str(i)))
            assert orphan_detail["age_seconds"] >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])