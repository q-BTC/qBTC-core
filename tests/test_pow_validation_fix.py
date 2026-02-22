"""
Test cases for PoW validation fix in ChainManager:
1. Blocks with invalid PoW should not be stored in database
2. Valid orphan blocks should be cached with size/age limits
3. Chain reorganization should handle orphan blocks correctly
"""
import pytest
import json
import time
import asyncio
from unittest.mock import MagicMock, patch
from blockchain.chain_manager import ChainManager
from blockchain.blockchain import Block, sha256d
from rocksdict import WriteBatch


class DummyWriteBatch:
    """Mimics RocksDB WriteBatch"""
    def __init__(self):
        self.ops = []
        self.deletes = []

    def put(self, key, val):
        self.ops.append((key, val))

    def delete(self, key):
        self.deletes.append(key)


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
        for key in getattr(batch, 'deletes', []):
            self.pop(key, None)


class TestPoWValidationFix:
    """Test that blocks with invalid PoW are not stored in database"""

    @pytest.fixture(autouse=True)
    def _reset_singletons(self, monkeypatch, _stub_database):
        """Reset singletons and patch get_db for modules that import it directly"""
        import blockchain.block_height_index as bhi
        import blockchain.chain_singleton as bcs
        # Patch get_db in block_height_index module so its singleton uses stub db
        monkeypatch.setattr("blockchain.block_height_index.get_db",
                            lambda: _stub_database, raising=True)
        monkeypatch.setattr(bhi, "_height_index_instance", None)
        bcs.reset_chain_manager()

        # Stub out get_height_index to avoid real WriteBatch on plain-dict db
        class _StubHeightIndex:
            def add_block_to_index(self, height, block_hash):
                pass
            def remove_block_from_index(self, height):
                pass
            def get_block_hash_by_height(self, height):
                return None

        monkeypatch.setattr("blockchain.chain_manager.get_height_index",
                            lambda: _StubHeightIndex())
        yield

    @staticmethod
    def _make_block(height, prev_hash, nonce=0, ts=None):
        """Create a block with a valid computed hash"""
        if ts is None:
            ts = int(time.time()) + height
        block_obj = Block(1, prev_hash, "00" * 32, ts, 0x1d00ffff, nonce)
        return {
            "block_hash": block_obj.hash(),
            "previous_hash": prev_hash,
            "height": height,
            "version": 1,
            "merkle_root": "00" * 32,
            "timestamp": ts,
            "bits": 0x1d00ffff,
            "nonce": nonce,
            "tx_ids": [],
            "full_transactions": [],
            "miner_address": "test"
        }

    def test_invalid_pow_not_stored(self):
        """Test that blocks with invalid PoW are rejected and not stored"""
        db = FakeDB()
        manager = ChainManager(db=db)

        # Create a block — let the Block class compute the real hash
        block_obj = Block(1, "0" * 64, "00" * 32, int(time.time()), 0x1d00ffff, 12345)
        real_hash = block_obj.hash()

        invalid_block = {
            "block_hash": real_hash,
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

        async def run_test():
            success, error = await manager.add_block(invalid_block)

            # Should fail (PoW or other validation)
            assert not success
            assert error is not None

            # Block should NOT be in database
            block_key = f"block:{real_hash}".encode()
            assert block_key not in db

            # Block should NOT be in block index
            assert real_hash not in manager.block_index

        asyncio.run(run_test())

    @patch('blockchain.chain_manager.WriteBatch', DummyWriteBatch)
    @patch('blockchain.chain_manager.validate_pow', return_value=True)
    @patch('blockchain.chain_manager.get_next_bits', return_value=0x1d00ffff)
    @patch('blockchain.chain_manager.validate_block_bits', return_value=True)
    @patch('blockchain.chain_manager.validate_block_timestamp', return_value=True)
    def test_valid_pow_is_stored(self, mock_timestamp, mock_bits, mock_next_bits, mock_validate_pow):
        """Test that blocks with valid PoW are stored"""
        db = FakeDB()

        ts = int(time.time())
        # Create genesis block with real hash
        genesis_obj = Block(1, "00" * 32, "00" * 32, ts, 0x1d00ffff, 0)
        genesis_hash = genesis_obj.hash()
        genesis = {
            "block_hash": genesis_hash,
            "previous_hash": "00" * 32,
            "height": 0,
            "version": 1,
            "merkle_root": "00" * 32,
            "timestamp": ts,
            "bits": 0x1d00ffff,
            "nonce": 0,
            "tx_ids": [],
            "full_transactions": [],
            "miner_address": "test"
        }
        db[f"block:{genesis_hash}".encode()] = json.dumps(genesis).encode()

        manager = ChainManager(db=db)

        # Create a valid block with real hash
        block_obj = Block(1, genesis_hash, "00" * 32, ts + 1, 0x1d00ffff, 12345)
        block_hash = block_obj.hash()
        valid_block = {
            "block_hash": block_hash,
            "previous_hash": genesis_hash,
            "height": 1,
            "version": 1,
            "merkle_root": "00" * 32,
            "timestamp": ts + 1,
            "bits": 0x1d00ffff,
            "nonce": 12345,
            "tx_ids": [],
            "full_transactions": [],
            "miner_address": "test"
        }

        async def run_test():
            # First add genesis
            s, e = await manager.add_block(genesis)
            assert s, f"Genesis failed: {e}"

            # Then add the valid block
            success, error = await manager.add_block(valid_block)
            assert success, f"Block failed: {error}"
            assert error is None

            # Block SHOULD be in database
            block_key = f"block:{block_hash}".encode()
            assert block_key in db

            # Block SHOULD be in block index
            assert block_hash in manager.block_index

        asyncio.run(run_test())

    @patch('blockchain.chain_manager.WriteBatch', DummyWriteBatch)
    @patch('blockchain.chain_manager.validate_pow', return_value=True)
    @patch('blockchain.chain_manager.get_next_bits', return_value=0x1d00ffff)
    @patch('blockchain.chain_manager.validate_block_bits', return_value=True)
    @patch('blockchain.chain_manager.validate_block_timestamp', return_value=True)
    def test_orphan_block_cached_not_stored(self, mock_timestamp, mock_bits, mock_next_bits, mock_validate_pow):
        """Test that orphan blocks are cached but not stored in database"""
        db = FakeDB()
        manager = ChainManager(db=db)

        # Create an orphan block with valid hash (parent doesn't exist)
        fake_parent = "ab" * 32  # Non-existent parent
        orphan_block = self._make_block(100, fake_parent, nonce=12345)

        async def run_test():
            success, error = await manager.add_block(orphan_block)

            # Orphan blocks return False with an orphan message
            assert not success
            assert "orphan" in error.lower()

            block_hash = orphan_block["block_hash"]
            # Block should NOT be in database
            assert f"block:{block_hash}".encode() not in db
            # Block should NOT be in block index
            assert block_hash not in manager.block_index
            # Block SHOULD be in orphan cache
            assert block_hash in manager.orphan_blocks
            assert block_hash in manager.orphan_timestamps

        asyncio.run(run_test())

    @patch('blockchain.chain_manager.WriteBatch', DummyWriteBatch)
    @patch('blockchain.chain_manager.validate_pow', return_value=True)
    @patch('blockchain.chain_manager.get_next_bits', return_value=0x1d00ffff)
    @patch('blockchain.chain_manager.validate_block_bits', return_value=True)
    @patch('blockchain.chain_manager.validate_block_timestamp', return_value=True)
    def test_orphan_cache_size_limit(self, mock_timestamp, mock_bits, mock_next_bits, mock_validate_pow):
        """Test that orphan cache respects size limits"""
        db = FakeDB()
        manager = ChainManager(db=db)
        manager.MAX_ORPHAN_BLOCKS = 5

        async def run_test():
            fake_parent = "ab" * 32
            orphan_hashes = []
            for i in range(10):
                orphan = self._make_block(100 + i, fake_parent, nonce=12345 + i, ts=int(time.time()) + i)
                orphan_hashes.append(orphan["block_hash"])
                await manager.add_block(orphan)

            # Should only have MAX_ORPHAN_BLOCKS in cache
            assert len(manager.orphan_blocks) == 5
            assert len(manager.orphan_timestamps) == 5

            # Oldest orphans should have been removed (0-4)
            for i in range(5):
                assert orphan_hashes[i] not in manager.orphan_blocks

            # Newest orphans should still be there (5-9)
            for i in range(5, 10):
                assert orphan_hashes[i] in manager.orphan_blocks

        asyncio.run(run_test())

    @patch('blockchain.chain_manager.WriteBatch', DummyWriteBatch)
    @patch('blockchain.chain_manager.validate_pow', return_value=True)
    @patch('blockchain.chain_manager.get_next_bits', return_value=0x1d00ffff)
    @patch('blockchain.chain_manager.validate_block_bits', return_value=True)
    @patch('blockchain.chain_manager.validate_block_timestamp', return_value=True)
    def test_orphan_age_cleanup(self, mock_timestamp, mock_bits, mock_next_bits, mock_validate_pow):
        """Test that old orphans are cleaned up"""
        db = FakeDB()
        manager = ChainManager(db=db)
        manager.MAX_ORPHAN_AGE = 10  # 10 seconds

        async def run_test():
            # Add an orphan with old timestamp (directly in cache)
            old_time = int(time.time()) - 20
            manager.orphan_blocks["old_orphan"] = {
                "block_hash": "old_orphan",
                "previous_hash": "parent",
                "height": 100
            }
            manager.orphan_timestamps["old_orphan"] = old_time

            # Add a new orphan with valid hash (triggers cleanup)
            fake_parent = "cd" * 32
            new_orphan = self._make_block(101, fake_parent, nonce=12345)
            await manager.add_block(new_orphan)

            # Old orphan should be cleaned up
            assert "old_orphan" not in manager.orphan_blocks
            assert "old_orphan" not in manager.orphan_timestamps

            # New orphan should still be there
            assert new_orphan["block_hash"] in manager.orphan_blocks

        asyncio.run(run_test())

    @patch('blockchain.chain_manager.WriteBatch', DummyWriteBatch)
    @patch('blockchain.chain_manager.validate_pow', return_value=True)
    @patch('blockchain.chain_manager.get_next_bits', return_value=0x1d00ffff)
    @patch('blockchain.chain_manager.validate_block_bits', return_value=True)
    @patch('blockchain.chain_manager.validate_block_timestamp', return_value=True)
    def test_get_orphan_info(self, mock_timestamp, mock_bits, mock_next_bits, mock_validate_pow):
        """Test get_orphan_info method"""
        db = FakeDB()
        manager = ChainManager(db=db)

        async def run_test():
            orphan_hashes = []
            for i in range(3):
                fake_parent = f"ef{i:02d}" + "0" * 60
                orphan = self._make_block(100 + i, fake_parent, nonce=12345 + i)
                orphan_hashes.append(orphan["block_hash"])
                await manager.add_block(orphan)
                await asyncio.sleep(0.1)  # Small delay to ensure different timestamps

            info = await manager.get_orphan_info()

            assert info["count"] == 3
            assert len(info["orphans"]) == 3
            # Production sorts by height ascending
            result_hashes = [o["hash"] for o in info["orphans"]]
            assert result_hashes[0] == orphan_hashes[0]
            assert result_hashes[2] == orphan_hashes[2]

        asyncio.run(run_test())
