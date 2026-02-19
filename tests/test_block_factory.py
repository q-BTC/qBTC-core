"""
Tests for block factory module to ensure backward compatibility
"""

import json
import pytest
from blockchain.block_factory import (
    create_block, 
    normalize_block, 
    validate_block_structure,
    create_genesis_block,
    STANDARD_FIELD_ORDER
)


class TestBlockFactory:
    
    def test_create_block_basic(self):
        """Test basic block creation with required fields"""
        block = create_block(
            version=1,
            height=100,
            block_hash="abc123",
            previous_hash="def456",
            bits=0x1d00ffff,
            nonce=12345,
            timestamp=1234567890,
            merkle_root="merkle123",
            tx_ids=["tx1", "tx2"]
        )
        
        assert block["version"] == 1
        assert block["height"] == 100
        assert block["block_hash"] == "abc123"
        assert block["tx_ids"] == ["tx1", "tx2"]
        assert block["full_transactions"] == []  # Default value
        assert block["connected"] == False  # Default value
    
    def test_create_block_with_optionals(self):
        """Test block creation with optional fields"""
        block = create_block(
            version=1,
            height=100,
            block_hash="abc123",
            previous_hash="def456",
            bits=0x1d00ffff,
            nonce=12345,
            timestamp=1234567890,
            merkle_root="merkle123",
            tx_ids=["tx1"],
            full_transactions=[{"txid": "tx1"}],
            miner_address="miner_addr",
            connected=True,
            cumulative_difficulty="1000"
        )
        
        assert block["full_transactions"] == [{"txid": "tx1"}]
        assert block["miner_address"] == "miner_addr"
        assert block["connected"] == True
        assert block["cumulative_difficulty"] == "1000"
    
    def test_normalize_block_reorders_fields(self):
        """Test that normalize_block reorders fields correctly"""
        # Create a block with wrong field order (like in tests)
        messy_block = {
            "height": 100,  # Wrong order - height first
            "block_hash": "abc123",
            "previous_hash": "def456",
            "tx_ids": ["tx1"],
            "nonce": 12345,
            "timestamp": 1234567890,
            "miner_address": "miner",
            "merkle_root": "merkle",
            "version": 1,  # Wrong order - version last
            "bits": 0x1d00ffff
        }
        
        normalized = normalize_block(messy_block)
        
        # Check field order matches standard
        field_list = list(normalized.keys())
        expected_order = [f for f in STANDARD_FIELD_ORDER if f in normalized]
        assert field_list[:len(expected_order)] == expected_order
        
        # Check all fields preserved
        assert normalized["version"] == 1
        assert normalized["height"] == 100
        assert normalized["block_hash"] == "abc123"
    
    def test_normalize_block_adds_defaults(self):
        """Test that normalize_block adds default values"""
        minimal_block = {
            "version": 1,
            "height": 100,
            "block_hash": "abc123",
            "previous_hash": "def456",
            "bits": 0x1d00ffff,
            "nonce": 12345,
            "timestamp": 1234567890,
            "merkle_root": "merkle",
            "tx_ids": ["tx1"]
        }
        
        normalized = normalize_block(minimal_block, add_defaults=True)
        
        # Check defaults were added
        assert "full_transactions" in normalized
        assert normalized["full_transactions"] == []
        assert "connected" in normalized
        assert normalized["connected"] == False
    
    def test_validate_block_structure_valid(self):
        """Test validation of valid block"""
        block = create_block(
            version=1,
            height=100,
            block_hash="abc123",
            previous_hash="def456",
            bits=0x1d00ffff,
            nonce=12345,
            timestamp=1234567890,
            merkle_root="merkle123",
            tx_ids=["tx1"]
        )
        
        is_valid, error = validate_block_structure(block)
        assert is_valid
        assert error is None
    
    def test_validate_block_structure_missing_required(self):
        """Test validation catches missing required fields"""
        incomplete_block = {
            "version": 1,
            "height": 100,
            # Missing block_hash and other required fields
        }
        
        is_valid, error = validate_block_structure(incomplete_block)
        assert not is_valid
        assert "Missing required fields" in error
    
    def test_validate_block_structure_wrong_types(self):
        """Test validation catches wrong field types"""
        bad_block = {
            "version": "1",  # Should be int
            "height": 100,
            "block_hash": "abc123",
            "previous_hash": "def456",
            "bits": 0x1d00ffff,
            "nonce": 12345,
            "timestamp": 1234567890,
            "merkle_root": "merkle",
            "tx_ids": ["tx1"]
        }
        
        is_valid, error = validate_block_structure(bad_block)
        assert not is_valid
        assert "version must be an integer" in error
    
    def test_json_serialization_consistency(self):
        """Test that JSON serialization works regardless of field order"""
        block1 = create_block(
            version=1,
            height=100,
            block_hash="abc123",
            previous_hash="def456",
            bits=0x1d00ffff,
            nonce=12345,
            timestamp=1234567890,
            merkle_root="merkle123",
            tx_ids=["tx1"]
        )
        
        # Create same block with different field order
        block2 = {
            "height": 100,
            "version": 1,
            "block_hash": "abc123",
            "previous_hash": "def456",
            "bits": 0x1d00ffff,
            "nonce": 12345,
            "timestamp": 1234567890,
            "merkle_root": "merkle123",
            "tx_ids": ["tx1"],
            "full_transactions": [],
            "connected": False
        }
        
        # Normalize to same order (don't add defaults to block2)
        block2_normalized = normalize_block(block2, add_defaults=False)
        
        # Both blocks should have same keys after normalization
        assert set(block1.keys()) == set(block2_normalized.keys())
        
        # All values should match
        for key in block1:
            assert block1[key] == block2_normalized[key]
    
    def test_backward_compatibility_extra_fields(self):
        """Test that extra fields are preserved for backward compatibility"""
        block = create_block(
            version=1,
            height=100,
            block_hash="abc123",
            previous_hash="def456",
            bits=0x1d00ffff,
            nonce=12345,
            timestamp=1234567890,
            merkle_root="merkle123",
            tx_ids=["tx1"],
            # Extra field not in standard
            custom_field="custom_value",
            another_field=42
        )
        
        assert "custom_field" in block
        assert block["custom_field"] == "custom_value"
        assert "another_field" in block
        assert block["another_field"] == 42
    
    def test_genesis_block_creation(self):
        """Test genesis block creation"""
        genesis_tx = {
            "txid": "genesis_tx_id",
            "inputs": [],
            "outputs": [{"amount": "21000000"}]
        }
        
        genesis = create_genesis_block(
            genesis_tx=genesis_tx,
            genesis_address="genesis_miner",
            timestamp=1730000000
        )
        
        assert genesis["height"] == 0
        assert genesis["block_hash"] == "0" * 64
        assert genesis["previous_hash"] == "00" * 32
        assert genesis["tx_ids"] == ["genesis_tx_id"]
        assert genesis["full_transactions"] == [genesis_tx]
        assert genesis["miner_address"] == "genesis_miner"
        assert genesis["connected"] == False
        assert genesis["cumulative_difficulty"] == "0"
    
    def test_field_access_by_name(self):
        """Test that field access by name works regardless of order"""
        block = create_block(
            version=1,
            height=100,
            block_hash="abc123",
            previous_hash="def456",
            bits=0x1d00ffff,
            nonce=12345,
            timestamp=1234567890,
            merkle_root="merkle123",
            tx_ids=["tx1"]
        )
        
        # All existing code accesses fields like this
        assert block["version"] == 1
        assert block["height"] == 100
        assert block.get("full_transactions", []) == []
        assert block.get("miner_address") is None
        
        # This is safe regardless of field order
        for key in ["version", "height", "block_hash"]:
            assert key in block