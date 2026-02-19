"""
Block Factory Module
Provides consistent block creation and normalization across the codebase.

This module ensures all blocks have consistent field ordering and required fields,
while maintaining backward compatibility with existing code.
"""

from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

# Standard field ordering for blocks
# This ordering is logical: identification → chain → pow → transactions → metadata → state
STANDARD_FIELD_ORDER = [
    # Block identification
    "version",
    "height", 
    "block_hash",
    
    # Chain linkage
    "previous_hash",
    
    # Proof of Work
    "bits",
    "nonce",
    "timestamp",
    
    # Transaction data
    "merkle_root",
    "tx_ids",
    "full_transactions",
    
    # Mining info
    "miner_address",
    
    # Chain state (added by ChainManager)
    "connected",
    "cumulative_difficulty"
]

# Required fields that must be present in all blocks
REQUIRED_FIELDS = {
    "version",
    "height",
    "block_hash",
    "previous_hash",
    "bits",
    "nonce",
    "timestamp",
    "merkle_root",
    "tx_ids"
}

# Optional fields with their default values
OPTIONAL_FIELDS = {
    "full_transactions": [],
    "miner_address": None,
    "connected": False,
    "cumulative_difficulty": None
}


def create_block(
    version: int,
    height: int,
    block_hash: str,
    previous_hash: str,
    bits: int,
    nonce: int,
    timestamp: int,
    merkle_root: str,
    tx_ids: List[str],
    full_transactions: Optional[List[Dict]] = None,
    miner_address: Optional[str] = None,
    connected: bool = False,
    cumulative_difficulty: Optional[str] = None,
    **extra_fields
) -> Dict[str, Any]:
    """
    Create a new block with consistent field ordering.
    
    Args:
        version: Block version number
        height: Block height in chain
        block_hash: Block hash
        previous_hash: Previous block hash
        bits: Difficulty bits
        nonce: Mining nonce
        timestamp: Block timestamp
        merkle_root: Merkle root of transactions
        tx_ids: List of transaction IDs
        full_transactions: Optional full transaction data
        miner_address: Optional miner address
        connected: Whether block is connected to main chain
        cumulative_difficulty: Optional cumulative difficulty
        **extra_fields: Any additional fields (preserved for compatibility)
    
    Returns:
        Block dictionary with consistent field ordering
    """
    block = {}
    
    # Add fields in standard order
    block["version"] = version
    block["height"] = height
    block["block_hash"] = block_hash
    block["previous_hash"] = previous_hash
    block["bits"] = bits
    block["nonce"] = nonce
    block["timestamp"] = timestamp
    block["merkle_root"] = merkle_root
    block["tx_ids"] = tx_ids
    
    # Add optional fields if provided
    if full_transactions is not None:
        block["full_transactions"] = full_transactions
    else:
        block["full_transactions"] = []
    
    if miner_address is not None:
        block["miner_address"] = miner_address
    
    block["connected"] = connected
    
    if cumulative_difficulty is not None:
        block["cumulative_difficulty"] = cumulative_difficulty
    
    # Add any extra fields (for backward compatibility)
    for key, value in extra_fields.items():
        if key not in block:
            logger.debug(f"Adding extra field to block: {key}")
            block[key] = value
    
    return block


def normalize_block(block_data: Dict[str, Any], add_defaults: bool = True) -> Dict[str, Any]:
    """
    Normalize an existing block to have consistent field ordering.
    
    This function takes any block dictionary and returns a new dictionary
    with fields in the standard order. It preserves all existing fields
    and optionally adds default values for missing optional fields.
    
    Args:
        block_data: Existing block dictionary
        add_defaults: Whether to add default values for missing optional fields
    
    Returns:
        Normalized block dictionary with consistent field ordering
    
    Raises:
        ValueError: If required fields are missing
    """
    # Check for required fields
    missing_required = REQUIRED_FIELDS - set(block_data.keys())
    if missing_required:
        raise ValueError(f"Block missing required fields: {missing_required}")
    
    normalized = {}
    
    # Add fields in standard order
    for field in STANDARD_FIELD_ORDER:
        if field in block_data:
            normalized[field] = block_data[field]
        elif add_defaults and field in OPTIONAL_FIELDS:
            normalized[field] = OPTIONAL_FIELDS[field]
    
    # Add any extra fields not in standard order (for compatibility)
    for key, value in block_data.items():
        if key not in normalized:
            logger.debug(f"Preserving non-standard field: {key}")
            normalized[key] = value
    
    return normalized


def validate_block_structure(block_data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate that a block has all required fields and proper types.
    
    Args:
        block_data: Block dictionary to validate
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check required fields
    missing_required = REQUIRED_FIELDS - set(block_data.keys())
    if missing_required:
        return False, f"Missing required fields: {missing_required}"
    
    # Validate field types
    if not isinstance(block_data.get("version"), int):
        return False, "version must be an integer"
    
    if not isinstance(block_data.get("height"), int):
        return False, "height must be an integer"
    
    if not isinstance(block_data.get("block_hash"), str):
        return False, "block_hash must be a string"
    
    if not isinstance(block_data.get("previous_hash"), str):
        return False, "previous_hash must be a string"
    
    if not isinstance(block_data.get("bits"), int):
        return False, "bits must be an integer"
    
    if not isinstance(block_data.get("nonce"), int):
        return False, "nonce must be an integer"
    
    if not isinstance(block_data.get("timestamp"), (int, float)):
        return False, "timestamp must be a number"
    
    if not isinstance(block_data.get("merkle_root"), str):
        return False, "merkle_root must be a string"
    
    if not isinstance(block_data.get("tx_ids"), list):
        return False, "tx_ids must be a list"
    
    # Validate optional fields if present
    if "full_transactions" in block_data:
        if not isinstance(block_data["full_transactions"], list):
            return False, "full_transactions must be a list"
    
    if "miner_address" in block_data:
        if block_data["miner_address"] is not None and not isinstance(block_data["miner_address"], str):
            return False, "miner_address must be a string or None"
    
    if "connected" in block_data:
        if not isinstance(block_data["connected"], bool):
            return False, "connected must be a boolean"
    
    if "cumulative_difficulty" in block_data:
        if block_data["cumulative_difficulty"] is not None and not isinstance(block_data["cumulative_difficulty"], str):
            return False, "cumulative_difficulty must be a string or None"
    
    return True, None


def create_genesis_block(
    genesis_tx: Dict[str, Any],
    genesis_address: str = None,
    timestamp: int = 0,
    bits: int = 0x1d00ffff
) -> Dict[str, Any]:
    """
    Create a genesis block with proper structure.
    
    Args:
        genesis_tx: Genesis transaction
        genesis_address: Optional genesis miner address
        timestamp: Genesis timestamp (default: 0)
        bits: Genesis difficulty bits
    
    Returns:
        Genesis block dictionary
    """
    from blockchain.blockchain import calculate_merkle_root
    
    # Genesis block has special all-zeros hash
    genesis_hash = "0" * 64
    
    return create_block(
        version=1,
        height=0,
        block_hash=genesis_hash,
        previous_hash="00" * 32,
        bits=bits,
        nonce=0,
        timestamp=timestamp,
        merkle_root=calculate_merkle_root([genesis_tx["txid"]]),
        tx_ids=[genesis_tx["txid"]],
        full_transactions=[genesis_tx],
        miner_address=genesis_address,
        connected=False,
        cumulative_difficulty="0"
    )


# Backward compatibility: Allow importing individual functions
__all__ = [
    "create_block",
    "normalize_block",
    "validate_block_structure",
    "create_genesis_block",
    "STANDARD_FIELD_ORDER",
    "REQUIRED_FIELDS",
    "OPTIONAL_FIELDS"
]