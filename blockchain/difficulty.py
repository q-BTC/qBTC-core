"""
Difficulty Adjustment Algorithm for qBTC
Implements a Bitcoin-style difficulty adjustment with improvements
"""
import json
import logging
from typing import Optional, Tuple
from config.config import DIFFICULTY_ADJUSTMENT_INTERVAL, BLOCK_TIME_TARGET
from blockchain.block_height_index import get_height_index

logger = logging.getLogger(__name__)

# Constants
MAX_TARGET_BITS = 0x1f7fffff  # Minimum difficulty bits (very easy)
MIN_TARGET_BITS = 0x1900ffff  # Maximum difficulty bits we'll allow
MAX_ADJUSTMENT_FACTOR = 4  # Maximum 4x increase
MIN_ADJUSTMENT_FACTOR = 0.25  # Maximum 4x decrease (1/4)

# Time constraints
MAX_FUTURE_TIME = 2 * 60 * 60  # 2 hours in the future
MAX_PAST_TIME = 2 * 60 * 60  # 2 hours in the past


def compact_to_target(bits: int) -> int:
    """Convert compact bits representation to full target"""
    exponent = bits >> 24
    coefficient = bits & 0xffffff
    return coefficient * (1 << (8 * (exponent - 3)))


def target_to_compact(target: int) -> int:
    """Convert full target to compact bits representation"""
    # Find the most significant byte
    for i in range(31, -1, -1):
        if target >> (i * 8):
            break
    else:
        return 0
    
    # Extract coefficient (3 bytes)
    if i >= 2:
        coefficient = (target >> ((i - 2) * 8)) & 0xffffff
    else:
        coefficient = (target << ((2 - i) * 8)) & 0xffffff
    
    # Normalize if coefficient has its highest bit set
    if coefficient & 0x800000:
        coefficient >>= 8
        i += 1
    
    # Construct compact representation
    return (i + 1) << 24 | coefficient


def calculate_next_bits(
    last_bits: int,
    first_timestamp: int,
    last_timestamp: int,
    block_count: int = DIFFICULTY_ADJUSTMENT_INTERVAL
) -> int:
    """
    Calculate the next difficulty bits based on the time taken for the last interval
    
    Args:
        last_bits: The current difficulty bits
        first_timestamp: Timestamp of the first block in the interval
        last_timestamp: Timestamp of the last block in the interval
        block_count: Number of blocks in the interval (should be DIFFICULTY_ADJUSTMENT_INTERVAL)
    
    Returns:
        New difficulty bits
    """
    # Calculate actual time taken
    actual_time = last_timestamp - first_timestamp
    
    # Calculate expected time
    expected_time = BLOCK_TIME_TARGET * (block_count - 1)  # -1 because we measure between blocks
    
    # Prevent negative or zero time
    if actual_time <= 0:
        logger.warning(f"Invalid actual time: {actual_time}, using expected time")
        actual_time = expected_time
    
    # Calculate adjustment ratio
    # When blocks are fast (actual < expected), ratio > 1, so we need to decrease target
    # When blocks are slow (actual > expected), ratio < 1, so we need to increase target
    ratio = actual_time / expected_time
    
    # Apply limits to prevent attacks
    if ratio > MAX_ADJUSTMENT_FACTOR:
        ratio = MAX_ADJUSTMENT_FACTOR
        logger.info(f"Difficulty adjustment capped at {MAX_ADJUSTMENT_FACTOR}x increase")
    elif ratio < MIN_ADJUSTMENT_FACTOR:
        ratio = MIN_ADJUSTMENT_FACTOR
        logger.info(f"Difficulty adjustment capped at {MIN_ADJUSTMENT_FACTOR}x decrease")
    
    # Convert current bits to target
    current_target = compact_to_target(last_bits)
    
    # Calculate new target (inverse relationship: higher target = lower difficulty)
    new_target = int(current_target * ratio)
    
    # Ensure target stays within bounds
    max_target = compact_to_target(MAX_TARGET_BITS)
    min_target = compact_to_target(MIN_TARGET_BITS)
    
    if new_target > max_target:
        new_target = max_target
        logger.info("Difficulty adjustment hit minimum difficulty limit")
    elif new_target < min_target:
        new_target = min_target
        logger.info("Difficulty adjustment hit maximum difficulty limit")
    
    # Convert back to compact format
    new_bits = target_to_compact(new_target)
    
    # Log the adjustment
    old_difficulty = (1 << 256) / current_target
    new_difficulty = (1 << 256) / new_target
    logger.info(
        f"Difficulty adjustment: {old_difficulty:.2f} -> {new_difficulty:.2f} "
        f"(ratio: {ratio:.2f}, actual: {actual_time}s, expected: {expected_time}s)"
    )
    
    return new_bits


def get_next_bits(db, current_height: int) -> int:
    """
    Get the difficulty bits for the next block
    
    Args:
        db: Database instance
        current_height: Current blockchain height
        
    Returns:
        Difficulty bits for the next block
    """
    # Check if we need to adjust difficulty
    next_height = current_height + 1
    
    # Genesis and early blocks use minimum difficulty
    if current_height < DIFFICULTY_ADJUSTMENT_INTERVAL:
        return MAX_TARGET_BITS
    
    # Only adjust at interval boundaries
    if next_height % DIFFICULTY_ADJUSTMENT_INTERVAL != 0:
        # Use the same difficulty as the last block
        height_index = get_height_index()
        last_block = height_index.get_block_by_height(current_height)
        if last_block:
            return last_block.get("bits", MAX_TARGET_BITS)
        return MAX_TARGET_BITS
    
    # Find the first and last block of the interval
    interval_start_height = current_height - DIFFICULTY_ADJUSTMENT_INTERVAL + 1
    
    height_index = get_height_index()
    first_block = height_index.get_block_by_height(interval_start_height)
    last_block = height_index.get_block_by_height(current_height)
    
    if not first_block or not last_block:
        logger.error(f"Could not find blocks for difficulty adjustment at height {current_height}")
        return MAX_TARGET_BITS
    
    # Calculate new difficulty
    return calculate_next_bits(
        last_block.get("bits", MAX_TARGET_BITS),
        first_block.get("timestamp"),
        last_block.get("timestamp"),
        DIFFICULTY_ADJUSTMENT_INTERVAL
    )


def validate_block_bits(block_bits: int, expected_bits: int) -> bool:
    """
    Validate that a block has the correct difficulty bits
    
    Args:
        block_bits: The bits field from the block
        expected_bits: The expected bits based on difficulty adjustment
        
    Returns:
        True if valid, False otherwise
    """
    if block_bits != expected_bits:
        logger.warning(f"Block has incorrect difficulty: {block_bits:#x} != {expected_bits:#x}")
        return False
    return True


def validate_block_timestamp(timestamp: int, previous_timestamp: int, current_time: int) -> bool:
    """
    Validate block timestamp against rules
    
    Args:
        timestamp: Block timestamp to validate
        previous_timestamp: Timestamp of previous block
        current_time: Current system time
        
    Returns:
        True if valid, False otherwise
    """
    # Must be greater than previous block
    if timestamp <= previous_timestamp:
        logger.warning(f"Block timestamp {timestamp} not greater than previous {previous_timestamp}")
        return False
    
    # Cannot be too far in the future
    if timestamp > current_time + MAX_FUTURE_TIME:
        logger.warning(f"Block timestamp {timestamp} too far in future (current: {current_time})")
        return False
    
    # Cannot be too far in the past relative to previous block
    if timestamp < previous_timestamp - MAX_PAST_TIME:
        logger.warning(f"Block timestamp {timestamp} too far in past relative to previous")
        return False
    
    return True


# Import json here to avoid circular imports
import json