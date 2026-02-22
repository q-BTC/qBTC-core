"""
Difficulty Adjustment Algorithm for qBTC
Implements Bitcoin-proportional difficulty adjustment for enhanced security
With 10-second blocks, adjusts every 120,960 blocks (2 weeks) to match Bitcoin's security model
"""
import json
import logging
from typing import Optional, Tuple, List
from config.config import DIFFICULTY_ADJUSTMENT_INTERVAL, BLOCK_TIME_TARGET
from blockchain.block_height_index import get_height_index

logger = logging.getLogger(__name__)

# Constants
MAX_TARGET_BITS = 0x1f7fffff  # Minimum difficulty bits (very easy)
MIN_TARGET_BITS = 0x1900ffff  # Maximum difficulty bits we'll allow
MAX_ADJUSTMENT_FACTOR = 4  # Maximum 4x increase
MIN_ADJUSTMENT_FACTOR = 0.25  # Maximum 4x decrease (1/4)

# Median Time Past
MTP_BLOCK_COUNT = 11  # Number of blocks to use for Median Time Past (matches Bitcoin)

# Time constraints
MAX_FUTURE_TIME = 2 * 60 * 60  # 2 hours in the future
MAX_PAST_TIME = 2 * 60 * 60  # 2 hours in the past


def compact_to_target(bits: int) -> int:
    """Convert compact bits representation to full target"""
    exponent = bits >> 24
    coefficient = bits & 0xffffff
    if exponent < 3:
        return coefficient >> (8 * (3 - exponent))
    if exponent > 32:
        raise ValueError(f"Invalid compact bits: exponent {exponent} exceeds maximum (32)")
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

    # Guard against division-by-zero when block_count <= 1
    if expected_time <= 0:
        expected_time = BLOCK_TIME_TARGET

    # Prevent negative or zero time
    if actual_time <= 0:
        logger.warning(f"Invalid actual time: {actual_time}, using expected time")
        actual_time = expected_time
    
    # Calculate adjustment ratio
    # When blocks are fast (actual < expected), ratio < 1, so we need to decrease target (increase difficulty)
    # When blocks are slow (actual > expected), ratio > 1, so we need to increase target (decrease difficulty)
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
        f"Difficulty adjustment at height {block_count}: "
        f"{old_difficulty:.2f} -> {new_difficulty:.2f} "
        f"(ratio: {ratio:.2f}, actual: {actual_time}s, expected: {expected_time}s, "
        f"interval: {DIFFICULTY_ADJUSTMENT_INTERVAL} blocks)"
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
        # First try height index (fast path)
        height_index = get_height_index()
        last_block = height_index.get_block_by_height(current_height)
        if last_block and last_block.get("bits") is not None:
            return last_block["bits"]
        
        # Fallback: scan the blockchain to find the block at this height
        # This is slower but ensures we can always find the correct difficulty
        logger.warning(f"Height index miss for height {current_height}, scanning blockchain")
        
        # Get the current chain tip and work backwards
        tip_key = b"chain:best_tip"
        tip_data = db.get(tip_key)
        if not tip_data:
            logger.error("No chain tip found")
            raise ValueError("Cannot determine difficulty: no chain tip")

        # chain:best_tip stores JSON {"hash": "...", "height": ...}
        try:
            tip_info = json.loads(tip_data.decode())
            block_hash = tip_info["hash"]
        except (json.JSONDecodeError, KeyError):
            # Fallback for raw hash format
            block_hash = tip_data.decode()
        MAX_WALK = 500  # Safety bound for chain walking
        walk_count = 0
        while block_hash:
            walk_count += 1
            if walk_count > MAX_WALK:
                raise ValueError(f"Cannot determine difficulty: chain walk exceeded {MAX_WALK} blocks")

            block_key = f"block:{block_hash}".encode()
            block_data = db.get(block_key)
            if not block_data:
                logger.error(f"Block {block_hash} not found in database")
                raise ValueError(f"Cannot determine difficulty: block {block_hash} not found")

            block = json.loads(block_data.decode())
            if block["height"] == current_height:
                if block.get("bits") is None:
                    logger.error(f"Block at height {current_height} has no bits field")
                    raise ValueError(f"Cannot determine difficulty: block at height {current_height} missing bits field")
                return block["bits"]
            elif block["height"] < current_height:
                logger.error(f"Could not find block at height {current_height}")
                raise ValueError(f"Cannot determine difficulty: block at height {current_height} not found")

            block_hash = block.get("previous_hash")

        logger.error(f"Reached genesis without finding height {current_height}")
        raise ValueError(f"Cannot determine difficulty: block at height {current_height} not found")
    
    # Find the first and last block of the interval
    interval_start_height = current_height - DIFFICULTY_ADJUSTMENT_INTERVAL + 1
    
    height_index = get_height_index()
    first_block = height_index.get_block_by_height(interval_start_height)
    last_block = height_index.get_block_by_height(current_height)
    
    # If height index fails, try direct lookup
    if not first_block or not last_block:
        logger.warning(f"Height index miss for difficulty adjustment, using direct lookup")
        
        # Get chain tip and walk back
        tip_key = b"chain:best_tip"
        tip_data = db.get(tip_key)
        if not tip_data:
            raise ValueError("Cannot calculate difficulty: no chain tip")

        # chain:best_tip stores JSON {"hash": "...", "height": ...}
        try:
            tip_info = json.loads(tip_data.decode())
            block_hash = tip_info["hash"]
        except (json.JSONDecodeError, KeyError):
            # Fallback for raw hash format
            block_hash = tip_data.decode()

        # Find blocks by walking the chain (bounded)
        blocks_needed = {interval_start_height: None, current_height: None}
        max_walk = DIFFICULTY_ADJUSTMENT_INTERVAL + 100  # Only need to walk one interval
        walk_count = 0

        while block_hash and (blocks_needed[interval_start_height] is None or blocks_needed[current_height] is None):
            walk_count += 1
            if walk_count > max_walk:
                raise ValueError(f"Cannot calculate difficulty: chain walk exceeded {max_walk} blocks")

            block_key = f"block:{block_hash}".encode()
            block_data = db.get(block_key)
            if not block_data:
                raise ValueError(f"Cannot calculate difficulty: block {block_hash} not found")

            block = json.loads(block_data.decode())
            height = block["height"]

            if height in blocks_needed:
                blocks_needed[height] = block

            if height < interval_start_height:
                break

            block_hash = block.get("previous_hash")
        
        first_block = blocks_needed[interval_start_height]
        last_block = blocks_needed[current_height]
        
        if not first_block or not last_block:
            raise ValueError(f"Cannot calculate difficulty adjustment: missing blocks at heights {interval_start_height} or {current_height}")
    
    # Calculate new difficulty
    last_bits = last_block.get("bits")
    if last_bits is None:
        raise ValueError(f"Block at height {current_height} missing bits field for difficulty adjustment")
    
    first_timestamp = first_block.get("timestamp")
    last_timestamp = last_block.get("timestamp")
    if first_timestamp is None or last_timestamp is None:
        raise ValueError(f"Blocks missing timestamp fields for difficulty adjustment")
    
    return calculate_next_bits(
        last_bits,
        first_timestamp,
        last_timestamp,
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


def get_median_time_past(height: int, block_index: dict, db=None) -> Optional[int]:
    """
    Calculate the Median Time Past (MTP) for a given height.
    MTP is the median of the timestamps of the last MTP_BLOCK_COUNT (11) blocks.

    Args:
        height: The height for which to compute MTP (uses blocks at heights height-1 down to height-11)
        block_index: In-memory block index mapping hash -> block metadata
        db: Database instance (fallback if block_index doesn't have timestamps)

    Returns:
        The median timestamp, or None if not enough blocks exist
    """
    if height < MTP_BLOCK_COUNT:
        return None  # Not enough blocks to compute MTP

    # Collect timestamps from the last MTP_BLOCK_COUNT blocks
    # We need to find blocks by height, so use the height index
    height_idx = get_height_index()
    timestamps: List[int] = []

    for h in range(height - MTP_BLOCK_COUNT, height):
        block_info = height_idx.get_block_by_height(h)
        if block_info and block_info.get("timestamp") is not None:
            timestamps.append(block_info["timestamp"])
        else:
            # Fallback: search block_index for a block at this height
            found = False
            for bhash, binfo in block_index.items():
                if binfo.get("height") == h and binfo.get("timestamp") is not None:
                    timestamps.append(binfo["timestamp"])
                    found = True
                    break
            if not found:
                logger.warning(f"Cannot find block at height {h} for MTP calculation")
                return None

    if len(timestamps) < MTP_BLOCK_COUNT:
        return None

    timestamps.sort()
    return timestamps[len(timestamps) // 2]


def validate_block_timestamp(timestamp: int, previous_timestamp: int, current_time: int,
                             median_time_past: Optional[int] = None) -> bool:
    """
    Validate block timestamp against rules

    Args:
        timestamp: Block timestamp to validate
        previous_timestamp: Timestamp of previous block
        current_time: Current system time
        median_time_past: If provided, block timestamp must be > MTP

    Returns:
        True if valid, False otherwise
    """
    # If MTP is provided, block timestamp must be strictly greater than MTP
    if median_time_past is not None and timestamp <= median_time_past:
        logger.warning(f"Block timestamp {timestamp} not greater than median time past {median_time_past}")
        return False

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
