"""
Broadcast tracker to prevent message storms
"""
import time
from typing import Set, Dict, Tuple
import logging

logger = logging.getLogger(__name__)

class BroadcastTracker:
    """Track recently broadcast messages to prevent storms"""
    
    def __init__(self, ttl: int = 60):
        """
        Initialize tracker
        Args:
            ttl: Time-to-live in seconds for tracking entries
        """
        self.ttl = ttl
        # Track: (message_type, message_id) -> timestamp
        self.recent_broadcasts: Dict[Tuple[str, str], float] = {}
        # Track: (message_id, peer_address) -> timestamp  
        self.peer_sends: Dict[Tuple[str, str], float] = {}
        
    def should_broadcast(self, msg_type: str, msg_id: str, from_peer: str = None) -> bool:
        """
        Check if we should broadcast this message
        
        Args:
            msg_type: Type of message (e.g., "blocks_response")
            msg_id: Unique message identifier (e.g., block_hash)
            from_peer: Peer who sent us this message
            
        Returns:
            True if we should broadcast, False if we should skip
        """
        current_time = time.time()
        self._cleanup_old_entries(current_time)
        
        key = (msg_type, msg_id)
        
        # Check if we've broadcast this recently
        if key in self.recent_broadcasts:
            time_since_broadcast = current_time - self.recent_broadcasts[key]
            if time_since_broadcast < self.ttl:
                logger.debug(f"Skipping broadcast of {msg_type}:{msg_id[:8]}... (broadcast {time_since_broadcast:.1f}s ago)")
                return False
        
        # Mark as broadcast
        self.recent_broadcasts[key] = current_time
        return True
    
    def should_send_to_peer(self, msg_id: str, peer_address: str) -> bool:
        """
        Check if we should send this message to a specific peer
        
        Args:
            msg_id: Message identifier
            peer_address: Peer address (IP:port or identifier)
            
        Returns:
            True if we should send, False if peer already has it
        """
        current_time = time.time()
        key = (msg_id, str(peer_address))
        
        # Check if we've sent to this peer recently
        if key in self.peer_sends:
            time_since_send = current_time - self.peer_sends[key]
            if time_since_send < self.ttl:
                return False
        
        # Mark as sent to this peer
        self.peer_sends[key] = current_time
        return True
    
    def mark_peer_has_block(self, block_hash: str, peer_address: str):
        """Mark that a peer already has a block"""
        key = (block_hash, str(peer_address))
        self.peer_sends[key] = time.time()
    
    def _cleanup_old_entries(self, current_time: float):
        """Remove expired entries to prevent memory leak"""
        # Clean broadcast cache
        expired_broadcasts = [
            key for key, timestamp in self.recent_broadcasts.items()
            if current_time - timestamp > self.ttl * 2
        ]
        for key in expired_broadcasts:
            del self.recent_broadcasts[key]
        
        # Clean peer sends cache  
        expired_sends = [
            key for key, timestamp in self.peer_sends.items()
            if current_time - timestamp > self.ttl * 2
        ]
        for key in expired_sends:
            del self.peer_sends[key]
        
        # Log cleanup if significant
        if len(expired_broadcasts) > 100 or len(expired_sends) > 100:
            logger.debug(f"Cleaned up {len(expired_broadcasts)} broadcasts and {len(expired_sends)} peer sends")

# Global instance
_broadcast_tracker = None

def get_broadcast_tracker() -> BroadcastTracker:
    """Get or create the global broadcast tracker"""
    global _broadcast_tracker
    if _broadcast_tracker is None:
        _broadcast_tracker = BroadcastTracker(ttl=30)  # 30 second TTL
    return _broadcast_tracker