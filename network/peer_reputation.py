"""
Peer reputation system for qBTC-core network
"""

import time
import logging
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from enum import Enum
from collections import defaultdict

logger = logging.getLogger(__name__)

class PeerBehavior(Enum):
    GOOD = "good"
    SUSPICIOUS = "suspicious"
    MALICIOUS = "malicious"
    BANNED = "banned"

@dataclass
class PeerMetrics:
    peer_id: str
    ip: str
    port: int
    first_seen: float
    last_seen: float
    
    # Connection metrics
    successful_connections: int = 0
    failed_connections: int = 0
    disconnections: int = 0
    
    # Message metrics
    valid_messages: int = 0
    invalid_messages: int = 0
    spam_messages: int = 0
    
    # Blockchain metrics
    valid_blocks: int = 0
    invalid_blocks: int = 0
    valid_transactions: int = 0
    invalid_transactions: int = 0
    
    # Performance metrics
    avg_response_time: float = 0.0
    timeouts: int = 0
    
    # Behavior tracking
    behavior: PeerBehavior = PeerBehavior.GOOD
    reputation_score: float = 100.0
    banned_until: float = 0
    warnings: int = 0
    
    def is_banned(self) -> bool:
        return time.time() < self.banned_until or self.behavior == PeerBehavior.BANNED
    
    def update_last_seen(self):
        self.last_seen = time.time()
    
    def get_age(self) -> float:
        return time.time() - self.first_seen
    
    def get_success_rate(self) -> float:
        total_connections = self.successful_connections + self.failed_connections
        if total_connections == 0:
            return 1.0
        return self.successful_connections / total_connections
    
    def get_message_validity_rate(self) -> float:
        total_messages = self.valid_messages + self.invalid_messages
        if total_messages == 0:
            return 1.0
        return self.valid_messages / total_messages
    
    def get_blockchain_validity_rate(self) -> float:
        total_blockchain_items = (
            self.valid_blocks + self.invalid_blocks + 
            self.valid_transactions + self.invalid_transactions
        )
        if total_blockchain_items == 0:
            return 1.0
        
        valid_items = self.valid_blocks + self.valid_transactions
        return valid_items / total_blockchain_items

class PeerReputationManager:
    """Manages peer reputation and behavior analysis"""

    def __init__(self):
        self.peers: Dict[str, PeerMetrics] = {}
        self.trusted_peers: Set[str] = set()  # Manually trusted peers
        self._disconnect_callbacks: List = []  # Callbacks fired on MALICIOUS/BANNED transitions
        self.reputation_thresholds = {
            PeerBehavior.GOOD: 70.0,
            PeerBehavior.SUSPICIOUS: 40.0,
            PeerBehavior.MALICIOUS: 20.0,
            PeerBehavior.BANNED: 0.0
        }
        
        # Weights for reputation calculation
        self.reputation_weights = {
            'connection_success': 0.2,
            'message_validity': 0.3,
            'blockchain_validity': 0.3,
            'response_time': 0.1,
            'age_bonus': 0.1
        }
    
    def register_disconnect_callback(self, fn):
        """Register a callback fired when a peer transitions to MALICIOUS or BANNED.
        Callback signature: fn(ip: str, port: int, reason: str)"""
        self._disconnect_callbacks.append(fn)

    def _fire_disconnect_callbacks(self, peer: PeerMetrics, reason: str):
        """Notify all registered callbacks that a peer should be disconnected."""
        for fn in self._disconnect_callbacks:
            try:
                fn(peer.ip, peer.port, reason)
            except Exception as e:
                logger.error(f"Disconnect callback error for {peer.peer_id}: {e}")

    def get_peer_id(self, ip: str, port: int) -> str:
        """Generate unique peer identifier"""
        return f"{ip}:{port}"
    
    def add_peer(self, ip: str, port: int) -> PeerMetrics:
        """Add new peer or get existing one"""
        peer_id = self.get_peer_id(ip, port)
        
        if peer_id not in self.peers:
            self.peers[peer_id] = PeerMetrics(
                peer_id=peer_id,
                ip=ip,
                port=port,
                first_seen=time.time(),
                last_seen=time.time()
            )
            logger.info(f"New peer added: {peer_id}")
        
        self.peers[peer_id].update_last_seen()
        return self.peers[peer_id]
    
    def record_connection_success(self, ip: str, port: int):
        """Record successful connection"""
        peer = self.add_peer(ip, port)
        peer.successful_connections += 1
        self._update_reputation(peer)
        
        logger.debug(f"Connection success recorded for {peer.peer_id}")
    
    def record_connection_failure(self, ip: str, port: int, reason: str = "unknown"):
        """Record failed connection"""
        peer = self.add_peer(ip, port)
        peer.failed_connections += 1
        self._update_reputation(peer)
        
        logger.warning(f"Connection failure recorded for {peer.peer_id}: {reason}")
    
    def record_disconnection(self, ip: str, port: int, reason: str = "unknown"):
        """Record peer disconnection"""
        peer_id = self.get_peer_id(ip, port)
        if peer_id in self.peers:
            peer = self.peers[peer_id]
            peer.disconnections += 1
            self._update_reputation(peer)
            
            logger.debug(f"Disconnection recorded for {peer_id}: {reason}")
    
    def record_valid_message(self, ip: str, port: int, message_type: str = "unknown"):
        """Record valid message received"""
        peer = self.add_peer(ip, port)
        peer.valid_messages += 1
        self._update_reputation(peer)
        
        logger.debug(f"Valid message recorded for {peer.peer_id}: {message_type}")
    
    def record_invalid_message(self, ip: str, port: int, reason: str = "unknown"):
        """Record invalid message received"""
        peer = self.add_peer(ip, port)
        peer.invalid_messages += 1
        self._update_reputation(peer)
        
        logger.warning(f"Invalid message recorded for {peer.peer_id}: {reason}")
        
        # Check for spam behavior
        if peer.invalid_messages > 10 and peer.get_message_validity_rate() < 0.5:
            self._flag_suspicious_behavior(peer, "high invalid message rate")
    
    def record_spam_message(self, ip: str, port: int):
        """Record spam message"""
        peer = self.add_peer(ip, port)
        peer.spam_messages += 1
        peer.invalid_messages += 1  # Spam counts as invalid
        self._update_reputation(peer)
        
        logger.warning(f"Spam message recorded for {peer.peer_id}")
        
        # Immediate penalty for spam
        if peer.spam_messages > 5:
            self._flag_malicious_behavior(peer, "spam flooding")
    
    def record_valid_block(self, ip: str, port: int, block_hash: str):
        """Record valid block received"""
        peer = self.add_peer(ip, port)
        peer.valid_blocks += 1
        self._update_reputation(peer)
        
        logger.info(f"Valid block recorded for {peer.peer_id}: {block_hash[:16]}...")
    
    def record_invalid_block(self, ip: str, port: int, reason: str):
        """Record invalid block received"""
        peer = self.add_peer(ip, port)
        peer.invalid_blocks += 1
        self._update_reputation(peer)
        
        logger.warning(f"Invalid block recorded for {peer.peer_id}: {reason}")
        
        # Check for malicious behavior
        if peer.invalid_blocks > 3:
            self._flag_suspicious_behavior(peer, "multiple invalid blocks")
    
    def record_valid_transaction(self, ip: str, port: int, tx_id: str):
        """Record valid transaction received"""
        peer = self.add_peer(ip, port)
        peer.valid_transactions += 1
        self._update_reputation(peer)
        
        logger.debug(f"Valid transaction recorded for {peer.peer_id}: {tx_id[:16]}...")
    
    def record_invalid_transaction(self, ip: str, port: int, reason: str):
        """Record invalid transaction received"""
        peer = self.add_peer(ip, port)
        peer.invalid_transactions += 1
        self._update_reputation(peer)
        
        logger.warning(f"Invalid transaction recorded for {peer.peer_id}: {reason}")
    
    def record_response_time(self, ip: str, port: int, response_time: float):
        """Record response time for request"""
        peer = self.add_peer(ip, port)
        
        # Update average response time
        if peer.avg_response_time == 0:
            peer.avg_response_time = response_time
        else:
            # Exponential moving average
            peer.avg_response_time = 0.9 * peer.avg_response_time + 0.1 * response_time
        
        self._update_reputation(peer)
    
    def record_timeout(self, ip: str, port: int):
        """Record request timeout"""
        peer = self.add_peer(ip, port)
        peer.timeouts += 1
        self._update_reputation(peer)
        
        logger.warning(f"Timeout recorded for {peer.peer_id}")
        
        # Check for reliability issues
        if peer.timeouts > 5:
            self._flag_suspicious_behavior(peer, "frequent timeouts")
    
    def _calculate_reputation_score(self, peer: PeerMetrics) -> float:
        """Calculate reputation score based on peer metrics"""
        weights = self.reputation_weights
        
        # Connection success component
        connection_score = peer.get_success_rate() * 100
        
        # Message validity component
        message_score = peer.get_message_validity_rate() * 100
        
        # Blockchain validity component
        blockchain_score = peer.get_blockchain_validity_rate() * 100
        
        # Response time component (penalty for slow responses)
        response_score = 100
        if peer.avg_response_time > 0:
            # Penalty starts at 5 seconds, full penalty at 30 seconds
            if peer.avg_response_time > 5.0:
                response_score = max(0, 100 - (peer.avg_response_time - 5.0) * 4)
        
        # Age bonus (trusted older peers)
        age_score = min(100, peer.get_age() / 3600 * 10)  # 10 points per hour, max 100
        
        # Calculate weighted score
        total_score = (
            connection_score * weights['connection_success'] +
            message_score * weights['message_validity'] +
            blockchain_score * weights['blockchain_validity'] +
            response_score * weights['response_time'] +
            age_score * weights['age_bonus']
        )
        
        # Apply penalties
        if peer.spam_messages > 0:
            total_score *= (1 - min(peer.spam_messages * 0.1, 0.8))
        
        if peer.timeouts > 0:
            total_score *= (1 - min(peer.timeouts * 0.05, 0.5))
        
        return max(0, min(100, total_score))
    
    def _update_reputation(self, peer: PeerMetrics):
        """Update peer reputation score and behavior"""
        old_score = peer.reputation_score
        peer.reputation_score = self._calculate_reputation_score(peer)
        
        # Update behavior based on score
        old_behavior = peer.behavior
        if peer.reputation_score >= self.reputation_thresholds[PeerBehavior.GOOD]:
            peer.behavior = PeerBehavior.GOOD
        elif peer.reputation_score >= self.reputation_thresholds[PeerBehavior.SUSPICIOUS]:
            peer.behavior = PeerBehavior.SUSPICIOUS
        elif peer.reputation_score >= self.reputation_thresholds[PeerBehavior.MALICIOUS]:
            peer.behavior = PeerBehavior.MALICIOUS
        else:
            peer.behavior = PeerBehavior.BANNED
        
        # Log significant changes and fire disconnect callbacks on escalation
        if old_behavior != peer.behavior:
            logger.warning(
                f"Peer {peer.peer_id} behavior changed: {old_behavior.value} -> {peer.behavior.value} "
                f"(score: {old_score:.1f} -> {peer.reputation_score:.1f})"
            )
            if peer.behavior in (PeerBehavior.MALICIOUS, PeerBehavior.BANNED):
                self._fire_disconnect_callbacks(
                    peer, f"reputation escalated to {peer.behavior.value}"
                )
    
    def _flag_suspicious_behavior(self, peer: PeerMetrics, reason: str):
        """Flag peer for suspicious behavior"""
        peer.warnings += 1
        logger.warning(f"Suspicious behavior flagged for {peer.peer_id}: {reason} (warning #{peer.warnings})")
        
        # Escalate to malicious if too many warnings
        if peer.warnings >= 3:
            self._flag_malicious_behavior(peer, "multiple suspicious activities")
    
    def _flag_malicious_behavior(self, peer: PeerMetrics, reason: str):
        """Flag peer as malicious"""
        peer.behavior = PeerBehavior.MALICIOUS
        peer.reputation_score = min(peer.reputation_score, 10.0)
        
        logger.error(f"Malicious behavior flagged for {peer.peer_id}: {reason}")
        
        # Auto-ban for severe malicious behavior
        if peer.warnings >= 5 or peer.spam_messages > 10:
            self.ban_peer(peer.ip, peer.port, reason="automatic ban for malicious behavior")
    
    def ban_peer(self, ip: str, port: int, duration: float = 86400, reason: str = "manual ban"):
        """Ban peer for specified duration"""
        peer = self.add_peer(ip, port)
        peer.behavior = PeerBehavior.BANNED
        peer.banned_until = time.time() + duration
        peer.reputation_score = 0.0

        logger.error(f"Peer {peer.peer_id} banned for {duration}s: {reason}")
        self._fire_disconnect_callbacks(peer, reason)
    
    def unban_peer(self, ip: str, port: int) -> bool:
        """Unban peer"""
        peer_id = self.get_peer_id(ip, port)
        if peer_id in self.peers:
            peer = self.peers[peer_id]
            peer.banned_until = 0
            peer.behavior = PeerBehavior.SUSPICIOUS  # Start as suspicious after unban
            peer.reputation_score = 50.0  # Give another chance
            
            logger.info(f"Peer {peer_id} unbanned")
            return True
        return False
    
    def get_peer_reputation(self, ip: str, port: int) -> Optional[PeerMetrics]:
        """Get peer reputation info"""
        peer_id = self.get_peer_id(ip, port)
        return self.peers.get(peer_id)
    
    def get_trusted_peers(self) -> List[PeerMetrics]:
        """Get list of trusted peers for priority connections"""
        trusted = []
        
        for peer in self.peers.values():
            if (peer.peer_id in self.trusted_peers or
                (peer.behavior == PeerBehavior.GOOD and 
                 peer.reputation_score > 80 and 
                 peer.get_age() > 3600)):  # At least 1 hour old
                trusted.append(peer)
        
        # Sort by reputation score
        trusted.sort(key=lambda p: p.reputation_score, reverse=True)
        return trusted
    
    def get_banned_peers(self) -> List[PeerMetrics]:
        """Get list of currently banned peers"""
        return [peer for peer in self.peers.values() if peer.is_banned()]
    
    def get_suspicious_peers(self) -> List[PeerMetrics]:
        """Get list of suspicious peers"""
        return [peer for peer in self.peers.values() 
                if peer.behavior in [PeerBehavior.SUSPICIOUS, PeerBehavior.MALICIOUS]]
    
    def should_connect_to_peer(self, ip: str, port: int) -> bool:
        """Check if we should connect to this peer"""
        peer_id = self.get_peer_id(ip, port)
        
        # Always allow trusted peers
        if peer_id in self.trusted_peers:
            return True
        
        # Check if peer exists in our records
        if peer_id in self.peers:
            peer = self.peers[peer_id]
            
            # Don't connect to banned peers
            if peer.is_banned():
                return False
            
            # Be cautious with malicious peers
            if peer.behavior == PeerBehavior.MALICIOUS:
                return False
            
            # Allow good and suspicious peers
            return True
        
        # Allow unknown peers (give them a chance)
        return True
    
    def get_reputation_summary(self) -> Dict:
        """Get summary of peer reputation system"""
        total_peers = len(self.peers)
        behavior_counts = defaultdict(int)
        
        for peer in self.peers.values():
            behavior_counts[peer.behavior.value] += 1
        
        return {
            "total_peers": total_peers,
            "trusted_peers": len(self.trusted_peers),
            "behavior_distribution": dict(behavior_counts),
            "banned_peers": len(self.get_banned_peers()),
            "average_reputation": sum(p.reputation_score for p in self.peers.values()) / max(total_peers, 1)
        }
    
    def cleanup_old_peers(self, max_age: float = 86400 * 7):  # 7 days
        """Remove old inactive peers"""
        now = time.time()
        to_remove = []
        
        for peer_id, peer in self.peers.items():
            if (now - peer.last_seen) > max_age and peer_id not in self.trusted_peers:
                to_remove.append(peer_id)
        
        for peer_id in to_remove:
            del self.peers[peer_id]
            logger.info(f"Removed old peer: {peer_id}")
        
        return len(to_remove)

# Global instance
peer_reputation_manager = PeerReputationManager()