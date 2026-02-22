import asyncio
import hashlib
import json
import os
import time
import random
from collections import deque
from asyncio import StreamReader, StreamWriter
from config.config import (
    DEFAULT_GOSSIP_PORT,
    MAX_INBOUND_CONNECTIONS,
    MAX_OUTBOUND_CONNECTIONS,
    GOSSIP_RATE_LIMIT_WINDOW,
    GOSSIP_RATE_LIMIT_TX,
    GOSSIP_RATE_LIMIT_BLOCK,
    GOSSIP_RATE_LIMIT_QUERY,
    GOSSIP_RATE_LIMIT_DEFAULT,
    MAX_PEERS_PER_SUBNET,
    NUM_ANCHOR_PEERS,
    BANNED_PEER_DURATION,
    MAX_CONNECTIONS_PER_IP,
)
from state.state import mempool_manager
from wallet.wallet import verify_transaction
from database.database import get_db, get_current_height
from blockchain.block_height_index import get_height_index
from dht.dht import push_blocks
from sync.sync import process_blocks_from_peer
from network.peer_reputation import peer_reputation_manager
from log_utils import get_logger
from gossip.broadcast_tracker import get_broadcast_tracker

logger = get_logger(__name__)
broadcast_tracker = get_broadcast_tracker()

# Import NAT traversal
try:
    from network.nat_traversal import TCPHolePuncher, nat_traversal
    NAT_TRAVERSAL_AVAILABLE = True
except ImportError:
    TCPHolePuncher = None
    nat_traversal = None
    NAT_TRAVERSAL_AVAILABLE = False

GENESIS_HASH = "0" * 64
MAX_LINE_BYTES = 5 * 1024 * 1024  # 5MB — sufficient for largest valid block
MAX_SEEN_TX_SIZE = 10000  # Maximum number of transaction IDs to remember

# Known valid gossip message types for deserialization safety (C12)
_VALID_MSG_TYPES = {
    "transaction", "blocks_response", "blocks_by_hash_response",
    "blocks_response_chunked", "get_height", "get_blocks",
    "get_blocks_by_hash", "get_transactions", "height_response",
    "transactions_response", "error",
}

# Map gossip message types to rate-limit categories
_MSG_TYPE_CATEGORY = {
    "transaction": "tx",
    "blocks_response": "block",
    "blocks_by_hash_response": "block",
    "blocks_response_chunked": "block",
    "get_height": "query",
    "get_blocks": "query",
    "get_blocks_by_hash": "query",
    "get_transactions": "query",
    "height_response": "default",
}

_CATEGORY_LIMITS = {
    "tx": GOSSIP_RATE_LIMIT_TX,
    "block": GOSSIP_RATE_LIMIT_BLOCK,
    "query": GOSSIP_RATE_LIMIT_QUERY,
    "default": GOSSIP_RATE_LIMIT_DEFAULT,
}


class GossipRateLimiter:
    """Per-peer, per-category sliding-window rate limiter for gossip messages."""

    def __init__(self, window: int = GOSSIP_RATE_LIMIT_WINDOW):
        self.window = window
        # {peer_key: {category: deque_of_timestamps}}
        self._buckets: dict[str, dict[str, deque]] = {}

    def check_rate_limit(self, peer_ip: str, peer_port: int, msg_type: str) -> bool:
        """Return True if the message is within limits, False if rate-limited."""
        now = time.monotonic()
        key = f"{peer_ip}:{peer_port}"
        category = _MSG_TYPE_CATEGORY.get(msg_type, "default")
        limit = _CATEGORY_LIMITS.get(category, GOSSIP_RATE_LIMIT_DEFAULT)

        peer_buckets = self._buckets.setdefault(key, {})
        dq = peer_buckets.setdefault(category, deque())

        # Evict timestamps outside the window
        cutoff = now - self.window
        while dq and dq[0] < cutoff:
            dq.popleft()

        if len(dq) >= limit:
            return False

        dq.append(now)
        return True

    def cleanup_peer(self, peer_ip: str, peer_port: int):
        """Remove all state for a disconnected peer."""
        self._buckets.pop(f"{peer_ip}:{peer_port}", None)

class GossipNode:
    def __init__(self, node_id, wallet=None, is_bootstrap=False, is_full_node=True):
        self.node_id = node_id
        self.wallet = wallet
        self.seen_tx = set()
        self.seen_tx_timestamps = {}  # Track when we first saw each tx
        self.tx_stats = {
            'received': 0,
            'duplicates': 0,
            'new': 0,
            'invalid': 0
        }
        self.dht_peers = set()
        self.client_peers = set()
        self.failed_peers = {}
        self.peer_info = {}  # Store full peer info for NAT traversal
        self.server_task = None
        self.server = None
        self.partition_task = None
        self.sync_task = None
        self.cleanup_task = None
        self.is_bootstrap = is_bootstrap
        self.is_full_node = is_full_node
        self.gossip_port = None  # Will be set when server starts
        self.synced_peers = set()
        self.bootstrap_peer = None
        self.ip_to_peer = {}  # Track IP -> (port, validator_id, timestamp) mapping
        self.peer_timestamps = {}  # Track (ip, port) -> timestamp
        # CRITICAL: Protected peers that must NEVER be removed
        self.protected_peers = set()  # Peers that are critical for network operation

        # --- Gossip hardening state ---
        self.gossip_rate_limiter = GossipRateLimiter()
        self.inbound_connections = 0
        self._inbound_lock = asyncio.Lock()
        # H7: Temporary ban tracking {ip: expiry_timestamp}
        self.banned_peers: dict[str, float] = {}
        # C13: Per-IP connection tracking {ip: active_count}
        self.connections_per_ip: dict[str, int] = {}
        # Subnet diversity: /16 prefix -> count of connected peers
        self.subnet_counts: dict[str, int] = {}
        # Anchor peers file path (persistent across restarts)
        self._anchor_path = os.path.join(
            os.environ.get("ROCKSDB_PATH", "/app/db"), "anchor_peers.json"
        )

    async def start_server(self, host="0.0.0.0", port=DEFAULT_GOSSIP_PORT):
        self.gossip_port = port  # Store for NAT traversal
        self.server = await asyncio.start_server(self.handle_client, host, port, limit=MAX_LINE_BYTES)
        self.server_task = asyncio.create_task(self.server.serve_forever())
        # Enable partition check to recover failed peers
        self.partition_task = asyncio.create_task(self.check_partition())
        # Start periodic sync task
        self.sync_task = asyncio.create_task(self.periodic_sync())
        # Start periodic cleanup task
        self.cleanup_task = asyncio.create_task(self.periodic_cleanup())

        # Register auto-disconnect callback with the reputation system
        peer_reputation_manager.register_disconnect_callback(
            self._on_peer_reputation_disconnect
        )

        # Load anchor peers from last shutdown
        self._load_anchor_peers()

        logger.info(
            f"Gossip server started on {host}:{port} "
            f"(max_in={MAX_INBOUND_CONNECTIONS}, max_out={MAX_OUTBOUND_CONNECTIONS}, "
            f"rate_window={GOSSIP_RATE_LIMIT_WINDOW}s, subnet_limit={MAX_PEERS_PER_SUBNET})"
        )

    def _cleanup_expired_bans(self):
        """Remove expired bans from the banned_peers dict."""
        now = time.time()
        expired = [ip for ip, expiry in self.banned_peers.items() if now >= expiry]
        for ip in expired:
            del self.banned_peers[ip]

    async def handle_client(self, reader: StreamReader, writer: StreamWriter):
        peer_info = writer.get_extra_info('peername')
        peer_ip, peer_port = peer_info[0], peer_info[1]

        # H7: Check if peer is temporarily banned
        self._cleanup_expired_bans()
        if peer_ip in self.banned_peers:
            logger.warning(f"Rejecting banned peer {peer_ip}:{peer_port}")
            writer.close()
            await writer.wait_closed()
            return

        # --- Inbound connection limit ---
        async with self._inbound_lock:
            if self.inbound_connections >= MAX_INBOUND_CONNECTIONS:
                logger.warning(
                    f"Rejecting {peer_ip}:{peer_port} — at inbound limit "
                    f"({self.inbound_connections}/{MAX_INBOUND_CONNECTIONS})"
                )
                writer.close()
                await writer.wait_closed()
                return
            # C13: Per-IP connection limit
            current_ip_conns = self.connections_per_ip.get(peer_ip, 0)
            if current_ip_conns >= MAX_CONNECTIONS_PER_IP:
                logger.warning(
                    f"Rejecting {peer_ip}:{peer_port} — IP already has "
                    f"{current_ip_conns}/{MAX_CONNECTIONS_PER_IP} connections"
                )
                writer.close()
                await writer.wait_closed()
                return
            self.inbound_connections += 1
            self.connections_per_ip[peer_ip] = current_ip_conns + 1

        logger.info(f"New peer connection: {peer_ip}:{peer_port}")

        # Check peer reputation before accepting connection
        if not peer_reputation_manager.should_connect_to_peer(peer_ip, peer_port):
            logger.warning(f"Rejecting connection from banned/malicious peer {peer_ip}:{peer_port}")
            async with self._inbound_lock:
                self.inbound_connections -= 1
                ip_count = self.connections_per_ip.get(peer_ip, 1)
                if ip_count <= 1:
                    self.connections_per_ip.pop(peer_ip, None)
                else:
                    self.connections_per_ip[peer_ip] = ip_count - 1
            writer.close()
            await writer.wait_closed()
            return

        # Record successful connection
        peer_reputation_manager.record_connection_success(peer_ip, peer_port)

        if peer_info not in self.client_peers and peer_info not in self.dht_peers:
            self.client_peers.add(peer_info)
            logger.info(f"Added temporary client peer {peer_info}")

        start_time = time.time()
        try:
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=10)
                if not line:
                    break

                # C12: Explicit line size check before parsing
                if len(line) > MAX_LINE_BYTES:
                    logger.warning(f"Oversized message ({len(line)} bytes) from {peer_ip}:{peer_port} — disconnecting")
                    peer_reputation_manager.record_spam_message(peer_ip, peer_port)
                    break

                try:
                    # C12: Guard against deeply nested JSON causing recursion
                    try:
                        msg = json.loads(line.decode('utf-8').strip())
                    except RecursionError:
                        logger.warning(f"Deeply nested JSON from {peer_ip}:{peer_port} — disconnecting")
                        peer_reputation_manager.record_invalid_message(peer_ip, peer_port, "recursion_bomb")
                        break

                    # C12: Validate required 'type' field and known message type
                    msg_type = msg.get("type", "unknown")
                    if not isinstance(msg_type, str) or msg_type not in _VALID_MSG_TYPES:
                        logger.warning(f"Unknown message type '{msg_type}' from {peer_ip}:{peer_port}")
                        peer_reputation_manager.record_invalid_message(peer_ip, peer_port, f"unknown_type:{msg_type}")
                        continue  # skip unknown types but don't disconnect

                    # --- Per-peer gossip rate limiting ---
                    if not self.gossip_rate_limiter.check_rate_limit(
                        peer_ip, peer_port, msg_type
                    ):
                        # H7: Ban peer for BANNED_PEER_DURATION on rate limit violation
                        self.banned_peers[peer_ip] = time.time() + BANNED_PEER_DURATION
                        logger.warning(
                            f"Rate limit exceeded for {peer_ip}:{peer_port} "
                            f"on {msg_type} — banning for {BANNED_PEER_DURATION}s and disconnecting"
                        )
                        peer_reputation_manager.record_spam_message(peer_ip, peer_port)
                        break  # disconnect the flooding peer

                    await self.handle_gossip_message(msg, peer_info, writer)

                    # Record valid message
                    peer_reputation_manager.record_valid_message(
                        peer_ip, peer_port, msg_type
                    )

                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON from {peer_info}: {e}")
                    peer_reputation_manager.record_invalid_message(
                        peer_ip, peer_port, "invalid_json"
                    )
                except Exception as e:
                    logger.error(f"Error processing message from {peer_info}: {e}")
                    peer_reputation_manager.record_invalid_message(
                        peer_ip, peer_port, str(e)
                    )

        except asyncio.TimeoutError:
            logger.warning(f"Timeout handling client {peer_info}")
            peer_reputation_manager.record_timeout(peer_ip, peer_port)
        except Exception as e:
            logger.error(f"Error handling client {peer_info}: {e}")
            peer_reputation_manager.record_connection_failure(
                peer_ip, peer_port, str(e)
            )
        finally:
            # Decrement inbound counter, per-IP counter, and clean up rate limiter state
            async with self._inbound_lock:
                self.inbound_connections -= 1
                ip_count = self.connections_per_ip.get(peer_ip, 1)
                if ip_count <= 1:
                    self.connections_per_ip.pop(peer_ip, None)
                else:
                    self.connections_per_ip[peer_ip] = ip_count - 1
            self.gossip_rate_limiter.cleanup_peer(peer_ip, peer_port)

            # Record disconnection
            peer_reputation_manager.record_disconnection(peer_ip, peer_port)

            # Record response time
            response_time = time.time() - start_time
            peer_reputation_manager.record_response_time(peer_ip, peer_port, response_time)

            if peer_info in self.client_peers:
                self.client_peers.remove(peer_info)
                logger.info(f"Removed temporary client peer {peer_info}")

            writer.close()
            await writer.wait_closed()


    

   



    def _cleanup_seen_tx(self):
        """Remove oldest transaction IDs when we exceed the maximum size."""
        if len(self.seen_tx) <= MAX_SEEN_TX_SIZE:
            return
        
        # Sort by timestamp and remove the oldest ones
        # Copy items to avoid RuntimeError from dict mutation during iteration
        sorted_txs = sorted(list(self.seen_tx_timestamps.items()), key=lambda x: x[1])
        to_remove = len(self.seen_tx) - int(MAX_SEEN_TX_SIZE * 0.9)  # Remove 10% when at capacity

        for txid, _ in sorted_txs[:to_remove]:
            self.seen_tx.discard(txid)
            self.seen_tx_timestamps.pop(txid, None)
    
    async def handle_gossip_message(self, msg, from_peer, writer):
        # M2: Verify message integrity hash if present
        received_hash = msg.pop("msg_hash", None)
        if received_hash is not None:
            verify_json = json.dumps(msg, sort_keys=True)
            expected_hash = hashlib.sha256(verify_json.encode('utf-8')).hexdigest()
            if received_hash != expected_hash:
                logger.warning(f"Message integrity check failed from {from_peer} — possible corruption")
                # Don't reject (could be benign serialization difference), just log

        db = get_db()
        msg_type = msg.get("type")
        timestamp = msg.get("timestamp", int(time.time() * 1000))
        txid = msg.get("txid")

        if timestamp < int(time.time() * 1000) - 60000:
            logger.debug("Stale transaction received, ignoring")
            return

        if msg_type == "transaction":
            self.tx_stats['received'] += 1

            # --- Cheap structural checks BEFORE expensive ML-DSA-87 sig verification ---
            body = msg.get("body")
            if not body or not isinstance(body, dict):
                self.tx_stats['invalid'] += 1
                return
            if not all(k in body for k in ("msg_str", "signature", "pubkey")):
                self.tx_stats['invalid'] += 1
                return
            if not msg.get("inputs") or not msg.get("outputs"):
                self.tx_stats['invalid'] += 1
                return

            # Recalculate txid to ensure consistency
            from blockchain.blockchain import serialize_transaction, sha256d
            msg_copy = msg.copy()
            if "txid" in msg_copy:
                del msg_copy["txid"]
            raw_tx = serialize_transaction(msg_copy)
            calculated_txid = sha256d(bytes.fromhex(raw_tx))[::-1].hex()

            # Verify txid matches
            if txid and txid != calculated_txid:
                logger.warning(f"[TX MISMATCH] Received txid {txid} but calculated {calculated_txid}")
                txid = calculated_txid  # Use the calculated one for consistency
            else:
                txid = calculated_txid

            # --- All cheap duplicate / already-confirmed checks before sig verification ---
            if txid in self.seen_tx:
                self.tx_stats['duplicates'] += 1
                return
            if mempool_manager.get_transaction(txid) is not None:
                self.tx_stats['duplicates'] += 1
                return
            # Check if transaction is already confirmed on-chain
            if db.get(f"tx:{txid}".encode()):
                self.tx_stats['duplicates'] += 1
                return

            # Only log if this is a new transaction
            logger.debug(f"Received transaction {txid} from {from_peer}")

            # --- Expensive: ML-DSA-87 post-quantum signature verification ---
            if not verify_transaction(body["msg_str"], body["signature"], body["pubkey"]):
                logger.warning(f"Transaction {txid} failed verification")
                self.tx_stats['invalid'] += 1
                return
            
            self.tx_stats['new'] += 1
            
            # Normalize amounts to prevent scientific notation issues
            if "inputs" in msg:
                for inp in msg["inputs"]:
                    if "amount" in inp:
                        inp["amount"] = str(inp["amount"])
            if "outputs" in msg:
                for out in msg["outputs"]:
                    if "amount" in out:
                        out["amount"] = str(out["amount"])
            
            tx_lock = asyncio.Lock()
            
            async with tx_lock:
                if mempool_manager.get_transaction(txid) is not None:
                    return
                
                # Use the calculated txid for consistency
                msg["txid"] = txid
                
                # Store just the transaction data, not the entire gossip message
                # This ensures consistency with how transactions are stored from web API
                success, error = mempool_manager.add_transaction(msg)
                if not success:
                    # This means the transaction conflicts with existing mempool transactions
                    logger.warning(f"[MEMPOOL] Rejected transaction {txid}: {error}")
                    return
                    
                logger.info(f"[MEMPOOL] Added gossiped transaction {txid} to mempool. Current size: {mempool_manager.size()}")
                    
                # Add to seen_tx BEFORE broadcasting to prevent loops
                self.seen_tx.add(txid)
                self.seen_tx_timestamps[txid] = int(time.time())
                
                # Clean up old entries if we're at capacity
                if len(self.seen_tx) > MAX_SEEN_TX_SIZE:
                    self._cleanup_seen_tx()
            
            logger.debug(f"Transaction {txid} added to mempool")

            # Log stats every 10 new transactions
            if self.tx_stats['new'] % 10 == 0:
                total = self.tx_stats['received']
                dups = self.tx_stats['duplicates']
                dup_rate = (dups / total * 100) if total > 0 else 0
                logger.info(f"TX stats - Total: {total}, New: {self.tx_stats['new']}, Duplicates: {dups} ({dup_rate:.1f}%), Invalid: {self.tx_stats['invalid']}")
            
            await self.randomized_broadcast(msg)

        elif msg_type == "blocks_response":
            logger.info(f"Received blocks_response from {from_peer}")
            blocks = msg.get("blocks", [])
            is_new_block = msg.get("is_new_block", False)
            
            if blocks:
                logger.info(f"Processing {len(blocks)} blocks from peer (is_new_block={is_new_block})")
                # Log first block structure for debugging
                if blocks and len(blocks) > 0:
                    logger.info(f"First block keys: {list(blocks[0].keys())}")
                
                # Validate and fix missing txid in transactions before processing
                from blockchain.blockchain import serialize_transaction, sha256d
                blocks_to_process = []
                for block in blocks:
                    block_valid = True
                    if "full_transactions" in block and block["full_transactions"]:
                        tx_ids = block.get("tx_ids", [])
                        for i, tx in enumerate(block["full_transactions"]):
                            if tx and "txid" not in tx:
                                # Try to get txid from tx_ids array
                                if i < len(tx_ids):
                                    claimed_txid = tx_ids[i]
                                    # Validate the txid matches the transaction hash
                                    try:
                                        tx_bytes = serialize_transaction(tx)
                                        calculated_txid = sha256d(tx_bytes)
                                        if calculated_txid != claimed_txid:
                                            logger.error(f"Transaction hash mismatch! Claimed: {claimed_txid}, Calculated: {calculated_txid}")
                                            logger.error(f"Block height: {block.get('height', 'unknown')}, tx index: {i}")
                                            logger.error(f"Rejecting block {block.get('block_hash', 'unknown')} due to invalid transaction hash")
                                            block_valid = False
                                            break
                                        tx["txid"] = claimed_txid
                                    except Exception as e:
                                        logger.error(f"Failed to validate transaction hash: {e}")
                                        logger.error(f"Rejecting block {block.get('block_hash', 'unknown')} due to transaction validation error")
                                        block_valid = False
                                        break
                                # Coinbase transactions should already have a txid from when they were mined
                                elif tx.get("inputs") and len(tx["inputs"]) > 0 and tx["inputs"][0].get("txid") == "00" * 32:
                                    logger.error(f"Coinbase transaction missing txid in block {block.get('height', 0)}")
                                else:
                                    logger.warning(f"Could not determine txid for transaction {i} in block {block.get('height', 'unknown')}")
                    if block_valid:
                        blocks_to_process.append(block)
                blocks = blocks_to_process
                
                await process_blocks_from_peer(blocks)
                
                # If this is a new block announcement (not a sync response), propagate it
                if is_new_block:
                    # Check if we should broadcast this block
                    if blocks and len(blocks) > 0:
                        block_hash = blocks[0].get("block_hash", "")
                        if block_hash and broadcast_tracker.should_broadcast("blocks_response", block_hash, from_peer):
                            logger.info(f"Propagating new block {block_hash[:8]}... to other peers")
                            # Mark the sender as having this block
                            if from_peer:
                                broadcast_tracker.mark_peer_has_block(block_hash, from_peer)
                            await self.randomized_broadcast(msg)
                        else:
                            logger.debug(f"Skipping broadcast of block {block_hash[:8]}... (recently broadcast)")
        
        elif msg_type == "blocks_by_hash_response":
            logger.info(f"Received blocks_by_hash_response from {from_peer}")
            blocks = msg.get("blocks", [])
            if blocks:
                logger.info(f"Processing {len(blocks)} blocks received by hash request")
                # Validate and fix missing txid in transactions before processing
                from blockchain.blockchain import serialize_transaction, sha256d
                blocks_to_process = []
                for block in blocks:
                    block_valid = True
                    if "full_transactions" in block and block["full_transactions"]:
                        tx_ids = block.get("tx_ids", [])
                        for i, tx in enumerate(block["full_transactions"]):
                            if tx and "txid" not in tx:
                                if i < len(tx_ids):
                                    claimed_txid = tx_ids[i]
                                    # Validate the txid matches the transaction hash
                                    try:
                                        tx_bytes = serialize_transaction(tx)
                                        calculated_txid = sha256d(tx_bytes)
                                        if calculated_txid != claimed_txid:
                                            logger.error(f"Transaction hash mismatch! Claimed: {claimed_txid}, Calculated: {calculated_txid}")
                                            logger.error(f"Block height: {block.get('height', 'unknown')}, tx index: {i}")
                                            logger.error(f"Rejecting block {block.get('block_hash', 'unknown')} due to invalid transaction hash")
                                            block_valid = False
                                            break
                                        tx["txid"] = claimed_txid
                                    except Exception as e:
                                        logger.error(f"Failed to validate transaction hash: {e}")
                                        logger.error(f"Rejecting block {block.get('block_hash', 'unknown')} due to transaction validation error")
                                        block_valid = False
                                        break
                    if block_valid:
                        blocks_to_process.append(block)
                blocks = blocks_to_process
                
                await process_blocks_from_peer(blocks)
            else:
                logger.warning("Received empty blocks_response")
                
        elif msg_type == "blocks_response_chunked":
            # This would be handled by the client side, not here
            # The gossip handler only sends chunked responses, doesn't receive them
            logger.warning(f"Received unexpected blocks_response_chunked from {from_peer}")
   

        elif msg_type == "get_height":
            height, tip_hash = await get_current_height(db)

            response = {"type": "height_response", "height": height, "current_tip": tip_hash}
            writer.write((json.dumps(response) + "\n").encode('utf-8'))
            await writer.drain()

        elif msg_type == "get_blocks":
            logger.debug(f"get_blocks request: start={msg.get('start_height')}, end={msg.get('end_height')}")
            start_height = msg.get("start_height")
            end_height = msg.get("end_height")
            if start_height is None or end_height is None:
                logger.warning("get_blocks request missing start_height or end_height")
                return
            
            # Validate range
            if start_height < 0 or end_height < start_height:
                logger.warning(f"Invalid block range: {start_height} to {end_height}")
                return
            
            # Limit request size to prevent DoS
            from config.config import MAX_BLOCKS_PER_SYNC_REQUEST
            max_blocks = MAX_BLOCKS_PER_SYNC_REQUEST  # Default 500
            if end_height - start_height + 1 > max_blocks:
                logger.warning(f"Block request too large: {end_height - start_height + 1} blocks (max: {max_blocks})")
                end_height = start_height + max_blocks - 1

            blocks = []
            try:
                height_index = get_height_index()
                
                for h in range(start_height, end_height + 1):
                    # Use the efficient height index
                    block = height_index.get_block_by_height(h)
                    
                    if block:
                        # Check if block already has full_transactions
                        if "full_transactions" not in block or not block["full_transactions"]:
                            expanded_txs = []
                            for txid in block.get("tx_ids", []):
                                tx_key = f"tx:{txid}".encode()
                                if tx_key in db:
                                    tx_data = json.loads(db[tx_key].decode())
                                    # Ensure txid is in the transaction data
                                    if "txid" not in tx_data:
                                        tx_data["txid"] = txid
                                    expanded_txs.append(tx_data)
                                else:
                                    logger.warning(f"Transaction {txid} not found in DB for block at height {h}")

                            # Skip legacy coinbase lookup - coinbase is already included in regular transactions
                            # cb_key = f"tx:coinbase_{h}".encode()
                            # if cb_key in db:
                            #     cb_data = json.loads(db[cb_key].decode())
                            #     # Ensure coinbase has txid
                            #     if "txid" not in cb_data:
                            #         cb_data["txid"] = f"coinbase_{h}"
                            #     expanded_txs.append(cb_data)

                            block["full_transactions"] = expanded_txs
                        else:
                            logger.info(f"Block at height {h} already has {len(block['full_transactions'])} full transactions")
                            # IMPORTANT: Validate and ensure all transactions have txid field even if block already has full_transactions
                            from blockchain.blockchain import serialize_transaction, sha256d
                            tx_ids = block.get("tx_ids", [])
                            for i, tx in enumerate(block.get("full_transactions", [])):
                                if tx and "txid" not in tx and i < len(tx_ids):
                                    claimed_txid = tx_ids[i]
                                    # Validate the txid matches the transaction hash before sending
                                    try:
                                        tx_bytes = serialize_transaction(tx)
                                        calculated_txid = sha256d(tx_bytes)
                                        if calculated_txid != claimed_txid:
                                            logger.error(f"Not sending block with invalid tx hash! Claimed: {claimed_txid}, Calculated: {calculated_txid}")
                                            logger.error(f"Block height: {h}, tx index: {i}")
                                            # Skip this transaction - don't add invalid txid
                                            continue
                                        tx["txid"] = claimed_txid
                                    except Exception as e:
                                        logger.error(f"Failed to validate transaction hash before sending: {e}")
                                        # Skip this transaction
                                        continue
                        
                        # Make a deep copy to avoid modifying the original
                        import copy
                        blocks.append(copy.deepcopy(block))
                
                # Validate blocks before sending  
                for block in blocks:
                    if isinstance(block.get("height"), str) and len(block.get("height")) == 64:
                        logger.error(f"CRITICAL: About to send block with hash in height field: {block}")
                        # Skip this malformed block
                        blocks = [b for b in blocks if b != block]
                
                # Check if response is too large and needs chunking
                response = {"type": "blocks_response", "blocks": blocks}
                response_json = json.dumps(response)
                
                # If response is larger than 50MB, use chunked protocol
                if len(response_json) > 50 * 1024 * 1024:
                    # Split blocks into chunks
                    chunk_size = 10  # blocks per chunk
                    total_chunks = (len(blocks) + chunk_size - 1) // chunk_size
                    
                    # Send header first
                    header = {
                        "type": "blocks_response_chunked",
                        "total_chunks": total_chunks,
                        "total_blocks": len(blocks)
                    }
                    writer.write((json.dumps(header) + "\n").encode('utf-8'))
                    await writer.drain()
                    
                    # Send each chunk
                    for chunk_num in range(total_chunks):
                        start_idx = chunk_num * chunk_size
                        end_idx = min((chunk_num + 1) * chunk_size, len(blocks))
                        chunk_blocks = blocks[start_idx:end_idx]
                        
                        chunk_data = {
                            "chunk_num": chunk_num,
                            "blocks": chunk_blocks
                        }
                        writer.write((json.dumps(chunk_data) + "\n").encode('utf-8'))
                        await writer.drain()
                        
                    logger.info(f"Sent {len(blocks)} blocks in {total_chunks} chunks")
                else:
                    # Send as single response
                    writer.write((response_json + "\n").encode('utf-8'))
                    await writer.drain()
                    
            except Exception as e:
                logger.error(f"Error in get_blocks handler: {e}")
                error_response = {"type": "error", "message": f"Failed to retrieve blocks: {str(e)}"}
                writer.write((json.dumps(error_response) + "\n").encode('utf-8'))
                await writer.drain()

        elif msg_type == "get_blocks_by_hash":
            # Request specific blocks by their hashes
            block_hashes = msg.get("block_hashes", [])
            if not block_hashes:
                logger.warning("Received get_blocks_by_hash with no hashes")
                return
            
            logger.info(f"Received request for {len(block_hashes)} specific blocks")
            blocks = []
            
            for block_hash in block_hashes[:100]:  # Limit to 100 blocks per request
                block_key = f"block:{block_hash}".encode()
                if block_key in db:
                    block_data = json.loads(db[block_key].decode())
                    
                    # Add full transactions if not present
                    if "full_transactions" not in block_data or not block_data["full_transactions"]:
                        expanded_txs = []
                        for txid in block_data.get("tx_ids", []):
                            tx_key = f"tx:{txid}".encode()
                            if tx_key in db:
                                tx_data = json.loads(db[tx_key].decode())
                                if "txid" not in tx_data:
                                    tx_data["txid"] = txid
                                expanded_txs.append(tx_data)
                            else:
                                logger.warning(f"Transaction {txid} not found in DB for block {block_hash}")
                        block_data["full_transactions"] = expanded_txs
                    else:
                        # IMPORTANT: Validate and ensure all transactions have txid field even if block already has full_transactions
                        from blockchain.blockchain import serialize_transaction, sha256d
                        tx_ids = block_data.get("tx_ids", [])
                        for i, tx in enumerate(block_data.get("full_transactions", [])):
                            if tx and "txid" not in tx and i < len(tx_ids):
                                claimed_txid = tx_ids[i]
                                # Validate the txid matches the transaction hash before sending
                                try:
                                    tx_bytes = serialize_transaction(tx)
                                    calculated_txid = sha256d(tx_bytes)
                                    if calculated_txid != claimed_txid:
                                        logger.error(f"Not sending block with invalid tx hash! Claimed: {claimed_txid}, Calculated: {calculated_txid}")
                                        logger.error(f"Block hash: {block_hash}, tx index: {i}")
                                        # Skip this transaction - don't add invalid txid
                                        continue
                                    tx["txid"] = claimed_txid
                                except Exception as e:
                                    logger.error(f"Failed to validate transaction hash before sending: {e}")
                                    # Skip this transaction
                                    continue
                    
                    blocks.append(block_data)
                else:
                    logger.info(f"Block {block_hash} not found in database")
            
            response = {"type": "blocks_by_hash_response", "blocks": blocks}
            writer.write((json.dumps(response) + "\n").encode('utf-8'))
            await writer.drain()
            logger.info(f"Sent {len(blocks)} blocks by hash")

        elif msg_type == "get_transactions":
            # Request specific transactions by their IDs
            tx_ids = msg.get("tx_ids", [])
            if not tx_ids:
                logger.warning("Received get_transactions with no tx_ids")
                return

            logger.info(f"Processing request for {len(tx_ids)} transactions")
            transactions = []

            for txid in tx_ids[:100]:  # Limit to 100 transactions per request
                tx_key = f"tx:{txid}".encode()
                if tx_key in db:
                    try:
                        tx_data = json.loads(db[tx_key].decode())
                        # Ensure txid is in the transaction data
                        if "txid" not in tx_data:
                            tx_data["txid"] = txid
                        transactions.append(tx_data)
                    except Exception as e:
                        logger.error(f"Failed to load transaction {txid}: {e}")
                else:
                    logger.debug(f"Transaction {txid} not found in database")

            response = {"type": "transactions_response", "transactions": transactions}
            writer.write((json.dumps(response) + "\n").encode('utf-8'))
            await writer.drain()
            logger.info(f"Sent {len(transactions)} transactions")



    async def randomized_broadcast(self, msg_dict):
        peers = self.dht_peers | self.client_peers 
        
        # For critical messages like transactions, try to discover new peers first
        msg_type = msg_dict.get("type", "unknown")
        # Disabled peer discovery for transactions to improve submission speed
        # This was causing significant delays (several seconds) when submitting transactions
        # if msg_type == "transaction" and len(peers) < 3:
        #     logger.info(f"Only {len(peers)} peers known for transaction broadcast, checking for new peers...")
        #     # Import here to avoid circular dependency
        #     from dht.dht import discover_peers_once
        #     try:
        #         await discover_peers_once(self)
        #         peers = self.dht_peers | self.client_peers
        #         logger.info(f"After discovery, have {len(peers)} peers")
        #     except Exception as e:
        #         logger.warning(f"Failed to discover new peers: {e}")
        
        if not peers:
            # CRITICAL: This should NEVER happen for blocks!
            if msg_type == "blocks_response" and msg_dict.get("is_new_block"):
                logger.error("CRITICAL: No peers available for NEW BLOCK broadcast! This will cause validators to fall behind!")
                # Try to recover by re-discovering peers
                logger.info("Attempting emergency peer discovery...")
                from dht.dht import discover_peers_once
                try:
                    await discover_peers_once(self)
                    peers = self.dht_peers | self.client_peers
                    if peers:
                        logger.info(f"Emergency discovery found {len(peers)} peers")
                    else:
                        logger.error("Emergency discovery failed - validators will not receive blocks!")
                except Exception as e:
                    logger.error(f"Emergency discovery failed: {e}")
            else:
                logger.warning(f"No peers available for {msg_type} broadcast")
            
            if not peers:
                return
            
        # For transactions, broadcast to ALL peers, not just a subset
        if msg_type == "transaction":
            peers_to_send = list(peers)
            logger.info(f"Broadcasting transaction to ALL {len(peers_to_send)} peers")
        else:
            # For blocks, filter out peers that already have it
            if msg_type == "blocks_response":
                blocks = msg_dict.get("blocks", [])
                if blocks and len(blocks) > 0:
                    block_hash = blocks[0].get("block_hash", "")
                    if block_hash:
                        # Filter out peers that we've recently sent this block to
                        filtered_peers = [
                            p for p in peers 
                            if broadcast_tracker.should_send_to_peer(block_hash, str(p))
                        ]
                        if len(filtered_peers) < len(peers):
                            logger.debug(f"Filtered {len(peers) - len(filtered_peers)} peers that already have block {block_hash[:8]}...")
                        peers = filtered_peers
            
            if not peers:
                logger.debug(f"No peers need {msg_type}")
                return
                
            num_peers = max(2, int(len(peers) ** 0.5))
            peers_to_send = random.sample(list(peers), min(len(peers), num_peers))
        
        # Log broadcast details
        logger.info(f"Broadcasting {msg_type} to {len(peers_to_send)} peers: {peers_to_send}")
        
        # M2: Add message integrity hash for corruption detection
        msg_json = json.dumps(msg_dict, sort_keys=True)
        msg_dict["msg_hash"] = hashlib.sha256(msg_json.encode('utf-8')).hexdigest()
        payload = (json.dumps(msg_dict) + "\n").encode('utf-8')
        results = await asyncio.gather(
            *[self._send_message(p, payload) for p in peers_to_send],
            return_exceptions=True          
        )
        for peer, result in zip(peers_to_send, results):
            if isinstance(result, Exception):
                logger.warning(f"broadcast {peer} failed: {result}")
            else:
                logger.info(f"Successfully broadcast {msg_type} to {peer}")

    async def send_message(self, peer, msg_dict, wait_for_response=True, timeout=10):
        """Send a message to a peer and optionally wait for response"""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(peer[0], peer[1], limit=MAX_LINE_BYTES),
                timeout=2  # Reduced from 5s to 2s for faster failures
            )
            
            # Send the message
            payload = (json.dumps(msg_dict) + "\n").encode('utf-8')
            writer.write(payload)
            await writer.drain()
            
            if wait_for_response:
                # Wait for response
                try:
                    data = await asyncio.wait_for(reader.readline(), timeout=timeout)
                    if data:
                        response = json.loads(data.decode('utf-8').strip())
                        writer.close()
                        await writer.wait_closed()
                        return response
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout waiting for response from {peer}")
                except Exception as e:
                    logger.warning(f"Error reading response from {peer}: {e}")
            
            writer.close()
            await writer.wait_closed()
            return None
            
        except Exception as e:
            logger.warning(f"Failed to send message to {peer}: {e}")
            return None

    async def _send_message(self, peer, payload):
        # Try direct connection with reduced retries for faster failures
        for attempt in range(2):  # Reduced from 5 to 2 attempts
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(peer[0], peer[1], limit=MAX_LINE_BYTES),
                    timeout=2  # Reduced from 5s to 2s for faster failures
                )
                writer.write(payload)
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                self.failed_peers[peer] = 0
                return
            except Exception as e:
                logger.debug(f"Direct connection attempt {attempt + 1} to {peer} failed: {e}")
                # Faster exponential backoff: 0.5s, 1s
                await asyncio.sleep(min(0.5 * (2 ** attempt), 2))
        
        # Try NAT traversal if available and peer supports it
        if NAT_TRAVERSAL_AVAILABLE and peer in self.peer_info:
            peer_info = self.peer_info[peer]
            if peer_info.get('supports_nat_traversal') and peer_info.get('nat_type') != 'direct':
                logger.info(f"Attempting NAT traversal for peer {peer}")
                
                # Try local network connection if on same network
                if peer_info.get('local_ip') and self._is_same_network(peer_info['local_ip']):
                    try:
                        local_peer = (peer_info['local_ip'], peer_info['local_port'])
                        reader, writer = await asyncio.wait_for(
                            asyncio.open_connection(local_peer[0], local_peer[1], limit=MAX_LINE_BYTES),
                            timeout=1  # Reduced from 3s to 1s for local network
                        )
                        writer.write(payload)
                        await writer.drain()
                        writer.close()
                        await writer.wait_closed()
                        self.failed_peers[peer] = 0
                        logger.info(f"Local network connection successful to {local_peer}")
                        return
                    except Exception:
                        pass
        
        # All attempts failed
        self.failed_peers[peer] = self.failed_peers.get(peer, 0) + 1
        if peer in self.dht_peers and self.failed_peers[peer] > 5:  # Reduced from 10 to 5
            # Remove from synced_peers immediately on repeated failures
            if peer in self.synced_peers:
                self.synced_peers.discard(peer)
                logger.warning(f"Removed {peer} from synced_peers after {self.failed_peers[peer]} failures")
            # The partition check will handle final removal from dht_peers
    
    def _is_same_network(self, peer_local_ip: str) -> bool:
        """Check if peer is in same local network"""
        try:
            import ipaddress
            import socket
            
            # Get our local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            my_ip = s.getsockname()[0]
            s.close()
            
            my_addr = ipaddress.ip_address(my_ip)
            peer_addr = ipaddress.ip_address(peer_local_ip)
            
            # Check common private networks
            private_networks = [
                ipaddress.ip_network('10.0.0.0/8'),
                ipaddress.ip_network('172.16.0.0/12'),
                ipaddress.ip_network('192.168.0.0/16')
            ]
            
            for network in private_networks:
                if my_addr in network and peer_addr in network:
                    return True
        except Exception:
            pass
        return False

    async def check_partition(self):
        """Periodically check and recover failed peers"""
        while True:
            # Check all peers with failures every 30 seconds
            for peer in list(self.dht_peers):
                if self.failed_peers.get(peer, 0) > 0:
                    try:
                        # Simple ping to check if peer is back online
                        reader, writer = await asyncio.wait_for(
                            asyncio.open_connection(peer[0], peer[1]),
                            timeout=1
                        )
                        # Send a get_height request as ping
                        ping_msg = json.dumps({"type": "get_height", "timestamp": int(time.time() * 1000)}) + "\n"
                        writer.write(ping_msg.encode('utf-8'))
                        await writer.drain()

                        # Wait for response
                        response = await asyncio.wait_for(reader.readline(), timeout=2)
                        if response:
                            # Peer is back online, reset failure count
                            self.failed_peers[peer] = 0
                            logger.info(f"Peer {peer} is back online, resetting failure count")

                            # Ensure peer is in both lists if it's back online
                            if peer not in self.synced_peers:
                                self.synced_peers.add(peer)
                                asyncio.create_task(push_blocks(peer[0], peer[1]))

                        writer.close()
                        await writer.wait_closed()
                    except Exception as e:
                        logger.debug(f"Peer {peer} still unreachable: {e}")
                        # Remove peer after fewer failures (5 instead of 20)
                        if self.failed_peers.get(peer, 0) > 5:
                            # Don't remove protected peers
                            if peer in self.protected_peers:
                                logger.info(f"Not removing protected peer {peer} despite failures")
                                continue

                            fail_count = self.failed_peers.get(peer, 0)
                            self.synced_peers.discard(peer)
                            self.remove_peer(peer[0], peer[1])
                            logger.info(f"Removed unreachable peer {peer} after {fail_count} failures")

            # Clean up peers that are in dht_peers but not in synced_peers for too long
            for peer in list(self.dht_peers):
                if peer not in self.synced_peers and peer not in self.protected_peers:
                    if peer in self.peer_timestamps:
                        age = time.time() - self.peer_timestamps[peer]
                        if age > 300:  # 5 minutes
                            logger.info(f"Removing stale unsynced peer {peer} (age: {age:.0f}s)")
                            self.synced_peers.discard(peer)
                            self.remove_peer(peer[0], peer[1])

            await asyncio.sleep(30)

    async def periodic_sync(self):
        """Periodically check and sync with all connected peers"""
        await asyncio.sleep(10)  # Initial delay to let connections establish
        
        while True:
            try:
                all_peers = self.dht_peers | self.client_peers
                if not all_peers:
                    logger.debug("No peers available for periodic sync")
                    await asyncio.sleep(60)
                    continue
                
                # Get our current height
                db = get_db()
                local_height, local_tip = await get_current_height(db)
                logger.info(f"[PERIODIC_SYNC] Starting sync check - local height: {local_height}")
                
                # Check height of each peer
                for peer in list(all_peers):
                    try:
                        reader, writer = await asyncio.wait_for(
                            asyncio.open_connection(peer[0], peer[1]),
                            timeout=2  # Reduced from 5s to 2s for faster sync
                        )
                        
                        # Request peer's height
                        height_request = {"type": "get_height", "timestamp": int(time.time() * 1000)}
                        writer.write((json.dumps(height_request) + "\n").encode('utf-8'))
                        await writer.drain()
                        
                        # Read response
                        response = await asyncio.wait_for(reader.readline(), timeout=3)  # Reduced from 5s to 3s
                        if response:
                            msg = json.loads(response.decode('utf-8').strip())
                            if msg.get("type") == "height_response":
                                peer_height = msg.get("height", -1)
                                logger.info(f"[PERIODIC_SYNC] Peer {peer} height: {peer_height}, local: {local_height}")
                                
                                # If peer is ahead, request blocks
                                if peer_height > local_height:
                                    # Check for gaps in our blockchain first
                                    height_index = get_height_index()
                                    gap_start = None
                                    
                                    # Look back up to 100 blocks for gaps
                                    check_from = max(1, local_height - 100)
                                    for h in range(check_from, local_height + 1):
                                        if not height_index.get_block_by_height(h):
                                            gap_start = h
                                            logger.warning(f"[PERIODIC_SYNC] Found gap in blockchain at height {h}")
                                            break
                                    
                                    # If gap found, sync from gap, otherwise from current height + 1
                                    sync_start = gap_start if gap_start else local_height + 1
                                    
                                    logger.info(f"[PERIODIC_SYNC] Peer {peer} is ahead by {peer_height - local_height} blocks, requesting sync from height {sync_start}")
                                    blocks_request = {
                                        "type": "get_blocks",
                                        "start_height": sync_start,
                                        "end_height": min(sync_start + 19, peer_height),  # Request max 20 blocks at a time for reliable ordering
                                        "timestamp": int(time.time() * 1000)
                                    }
                                    writer.write((json.dumps(blocks_request) + "\n").encode('utf-8'))
                                    await writer.drain()
                                    
                                    # Read blocks response
                                    blocks_response = await asyncio.wait_for(reader.readline(), timeout=30)
                                    if blocks_response:
                                        blocks_msg = json.loads(blocks_response.decode('utf-8').strip())
                                        if blocks_msg.get("type") == "blocks_response":
                                            blocks = blocks_msg.get("blocks", [])
                                            if blocks:
                                                logger.info(f"[PERIODIC_SYNC] Received {len(blocks)} blocks from {peer}")
                                                await process_blocks_from_peer(blocks)
                                        elif blocks_msg.get("type") == "blocks_response_chunked":
                                            # Handle chunked response
                                            total_chunks = blocks_msg.get("total_chunks", 0)
                                            total_blocks = blocks_msg.get("total_blocks", 0)
                                            blocks = []
                                            
                                            logger.info(f"[PERIODIC_SYNC] Receiving chunked response with {total_chunks} chunks, {total_blocks} total blocks")
                                            
                                            for chunk_num in range(total_chunks):
                                                chunk_line = await asyncio.wait_for(reader.readline(), timeout=30)
                                                if not chunk_line:
                                                    logger.error(f"[PERIODIC_SYNC] Connection closed while reading chunk {chunk_num}")
                                                    break
                                                
                                                try:
                                                    chunk_data = json.loads(chunk_line.decode('utf-8').strip())
                                                    if chunk_data.get("chunk_num") != chunk_num:
                                                        logger.error(f"[PERIODIC_SYNC] Expected chunk {chunk_num}, got {chunk_data.get('chunk_num')}")
                                                        break
                                                    
                                                    chunk_blocks = chunk_data.get("blocks", [])
                                                    blocks.extend(chunk_blocks)
                                                    logger.info(f"[PERIODIC_SYNC] Received chunk {chunk_num + 1}/{total_chunks} with {len(chunk_blocks)} blocks")
                                                    
                                                except json.JSONDecodeError as e:
                                                    logger.error(f"[PERIODIC_SYNC] Failed to decode chunk {chunk_num}: {e}")
                                                    break
                                            
                                            if blocks:
                                                logger.info(f"[PERIODIC_SYNC] Received total of {len(blocks)} blocks from {peer}")
                                                await process_blocks_from_peer(blocks)
                                
                                # If we're ahead, push blocks to peer
                                elif peer_height < local_height:
                                    logger.info(f"[PERIODIC_SYNC] We're ahead of {peer} by {local_height - peer_height} blocks")
                                    # Close this connection and use push_blocks
                                    writer.close()
                                    await writer.wait_closed()
                                    await push_blocks(peer[0], peer[1])
                                    continue
                        
                        writer.close()
                        await writer.wait_closed()
                        
                    except Exception as e:
                        logger.debug(f"[PERIODIC_SYNC] Error syncing with peer {peer}: {e}")
                        # Mark failed peers
                        self.failed_peers[peer] = self.failed_peers.get(peer, 0) + 1
                        # Remove from synced_peers if too many failures
                        if self.failed_peers[peer] > 3 and peer in self.synced_peers:
                            self.synced_peers.discard(peer)
                            logger.info(f"[PERIODIC_SYNC] Removed {peer} from synced_peers after repeated failures")
                
            except Exception as e:
                logger.error(f"[PERIODIC_SYNC] Error in periodic sync: {e}", exc_info=True)
            
            # Also check for orphan chains that need missing blocks
            try:
                from sync.sync import _check_orphan_chains_for_missing_blocks
                await _check_orphan_chains_for_missing_blocks(self)
            except Exception as e:
                logger.debug(f"[PERIODIC_SYNC] Error checking orphan chains: {e}")
            
            # Check every 30 seconds
            await asyncio.sleep(30)

    async def periodic_cleanup(self):
        """Periodically clean up peer lists to ensure consistency"""
        await asyncio.sleep(60)  # Initial delay

        while True:
            try:
                # Remove peers from dht_peers that are not in synced_peers and not protected
                peers_to_remove = []
                for peer in list(self.dht_peers):
                    if peer not in self.synced_peers and peer not in self.protected_peers:
                        if peer in self.peer_timestamps:
                            age = time.time() - self.peer_timestamps[peer]
                            if age > 180:  # 3 minutes
                                peers_to_remove.append(peer)
                                logger.info(f"[CLEANUP] Marking {peer} for removal (unsynced for {age:.0f}s)")

                # Remove stale peers via remove_peer to keep subnet_counts consistent
                for peer in peers_to_remove:
                    self.synced_peers.discard(peer)
                    self.remove_peer(peer[0], peer[1])
                    logger.info(f"[CLEANUP] Removed stale peer {peer}")

                # Log current state
                logger.info(
                    f"[CLEANUP] Current state - dht_peers: {len(self.dht_peers)}, "
                    f"synced_peers: {len(self.synced_peers)}, protected: {len(self.protected_peers)}, "
                    f"inbound: {self.inbound_connections}"
                )

                # Ensure all synced_peers are also in dht_peers
                for peer in list(self.synced_peers):
                    if peer not in self.dht_peers:
                        self.dht_peers.add(peer)
                        logger.warning(f"[CLEANUP] Added missing synced peer {peer} to dht_peers")

            except Exception as e:
                logger.error(f"[CLEANUP] Error in periodic cleanup: {e}", exc_info=True)

            # Run every 2 minutes
            await asyncio.sleep(120)

    @staticmethod
    def _get_subnet(ip: str) -> str:
        """Extract /16 subnet prefix for IPv4 addresses."""
        try:
            parts = ip.split(".")
            if len(parts) == 4:
                return f"{parts[0]}.{parts[1]}"
        except Exception:
            pass
        return ip  # fallback: treat whole IP as its own group

    def add_peer(self, ip: str, port: int, peer_info=None, protected=False):
        peer = (ip, port)
        current_time = time.time()
        is_protected = protected or (peer_info and peer_info.get('protected', False))

        # CRITICAL: Mark important peers as protected
        if is_protected:
            self.protected_peers.add(peer)
            logger.info(f"Added PROTECTED peer {peer} - this peer will NEVER be removed")

        # Don't add ourselves as a peer - check using dynamic validator ID
        if peer_info and 'validator_id' in peer_info:
            if peer_info['validator_id'] == self.node_id:
                logger.warning(f"Refusing to add self as peer (same validator ID): {ip}:{port}")
                return

        # Also check port and common local IPs
        if port == self.gossip_port:
            import socket
            try:
                hostname = socket.gethostname()
                local_ips = {'localhost', '127.0.0.1', '0.0.0.0', hostname}
                if hasattr(self, '_external_ip'):
                    local_ips.add(self._external_ip)
                if hasattr(self, '_nat_external_ip'):
                    if ip == self._nat_external_ip:
                        logger.warning(f"Refusing to add self as peer (NAT external IP match): {ip}:{port}")
                        return
                if ip in local_ips:
                    logger.warning(f"Refusing to add self as peer: {ip}:{port}")
                    return
            except Exception as e:
                logger.debug(f"Error checking self-connection: {e}")

        # Check if this IP already has a peer entry
        if ip in self.ip_to_peer:
            old_port, old_vid, old_timestamp = self.ip_to_peer[ip]
            old_peer = (ip, old_port)
            new_vid = peer_info.get('validator_id', 'unknown') if peer_info else 'unknown'

            if old_port != port or old_vid != new_vid:
                if old_timestamp > current_time:
                    logger.info(f"Keeping existing peer {old_vid} on port {old_port} for IP {ip}")
                    return

                logger.warning(
                    f"IP {ip} already has peer {old_vid} on port {old_port}, "
                    f"replacing with {new_vid} on port {port}"
                )

                # Remove old peer through remove_peer to keep subnet_counts consistent
                self.synced_peers.discard(old_peer)
                # Temporarily allow removal even if old_peer was protected (we're replacing)
                was_protected = old_peer in self.protected_peers
                if was_protected:
                    self.protected_peers.discard(old_peer)
                self.remove_peer(ip, old_port)
                if was_protected and is_protected:
                    pass  # new peer will be protected
            else:
                logger.info(f"Updating timestamp for existing peer {ip}:{port}")

        # --- Subnet diversity check (non-protected only) ---
        subnet = self._get_subnet(ip)
        if not is_protected and peer not in self.dht_peers:
            current_subnet_count = self.subnet_counts.get(subnet, 0)
            if current_subnet_count >= MAX_PEERS_PER_SUBNET:
                logger.warning(
                    f"Rejecting peer {ip}:{port} — subnet {subnet}.0.0/16 already "
                    f"has {current_subnet_count} peers (limit {MAX_PEERS_PER_SUBNET})"
                )
                return

        # --- Outbound connection limit (non-protected only) ---
        if not is_protected and peer not in self.dht_peers:
            if len(self.dht_peers) >= MAX_OUTBOUND_CONNECTIONS:
                # Try to evict the lowest-reputation non-protected peer
                evict_candidate = None
                evict_score = float('inf')
                for existing in self.dht_peers:
                    if existing in self.protected_peers:
                        continue
                    rep = peer_reputation_manager.get_peer_reputation(existing[0], existing[1])
                    score = rep.reputation_score if rep else 50.0
                    if score < evict_score:
                        evict_score = score
                        evict_candidate = existing

                # Only evict if new peer has better reputation
                new_rep = peer_reputation_manager.get_peer_reputation(ip, port)
                new_score = new_rep.reputation_score if new_rep else 50.0
                if evict_candidate and new_score > evict_score:
                    logger.info(
                        f"Evicting low-reputation peer {evict_candidate} (score {evict_score:.0f}) "
                        f"for {ip}:{port} (score {new_score:.0f})"
                    )
                    self.synced_peers.discard(evict_candidate)
                    self.remove_peer(evict_candidate[0], evict_candidate[1])
                else:
                    logger.info(
                        f"Rejecting peer {ip}:{port} — at outbound limit "
                        f"({len(self.dht_peers)}/{MAX_OUTBOUND_CONNECTIONS}) "
                        f"and no lower-reputation peer to evict"
                    )
                    return

        # Update IP mapping with validator ID and timestamp
        vid = peer_info.get('validator_id', 'unknown') if peer_info else 'unknown'
        self.ip_to_peer[ip] = (port, vid, current_time)
        self.peer_timestamps[peer] = current_time

        # Reset failure count if peer is being re-added
        if peer in self.failed_peers:
            logger.info(f"Resetting failure count for peer {peer} (was {self.failed_peers[peer]})")
            self.failed_peers[peer] = 0

        # Always update peer info if provided
        if peer_info:
            self.peer_info[peer] = peer_info

        if peer not in self.dht_peers:
            self.dht_peers.add(peer)
            # Track subnet count
            self.subnet_counts[subnet] = self.subnet_counts.get(subnet, 0) + 1
            logger.info(f"Added DHT peer {peer} to validator list (validator_id: {vid})")
            if peer not in self.synced_peers:
                self.synced_peers.add(peer)
                asyncio.create_task(push_blocks(ip, port))
        else:
            # Peer already exists, but might have been marked as failed
            logger.info(f"Peer {peer} already in list, ensuring it's active")
            if peer not in self.synced_peers:
                self.synced_peers.add(peer)
                asyncio.create_task(push_blocks(ip, port))

    def remove_peer(self, ip: str, port: int):
        peer = (ip, port)

        # CRITICAL: NEVER remove protected peers
        if peer in self.protected_peers:
            logger.error(f"REFUSING to remove PROTECTED peer {peer}! This peer is critical for network operation.")
            return

        # Clean up IP mapping
        if ip in self.ip_to_peer and self.ip_to_peer[ip][0] == port:
            del self.ip_to_peer[ip]

        # Clean up timestamp
        if peer in self.peer_timestamps:
            del self.peer_timestamps[peer]

        if peer in self.dht_peers:
            self.dht_peers.remove(peer)
            self.failed_peers.pop(peer, None)
            self.peer_info.pop(peer, None)
            # Decrement subnet count
            subnet = self._get_subnet(ip)
            if subnet in self.subnet_counts:
                self.subnet_counts[subnet] = max(0, self.subnet_counts[subnet] - 1)
                if self.subnet_counts[subnet] == 0:
                    del self.subnet_counts[subnet]
            logger.info(f"Removed DHT peer {peer} from validator list")

    # --- Reputation auto-disconnect callback ---

    def _on_peer_reputation_disconnect(self, ip: str, port: int, reason: str):
        """Called by PeerReputationManager when a peer becomes MALICIOUS/BANNED."""
        peer = (ip, port)
        logger.warning(f"Auto-disconnecting peer {ip}:{port} — {reason}")
        self.synced_peers.discard(peer)
        self.client_peers.discard(peer)
        # remove_peer respects protected status internally
        self.remove_peer(ip, port)

    # --- Anchor peer persistence ---

    def _load_anchor_peers(self):
        """Load anchor peers saved from the previous shutdown."""
        try:
            with open(self._anchor_path, "r") as f:
                anchors = json.load(f)
            for entry in anchors:
                ip, port = entry["ip"], entry["port"]
                logger.info(f"Loading anchor peer {ip}:{port}")
                self.add_peer(ip, port, peer_info=entry.get("info"), protected=True)
        except FileNotFoundError:
            logger.debug("No anchor peers file found — first run")
        except Exception as e:
            logger.warning(f"Failed to load anchor peers: {e}")

    def save_anchor_peers(self):
        """Persist top N long-lived GOOD peers for next startup."""
        from network.peer_reputation import PeerBehavior
        candidates = []
        for peer in self.dht_peers:
            rep = peer_reputation_manager.get_peer_reputation(peer[0], peer[1])
            if rep and rep.behavior == PeerBehavior.GOOD:
                candidates.append((peer, rep.get_age(), rep.reputation_score))

        # Sort by age descending (prefer long-lived peers), break ties by score
        candidates.sort(key=lambda c: (c[1], c[2]), reverse=True)

        anchors = []
        for peer, age, score in candidates[:NUM_ANCHOR_PEERS]:
            anchors.append({
                "ip": peer[0],
                "port": peer[1],
                "info": self.peer_info.get(peer),
            })

        try:
            os.makedirs(os.path.dirname(self._anchor_path), exist_ok=True)
            with open(self._anchor_path, "w") as f:
                json.dump(anchors, f)
            logger.info(f"Saved {len(anchors)} anchor peers")
        except Exception as e:
            logger.warning(f"Failed to save anchor peers: {e}")

    async def stop(self):
        # Save anchor peers before shutdown
        self.save_anchor_peers()

        if self.server_task:
            self.server_task.cancel()
        if self.partition_task:
            self.partition_task.cancel()
        if self.sync_task:
            self.sync_task.cancel()
        if hasattr(self, 'cleanup_task') and self.cleanup_task:
            self.cleanup_task.cancel()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
