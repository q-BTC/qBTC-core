import asyncio
import json
import time
import logging
import random
from asyncio import StreamReader, StreamWriter
from config.config import DEFAULT_GOSSIP_PORT
from state.state import pending_transactions
from wallet.wallet import verify_transaction
from database.database import get_db, get_current_height
from dht.dht import push_blocks
from sync.sync import process_blocks_from_peer
from network.peer_reputation import peer_reputation_manager

# Import NAT traversal
try:
    from network.nat_traversal import TCPHolePuncher, nat_traversal
    NAT_TRAVERSAL_AVAILABLE = True
except ImportError:
    TCPHolePuncher = None
    nat_traversal = None
    NAT_TRAVERSAL_AVAILABLE = False

GENESIS_HASH = "0" * 64
MAX_LINE_BYTES = 30 * 1024 * 1024  

class GossipNode:
    def __init__(self, node_id, wallet=None, is_bootstrap=False, is_full_node=True):
        self.node_id = node_id
        self.wallet = wallet
        self.seen_tx = set()
        self.dht_peers = set()  
        self.client_peers = set()  
        self.failed_peers = {}
        self.peer_info = {}  # Store full peer info for NAT traversal
        self.server_task = None
        self.server = None
        self.partition_task = None
        self.is_bootstrap = is_bootstrap
        self.is_full_node = is_full_node
        self.gossip_port = None  # Will be set when server starts
        self.synced_peers = set()
        #if not is_bootstrap:
            # Temporary workaround until DHT is fully debugged
        #    self.dht_peers.add(('api.bitcoinqs.org', 7002))

    async def start_server(self, host="0.0.0.0", port=DEFAULT_GOSSIP_PORT):
        self.gossip_port = port  # Store for NAT traversal
        self.server = await asyncio.start_server(self.handle_client, host, port, limit=MAX_LINE_BYTES)
        self.server_task = asyncio.create_task(self.server.serve_forever())
        # Enable partition check to recover failed peers
        self.partition_task = asyncio.create_task(self.check_partition())
        logging.info(f"Gossip server started on {host}:{port}")

    async def handle_client(self, reader: StreamReader, writer: StreamWriter):
        peer_info = writer.get_extra_info('peername')
        peer_ip, peer_port = peer_info[0], peer_info[1]
        
        logging.info(f"New peer connection: {peer_ip}:{peer_port}")
        
        # Check peer reputation before accepting connection
        if not peer_reputation_manager.should_connect_to_peer(peer_ip, peer_port):
            logging.warning(f"Rejecting connection from banned/malicious peer {peer_ip}:{peer_port}")
            writer.close()
            await writer.wait_closed()
            return
        
        # Record successful connection
        peer_reputation_manager.record_connection_success(peer_ip, peer_port)
        
        if peer_info not in self.client_peers and peer_info not in self.dht_peers:
            self.client_peers.add(peer_info)
            logging.info(f"Added temporary client peer {peer_info}")
        
        start_time = time.time()
        try:
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=10)
                if not line:
                    break
                
                try:
                    msg = json.loads(line.decode('utf-8').strip())
                    await self.handle_gossip_message(msg, peer_info, writer)
                    
                    # Record valid message
                    peer_reputation_manager.record_valid_message(
                        peer_ip, peer_port, msg.get("type", "unknown")
                    )
                    
                except json.JSONDecodeError as e:
                    logging.warning(f"Invalid JSON from {peer_info}: {e}")
                    peer_reputation_manager.record_invalid_message(
                        peer_ip, peer_port, "invalid_json"
                    )
                except Exception as e:
                    logging.error(f"Error processing message from {peer_info}: {e}")
                    peer_reputation_manager.record_invalid_message(
                        peer_ip, peer_port, str(e)
                    )
                    
        except asyncio.TimeoutError:
            logging.warning(f"Timeout handling client {peer_info}")
            peer_reputation_manager.record_timeout(peer_ip, peer_port)
        except Exception as e:
            logging.error(f"Error handling client {peer_info}: {e}")
            peer_reputation_manager.record_connection_failure(
                peer_ip, peer_port, str(e)
            )
        finally:
            # Record disconnection
            peer_reputation_manager.record_disconnection(peer_ip, peer_port)
            
            # Record response time
            response_time = time.time() - start_time
            peer_reputation_manager.record_response_time(peer_ip, peer_port, response_time)
            
            if peer_info in self.client_peers:
                self.client_peers.remove(peer_info)
                logging.info(f"Removed temporary client peer {peer_info}")
            
            writer.close()
            await writer.wait_closed()


    

   



    async def handle_gossip_message(self, msg, from_peer, writer):
        db = get_db()  
        msg_type = msg.get("type")
        timestamp = msg.get("timestamp", int(time.time() * 1000))
        tx_id = msg.get("tx_id")

        print(msg)

        if timestamp < int(time.time() * 1000) - 60000:  
            print("**** TRANSACTION IS STALE")
            return

        if msg_type == "transaction":
            print(msg)
            if tx_id in self.seen_tx or tx_id in pending_transactions:
                return
            if not verify_transaction(msg["body"]["msg_str"], msg["body"]["signature"], msg["body"]["pubkey"]):
                return
            
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
                if tx_id in pending_transactions:
                    return
                pending_transactions[tx_id] = msg
            await self.randomized_broadcast(msg)
            self.seen_tx.add(tx_id)

        elif msg_type == "blocks_response":
            logging.info(f"Received blocks_response from {from_peer}")
            blocks = msg.get("blocks", [])
            if blocks:
                logging.info(f"Processing {len(blocks)} blocks from peer")
                # Log first block structure for debugging
                if blocks and len(blocks) > 0:
                    logging.info(f"First block keys: {list(blocks[0].keys())}")
                process_blocks_from_peer(blocks)
            else:
                logging.warning("Received empty blocks_response")
   

        elif msg_type == "get_height":
            height, tip_hash = get_current_height(db)

            response = {"type": "height_response", "height": height, "current_tip": tip_hash}
            writer.write((json.dumps(response) + "\n").encode('utf-8'))
            await writer.drain()

        elif msg_type == "get_blocks":
            print(msg)
            start_height = msg.get("start_height")
            end_height = msg.get("end_height")
            if start_height is None or end_height is None:
                return

            blocks = []

            for h in range(start_height, end_height + 1):
                found_block = None

                for key in db.keys():
                    if key.startswith(b"block:"):
                        block = json.loads(db[key].decode())

                        if block.get("height") == h:
                            expanded_txs = []
                            for tx_id in block["tx_ids"]:
                                tx_key = f"tx:{tx_id}".encode()
                                if tx_key in db:
                                    expanded_txs.append({
                                        "tx_id": tx_id,
                                        "transaction": json.loads(db[tx_key].decode())
                                    })
                                    #expanded_txs.append({
                                    #    "tx_id": tx_id,
                                    #    "transaction": tx_data
                                    #})
                                else:
                                    continue
                                    #expanded_txs.append({
                                    #    "tx_id": tx_id,
                                    #    "transaction": None
                                    #})

                            cb_key = f"tx:coinbase_{h}".encode()
                            if cb_key in db:
                                expanded_txs.append(json.loads(db[cb_key].decode()))

                            block["full_transactions"] = expanded_txs
                            found_block = block
                            break

                if found_block:
                    blocks.append(found_block)

            response = {"type": "blocks_response", "blocks": blocks}
            writer.write((json.dumps(response) + "\n").encode('utf-8'))
            await writer.drain()

  

    async def randomized_broadcast(self, msg_dict):
        peers = self.dht_peers | self.client_peers 
        if not peers:
            logging.warning("No peers available for broadcast")
            return
        num_peers = max(2, int(len(peers) ** 0.5))
        peers_to_send = random.sample(list(peers), min(len(peers), num_peers))
        
        # Log broadcast details
        msg_type = msg_dict.get("type", "unknown")
        logging.info(f"Broadcasting {msg_type} to {len(peers_to_send)} peers: {peers_to_send}")
        
        payload = (json.dumps(msg_dict) + "\n").encode('utf-8')
        results = await asyncio.gather(
            *[self._send_message(p, payload) for p in peers_to_send],
            return_exceptions=True          
        )
        for peer, result in zip(peers_to_send, results):
            if isinstance(result, Exception):
                logging.warning(f"broadcast {peer} failed: {result}")
            else:
                logging.info(f"Successfully broadcast {msg_type} to {peer}")

    async def _send_message(self, peer, payload):
        # Try direct connection with more retries and exponential backoff
        for attempt in range(5):  # Increased from 2 to 5 attempts
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(peer[0], peer[1], limit=MAX_LINE_BYTES),
                    timeout=5
                )
                writer.write(payload)
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                self.failed_peers[peer] = 0
                return
            except Exception as e:
                logging.debug(f"Direct connection attempt {attempt + 1} to {peer} failed: {e}")
                # Exponential backoff: 1s, 2s, 4s, 8s
                await asyncio.sleep(min(2 ** attempt, 8))
        
        # Try NAT traversal if available and peer supports it
        if NAT_TRAVERSAL_AVAILABLE and peer in self.peer_info:
            peer_info = self.peer_info[peer]
            if peer_info.get('supports_nat_traversal') and peer_info.get('nat_type') != 'direct':
                logging.info(f"Attempting NAT traversal for peer {peer}")
                
                # Try local network connection if on same network
                if peer_info.get('local_ip') and self._is_same_network(peer_info['local_ip']):
                    try:
                        local_peer = (peer_info['local_ip'], peer_info['local_port'])
                        reader, writer = await asyncio.wait_for(
                            asyncio.open_connection(local_peer[0], local_peer[1], limit=MAX_LINE_BYTES),
                            timeout=3
                        )
                        writer.write(payload)
                        await writer.drain()
                        writer.close()
                        await writer.wait_closed()
                        self.failed_peers[peer] = 0
                        logging.info(f"Local network connection successful to {local_peer}")
                        return
                    except Exception:
                        pass
        
        # All attempts failed
        self.failed_peers[peer] = self.failed_peers.get(peer, 0) + 1
        if peer in self.dht_peers and self.failed_peers[peer] > 10:  # Increased from 3 to 10
            # Don't remove peer, just mark it as temporarily unreachable
            logging.warning(f"Peer {peer} has failed {self.failed_peers[peer]} times, marking as unreachable")
            # The partition check will attempt to recover this peer
    
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
                            timeout=3
                        )
                        # Send a get_height request as ping
                        ping_msg = json.dumps({"type": "get_height", "timestamp": int(time.time() * 1000)}) + "\n"
                        writer.write(ping_msg.encode('utf-8'))
                        await writer.drain()
                        
                        # Wait for response
                        response = await asyncio.wait_for(reader.readline(), timeout=3)
                        if response:
                            # Peer is back online, reset failure count
                            self.failed_peers[peer] = 0
                            logging.info(f"Peer {peer} is back online, resetting failure count")
                            
                            # If peer was in synced_peers, trigger sync
                            if peer in self.synced_peers:
                                asyncio.create_task(push_blocks(peer[0], peer[1]))
                        
                        writer.close()
                        await writer.wait_closed()
                    except Exception as e:
                        logging.debug(f"Peer {peer} still unreachable: {e}")
                        # Only remove peer after many failures and extended downtime
                        if self.failed_peers.get(peer, 0) > 20:
                            self.dht_peers.discard(peer)
                            self.peer_info.pop(peer, None)
                            self.synced_peers.discard(peer)
                            del self.failed_peers[peer]
                            logging.info(f"Permanently removed peer {peer} after extended downtime")
            
            await asyncio.sleep(30)

    def add_peer(self, ip: str, port: int, peer_info=None):
        peer = (ip, port)
        
        # Reset failure count if peer is being re-added
        if peer in self.failed_peers:
            logging.info(f"Resetting failure count for peer {peer} (was {self.failed_peers[peer]})")
            self.failed_peers[peer] = 0
        
        # Always update peer info if provided
        if peer_info:
            self.peer_info[peer] = peer_info
        
        if peer not in self.dht_peers:
            self.dht_peers.add(peer)
            logging.info(f"Added DHT peer {peer} to validator list")
            if peer not in self.synced_peers:
                self.synced_peers.add(peer)
                asyncio.create_task(push_blocks(ip, port))
        else:
            # Peer already exists, but might have been marked as failed
            logging.info(f"Peer {peer} already in list, ensuring it's active")
            # Trigger sync if needed
            if peer not in self.synced_peers:
                self.synced_peers.add(peer)
                asyncio.create_task(push_blocks(ip, port))

    def remove_peer(self, ip: str, port: int):
        peer = (ip, port)
        if peer in self.dht_peers:
            self.dht_peers.remove(peer)
            self.failed_peers.pop(peer, None)
            logging.info(f"Removed DHT peer {peer} from validator list")

    async def stop(self):
        if self.server_task:
            self.server_task.cancel()
        if self.partition_task:
            self.partition_task.cancel()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
