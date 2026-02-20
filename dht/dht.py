import asyncio
import json
import time
import aiohttp
from kademlia.network import Server as KademliaServer
from config.config import  shutdown_event, VALIDATOR_ID, HEARTBEAT_INTERVAL, VALIDATOR_TIMEOUT, VALIDATORS_LIST_KEY, DEFAULT_GOSSIP_PORT, KNOWN_VALIDATORS, MAX_HEARTBEAT_FAILURES, MAX_BLOCKS_PER_SYNC_REQUEST, MAX_OUTBOUND_CONNECTIONS
from state.state import validator_keys, known_validators

# Track validator heartbeat failures
validator_heartbeat_failures = {}  # validator_id -> consecutive_failures
# Track validator peer mapping to maintain connections even if DHT fails
validator_peer_mapping = {}  # validator_id -> {'ip': ip, 'port': port}
from database.database import get_db,get_current_height
from sync.sync import process_blocks_from_peer
from log_utils import get_logger
from blockchain.block_height_index import get_height_index

logger = get_logger(__name__)

# Import NAT traversal
try:
    from network.nat_traversal import nat_traversal, SimpleSTUN
    NAT_TRAVERSAL_AVAILABLE = True
except ImportError:
    nat_traversal = None
    SimpleSTUN = None
    NAT_TRAVERSAL_AVAILABLE = False

kad_server = None
own_ip = None

def b2s(v: bytes | str | None) -> str | None:
    """Decode bytes from DB/DHT to str; leave str or None unchanged."""
    return v.decode() if isinstance(v, bytes) else v

async def get_external_ip():
    global own_ip
    if own_ip:
        return own_ip
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.ipify.org") as resp:
            own_ip = await resp.text()
    return own_ip

async def run_kad_server(port, bootstrap_addr=None, wallet=None, gossip_node=None, ip_address=None, gossip_port=None):
    global kad_server
    try:
        kad_server = KademliaServer()
        await kad_server.listen(port)
    except Exception as e:
        logger.error(f"Failed to start Kademlia server: {e}")
        raise
    
    if bootstrap_addr:
        # Resolve hostnames to IPs if needed
        resolved_bootstrap = []
        for addr in bootstrap_addr:
            if isinstance(addr[0], str) and not addr[0].replace('.', '').isdigit():
                # It's a hostname, resolve it asynchronously
                try:
                    import socket
                    # Use asyncio's getaddrinfo for non-blocking DNS resolution
                    loop = asyncio.get_event_loop()
                    addrinfo = await loop.getaddrinfo(addr[0], addr[1], family=socket.AF_INET, type=socket.SOCK_DGRAM)
                    if addrinfo:
                        ip = addrinfo[0][4][0]  # Get the IP address from the first result
                        resolved_bootstrap.append((ip, addr[1]))
                        logger.info(f"Resolved {addr[0]} to {ip}")
                    else:
                        logger.error(f"No address info found for {addr[0]}")
                        resolved_bootstrap.append(addr)
                except Exception as e:
                    logger.error(f"Failed to resolve {addr[0]}: {e}")
                    # In Docker, try using the hostname directly
                    resolved_bootstrap.append(addr)
            else:
                resolved_bootstrap.append(addr)
        
        # Filter out our own address from bootstrap list
        # Only filter if it's actually our local address (not just same port)
        local_addrs = [('127.0.0.1', port), ('localhost', port), ('0.0.0.0', port)]
        if ip_address:
            local_addrs.append((ip_address, port))
        bootstrap_addr = [addr for addr in resolved_bootstrap if addr not in local_addrs]
        if bootstrap_addr:
            await kad_server.bootstrap(bootstrap_addr)
            logger.info(f"Bootstrapped to {bootstrap_addr}")
            
            # Verify bootstrap success
            await asyncio.sleep(2)
            neighbors = kad_server.bootstrappable_neighbors()
            if not neighbors:
                logger.warning("Bootstrap may have failed - no neighbors found")
            else:
                logger.info(f"Bootstrap successful - {len(neighbors)} neighbors found")
    
    logger.info(f"Validator {VALIDATOR_ID} running DHT on port {port}")
    
    # Wait a bit more before registering to ensure DHT is ready
    await asyncio.sleep(2)
    await register_validator_once()
    
    if wallet and gossip_node and ip_address:
        # Use the provided IP address and gossip port instead of defaults
        actual_gossip_port = gossip_port if gossip_port else DEFAULT_GOSSIP_PORT
        is_bootstrap = bootstrap_addr is None
        logger.info(f"Node type: {'bootstrap' if is_bootstrap else 'validator'}, IP: {ip_address}, Port: {actual_gossip_port}")
        
        # CRITICAL: If this is a bootstrap node, immediately add all known validators as protected peers
        if is_bootstrap:
            logger.info("Bootstrap node: Adding all known validators as PROTECTED peers")
            for validator_ip, validator_info in KNOWN_VALIDATORS.items():
                validator_port = validator_info.get('port', DEFAULT_GOSSIP_PORT)
                validator_name = validator_info.get('name', 'unknown')
                logger.info(f"Adding KNOWN validator {validator_name} at {validator_ip}:{validator_port} as PROTECTED peer")
                gossip_node.add_peer(validator_ip, validator_port, 
                                   peer_info={'protected': True, 'name': validator_name}, 
                                   protected=True)
        
        await announce_gossip_port(wallet, ip=ip_address, port=actual_gossip_port, gossip_node=gossip_node, is_bootstrap=is_bootstrap)
        
        # All nodes should discover peers
        await discover_peers_once(gossip_node)
        
        # For non-bootstrap nodes, always add bootstrap server as a peer to ensure we can sync
        if not is_bootstrap:
            logger.info("Adding bootstrap server as a gossip peer for synchronization")
            # Resolve bootstrap hostname if needed
            import socket
            bootstrap_host = bootstrap_addr[0][0] if bootstrap_addr else "api.bitcoinqs.org"
            try:
                # Resolve hostname to IP asynchronously
                loop = asyncio.get_event_loop()
                addrinfo = await loop.getaddrinfo(bootstrap_host, None, family=socket.AF_INET)
                if addrinfo:
                    bootstrap_ip = addrinfo[0][4][0]
                    logger.info(f"Resolved bootstrap server {bootstrap_host} to {bootstrap_ip}")
                else:
                    bootstrap_ip = bootstrap_host
                    logger.warning(f"No address info found for {bootstrap_host}, using as-is")
            except Exception as e:
                bootstrap_ip = bootstrap_host
                logger.warning(f"Could not resolve {bootstrap_host}: {e}, using as-is")
            
            bootstrap_info = {
                "ip": bootstrap_ip,
                "port": 8002,  # Bootstrap gossip port
                "validator_id": "bootstrap",
                "nat_type": "direct"
            }
            gossip_node.add_peer(bootstrap_info["ip"], bootstrap_info["port"], peer_info=bootstrap_info)
        
        if not is_bootstrap:
            # Regular nodes: periodic peer discovery
            asyncio.create_task(periodic_peer_discovery(gossip_node))
        else:
            # Bootstrap nodes: both peer discovery and maintenance
            asyncio.create_task(periodic_peer_discovery(gossip_node))
            asyncio.create_task(bootstrap_maintenance(gossip_node, wallet, ip_address, actual_gossip_port))
        
        # Start heartbeat and validator list maintenance for all nodes
        asyncio.create_task(update_heartbeat())
        asyncio.create_task(maintain_validator_list(gossip_node))
    
    # Keep DHT server running continuously
    logger.info("DHT server is running and will continue serving...")
    try:
        while not shutdown_event.is_set():
            await asyncio.sleep(30)  # Check every 30 seconds
            # Optionally log DHT status
            try:
                neighbors = kad_server.bootstrappable_neighbors() if kad_server else []
                logger.debug(f"DHT status: {len(neighbors)} neighbors")
            except Exception as e:
                logger.error(f"Error checking DHT status: {e}")
    except asyncio.CancelledError:
        logger.info("DHT server shutting down...")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in DHT server main loop: {e}", exc_info=True)
        raise
    
    return kad_server

async def register_validator_once():
    """Register validator using individual keys to avoid race conditions"""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Register this validator with a unique key
            validator_key = f"validator_{VALIDATOR_ID}"
            validator_info = {
                "id": VALIDATOR_ID,
                "joined_at": int(time.time()),
                "active": True,
                "known_peers": list(known_validators)  # Share what we know
            }
            
            await kad_server.set(validator_key, json.dumps(validator_info))
            logger.info(f"Validator registered: {VALIDATOR_ID}")
            
            # Also update the shared list with all validators we know about
            all_validators = known_validators.copy()
            all_validators.add(VALIDATOR_ID)
            
            try:
                # Get current list and merge with our knowledge
                existing_json = b2s(await kad_server.get(VALIDATORS_LIST_KEY))
                if existing_json:
                    existing = set(json.loads(existing_json))
                    all_validators.update(existing)
                
                # Write the merged list
                await kad_server.set(VALIDATORS_LIST_KEY, json.dumps(sorted(list(all_validators))))
                logger.info(f"Updated validator list with {len(all_validators)} validators")
            except Exception as e:
                logger.warning(f"Failed to update validator list (non-critical): {e}")
            
            return  # Success
            
        except Exception as e:
            logger.error(f"Registration attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Failed to register validator after {max_retries} attempts")

async def announce_gossip_port(wallet, ip="127.0.0.1", port=None, gossip_node=None, is_bootstrap=False):
    if not kad_server:
        logger.error("Cannot announce gossip port: kad_server not initialized")
        return
    
    if port is None:
        port = DEFAULT_GOSSIP_PORT
    
    # Determine if we should use NAT traversal
    external_ip = ip
    external_port = port
    nat_type = "direct"
    
    # Check if we're in a Docker/private network environment
    import ipaddress
    try:
        ip_obj = ipaddress.ip_address(ip)
        is_private = ip_obj.is_private
    except:
        is_private = False
    
    # Only try NAT traversal if:
    # - NAT traversal is available
    # - We're not using a private IP (Docker/local network)
    # - We're not explicitly in bootstrap mode with private IP
    if NAT_TRAVERSAL_AVAILABLE and nat_traversal and not is_private:
        # Try UPnP mapping
        upnp_port = await nat_traversal.setup_upnp(port, 'TCP')
        if upnp_port and nat_traversal.external_ip:
            external_ip = nat_traversal.external_ip
            external_port = upnp_port
            nat_type = "upnp"
            logger.info(f"UPnP mapping successful: {ip}:{port} -> {external_ip}:{external_port}")
        elif SimpleSTUN:
            # Try STUN as fallback
            stun_result = await SimpleSTUN.get_external_address(port)
            if stun_result:
                external_ip, external_port = stun_result
                nat_type = "stun"
                logger.info(f"STUN discovery successful: {external_ip}:{external_port}")
    elif is_private:
        logger.info(f"Using private network address {ip}:{port} - NAT traversal not needed")
    
    key = f"gossip_{VALIDATOR_ID}"
    # Enhanced info with NAT details
    info = {
        "ip": external_ip,
        "port": external_port,
        "local_ip": ip,
        "local_port": port,
        "nat_type": nat_type,
        "supports_nat_traversal": NAT_TRAVERSAL_AVAILABLE,
        "publicKey": wallet.get("publicKey", "")  # Include publicKey for validator_keys
    }
    
    for attempt in range(5):
        try:
            logger.info(f"Attempting to announce gossip port (attempt {attempt+1}/5): {key} -> {info}")
            await kad_server.set(key, json.dumps(info))
            stored_value = await kad_server.get(key)
            if stored_value:
                stored_info = json.loads(stored_value)
                # Check if essential fields match
                if stored_info.get("ip") == info["ip"] and stored_info.get("port") == info["port"]:
                    logger.info(f"Successfully announced gossip info with NAT type '{nat_type}': {key} -> {info}")
                    # Bootstrap nodes should not add themselves as peers
                    return
                else:
                    logger.warning(f"Stored info doesn't match: expected {info}, got {stored_info}")
            else:
                logger.warning(f"Failed to retrieve stored value for {key}")
        except Exception as e:
            logger.error(f"Error during gossip announcement: {e}")
        await asyncio.sleep(2)
    logger.error("Failed to announce gossip port after 5 attempts")

async def bootstrap_maintenance(gossip_node, wallet, ip_address, port):
    """Maintenance tasks for bootstrap nodes"""
    while not shutdown_event.is_set():
        try:
            # Re-announce our presence periodically
            neighbors = kad_server.bootstrappable_neighbors() if kad_server else []
            logger.info(f"Bootstrap node maintenance: {len(neighbors)} neighbors")
            
            # Try to re-announce even without neighbors (for new nodes to find us)
            if kad_server:
                # Re-announce validator list
                await register_validator_once()
                # Re-announce gossip info
                await announce_gossip_port(wallet, ip=ip_address, port=port, gossip_node=gossip_node, is_bootstrap=True)
                
        except Exception as e:
            logger.error(f"Error in bootstrap maintenance: {e}", exc_info=True)
        await asyncio.sleep(60)  # Check every minute

async def periodic_peer_discovery(gossip_node):
    """Periodically discover new peers from DHT"""
    last_validator_check = 0
    while not shutdown_event.is_set():
        try:
            await discover_peers_once(gossip_node)
            
            # Periodically re-register to share our peer knowledge
            current_time = time.time()
            if current_time - last_validator_check > 60:  # Every minute for faster convergence
                last_validator_check = current_time
                logger.info("Periodic validator list refresh and reconciliation")
                await register_validator_once()
                
                # Force a rediscovery after registration to pick up any new peers
                await discover_peers_once(gossip_node)
            
            # Also re-announce our gossip info periodically
            if kad_server:
                neighbors = kad_server.bootstrappable_neighbors()
                if len(neighbors) > 0:
                    # We have neighbors, try to re-announce
                    logger.debug(f"Re-announcing to {len(neighbors)} neighbors")
        except Exception as e:
            logger.error(f"Error in periodic peer discovery: {e}", exc_info=True)
        await asyncio.sleep(30)  # Check every 30 seconds

async def discover_peers_once(gossip_node):
    """Discover peers using a reconciliation approach"""
    discovered_validators = set()
    
    # Check if kad_server is initialized
    if not kad_server:
        logger.warning("DHT server not initialized yet, skipping peer discovery")
        return
    
    # First, add ourselves to ensure we're always in the set
    discovered_validators.add(VALIDATOR_ID)
    
    # Get the current validator list
    try:
        validators_json = await kad_server.get(VALIDATORS_LIST_KEY)
        if validators_json:
            try:
                validator_ids = json.loads(validators_json)
                discovered_validators.update(validator_ids)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse validator list JSON: {e}")
            except Exception as e:
                logger.warning(f"Failed to process validator list: {e}")
    except Exception as e:
        logger.warning(f"Failed to get validator list: {e}")
    
    # Check each validator's individual registration
    validators_to_check = list(discovered_validators)
    for vid in validators_to_check:
        if vid == VALIDATOR_ID:
            continue
        validator_key = f"validator_{vid}"
        try:
            validator_info = await kad_server.get(validator_key)
            if validator_info:
                info = json.loads(validator_info)
                # Also check if they know about other validators
                if "known_peers" in info:
                    discovered_validators.update(info["known_peers"])
        except:
            pass
    
    # Reconcile the validator list if we found new ones
    if len(discovered_validators) > len(known_validators):
        logger.info(f"Found new validators, updating list: {discovered_validators}")
        try:
            await kad_server.set(VALIDATORS_LIST_KEY, json.dumps(sorted(list(discovered_validators))))
        except Exception as e:
            logger.warning(f"Failed to update validator list: {e}")
    
    # Update our known validators
    known_validators.clear()
    known_validators.update(discovered_validators)
    logger.info(f"Total discovered validators: {list(discovered_validators)}")
    
    # Now discover gossip endpoints for each validator
    discovered_count = 0
    
    logger.info(f"Checking gossip info for {len(discovered_validators)} validators...")
    for vid in discovered_validators:
        if vid == VALIDATOR_ID:
            logger.debug(f"Skipping self (validator {vid})")
            continue

        # Early-exit: stop discovering if already at outbound peer capacity
        if gossip_node and hasattr(gossip_node, 'dht_peers'):
            if len(gossip_node.dht_peers) >= MAX_OUTBOUND_CONNECTIONS:
                logger.info(
                    f"At outbound peer capacity ({len(gossip_node.dht_peers)}/{MAX_OUTBOUND_CONNECTIONS}), "
                    f"stopping discovery"
                )
                break

        gossip_key = f"gossip_{vid}"
        try:
            logger.debug(f"Looking up gossip info for validator {vid} with key {gossip_key}")
            gossip_info_json = await kad_server.get(gossip_key)
            if not gossip_info_json:
                logger.warning(f"No gossip info found for validator {vid}")
                continue

            info = json.loads(gossip_info_json)

            # Handle both old format (just ip/port) and new NAT-aware format
            if isinstance(info, dict):
                ip = info.get("ip", info.get("external_ip"))
                port = info.get("port", info.get("external_port"))

                # Skip if it's our own validator ID (dynamic check)
                if vid == VALIDATOR_ID:
                    logger.info(f"Skipping self-connection to {vid} at {ip}:{port}")
                    continue

                try:
                    # Also check IP-based self-detection using dynamic values
                    if ip == own_ip or (nat_traversal and hasattr(nat_traversal, 'external_ip') and ip == nat_traversal.external_ip):
                        logger.info(f"Skipping self-connection based on IP match: {ip}")
                        continue
                except:
                    pass

                # Store full peer info for NAT traversal
                logger.info(f"Adding peer {vid} at {ip}:{port} to gossip node")
                gossip_node.add_peer(ip, port, peer_info=info)
                discovered_count += 1

                nat_type = info.get("nat_type", "unknown")
                logger.info(f"Successfully added peer {vid} at {ip}:{port} (NAT type: {nat_type})")

                # Also store publicKey in validator_keys for TX signature validation
                if "publicKey" in info:
                    validator_keys[vid] = info["publicKey"]
                    logger.info(f"Added validator publicKey for {vid}")
            else:
                # Old format compatibility
                logger.warning(f"Old peer info format for {vid}: {info}")
        except Exception as e:
            logger.error(f"Error processing peer {vid}: {e}")
    
    logger.info(f"Peer discovery complete: discovered {discovered_count}/{len(discovered_validators)-1} peers")


async def push_blocks(peer_ip, peer_port):
    logger.info(f"Pushing blocks to peer {peer_ip}:{peer_port}")
    db = get_db()

    height_request = {
        "type": "get_height",
        "timestamp": int(time.time() * 1000)
    }

    height_temp = await get_current_height(db)
    local_height = height_temp[0]
    local_tip = height_temp[1]

    logger.info(f"Local height: {local_height}, Local tip: {local_tip}")

    logger.info("Opening connection...")
    try:
        r, w = await asyncio.open_connection(peer_ip, peer_port, limit=100 * 1024 * 1024)  # 100MB limit
        logger.info("Opened connection.")

        # Ask peer for its height
        w.write((json.dumps(height_request) + "\n").encode('utf-8'))
        await w.drain()
        line = await r.readline()
        if not line:
            raise ValueError("Empty response when querying height")

        msg = json.loads(line.decode('utf-8').strip())

        if msg.get("type") == "height_response":
            logger.info(f"Peer height response: {msg}")
            peer_height = msg.get("height")
            #peer_tip = msg.get("current_tip")
            logger.info(f"*** Peer {peer_ip} responded with height {peer_height}")

        # Validate peer height before any comparisons
        try:
            peer_height_int = int(peer_height) if peer_height is not None else -1
        except (ValueError, TypeError):
            logger.error(f"Invalid peer height received: {peer_height} (type: {type(peer_height)})")
            logger.error(f"Cannot sync with peer {peer_ip} - invalid height")
            w.close()
            await w.wait_closed()
            return
        
        # Validate local height as well
        if not isinstance(local_height, int):
            logger.error(f"Invalid local height: {local_height} (type: {type(local_height)})")
            w.close()
            await w.wait_closed()
            return

        # Only push if our height is greater than peer's
        if peer_height_int < local_height:
            logger.info("Will push blocks to peer")

            start_height = peer_height_int + 1  # Fixed: proper integer addition
            end_height = local_height

            blocks_to_send = []
            height_index = get_height_index()

            for h in range(start_height, end_height+1):
                # Use the efficient height index
                found_block = height_index.get_block_by_height(h)
                
                if not found_block:
                    logger.warning(f"Block at height {h} not found!")
                    continue
                
                # Make a deep copy to avoid modifying the original
                import copy
                found_block = copy.deepcopy(found_block)

                # Now fetch full transaction objects
                full_transactions = []

                for tx_id in found_block.get("tx_ids", []):
                    tx_key = f"tx:{tx_id}".encode()
                    if tx_key in db:
                        tx_obj = json.loads(db[tx_key].decode())
                        # Ensure the transaction has txid field
                        tx_obj["txid"] = tx_id
                        full_transactions.append(tx_obj)

                # Legacy coinbase lookup removed - coinbase is already included in regular transactions

                found_block["full_transactions"] = full_transactions

                blocks_to_send.append(found_block)

            # Validate blocks before sending
            validated_blocks = []
            for block in blocks_to_send:
                if isinstance(block.get("height"), str) and len(block.get("height")) == 64:
                    logger.error(f"CRITICAL: About to send block with hash in height field: {block}")
                    continue  # Skip this malformed block
                validated_blocks.append(block)
            
            # Now send the blocks
            blocks_message = {
                "type": "blocks_response",
                "blocks": validated_blocks,
                "timestamp": int(time.time() * 1000)
            }
            message_json = json.dumps(blocks_message)
            logger.info(f"Sending message of {len(message_json)} bytes to {peer_ip}")
            w.write((message_json + "\n").encode('utf-8'))
            await w.drain()
            logger.info(f"Sent {len(blocks_to_send)} blocks to {peer_ip}")
        
        elif peer_height_int > local_height:
            # We need blocks from peer
            logger.info(f"Will pull blocks from peer (peer_height={peer_height_int}, local_height={local_height})")
            
            # Keep connection alive and sync all blocks in batches
            current_height = local_height
            total_blocks_needed = peer_height_int - (local_height if local_height >= 0 else -1)
            blocks_synced = 0
            sync_start_time = time.time()
            
            logger.info(f"Starting sync: need {total_blocks_needed} blocks (from {local_height + 1} to {peer_height_int})")
            
            # Continue syncing until we're caught up
            while current_height < peer_height_int:
                if current_height == -1:
                    # We have no blocks, request from genesis
                    start_height = 0
                else:
                    start_height = current_height + 1
                
                # Cap the end height to avoid requesting too many blocks at once
                end_height = min(peer_height_int, start_height + MAX_BLOCKS_PER_SYNC_REQUEST - 1)
                
                # Calculate and log progress
                progress_pct = (blocks_synced / total_blocks_needed * 100) if total_blocks_needed > 0 else 0
                elapsed_time = time.time() - sync_start_time
                if blocks_synced > 0:
                    blocks_per_sec = blocks_synced / elapsed_time
                    remaining_blocks = total_blocks_needed - blocks_synced
                    eta_seconds = remaining_blocks / blocks_per_sec if blocks_per_sec > 0 else 0
                    eta_str = f", ETA: {int(eta_seconds)}s" if blocks_per_sec > 0 else ""
                else:
                    eta_str = ""
                
                logger.info(f"Sync progress: {progress_pct:.1f}% ({blocks_synced}/{total_blocks_needed} blocks){eta_str}")
                logger.info(f"Requesting blocks from height {start_height} to {end_height}")

                get_blocks_request = {
                    "type": "get_blocks",
                    "start_height": start_height,
                    "end_height": end_height,
                    "timestamp": int(time.time() * 1000)
                }
                w.write((json.dumps(get_blocks_request) + "\n").encode('utf-8'))
                await w.drain()

                # First, read the response header to check if it's chunked
                header_line = await r.readline()
                if not header_line:
                    logger.error("Peer closed the connection during sync")
                    break
                
                try:
                    header = json.loads(header_line.decode())
                except json.JSONDecodeError as e:
                    logger.error(f"Bad JSON header from peer: {e}")
                    break
                
                if header.get("type") == "blocks_response_chunked":
                    # Handle chunked response
                    total_chunks = header.get("total_chunks", 0)
                    blocks = []
                    
                    logger.info(f"Receiving chunked response with {total_chunks} chunks")
                    
                    for chunk_num in range(total_chunks):
                        chunk_line = await r.readline()
                        if not chunk_line:
                            logger.error(f"Connection closed while reading chunk {chunk_num}")
                            break
                        
                        try:
                            chunk_data = json.loads(chunk_line.decode())
                            if chunk_data.get("chunk_num") != chunk_num:
                                logger.error(f"Expected chunk {chunk_num}, got {chunk_data.get('chunk_num')}")
                                break
                            
                            blocks.extend(chunk_data.get("blocks", []))
                            logger.info(f"Received chunk {chunk_num + 1}/{total_chunks} with {len(chunk_data.get('blocks', []))} blocks")
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Bad JSON in chunk {chunk_num}: {e}")
                            break
                    
                    response = {"blocks": blocks}
                else:
                    # Handle regular response (backward compatibility)
                    response = header

                blocks = sorted(response.get("blocks", []), key=lambda x: x["height"])
                logger.info(f"Received {len(blocks)} blocks from from peer")
            
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
                
                # Process the blocks
                await process_blocks_from_peer(blocks)
                
                # Update our current height based on blocks we processed
                if blocks:
                    # Get the highest block height we just processed
                    highest_block = max(blocks, key=lambda x: x.get("height", -1))
                    new_height = highest_block.get("height", current_height)
                    blocks_in_batch = len(blocks)
                    
                    # Update tracking variables
                    current_height = new_height
                    blocks_synced += blocks_in_batch
                    
                    logger.info(f"Processed {blocks_in_batch} blocks, new height: {current_height}")
                else:
                    logger.warning("No valid blocks received in this batch")
                    break
            
            # Log final sync summary
            total_sync_time = time.time() - sync_start_time
            if blocks_synced > 0:
                blocks_per_sec = blocks_synced / total_sync_time
                logger.info(f"Sync completed: {blocks_synced} blocks in {total_sync_time:.1f}s ({blocks_per_sec:.1f} blocks/s)")
            else:
                logger.info("No blocks were synced")

        else:
            logger.info(f"Peer up to date (peer_height={peer_height_int}, local_height={local_height})")
            logger.info(f"No sync needed: peer_height={peer_height_int}, local_height={local_height}")

        w.close()
        await w.wait_closed()
    except Exception as e:
        logger.error(f"Could not connect to peer: {e}")




async def discover_peers_periodically(gossip_node, local_ip=None):
    known_peers = set()
    # Use provided local IP or fall back to global own_ip
    current_ip = local_ip if local_ip else own_ip
    while not shutdown_event.is_set():
        try:
            # Check if kad_server is initialized
            if not kad_server:
                logger.warning("DHT server not initialized, waiting...")
                await asyncio.sleep(5)
                continue
                
            validators_json = await kad_server.get(VALIDATORS_LIST_KEY)
            try:
                validator_ids = json.loads(validators_json) if validators_json else []
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning(f"Failed to parse validators JSON in discover_peers_periodically: {e}")
                validator_ids = []
            logger.info(f"Discovered {len(validator_ids)} validators in DHT")
            
            for vid in validator_ids:
                if vid == VALIDATOR_ID:
                    continue
                gossip_key = f"gossip_{vid}"
                gossip_info_json = await kad_server.get(gossip_key)
                if gossip_info_json:
                    info = json.loads(gossip_info_json)
                    # Handle both old and new format
                    ip = info.get("ip", info.get("external_ip"))
                    port = info.get("port", info.get("external_port"))
                    
                    if ip == current_ip:
                        continue
                    peer = (ip, port)
                    
                    # Always call add_peer - it will handle re-adding failed peers
                    gossip_node.add_peer(ip, port, peer_info=info)
                    
                    if peer not in known_peers:
                        logger.info(f"Connected to peer {vid} at {peer}")
                        known_peers.add(peer)
                    else:
                        # Peer already known, but add_peer will reset failure count if needed
                        logger.debug(f"Re-checking peer {vid} at {peer}")
                else:
                    logger.warning(f"No gossip info found for validator {vid}")
        except Exception as e:
            logger.error(f"Error in discover_peers_periodically: {e}")
        await asyncio.sleep(5)

async def update_heartbeat():
    heartbeat_key = f"validator_{VALIDATOR_ID}_heartbeat"
    while not shutdown_event.is_set():
        try:
            # Check if kad_server is initialized
            if not kad_server:
                logger.debug("DHT server not initialized, skipping heartbeat")
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                continue
            
            # Check if we have any neighbors before trying to set values
            try:
                neighbors = kad_server.bootstrappable_neighbors()
                if not neighbors:
                    logger.debug("No DHT neighbors available, skipping heartbeat update")
                    await asyncio.sleep(HEARTBEAT_INTERVAL)
                    continue
            except Exception as e:
                logger.debug(f"Could not check neighbors: {e}")
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                continue
                
            # Always try to update heartbeat, not just when bootstrapped
            await kad_server.set(heartbeat_key, str(time.time()))
            logger.debug(f"Updated heartbeat for {VALIDATOR_ID}")
        except Exception as e:
            # Only log as warning if it's not the "empty sequence" error
            if "empty sequence" not in str(e):
                logger.warning(f"Failed to update heartbeat: {e}")
            else:
                logger.debug(f"DHT not ready for heartbeat update (no nodes): {e}")
        await asyncio.sleep(HEARTBEAT_INTERVAL)

async def maintain_validator_list(gossip_node):
    while not shutdown_event.is_set():
        try:
            # CRITICAL: Keep all validators that are in our gossip peer list
            # These are ACTIVE connections that we should NEVER remove just because DHT is down
            active_validators = set()
            
            # Check gossip node for active peers
            if gossip_node and hasattr(gossip_node, 'dht_peers'):
                for peer in gossip_node.dht_peers:
                    # Find validator ID for this peer
                    for v, info in validator_peer_mapping.items():
                        if info.get('ip') == peer[0] and info.get('port') == peer[1]:
                            active_validators.add(v)
                            logger.debug(f"Validator {v} is actively connected via gossip at {peer}")
            
            # Check if kad_server is initialized
            if not kad_server:
                logger.debug("DHT server not initialized, keeping existing validators")
                # Keep all known validators when DHT is down
                alive = known_validators.copy()
                alive.add(VALIDATOR_ID)
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                continue
                
            try:
                dht_list_json = await kad_server.get(VALIDATORS_LIST_KEY)
                dht_set = set(json.loads(dht_list_json)) if dht_list_json else set()
            except Exception as e:
                logger.error(f"Failed to fetch validator list from DHT: {e}")
                # CRITICAL: When DHT fails, keep ALL known validators
                logger.warning("DHT is down - keeping all known validators to prevent disconnection")
                alive = known_validators.copy()
                alive.add(VALIDATOR_ID)
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                continue

            current_time = time.time()
            alive = set()
            force_keep = set()  # Track validators we must keep regardless

            # Always keep actively connected validators
            alive.update(active_validators)
            force_keep.update(active_validators)
            
            # Check heartbeats for all validators
            for v in dht_set:
                try:
                    # Skip heartbeat check for actively connected validators
                    if v in active_validators:
                        alive.add(v)
                        continue
                        
                    last_seen_raw = await kad_server.get(f"validator_{v}_heartbeat")
                    last_seen_str = b2s(last_seen_raw)
                    last_seen = float(last_seen_str) if last_seen_str else None
                    if last_seen and (current_time - last_seen) <= VALIDATOR_TIMEOUT:
                        alive.add(v)
                        # Reset failure count on successful heartbeat
                        if v in validator_heartbeat_failures:
                            del validator_heartbeat_failures[v]
                    else:
                        # Heartbeat is stale - INCREMENT the failure counter
                        validator_heartbeat_failures[v] = validator_heartbeat_failures.get(v, 0) + 1
                        
                        # Check if validator is in our known list and hasn't exceeded failure threshold
                        if v in known_validators:
                            if validator_heartbeat_failures[v] < MAX_HEARTBEAT_FAILURES:
                                if last_seen:
                                    logger.warning(f"Validator {v} heartbeat is stale ({current_time - last_seen:.1f}s old), "
                                                f"failures: {validator_heartbeat_failures[v]}/{MAX_HEARTBEAT_FAILURES}, keeping in list")
                                else:
                                    logger.warning(f"Validator {v} has no heartbeat recorded, "
                                                f"failures: {validator_heartbeat_failures[v]}/{MAX_HEARTBEAT_FAILURES}, keeping in list")
                                alive.add(v)  # Keep validator alive until it exceeds failure threshold
                            else:
                                logger.error(f"Validator {v} exceeded failure threshold ({validator_heartbeat_failures[v]}/{MAX_HEARTBEAT_FAILURES}), "
                                           f"will be removed from list")
                        else:
                            # Not a known validator, apply stricter timeout (1 failure)
                            if validator_heartbeat_failures[v] < 1:
                                logger.info(f"Unknown validator {v} has stale heartbeat, failures: {validator_heartbeat_failures[v]}/1")
                                alive.add(v)  # Give unknown validators only 1 chance
                            else:
                                logger.info(f"Removing unknown validator {v} after {validator_heartbeat_failures[v]} failures")
                except Exception as e:
                    logger.warning(f"Failed to fetch heartbeat for {v}: {e}")
                    # On DHT lookup failure, keep the validator if it's known
                    if v in known_validators or v in active_validators:
                        logger.info(f"Keeping validator {v} despite DHT lookup failure (known or active)")
                        alive.add(v)

            alive.add(VALIDATOR_ID)

            if alive != dht_set:
                try:
                    await kad_server.set(VALIDATORS_LIST_KEY, json.dumps(list(alive)))
                except Exception as e:
                    logger.error(f"Failed to update validator list in DHT: {e}")

            # Process newly joined validators
            newly_joined = alive - known_validators
            for v in newly_joined:
                if v == VALIDATOR_ID:
                    continue
                try:
                    gossip_info_json = await kad_server.get(f"gossip_{v}")
                    if gossip_info_json:
                        info = json.loads(gossip_info_json)
                        # Handle both old and new format
                        ip = info.get("ip", info.get("external_ip"))
                        port = info.get("port", info.get("external_port"))
                        public_key = info.get("publicKey", "")
                        
                        if ip and port and ip != own_ip:
                            logger.info(f"Adding new validator {v} at {ip}:{port} to gossip peers")
                            # CRITICAL: Mark all validators as protected on bootstrap servers
                            is_protected = True  # All validators are protected!
                            gossip_node.add_peer(ip, port, peer_info=info, protected=is_protected)
                            # Store the mapping for this validator
                            validator_peer_mapping[v] = {'ip': ip, 'port': port}
                            #await push_blocks(ip, port)
                            if public_key:
                                validator_keys[v] = public_key
                            logger.info(f"New validator joined: {v} at {ip}:{port}")
                            # Reset any failure tracking for this validator
                            if v in validator_heartbeat_failures:
                                del validator_heartbeat_failures[v]
                except Exception as e:
                    logger.warning(f"Failed to process new validator {v}: {e}")

            # Process validators that have left
            just_left = known_validators - alive
            for v in just_left:
                try:
                    # CRITICAL: Never remove actively connected validators
                    if v in active_validators:
                        logger.warning(f"REFUSING to remove validator {v} - it has an active gossip connection!")
                        force_keep.add(v)  # Use force_keep instead of modifying alive directly
                        continue
                    
                    # Check if validator is still in gossip peer list
                    validator_info = validator_peer_mapping.get(v, {})
                    if validator_info:
                        peer = (validator_info.get('ip'), validator_info.get('port'))
                        if gossip_node and hasattr(gossip_node, 'dht_peers') and peer in gossip_node.dht_peers:
                            logger.warning(f"REFUSING to remove validator {v} at {peer} - still in gossip peer list!")
                            force_keep.add(v)  # Use force_keep instead of modifying alive directly
                            continue
                    
                    # Only remove if DHT is actually working
                    if not kad_server:
                        logger.warning(f"REFUSING to remove validator {v} - DHT is not initialized")
                        force_keep.add(v)
                        continue
                        
                    # Check if this validator has exceeded failure threshold
                    if validator_heartbeat_failures.get(v, 0) < MAX_HEARTBEAT_FAILURES:
                        logger.info(f"Validator {v} marked as left but hasn't exceeded failure threshold ({validator_heartbeat_failures.get(v, 0)}/{MAX_HEARTBEAT_FAILURES}), skipping removal")
                        force_keep.add(v)  # Use force_keep instead of modifying alive directly
                        continue
                        
                    # Only proceed with removal if we're ABSOLUTELY SURE
                    logger.error(f"Removing validator {v} after {validator_heartbeat_failures.get(v, 0)} failures - this should be rare!")
                    
                    gossip_info_json = await kad_server.get(f"gossip_{v}")
                    if gossip_info_json:
                        info = json.loads(gossip_info_json)
                        # Handle both old and new format
                        ip = info.get("ip", info.get("external_ip"))
                        port = info.get("port", info.get("external_port"))
                        if ip and port:
                            logger.warning(f"Removing validator {v} at {ip}:{port} after {validator_heartbeat_failures.get(v, 0)} failures")
                            gossip_node.remove_peer(ip, port)
                    
                    # Clean up DHT entries for the dead validator
                    try:
                        logger.info(f"Cleaning up DHT entries for validator {v}")
                        # Note: Kademlia doesn't support delete, so we set to None or empty string
                        # This effectively removes the entry from active use
                        await kad_server.set(f"validator_{v}_heartbeat", "")
                        await kad_server.set(f"gossip_{v}", "")
                        await kad_server.set(f"validator_{v}", "")
                        logger.info(f"DHT cleanup completed for validator {v}")
                    except Exception as cleanup_error:
                        logger.warning(f"Failed to clean up DHT entries for {v}: {cleanup_error}")
                    
                    validator_keys.pop(v, None)
                    validator_peer_mapping.pop(v, None)
                    # Clear failure tracking
                    if v in validator_heartbeat_failures:
                        del validator_heartbeat_failures[v]
                    logger.info(f"Validator left: {v}")
                except Exception as e:
                    logger.warning(f"Failed to remove validator {v}: {e}")
                    # On error, keep the validator
                    force_keep.add(v)

            # Merge force_keep validators back into alive set to avoid race conditions
            alive.update(force_keep)
            
            # Update known validators
            known_validators.clear()
            known_validators.update(alive)

            await asyncio.sleep(HEARTBEAT_INTERVAL)
        except Exception as e:
            logger.error(f"Error in maintain_validator_list: {e}")
            await asyncio.sleep(HEARTBEAT_INTERVAL)