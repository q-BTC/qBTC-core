import asyncio
import json
import time
import aiohttp
from kademlia.network import Server as KademliaServer
from config.config import  shutdown_event, VALIDATOR_ID, HEARTBEAT_INTERVAL, VALIDATOR_TIMEOUT, VALIDATORS_LIST_KEY, DEFAULT_GOSSIP_PORT
from state.state import validator_keys, known_validators
from database.database import get_db,get_current_height
from sync.sync import process_blocks_from_peer
from log_utils import get_logger

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
                # It's a hostname, resolve it
                try:
                    import socket
                    ip = socket.gethostbyname(addr[0])
                    resolved_bootstrap.append((ip, addr[1]))
                    logger.info(f"Resolved {addr[0]} to {ip}")
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
        await announce_gossip_port(wallet, ip=ip_address, port=actual_gossip_port, gossip_node=gossip_node, is_bootstrap=is_bootstrap)
        
        # All nodes should discover peers
        await discover_peers_once(gossip_node)
        
        # For non-bootstrap nodes, if we didn't find any peers, add bootstrap as fallback
        if not is_bootstrap and len(gossip_node.dht_peers) == 0:
            logger.warning("No peers discovered via DHT, adding bootstrap as fallback peer")
            # Add bootstrap node as a peer directly
            bootstrap_info = {
                "ip": bootstrap_addr[0][0] if bootstrap_addr else "api.bitcoinqs.org",
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
            validator_ids = json.loads(validators_json)
            discovered_validators.update(validator_ids)
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
    print("******* IM IN PUSH BLOCKS ******")
    print(peer_ip)
    print(peer_port)
    db = get_db()

    height_request = {
        "type": "get_height",
        "timestamp": int(time.time() * 1000)
    }

    height_temp = get_current_height(db)
    local_height = height_temp[0]
    local_tip = height_temp[1]

    print(f"Local height: {local_height}, Local tip: {local_tip}")

    print("Opening connection...")
    try:
        r, w = await asyncio.open_connection(peer_ip, peer_port, limit=100 * 1024 * 1024)  # 100MB limit
        print("Opened connection.")

        # Ask peer for its height
        w.write((json.dumps(height_request) + "\n").encode('utf-8'))
        await w.drain()
        line = await r.readline()
        if not line:
            raise ValueError("Empty response when querying height")

        msg = json.loads(line.decode('utf-8').strip())

        if msg.get("type") == "height_response":
            print(msg)
            peer_height = msg.get("height")
            #peer_tip = msg.get("current_tip")
            logger.info(f"*** Peer {peer_ip} responded with height {peer_height}")

        # Only push if our height is greater than peer's
        if int(peer_height) < local_height:
            print("***** WILL PUSH BLOCKS TO PEER ****")

            start_height = int(peer_height+1)
            end_height = local_height

            blocks_to_send = []

            for h in range(start_height, end_height+1):
                found_block = None

                print(f"Looking for block at height {h}")

                # Search for the block at height h
                for key in db.keys():
                    if key.startswith(b"block:"):
                        block = json.loads(db[key].decode())
                        if block.get("height") == h:
                            found_block = block
                            break

                if not found_block:
                    print(f"Block at height {h} not found!")
                    continue

                # Now fetch full transaction objects
                full_transactions = []

                for tx_id in found_block.get("tx_ids", []):
                    tx_key = f"tx:{tx_id}".encode()
                    if tx_key in db:
                        tx_obj = json.loads(db[tx_key].decode())
                        full_transactions.append({
                            "tx_id": tx_id,
                            "transaction": tx_obj
                        })

                cbase_key = f"tx:coinbase_{h}".encode()
                if cbase_key in db:
                    full_transactions.append(json.loads(db[cbase_key].decode()))

                found_block["full_transactions"] = full_transactions

                blocks_to_send.append(found_block)

            # Now send the blocks
            blocks_message = {
                "type": "blocks_response",
                "blocks": blocks_to_send,
                "timestamp": int(time.time() * 1000)
            }
            message_json = json.dumps(blocks_message)
            print(f"Sending message of {len(message_json)} bytes to {peer_ip}")
            w.write((message_json + "\n").encode('utf-8'))
            await w.drain()
            print(f"Sent {len(blocks_to_send)} blocks to {peer_ip}")
        
        elif peer_height > local_height:
            # We need blocks from peer
            print("***** WILL PULL BLOCKS FROM PEER *****")
            print(f"Decision logic: peer_height={peer_height}, local_height={local_height}")
            
            if local_height == -1:
                # We have no blocks, request from genesis
                start_height = 0
            else:
                start_height = local_height + 1
            end_height = peer_height
            
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
                raise ConnectionError("Peer closed the connection")
            
            try:
                header = json.loads(header_line.decode())
            except json.JSONDecodeError as e:
                raise ValueError(f"Bad JSON header from peer: {e}")
            
            if header.get("type") == "blocks_response_chunked":
                # Handle chunked response
                total_chunks = header.get("total_chunks", 0)
                blocks = []
                
                logger.info(f"Receiving chunked response with {total_chunks} chunks")
                
                for chunk_num in range(total_chunks):
                    chunk_line = await r.readline()
                    if not chunk_line:
                        raise ConnectionError(f"Connection closed while reading chunk {chunk_num}")
                    
                    try:
                        chunk_data = json.loads(chunk_line.decode())
                        if chunk_data.get("chunk_num") != chunk_num:
                            raise ValueError(f"Expected chunk {chunk_num}, got {chunk_data.get('chunk_num')}")
                        
                        blocks.extend(chunk_data.get("blocks", []))
                        logger.info(f"Received chunk {chunk_num + 1}/{total_chunks} with {len(chunk_data.get('blocks', []))} blocks")
                        
                    except json.JSONDecodeError as e:
                        raise ValueError(f"Bad JSON in chunk {chunk_num}: {e}")
                
                response = {"blocks": blocks}
            else:
                # Handle regular response (backward compatibility)
                response = header

            blocks = sorted(response.get("blocks", []), key=lambda x: x["height"])
            logger.info(f"Received {len(blocks)} blocks from from peer")
            process_blocks_from_peer(blocks)

        else:
            print("Peer up to date")
            print(f"Decision logic: peer_height={peer_height}, local_height={local_height}")
            logger.info(f"No sync needed: peer_height={peer_height}, local_height={local_height}")

        w.close()
        await w.wait_closed()
    except Exception as e:
        print(f"**** Could not connect to peer: {e}")




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
            validator_ids = json.loads(validators_json) if validators_json else []
            print("*** in discover peers periodically")
            logger.info(f"Discovered {len(validator_ids)} validators in DHT")
            
            for vid in validator_ids:
                if vid == VALIDATOR_ID:
                    continue
                gossip_key = f"gossip_{vid}"
                gossip_info_json = await kad_server.get(gossip_key)
                if gossip_info_json:
                    info = json.loads(gossip_info_json)
                    print("*** in discover peers periodically")
                    print(info.get("ip", info.get("external_ip", "unknown")))
                    print(current_ip)
                    
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
            # Always try to update heartbeat, not just when bootstrapped
            await kad_server.set(heartbeat_key, str(time.time()))
            logger.debug(f"Updated heartbeat for {VALIDATOR_ID}")
        except Exception as e:
            logger.warning(f"Failed to update heartbeat: {e}")
        await asyncio.sleep(HEARTBEAT_INTERVAL)

async def maintain_validator_list(gossip_node):
    while not shutdown_event.is_set():
        try:
            # Check if kad_server is initialized
            if not kad_server:
                logger.debug("DHT server not initialized, skipping validator list maintenance")
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                continue
            dht_list_json = await kad_server.get(VALIDATORS_LIST_KEY)
            dht_set = set(json.loads(dht_list_json)) if dht_list_json else set()
        except Exception as e:
            logger.error(f"Failed to fetch validator list from DHT: {e}")
            dht_set = set()

        current_time = time.time()
        alive = set()

        # Check heartbeats for all validators
        for v in dht_set:
            try:
                last_seen_raw = await kad_server.get(f"validator_{v}_heartbeat")
                last_seen_str = b2s(last_seen_raw)
                last_seen = float(last_seen_str) if last_seen_str else None
                if last_seen and (current_time - last_seen) <= VALIDATOR_TIMEOUT:
                    alive.add(v)
            except Exception as e:
                logger.warning(f"Failed to fetch heartbeat for {v}: {e}")

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
                        gossip_node.add_peer(ip, port, peer_info=info)
                        #await push_blocks(ip, port)
                        if public_key:
                            validator_keys[v] = public_key
                        logger.info(f"New validator joined: {v} at {ip}:{port}")
            except Exception as e:
                logger.warning(f"Failed to process new validator {v}: {e}")

        # Process validators that have left
        just_left = known_validators - alive
        for v in just_left:
            try:
                gossip_info_json = await kad_server.get(f"gossip_{v}")
                if gossip_info_json:
                    info = json.loads(gossip_info_json)
                    # Handle both old and new format
                    ip = info.get("ip", info.get("external_ip"))
                    port = info.get("port", info.get("external_port"))
                    if ip and port:
                        gossip_node.remove_peer(ip, port)
                validator_keys.pop(v, None)
                logger.info(f"Validator left: {v}")
            except Exception as e:
                logger.warning(f"Failed to remove validator {v}: {e}")

        # Update known validators
        known_validators.clear()
        known_validators.update(alive)

        await asyncio.sleep(HEARTBEAT_INTERVAL)