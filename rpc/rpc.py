import copy
import time
import struct
import json
import logging
import asyncio
import os
from decimal import Decimal
from fastapi import FastAPI, Request
# CORSMiddleware removed - nginx handles CORS at the proxy level
from database.database import get_db, get_current_height
from config.config import ADMIN_ADDRESS, CHAIN_ID
from wallet.wallet import verify_transaction
from blockchain.blockchain import derive_qsafe_address,Block, bits_to_target, serialize_transaction,scriptpubkey_to_address, read_varint, parse_tx, validate_pow, sha256d, calculate_merkle_root
from blockchain.difficulty import get_next_bits
from blockchain.block_factory import create_block, normalize_block
from state.state import blockchain, state_lock, mempool_manager
# from rocksdict import WriteBatch  # No longer needed - ChainManager handles all database operations
from sync.sync import get_blockchain_info

# Import security components
from models.validation import RPCRequest, BlockSubmissionRequest
from errors.exceptions import ValidationError
from middleware.error_handler import setup_error_handlers
from security.simple_middleware import simple_security_middleware

logger = logging.getLogger(__name__)

# Longpoll support for miners
longpoll_waiters = []  # List of (longpollid, future) tuples
longpoll_lock = None  # Will be created lazily

def _get_longpoll_lock():
    """Get or create the longpoll lock in the current event loop"""
    global longpoll_lock
    if longpoll_lock is None:
        longpoll_lock = asyncio.Lock()
    return longpoll_lock

async def notify_new_block():
    """Notify all waiting longpoll requests that a new block has arrived"""
    async with _get_longpoll_lock():
        logger.info(f"notify_new_block called - {len(longpoll_waiters)} miners waiting")
        notified = 0
        for longpollid, future in longpoll_waiters:
            if not future.done():
                logger.info(f"Notifying miner waiting for block {longpollid}")
                future.set_result(True)
                notified += 1
        logger.info(f"Notified {notified} miners")
        longpoll_waiters.clear()


rpc_app = FastAPI(title="qBTC RPC API", version="1.0.0")

# Setup security middleware
rpc_app.middleware("http")(simple_security_middleware)

# Setup error handlers
setup_error_handlers(rpc_app)

# NOTE: CORS is handled by nginx at the proxy level
# Do NOT add CORSMiddleware here as it will cause duplicate headers



@rpc_app.post("/")
async def rpc_handler(request: Request):
    """Handle RPC requests with validation"""
    # Check authorization header for cpuminer compatibility
    auth_header = request.headers.get("Authorization")
    if auth_header:
        # cpuminer sends basic auth, just accept any credentials for now
        logger.debug(f"Auth header present: {auth_header[:20]}...")
    
    try:
        data = await request.json()
    except json.JSONDecodeError:
        raise ValidationError("Invalid JSON in RPC request")
    
    # Validate RPC request structure
    try:
        rpc_request = RPCRequest(**data)
    except Exception as e:
        return {"error": f"Invalid RPC request: {str(e)}", "id": data.get("id")}
    
    method = rpc_request.method
    
    try:
        if method == "getblocktemplate":
            return await get_block_template(data)
        elif method == "submitblock":
            return await submit_block(request, data)
        elif method == "getblockchaininfo":
            return await get_blockchain_info_rpc(data)
        elif method == "getmininginfo":
            return await get_mining_info(data)
        elif method == "getnetworkinfo":
            return await get_network_info(data)
        elif method == "getpeerinfo":
            return await get_peer_info(request, data)
        elif method == "getwork":
            return await get_work(data)
        elif method == "createwallet":
            return await create_wallet_rpc(data)
        elif method == "listwallets":
            return await list_wallets_rpc(data)
        elif method == "getwalletinfo":
            return await get_wallet_info_rpc(data)
        elif method == "getbalance":
            return await get_balance_rpc(data)
        elif method == "listunspent":
            return await list_unspent_rpc(data)
        elif method == "walletpassphrase":
            return await wallet_passphrase_rpc(data)
        elif method == "walletlock":
            return await wallet_lock_rpc(data)
        elif method == "walletpassphrasechange":
            return await wallet_passphrase_change_rpc(data)
        elif method == "encryptwallet":
            return await encrypt_wallet_rpc(data)
        else:
            logger.warning(f"Unknown RPC method requested: {method}")
            return {"error": "unknown method", "id": data.get("id")}
    except Exception as e:
        logger.error(f"RPC method {method} failed: {str(e)}")
        return {"error": f"RPC method failed: {str(e)}", "id": data.get("id")}


async def get_blockchain_info_rpc(data):
    """Handle getblockchaininfo RPC call"""
    try:
        info = await get_blockchain_info()
        return {
            "result": info,
            "error": None,
            "id": data.get("id")
        }
    except Exception as e:
        logger.error(f"getblockchaininfo failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


async def get_mining_info(data):
    """Handle getmininginfo RPC call - required for cpuminer"""
    try:
        db = get_db()
        height, _ = await get_current_height(db)
        
        # Calculate current difficulty from bits
        from blockchain.difficulty import get_next_bits, compact_to_target
        
        # Get current bits for mining
        current_bits = get_next_bits(db, height if height is not None else -1)
        current_target = compact_to_target(current_bits)
        max_target = compact_to_target(0x1d00ffff)  # Bitcoin's max target
        current_difficulty = max_target / current_target
        
        # Estimate network hash rate (simplified)
        network_hashps = int(current_difficulty * 7000000)  # Rough estimate
        
        # Count pending transactions
        pooled_tx_count = mempool_manager.size()
        
        result = {
            "blocks": height if height is not None else 0,
            "difficulty": current_difficulty,
            "networkhashps": network_hashps,
            "pooledtx": pooled_tx_count,
            "chain": "main",  # qBTC main chain
            "warnings": ""
        }
        
        return {
            "result": result,
            "error": None,
            "id": data.get("id")
        }
    except Exception as e:
        logger.error(f"getmininginfo failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


async def get_network_info(data):
    """Handle getnetworkinfo RPC call"""
    try:
        result = {
            "version": 1000000,  # Protocol version
            "subversion": "/qBTC:1.0.0/",
            "protocolversion": 70015,  # Bitcoin protocol version
            "localservices": "0000000000000000",
            "localrelay": True,
            "timeoffset": 0,
            "networkactive": True,
            "connections": 0,  # Would need gossip_client info
            "networks": [{
                "name": "ipv4",
                "limited": False,
                "reachable": True,
                "proxy": "",
                "proxy_randomize_credentials": False
            }],
            "relayfee": 0.00001000,
            "incrementalfee": 0.00001000,
            "localaddresses": [],
            "warnings": ""
        }
        
        return {
            "result": result,
            "error": None,
            "id": data.get("id")
        }
    except Exception as e:
        logger.error(f"getnetworkinfo failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


async def get_peer_info(request, data):
    """Handle getpeerinfo RPC call"""
    try:
        # Get gossip client to check peer connections
        gossip_client = getattr(request.app.state, 'gossip_client', None)
        peers = []
        
        if gossip_client:
            # Get peer info from gossip client
            # This is simplified - would need to expose peer info from gossip_client
            for peer_addr, peer_info in getattr(gossip_client, 'peers', {}).items():
                peers.append({
                    "id": len(peers),
                    "addr": f"{peer_addr[0]}:{peer_addr[1]}",
                    "addrlocal": "127.0.0.1:8333",
                    "services": "0000000000000000",
                    "relaytxes": True,
                    "lastsend": int(time.time()),
                    "lastrecv": int(time.time()),
                    "bytessent": 0,
                    "bytesrecv": 0,
                    "conntime": int(time.time()) - 3600,  # Connected 1 hour ago
                    "timeoffset": 0,
                    "pingtime": 0.001,
                    "minping": 0.001,
                    "version": 70015,
                    "subver": "/qBTC:1.0.0/",
                    "inbound": False,
                    "addnode": False,
                    "startingheight": 0,
                    "banscore": 0,
                    "synced_headers": -1,
                    "synced_blocks": -1,
                    "inflight": [],
                    "whitelisted": False,
                    "permissions": [],
                    "minfeefilter": 0.00001000,
                    "bytessent_per_msg": {},
                    "bytesrecv_per_msg": {}
                })
        
        return {
            "result": peers,
            "error": None,
            "id": data.get("id")
        }
    except Exception as e:
        logger.error(f"getpeerinfo failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


async def get_work(data):
    """Handle getwork RPC call - legacy mining protocol"""
    try:
        logger.info("getwork called - legacy protocol")
        # For now, return an error indicating to use getblocktemplate
        return rpc_error(-1, "getwork is deprecated, please use getblocktemplate", data.get("id"))
    except Exception as e:
        logger.error(f"getwork failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


async def get_block_template(data):
    db = get_db()
    
    # Check if this is a longpoll request
    params = data.get("params", [{}])
    request_params = params[0] if params else {}
    longpollid = request_params.get("longpollid")
    
    logger.info(f"getblocktemplate called with params: {request_params}")
    logger.info(f"Longpoll ID from request: {longpollid}")
    
    # Get current blockchain state
    height, previous_block_hash = await get_current_height(db)
    
    # If longpoll is requested and the tip hasn't changed, wait for a new block
    if longpollid and longpollid == previous_block_hash:
        logger.info(f"Longpoll request received for block {longpollid}, waiting for new block...")
        
        # Create a future to wait on
        future = asyncio.Future()
        
        # Add to waiters list
        async with _get_longpoll_lock():
            longpoll_waiters.append((longpollid, future))
        
        try:
            # Wait for up to 60 seconds for a new block
            await asyncio.wait_for(future, timeout=60.0)
            logger.info("Longpoll triggered by new block")
            # Re-fetch the current state after new block
            height, previous_block_hash = await get_current_height(db)
        except asyncio.TimeoutError:
            logger.info("Longpoll timed out after 60 seconds")
            # Remove from waiters if still there
            async with _get_longpoll_lock():
                longpoll_waiters[:] = [(lid, f) for lid, f in longpoll_waiters 
                                      if f is not future]
        finally:
            # Ensure we're removed from waiters
            async with _get_longpoll_lock():
                longpoll_waiters[:] = [(lid, f) for lid, f in longpoll_waiters 
                                      if f is not future]
    elif longpollid and longpollid != previous_block_hash:
        # If longpollid is provided but doesn't match current tip, return immediately
        # This handles the case where a new block was just mined
        logger.info(f"Longpoll ID {longpollid} doesn't match current tip {previous_block_hash}, returning new template immediately")
    
    timestamp = int(time.time())
    logger.info(f"get_block_template: height={height}, previous_block_hash={previous_block_hash}")
    transactions = []
    txids = [] 
    seen_txids = set()  # Track which txids we've already added

    # Include pending transactions in the block template
    # Use mempool manager to get transactions sorted by fee and without conflicts
    
    # Get transactions for block, already sorted by fee rate and conflict-free
    # Uses MAX_TRANSACTIONS_PER_BLOCK from config (4000 by default)
    from config.config import MAX_TRANSACTIONS_PER_BLOCK
    mempool_txs = mempool_manager.get_transactions_for_block(max_count=MAX_TRANSACTIONS_PER_BLOCK)
    
    # Also need to check against confirmed UTXOs in database
    for orig_tx in mempool_txs:
        tx = copy.deepcopy(orig_tx)
        stored_txid = tx.get("txid")  # Get the txid if it exists
        
        # Check if inputs are still unspent in the database
        valid_tx = True
        for input_ in tx.get("inputs", []):
            utxo_key = f"utxo:{input_['txid']}:{input_.get('utxo_index', 0)}".encode()
            if utxo_key in db:
                utxo_data = json.loads(db[utxo_key].decode())
                if utxo_data.get("spent", False):
                    logger.info(f"Skipping transaction {stored_txid} - input {utxo_key.decode()} already spent in DB")
                    valid_tx = False
                    break
            else:
                logger.warning(f"Skipping transaction {stored_txid} - input {utxo_key.decode()} not found in DB")
                valid_tx = False
                break
        
        if not valid_tx:
            continue
        
        # Remove txid from transaction and outputs before serialization
        if "txid" in tx:
            del tx["txid"]
        for output in tx.get("outputs", []):
            output.pop("txid", None)
        
        # Always recalculate to ensure consistency
        raw_tx = serialize_transaction(tx)
        calculated_txid = sha256d(bytes.fromhex(raw_tx))[::-1].hex()
        
        # Verify the txid matches
        if stored_txid and stored_txid != calculated_txid:
            logger.warning(f"TXID mismatch! Stored: {stored_txid}, Calculated: {calculated_txid}")
            logger.warning(f"Using calculated TXID")
        
        txid = calculated_txid
        
        # Only add if we haven't seen this txid before
        if txid not in seen_txids:
            transactions.append({
                "data": raw_tx,  
                "txid": txid
            })
            txids.append(txid)
            seen_txids.add(txid)
            logger.debug(f"Added transaction {txid} to block template")
        else:
            logger.warning(f"Skipping duplicate transaction {txid} in block template") 


    # Handle case where we're at genesis
    if height is None or previous_block_hash is None:
        logger.error("Cannot create block template: no valid chain tip found")
        return {
            "result": None,
            "error": {"code": -1, "message": "No valid chain tip found"},
            "id": data["id"]
        }
    
    # Calculate the appropriate difficulty for the next block
    next_bits = get_next_bits(db, height)
    next_target = bits_to_target(next_bits)
    
    block_template = {
        "version": 1,
        "previousblockhash": f"{previous_block_hash}",
        "target": f"{next_target:064x}",
        "bits": f"{next_bits:08x}", 
        "curtime": timestamp,
        "height": height + 1,
        "mutable": ["time", "transactions", "prevblock"],
        "noncerange": "00000000ffffffff",
        "capabilities": ["proposal"],
        "coinbaseaux": {},
        "coinbasevalue": 50,
        "transactions": transactions,
        "longpollid": previous_block_hash,
    }

    return {
        "result": block_template,
        "error": None,
        "id": data["id"]
    }


def rpc_error(code, msg, _id):
    return {"result": None,
            "error": {"code": code, "message": msg},
            "id": _id}


async def submit_block(request: Request, data: dict) -> dict:
    """Submit a new block with comprehensive validation"""
    logger.info(f"Block submission request: {data}")
    
    try:
        # Get gossip node - try multiple sources
        gossip_client = None
        
        # Try web module first
        try:
            from web.web import get_gossip_node
            gossip_client = get_gossip_node()
        except ImportError:
            pass
        
        # If not found, try sys.modules
        if not gossip_client:
            import sys
            gossip_client = getattr(sys.modules.get('__main__', None), 'gossip_node', None)
        
        # If still not found, try app.state as fallback
        if not gossip_client:
            gossip_client = getattr(request.app.state, 'gossip_client', None)
            
        logger.info(f"Retrieved gossip_client: {gossip_client}")
        
        # Validate block submission parameters
        if "params" not in data or not isinstance(data["params"], list) or len(data["params"]) == 0:
            return rpc_error(-1, "Missing or invalid block data", data.get("id"))
        
        raw_block_hex = data["params"][0]
        
        # Validate hex format
        try:
            block_request = BlockSubmissionRequest(block_hex=raw_block_hex)
            raw = bytes.fromhex(raw_block_hex)
        except Exception as e:
            return rpc_error(-1, f"Invalid block format: {str(e)}", data.get("id"))
        
        db = get_db()
        # No batch needed - ChainManager handles all database operations
        txids = []
        tx_list = []
        hdr = raw[:80]
        version = struct.unpack_from('<I', hdr, 0)[0]
        prev_block = hdr[4:36][::-1].hex()
        merkle_root_block = hdr[36:68][::-1].hex()
        timestamp = struct.unpack_from('<I', hdr, 68)[0]
        bits = struct.unpack_from('<I', hdr, 72)[0] 
        nonce = struct.unpack_from('<I', hdr, 76)[0]
        block = Block(version, prev_block, merkle_root_block, timestamp, bits, nonce)

        if block.hash() in blockchain:
            return rpc_error(-2,"duplicate", data["id"])

        if not validate_pow(block):
            logger.warning(f"Block validation failed - invalid PoW: {block.hash()}")
            return rpc_error(-1, "Block validation failed - invalid proof of work", data.get("id"))
        else:
            logger.info(f"Block PoW validation successful: {block.hash()}")

        height_temp = await get_current_height(db)
        local_height = height_temp[0]
        local_tip = height_temp[1]

        logger.info(f"Block submission check - Local height: {local_height}, Local tip: {local_tip}, Previous block in submission: {prev_block}")
        
        # Special check for genesis block submission
        if local_height == -1 and prev_block == "0" * 64:
            logger.info("Detected genesis block submission (first block in chain)")
            local_height = -1  # Genesis will be at height 0
            local_tip = "0" * 64  # Genesis has no previous block

        if prev_block != local_tip:
            if db.get(f"block:{prev_block}".encode()):      # we do know that block
                logger.warning(f"Stale block submitted: {block.hash()}")
                return rpc_error(-5, "stale-prevblk", data["id"])   # Use Bitcoin Core's error code
            logger.error(f"Block references unknown previous block: {prev_block}")
            return rpc_error(-1, "bad-prevblk", data["id"])

        future_limit = int(time.time()) + 2*60 # 2 mins in the future

        if (timestamp > future_limit):
            logger.warning(f"Block timestamp too far in future: {timestamp}")
            return rpc_error(-1, "Block timestamp too far in future", data["id"])


        offset = 80
        tx_count, sz = read_varint(raw, offset)
        offset += sz
        logger.info(f"Block transaction count from header: {tx_count}")
        coinbase_start = offset
        coinbase_tx, size = parse_tx(raw, offset)
        coinbase_script_pubkey = coinbase_tx["outputs"][0]["script_pubkey"]
        # For cpuminer compatibility, extract standard Bitcoin address from coinbase
        bitcoin_miner_address = None
        try:
            bitcoin_miner_address = scriptpubkey_to_address(coinbase_script_pubkey)
            logger.info(f"Coinbase miner address (Bitcoin format): {bitcoin_miner_address}")
            
            # Check if this Bitcoin address has been committed to a qBTC address
            db = get_db()
            commitment_key = f"commitment:{bitcoin_miner_address}".encode()
            commitment_data = db.get(commitment_key)
            
            if commitment_data:
                # Use the committed qBTC address
                commitment = json.loads(commitment_data.decode())
                coinbase_miner_address = commitment.get("qbtc_address")
                logger.info(f"Found commitment: Bitcoin {bitcoin_miner_address} -> qBTC {coinbase_miner_address}")
            else:
                # No commitment found, store the Bitcoin address directly
                # This allows tracking miners even without qBTC address commitment
                coinbase_miner_address = bitcoin_miner_address
                logger.info(f"No qBTC address commitment found for {bitcoin_miner_address}, storing Bitcoin address as miner")
                logger.info("Note: To spend rewards, commit your Bitcoin address to a qBTC address using the /commit endpoint")
        except Exception as e:
            # If address extraction fails entirely, use admin address
            coinbase_miner_address = ADMIN_ADDRESS
            logger.warning(f"Could not extract miner address from coinbase: {e}, using admin: {coinbase_miner_address}")
        coinbase_raw = raw[coinbase_start:coinbase_start + size]
        coinbase_txid = sha256d(coinbase_raw)[::-1].hex() 
        logger.info(f"Coinbase TXID: {coinbase_txid}")
        logger.info(f"Coinbase miner address: {coinbase_miner_address}")
        txids.append(coinbase_txid)

        #batch.put(b"tx:" + coinbase_txid.encode(), json.dumps(coinbase_tx).encode())

        #
        # Add mapping to quantum safe miner address here through endpoint 
        #

        offset += size
        
        # Check if there's any data after the coinbase transaction
        if offset >= len(raw):
            logger.info("Block contains only coinbase transaction")
        else:
            # Try to parse additional transactions
            # cpuminer might send them as either JSON or binary format
            remaining_data = raw[offset:]
            
            # First, try to parse as binary transactions (Bitcoin format)
            binary_offset = 0
            while binary_offset < len(remaining_data):
                try:
                    # Try to parse a binary transaction
                    tx_dict, tx_size = parse_tx(remaining_data[binary_offset:])
                    if tx_dict:
                        # Calculate TXID for this transaction
                        tx_bytes = remaining_data[binary_offset:binary_offset + tx_size]
                        temp_txid = sha256d(tx_bytes)[::-1].hex()
                        logger.info(f"Parsed binary transaction with TXID: {temp_txid}")
                        
                        # Convert to our internal format if needed
                        # For now, just track the TXID
                        txids.append(temp_txid)
                        binary_offset += tx_size
                    else:
                        break
                except Exception as e:
                    logger.debug(f"Not a binary transaction at offset {binary_offset}: {e}")
                    break
            
            # If no binary transactions found, try JSON format
            if binary_offset == 0:
                try:
                    blob = remaining_data.decode('utf-8')
                    logger.info(f"JSON blob length: {len(blob)} bytes")
                    logger.debug(f"First 200 chars of blob: {blob[:200]}...")
                    
                    decoder = json.JSONDecoder()
                    pos     = 0
                    json_obj_count = 0
                    while pos < len(blob):
                        try:
                            obj, next_pos = decoder.raw_decode(blob, pos)
                            
                            # Check if this looks like a transaction object
                            if isinstance(obj, dict):
                                # Add the transaction - we'll calculate TXID later when processing
                                tx_list.append(obj)
                                json_obj_count += 1
                                
                                # Try to extract/log the TXID if possible
                                if "txid" in obj:
                                    logger.info(f"Parsed JSON transaction with existing TXID: {obj['txid']}")
                                elif "body" in obj:
                                    logger.info(f"Parsed JSON transaction with body (TXID will be calculated later)")
                                else:
                                    logger.info(f"Parsed JSON object {json_obj_count}")
                                
                                logger.info(f"Added JSON object {json_obj_count} to tx_list")
                            
                            pos = next_pos
                            # Skip whitespace and commas
                            while pos < len(blob) and blob[pos] in ' \t\r\n,':
                                pos += 1
                        except json.JSONDecodeError:
                            # No more valid JSON objects, exit the loop
                            break
                        except Exception as e:
                            logger.warning(f"Error parsing JSON transaction at position {pos}: {e}")
                            break
                    
                    if json_obj_count > 0:
                        logger.info(f"Parsed {json_obj_count} JSON transactions")
                except Exception as e:
                    # No valid JSON data after coinbase, which is fine for cpuminer blocks
                    logger.debug(f"No JSON transactions after coinbase: {e}")
        
        logger.info(f"Total transactions parsed from JSON: {len(tx_list)}")
        logger.info(f"Transaction count from header: {tx_count}")
        logger.info(f"Expected non-coinbase txs: {tx_count - 1}")
        
        # If we have more transactions than expected, cpuminer might have duplicated them
        if len(tx_list) > (tx_count - 1):
            logger.warning(f"Found {len(tx_list)} JSON transactions but header says {tx_count - 1}")
            logger.warning("cpuminer may have duplicated transaction data")


        # Track UTXOs spent in this block to prevent double-spending within the same block
        spent_in_this_block = set()
        unique_txids_processed = set()  # Track which txids we've already processed
        
        for i, tx in enumerate(tx_list):
            # ALWAYS calculate txid - don't preserve existing ones
            # This ensures consistency with cpuminer's expectations
            raw_tx = serialize_transaction(tx)
            # sha256d returns hash in internal byte order
            # Reverse bytes for standard Bitcoin txid format
            txid = sha256d(bytes.fromhex(raw_tx))[::-1].hex()
            logger.debug(f"Calculated TXID for transaction {i}: {txid}")
            # Add txid to the transaction object itself
            tx["txid"] = txid
            
            # Only add unique txids to our list
            if txid not in unique_txids_processed:
                txids.append(txid)
                unique_txids_processed.add(txid)
            else:
                logger.info(f"Skipping duplicate transaction {txid} in block data")
                continue  # Skip processing duplicate transactions
            inputs = tx["inputs"]
            outputs = tx["outputs"]
            message_str = tx["body"]["msg_str"]
            pubkey = tx["body"]["pubkey"]
            signature = tx["body"]["signature"]
            if verify_transaction(message_str, signature, pubkey) is True:

                from_ = message_str.split(":")[0]
                to_ = message_str.split(":")[1]
                amount_ = message_str.split(":")[2]
                total_available = Decimal(0)
                total_required = Decimal(0)
                total_authorised = Decimal(amount_)

                if derive_qsafe_address(pubkey) != from_:
                    raise ValueError("Transaction signature validation failed: wrong signer")

                for input_ in inputs:
                    input_receiver = input_["receiver"]
                    input_amount = input_["amount"]
                    input_spent = input_["spent"]
                    if (input_receiver == from_):
                        if (input_spent == False):
                            total_available += Decimal(input_amount)
                for output_ in outputs:
                    output_receiver = output_["receiver"]
                    output_amount = output_["amount"]
                    if output_receiver in (from_, to_, ADMIN_ADDRESS):
                        total_required += Decimal(output_amount)
                    else:
                        logger.warning(f"Invalid output receiver in transaction: {output_receiver}")
                        return rpc_error(-1, f"Invalid output receiver: {output_receiver}", data["id"])
                miner_fee = (Decimal(total_authorised) * Decimal("0.001")).quantize(Decimal("0.00000001"))
                total_required = Decimal(total_authorised) + Decimal(miner_fee)
                if (total_required <= total_available):
                    # Validation passed - just check for double-spends within this block
                    for input_ in inputs:
                        utxo_key = f"utxo:{input_['txid']}:{input_.get('utxo_index', 0)}".encode()
                        utxo_key_str = utxo_key.decode()
                        
                        # Check if this UTXO was already spent in this block
                        if utxo_key_str in spent_in_this_block:
                            logger.error(f"Double-spend within block: {utxo_key}")
                            return rpc_error(-1, f"Double-spend detected within block: {utxo_key.decode()}", data["id"])
                            
                        # Check if UTXO exists and isn't already spent
                        if utxo_key in db:
                            utxo_raw = db.get(utxo_key)
                            if utxo_raw is None:
                                logger.error(f"UTXO not found: {utxo_key}")
                                return rpc_error(-1, f"Input not found: {utxo_key.decode()}", data["id"])
                            utxo = json.loads(utxo_raw.decode())
                            if utxo["spent"]:
                                logger.error(f"Double-spend attempt: {utxo_key}")
                                return rpc_error(-1, f"Double-spend detected: {utxo_key.decode()}", data["id"])
                            
                            # Track that this UTXO is being spent in this block
                            spent_in_this_block.add(utxo_key_str)
                        else:
                            logger.error(f"UTXO not found in database: {utxo_key}")
                            return rpc_error(-1, f"Input not found: {utxo_key.decode()}", data["id"])
                    
                    # ChainManager will handle all UTXO creation and wallet index updates
                    # Transaction will be stored by ChainManager when it processes the block
                    pass  # No need to store transaction here


        # Debug: Log all transaction IDs
        logger.info(f"Block contains {len(txids)} unique transactions")
        for i, txid in enumerate(txids):
            logger.info(f"  TX {i}: {txid}")
        
        # Handle cpuminer duplicate transactions for merkle calculation
        # If header says more transactions than we have unique ones, cpuminer duplicated them
        merkle_txids = txids.copy()
        if tx_count > len(txids):
            logger.warning(f"Header says {tx_count} txs but only {len(txids)} unique - cpuminer duplicated transactions")
            # For single transaction (coinbase only), Bitcoin duplicates it for merkle calculation
            # This is standard Bitcoin behavior when there's only one transaction in a block
            while len(merkle_txids) < tx_count:
                merkle_txids.append(txids[-1])  # Duplicate the last (or only) transaction
            logger.info(f"Extended txid list to {len(merkle_txids)} for merkle calculation")
        
        calculated_merkle = calculate_merkle_root(merkle_txids)
        logger.info(f"Calculated merkle root with {len(merkle_txids)} txids: {calculated_merkle}")
        logger.info(f"Block merkle root: {merkle_root_block}")
        
        if calculated_merkle != merkle_root_block:
            logger.error(f"Merkle root mismatch: calculated={calculated_merkle}, block={merkle_root_block}")
            logger.error(f"Transaction count: {tx_count}, Unique TXIDs: {len(txids)}, Merkle TXIDs: {len(merkle_txids)}")
            return rpc_error(-1, "Merkle root mismatch", data["id"])

        logger.info("Block merkle root validation successful")
        
        # Store transactions first before adding block
        full_transactions = []
        
        # Store coinbase transaction with proper format and txid
        # Fix coinbase inputs to use 'txid' instead of 'prev_txid' for validator
        coinbase_inputs = []
        for inp in coinbase_tx["inputs"]:
            coinbase_inputs.append({
                "txid": inp.get("prev_txid", "0" * 64),  # Coinbase should have all zeros
                "utxo_index": inp.get("prev_index", 0),
                "script_sig": inp.get("script_sig", ""),
                "sequence": inp.get("sequence", 0xffffffff)
            })
        
        # First create coinbase_tx_data without txid
        coinbase_tx_data = {
            "version": coinbase_tx["version"],
            "inputs": coinbase_inputs,
            "outputs": [{
                "utxo_index": idx,
                "receiver": coinbase_miner_address,
                "amount": str(out.get("value", out.get("amount", "0"))),  # Use value from parsed tx, fallback to amount
                "script_pubkey": out.get("script_pubkey", "")
            } for idx, out in enumerate(coinbase_tx["outputs"])],
            "body": {
                "msg_str": f"coinbase:{coinbase_miner_address}:0:0:{CHAIN_ID}",  # Add proper msg_str for coinbase
                "pubkey": "",       # Coinbase has no pubkey
                "signature": ""     # Coinbase has no signature
            },
            "locktime": coinbase_tx.get("locktime", 0)
        }
        
        # Calculate txid using the same method as chain_manager (serialize_transaction)
        # serialize_transaction is already imported at the top of the file
        coinbase_tx_hex = serialize_transaction(coinbase_tx_data)
        coinbase_tx_bytes = bytes.fromhex(coinbase_tx_hex)
        calculated_coinbase_txid = sha256d(coinbase_tx_bytes)[::-1].hex()
        
        # Use the original coinbase txid from the miner (calculated from raw Bitcoin bytes)
        # This ensures merkle root validation works correctly
        coinbase_tx_data["txid"] = coinbase_txid  # Use the original txid from line 522
        
        logger.info(f"Coinbase TXID (from raw bytes): {coinbase_txid}")
        
        # The merkle root should be validated using the original txids
        # This is critical for security - we MUST validate the merkle root
        
        # ChainManager will store the coinbase transaction when it processes the block
        # batch.put(f"tx:{calculated_coinbase_txid}".encode(), json.dumps(coinbase_tx_data).encode())
        full_transactions.append(coinbase_tx_data)
        
        # Add all other transactions to full_transactions
        for tx in tx_list:
            if tx:  # Only add non-None transactions
                # Calculate and add txid to transaction
                raw_tx = serialize_transaction(tx)
                calculated_txid = sha256d(bytes.fromhex(raw_tx))[::-1].hex()
                tx["txid"] = calculated_txid
                logger.info(f"[SUBMIT_BLOCK] Added txid {calculated_txid} to transaction")
                full_transactions.append(tx)
        
        # Use ChainManager singleton to add the block
        from blockchain.chain_singleton import get_chain_manager
        cm = await get_chain_manager()
        
        # Use the height we already got earlier
        new_height = local_height + 1
        logger.info(f"Current height: {local_height}, new block height will be: {new_height}")
        
        # CRITICAL: For the block hash calculation, we must use the miner's merkle root
        # The proof of work was done with that specific merkle root
        # But we'll validate that it matches our calculated one
        block_data = create_block(
            version=version,
            height=new_height,
            block_hash=block.hash(),
            previous_hash=prev_block,
            bits=bits,
            nonce=nonce,
            timestamp=timestamp,
            merkle_root=merkle_root_block,  # Use miner's merkle root for hash calculation
            tx_ids=txids,
            full_transactions=full_transactions,
            miner_address=coinbase_miner_address,
            connected=False  # New blocks always start as not connected
        )
        
        # Add the calculated merkle for validation
        block_data["calculated_merkle_root"] = calculated_merkle
        
        # CRITICAL FIX: Pass everything to ChainManager for atomic validation
        # This prevents database corruption from invalid blocks
        
        # Don't add anything to batch - let ChainManager handle everything
        # ChainManager will handle all UTXO operations, transaction storage, and block storage atomically
        
        # Since batch is now empty, we can pass None instead
        success, error_msg = await cm.add_block(block_data, pre_validated_batch=None)
        if not success:
            # Batch was never written, so no cleanup needed!
            logger.error(f"ChainManager rejected block: {error_msg}")
            return rpc_error(-1, f"Block rejected: {error_msg}", data["id"])
        
        # No need to invalidate cache - we removed caching

        # Mempool cleanup is already handled by ChainManager._connect_block()
        # No need to duplicate the cleanup here
        logger.info(f"[MEMPOOL] Block added - mempool cleanup handled by ChainManager")
        logger.info(f"[MEMPOOL] Current mempool size: {mempool_manager.size()}")

        async with state_lock:
            blockchain.append(block.hash())
        logger.info(f"Block successfully added: {block.hash()} height={block_data['height']} txs={len(tx_list)}")
        logger.info("About to notify waiting miners...")
        
        # Notify waiting miners of new block
        await notify_new_block()
        
        logger.info("Miners notified of new block")
        
        logger.info(f"About to broadcast block to peers...")

        # Full transactions are already in block_data from above
        # No need to fetch them again from DB


        
        block_gossip = {
                "type": "blocks_response",
                "blocks": [block_data],  # blocks should be a list
                "timestamp": int(time.time() * 1000),
                "is_new_block": True  # Flag to indicate this is a new block announcement, not a sync response
        }

        logger.info(f"Broadcasting block {block.hash()} at height {block_data['height']}")
        logger.info(f"gossip_client = {gossip_client}")
        logger.info(f"gossip_client type = {type(gossip_client)}")
        
        if not gossip_client:
            logger.error("gossip_client is None! Cannot broadcast block")
        else:
            try:
                logger.info("Calling randomized_broadcast...")
                await gossip_client.randomized_broadcast(block_gossip)
                logger.info("Block broadcast completed successfully")
            except Exception as e:
                logger.error(f"Failed to broadcast block: {e}", exc_info=True)

        return {"result": None, "error": None, "id": data["id"]}
    
    except Exception as e:
        import traceback
        logger.error(f"Unexpected error in submit_block: {str(e)}")
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        return rpc_error(-1, f"Internal error: {str(e)}", data.get("id"))


async def create_wallet_rpc(data):
    """Handle createwallet RPC call - creates a new qBTC wallet

    Simplified format for qBTC:
    bitcoin-cli createwallet "wallet_name" "password"

    Parameters:
    - wallet_name: Required wallet name/identifier
    - password: Required password for wallet encryption
    """
    try:
        params = data.get("params", [])

        # Both wallet_name and password are required
        if len(params) < 2:
            return rpc_error(-8, "createwallet requires wallet_name and password parameters", data.get("id"))

        wallet_name = params[0]
        password = params[1]

        # Strip quotes from bitcoin-cli string parameters
        if isinstance(wallet_name, str):
            wallet_name = wallet_name.strip('"')
        if isinstance(password, str):
            password = password.strip('"')

        # Validate password
        if not password or len(password) < 8:
            return rpc_error(-8, "Password must be at least 8 characters", data.get("id"))

        # Validate wallet name
        if not wallet_name:
            return rpc_error(-8, "Invalid wallet name", data.get("id"))

        # Check if wallet already exists
        wallet_dir = os.path.join("wallets", wallet_name)
        if os.path.exists(wallet_dir):
            return rpc_error(-4, f"Wallet {wallet_name} already exists", data.get("id"))

        # Create wallet directory
        os.makedirs(wallet_dir, exist_ok=True)

        # Generate ML-DSA-87 quantum-resistant keypair
        import oqs
        from wallet.wallet import _as_bytes, _hex, _derive_address, _encrypt_privkey

        try:
            with oqs.Signature("ML-DSA-87") as signer:
                public_key = _as_bytes(signer.generate_keypair())
                secret_key = _as_bytes(signer.export_secret_key())

            # Derive address
            address = _derive_address(public_key)

            # Always encrypt the private key with the provided password
            enc_priv, salt, iv = _encrypt_privkey(_hex(secret_key), password)

            # Store wallet data with encrypted key
            wallet_data = {
                "name": wallet_name,
                "version": 1,
                "created": int(time.time()),
                "encrypted": True,  # Always encrypted with password
                "locked": True,  # Start locked
                "keys": [{
                    "address": address,
                    "publicKey": _hex(public_key),
                    "encryptedPrivateKey": enc_priv,
                    "salt": salt,
                    "iv": iv
                }]
            }

        except Exception as e:
            # Clean up on key generation failure
            import shutil
            if os.path.exists(wallet_dir):
                shutil.rmtree(wallet_dir)
            logger.error(f"Failed to generate keys: {str(e)}")
            return rpc_error(-1, f"Failed to generate keys: {str(e)}", data.get("id"))

        # Save wallet file
        wallet_file = os.path.join(wallet_dir, "wallet.json")
        with open(wallet_file, "w") as f:
            json.dump(wallet_data, f, indent=2)

        # Store in database for quick access
        db = get_db()
        wallet_key = f"wallet:{wallet_name}".encode()
        wallet_meta = {
            "name": wallet_name,
            "address": address,
            "created": wallet_data["created"],
            "encrypted": wallet_data["encrypted"]
        }
        from rocksdict import WriteBatch
        batch = WriteBatch()
        batch.put(wallet_key, json.dumps(wallet_meta).encode())
        db.write(batch)

        logger.info(f"Created encrypted wallet '{wallet_name}' with address {address}")

        # Return success with wallet info including crypto details
        result = {
            "name": wallet_name,
            "warning": "",
            "address": address,
            "publicKey": wallet_data["keys"][0]["publicKey"],
            "encryptedPrivateKey": wallet_data["keys"][0]["encryptedPrivateKey"],
            "salt": wallet_data["keys"][0]["salt"],
            "iv": wallet_data["keys"][0]["iv"]
        }

        return {
            "result": result,
            "error": None,
            "id": data.get("id")
        }

    except Exception as e:
        logger.error(f"createwallet failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


async def list_wallets_rpc(data):
    """Handle listwallets RPC call - lists all available wallets"""
    try:
        wallets = []

        # Check wallets directory
        wallets_dir = "wallets"
        if os.path.exists(wallets_dir):
            for wallet_name in os.listdir(wallets_dir):
                wallet_path = os.path.join(wallets_dir, wallet_name)
                if os.path.isdir(wallet_path):
                    wallet_file = os.path.join(wallet_path, "wallet.json")
                    if os.path.exists(wallet_file):
                        wallets.append(wallet_name)

        return {
            "result": wallets,
            "error": None,
            "id": data.get("id")
        }
    except Exception as e:
        logger.error(f"listwallets failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


async def get_wallet_info_rpc(data):
    """Handle getwalletinfo RPC call - returns information about a specific wallet"""
    try:
        params = data.get("params", [])
        wallet_name = params[0] if len(params) > 0 else "default"

        # Strip quotes from bitcoin-cli parameters
        if isinstance(wallet_name, str):
            wallet_name = wallet_name.strip('"')

        # Check if wallet exists
        wallet_dir = os.path.join("wallets", wallet_name)
        wallet_file = os.path.join(wallet_dir, "wallet.json")

        if not os.path.exists(wallet_file):
            return rpc_error(-18, f"Wallet {wallet_name} not found", data.get("id"))

        # Load wallet data
        with open(wallet_file, "r") as f:
            wallet_data = json.load(f)

        # Get balance for all addresses in wallet
        db = get_db()
        total_balance = Decimal("0")
        addresses = []

        for key_data in wallet_data.get("keys", []):
            address = key_data.get("address")
            if address:
                addresses.append(address)

                # Get balance from wallet index
                from blockchain.wallet_index import get_wallet_index
                wallet_index = get_wallet_index()
                balance = wallet_index.get_wallet_balance(address)
                total_balance += balance

        # Return wallet info
        return {
            "result": {
                "walletname": wallet_name,
                "walletversion": wallet_data.get("version", 1),
                "format": "qbtc",
                "balance": float(total_balance),
                "unconfirmed_balance": 0.0,
                "immature_balance": 0.0,
                "txcount": 0,  # Could be calculated from wallet_index
                "keypoololdest": wallet_data.get("created", 0),
                "keypoolsize": len(wallet_data.get("keys", [])),
                "keypoolsize_hd_internal": 0,
                "paytxfee": 0.0,
                "private_keys_enabled": not wallet_data.get("blank", False),
                "avoid_reuse": False,
                "scanning": False,
                "descriptors": False,
                "external_signer": False,
                "blank": wallet_data.get("blank", False),
                "addresses": addresses
            },
            "error": None,
            "id": data.get("id")
        }
    except Exception as e:
        logger.error(f"getwalletinfo failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


async def get_balance_rpc(data):
    """Handle getbalance RPC call - returns balance for a wallet or address

    Bitcoin-cli compatible:
    bitcoin-cli getbalance [account] [minconf] [include_watchonly] [avoid_reuse]

    For qBTC we support:
    - No params: Returns total balance of all wallets
    - wallet_name: Returns balance for specific wallet
    - address: Returns balance for specific address
    """
    try:
        params = data.get("params", [])
        target = params[0] if len(params) > 0 else "*"  # "*" means all wallets
        minconf = params[1] if len(params) > 1 else 1  # Minimum confirmations

        # Strip quotes from bitcoin-cli parameters
        if isinstance(target, str):
            target = target.strip('"')

        from blockchain.wallet_index import get_wallet_index
        wallet_index = get_wallet_index()
        db = get_db()

        total_balance = Decimal("0")

        if target == "*":
            # Get balance for all wallets
            wallets_dir = "wallets"
            if os.path.exists(wallets_dir):
                for wallet_name in os.listdir(wallets_dir):
                    wallet_path = os.path.join(wallets_dir, wallet_name)
                    if os.path.isdir(wallet_path):
                        wallet_file = os.path.join(wallet_path, "wallet.json")
                        if os.path.exists(wallet_file):
                            with open(wallet_file, "r") as f:
                                wallet_data = json.load(f)
                            for key_data in wallet_data.get("keys", []):
                                address = key_data.get("address")
                                if address:
                                    balance = wallet_index.get_wallet_balance(address)
                                    total_balance += balance

        elif target.startswith("bqs"):
            # It's an address
            balance = wallet_index.get_wallet_balance(target)
            total_balance = balance

        else:
            # It's a wallet name
            wallet_dir = os.path.join("wallets", target)
            wallet_file = os.path.join(wallet_dir, "wallet.json")

            if not os.path.exists(wallet_file):
                return rpc_error(-18, f"Wallet {target} not found", data.get("id"))

            with open(wallet_file, "r") as f:
                wallet_data = json.load(f)

            for key_data in wallet_data.get("keys", []):
                address = key_data.get("address")
                if address:
                    balance = wallet_index.get_wallet_balance(address)
                    total_balance += balance

        # Return as float for Bitcoin compatibility
        return {
            "result": float(total_balance),
            "error": None,
            "id": data.get("id")
        }

    except Exception as e:
        logger.error(f"getbalance failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


async def list_unspent_rpc(data):
    """Handle listunspent RPC call - returns unspent transaction outputs

    Bitcoin-cli compatible:
    bitcoin-cli listunspent [minconf] [maxconf] [addresses] [include_unsafe] [query_options]

    Returns array of unspent transaction outputs
    """
    try:
        params = data.get("params", [])
        minconf = params[0] if len(params) > 0 else 1
        maxconf = params[1] if len(params) > 1 else 9999999
        addresses = params[2] if len(params) > 2 else []

        db = get_db()
        from blockchain.wallet_index import get_wallet_index
        wallet_index = get_wallet_index()

        unspent_outputs = []

        # If no addresses specified, get all from wallets
        if not addresses:
            wallets_dir = "wallets"
            if os.path.exists(wallets_dir):
                for wallet_name in os.listdir(wallets_dir):
                    wallet_path = os.path.join(wallets_dir, wallet_name)
                    if os.path.isdir(wallet_path):
                        wallet_file = os.path.join(wallet_path, "wallet.json")
                        if os.path.exists(wallet_file):
                            with open(wallet_file, "r") as f:
                                wallet_data = json.load(f)
                            for key_data in wallet_data.get("keys", []):
                                address = key_data.get("address")
                                if address:
                                    addresses.append(address)

        # Get current blockchain height for confirmations
        current_height, _ = await get_current_height(db)

        # Collect UTXOs for each address
        for address in addresses:
            utxos = wallet_index.get_wallet_utxos(address)

            for utxo in utxos:
                # Calculate confirmations
                utxo_key = f"utxo:{utxo.get('txid', '')}:{utxo.get('index', 0)}"

                # Get the block height of the transaction that created this UTXO
                tx_key = f"tx:{utxo.get('txid', '')}".encode()
                tx_data = db.get(tx_key)

                confirmations = 1  # Default to 1 if we can't find block info
                if tx_data:
                    tx = json.loads(tx_data.decode())
                    block_height = tx.get("height", current_height)
                    confirmations = current_height - block_height + 1

                # Check confirmation requirements
                if confirmations < minconf or confirmations > maxconf:
                    continue

                # Format UTXO for Bitcoin compatibility
                unspent_output = {
                    "txid": utxo.get("txid", ""),
                    "vout": utxo.get("index", 0),
                    "address": address,
                    "account": "",  # Deprecated in Bitcoin, kept for compatibility
                    "scriptPubKey": utxo.get("script_pubkey", ""),
                    "amount": float(Decimal(str(utxo.get("amount", "0")))),
                    "confirmations": confirmations,
                    "spendable": True,
                    "solvable": True,
                    "desc": f"addr({address})",
                    "safe": confirmations >= 6
                }

                unspent_outputs.append(unspent_output)

        # Sort by confirmations (most confirmed first)
        unspent_outputs.sort(key=lambda x: x["confirmations"], reverse=True)

        return {
            "result": unspent_outputs,
            "error": None,
            "id": data.get("id")
        }

    except Exception as e:
        logger.error(f"listunspent failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


async def wallet_passphrase_rpc(data):
    """Handle walletpassphrase RPC call - unlocks an encrypted wallet temporarily

    Bitcoin Core compatible format:
    bitcoin-cli walletpassphrase "passphrase" timeout

    Parameters:
    - passphrase: The wallet passphrase
    - timeout: Time in seconds to keep the wallet unlocked (0 = unlock until restart)
    """
    try:
        params = data.get("params", [])

        if len(params) < 2:
            return rpc_error(-8, "walletpassphrase requires passphrase and timeout parameters", data.get("id"))

        passphrase = params[0]
        timeout = params[1]

        # Strip quotes from bitcoin-cli string parameters
        if isinstance(passphrase, str):
            passphrase = passphrase.strip('"')

        try:
            timeout = int(timeout)
        except (ValueError, TypeError):
            return rpc_error(-8, "Invalid timeout value", data.get("id"))

        # For now, we'll store unlocked state in memory
        # In production, this should be handled more securely
        # This is a placeholder implementation

        # TODO: Implement actual wallet unlocking logic
        # This would involve:
        # 1. Finding the currently loaded wallet
        # 2. Verifying the passphrase against encrypted keys
        # 3. Temporarily storing decrypted keys in memory
        # 4. Setting up a timer to re-lock after timeout

        logger.info(f"walletpassphrase called with timeout={timeout}")

        # For now, return success (placeholder)
        return {
            "result": None,  # Bitcoin Core returns null on success
            "error": None,
            "id": data.get("id")
        }

    except Exception as e:
        logger.error(f"walletpassphrase failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


async def encrypt_wallet_rpc(data):
    """Handle encryptwallet RPC call - encrypts an unencrypted wallet

    Bitcoin Core compatible format:
    bitcoin-cli encryptwallet "passphrase"

    Parameters:
    - passphrase: The passphrase to encrypt the wallet with

    Note: This permanently encrypts the wallet. Bitcoin Core shuts down after encryption.
    """
    try:
        params = data.get("params", [])

        if len(params) < 1:
            return rpc_error(-8, "encryptwallet requires passphrase parameter", data.get("id"))

        passphrase = params[0]

        # Strip quotes from bitcoin-cli string parameters
        if isinstance(passphrase, str):
            passphrase = passphrase.strip('"')

        # Validate passphrase strength
        if len(passphrase) < 1:
            return rpc_error(-8, "Passphrase cannot be empty", data.get("id"))

        # Find the first unencrypted wallet (in production, should track "loaded" wallet)
        wallets_dir = "wallets"
        encrypted_wallet = None

        if os.path.exists(wallets_dir):
            for wallet_name in os.listdir(wallets_dir):
                wallet_path = os.path.join(wallets_dir, wallet_name)
                if os.path.isdir(wallet_path):
                    wallet_file = os.path.join(wallet_path, "wallet.json")
                    if os.path.exists(wallet_file):
                        with open(wallet_file, "r") as f:
                            wallet_data = json.load(f)

                        # Check if already encrypted
                        if wallet_data.get("encrypted", False):
                            continue

                        # Encrypt this wallet
                        from wallet.wallet import _encrypt_privkey

                        for key_data in wallet_data.get("keys", []):
                            if "privateKey" in key_data:
                                # Encrypt the private key
                                private_key = key_data["privateKey"]
                                enc_priv, salt, iv = _encrypt_privkey(private_key, passphrase)

                                # Replace plain key with encrypted version
                                del key_data["privateKey"]
                                key_data["encryptedPrivateKey"] = enc_priv
                                key_data["salt"] = salt
                                key_data["iv"] = iv

                        # Mark wallet as encrypted and locked
                        wallet_data["encrypted"] = True
                        wallet_data["locked"] = True

                        # Save encrypted wallet
                        with open(wallet_file, "w") as f:
                            json.dump(wallet_data, f, indent=2)

                        encrypted_wallet = wallet_name
                        break

        if not encrypted_wallet:
            return rpc_error(-15, "Error: no unencrypted wallets found to encrypt.", data.get("id"))

        logger.info(f"Encrypted wallet: {encrypted_wallet}")

        # Bitcoin Core returns this message and shuts down
        return {
            "result": "wallet encrypted; Bitcoin server stopping, restart to run with encrypted wallet. The keypool has been flushed and a new HD seed was generated (if you are using HD). You need to make a new backup.",
            "error": None,
            "id": data.get("id")
        }

    except Exception as e:
        logger.error(f"encryptwallet failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


async def wallet_lock_rpc(data):
    """Handle walletlock RPC call - locks an encrypted wallet

    Bitcoin Core compatible format:
    bitcoin-cli walletlock

    Immediately locks the wallet, overriding any timeout from walletpassphrase.
    """
    try:
        # TODO: Implement actual wallet locking logic
        # This would involve:
        # 1. Finding the currently loaded wallet
        # 2. Verifying it's encrypted
        # 3. Clearing any decrypted keys from memory
        # 4. Marking wallet as locked

        logger.info(f"walletlock called")

        # For now, return success (Bitcoin Core returns null on success)
        return {
            "result": None,
            "error": None,
            "id": data.get("id")
        }

    except Exception as e:
        logger.error(f"walletlock failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


async def wallet_passphrase_change_rpc(data):
    """Handle walletpassphrasechange RPC call - changes wallet encryption passphrase

    Bitcoin Core compatible format:
    bitcoin-cli walletpassphrasechange "oldpassphrase" "newpassphrase"

    Changes the wallet encryption passphrase from oldpassphrase to newpassphrase.
    """
    try:
        params = data.get("params", [])

        if len(params) < 2:
            return rpc_error(-8, "walletpassphrasechange requires oldpassphrase and newpassphrase", data.get("id"))

        oldpassphrase = params[0]
        newpassphrase = params[1]

        # Strip quotes from bitcoin-cli string parameters
        if isinstance(oldpassphrase, str):
            oldpassphrase = oldpassphrase.strip('"')
        if isinstance(newpassphrase, str):
            newpassphrase = newpassphrase.strip('"')

        # Validate new passphrase
        if len(newpassphrase) < 1:
            return rpc_error(-8, "New passphrase cannot be empty", data.get("id"))

        # TODO: Implement actual passphrase change logic
        # This would involve:
        # 1. Finding the currently loaded wallet
        # 2. Verifying it's encrypted
        # 3. Decrypting all keys with old passphrase
        # 4. Re-encrypting all keys with new passphrase
        # 5. Saving the wallet

        logger.info(f"walletpassphrasechange called")

        # Bitcoin Core returns null on success
        return {
            "result": None,
            "error": None,
            "id": data.get("id")
        }

    except Exception as e:
        logger.error(f"walletpassphrasechange failed: {str(e)}")
        return rpc_error(-1, str(e), data.get("id"))


