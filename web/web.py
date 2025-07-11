import logging
import json
import base64
import time
import asyncio
import hashlib
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, Set, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from database.database import get_db, get_current_height
from wallet.wallet import verify_transaction
from pydantic import BaseModel
from blockchain.blockchain import sha256d, serialize_transaction
from state.state import mempool_manager
from config.config import CHAIN_ID

# Import security components
from models.validation import (
    TransactionRequest, WebSocketSubscription, CommitRequest
)
from errors.exceptions import (
    ValidationError, InsufficientFundsError, InvalidSignatureError
)
from middleware.error_handler import setup_error_handlers
from security.integrated_security import integrated_security_middleware
from monitoring.health import health_monitor
from security.integrated_security import get_security_status, unblock_client, get_client_info

# Import event system
from events.event_bus import event_bus, EventTypes
from web.websocket_handlers import WebSocketEventHandlers

# Import Bitcoin utilities
from utils.bitcoin_utils import verify_bitcoin_signature

# Import OpenTimestamps
try:
    import opentimestamps
    from opentimestamps.core.timestamp import Timestamp
    from opentimestamps.core.serialize import BytesSerializationContext, BytesDeserializationContext
    from opentimestamps.calendar import RemoteCalendar
    OTS_AVAILABLE = True
except ImportError:
    logger.warning("OpenTimestamps not available - commitments will not be timestamped")
    OTS_AVAILABLE = False

logger = logging.getLogger(__name__)

# Global reference to gossip node (set by startup)
_gossip_node = None

def set_gossip_node(node):
    """Set the global gossip node reference"""
    global _gossip_node
    _gossip_node = node
    logger.info(f"Gossip node reference set: {node}")

def get_gossip_node():
    """Get the global gossip node reference"""
    return _gossip_node

app = FastAPI(title="qBTC Core API", version="1.0.0")

# Setup security middleware
app.middleware("http")(integrated_security_middleware)

# Setup error handlers
setup_error_handlers(app)

# CORS - in production, restrict origins
app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"],  # TODO: Restrict in production
    allow_credentials=True, 
    allow_methods=["GET", "POST"],  # Only allow necessary methods
    allow_headers=["*"]
)
websocket_clients: Set[WebSocket] = set()

# Initialize event handlers on startup
@app.on_event("startup")
async def startup_event():
    """Initialize event system on startup"""
    try:
        # Start event bus
        await event_bus.start()
        logger.info("Event bus started")
        
        # Register WebSocket event handlers
        ws_handlers = WebSocketEventHandlers(websocket_manager)
        ws_handlers.register_handlers(event_bus)
        logger.info("WebSocket event handlers registered")
        
        # Store handlers reference
        app.state.ws_handlers = ws_handlers
        
    except Exception as e:
        logger.error(f"Failed to start event system: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up event system on shutdown"""
    try:
        await event_bus.stop()
        logger.info("Event bus stopped")
    except Exception as e:
        logger.error(f"Error stopping event bus: {e}")

class WorkerRequest(BaseModel):
    request_type: str
    message: Optional[str] = None
    signature: Optional[str] = None
    pubkey: Optional[str] = None
    wallet_address: Optional[str] = None
    network: Optional[str] = None
    direction: Optional[str] = None
    btc_account: Optional[str] = None

class WorkerResponse(BaseModel):
    status: str
    message: str
    tx_id: Optional[str] = None
    address: Optional[str] = None
    secret: Optional[str] = None

async def require_localhost(request: Request):
    """Dependency to ensure request is from localhost"""
    client_host = request.client.host
    allowed_hosts = ["127.0.0.1", "localhost", "::1"]  # IPv4 and IPv6 localhost
    
    # Also allow Docker host IPs and private network ranges for development
    # In production, remove these or make them configurable
    docker_hosts = ["192.168.65.1", "172.17.0.1", "host.docker.internal"]
    private_prefixes = ["192.168.", "10.", "172."]
    
    if client_host in allowed_hosts:
        return True
    
    if client_host in docker_hosts:
        return True
        
    # Check if it's a private network IP (for Docker)
    for prefix in private_prefixes:
        if client_host.startswith(prefix):
            logger.debug(f"Allowing debug access from private network IP: {client_host}")
            return True
    
    logger.warning(f"Attempted access to debug endpoint from non-localhost IP: {client_host}")
    raise HTTPException(status_code=403, detail="Debug endpoints are only accessible from localhost or private networks")
    
    return True

class WebSocketManager:
    def __init__(self):
        self.active_connections: Dict[WebSocket, Set[str]] = {}
        self.wallet_map: Dict[str, Set[WebSocket]] = {}
        self.bridge_sessions: Dict[str, Dict] = {}
        self.background_tasks: Dict[str, asyncio.Task] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[websocket] = set()

    async def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.pop(websocket)
            for wallet in list(self.wallet_map.keys()):
                if websocket in self.wallet_map[wallet]:
                    self.wallet_map[wallet].discard(websocket)
                    if not self.wallet_map[wallet]:
                        del self.wallet_map[wallet]

    def subscribe(self, websocket: WebSocket, update_type: str, wallet_address: str = None):
        if websocket in self.active_connections:
            self.active_connections[websocket].add(update_type)
            logging.info(f"WebSocket subscribed to {update_type} (wallet: {wallet_address})")
            if wallet_address and update_type in ["combined_update", "bridge"]:
                self.wallet_map.setdefault(wallet_address, set()).add(websocket)
                logging.debug(f"Added wallet {wallet_address} to wallet_map for {update_type}")

    async def broadcast(self, message: dict, update_type: str, wallet_address: str = None):
        target_connections = (
            set(self.active_connections.keys()) if update_type in ["all_transactions", "l1_proofs_testnet"]
            else self.wallet_map.get(wallet_address, set())
        )
        logging.info(f"Broadcasting to {len(target_connections)} connections for {update_type} (wallet: {wallet_address})")
        logging.info(f"Active connections: {len(self.active_connections)}")
        logging.info(f"Wallet map: {list(self.wallet_map.keys())}")
        
        sent_count = 0
        for connection in target_connections:
            subscriptions = self.active_connections.get(connection, set())
            logging.info(f"Connection subscriptions: {subscriptions}")
            if update_type in subscriptions:
                try:
                    logging.info(f"About to send message: {message}")
                    await connection.send_json(message)
                    sent_count += 1
                    logging.info(f"Successfully sent {update_type} to connection")
                except Exception as e:
                    logging.error(f"Failed to send message to WebSocket: {e}")
                    logging.error(f"Exception type: {type(e)}")
                    logging.error(f"Connection state: {connection.client_state if hasattr(connection, 'client_state') else 'unknown'}")
                    await self.disconnect(connection)
        logging.info(f"Broadcast complete: sent to {sent_count} connections")

    def create_bridge_session(self, wallet_address: str, direction: str, bridge_address: str = None, secret: str = None):
        session_id = f"{wallet_address}_{direction}_{int(time.time())}"
        self.bridge_sessions[session_id] = {
            "wallet_address": wallet_address, "direction": direction, "bridge_address": bridge_address,
            "secret": secret, "status": "waiting", "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        return session_id

    def get_bridge_sessions(self, wallet_address: str):
        return [session for session_id, session in self.bridge_sessions.items() if session["wallet_address"] == wallet_address]

    def update_bridge_status(self, wallet_address: str, bridge_address: str, status: str):
        for session_id, session in self.bridge_sessions.items():
            if session["wallet_address"] == wallet_address and session.get("bridge_address") == bridge_address:
                session["status"] = status
                session["updated_at"] = datetime.now().isoformat()
                return session
        return None

websocket_manager = WebSocketManager()


async def create_opentimestamp(data: bytes) -> Optional[str]:
    """
    Create an OpenTimestamp for the given data.
    
    Args:
        data: The data to timestamp
        
    Returns:
        Optional[str]: The OTS proof in hex format, or None if OTS is not available
    """
    if not OTS_AVAILABLE:
        return None
        
    try:
        # Create timestamp
        timestamp = Timestamp(data)
        
        # Create calendar submissions
        calendars = [
            "https://alice.btc.calendar.opentimestamps.org",
            "https://bob.btc.calendar.opentimestamps.org",
            "https://finney.calendar.eternitywall.com"
        ]
        
        for calendar_url in calendars:
            try:
                calendar = RemoteCalendar(calendar_url)
                result = await asyncio.get_event_loop().run_in_executor(
                    None, calendar.submit, timestamp.msg
                )
                if result:
                    timestamp.merge(result)
            except Exception as e:
                logger.warning(f"Failed to submit to calendar {calendar_url}: {e}")
        
        # Serialize the timestamp proof
        ctx = BytesSerializationContext()
        timestamp.serialize(ctx)
        return ctx.getbytes().hex()
        
    except Exception as e:
        logger.error(f"Failed to create OpenTimestamp: {e}")
        return None

def get_balance(wallet_address: str) -> Decimal:
    db = get_db()
    total = Decimal("0")
    for key, value in db.items():
        if key.startswith(b"utxo:"):
            utxo_data = json.loads(value.decode())
            if utxo_data["receiver"] == wallet_address and not utxo_data["spent"]:
                total += Decimal(utxo_data["amount"])
    return total

def get_transactions(wallet_address: str, limit: int = 50):
    logging.info(f"=== get_transactions called for wallet: {wallet_address} ===")
    db = get_db()
    tx_list = []
    transactions = {}
    utxo_count = 0
    matching_utxos = 0
    
    # Log all transaction entries in the database
    logging.info("=== SCANNING ALL TRANSACTIONS IN DATABASE ===")
    tx_entries = []
    for key, value in db.items():
        if key.startswith(b"tx:"):
            tx_data = json.loads(value.decode('utf-8'))
            tx_entries.append((key.decode(), tx_data))
    
    logging.info(f"Found {len(tx_entries)} transaction entries in database")
    for tx_key, tx_data in tx_entries[:10]:  # Log first 10
        logging.info(f"Transaction: {tx_key} -> {json.dumps(tx_data, default=str)}")

    for key, value in db.items():
        if not key.startswith(b"utxo:"):
            continue

        utxo_count += 1
        utxo = json.loads(value.decode('utf-8'))
        sender = utxo["sender"]
        receiver = utxo["receiver"]
        amount = Decimal(utxo["amount"])
        txid = utxo["txid"]
        
        logging.info(f"UTXO {utxo_count}: key={key.decode()}, txid={txid}, sender={sender}, receiver={receiver}, amount={amount}")

        # Skip change transactions explicitly
        if sender == wallet_address and receiver == wallet_address:
            logging.debug(f"  -> Skipping change transaction for txid {txid}")
            continue
        
        # Check if this UTXO involves our wallet
        involves_wallet = (sender == wallet_address or receiver == wallet_address)
        if involves_wallet:
            matching_utxos += 1
            logging.debug(f"  -> UTXO involves wallet: sender={sender}, receiver={receiver}, wallet={wallet_address}")
        else:
            logging.debug(f"  -> UTXO does NOT involve wallet: sender={sender}, receiver={receiver}, wallet={wallet_address}")

        # Initialize if needed
        if txid not in transactions:
            # Fetch the correct timestamp from corresponding tx entry
            tx_key = f"tx:{txid}".encode()
            tx_data_raw = db.get(tx_key)

            if tx_data_raw:
                tx_data = json.loads(tx_data_raw.decode('utf-8'))
                timestamp = tx_data.get("timestamp", 0)
            else:
                timestamp = 0

            transactions[txid] = {
                "sent": Decimal("0"),
                "received": Decimal("0"),
                "sent_to": [],
                "received_from": [],
                "timestamp": timestamp
            }

        if sender == wallet_address and receiver != wallet_address:
            transactions[txid]["sent"] += amount
            transactions[txid]["sent_to"].append(receiver)
            logging.debug(f"  -> Added SENT transaction: txid={txid}, amount={amount}, to={receiver}")

        elif receiver == wallet_address and (sender != wallet_address or sender == ""):
            transactions[txid]["received"] += amount
            transactions[txid]["received_from"].append(sender)
            if sender == "":
                logging.debug(f"  -> Added GENESIS transaction: txid={txid}, amount={amount}, from=GENESIS")
            else:
                logging.debug(f"  -> Added RECEIVED transaction: txid={txid}, amount={amount}, from={sender}")

    for txid, data in transactions.items():
        if data["sent"] > 0:
            sent_to_addr = next((addr for addr in data["sent_to"] if addr != wallet_address), "Unknown")
            tx_list.append({
                "txid": txid,
                "direction": "sent",
                "amount": f"-{data['sent']}",
                "counterpart": sent_to_addr,
                "timestamp": data["timestamp"]
            })

        if data["received"] > 0:
            received_from_addr = next((addr for addr in data["received_from"] if addr != wallet_address), "Unknown")
            # Display "GENESIS" for genesis transactions with empty sender
            if received_from_addr == "":
                received_from_addr = "GENESIS"
            
            # Log genesis transaction detection
            if received_from_addr == "GENESIS":
                logging.info(f"*** GENESIS TRANSACTION DETECTED ***")
                logging.info(f"  txid: {txid}")
                logging.info(f"  counterpart: {received_from_addr}")
                logging.info(f"  amount: {data['received']}")
                logging.info(f"  timestamp: {data['timestamp']}")
                
            tx_list.append({
                "txid": txid,
                "direction": "received",
                "amount": f"{data['received']}",
                "counterpart": received_from_addr,
                "timestamp": data["timestamp"]
            })

    logging.info(f"Summary: Found {utxo_count} total UTXOs, {matching_utxos} involving wallet {wallet_address}")
    logging.info(f"Grouped into {len(transactions)} unique transactions")
    
    # Add pending transactions from mempool
    logging.info(f"Checking mempool for pending transactions...")
    all_mempool_txs = mempool_manager.get_all_transactions()
    logging.info(f"Current mempool size: {len(all_mempool_txs)}")
    logging.info(f"Mempool transactions: {list(all_mempool_txs.keys())}")
    mempool_count = 0
    for txid, tx in all_mempool_txs.items():
        # Check if this transaction involves our wallet
        involves_wallet = False
        
        # Check outputs for involvement
        for output in tx.get("outputs", []):
            if output.get("sender") == wallet_address or output.get("receiver") == wallet_address:
                involves_wallet = True
                break
        
        if involves_wallet:
            mempool_count += 1
            # Determine direction and counterpart
            for output in tx.get("outputs", []):
                if output.get("sender") == wallet_address and output.get("receiver") != wallet_address:
                    # Sending transaction
                    tx_list.append({
                        "txid": txid,
                        "direction": "sent",
                        "amount": f"-{output.get('amount', '0')}",
                        "counterpart": output.get("receiver", "Unknown"),
                        "timestamp": tx.get("timestamp", int(time.time() * 1000)),
                        "isMempool": True,
                        "isPending": True
                    })
                elif output.get("receiver") == wallet_address and output.get("sender") != wallet_address:
                    # Receiving transaction
                    tx_list.append({
                        "txid": txid,
                        "direction": "received", 
                        "amount": f"{output.get('amount', '0')}",
                        "counterpart": output.get("sender", "Unknown"),
                        "timestamp": tx.get("timestamp", int(time.time() * 1000)),
                        "isMempool": True,
                        "isPending": True
                    })
    
    logging.info(f"Found {mempool_count} pending transactions in mempool for wallet {wallet_address}")
    
    tx_list.sort(key=lambda x: x["timestamp"], reverse=True)
    
    logging.info(f"Final transaction list has {len(tx_list)} entries")
    logging.info("=== ALL TRANSACTIONS BEING RETURNED ===")
    for idx, tx in enumerate(tx_list):
        logging.info(f"  Transaction {idx+1}: txid={tx['txid']}, direction={tx['direction']}, counterpart={tx['counterpart']}, amount={tx['amount']}, timestamp={tx['timestamp']}")

    return tx_list[:limit]

# DEPRECATED: Replaced by event-based system
# async def simulate_all_transactions():
#     while True:
#         try:
#             db = get_db()
#         except Exception as e:
#             logging.warning(f"Database not available: {e}, sending test data")
#             # Send test data when database is not available
#             test_data = {
#                 "type": "transaction_update",
#                 "transactions": [
#                     {
#                         "id": "test_tx_001",
#                         "hash": "test_tx_001",
#                         "sender": "bqs1test_sender",
#                         "receiver": "bqs1test_receiver",
#                         "amount": "10.00000000 qBTC",
#                         "timestamp": datetime.utcnow().isoformat(),
#                         "status": "confirmed"
#                     }
#                 ],
#                 "timestamp": datetime.utcnow().isoformat()
#             }
#             await websocket_manager.broadcast(test_data, "all_transactions")
#             await asyncio.sleep(10)
#             continue
#             
#         formatted = []
# 
#         for key, value in db.items():
#             key_text = key.decode("utf-8")
#             if not key_text.startswith("utxo:"):
#                 continue
# 
#             try:
#                 utxo = json.loads(value.decode("utf-8"))
#                 txid = utxo["txid"]
#                 sender = utxo["sender"]
#                 receiver = utxo["receiver"]
#                 amount = Decimal(utxo["amount"])
# 
#                 # Skip change outputs (self-to-self)
#                 if sender == receiver:
#                     continue
# 
#                 # Skip mining rewards (no sender)
#                 if sender == "":
#                     continue
# 
#                 # Get timestamp if available
#                 tx_data_raw = db.get(f"tx:{txid}".encode())
#                 if tx_data_raw:
#                     tx_data = json.loads(tx_data_raw.decode())
#                     ts = tx_data.get("timestamp", 0)
#                 else:
#                     ts = 0
# 
#                 timestamp_iso = datetime.fromtimestamp(ts / 1000).isoformat() if ts else datetime.utcnow().isoformat()
# 
#                 formatted.append({
#                     "id": txid,
#                     "hash": txid,
#                     "sender": sender,
#                     "receiver": receiver,
#                     "amount": f"{amount:.8f} qBTC",
#                     "timestamp": timestamp_iso,
#                     "status": "confirmed",
#                     "_sort_ts": ts  # hidden field for sorting
#                 })
# 
#             except (json.JSONDecodeError, InvalidOperation, KeyError) as e:
#                 print(f"Skipping bad UTXO: {e}")
#                 continue
# 
#         # Sort most recent first (descending by timestamp)
#         formatted.sort(key=lambda x: x["_sort_ts"], reverse=True)
# 
#         # Remove internal sorting field
#         for tx in formatted:
#             tx.pop("_sort_ts", None)
# 
#         update_data = {
#             "type": "transaction_update",
#             "transactions": formatted,
#             "timestamp": datetime.utcnow().isoformat()
#         }
# 
#         await websocket_manager.broadcast(update_data, "all_transactions")
#         await asyncio.sleep(10)

async def broadcast_to_websocket_clients(message: str):
    # Copy clients to avoid modifying set during iteration
    disconnected_clients = []
    for client in websocket_clients:
        try:
            await client.send_text(message)
        except WebSocketDisconnect:
            disconnected_clients.append(client)
        except Exception as e:
            logging.error(f"Error broadcasting to client: {e}")
            disconnected_clients.append(client)
    
    # Remove disconnected clients
    for client in disconnected_clients:
        websocket_clients.remove(client)

# 
# async def simulate_combined_updates(wallet_address: str):
#     # Send initial update immediately
#     first_run = True
#     while True:
#         try:
#             balance = get_balance(wallet_address)
#             transactions = get_transactions(wallet_address)
#             logging.info(f"Found {len(transactions)} transactions for wallet {wallet_address}")
#             logging.debug(f"Transactions: {transactions}")
#         except Exception as e:
#             logging.warning(f"Database not available for wallet {wallet_address}: {e}, sending test data")
#             # Send test data when database is not available
#             test_data = {
#                 "type": "combined_update",
#                 "balance": "100.00000000",
#                 "transactions": [
#                     {
#                         "id": "test_tx_wallet_001",
#                         "type": "receive",
#                         "amount": "50.00000000 qBTC",
#                         "address": "bqs1test_sender",
#                         "timestamp": datetime.utcnow().isoformat(),
#                         "hash": "test_tx_wallet_001",
#                         "status": "confirmed"
#                     }
#                 ]
#             }
#             await websocket_manager.broadcast(test_data, "combined_update", wallet_address)
#             await asyncio.sleep(10)
#             continue
#         formatted = []
# 
#         for tx in transactions:
#             tx_type = "send" if tx["direction"] == "sent" else "receive"
#             
#             amt_dec = Decimal(tx["amount"])
#             amount_fmt = f"{abs(amt_dec):.8f} qBTC"
# 
#             address = tx["counterpart"] if tx["counterpart"] else "n/a"
# 
#             formatted.append({
#                 "id":        tx["txid"],
#                 "type":      tx_type,
#                 "amount":    amount_fmt,
#                 "address":   address,
#                 "timestamp": datetime.fromtimestamp(tx["timestamp"] / 1000).isoformat(),
#                 "hash":      tx["txid"],
#                 "status":    "confirmed"
#             })
# 
#         await websocket_manager.broadcast(
#             {
#                 "type":         "combined_update",
#                 "balance":      f"{balance:.8f}",
#                 "transactions": formatted
#             },
#             "combined_update",
#             wallet_address
#         )
#         
#         # Sleep less on first run to send initial data quickly
#         if first_run:
#             await asyncio.sleep(1)
#             first_run = False
#         else:
#             await asyncio.sleep(10)

# async def simulate_l1_proofs_testnet():
#     while True:
#         db = get_db()
#         proofs = {}
#         for key, value in db.items():
#             if key.startswith(b"block:"):
#                 block = json.loads(value.decode())
#                 tx_ids = block["tx_ids"]
#                 proofs[block["height"]] = {
#                     "blockHeight": block["height"], "merkleRoot": block["block_hash"],
#                     "bitcoinTxHash": None, "timestamp": datetime.fromtimestamp(block["timestamp"] / 1000).isoformat(),
#                     "transactions": [{"id": tx_id, "hash": tx_id, "status": "confirmed"} for tx_id in tx_ids],
#                     "status": "confirmed"
#                 }
#         update_data = {"type": "l1proof_update", "proofs": list(proofs.values()), "timestamp": datetime.now().isoformat()}
#         await websocket_manager.broadcast(update_data, "l1_proofs_testnet")
#         await asyncio.sleep(10)



@app.get("/balance/{wallet_address}")
async def get_balance_endpoint(wallet_address: str):
    # Validate address format
    if not wallet_address.startswith('bqs') or len(wallet_address) < 20:
        raise ValidationError("Invalid wallet address format")
    
    try:
        balance = get_balance(wallet_address)
        return {"wallet_address": wallet_address, "balance": str(balance)}
    except Exception as e:
        logger.error(f"Error getting balance for {wallet_address}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving balance")

@app.get("/transactions/{wallet_address}")
async def get_transactions_endpoint(wallet_address: str, limit: int = 50):
    logging.info(f"=== API: /transactions/{wallet_address} called (limit={limit}) ===")
    
    # Validate inputs
    if not wallet_address.startswith('bqs') or len(wallet_address) < 20:
        logging.error(f"Invalid wallet address format: {wallet_address}")
        raise ValidationError("Invalid wallet address format")
    
    if limit < 1 or limit > 1000:
        logging.error(f"Invalid limit: {limit}")
        raise ValidationError("Limit must be between 1 and 1000")
    
    try:
        transactions = get_transactions(wallet_address, limit)
        logging.info(f"API returning {len(transactions)} transactions for {wallet_address}")
        return {"wallet_address": wallet_address, "transactions": transactions}
    except Exception as e:
        logger.error(f"Error getting transactions for {wallet_address}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving transactions")

@app.get("/utxos/{wallet_address}")
async def get_utxos_endpoint(wallet_address: str):
    """Get unspent transaction outputs (UTXOs) for a wallet address"""
    # Validate address format
    if not wallet_address.startswith('bqs') or len(wallet_address) < 20:
        raise ValidationError("Invalid wallet address format")
    
    try:
        db = get_db()
        utxos = []
        
        for key, value in db.items():
            if key.startswith(b"utxo:"):
                utxo_data = json.loads(value.decode())
                # Only include unspent UTXOs that belong to this address
                if utxo_data["receiver"] == wallet_address and not utxo_data.get("spent", False):
                    utxos.append({
                        "txid": utxo_data.get("txid"),
                        "vout": utxo_data.get("utxo_index", 0),  # output index
                        "amount": utxo_data.get("amount"),
                        "address": wallet_address,
                        "confirmations": 1,  # Since we don't track confirmations in qBTC, assume confirmed
                        "spendable": True,
                        "solvable": True
                    })
        
        return {
            "wallet_address": wallet_address,
            "utxos": utxos,
            "count": len(utxos)
        }
    except Exception as e:
        logger.error(f"Error getting UTXOs for {wallet_address}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving UTXOs")

@app.get("/debug/utxos")
async def debug_utxos(localhost_only: bool = Depends(require_localhost)):
    """Debug endpoint to show all UTXOs in the database"""
    try:
        db = get_db()
        utxos = []
        count = 0
        
        for key, value in db.items():
            if key.startswith(b"utxo:") and count < 20:  # Limit to first 20
                count += 1
                utxo_data = json.loads(value.decode())
                utxos.append({
                    "key": key.decode(),
                    "txid": utxo_data.get("txid"),
                    "sender": utxo_data.get("sender"),
                    "receiver": utxo_data.get("receiver"),
                    "amount": utxo_data.get("amount"),
                    "spent": utxo_data.get("spent", False)
                })
        
        return {
            "total_shown": count,
            "utxos": utxos
        }
    except Exception as e:
        logger.error(f"Error in debug_utxos: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/genesis")
async def debug_genesis(localhost_only: bool = Depends(require_localhost)):
    """Debug endpoint to find genesis transactions"""
    try:
        db = get_db()
        genesis_txs = []
        all_txs = []
        
        # Check all transaction entries
        for key, value in db.items():
            if key.startswith(b"tx:"):
                tx_data = json.loads(value.decode('utf-8'))
                txid = key.decode().replace("tx:", "")
                all_txs.append({
                    "txid": txid,
                    "data": tx_data
                })
                
        # Check all UTXOs for genesis patterns
        for key, value in db.items():
            if key.startswith(b"utxo:"):
                utxo_data = json.loads(value.decode())
                if utxo_data.get("sender") == "" or utxo_data.get("sender") == "GENESIS":
                    genesis_txs.append({
                        "key": key.decode(),
                        "txid": utxo_data.get("txid"),
                        "sender": utxo_data.get("sender"),
                        "receiver": utxo_data.get("receiver"),
                        "amount": utxo_data.get("amount"),
                        "spent": utxo_data.get("spent", False)
                    })
        
        return {
            "genesis_utxos": genesis_txs,
            "total_transactions": len(all_txs),
            "first_10_transactions": all_txs[:10],
            "possible_genesis_txids": [tx["txid"] for tx in genesis_txs]
        }
    except Exception as e:
        logger.error(f"Error in debug_genesis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check(request: Request):
    """Prometheus metrics endpoint"""
    try:
        # Get gossip node from global reference
        gossip_client = get_gossip_node()
        
        # If not found there, try sys.modules as fallback
        if not gossip_client:
            import sys
            gossip_client = getattr(sys.modules.get('__main__', None), 'gossip_node', None)
        
        # If not found there, try app.state as final fallback
        if not gossip_client:
            gossip_client = getattr(request.app.state, 'gossip_client', None)
        
        # Log for debugging
        if gossip_client:
            logger.debug(f"Found gossip_client: {type(gossip_client)}, has dht_peers: {hasattr(gossip_client, 'dht_peers')}")
        else:
            logger.warning("No gossip_client found for health check")
        
        # Run health checks to update metrics
        await health_monitor.run_health_checks(gossip_client)
        
        # Generate Prometheus metrics
        metrics, content_type = health_monitor.generate_metrics()
        
        from fastapi.responses import Response
        return Response(
            content=metrics,
            media_type=content_type
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        from fastapi.responses import JSONResponse
        return JSONResponse(
            content={
                "status": "unhealthy",
                "message": "Health check system error",
                "timestamp": time.time()
            },
            status_code=503
        )

@app.get("/debug/mempool")
async def debug_mempool(localhost_only: bool = Depends(require_localhost)):
    """Debug endpoint to check mempool status"""
    try:
        stats = mempool_manager.get_stats()
        all_txs = mempool_manager.get_all_transactions()
        return {
            "mempool_size": len(all_txs),
            "transactions": list(all_txs.keys()),
            "timestamp": datetime.utcnow().isoformat(),
            "stats": stats
        }
    except Exception as e:
        logger.error(f"Error getting mempool status: {str(e)}")
        raise HTTPException(status_code=500, detail="Error retrieving mempool")

@app.get("/debug/network")
async def debug_network(localhost_only: bool = Depends(require_localhost)):
    """Debug endpoint to check network status"""
    import sys
    from config.config import VALIDATOR_ID
    try:
        status = {
            "timestamp": datetime.utcnow().isoformat(),
            "validator_id": VALIDATOR_ID
        }
        
        # Check gossip node
        if hasattr(sys.modules.get('__main__'), 'gossip_node'):
            gossip_node = sys.modules['__main__'].gossip_node
            status["gossip"] = {
                "running": True,
                "node_id": gossip_node.node_id,
                "port": gossip_node.gossip_port,
                "is_bootstrap": gossip_node.is_bootstrap,
                "dht_peers": len(gossip_node.dht_peers),
                "client_peers": len(gossip_node.client_peers),
                "total_peers": len(gossip_node.dht_peers) + len(gossip_node.client_peers),
                "dht_peer_list": [list(peer) if isinstance(peer, tuple) else peer for peer in gossip_node.dht_peers],
                "client_peer_list": [list(peer) if isinstance(peer, tuple) else peer for peer in gossip_node.client_peers],
                "synced_peers": len(gossip_node.synced_peers),
                "failed_peers": {str(k): v for k, v in gossip_node.failed_peers.items()}
            }
        else:
            status["gossip"] = {"running": False}
            
        # Check DHT
        if hasattr(sys.modules.get('__main__'), 'dht_task'):
            dht_task = sys.modules['__main__'].dht_task
            status["dht"] = {
                "running": not dht_task.done(),
                "task_state": "done" if dht_task.done() else "running"
            }
            if dht_task.done() and dht_task.exception():
                status["dht"]["error"] = str(dht_task.exception())
        else:
            status["dht"] = {"running": False}
            
        return status
    except Exception as e:
        logger.error(f"Error getting network status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/peers")
async def debug_peers(localhost_only: bool = Depends(require_localhost)):
    """Debug endpoint to see detailed peer information"""
    import sys
    try:
        # Get gossip node
        gossip_node = get_gossip_node()
        if not gossip_node:
            gossip_node = getattr(sys.modules.get('__main__', None), 'gossip_node', None)
        
        if not gossip_node:
            return {"error": "Gossip node not available"}
        
        return {
            "node_id": gossip_node.node_id,
            "is_bootstrap": gossip_node.is_bootstrap,
            "dht_peers": {
                "count": len(gossip_node.dht_peers),
                "peers": [{"host": p[0], "port": p[1]} for p in gossip_node.dht_peers]
            },
            "client_peers": {
                "count": len(gossip_node.client_peers),
                "peers": [{"host": p[0], "port": p[1]} for p in gossip_node.client_peers]
            },
            "synced_peers": {
                "count": len(gossip_node.synced_peers),
                "peers": list(gossip_node.synced_peers)
            },
            "failed_peers": dict(gossip_node.failed_peers),
            "total_active_peers": len(gossip_node.dht_peers) + len(gossip_node.client_peers),
            "peer_info": {str(k): v for k, v in gossip_node.peer_info.items()},
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error in debug_peers: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/worker")
async def worker_endpoint(request: Request):
    """Process transaction broadcast requests with validation"""
    db = get_db()
    
    # Get gossip node from global reference
    gossip_client = get_gossip_node()
    
    # If not found there, try sys.modules as fallback
    if not gossip_client:
        import sys
        gossip_client = getattr(sys.modules.get('__main__', None), 'gossip_node', None)
    
    # If still not found, try app.state as final fallback
    if not gossip_client:
        gossip_client = getattr(request.app.state, 'gossip_client', None)  

    try:
        payload = await request.json()
    except json.JSONDecodeError:
        raise ValidationError("Invalid JSON in request body")
    
    # Validate request structure
    if not isinstance(payload, dict) or "request_type" not in payload:
        raise ValidationError("Missing or invalid request_type")

    if payload.get("request_type") == "broadcast_tx":
        # Validate required fields
        required_fields = ["message", "signature", "pubkey"]
        for field in required_fields:
            if field not in payload:
                raise ValidationError(f"Missing required field: {field}")
        
        # Validate using Pydantic model
        try:
            tx_request = TransactionRequest(
                message=payload["message"],
                signature=payload["signature"],
                pubkey=payload["pubkey"]
            )
        except Exception as e:
            raise ValidationError(f"Transaction validation failed: {str(e)}")
        
        try:
            message_bytes = base64.b64decode(tx_request.message)
            signature_bytes = base64.b64decode(tx_request.signature)
            pubkey_bytes = base64.b64decode(tx_request.pubkey)
        except Exception:
            raise ValidationError("Invalid base64 encoding")
        
        signature_hex = signature_bytes.hex()
        pubkey_hex = pubkey_bytes.hex()
        message_str = message_bytes.decode("utf-8")
        
        # Store original message for signature verification
        original_message_str = message_str
        
        parts = message_str.split(":")
        if len(parts) == 3:
            # Old format - add timestamp and chain_id for compatibility
            sender_, receiver_, send_amount = parts[0], parts[1], parts[2]
            timestamp = str(int(time.time() * 1000))
            chain_id = str(CHAIN_ID)
            # Update message_str to new format for storage
            message_str = f"{sender_}:{receiver_}:{send_amount}:{timestamp}:{chain_id}"
        elif len(parts) == 5:
            # New format with timestamp and chain_id
            sender_, receiver_, send_amount, timestamp, chain_id = parts
            # Validate chain_id
            if int(chain_id) != CHAIN_ID:
                raise ValidationError(f"Invalid chain ID: expected {CHAIN_ID}, got {chain_id}")
        else:
            raise ValidationError("Invalid message format - expected sender:receiver:amount:timestamp:chain_id")
        
        # Verify transaction signature against original message
        if not verify_transaction(original_message_str, signature_hex, pubkey_hex):
            raise InvalidSignatureError("Transaction signature verification failed")

        inputs = []
        total_available = Decimal("0")
        for key, value in db.items():
            if key.startswith(b"utxo:"):
                utxo_data = json.loads(value.decode())
                if utxo_data["receiver"] == sender_ and not utxo_data["spent"]:
                    amount_decimal = Decimal(str(utxo_data.get("amount")))
                    inputs.append({
                        "txid": utxo_data.get("txid"),
                        "utxo_index": utxo_data.get("utxo_index"),
                        "sender": utxo_data.get("sender"),
                        "receiver": utxo_data.get("receiver"),
                        "amount": str(amount_decimal),
                        "spent": False
                    })
                    total_available += amount_decimal

        miner_fee = (Decimal(send_amount) * Decimal("0.001")).quantize(Decimal("0.00000001"))
        total_required = Decimal(send_amount) + Decimal(miner_fee)
        
        if total_available < total_required:
            raise InsufficientFundsError(
                required=str(total_required),
                available=str(total_available)
            )

        outputs = [
            {"utxo_index": 0, "sender": sender_, "receiver": receiver_, "amount": str(send_amount), "spent": False},
        ]

        change = total_available - total_required
        if change > 0:
            outputs.insert(1, {
                "utxo_index": 1, "sender": sender_, "receiver": sender_, "amount": str(change), "spent": False
            })

        transaction = {
            "type": "transaction",
            "inputs": inputs,
            "outputs": outputs,
            "body": {
                "msg_str": message_str,
                "pubkey": pubkey_hex,
                "signature": signature_hex
            },
            "timestamp": int(time.time() * 1000)
        }

        raw_tx = serialize_transaction(transaction)
        txid = sha256d(bytes.fromhex(raw_tx))[::-1].hex() 
        transaction["txid"] = txid
        # Don't add txid to outputs - this contaminates the transaction structure
        success, error = mempool_manager.add_transaction(transaction)
        if not success:
            # This means the transaction conflicts with existing mempool transactions
            logger.warning(f"[MEMPOOL] Rejected transaction {txid}: {error}")
            raise ValidationError(f"Transaction rejected: {error}")
        logger.info(f"[MEMPOOL] Added transaction {txid} to mempool. Current size: {mempool_manager.size()}")
        #db.put(b"tx:" + txid.encode(), json.dumps(transaction).encode())

        # Emit mempool transaction event
        await event_bus.emit(EventTypes.TRANSACTION_PENDING, {
            'txid': txid,
            'transaction': transaction,
            'sender': sender_,
            'receiver': receiver_,
            'amount': send_amount
        }, source='web')
        logger.info(f"[EVENT] Emitted TRANSACTION_PENDING event for {txid}")

        # Broadcast to network if gossip client is available
        if gossip_client:
            await gossip_client.randomized_broadcast(transaction)
            logger.info(f"Transaction {txid} broadcast to network")
        else:
            logger.warning(f"No gossip client available - transaction {txid} added to mempool but not broadcast")

        return {"status": "success", "message": "Transaction broadcast successfully", "txid": txid}
    
    else:
        raise ValidationError(f"Unsupported request type: {payload.get('request_type')}")

   

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    logging.info(f"WebSocket connection attempt from {websocket.client} with headers: {dict(websocket.headers)}")
    try:
        logging.info(f"WebSocket accepted: {websocket.client}")
        await websocket_manager.connect(websocket)
        while True:
            try:
                data = await websocket.receive_json()
                logging.debug(f"Received: {data}")
                
                # Handle ping messages
                if data.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
                    continue
                
                # Validate WebSocket subscription message
                try:
                    ws_request = WebSocketSubscription(**data)
                    update_type = ws_request.update_type
                    wallet_address = ws_request.wallet_address
                except Exception as e:
                    await websocket.send_json({
                        "error": "validation_error", 
                        "message": f"Invalid subscription request: {str(e)}"
                    })
                    continue
                if update_type:
                    websocket_manager.subscribe(websocket, update_type, wallet_address)
                    
                    # Send immediate acknowledgment
                    await websocket.send_json({
                        "type": "subscription_confirmed",
                        "update_type": update_type,
                        "wallet_address": wallet_address
                    })
                    
                    # Send initial data immediately for new subscriptions
                    if update_type == "combined_update" and wallet_address:
                        # Send initial data directly without relying on event system
                        try:
                            balance = get_balance(wallet_address)
                            transactions = get_transactions(wallet_address)
                            
                            formatted = []
                            logging.info(f"=== WEBSOCKET FORMATTING {len(transactions)} TRANSACTIONS ===")
                            for idx, tx in enumerate(transactions):
                                logging.info(f"WebSocket TX {idx+1}: txid={tx['txid']}, direction={tx['direction']}, counterpart={tx['counterpart']}, timestamp={tx['timestamp']}")
                                
                                tx_type = "send" if tx["direction"] == "sent" else "receive"
                                amt_dec = Decimal(tx["amount"])
                                amount_fmt = f"{abs(amt_dec):.8f} qBTC"
                                address = tx["counterpart"] if tx["counterpart"] else "n/a"
                                
                                # Check if this is a genesis transaction
                                logging.info(f"  Checking genesis conditions:")
                                logging.info(f"    - txid == 'genesis_tx'? {tx['txid'] == 'genesis_tx'}")
                                logging.info(f"    - direction == 'received'? {tx['direction'] == 'received'}")
                                logging.info(f"    - counterpart == 'GENESIS'? {tx['counterpart'] == 'GENESIS'}")
                                
                                # Check if this is a genesis transaction by looking at the counterpart or txid
                                if tx["txid"] == "genesis_tx" or tx["counterpart"] == "bqs1genesis00000000000000000000000000000000":
                                    timestamp_str = "Genesis Block"
                                    logging.info(f"  *** GENESIS BLOCK TIMESTAMP SET ***")
                                else:
                                    timestamp_str = datetime.fromtimestamp(tx["timestamp"] / 1000).isoformat() if tx["timestamp"] else "Unknown"
                                    logging.info(f"  Regular timestamp: {timestamp_str}")
                                
                                formatted.append({
                                    "id": tx["txid"],
                                    "type": tx_type,
                                    "amount": amount_fmt,
                                    "address": address,
                                    "timestamp": timestamp_str,
                                    "hash": tx["txid"],
                                    "status": "confirmed" if not tx.get("isMempool") else "pending",
                                    "isMempool": tx.get("isMempool", False),
                                    "isPending": tx.get("isPending", False)
                                })
                            
                            initial_data = {
                                "type": "combined_update",
                                "balance": f"{balance:.8f}",
                                "transactions": formatted
                            }
                            
                            await websocket.send_json(initial_data)
                            logging.info(f"Sent initial data for wallet {wallet_address}: balance={balance}, txs={len(transactions)}")
                            
                        except Exception as e:
                            logging.error(f"Error sending initial data: {e}")
                        
                        # Also emit event for future updates
                        await event_bus.emit(EventTypes.WALLET_BALANCE_CHANGED, {
                            'wallet_address': wallet_address,
                            'reason': 'subscription'
                        }, source='websocket')
                        logging.debug(f"Triggered event for future updates for wallet {wallet_address}")
                    
                    elif update_type == "all_transactions":
                        # Send current transaction list immediately
                        if hasattr(app.state, 'ws_handlers'):
                            await app.state.ws_handlers._broadcast_all_transactions_update()
                        logging.debug("Sent initial all_transactions data")
                    
                    elif update_type == "l1_proofs_testnet":
                        # Send current L1 proofs immediately
                        if hasattr(app.state, 'ws_handlers'):
                            await app.state.ws_handlers._broadcast_l1_proofs_update()
                        logging.debug("Sent initial L1 proofs data")
            except json.JSONDecodeError as e:
                logging.warning(f"Invalid JSON received: {e}")
                await websocket.send_json({"error": "Invalid JSON", "message": str(e)})
    except WebSocketDisconnect:
        await websocket_manager.disconnect(websocket)
        logging.info(f"WebSocket disconnected: {websocket.client}")
    except Exception as e:
        logging.error(f"WebSocket error: {str(e)}", exc_info=True)
        await websocket.close(code=1008, reason=f"Server error: {str(e)}")


# Security Management Endpoints
@app.get("/admin/security/status")
async def get_security_status_endpoint():
    """Get comprehensive security status (admin only)"""
    try:
        status = await get_security_status()
        return status
    except Exception as e:
        logger.error(f"Failed to get security status: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve security status")

@app.post("/admin/security/unblock/{client_ip}")
async def unblock_client_endpoint(client_ip: str):
    """Unblock a specific client IP (admin only)"""
    try:
        # Validate IP format
        import ipaddress
        ipaddress.ip_address(client_ip)
        
        success = await unblock_client(client_ip)
        if success:
            logger.info(f"Client {client_ip} unblocked by admin")
            return {"status": "success", "message": f"Client {client_ip} unblocked"}
        else:
            return {"status": "error", "message": f"Client {client_ip} not found or not blocked"}
    except ValueError:
        raise ValidationError("Invalid IP address format")
    except Exception as e:
        logger.error(f"Failed to unblock client {client_ip}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to unblock client")

@app.get("/admin/security/client/{client_ip}")
async def get_client_info_endpoint(client_ip: str):
    """Get detailed information about a specific client (admin only)"""
    try:
        # Validate IP format
        import ipaddress
        ipaddress.ip_address(client_ip)
        
        client_info = await get_client_info(client_ip)
        if client_info:
            return client_info
        else:
            raise HTTPException(status_code=404, detail="Client not found")
    except ValueError:
        raise ValidationError("Invalid IP address format")
    except Exception as e:
        logger.error(f"Failed to get client info for {client_ip}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve client information")

@app.post("/commit")
async def commit_btc_to_qbtc(request: CommitRequest):
    """
    Commit a Bitcoin address to a qBTC address.
    This creates a binding between BTC and qBTC addresses without spending funds.
    """
    try:
        # Validate the request using Pydantic model (already done)
        
        # Extract addresses from the message
        btc_address = request.btcAddress
        qbtc_address = request.qbtcAddress
        message = request.message
        signature = request.signature
        
        # Verify that the message contains the correct addresses
        if btc_address not in message or qbtc_address not in message:
            raise ValidationError("Message does not contain the specified addresses")
        
        # Verify Bitcoin signature
        if not verify_bitcoin_signature(message, signature, btc_address):
            raise ValidationError("Invalid Bitcoin signature")
        
        # Get database connection
        db = get_db()
        
        # Check if this Bitcoin address has already been committed
        commitment_key = f"commitment:{btc_address}".encode()
        existing = db.get(commitment_key)
        
        if existing:
            # Bitcoin address already committed
            existing_data = json.loads(existing.decode())
            if existing_data.get("qbtc_address") == qbtc_address:
                # Same commitment, return success
                return {
                    "result": "success",
                    "message": "This commitment already exists",
                    "btc_address": btc_address,
                    "qbtc_address": qbtc_address,
                    "timestamp": existing_data.get("timestamp")
                }
            else:
                # Different qBTC address - not allowed
                return {
                    "result": "error",
                    "detail": "alradycommitted",  # Match the typo from BlueWallet
                    "message": f"Bitcoin address {btc_address} is already committed to a different qBTC address"
                }
        
        # Create commitment record
        timestamp = datetime.utcnow().isoformat()
        
        # Create a hash of the commitment for OpenTimestamps
        commitment_string = f"{btc_address}:{qbtc_address}:{message}:{signature}:{timestamp}"
        commitment_hash = hashlib.sha256(commitment_string.encode()).digest()
        
        # Create OpenTimestamp proof
        ots_proof = await create_opentimestamp(commitment_hash)
        
        commitment_data = {
            "btc_address": btc_address,
            "qbtc_address": qbtc_address,
            "message": message,
            "signature": signature,
            "timestamp": timestamp,
            "block_height": get_current_height(),
            "commitment_hash": commitment_hash.hex(),
            "ots_proof": ots_proof
        }
        
        # Store commitment in database
        db.put(commitment_key, json.dumps(commitment_data).encode())
        
        # Also store reverse lookup (qBTC -> BTC)
        reverse_key = f"commitment_reverse:{qbtc_address}".encode()
        db.put(reverse_key, btc_address.encode())
        
        # Log the commitment
        logger.info(f"BTC commitment created: {btc_address} -> {qbtc_address}")
        
        # Emit event for potential future use
        await event_bus.emit(EventTypes.CUSTOM, {
            'type': 'btc_commitment',
            'btc_address': btc_address,
            'qbtc_address': qbtc_address,
            'timestamp': timestamp
        }, source='commit_endpoint')
        
        return {
            "result": "success",
            "message": "BTC to qBTC commitment created successfully",
            "btc_address": btc_address,
            "qbtc_address": qbtc_address,
            "timestamp": timestamp,
            "commitment_hash": commitment_hash.hex(),
            "ots_proof": ots_proof if ots_proof else None,
            "ots_available": OTS_AVAILABLE
        }
        
    except ValidationError as e:
        logger.warning(f"Validation error in commit endpoint: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error in commit endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/commitment/{btc_address}")
async def get_commitment(btc_address: str):
    """Get commitment details for a Bitcoin address"""
    try:
        # Basic validation
        if not btc_address or len(btc_address) < 26:
            raise ValidationError("Invalid Bitcoin address")
        
        db = get_db()
        commitment_key = f"commitment:{btc_address}".encode()
        commitment_data = db.get(commitment_key)
        
        if not commitment_data:
            raise HTTPException(status_code=404, detail="No commitment found for this Bitcoin address")
        
        data = json.loads(commitment_data.decode())
        return {
            "btc_address": data.get("btc_address"),
            "qbtc_address": data.get("qbtc_address"),
            "timestamp": data.get("timestamp"),
            "block_height": data.get("block_height"),
            "commitment_hash": data.get("commitment_hash"),
            "ots_proof": data.get("ots_proof"),
            "has_ots": data.get("ots_proof") is not None
        }
        
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving commitment: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/commit/status")
async def get_commit_status():
    """Get the status of Bitcoin commitment verification capabilities"""
    return {
        "bitcoin_verification_available": True,
        "opentimestamps_available": OTS_AVAILABLE,
        "supported_address_types": ["P2PKH", "P2SH", "Bech32", "Testnet"],
        "message": "Bitcoin signature verification enabled using python-bitcoinlib"
    }
