"""
Event-based WebSocket handlers
"""

import logging
import json
import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Set, Dict

from events.event_bus import Event, EventTypes
from database.database import get_db

logger = logging.getLogger(__name__)


class WebSocketEventHandlers:
    """
    Event handlers for WebSocket notifications
    """
    
    def __init__(self, websocket_manager):
        self.websocket_manager = websocket_manager
        self.wallet_cache: Dict[str, Dict] = {}  # Cache wallet data to detect changes
        logger.info("WebSocketEventHandlers initialized")
    
    async def handle_transaction_confirmed(self, event: Event):
        """Handle confirmed transaction events"""
        try:
            tx_data = event.data
            txid = tx_data.get('txid')
            transaction = tx_data.get('transaction', {})
            confirmed_from_mempool = tx_data.get('confirmed_from_mempool', False)
            
            logger.info(f"Processing confirmed transaction: {txid} (from_mempool: {confirmed_from_mempool})")
            
            # Always collect affected wallets from the transaction
            affected_wallets = set()
            if transaction.get('sender'):
                affected_wallets.add(transaction.get('sender'))
            if transaction.get('receiver'):
                affected_wallets.add(transaction.get('receiver'))
            
            # If this was a mempool transaction, we need to update affected wallets with a delay
            if confirmed_from_mempool:
                logger.info(f"Transaction {txid} was confirmed from mempool, scheduling wallet updates")
                
                # Schedule wallet updates with a longer delay to ensure everything is processed
                async def delayed_wallet_update():
                    await asyncio.sleep(1.0)  # Wait 1 second to ensure all processing is done
                    logger.info(f"Executing delayed wallet updates for transaction {txid}")
                    for wallet in affected_wallets:
                        logger.info(f"Updating wallet {wallet} after confirming {txid}")
                        await self._broadcast_wallet_update(wallet)
                
                # Create the task to run in background
                asyncio.create_task(delayed_wallet_update())
            
            # Update all_transactions subscribers
            await self._broadcast_all_transactions_update()
            
            # Check for affected wallets from outputs (for regular confirmed transactions)
            affected_wallets = set()
            
            # Try new structure first
            if transaction.get('sender'):
                affected_wallets.add(transaction.get('sender'))
            if transaction.get('receiver'):
                affected_wallets.add(transaction.get('receiver'))
            
            # Fall back to old structure if needed
            if not affected_wallets:
                for output in tx_data.get('outputs', []):
                    sender = output.get('sender')
                    receiver = output.get('receiver')
                    if sender:
                        affected_wallets.add(sender)
                    if receiver:
                        affected_wallets.add(receiver)
            
            # Update each affected wallet
            for wallet in affected_wallets:
                await self._broadcast_wallet_update(wallet)
                
        except Exception as e:
            logger.error(f"Error handling transaction confirmed: {e}")
    
    async def handle_transaction_pending(self, event: Event):
        """Handle pending transaction events"""
        try:
            tx_data = event.data
            txid = tx_data.get('txid')
            transaction = tx_data.get('transaction')
            
            logger.info(f"Processing pending transaction: {txid}")
            
            # Broadcast mempool transaction to relevant subscribers
            mempool_msg = {
                "type": "mempool_transaction",
                "transaction": {
                    "id": txid,
                    "hash": txid,
                    "sender": tx_data.get('sender'),
                    "receiver": tx_data.get('receiver'),
                    "amount": tx_data.get('amount'),
                    "timestamp": datetime.utcnow().isoformat(),
                    "isMempool": True,
                    "isPending": True
                }
            }
            
            # Collect affected wallets
            affected_wallets = set()
            if tx_data.get('sender'):
                affected_wallets.add(tx_data.get('sender'))
            if tx_data.get('receiver'):
                affected_wallets.add(tx_data.get('receiver'))
            
            # Broadcast mempool transaction to affected wallet subscribers
            for wallet in affected_wallets:
                # Send targeted message to wallet subscribers
                await self.websocket_manager.broadcast(mempool_msg, "mempool_transaction", wallet)
            
            logger.info(f"Broadcasted mempool transaction {txid} to affected wallets: {affected_wallets}")
            
            # Also trigger wallet balance updates
            for wallet in affected_wallets:
                await self._broadcast_wallet_update(wallet)
            
        except Exception as e:
            logger.error(f"Error handling pending transaction: {e}")
    
    async def handle_block_added(self, event: Event):
        """Handle new block events"""
        try:
            block_data = event.data
            height = block_data.get('height')
            
            logger.info(f"New block added at height: {height}")
            
            # Update L1 proofs subscribers
            await self._broadcast_l1_proofs_update()
            
        except Exception as e:
            logger.error(f"Error handling block added: {e}")
    
    async def handle_wallet_balance_changed(self, event: Event):
        """Handle wallet balance change events"""
        try:
            wallet_address = event.data.get('wallet_address')
            logger.info(f"Handling wallet balance change event for: {wallet_address}")
            
            if wallet_address:
                await self._broadcast_wallet_update(wallet_address)
            else:
                logger.warning("Wallet balance change event missing wallet_address")
                
        except Exception as e:
            logger.error(f"Error handling wallet balance change: {e}")
    
    async def _broadcast_all_transactions_update(self):
        """Broadcast update to all_transactions subscribers"""
        try:
            db = get_db()
            formatted = []
            
            for key, value in db.items():
                key_text = key.decode("utf-8")
                if not key_text.startswith("utxo:"):
                    continue
                
                try:
                    utxo = json.loads(value.decode("utf-8"))
                    txid = utxo["txid"]
                    sender = utxo["sender"]
                    receiver = utxo["receiver"]
                    amount = Decimal(utxo["amount"])
                    
                    # Skip change outputs (self-to-self)
                    if sender == receiver:
                        continue
                    
                    # For genesis transactions (empty sender), set sender to "GENESIS"
                    if sender == "":
                        sender = "GENESIS"
                    
                    # Get timestamp
                    tx_data_raw = db.get(f"tx:{txid}".encode())
                    if tx_data_raw:
                        tx_data = json.loads(tx_data_raw.decode())
                        ts = tx_data.get("timestamp", 0)
                    else:
                        ts = 0
                    
                    timestamp_iso = datetime.fromtimestamp(ts / 1000).isoformat() if ts else datetime.utcnow().isoformat()
                    
                    formatted.append({
                        "id": txid,
                        "hash": txid,
                        "sender": sender,
                        "receiver": receiver,
                        "amount": f"{amount:.8f} qBTC",
                        "timestamp": timestamp_iso,
                        "status": "confirmed",
                        "_sort_ts": ts
                    })
                    
                except Exception as e:
                    logger.debug(f"Skipping UTXO: {e}")
                    continue
            
            # Sort by timestamp
            formatted.sort(key=lambda x: x["_sort_ts"], reverse=True)
            
            # Remove internal field
            for tx in formatted:
                tx.pop("_sort_ts", None)
            
            update_data = {
                "type": "transaction_update",
                "transactions": formatted,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            logger.info(f"Broadcasting {len(formatted)} transactions to all_transactions subscribers")
            if formatted:
                logger.info(f"First transaction: {formatted[0]}")
            await self.websocket_manager.broadcast(update_data, "all_transactions")
            
        except Exception as e:
            logger.error(f"Error broadcasting all transactions: {e}")
    
    async def _broadcast_wallet_update(self, wallet_address: str):
        """Broadcast update for specific wallet"""
        try:
            from web.web import get_balance, get_transactions
            from state.state import mempool_manager
            
            logger.info(f"Broadcasting wallet update for: {wallet_address}")
            
            # Safely log mempool state
            if mempool_manager is not None:
                mempool_txs = mempool_manager.get_all_transactions()
                if mempool_txs is not None:
                    logger.info(f"Current mempool before update: {list(mempool_txs.keys())}")
                else:
                    logger.warning("Mempool manager returned None for get_all_transactions()")
            else:
                logger.warning("Mempool manager is None")
            
            balance = get_balance(wallet_address)
            transactions = get_transactions(wallet_address, include_coinbase=False)
            
            logger.info(f"Wallet {wallet_address} - Balance: {balance}, Transactions: {len(transactions)}")
            
            formatted = []
            for tx in transactions:
                tx_type = "send" if tx["direction"] == "sent" else "receive"
                amt_dec = Decimal(tx["amount"])
                amount_fmt = f"{abs(amt_dec):.8f} qBTC"
                address = tx["counterpart"] if tx["counterpart"] else "n/a"
                
                # Check if this is a genesis transaction
                # Genesis transactions have either txid "genesis_tx" or counterpart "bqs1genesis..."
                if tx["txid"] == "genesis_tx" or tx["counterpart"] == "bqs1genesis00000000000000000000000000000000":
                    timestamp_str = "Genesis Block"
                    logger.info(f"Setting Genesis Block timestamp for tx {tx['txid']}")
                else:
                    timestamp_str = datetime.fromtimestamp(tx["timestamp"] / 1000).isoformat() if tx["timestamp"] else "Unknown"
                
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
            
            update_data = {
                "type": "combined_update",
                "balance": f"{balance:.8f}",
                "transactions": formatted
            }
            
            # Check if data actually changed
            cached = self.wallet_cache.get(wallet_address)
            if cached != update_data:
                self.wallet_cache[wallet_address] = update_data
                await self.websocket_manager.broadcast(
                    update_data,
                    "combined_update",
                    wallet_address
                )
                logger.debug(f"Broadcasted update for wallet: {wallet_address}")
            
        except Exception as e:
            logger.error(f"Error broadcasting wallet update: {e}")
    
    async def _broadcast_l1_proofs_update(self):
        """Broadcast L1 proofs update"""
        try:
            db = get_db()
            proofs = {}
            
            for key, value in db.items():
                if key.startswith(b"block:"):
                    block = json.loads(value.decode())
                    tx_ids = block["tx_ids"]
                    proofs[block["height"]] = {
                        "blockHeight": block["height"],
                        "merkleRoot": block["block_hash"],
                        "bitcoinTxHash": None,
                        "timestamp": datetime.fromtimestamp(block["timestamp"] / 1000).isoformat(),
                        "transactions": [
                            {"id": tx_id, "hash": tx_id, "status": "confirmed"}
                            for tx_id in tx_ids
                        ],
                        "status": "confirmed"
                    }
            
            update_data = {
                "type": "l1proof_update",
                "proofs": list(proofs.values()),
                "timestamp": datetime.now().isoformat()
            }
            
            await self.websocket_manager.broadcast(update_data, "l1_proofs_testnet")
            
        except Exception as e:
            logger.error(f"Error broadcasting L1 proofs: {e}")
    
    def register_handlers(self, event_bus):
        """Register all event handlers"""
        event_bus.subscribe(EventTypes.TRANSACTION_CONFIRMED, self.handle_transaction_confirmed)
        event_bus.subscribe(EventTypes.TRANSACTION_PENDING, self.handle_transaction_pending)
        event_bus.subscribe(EventTypes.BLOCK_ADDED, self.handle_block_added)
        event_bus.subscribe(EventTypes.WALLET_BALANCE_CHANGED, self.handle_wallet_balance_changed)
        logger.info("WebSocket event handlers registered")