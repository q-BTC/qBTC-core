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
from database.database import get_db, get_current_height
from utils.executors import run_in_ws_executor

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
            from web.web import get_broadcast_transactions
            
            # Check if transaction broadcasting is enabled
            if not get_broadcast_transactions():
                logger.info(f"Skipping websocket broadcast for confirmed transaction {event.data.get('txid')} - broadcasting disabled")
                return
            
            tx_data = event.data
            txid = tx_data.get('txid')
            transaction = tx_data.get('transaction', {})
            confirmed_from_mempool = tx_data.get('confirmed_from_mempool', False)
            
            # Check if this is a coinbase transaction and skip it
            inputs = transaction.get("inputs", [])
            if len(inputs) == 1 and inputs[0].get("txid", "") == "0" * 64:
                logger.info(f"Skipping coinbase transaction {txid} from websocket broadcast")
                return
            
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

            # Fall back to old structure if needed (extend the existing set)
            if not affected_wallets:
                for output in tx_data.get('outputs', []):
                    sender = output.get('sender')
                    receiver = output.get('receiver')
                    if sender:
                        affected_wallets.add(sender)
                    if receiver:
                        affected_wallets.add(receiver)

            # Update each affected wallet (immediate, for non-mempool confirmed txs)
            if not confirmed_from_mempool:
                for wallet in affected_wallets:
                    await self._broadcast_wallet_update(wallet)
                
        except Exception as e:
            logger.error(f"Error handling transaction confirmed: {e}")
    
    async def handle_transaction_pending(self, event: Event):
        """Handle pending transaction events"""
        try:
            from web.web import get_broadcast_transactions
            
            # Check if transaction broadcasting is enabled
            if not get_broadcast_transactions():
                logger.info(f"Skipping websocket broadcast for pending transaction {event.data.get('txid')} - broadcasting disabled")
                return
            
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
            
            # Broadcast mempool transaction to affected wallet subscribers (via "combined_update" routing)
            for wallet in affected_wallets:
                await self.websocket_manager.broadcast(mempool_msg, "combined_update", wallet)

            # Also broadcast to explorer subscribers (via "all_transactions" routing)
            await self.websocket_manager.broadcast(mempool_msg, "all_transactions")

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
        """Broadcast update to all_transactions subscribers - O(limit) complexity"""
        try:
            from web.web import get_broadcast_transactions
            from blockchain.explorer_index import get_explorer_index

            # Check if transaction broadcasting is enabled
            if not get_broadcast_transactions():
                logger.debug("Skipping all_transactions broadcast - broadcasting disabled")
                return

            # Offload blocking DB read to thread pool
            def _fetch_recent_transactions():
                explorer_index = get_explorer_index()
                return explorer_index.get_recent_transactions(limit=100)

            formatted = await run_in_ws_executor(_fetch_recent_transactions)

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
            from web.web import get_balance, get_transactions, get_broadcast_transactions, _is_web_process

            # Check if transaction broadcasting is enabled
            if not get_broadcast_transactions():
                logger.debug(f"Skipping wallet update broadcast for {wallet_address} - broadcasting disabled")
                return

            logger.info(f"Broadcasting wallet update for: {wallet_address}")

            # Safely log mempool state (skip in web process — no direct mempool_manager)
            if not _is_web_process:
                from state.state import mempool_manager
                if mempool_manager is not None:
                    mempool_txs = mempool_manager.get_all_transactions()
                    if mempool_txs is not None:
                        logger.info(f"Current mempool before update: {list(mempool_txs.keys())}")
                    else:
                        logger.warning("Mempool manager returned None for get_all_transactions()")
                else:
                    logger.warning("Mempool manager is None")

            # Bundle all blocking DB reads into a single sync function for the thread pool
            def _fetch_wallet_data():
                db = get_db()
                balance = get_balance(wallet_address)
                transactions = get_transactions(wallet_address, include_coinbase=False)

                # Read current height directly from DB (sync, since we're in a thread)
                current_height = None
                tip_data = db.get(b"chain:best_tip")
                if tip_data:
                    tip_info = json.loads(tip_data.decode())
                    current_height = tip_info.get("height")

                # Batch fetch all transaction block data
                tx_block_data_cache = {}
                for tx in transactions:
                    txid = tx["txid"]
                    tx_block_key = f"tx_block:{txid}".encode()
                    tx_block_data = db.get(tx_block_key)
                    if tx_block_data:
                        tx_block_data_cache[txid] = json.loads(tx_block_data.decode())

                return balance, transactions, current_height, tx_block_data_cache

            balance, transactions, current_height, tx_block_data_cache = await run_in_ws_executor(_fetch_wallet_data)

            logger.info(f"Wallet {wallet_address} - Balance: {balance}, Transactions: {len(transactions)}")
            if transactions:
                logger.debug(f"First transaction data: {transactions[0]}")

            # Format transactions on the event loop (CPU-only, fast)
            formatted = []
            for tx in transactions:
                tx_type = "sent" if tx["direction"] == "sent" else "received"
                amt_dec = Decimal(tx["amount"])
                amount_fmt = f"{abs(amt_dec):.8f} qBTC"

                txid = tx["txid"]
                from_address = tx.get("from", "Unknown")
                to_address = tx.get("to", "Unknown")

                if tx["direction"] == "sent":
                    address = to_address
                else:
                    address = from_address

                logger.debug(f"Processing tx {txid}: direction={tx['direction']}, from={from_address}, to={to_address}")

                block_height = None
                confirmations = 0

                tx_block = tx_block_data_cache.get(txid)
                if tx_block:
                    block_height = tx_block.get("height")
                    if block_height is not None and current_height is not None:
                        if current_height >= 0 and current_height >= block_height:
                            confirmations = current_height - block_height + 1
                            logger.debug(f"Calculated confirmations for tx {txid}: height={block_height}, current={current_height}, confirmations={confirmations}")

                if tx["txid"] == "genesis_tx" or from_address == "bqs1genesis00000000000000000000000000000000":
                    timestamp_str = "Genesis Block"
                    logger.debug(f"Setting Genesis Block timestamp for tx {tx['txid']}")
                else:
                    timestamp_str = datetime.fromtimestamp(tx["timestamp"] / 1000).isoformat() if tx["timestamp"] else "Unknown"

                formatted.append({
                    "id": tx["txid"],
                    "type": tx_type,
                    "amount": amount_fmt,
                    "address": address,
                    "from_address": from_address,
                    "to_address": to_address,
                    "block_height": block_height,
                    "confirmations": confirmations,
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
        """Broadcast L1 proofs update — O(50) via height index instead of full DB scan"""
        try:
            from blockchain.block_height_index import get_height_index

            # Offload all blocking DB reads to thread pool
            def _fetch_l1_proofs():
                db = get_db()
                height_index = get_height_index()

                # Read current height directly from DB (sync)
                tip_data = db.get(b"chain:best_tip")
                if not tip_data:
                    return None
                tip_info = json.loads(tip_data.decode())
                current_height = tip_info.get("height")
                if current_height is None:
                    return None

                proofs = []
                for h in range(max(0, current_height - 50), current_height + 1):
                    block = height_index.get_block_by_height(h)
                    if not block:
                        continue
                    tx_ids = block.get("tx_ids", [])
                    proofs.append({
                        "blockHeight": block["height"],
                        "merkleRoot": block["block_hash"],
                        "bitcoinTxHash": None,
                        "timestamp": datetime.fromtimestamp(block["timestamp"] / 1000).isoformat(),
                        "transactions": [
                            {"id": tx_id, "hash": tx_id, "status": "confirmed"}
                            for tx_id in tx_ids
                        ],
                        "status": "confirmed"
                    })
                return proofs

            proofs = await run_in_ws_executor(_fetch_l1_proofs)

            if proofs is None:
                logger.warning("Cannot broadcast L1 proofs: current height unknown")
                return

            update_data = {
                "type": "l1proof_update",
                "proofs": proofs,
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
