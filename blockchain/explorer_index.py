"""
Explorer Transaction Index - Maintains an efficient index of recent user transactions
"""
import json
import logging
import time
from typing import List, Dict, Optional
from decimal import Decimal
from datetime import datetime

logger = logging.getLogger(__name__)

class ExplorerIndex:
    """Maintains an efficient index of recent user transactions for the explorer"""

    def __init__(self, db, max_transactions: int = 1000):
        self.db = db
        self.max_transactions = max_transactions
        self.index_key = b"explorer:recent_txs"
        self._ensure_index()

    def _ensure_index(self):
        """Initialize index if it doesn't exist"""
        if not self.db.get(self.index_key):
            # Skip write in secondary mode (read-only DB)
            from database.database import is_secondary
            if is_secondary():
                logger.info("Explorer index not found (secondary mode, skip init)")
                return
            from rocksdict import WriteBatch
            batch = WriteBatch()
            batch.put(self.index_key, json.dumps([]).encode())
            self.db.write(batch)
            logger.info("Initialized empty explorer index")

    def rebuild_index(self):
        """Rebuild the entire index - ONLY call this for manual recovery, not on startup"""
        logger.warning("FULL REBUILD of explorer index requested - this is O(N) operation!")

        # Check if we really need to rebuild
        existing = self.db.get(self.index_key)
        if existing:
            try:
                current = json.loads(existing.decode())
                if current and len(current) > 0:
                    logger.info(f"Explorer index exists with {len(current)} entries, skipping rebuild")
                    return
            except:
                pass

        logger.info("Rebuilding explorer transaction index...")

        transactions = []
        count = 0
        limit = self.max_transactions * 2  # Scan at most 2x what we need

        # Collect recent transactions only - not ALL transactions
        # This is still O(N) but we limit the scan
        for key, value in self.db.items():
            if not key.startswith(b"tx:"):
                continue

            count += 1
            if count > limit:
                logger.info(f"Scanned {limit} transactions, stopping scan")
                break

            try:
                tx_data = json.loads(value.decode())
                inputs = tx_data.get("inputs", [])

                # Skip coinbase transactions
                if len(inputs) == 1 and inputs[0].get("txid", "") == "0" * 64:
                    continue

                # Get transaction timestamp
                ts = tx_data.get("timestamp", 0)
                if ts:
                    transactions.append({
                        "txid": tx_data.get("txid", ""),
                        "timestamp": ts
                    })
            except Exception as e:
                continue

        # Sort by timestamp (most recent first)
        transactions.sort(key=lambda x: x["timestamp"], reverse=True)

        # Keep only the most recent transactions
        transactions = transactions[:self.max_transactions]

        # Store the index
        from rocksdict import WriteBatch
        batch = WriteBatch()
        batch.put(self.index_key, json.dumps(transactions).encode())
        self.db.write(batch)
        logger.info(f"Explorer index rebuilt with {len(transactions)} transactions")

    def add_transaction(self, txid: str, timestamp: int, is_coinbase: bool = False,
                        sender: str = "", receiver: str = "", amount: str = ""):
        """Add a new transaction to the index with denormalized sender/receiver/amount"""
        if is_coinbase:
            return  # Skip coinbase transactions

        # Get current index
        index_data = self.db.get(self.index_key)
        if index_data:
            transactions = json.loads(index_data.decode())
        else:
            transactions = []

        # Add new transaction at the beginning with denormalized fields
        entry = {"txid": txid, "timestamp": timestamp}
        if sender:
            entry["sender"] = sender
        if receiver:
            entry["receiver"] = receiver
        if amount:
            entry["amount"] = amount

        transactions.insert(0, entry)

        # Trim to max size
        transactions = transactions[:self.max_transactions]

        # Save updated index atomically
        from rocksdict import WriteBatch
        batch = WriteBatch()
        batch.put(self.index_key, json.dumps(transactions).encode())
        self.db.write(batch)

    def get_recent_transactions(self, limit: int = 100) -> List[Dict]:
        """Get recent transactions efficiently using denormalized index data.

        New index entries contain sender/receiver/amount directly, avoiding
        N+1 DB lookups. Falls back to DB lookup for legacy entries only.
        """
        formatted = []

        # Get the index
        index_data = self.db.get(self.index_key)
        if not index_data:
            return []

        transactions = json.loads(index_data.decode())

        # Process only the requested number of transactions
        for tx_ref in transactions[:limit * 2]:  # Get extra to account for filtering
            if len(formatted) >= limit:
                break

            txid = tx_ref["txid"]

            # Fast path: use denormalized data if available
            sender = tx_ref.get("sender", "")
            receiver = tx_ref.get("receiver", "")
            amount_str = tx_ref.get("amount", "")
            ts = tx_ref.get("timestamp", 0)

            if sender and receiver and amount_str:
                # Denormalized entry — no DB lookup needed
                try:
                    amount = Decimal(amount_str)
                except Exception:
                    amount = Decimal("0")
                timestamp_iso = datetime.fromtimestamp(ts / 1000).isoformat() if ts else datetime.utcnow().isoformat()
                formatted.append({
                    "id": txid,
                    "hash": txid,
                    "sender": sender,
                    "receiver": receiver,
                    "amount": f"{amount:.8f} qBTC",
                    "timestamp": timestamp_iso,
                    "status": "confirmed"
                })
                continue

            # Slow path: legacy entry without denormalized fields — do DB lookups
            tx_key = f"tx:{txid}".encode()
            tx_data_raw = self.db.get(tx_key)

            if not tx_data_raw:
                continue

            try:
                tx_data = json.loads(tx_data_raw.decode())
                inputs = tx_data.get("inputs", [])
                outputs = tx_data.get("outputs", [])

                # Try sender field / msg_str before falling back to UTXO lookup
                sender = tx_data.get("sender", "")
                if not sender:
                    body = tx_data.get("body", {})
                    msg_str = body.get("msg_str", "")
                    if msg_str:
                        parts = msg_str.split(":")
                        if parts:
                            sender = parts[0]

                if not sender:
                    # Last resort: UTXO lookup for a single input
                    for inp in inputs:
                        inp_txid = inp.get("txid", "")
                        inp_index = inp.get("utxo_index", 0)
                        if inp_txid and inp_txid != "0" * 64:
                            utxo_key = f"utxo:{inp_txid}:{inp_index}".encode()
                            utxo_data = self.db.get(utxo_key)
                            if utxo_data:
                                utxo = json.loads(utxo_data.decode())
                                sender = utxo.get("receiver", "")
                                break

                if not sender:
                    continue

                # Find the primary recipient
                receiver = ""
                amount = Decimal("0")
                for out in outputs:
                    out_receiver = out.get("receiver", "")
                    out_amount = Decimal(str(out.get("amount", 0)))

                    if out_receiver == "bqs1genesis00000000000000000000000000000000":
                        continue

                    if out_receiver and out_receiver != sender:
                        receiver = out_receiver
                        amount = out_amount
                        break

                if not receiver and outputs:
                    out = outputs[0]
                    receiver = out.get("receiver", "")
                    amount = Decimal(str(out.get("amount", 0)))

                if not receiver:
                    continue

                ts = tx_data.get("timestamp", 0)
                timestamp_iso = datetime.fromtimestamp(ts / 1000).isoformat() if ts else datetime.utcnow().isoformat()

                formatted.append({
                    "id": txid,
                    "hash": txid,
                    "sender": sender,
                    "receiver": receiver,
                    "amount": f"{amount:.8f} qBTC",
                    "timestamp": timestamp_iso,
                    "status": "confirmed"
                })

            except Exception as e:
                logger.debug(f"Error processing transaction {txid}: {e}")
                continue

        return formatted

_explorer_index: Optional[ExplorerIndex] = None

def get_explorer_index(rebuild: bool = False) -> ExplorerIndex:
    """Get or create the explorer index singleton"""
    global _explorer_index

    if _explorer_index is None or rebuild:
        from database.database import get_db
        db = get_db()
        _explorer_index = ExplorerIndex(db)
        if rebuild:
            _explorer_index.rebuild_index()

    return _explorer_index