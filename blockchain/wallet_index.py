"""
Wallet Index Manager - Efficient wallet data indexing and retrieval
Provides O(1) lookups for wallet balances, transactions, and UTXOs
"""

import json
import logging
import time
from typing import Dict, List, Set, Optional, Tuple
from decimal import Decimal
from database.database import get_db

logger = logging.getLogger(__name__)

class WalletIndexManager:
    """
    Manages efficient indexes for wallet operations:
    - wallet_balance:{address} -> pre-computed balance
    - wallet_tx:{address} -> sorted list of transaction IDs
    - wallet_utxo_set:{address} -> set of UTXO keys
    - tx_by_sender:{address} -> transactions sent by address
    - tx_by_receiver:{address} -> transactions received by address
    """
    
    def __init__(self, db=None):
        self.db = db if db is not None else get_db()
        self._cache = {}  # Simple in-memory cache
        self._cache_ttl = 60  # Cache for 60 seconds
        
    def get_wallet_balance(self, address: str) -> Decimal:
        """Get wallet balance in O(1) time."""
        # Check cache first
        cache_key = f"balance:{address}"
        if cache_key in self._cache:
            cached_data, timestamp = self._cache[cache_key]
            if time.time() - timestamp < self._cache_ttl:
                return cached_data
        
        # Try pre-computed balance
        balance_key = f"wallet_balance:{address}".encode()
        balance_data = self.db.get(balance_key)
        
        if balance_data:
            balance = Decimal(balance_data.decode())
            self._cache[cache_key] = (balance, time.time())
            return balance
        
        # If no pre-computed balance, calculate from UTXO set
        balance = self._calculate_balance_from_utxos(address)
        
        # Store for next time using WriteBatch for atomicity
        from rocksdict import WriteBatch
        batch = WriteBatch()
        batch.put(balance_key, str(balance).encode())
        self.db.write(batch)
        self._cache[cache_key] = (balance, time.time())
        
        return balance
    
    def _calculate_balance_from_utxos(self, address: str) -> Decimal:
        """Calculate balance from UTXO set (O(U) where U = user's UTXOs)."""
        total = Decimal("0")
        
        # Use UTXO set index
        utxo_set_key = f"wallet_utxo_set:{address}".encode()
        utxo_set_data = self.db.get(utxo_set_key)
        
        if utxo_set_data:
            utxo_keys = json.loads(utxo_set_data.decode())
            for utxo_key in utxo_keys:
                utxo_data = self.db.get(utxo_key.encode())
                if utxo_data:
                    utxo = json.loads(utxo_data.decode())
                    if not utxo.get("spent", False):
                        total += Decimal(utxo.get("amount", "0"))
        
        return total
    
    def get_wallet_transactions(self, address: str, limit: int = 50, offset: int = 0) -> List[Dict]:
        """Get wallet transactions in O(T) time where T = number of transactions."""
        # Use wallet transaction index
        tx_list_key = f"wallet_tx:{address}".encode()
        tx_list_data = self.db.get(tx_list_key)
        
        if not tx_list_data:
            return []
        
        tx_ids = json.loads(tx_list_data.decode())
        
        # Get requested page of transactions
        start = offset
        end = min(offset + limit, len(tx_ids))
        
        transactions = []
        for txid in tx_ids[start:end]:
            tx_key = f"tx:{txid}".encode()
            tx_data = self.db.get(tx_key)
            if tx_data:
                tx = json.loads(tx_data.decode())
                transactions.append(tx)
        
        return transactions
    
    def get_wallet_utxos(self, address: str) -> List[Dict]:
        """Get wallet's unspent UTXOs in O(U) time."""
        utxos = []
        
        # Use UTXO set index
        utxo_set_key = f"wallet_utxo_set:{address}".encode()
        utxo_set_data = self.db.get(utxo_set_key)
        
        if utxo_set_data:
            utxo_keys = json.loads(utxo_set_data.decode())
            for utxo_key in utxo_keys:
                utxo_data = self.db.get(utxo_key.encode())
                if utxo_data:
                    utxo = json.loads(utxo_data.decode())
                    if not utxo.get("spent", False):
                        utxos.append(utxo)
        
        return utxos
    
    def update_for_new_transaction(self, tx: Dict, batch=None):
        """Update indexes when a new transaction is added."""
        txid = tx.get("txid")
        if not txid:
            return
        
        use_batch = batch is not None
        if not use_batch:
            from rocksdict import WriteBatch
            batch = WriteBatch()
        
        # Track affected addresses for balance updates
        affected_addresses = set()
        
        # First try to use the direct sender field if available
        direct_sender = tx.get("sender")
        if direct_sender and direct_sender not in ["coinbase", "", "bqs1genesis00000000000000000000000000000000"]:
            affected_addresses.add(direct_sender)
            
            # Add to sender's transaction list
            self._add_to_tx_list(direct_sender, txid, batch)
            
            # Add to tx_by_sender index
            sender_tx_key = f"tx_by_sender:{direct_sender}".encode()
            sender_txs = self._get_or_create_list(sender_tx_key)
            if txid not in sender_txs:
                sender_txs.append(txid)
                batch.put(sender_tx_key, json.dumps(sender_txs).encode())
        else:
            # Fallback to deriving sender from spent UTXOs
            # Process inputs (spending UTXOs)
            for inp in tx.get("inputs", []):
                if inp.get("txid") == "0" * 64:  # Skip coinbase
                    continue
                    
                # Mark UTXO as spent
                utxo_key = f"utxo:{inp['txid']}:{inp.get('utxo_index', 0)}"
                utxo_data = self.db.get(utxo_key.encode())
                
                if utxo_data:
                    utxo = json.loads(utxo_data.decode())
                    sender_address = utxo.get("receiver")  # The receiver of the UTXO is the sender of this tx
                    
                    if sender_address:
                        affected_addresses.add(sender_address)
                        
                        # Add to sender's transaction list
                        self._add_to_tx_list(sender_address, txid, batch)
                        
                        # Add to tx_by_sender index
                        sender_tx_key = f"tx_by_sender:{sender_address}".encode()
                        sender_txs = self._get_or_create_list(sender_tx_key)
                        if txid not in sender_txs:
                            sender_txs.append(txid)
                            batch.put(sender_tx_key, json.dumps(sender_txs).encode())
        
        # Also check for direct receiver field in transaction
        direct_receiver = tx.get("receiver")
        if direct_receiver and direct_receiver not in ["coinbase", "", "bqs1genesis00000000000000000000000000000000"]:
            affected_addresses.add(direct_receiver)
            
            # Add to receiver's transaction list
            self._add_to_tx_list(direct_receiver, txid, batch)
            
            # Add to tx_by_receiver index
            receiver_tx_key = f"tx_by_receiver:{direct_receiver}".encode()
            receiver_txs = self._get_or_create_list(receiver_tx_key)
            if txid not in receiver_txs:
                receiver_txs.append(txid)
                batch.put(receiver_tx_key, json.dumps(receiver_txs).encode())
        
        # Process outputs (creating UTXOs)
        for idx, out in enumerate(tx.get("outputs", [])):
            receiver_address = out.get("receiver")
            
            if receiver_address and receiver_address not in ["coinbase", "", "bqs1genesis00000000000000000000000000000000"]:
                affected_addresses.add(receiver_address)
                
                # Add to receiver's transaction list  
                self._add_to_tx_list(receiver_address, txid, batch)
                
                # Add to tx_by_receiver index
                receiver_tx_key = f"tx_by_receiver:{receiver_address}".encode()
                receiver_txs = self._get_or_create_list(receiver_tx_key)
                if txid not in receiver_txs:
                    receiver_txs.append(txid)
                    batch.put(receiver_tx_key, json.dumps(receiver_txs).encode())
                
                # Update UTXO set
                utxo_key = f"utxo:{txid}:{idx}"
                utxo_set_key = f"wallet_utxo_set:{receiver_address}".encode()
                utxo_set = self._get_or_create_list(utxo_set_key)
                if utxo_key not in utxo_set:
                    utxo_set.append(utxo_key)
                    batch.put(utxo_set_key, json.dumps(utxo_set).encode())
        
        # Update balances for affected addresses
        for address in affected_addresses:
            self._invalidate_balance(address, batch)
        
        if not use_batch:
            self.db.write(batch)
    
    def _add_to_tx_list(self, address: str, txid: str, batch):
        """Add transaction to wallet's transaction list."""
        tx_list_key = f"wallet_tx:{address}".encode()
        tx_list = self._get_or_create_list(tx_list_key)
        
        if txid not in tx_list:
            tx_list.append(txid)
            # Keep list sorted by adding timestamp if available
            batch.put(tx_list_key, json.dumps(tx_list).encode())
    
    def _get_or_create_list(self, key: bytes) -> List:
        """Get a list from database or create empty one."""
        data = self.db.get(key)
        if data:
            return json.loads(data.decode())
        return []
    
    def _invalidate_balance(self, address: str, batch):
        """Invalidate cached balance for an address."""
        # Remove from cache
        cache_key = f"balance:{address}"
        if cache_key in self._cache:
            del self._cache[cache_key]
        
        # Remove pre-computed balance (will be recalculated on next access)
        balance_key = f"wallet_balance:{address}".encode()
        if balance_key in self.db:
            batch.delete(balance_key)
    
    def rebuild_indexes(self, progress_callback=None):
        """Rebuild all wallet indexes from scratch - AVOID CALLING THIS!"""
        logger.warning("FULL REBUILD of wallet indexes requested - this is O(N) operation!")

        # Check if indexes already exist - avoid rebuild if possible
        test_key = b"wallet_tx:"
        index_exists = False
        check_count = 0
        for key, _ in self.db.items():
            if key.startswith(test_key):
                index_exists = True
                break
            check_count += 1
            if check_count > 100:  # Quick check only
                break

        if index_exists:
            logger.info("Wallet indexes already exist, skipping rebuild")
            return

        logger.info("Rebuilding wallet indexes (one-time initialization)...")
        start_time = time.time()

        from rocksdict import WriteBatch
        batch = WriteBatch()

        # Clear existing indexes
        self._clear_indexes(batch)

        # Track all addresses and their data
        wallet_data = {}
        tx_count = 0
        utxo_count = 0
        max_tx_scan = 50000  # Limit transaction scan
        max_utxo_scan = 100000  # Limit UTXO scan

        # First pass: collect recent transactions only (not ALL)
        for key, value in self.db.items():
            if key.startswith(b"tx:"):
                tx_count += 1
                if tx_count > max_tx_scan:
                    logger.warning(f"Reached tx scan limit of {max_tx_scan}, stopping")
                    break

                tx = json.loads(value.decode())
                txid = tx.get("txid")

                if not txid:
                    continue

                # Process transaction for indexes
                self.update_for_new_transaction(tx, batch)

                if progress_callback and tx_count % 1000 == 0:
                    progress_callback(f"Processed {tx_count} transactions")

        # Second pass: rebuild UTXO sets and balances (limited)
        for key, value in self.db.items():
            if key.startswith(b"utxo:"):
                utxo_count += 1
                if utxo_count > max_utxo_scan:
                    logger.warning(f"Reached UTXO scan limit of {max_utxo_scan}, stopping")
                    break

                utxo = json.loads(value.decode())

                if not utxo.get("spent", False):
                    receiver = utxo.get("receiver")
                    if receiver:
                        # Add to UTXO set
                        utxo_set_key = f"wallet_utxo_set:{receiver}".encode()
                        utxo_set = self._get_or_create_list(utxo_set_key)
                        utxo_key = key.decode()
                        if utxo_key not in utxo_set:
                            utxo_set.append(utxo_key)
                            batch.put(utxo_set_key, json.dumps(utxo_set).encode())

                if progress_callback and utxo_count % 10000 == 0:
                    progress_callback(f"Processed {utxo_count} UTXOs")
        
        # Write all updates
        self.db.write(batch)
        
        elapsed = time.time() - start_time
        logger.info(f"Wallet indexes rebuilt in {elapsed:.2f}s")
        logger.info(f"Indexed {tx_count} transactions and {utxo_count} UTXOs")
        
        if progress_callback:
            progress_callback(f"Complete: {tx_count} transactions, {utxo_count} UTXOs indexed")
    
    def _clear_indexes(self, batch):
        """Clear all wallet indexes."""
        prefixes_to_clear = [
            b"wallet_balance:",
            b"wallet_tx:",
            b"wallet_utxo_set:",
            b"tx_by_sender:",
            b"tx_by_receiver:"
        ]
        
        for key, _ in self.db.items():
            for prefix in prefixes_to_clear:
                if key.startswith(prefix):
                    batch.delete(key)
                    break
    
    def get_statistics(self) -> Dict:
        """Get statistics about indexes."""
        stats = {
            "wallet_balances": 0,
            "wallet_tx_lists": 0,
            "wallet_utxo_sets": 0,
            "tx_by_sender": 0,
            "tx_by_receiver": 0,
            "cache_size": len(self._cache)
        }
        
        for key, _ in self.db.items():
            if key.startswith(b"wallet_balance:"):
                stats["wallet_balances"] += 1
            elif key.startswith(b"wallet_tx:"):
                stats["wallet_tx_lists"] += 1
            elif key.startswith(b"wallet_utxo_set:"):
                stats["wallet_utxo_sets"] += 1
            elif key.startswith(b"tx_by_sender:"):
                stats["tx_by_sender"] += 1
            elif key.startswith(b"tx_by_receiver:"):
                stats["tx_by_receiver"] += 1
        
        return stats

# Global singleton instance
_wallet_index_instance = None

def get_wallet_index() -> WalletIndexManager:
    """Get the singleton WalletIndexManager instance."""
    global _wallet_index_instance
    if _wallet_index_instance is None:
        _wallet_index_instance = WalletIndexManager()
    return _wallet_index_instance