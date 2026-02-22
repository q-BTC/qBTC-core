import logging
import time
from typing import Dict, Set, Optional, Tuple, List
from decimal import Decimal
import json
from collections import OrderedDict
from blockchain.utxo_lock import get_utxo_lock_manager

logger = logging.getLogger(__name__)

class MempoolManager:
    """
    Manages the transaction mempool with conflict detection, size limits, and fee prioritization.
    """
    
    def __init__(self, max_size: int = None, max_memory_mb: int = None):
        """
        Initialize the mempool manager.
        
        Args:
            max_size: Maximum number of transactions in mempool (defaults to MAX_MEMPOOL_TRANSACTIONS)
            max_memory_mb: Maximum memory usage in MB (defaults to MAX_MEMPOOL_SIZE_MB)
        """
        from config.config import MAX_MEMPOOL_TRANSACTIONS, MAX_MEMPOOL_SIZE_MB
        
        self.transactions: OrderedDict[str, dict] = OrderedDict()
        self.in_use_utxos: Dict[str, str] = {}  # UTXO key -> txid mapping
        self.tx_fees: Dict[str, Decimal] = {}  # txid -> fee mapping
        self.tx_sizes: Dict[str, int] = {}  # txid -> size in bytes
        self.tx_fee_rates: Dict[str, Decimal] = {}  # txid -> fee rate (fee/byte)
        self.tx_utxo_versions: Dict[str, Dict[str, int]] = {}  # txid -> {utxo_key: version} for optimistic locking
        self.tx_sequence: int = 0  # Global sequence counter for transaction ordering
        self.tx_sequences: Dict[str, int] = {}  # txid -> sequence number
        self.address_txids: Dict[str, Set[str]] = {}  # address -> set of txids (O(1) per-address lookup)
        self.max_size = max_size if max_size is not None else MAX_MEMPOOL_TRANSACTIONS
        self.max_memory_bytes = (max_memory_mb if max_memory_mb is not None else MAX_MEMPOOL_SIZE_MB) * 1024 * 1024
        self.current_memory_usage = 0
        
    def add_transaction(self, tx: dict) -> Tuple[bool, Optional[str]]:
        """
        Add a transaction to the mempool with conflict detection.
        
        Args:
            tx: Transaction dictionary
            
        Returns:
            Tuple of (success, error_message)
        """
        txid = tx.get("txid")
        if not txid:
            return False, "Transaction missing txid"
            
        # Check if transaction already exists
        if txid in self.transactions:
            return False, "Transaction already in mempool"
            
        # CRITICAL: Use UTXO locking to prevent double-spend race conditions
        # Collect UTXOs that need to be locked
        utxos_to_lock = []
        for inp in tx.get("inputs", []):
            if inp.get("txid") == "0" * 64:  # Skip coinbase
                continue
            utxos_to_lock.append((inp['txid'], inp.get('utxo_index', 0)))
        
        # First, calculate transaction size and fee BEFORE locking
        # This avoids holding locks during expensive operations
        try:
            tx_json = json.dumps(tx)
            tx_size = len(tx_json.encode())
            if tx_size == 0:
                return False, "Transaction has zero size"
        except (TypeError, ValueError) as e:
            return False, f"Failed to serialize transaction: {e}"

        tx_fee = self._calculate_fee(tx)
        if tx_size > 0:
            tx_fee_rate = tx_fee / tx_size
        else:
            tx_fee_rate = Decimal("0")

        # Check minimum relay fee
        from config.config import MIN_RELAY_FEE
        if tx_fee < Decimal(str(MIN_RELAY_FEE)):
            return False, f"Transaction fee {tx_fee} below minimum relay fee {MIN_RELAY_FEE}"

        # Check if this transaction has a better fee rate than the lowest in mempool
        if len(self.transactions) >= self.max_size or self.current_memory_usage + tx_size > self.max_memory_bytes:
            # Find lowest fee rate transaction
            lowest_fee_rate_txid = self._get_lowest_fee_rate_txid()
            if lowest_fee_rate_txid:
                lowest_tx_size = self.tx_sizes.get(lowest_fee_rate_txid, 1)
                if lowest_tx_size > 0:
                    lowest_fee_rate = self.tx_fees[lowest_fee_rate_txid] / lowest_tx_size
                else:
                    lowest_fee_rate = Decimal("0")

                if tx_fee_rate <= lowest_fee_rate:
                    return False, f"Transaction fee rate {tx_fee_rate:.8f} too low for full mempool (minimum: {lowest_fee_rate:.8f})"

        # Check size limits (will evict if needed)
        if not self._check_size_limits(tx_size):
            return False, "Mempool size limit exceeded and cannot evict"

        # NOW acquire locks for atomic validation AND addition
        lock_manager = get_utxo_lock_manager()

        try:
            with lock_manager.lock_utxos(utxos_to_lock, timeout=5.0):
                # Now we have exclusive access to these UTXOs
                # Check if they exist and are unspent in the blockchain
                from database.database import get_db
                db = get_db()

                # Version tracking for optimistic locking
                utxo_versions = {}

                for inp in tx.get("inputs", []):
                    if inp.get("txid") == "0" * 64:  # Skip coinbase
                        continue

                    utxo_key = f"utxo:{inp['txid']}:{inp.get('utxo_index', 0)}".encode()
                    utxo_data = db.get(utxo_key)

                    if not utxo_data:
                        return False, f"UTXO {inp['txid']}:{inp.get('utxo_index', 0)} does not exist"

                    utxo = json.loads(utxo_data.decode())
                    if utxo.get("spent", False):
                        return False, f"UTXO {inp['txid']}:{inp.get('utxo_index', 0)} is already spent"

                    # Store UTXO version for optimistic locking
                    utxo_versions[f"{inp['txid']}:{inp.get('utxo_index', 0)}"] = utxo.get("version", 0)

                # Check for double-spend conflicts with other mempool transactions
                conflicting_txids = self._check_conflicts(tx)
                if conflicting_txids:
                    # For now, reject new transaction if it conflicts
                    # TODO: Implement replace-by-fee logic if needed
                    return False, f"Transaction conflicts with existing mempool transactions: {conflicting_txids}"

                # CRITICAL: Add to mempool INSIDE the lock to ensure atomicity
                # This prevents race conditions where another thread could use the same UTXOs

                # Add transaction to mempool
                self.transactions[txid] = tx
                self.tx_fees[txid] = tx_fee
                self.tx_sizes[txid] = tx_size
                self.tx_fee_rates[txid] = tx_fee_rate
                self.current_memory_usage += tx_size

                # Assign sequence number for ordering
                self.tx_sequence += 1
                self.tx_sequences[txid] = self.tx_sequence

                # Mark UTXOs as in-use
                for inp in tx.get("inputs", []):
                    utxo_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
                    self.in_use_utxos[utxo_key] = txid

                # Store UTXO versions for later validation
                self.tx_utxo_versions[txid] = utxo_versions

                # Update per-address index for O(1) lookups
                self._index_transaction_addresses(txid, tx)

                # Update wallet indexes for mempool transaction
                # This ensures transactions show up in wallet views immediately
                from blockchain.wallet_index import get_wallet_index
                from rocksdict import WriteBatch
                wallet_index = get_wallet_index()
                batch = WriteBatch()
                wallet_index.update_for_new_transaction(tx, batch)
                db.write(batch)

                logger.info(f"Added transaction {txid} to mempool. Size: {len(self.transactions)}, Memory: {self.current_memory_usage/1024/1024:.2f}MB, Fee rate: {tx_fee_rate:.8f}")
                return True, None

        except RuntimeError as e:
            return False, f"Failed to acquire UTXO locks: {e}"
        
    def remove_transaction(self, txid: str) -> bool:
        """
        Remove a transaction from the mempool.
        
        Args:
            txid: Transaction ID to remove
            
        Returns:
            True if removed, False if not found
        """
        if txid not in self.transactions:
            return False
            
        tx = self.transactions[txid]
        
        # Free up UTXOs
        for inp in tx.get("inputs", []):
            utxo_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
            if self.in_use_utxos.get(utxo_key) == txid:
                del self.in_use_utxos[utxo_key]
                
        # Remove from address index
        self._unindex_transaction_addresses(txid, tx)

        # Remove transaction
        del self.transactions[txid]
        self.current_memory_usage -= self.tx_sizes.get(txid, 0)
        self.tx_fees.pop(txid, None)
        self.tx_sizes.pop(txid, None)
        self.tx_fee_rates.pop(txid, None)
        self.tx_utxo_versions.pop(txid, None)
        self.tx_sequences.pop(txid, None)
        
        logger.info(f"Removed transaction {txid} from mempool")
        return True
        
    def get_transactions_for_block(self, max_count: int = None) -> List[dict]:
        """
        Get transactions for block template, sorted by fee rate.
        
        Args:
            max_count: Maximum number of transactions to include (defaults to MAX_TRANSACTIONS_PER_BLOCK)
            
        Returns:
            List of transactions sorted by fee rate
        """
        # Use system limit if no specific count provided
        if max_count is None:
            from config.config import MAX_TRANSACTIONS_PER_BLOCK
            max_count = MAX_TRANSACTIONS_PER_BLOCK
        
        # Sort transactions by fee rate (fee per byte)
        sorted_txids = sorted(
            self.transactions.keys(),
            key=lambda txid: self.tx_fees.get(txid, Decimal(0)) / max(self.tx_sizes.get(txid, 1), 1),
            reverse=True
        )
        
        # Build list ensuring no conflicts within the block
        block_txs = []
        block_utxos = set()
        
        for txid in sorted_txids[:max_count]:
            tx = self.transactions[txid]
            
            # Check if any input conflicts with already selected transactions
            has_conflict = False
            for inp in tx.get("inputs", []):
                utxo_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
                if utxo_key in block_utxos:
                    has_conflict = True
                    break
                    
            if not has_conflict:
                block_txs.append(tx)
                # Mark inputs as used in this block
                for inp in tx.get("inputs", []):
                    utxo_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
                    block_utxos.add(utxo_key)
                    
        return block_txs
        
    def remove_confirmed_transactions(self, txids: List[str]):
        """
        Remove confirmed transactions from mempool.
        
        Args:
            txids: List of confirmed transaction IDs
        """
        removed_count = 0
        for txid in txids:
            if self.remove_transaction(txid):
                removed_count += 1
                
        if removed_count > 0:
            logger.info(f"Removed {removed_count} confirmed transactions from mempool")
            
    def get_transaction(self, txid: str) -> Optional[dict]:
        """Get a specific transaction from mempool."""
        return self.transactions.get(txid)
        
    def get_all_transactions(self) -> Dict[str, dict]:
        """Get all transactions in mempool."""
        return dict(self.transactions)
        
    def size(self) -> int:
        """Get current mempool size."""
        return len(self.transactions)
        
    def _check_conflicts(self, tx: dict) -> List[str]:
        """
        Check if transaction conflicts with existing mempool transactions.
        
        Args:
            tx: Transaction to check
            
        Returns:
            List of conflicting transaction IDs
        """
        conflicts = []
        
        for inp in tx.get("inputs", []):
            utxo_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
            if utxo_key in self.in_use_utxos:
                conflicting_txid = self.in_use_utxos[utxo_key]
                if conflicting_txid not in conflicts:
                    conflicts.append(conflicting_txid)
                    
        return conflicts
        
    def _calculate_fee(self, tx: dict) -> Decimal:
        """
        Calculate transaction fee with robust parsing and validation (M5).

        Tries msg_str first, falls back to first output amount.
        Returns the actual calculated fee (caller checks against MIN_RELAY_FEE).
        """
        body = tx.get("body", {})
        msg_str = body.get("msg_str", "")

        # Primary: parse amount from msg_str
        try:
            parts = msg_str.split(":")
            if len(parts) >= 3:
                amount = Decimal(parts[2])
                # Validate the parsed amount
                if amount.is_finite() and amount > 0:
                    return (amount * Decimal("0.001")).quantize(Decimal("0.00000001"))
        except Exception as e:
            logger.warning(f"Failed to parse fee from msg_str: {e}")

        # Fallback: use the first output amount (the payment, not change) for fee estimate
        try:
            outputs = tx.get("outputs", [])
            if outputs:
                amt = Decimal(str(outputs[0].get("amount", "0")))
                if amt.is_finite() and amt > 0:
                    return (amt * Decimal("0.001")).quantize(Decimal("0.00000001"))
        except Exception as e:
            logger.warning(f"Failed to calculate fee from outputs: {e}")

        return Decimal("0")
        
    def _check_size_limits(self, new_tx_size: int) -> bool:
        """
        Check if adding transaction would exceed size limits.
        
        Args:
            new_tx_size: Size of new transaction in bytes
            
        Returns:
            True if within limits, False otherwise
        """
        # Check transaction count limit
        if len(self.transactions) >= self.max_size:
            logger.warning(f"Mempool transaction count limit reached ({self.max_size})")
            # Evict lowest fee rate transaction if possible
            if not self._evict_lowest_fee_transaction():
                return False
            
        # Check memory limit with safety counter to prevent infinite loops
        eviction_attempts = 0
        max_eviction_attempts = 10  # Safety limit
        
        while self.current_memory_usage + new_tx_size > self.max_memory_bytes:
            if eviction_attempts >= max_eviction_attempts:
                logger.error(f"Failed to evict enough transactions after {max_eviction_attempts} attempts")
                return False
                
            logger.warning(f"Mempool memory limit reached ({self.max_memory_bytes/1024/1024:.2f}MB)")
            # Evict lowest fee rate transaction
            if not self._evict_lowest_fee_transaction():
                return False
            eviction_attempts += 1
            
        return True
        
    def _get_lowest_fee_rate_txid(self) -> Optional[str]:
        """
        Get the transaction ID with the lowest fee rate.
        
        Returns:
            Transaction ID with lowest fee rate, or None if mempool is empty
        """
        if not self.transactions:
            return None
            
        # Calculate fee rates if not cached
        for txid in self.transactions:
            if txid not in self.tx_fee_rates:
                tx_size = self.tx_sizes.get(txid, 1)
                if tx_size > 0:
                    self.tx_fee_rates[txid] = self.tx_fees.get(txid, Decimal(0)) / tx_size
                else:
                    self.tx_fee_rates[txid] = Decimal(0)
        
        # Find transaction with lowest fee rate
        return min(self.transactions.keys(), 
                  key=lambda txid: self.tx_fee_rates.get(txid, Decimal(0)))
    
    def _evict_lowest_fee_transaction(self) -> bool:
        """
        Evict the transaction with the lowest fee rate.
        
        Returns:
            True if a transaction was evicted, False if unable to evict
        """
        lowest_txid = self._get_lowest_fee_rate_txid()
        if not lowest_txid:
            return False
            
        logger.info(f"Evicting transaction {lowest_txid} with fee rate {self.tx_fee_rates.get(lowest_txid, 0):.8f}")
        return self.remove_transaction(lowest_txid)
    
    def get_in_use_utxo_keys(self) -> Set[str]:
        """Return the set of UTXO keys currently spent in mempool — O(1)."""
        return set(self.in_use_utxos.keys())

    def is_utxo_in_use(self, utxo_key: str) -> bool:
        """Check if a UTXO is currently spent in mempool — O(1)."""
        return utxo_key in self.in_use_utxos

    def get_transactions_for_address(self, address: str) -> List[dict]:
        """Get mempool transactions involving an address — O(k) where k = address's txs."""
        txids = self.address_txids.get(address, set())
        return [self.transactions[txid] for txid in txids if txid in self.transactions]

    def _index_transaction_addresses(self, txid: str, tx: dict):
        """Add transaction to per-address index."""
        addresses = set()
        # Extract addresses from outputs
        for out in tx.get("outputs", []):
            recv = out.get("receiver")
            if recv:
                addresses.add(recv)
        # Extract sender from msg_str
        body = tx.get("body", {})
        msg_str = body.get("msg_str", "")
        if msg_str:
            parts = msg_str.split(":")
            if len(parts) >= 2:
                addresses.add(parts[0])  # sender
                addresses.add(parts[1])  # receiver
        for addr in addresses:
            if addr not in self.address_txids:
                self.address_txids[addr] = set()
            self.address_txids[addr].add(txid)

    def _unindex_transaction_addresses(self, txid: str, tx: dict):
        """Remove transaction from per-address index."""
        addresses = set()
        for out in tx.get("outputs", []):
            recv = out.get("receiver")
            if recv:
                addresses.add(recv)
        body = tx.get("body", {})
        msg_str = body.get("msg_str", "")
        if msg_str:
            parts = msg_str.split(":")
            if len(parts) >= 2:
                addresses.add(parts[0])
                addresses.add(parts[1])
        for addr in addresses:
            if addr in self.address_txids:
                self.address_txids[addr].discard(txid)
                if not self.address_txids[addr]:
                    del self.address_txids[addr]

    def get_stats(self) -> dict:
        """Get mempool statistics."""
        total_fees = sum(self.tx_fees.values())
        avg_fee = total_fees / len(self.transactions) if self.transactions else Decimal(0)
        
        # Calculate average fee rate
        total_fee_rate = Decimal(0)
        for txid in self.transactions:
            if txid not in self.tx_fee_rates:
                tx_size = self.tx_sizes.get(txid, 1)
                if tx_size > 0:
                    self.tx_fee_rates[txid] = self.tx_fees.get(txid, Decimal(0)) / tx_size
                else:
                    self.tx_fee_rates[txid] = Decimal(0)
            total_fee_rate += self.tx_fee_rates[txid]
        avg_fee_rate = total_fee_rate / len(self.transactions) if self.transactions else Decimal(0)
        
        return {
            "size": len(self.transactions),
            "memory_usage_mb": self.current_memory_usage / 1024 / 1024,
            "total_fees": str(total_fees),
            "average_fee": str(avg_fee),
            "average_fee_rate": str(avg_fee_rate),
            "in_use_utxos": len(self.in_use_utxos),
            "max_size": self.max_size,
            "max_memory_mb": self.max_memory_bytes / 1024 / 1024
        }