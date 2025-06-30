import logging
import time
from typing import Dict, Set, Optional, Tuple, List
from decimal import Decimal
import json
from collections import OrderedDict

logger = logging.getLogger(__name__)

class MempoolManager:
    """
    Manages the transaction mempool with conflict detection, size limits, and fee prioritization.
    """
    
    def __init__(self, max_size: int = 5000, max_memory_mb: int = 100):
        """
        Initialize the mempool manager.
        
        Args:
            max_size: Maximum number of transactions in mempool
            max_memory_mb: Maximum memory usage in MB
        """
        self.transactions: OrderedDict[str, dict] = OrderedDict()
        self.in_use_utxos: Dict[str, str] = {}  # UTXO key -> txid mapping
        self.tx_fees: Dict[str, Decimal] = {}  # txid -> fee mapping
        self.tx_sizes: Dict[str, int] = {}  # txid -> size in bytes
        self.max_size = max_size
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
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
            
        # Check for double-spend conflicts
        conflicting_txids = self._check_conflicts(tx)
        if conflicting_txids:
            # For now, reject new transaction if it conflicts
            # TODO: Implement replace-by-fee logic if needed
            return False, f"Transaction conflicts with existing mempool transactions: {conflicting_txids}"
            
        # Calculate transaction size and fee
        tx_size = len(json.dumps(tx).encode())
        tx_fee = self._calculate_fee(tx)
        
        # Check size limits
        if not self._check_size_limits(tx_size):
            return False, "Mempool size limit exceeded"
            
        # Add transaction to mempool
        self.transactions[txid] = tx
        self.tx_fees[txid] = tx_fee
        self.tx_sizes[txid] = tx_size
        self.current_memory_usage += tx_size
        
        # Mark UTXOs as in-use
        for inp in tx.get("inputs", []):
            utxo_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
            self.in_use_utxos[utxo_key] = txid
            
        logger.info(f"Added transaction {txid} to mempool. Size: {len(self.transactions)}, Memory: {self.current_memory_usage/1024/1024:.2f}MB")
        return True, None
        
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
                
        # Remove transaction
        del self.transactions[txid]
        self.current_memory_usage -= self.tx_sizes.get(txid, 0)
        self.tx_fees.pop(txid, None)
        self.tx_sizes.pop(txid, None)
        
        logger.info(f"Removed transaction {txid} from mempool")
        return True
        
    def get_transactions_for_block(self, max_count: int = 1000) -> List[dict]:
        """
        Get transactions for block template, sorted by fee rate.
        
        Args:
            max_count: Maximum number of transactions to include
            
        Returns:
            List of transactions sorted by fee rate
        """
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
        Calculate transaction fee.
        
        Args:
            tx: Transaction
            
        Returns:
            Fee amount
        """
        # For qBTC, fee is 0.1% of transaction amount
        # We need to calculate from the message string
        body = tx.get("body", {})
        msg_str = body.get("msg_str", "")
        
        try:
            parts = msg_str.split(":")
            if len(parts) >= 3:
                amount = Decimal(parts[2])
                fee = (amount * Decimal("0.001")).quantize(Decimal("0.00000001"))
                return fee
        except Exception as e:
            logger.warning(f"Failed to calculate fee for transaction: {e}")
            
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
            # TODO: Implement eviction of lowest fee transactions
            logger.warning("Mempool transaction count limit reached")
            return False
            
        # Check memory limit
        if self.current_memory_usage + new_tx_size > self.max_memory_bytes:
            logger.warning("Mempool memory limit reached")
            return False
            
        return True
        
    def get_stats(self) -> dict:
        """Get mempool statistics."""
        total_fees = sum(self.tx_fees.values())
        avg_fee = total_fees / len(self.transactions) if self.transactions else Decimal(0)
        
        return {
            "size": len(self.transactions),
            "memory_usage_mb": self.current_memory_usage / 1024 / 1024,
            "total_fees": str(total_fees),
            "average_fee": str(avg_fee),
            "in_use_utxos": len(self.in_use_utxos)
        }