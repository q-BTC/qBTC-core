"""
Transaction validation module for qBTC-core
Provides comprehensive validation for transactions before block acceptance
"""

import json
import time
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Set, Tuple, Optional
import logging

from wallet.wallet import verify_transaction
from config.config import CHAIN_ID, TX_EXPIRATION_TIME, ADMIN_ADDRESS, GENESIS_ADDRESS

logger = logging.getLogger(__name__)


class TransactionValidator:
    """Validates transactions according to consensus rules"""
    
    def __init__(self, db):
        self.db = db
        self.skip_time_validation = False
    
    def validate_block_transactions(self, block_data: dict) -> Tuple[bool, Optional[str], Decimal]:
        """
        Validate all transactions in a block before accepting it.
        Returns (is_valid, error_message, total_fees)
        """
        height = block_data.get("height", 0)
        full_transactions = block_data.get("full_transactions", [])
        
        if not full_transactions:
            # No transactions to validate (empty block)
            return True, None, Decimal("0")
        
        # Track spent UTXOs within this block to prevent intra-block double spends
        spent_in_block: Set[str] = set()
        total_fees = Decimal("0")
        has_coinbase = False
        
        for i, tx in enumerate(full_transactions):
            if tx is None:
                continue
                
            # Check if this is a coinbase transaction
            is_coinbase = self._is_coinbase_transaction(tx)
            
            
            if is_coinbase:
                if has_coinbase:
                    return False, "Block contains multiple coinbase transactions", Decimal("0")
                has_coinbase = True
                # Skip detailed validation for coinbase here, will check reward amount later
                continue
            
            # Validate non-coinbase transaction
            is_valid, error, tx_fee = self._validate_transaction(
                tx, height, spent_in_block
            )
            
            if not is_valid:
                return False, error, Decimal("0")
            
            total_fees += tx_fee
        
        return True, None, total_fees
    
    def _is_coinbase_transaction(self, tx: dict) -> bool:
        """Check if a transaction is a coinbase transaction"""
        inputs = tx.get("inputs", [])
        if len(inputs) != 1:
            return False
        
        first_input = inputs[0]
        # Check for coinbase pattern: either txid is all zeros or prev_txid is all zeros
        # Some blocks use txid field, others use prev_txid
        return (first_input.get("txid") == "00" * 32 or 
                first_input.get("prev_txid") == "00" * 32)
    
    def _validate_transaction(self, tx: dict, height: int, 
                            spent_in_block: Set[str]) -> Tuple[bool, Optional[str], Decimal]:
        """
        Validate a single transaction.
        Returns (is_valid, error_message, transaction_fee)
        """
        
        # Validate transaction structure
        if not tx:
            return False, "Invalid transaction format - empty transaction", Decimal("0")
        elif "txid" not in tx:
            return False, "Invalid transaction format - missing txid", Decimal("0")
        
        txid = tx["txid"]
        
        # Get transaction body for signature verification
        body = tx.get("body")
        
        if not body:
            return False, f"Transaction {txid} missing body", Decimal("0")
        
        # Extract and validate message string
        msg_str = body.get("msg_str", "")
        signature = body.get("signature", "")
        pubkey = body.get("pubkey", "")
        
        # Special handling ONLY for genesis distribution
        if body.get("transaction_data") == "initial_distribution" and height == 1:
            # Genesis transaction has special rules
            from_ = GENESIS_ADDRESS
            to_ = ADMIN_ADDRESS
            total_authorized = Decimal("21000000")
        else:
            # ALL OTHER TRANSACTIONS MUST FOLLOW STRICT RULES
            if not msg_str:
                return False, f"Transaction {txid} missing msg_str", Decimal("0")
            
            # Parse and validate message string
            parts = msg_str.split(":")
            
            # MANDATORY: All transactions must have exactly 5 parts including chain ID (relax during sync)
            if len(parts) != 5 and not self.skip_time_validation:
                return False, f"Transaction {txid} invalid format - must have sender:receiver:amount:timestamp:chain_id", Decimal("0")
            elif len(parts) != 5 and self.skip_time_validation:
                # During sync mode, be more lenient with old transaction formats
                logger.warning(f"Sync mode: transaction {txid} has {len(parts)} parts instead of 5, allowing...")
                # Pad missing parts with defaults
                while len(parts) < 5:
                    parts.append("0")  # Default values for missing fields
            
            from_, to_, amount_str, time_str, tx_chain_id = parts
            
            # Validate chain ID (skip during sync for historical blocks)
            if not self.skip_time_validation:
                try:
                    if int(tx_chain_id) != CHAIN_ID:
                        return False, f"Invalid chain ID in tx {txid}: expected {CHAIN_ID}, got {tx_chain_id}", Decimal("0")
                except ValueError:
                    return False, f"Invalid chain ID format in tx {txid}: {tx_chain_id}", Decimal("0")
            
            # Validate timestamp (skip during sync for historical blocks)
            if not self.skip_time_validation:
                try:
                    tx_timestamp = int(time_str)
                    current_time = int(time.time() * 1000)  # Convert to milliseconds
                    tx_age = (current_time - tx_timestamp) / 1000  # Age in seconds
                    
                    if tx_age > TX_EXPIRATION_TIME:
                        return False, f"Transaction {txid} expired: age {tx_age}s > max {TX_EXPIRATION_TIME}s", Decimal("0")
                    
                    # Reject transactions with future timestamps (more than 5 minutes in the future)
                    if tx_age < -300:  # -300 seconds = 5 minutes in the future
                        return False, f"Transaction {txid} has future timestamp: {-tx_age}s in the future", Decimal("0")
                        
                except (ValueError, TypeError):
                    return False, f"Invalid timestamp in tx {txid}: {time_str}", Decimal("0")
            
            try:
                total_authorized = Decimal(amount_str)
            except:
                return False, f"Invalid amount in tx {txid}: {amount_str}", Decimal("0")
        
        # Validate inputs
        inputs = tx.get("inputs", [])
        outputs = tx.get("outputs", [])
        
        total_available = Decimal("0")
        
        for inp in inputs:
            if "txid" not in inp:
                return False, f"Transaction {txid} has invalid input - missing txid", Decimal("0")
            
            # Check for intra-block double spend
            utxo_key = f"{inp['txid']}:{inp.get('utxo_index', 0)}"
            if utxo_key in spent_in_block:
                return False, f"Double spend detected: UTXO {utxo_key} already spent in this block", Decimal("0")
            
            # Check UTXO exists and is unspent
            utxo_db_key = f"utxo:{utxo_key}".encode()
            utxo_data = self.db.get(utxo_db_key)
            
            if not utxo_data:
                return False, f"Transaction {txid} references non-existent UTXO {utxo_key}", Decimal("0")
            
            utxo = json.loads(utxo_data.decode())
            
            if utxo.get("spent", False):
                return False, f"Transaction {txid} tries to spend already spent UTXO {utxo_key}", Decimal("0")
            
            # Verify ownership
            if utxo["receiver"] != from_:
                return False, f"UTXO {utxo_key} not owned by sender {from_}", Decimal("0")
            
            total_available += Decimal(utxo["amount"])
            spent_in_block.add(utxo_key)
        
        # Validate outputs
        total_to_recipient = Decimal("0")
        total_change = Decimal("0")
        total_output = Decimal("0")
        
        for out in outputs:
            recv = out.get("receiver")
            if not recv:
                return False, f"Output missing receiver in tx {txid}", Decimal("0")
            
            try:
                amt = Decimal(out.get("amount", "0"))
            except:
                return False, f"Invalid output amount in tx {txid}", Decimal("0")
            
            if recv == to_:
                total_to_recipient += amt
            elif recv == from_:
                total_change += amt
            elif recv == ADMIN_ADDRESS and from_ != ADMIN_ADDRESS:
                # This could be a fee to admin, but should be validated
                pass
            else:
                # For now, only allow outputs to: recipient, sender (change), or admin (fee)
                return False, f"Unauthorized output to {recv} in tx {txid}", Decimal("0")
            
            total_output += amt
        
        # Calculate required amount including miner fee
        miner_fee = (total_authorized * Decimal("0.001")).quantize(
            Decimal("0.00000001"), rounding=ROUND_DOWN
        )
        grand_total_required = total_authorized + miner_fee
        
        # Check sufficient balance (skip for genesis block initial distribution)
        if height > 1 and grand_total_required > total_available:
            return False, f"Insufficient balance in tx {txid}: available {total_available} < required {grand_total_required}", Decimal("0")
        
        # Verify exact payment amount
        if height > 1 and total_to_recipient != total_authorized:
            return False, f"Invalid tx {txid}: authorized amount {total_authorized} != amount sent to recipient {total_to_recipient}", Decimal("0")
        
        # Verify signature (skip for genesis transaction and during sync mode)
        if height != 1 and not self.skip_time_validation and not verify_transaction(msg_str, signature, pubkey):
            return False, f"Signature verification failed for tx {txid}", Decimal("0")
        
        # Calculate actual transaction fee
        tx_fee = total_available - total_output
        
        return True, None, tx_fee
    
    def validate_coinbase_transaction(self, coinbase_tx: dict, height: int, 
                                    total_fees: Decimal) -> Tuple[bool, Optional[str]]:
        """
        Validate coinbase transaction amount against block reward rules.
        Returns (is_valid, error_message)
        """
        # Calculate block subsidy with halving schedule
        halvings = height // 210000
        if halvings >= 64:
            block_subsidy = Decimal("0")
        else:
            block_subsidy = Decimal("50") / (2 ** halvings) * Decimal("100000000")  # In satoshis
        
        # Maximum allowed coinbase output
        max_coinbase_amount = block_subsidy + total_fees
        
        # Calculate total coinbase output
        total_coinbase_output = Decimal("0")
        for out in coinbase_tx.get("outputs", []):
            try:
                total_coinbase_output += Decimal(out.get("amount", "0"))
            except:
                return False, "Invalid amount in coinbase output"
        
        logger.info(f"Validating coinbase at height {height}: output={total_coinbase_output}, "
                   f"subsidy={block_subsidy}, fees={total_fees}, max={max_coinbase_amount}")
        
        if total_coinbase_output > max_coinbase_amount:
            return False, f"Coinbase output {total_coinbase_output} exceeds maximum allowed {max_coinbase_amount}"
        
        return True, None