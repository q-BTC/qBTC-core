"""
On-chain commitment transaction handler for BTC to qBTC address binding
"""

import json
import logging
import hashlib
from typing import Dict, Optional, Tuple
from decimal import Decimal
from utils.bitcoin_utils import verify_bitcoin_signature
from blockchain.blockchain import sha256d

logger = logging.getLogger(__name__)

class CommitmentTransactionHandler:
    """Handles creation and validation of on-chain commitment transactions"""
    
    COMMITMENT_FEE = Decimal("0.001")  # Small fee to prevent spam
    
    @staticmethod
    def create_commitment_transaction(
        btc_address: str,
        qbtc_address: str,
        message: str,
        signature: str,
        chain_id: int
    ) -> Dict:
        """
        Create a commitment transaction for on-chain storage.
        
        Args:
            btc_address: Bitcoin address to commit
            qbtc_address: qBTC address to bind to
            message: Original signed message
            signature: Bitcoin signature proving ownership
            chain_id: Chain ID for replay protection
            
        Returns:
            Commitment transaction dict
        """
        import time
        
        # Create unique transaction ID
        commitment_data = f"{btc_address}:{qbtc_address}:{message}:{signature}:{int(time.time())}"
        txid = sha256d(commitment_data.encode())[::-1].hex()
        
        # Create the commitment transaction
        commitment_tx = {
            "type": "commitment",
            "txid": txid,
            "inputs": [],  # Commitments don't need inputs
            "outputs": [{
                "type": "commitment_output",
                "btc_address": btc_address,
                "qbtc_address": qbtc_address,
                "amount": "0",  # No value transfer
                "commitment_data": {
                    "message": message,
                    "signature": signature,
                    "timestamp": int(time.time())
                }
            }],
            "body": {
                "msg_str": f"commitment:{btc_address}:{qbtc_address}:{int(time.time())}:{chain_id}",
                "signature": signature,
                "pubkey": "",  # Not needed for BTC commitments
                "btc_message": message  # Original message for verification
            },
            "timestamp": int(time.time() * 1000),
            "fee": str(CommitmentTransactionHandler.COMMITMENT_FEE)
        }
        
        return commitment_tx
    
    @staticmethod
    def validate_commitment_transaction(tx: Dict, db) -> Tuple[bool, Optional[str]]:
        """
        Validate a commitment transaction.
        
        Args:
            tx: Transaction to validate
            db: Database connection
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Check transaction type
            if tx.get("type") != "commitment":
                return True, None  # Not a commitment, skip validation
            
            # Check required fields
            if not tx.get("outputs") or len(tx["outputs"]) != 1:
                return False, "Commitment transaction must have exactly one output"
            
            output = tx["outputs"][0]
            if output.get("type") != "commitment_output":
                return False, "Invalid commitment output type"
            
            btc_address = output.get("btc_address")
            qbtc_address = output.get("qbtc_address")
            commitment_data = output.get("commitment_data", {})
            
            if not btc_address or not qbtc_address:
                return False, "Missing BTC or qBTC address in commitment"
            
            # Verify Bitcoin signature
            message = commitment_data.get("message", "")
            signature = commitment_data.get("signature", "")
            
            if not verify_bitcoin_signature(message, signature, btc_address):
                return False, "Invalid Bitcoin signature for commitment"
            
            # Check if this Bitcoin address is already committed (on-chain)
            # This prevents double-spending of commitments
            commitment_key = f"commitment_onchain:{btc_address}".encode()
            existing = db.get(commitment_key)
            
            if existing:
                existing_data = json.loads(existing.decode())
                if existing_data.get("qbtc_address") != qbtc_address:
                    return False, f"Bitcoin address {btc_address} already committed to different qBTC address"
            
            return True, None
            
        except Exception as e:
            logger.error(f"Error validating commitment transaction: {e}")
            return False, str(e)
    
    @staticmethod
    def process_commitment_transaction(tx: Dict, db, block_height: int) -> bool:
        """
        Process a validated commitment transaction when it's included in a block.
        
        Args:
            tx: Commitment transaction
            db: Database connection
            block_height: Height of the block containing this transaction
            
        Returns:
            Success boolean
        """
        try:
            if tx.get("type") != "commitment":
                return True  # Not a commitment
            
            output = tx["outputs"][0]
            btc_address = output["btc_address"]
            qbtc_address = output["qbtc_address"]
            commitment_data = output["commitment_data"]
            
            # Store the on-chain commitment
            commitment_record = {
                "btc_address": btc_address,
                "qbtc_address": qbtc_address,
                "txid": tx["txid"],
                "block_height": block_height,
                "timestamp": commitment_data["timestamp"],
                "message": commitment_data["message"],
                "signature": commitment_data["signature"],
                "on_chain": True
            }
            
            # Store all commitment updates atomically
            from rocksdict import WriteBatch
            batch = WriteBatch()

            # Store with special key to differentiate from local commitments
            commitment_key = f"commitment_onchain:{btc_address}".encode()
            batch.put(commitment_key, json.dumps(commitment_record).encode())

            # Also store reverse lookup
            reverse_key = f"commitment_onchain_reverse:{qbtc_address}".encode()
            batch.put(reverse_key, btc_address.encode())

            # Update any existing local commitment to mark it as confirmed on-chain
            local_key = f"commitment:{btc_address}".encode()
            local_data = db.get(local_key)
            if local_data:
                local_record = json.loads(local_data.decode())
                local_record["on_chain"] = True
                local_record["on_chain_txid"] = tx["txid"]
                local_record["on_chain_height"] = block_height
                batch.put(local_key, json.dumps(local_record).encode())

            # Write all changes atomically
            db.write(batch)
            
            logger.info(f"Processed on-chain commitment: {btc_address} -> {qbtc_address} at height {block_height}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing commitment transaction: {e}")
            return False
    
    @staticmethod
    def get_commitment_for_bitcoin_address(btc_address: str, db) -> Optional[str]:
        """
        Get the committed qBTC address for a Bitcoin address.
        Checks on-chain commitments first, then local.
        
        Args:
            btc_address: Bitcoin address to look up
            db: Database connection
            
        Returns:
            qBTC address if committed, None otherwise
        """
        # Check on-chain commitment first (highest priority)
        onchain_key = f"commitment_onchain:{btc_address}".encode()
        onchain_data = db.get(onchain_key)
        
        if onchain_data:
            commitment = json.loads(onchain_data.decode())
            return commitment.get("qbtc_address")
        
        # Fall back to local commitment
        local_key = f"commitment:{btc_address}".encode()
        local_data = db.get(local_key)
        
        if local_data:
            commitment = json.loads(local_data.decode())
            return commitment.get("qbtc_address")
        
        return None