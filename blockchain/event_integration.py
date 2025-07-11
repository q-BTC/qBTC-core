"""
Integration of event system with blockchain operations
"""

import json
import logging
import asyncio
from typing import Dict, Any

from database.database import get_db
from events.event_bus import event_bus, EventTypes

logger = logging.getLogger(__name__)


async def emit_transaction_event(txid: str, transaction: Dict[str, Any], confirmed: bool = False):
    """Emit transaction-related events"""
    try:
        # Emit transaction event
        event_type = EventTypes.TRANSACTION_CONFIRMED if confirmed else EventTypes.TRANSACTION_PENDING
        
        await event_bus.emit(event_type, {
            'txid': txid,
            'inputs': transaction.get('inputs', []),
            'outputs': transaction.get('outputs', []),
            'timestamp': transaction.get('timestamp'),
            'body': transaction.get('body', {})
        }, source='blockchain')
        
        # Collect affected wallets
        affected_wallets = set()
        
        # From inputs (spending wallets)
        for inp in transaction.get('inputs', []):
            if inp.get('receiver'):  # The receiver of the UTXO is spending it
                affected_wallets.add(inp['receiver'])
        
        # From outputs (receiving wallets)
        for output in transaction.get('outputs', []):
            if output.get('receiver'):
                affected_wallets.add(output['receiver'])
            if output.get('sender'):
                affected_wallets.add(output['sender'])
        
        # Emit wallet balance change events
        for wallet in affected_wallets:
            await event_bus.emit(EventTypes.WALLET_BALANCE_CHANGED, {
                'wallet_address': wallet,
                'txid': txid,
                'reason': 'transaction'
            }, source='blockchain')
        
        logger.info(f"Emitted events for transaction {txid}, affected wallets: {affected_wallets}")
        
    except Exception as e:
        logger.error(f"Error emitting transaction event: {e}")


async def emit_block_event(block_height: int, block_data: Dict[str, Any]):
    """Emit block-related events"""
    try:
        await event_bus.emit(EventTypes.BLOCK_ADDED, {
            'height': block_height,
            'block_hash': block_data.get('block_hash'),
            'timestamp': block_data.get('timestamp'),
            'tx_ids': block_data.get('tx_ids', []),
            'miner': block_data.get('miner')
        }, source='blockchain')
        
        logger.info(f"Emitted block event for height {block_height}")
        
    except Exception as e:
        logger.error(f"Error emitting block event: {e}")


async def emit_utxo_event(utxo_key: str, utxo_data: Dict[str, Any], spent: bool = False):
    """Emit UTXO-related events"""
    try:
        event_type = EventTypes.UTXO_SPENT if spent else EventTypes.UTXO_CREATED
        
        await event_bus.emit(event_type, {
            'utxo_key': utxo_key,
            'txid': utxo_data.get('txid'),
            'utxo_index': utxo_data.get('utxo_index'),
            'sender': utxo_data.get('sender'),
            'receiver': utxo_data.get('receiver'),
            'amount': utxo_data.get('amount'),
            'spent': spent
        }, source='blockchain')
        
        # Emit wallet balance change
        wallet = utxo_data.get('receiver')
        if wallet:
            await event_bus.emit(EventTypes.WALLET_BALANCE_CHANGED, {
                'wallet_address': wallet,
                'utxo_key': utxo_key,
                'reason': 'utxo_spent' if spent else 'utxo_created'
            }, source='blockchain')
        
        logger.debug(f"Emitted UTXO event for {utxo_key}, spent={spent}")
        
    except Exception as e:
        logger.error(f"Error emitting UTXO event: {e}")


class EventEmittingDatabase:
    """Wrapper for database that emits events on write operations"""
    
    def __init__(self, db):
        self._db = db
        
    def __getattr__(self, name):
        """Delegate all other attributes to the underlying database"""
        return getattr(self._db, name)
        
    def put(self, key: bytes, value: bytes):
        """Wrapped put operation that emits events"""
        # Call original put
        self._db.put(key, value)
        
        # Decode key to determine type
        key_str = key.decode('utf-8')
        
        # Handle different key types
        if key_str.startswith('tx:'):
            # Transaction was stored
            txid = key_str[3:]
            try:
                tx_data = json.loads(value.decode())
                # Use asyncio to run the async emit function
                asyncio.create_task(emit_transaction_event(txid, tx_data, confirmed=True))
            except Exception as e:
                logger.error(f"Error emitting event for transaction {txid}: {e}")
                
        elif key_str.startswith('block:'):
            # Block was stored - key format is block:{hash}
            block_hash = key_str[6:]
            try:
                block_data = json.loads(value.decode())
                # Get block height from the block data
                block_height = block_data.get('height')
                if block_height is not None:
                    asyncio.create_task(emit_block_event(block_height, block_data))
                else:
                    logger.error(f"Block {block_hash} missing height field")
            except Exception as e:
                logger.error(f"Error emitting event for block {block_hash}: {e}")
                
        elif key_str.startswith('utxo:'):
            # UTXO was stored/updated
            utxo_key = key_str[5:]
            try:
                utxo_data = json.loads(value.decode())
                spent = utxo_data.get('spent', False)
                asyncio.create_task(emit_utxo_event(utxo_key, utxo_data, spent=spent))
            except Exception as e:
                logger.error(f"Error emitting event for UTXO {utxo_key}: {e}")


# Global wrapper instance
_event_db_wrapper = None


def emit_database_event(key: bytes, value: bytes):
    """
    Emit events based on database operations.
    This should be called after any database put operation.
    """
    # Decode key to determine type
    key_str = key.decode('utf-8')
    
    # Handle different key types
    if key_str.startswith('tx:'):
        # Transaction was stored
        txid = key_str[3:]
        try:
            tx_data = json.loads(value.decode())
            # Use asyncio to run the async emit function
            asyncio.create_task(emit_transaction_event(txid, tx_data, confirmed=True))
        except Exception as e:
            logger.error(f"Error emitting event for transaction {txid}: {e}")
            
    elif key_str.startswith('block:'):
        # Block was stored - key format is block:{hash}
        block_hash = key_str[6:]
        try:
            block_data = json.loads(value.decode())
            # Get block height from the block data
            block_height = block_data.get('height')
            if block_height is not None:
                asyncio.create_task(emit_block_event(block_height, block_data))
            else:
                logger.error(f"Block {block_hash} missing height field")
        except Exception as e:
            logger.error(f"Error emitting event for block {block_hash}: {e}")
            
    elif key_str.startswith('utxo:'):
        # UTXO was stored/updated
        utxo_key = key_str[5:]
        try:
            utxo_data = json.loads(value.decode())
            spent = utxo_data.get('spent', False)
            asyncio.create_task(emit_utxo_event(utxo_key, utxo_data, spent=spent))
        except Exception as e:
            logger.error(f"Error emitting event for UTXO {utxo_key}: {e}")


def wrap_database_operations():
    """
    This function is now a no-op since we can't wrap RocksDB.
    Instead, we'll need to manually call emit_database_event after put operations.
    """
    logger.info("Event integration initialized - manual emission required")