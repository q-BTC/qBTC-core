"""
Event-emitting database wrapper with atomic operations
"""

import json
import logging
import asyncio
from typing import Optional, Dict, Any
from rocksdict import WriteBatch

from database.database import get_db
from events.event_bus import event_bus, EventTypes

logger = logging.getLogger(__name__)


class EventDatabase:
    """
    Database wrapper that emits events on state changes
    All operations are atomic using WriteBatch
    """
    
    def __init__(self):
        self.db = None
        logger.info("EventDatabase initialized with atomic operations")
    
    def _get_db(self):
        """Get database instance"""
        if self.db is None:
            self.db = get_db()
        return self.db
    
    async def put_transaction(self, txid: str, transaction: Dict[str, Any]):
        """Store transaction atomically and emit events"""
        try:
            db = self._get_db()
            
            # Check if this is a new transaction
            key = f"tx:{txid}".encode()
            existing = db.get(key)
            is_new = existing is None
            
            # Store transaction atomically
            batch = WriteBatch()
            batch.put(key, json.dumps(transaction).encode())
            db.write(batch)
            
            # Emit events only after successful write
            if is_new:
                # Emit transaction event
                event_type = (EventTypes.TRANSACTION_CONFIRMED 
                            if transaction.get('confirmed', False) 
                            else EventTypes.TRANSACTION_PENDING)
                
                await event_bus.emit(event_type, {
                    'txid': txid,
                    'transaction': transaction
                }, source='database')
                
                # Check for wallet balance changes
                affected_wallets = set()
                for output in transaction.get('outputs', []):
                    if output.get('receiver'):
                        affected_wallets.add(output['receiver'])
                    if output.get('sender'):
                        affected_wallets.add(output['sender'])
                
                # Emit wallet balance change events
                for wallet in affected_wallets:
                    await event_bus.emit(EventTypes.WALLET_BALANCE_CHANGED, {
                        'wallet_address': wallet,
                        'txid': txid
                    }, source='database')
            
            logger.debug(f"Stored transaction {txid} atomically with events")
            
        except Exception as e:
            logger.error(f"Error storing transaction {txid}: {e}")
            raise
    
    async def put_utxo(self, utxo_key: str, utxo_data: Dict[str, Any]):
        """Store UTXO atomically and emit events"""
        try:
            db = self._get_db()
            
            # Store UTXO atomically
            key = f"utxo:{utxo_key}".encode()
            batch = WriteBatch()
            batch.put(key, json.dumps(utxo_data).encode())
            db.write(batch)
            
            # Emit events only after successful write
            # Wallet indexes are managed by wallet_index.py
            
            # Emit UTXO created event
            await event_bus.emit(EventTypes.UTXO_CREATED, {
                'utxo_key': utxo_key,
                'utxo_data': utxo_data
            }, source='database')
            
            # Emit wallet balance change for receiver
            if utxo_data.get('receiver'):
                await event_bus.emit(EventTypes.WALLET_BALANCE_CHANGED, {
                    'wallet_address': utxo_data['receiver'],
                    'utxo_key': utxo_key
                }, source='database')
            
            logger.debug(f"Stored UTXO {utxo_key} atomically with events")
            
        except Exception as e:
            logger.error(f"Error storing UTXO {utxo_key}: {e}")
            raise
    
    async def spend_utxo(self, utxo_key: str):
        """Mark UTXO as spent atomically and emit events"""
        try:
            db = self._get_db()
            
            # Get existing UTXO
            key = f"utxo:{utxo_key}".encode()
            utxo_raw = db.get(key)
            
            if utxo_raw:
                utxo_data = json.loads(utxo_raw.decode())
                utxo_data['spent'] = True
                
                # Update UTXO atomically
                batch = WriteBatch()
                batch.put(key, json.dumps(utxo_data).encode())
                db.write(batch)
                
                # Emit events only after successful write
                # Wallet indexes are managed by wallet_index.py
                
                # Emit UTXO spent event
                await event_bus.emit(EventTypes.UTXO_SPENT, {
                    'utxo_key': utxo_key,
                    'utxo_data': utxo_data
                }, source='database')
                
                # Emit wallet balance change for sender
                if utxo_data.get('receiver'):  # The receiver is now spending it
                    await event_bus.emit(EventTypes.WALLET_BALANCE_CHANGED, {
                        'wallet_address': utxo_data['receiver'],
                        'utxo_key': utxo_key
                    }, source='database')
                
                logger.debug(f"Marked UTXO {utxo_key} as spent atomically with events")
            
        except Exception as e:
            logger.error(f"Error spending UTXO {utxo_key}: {e}")
            raise
    
    async def put_block(self, block_height: int, block_data: Dict[str, Any]):
        """Store block atomically and emit events"""
        try:
            db = self._get_db()
            
            # Get block hash from block_data
            block_hash = block_data.get('block_hash')
            if not block_hash:
                raise ValueError(f"Block data missing block_hash field at height {block_height}")
            
            # Store block atomically using block_hash as key (consistent with rest of codebase)
            key = f"block:{block_hash}".encode()
            batch = WriteBatch()
            batch.put(key, json.dumps(block_data).encode())
            db.write(batch)
            
            # Emit events only after successful write
            # Emit block added event
            await event_bus.emit(EventTypes.BLOCK_ADDED, {
                'height': block_height,
                'block_hash': block_hash,
                'timestamp': block_data.get('timestamp'),
                'tx_ids': block_data.get('tx_ids', [])
            }, source='database')
            
            logger.info(f"Stored block {block_hash} at height {block_height} atomically with events")
            
        except Exception as e:
            logger.error(f"Error storing block at height {block_height}: {e}")
            raise
    
    async def batch_put_utxos(self, utxos: Dict[str, Dict[str, Any]]):
        """Store multiple UTXOs atomically in a single transaction"""
        try:
            db = self._get_db()
            
            # Create batch for atomic write
            batch = WriteBatch()
            
            # Add all UTXOs to batch
            for utxo_key, utxo_data in utxos.items():
                key = f"utxo:{utxo_key}".encode()
                batch.put(key, json.dumps(utxo_data).encode())
            
            # Write all UTXOs atomically
            db.write(batch)
            
            # Emit events only after successful write
            for utxo_key, utxo_data in utxos.items():
                await event_bus.emit(EventTypes.UTXO_CREATED, {
                    'utxo_key': utxo_key,
                    'utxo_data': utxo_data
                }, source='database')
                
                if utxo_data.get('receiver'):
                    await event_bus.emit(EventTypes.WALLET_BALANCE_CHANGED, {
                        'wallet_address': utxo_data['receiver'],
                        'utxo_key': utxo_key
                    }, source='database')
            
            logger.debug(f"Stored {len(utxos)} UTXOs atomically with events")
            
        except Exception as e:
            logger.error(f"Error batch storing UTXOs: {e}")
            raise
    
    def get(self, key: bytes) -> Optional[bytes]:
        """Get value from database"""
        db = self._get_db()
        return db.get(key)
    
    def put(self, key: bytes, value: bytes):
        """Generic put operation atomically (no events)"""
        db = self._get_db()
        batch = WriteBatch()
        batch.put(key, value)
        db.write(batch)
    
    def delete(self, key: bytes):
        """Delete from database atomically"""
        db = self._get_db()
        batch = WriteBatch()
        batch.delete(key)
        db.write(batch)
    
    def batch_operation(self, operations: list):
        """
        Perform multiple database operations atomically
        
        Args:
            operations: List of tuples (operation, key, value)
                       where operation is 'put' or 'delete'
        """
        db = self._get_db()
        batch = WriteBatch()
        
        for op in operations:
            if op[0] == 'put':
                batch.put(op[1], op[2])
            elif op[0] == 'delete':
                batch.delete(op[1])
            else:
                raise ValueError(f"Unknown operation: {op[0]}")
        
        db.write(batch)
        logger.debug(f"Executed {len(operations)} operations atomically")


# Global event database instance
event_db = EventDatabase()