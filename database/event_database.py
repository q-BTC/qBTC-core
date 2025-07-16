"""
Event-emitting database wrapper
"""

import json
import logging
import asyncio
from typing import Optional, Dict, Any

from database.database import get_db
from events.event_bus import event_bus, EventTypes

logger = logging.getLogger(__name__)


class EventDatabase:
    """
    Database wrapper that emits events on state changes
    """
    
    def __init__(self):
        self.db = None
        logger.info("EventDatabase initialized")
    
    def _get_db(self):
        """Get database instance"""
        if self.db is None:
            self.db = get_db()
        return self.db
    
    async def put_transaction(self, txid: str, transaction: Dict[str, Any]):
        """Store transaction and emit events"""
        try:
            db = self._get_db()
            
            # Store transaction
            key = f"tx:{txid}".encode()
            db.put(key, json.dumps(transaction).encode())
            
            # Check if this is a new transaction
            is_new = True  # In real implementation, check if tx already existed
            
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
            
            logger.debug(f"Stored transaction {txid} with events")
            
        except Exception as e:
            logger.error(f"Error storing transaction {txid}: {e}")
            raise
    
    async def put_utxo(self, utxo_key: str, utxo_data: Dict[str, Any]):
        """Store UTXO and emit events"""
        try:
            db = self._get_db()
            
            # Store UTXO
            key = f"utxo:{utxo_key}".encode()
            db.put(key, json.dumps(utxo_data).encode())
            
            # Update wallet index
            if utxo_data.get('receiver'):
                wallet_index_key = f"wallet_utxos:{utxo_data['receiver']}".encode()
                wallet_utxos = []
                wallet_index_data = db.get(wallet_index_key)
                if wallet_index_data:
                    wallet_utxos = json.loads(wallet_index_data.decode())
                if key.decode() not in wallet_utxos:
                    wallet_utxos.append(key.decode())
                    db.put(wallet_index_key, json.dumps(wallet_utxos).encode())
            
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
            
            logger.debug(f"Stored UTXO {utxo_key} with events")
            
        except Exception as e:
            logger.error(f"Error storing UTXO {utxo_key}: {e}")
            raise
    
    async def spend_utxo(self, utxo_key: str):
        """Mark UTXO as spent and emit events"""
        try:
            db = self._get_db()
            
            # Get existing UTXO
            key = f"utxo:{utxo_key}".encode()
            utxo_raw = db.get(key)
            
            if utxo_raw:
                utxo_data = json.loads(utxo_raw.decode())
                utxo_data['spent'] = True
                
                # Update UTXO
                db.put(key, json.dumps(utxo_data).encode())
                
                # Remove from wallet index
                if utxo_data.get('receiver'):
                    wallet_index_key = f"wallet_utxos:{utxo_data['receiver']}".encode()
                    wallet_index_data = db.get(wallet_index_key)
                    if wallet_index_data:
                        wallet_utxos = json.loads(wallet_index_data.decode())
                        if key.decode() in wallet_utxos:
                            wallet_utxos.remove(key.decode())
                            db.put(wallet_index_key, json.dumps(wallet_utxos).encode())
                
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
                
                logger.debug(f"Marked UTXO {utxo_key} as spent with events")
            
        except Exception as e:
            logger.error(f"Error spending UTXO {utxo_key}: {e}")
            raise
    
    async def put_block(self, block_height: int, block_data: Dict[str, Any]):
        """Store block and emit events"""
        try:
            db = self._get_db()
            
            # Store block
            key = f"block:{block_height}".encode()
            db.put(key, json.dumps(block_data).encode())
            
            # Emit block added event
            await event_bus.emit(EventTypes.BLOCK_ADDED, {
                'height': block_height,
                'block_hash': block_data.get('block_hash'),
                'timestamp': block_data.get('timestamp'),
                'tx_ids': block_data.get('tx_ids', [])
            }, source='database')
            
            logger.info(f"Stored block {block_height} with events")
            
        except Exception as e:
            logger.error(f"Error storing block {block_height}: {e}")
            raise
    
    def get(self, key: bytes) -> Optional[bytes]:
        """Get value from database"""
        db = self._get_db()
        return db.get(key)
    
    def put(self, key: bytes, value: bytes):
        """Generic put operation (no events)"""
        db = self._get_db()
        db.put(key, value)
    
    def delete(self, key: bytes):
        """Delete from database"""
        db = self._get_db()
        db.delete(key)


# Global event database instance
event_db = EventDatabase()