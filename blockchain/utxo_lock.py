"""
UTXO Locking Mechanism for Preventing Double-Spend Race Conditions

This module provides thread-safe UTXO locking to ensure atomic transaction validation
and prevent double-spending during concurrent transaction processing.
"""

import threading
import time
import logging
from typing import Set, Dict, Optional, List
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class UTXOLockManager:
    """
    Thread-safe UTXO lock manager to prevent double-spend race conditions.
    
    Provides pessimistic locking for UTXOs during transaction validation
    and processing to ensure atomicity.
    """
    
    def __init__(self, lock_timeout: float = 30.0):
        """
        Initialize the UTXO lock manager.
        
        Args:
            lock_timeout: Maximum time (in seconds) a lock can be held
        """
        self.locks: Dict[str, threading.Lock] = {}
        self.lock_holders: Dict[str, threading.Thread] = {}
        self.lock_times: Dict[str, float] = {}
        self.lock_timeout = lock_timeout
        self.master_lock = threading.RLock()
        
    def _get_utxo_key(self, txid: str, index: int) -> str:
        """Generate a unique key for a UTXO."""
        return f"{txid}:{index}"
        
    def acquire_locks(self, utxos: List[tuple], timeout: float = 5.0) -> bool:
        """
        Acquire locks for multiple UTXOs atomically.
        
        Args:
            utxos: List of (txid, index) tuples
            timeout: Maximum time to wait for locks
            
        Returns:
            True if all locks acquired, False otherwise
        """
        start_time = time.time()
        acquired_locks = []
        
        try:
            # Sort UTXOs to prevent deadlocks (consistent lock ordering)
            sorted_utxos = sorted(utxos, key=lambda x: self._get_utxo_key(x[0], x[1]))
            
            for txid, index in sorted_utxos:
                utxo_key = self._get_utxo_key(txid, index)
                
                # Check timeout
                if time.time() - start_time > timeout:
                    logger.warning(f"Timeout acquiring UTXO locks after {timeout}s")
                    self._release_locks(acquired_locks)
                    return False
                    
                # Get or create lock for this UTXO
                with self.master_lock:
                    if utxo_key not in self.locks:
                        self.locks[utxo_key] = threading.Lock()
                    lock = self.locks[utxo_key]
                    
                # Try to acquire the lock
                remaining_time = timeout - (time.time() - start_time)
                if remaining_time <= 0 or not lock.acquire(timeout=remaining_time):
                    logger.warning(f"Failed to acquire lock for UTXO {utxo_key}")
                    self._release_locks(acquired_locks)
                    return False
                    
                # Record lock acquisition
                with self.master_lock:
                    self.lock_holders[utxo_key] = threading.current_thread()
                    self.lock_times[utxo_key] = time.time()
                    acquired_locks.append(utxo_key)
                    
            logger.debug(f"Acquired locks for {len(acquired_locks)} UTXOs")
            return True
            
        except Exception as e:
            logger.error(f"Error acquiring UTXO locks: {e}")
            self._release_locks(acquired_locks)
            return False
            
    def release_locks(self, utxos: List[tuple]):
        """
        Release locks for multiple UTXOs.
        
        Args:
            utxos: List of (txid, index) tuples
        """
        utxo_keys = [self._get_utxo_key(txid, index) for txid, index in utxos]
        self._release_locks(utxo_keys)
        
    def _release_locks(self, utxo_keys: List[str]):
        """
        Internal method to release locks by key.
        
        Args:
            utxo_keys: List of UTXO keys to release
        """
        for utxo_key in utxo_keys:
            try:
                with self.master_lock:
                    if utxo_key in self.locks:
                        # Only release if we hold the lock
                        if self.lock_holders.get(utxo_key) == threading.current_thread():
                            self.locks[utxo_key].release()
                            del self.lock_holders[utxo_key]
                            del self.lock_times[utxo_key]
                            logger.debug(f"Released lock for UTXO {utxo_key}")
            except Exception as e:
                logger.error(f"Error releasing lock for {utxo_key}: {e}")
                
    def cleanup_stale_locks(self):
        """
        Clean up locks that have been held for too long.
        This prevents deadlocks from crashed threads.
        """
        current_time = time.time()
        stale_locks = []
        
        with self.master_lock:
            for utxo_key, lock_time in self.lock_times.items():
                if current_time - lock_time > self.lock_timeout:
                    stale_locks.append(utxo_key)
                    
        for utxo_key in stale_locks:
            logger.warning(f"Cleaning up stale lock for UTXO {utxo_key}")
            try:
                with self.master_lock:
                    if utxo_key in self.locks:
                        # Force release the lock
                        lock = self.locks[utxo_key]
                        if lock.locked():
                            lock.release()
                        del self.lock_holders[utxo_key]
                        del self.lock_times[utxo_key]
            except Exception as e:
                logger.error(f"Error cleaning up stale lock {utxo_key}: {e}")
                
    @contextmanager
    def lock_utxos(self, utxos: List[tuple], timeout: float = 5.0):
        """
        Context manager for UTXO locking.
        
        Usage:
            with utxo_lock_manager.lock_utxos([(txid1, 0), (txid2, 1)]):
                # Process transaction with locked UTXOs
                pass
        """
        acquired = self.acquire_locks(utxos, timeout)
        if not acquired:
            raise RuntimeError("Failed to acquire UTXO locks")
            
        try:
            yield
        finally:
            self.release_locks(utxos)


# Global UTXO lock manager instance
_utxo_lock_manager = None


def get_utxo_lock_manager() -> UTXOLockManager:
    """Get the global UTXO lock manager instance."""
    global _utxo_lock_manager
    if _utxo_lock_manager is None:
        _utxo_lock_manager = UTXOLockManager()
    return _utxo_lock_manager