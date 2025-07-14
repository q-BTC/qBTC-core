"""
Singleton pattern for ChainManager to ensure consistency across the application
"""
import asyncio
from blockchain.chain_manager import ChainManager

_chain_manager_instance = None
_initialization_lock = asyncio.Lock()
_initialized = False

async def get_chain_manager() -> ChainManager:
    """Get the singleton ChainManager instance (async to ensure initialization)"""
    global _chain_manager_instance, _initialized
    
    async with _initialization_lock:
        if _chain_manager_instance is None:
            _chain_manager_instance = ChainManager()
            if not _initialized:
                await _chain_manager_instance.initialize()
                _initialized = True
    return _chain_manager_instance

def get_chain_manager_sync() -> ChainManager:
    """Get the singleton ChainManager instance without initialization (use only when you know it's initialized)"""
    global _chain_manager_instance
    if _chain_manager_instance is None:
        raise RuntimeError("ChainManager not initialized. Use get_chain_manager() instead.")
    return _chain_manager_instance

def reset_chain_manager():
    """Reset the chain manager (for testing only)"""
    global _chain_manager_instance, _initialized
    _chain_manager_instance = None
    _initialized = False