"""
Helper module to initialize ChainManager for standalone scripts
"""
import asyncio
from blockchain.chain_singleton import get_chain_manager

def init_chain_manager_sync():
    """Initialize ChainManager synchronously for standalone scripts"""
    async def _init():
        cm = await get_chain_manager()
        return cm
    
    # Run the async initialization in a new event loop
    return asyncio.run(_init())