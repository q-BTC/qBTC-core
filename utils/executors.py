"""
Thread pool executors for offloading blocking DB operations from the asyncio event loop.

Three separate pools ensure WebSocket/API reads are never starved by block processing:
- ws_executor (4 workers): high-priority, for WebSocket/API database reads
- block_executor (2 workers): for heavy block processing (_connect_block + db.write)
- general_executor (2 workers): for health checks, metrics, non-critical ops
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import TypeVar, Callable, Any

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Thread pool executors
ws_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="ws-db")
block_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="block-db")
general_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="general-db")


async def run_in_ws_executor(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a blocking function in the WebSocket/API executor (high-priority reads)."""
    loop = asyncio.get_running_loop()
    if kwargs:
        func = partial(func, *args, **kwargs)
        return await loop.run_in_executor(ws_executor, func)
    return await loop.run_in_executor(ws_executor, func, *args)


async def run_in_block_executor(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a blocking function in the block processing executor."""
    loop = asyncio.get_running_loop()
    if kwargs:
        func = partial(func, *args, **kwargs)
        return await loop.run_in_executor(block_executor, func)
    return await loop.run_in_executor(block_executor, func, *args)


async def run_in_general_executor(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a blocking function in the general-purpose executor."""
    loop = asyncio.get_running_loop()
    if kwargs:
        func = partial(func, *args, **kwargs)
        return await loop.run_in_executor(general_executor, func)
    return await loop.run_in_executor(general_executor, func, *args)


def shutdown_executors():
    """Shutdown all thread pool executors gracefully."""
    logger.info("Shutting down thread pool executors...")
    for name, executor in [("ws", ws_executor), ("block", block_executor), ("general", general_executor)]:
        try:
            executor.shutdown(wait=True, cancel_futures=False)
            logger.info(f"  {name}_executor shut down")
        except Exception as e:
            logger.error(f"  Error shutting down {name}_executor: {e}")
    logger.info("All executors shut down")
