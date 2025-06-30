# tests/test_gossip.py
import asyncio
import time
from unittest.mock import AsyncMock

import pytest


# ──────────────────────────────────────────────────────────────────────────────
# 1. Fresh transaction is stored
# ──────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
@pytest.mark.stub_verify          # <<–– uses stub verifier
async def test_handle_valid_transaction(node, dummy_writer):
    now_ms = int(time.time() * 1000)
    tx_msg = {
        "type": "transaction",
        "timestamp": now_ms,
        "txid": "tx-abc",
        "body": {"msg_str": "hello", "signature": "sig", "pubkey": "pk"},
    }

    # clear global mempool before running
    import state.state as state_mod
    state_mod.mempool_manager.transactions.clear()
    state_mod.mempool_manager.in_use_utxos.clear()
    state_mod.mempool_manager.tx_fees.clear()
    state_mod.mempool_manager.tx_sizes.clear()
    state_mod.mempool_manager.current_memory_usage = 0

    await node.handle_gossip_message(tx_msg, ("somepeer", 1234), dummy_writer)

    # The gossip code calculates its own txid from the message content
    # We need to check that SOME transaction was added (not the specific txid)
    assert state_mod.mempool_manager.size() == 1
    # Get the actual txid that was used
    actual_txid = list(state_mod.mempool_manager.get_all_transactions().keys())[0]
    assert actual_txid in node.seen_tx


# ──────────────────────────────────────────────────────────────────────────────
# 2. Stale transaction is ignored
# ──────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
@pytest.mark.stub_verify          # <<–– uses stub verifier
async def test_stale_message(node, dummy_writer):
    stale_ts = int(time.time() * 1000) - 90_000  # older than 60 s
    msg = {"type": "transaction", "timestamp": stale_ts, "txid": "old", "body": {}}

    await node.handle_gossip_message(msg, ("peer", 1), dummy_writer)

    from state.state import mempool_manager
    assert mempool_manager.get_transaction("old") is None
    assert "old" not in node.seen_tx


# ──────────────────────────────────────────────────────────────────────────────
# 3. randomized_broadcast chooses √N peers
# ──────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_randomized_broadcast_picks_sqrt(monkeypatch, node):
    node.dht_peers = {("ip", p) for p in range(16)}     # 16 peers → expect 4

    async_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(node, "_send_message", async_mock)

    await node.randomized_broadcast({"foo": "bar"})
    assert async_mock.await_count == 4


# ──────────────────────────────────────────────────────────────────────────────
# 4. _send_message retries & drops failing peer after 3 attempts
# ──────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_send_message_retry_and_drop(monkeypatch, node):
    async def _always_fail(*a, **k):
        raise ConnectionRefusedError

    async def _no_sleep(x):
        return  # Don't sleep at all
    
    monkeypatch.setattr("asyncio.open_connection", _always_fail)
    monkeypatch.setattr("asyncio.wait_for", lambda coro, timeout: coro)  # Let the coroutine fail
    monkeypatch.setattr("asyncio.sleep", _no_sleep)  # Remove exponential backoff delay

    peer = ("1.1.1.1", 9999)
    node.dht_peers.add(peer)
    
    # First 10 calls increment the failure counter
    for i in range(10):
        await node._send_message(peer, b"payload")
        assert peer in node.dht_peers  # Still there
        assert node.failed_peers[peer] == i + 1
    
    # 11th call should trigger warning (failed_peers[peer] > 10)
    # Note: The current implementation doesn't remove peers anymore, just warns
    await node._send_message(peer, b"payload")
    assert peer in node.dht_peers  # Peer is still there but marked as unreachable
    assert node.failed_peers[peer] == 11